// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package cache // import "go.opentelemetry.io/collector/exporter/exporterhelper/internal/cache"

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"sync"

	"go.uber.org/zap"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter/exporterhelper/internal/experr"
	"go.opentelemetry.io/collector/exporter/exporterhelper/internal/request"
	"go.opentelemetry.io/collector/extension/xextension/storage"
	"go.opentelemetry.io/collector/pipeline"
)

const (
	zapKey           = "key"
	zapErrorCount    = "errorCount"
	zapNumberOfItems = "numberOfItems"

	writeIndexKey               = "wi"
	currentlyDispatchedItemsKey = "di"
	itemsToDispatchKey          = "dt"
	cacheSizeKey                = "si"

	// cacheMetadataKey is the new single key for all cache metadata.
	// TODO: Enable when https://github.com/open-telemetry/opentelemetry-collector/issues/12890 is done
	//nolint:unused
	cacheMetadataKey = "qmv0"
)

var (
	errValueNotSet        = errors.New("value not set")
	errInvalidValue       = errors.New("invalid value")
	errNoStorageClient    = errors.New("no storage client extension found")
	errWrongExtensionType = errors.New("requested extension is not a storage extension")
)

var indexDonePool = sync.Pool{
	New: func() any {
		return &indexDone{}
	},
}

type persistentCacheSettings[T any] struct {
	mode            CacheMode
	sizer           request.Sizer[T]
	sizerType       request.SizerType
	capacity        int64
	blockOnOverflow bool
	signal          pipeline.Signal
	storageID       component.ID
	encoding        Encoding[T]
	id              component.ID
	telemetry       component.TelemetrySettings
}

// persistentCache provides a persistent cache implementation backed by file storage extension
//
// Write index describes the position at which next item is going to be stored.
// Read index describes which item needs to be read next.
// When Write index = Read index, no elements are in the cache.
//
// The items currently dispatched by consumers are not deleted until the processing is finished.
// Their list is stored under a separate key.
//
//	┌───file extension-backed cache (queue)───┐
//	│                                         │
//	│     ┌───┐     ┌───┐ ┌───┐ ┌───┐ ┌───┐   │
//	│ n+1 │ n │ ... │ 4 │ │ 3 │ │ 2 │ │ 1 │   │
//	│     └───┘     └───┘ └─x─┘ └─|─┘ └─x─┘   │
//	│                       x     |     x     │
//	└───────────────────────x─────|─────x─────┘
//	   ▲              ▲     x     |     x
//	   │              │     x     |     xxxx deleted
//	   │              │     x     |
//	 write          read    x     └── currently dispatched item
//	 index          index   x
//	                        xxxx deleted
type persistentCache[T any] struct {
	set    persistentCacheSettings[T]
	logger *zap.Logger
	client storage.Client

	// isRequestSized indicates whether the cache is sized by the number of requests.
	isRequestSized bool

	// mu guards everything declared below.
	mu              sync.Mutex
	hasMoreElements *sync.Cond
	hasMoreSpace    *cond
	metadata        CacheMetadata
	readCount       int64
	refClient       int64
	stopped         bool
}

// newPersistentCache creates a new cache backed by file storage; name and signal must be a unique combination that identifies the cache storage
func newPersistentCache[T any](set persistentCacheSettings[T]) readableCache[T] {
	_, isRequestSized := set.sizer.(request.RequestsSizer[T])
	pc := &persistentCache[T]{
		set:            set,
		logger:         set.telemetry.Logger,
		isRequestSized: isRequestSized,
	}
	pc.hasMoreElements = sync.NewCond(&pc.mu)
	pc.hasMoreSpace = newCond(&pc.mu)
	return pc
}

// Start starts the persistentCache with the given number of consumers.
func (pc *persistentCache[T]) Start(ctx context.Context, host component.Host) error {
	storageClient, err := toStorageClient(ctx, pc.set.storageID, host, pc.set.id, pc.set.signal)
	if err != nil {
		return err
	}
	pc.initClient(ctx, storageClient)
	return nil
}

func (pc *persistentCache[T]) Size() int64 {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	return pc.metadata.CacheSize
}

func (pc *persistentCache[T]) Capacity() int64 {
	return pc.set.capacity
}

func (pc *persistentCache[T]) initClient(ctx context.Context, client storage.Client) {
	pc.client = client
	// Start with a reference 1 which is the reference we use for the producer goroutines and initialization.
	pc.refClient = 1
	pc.initPersistentContiguousStorage(ctx)
	// Make sure the leftover requests are handled
	pc.retrieveAndProcessNotDispatchedReqs(ctx)
}

func (pc *persistentCache[T]) initPersistentContiguousStorage(ctx context.Context) {
	wiOp := storage.GetOperation(writeIndexKey)
	dtOp := storage.GetOperation(itemsToDispatchKey)

	err := pc.client.Batch(ctx, wiOp, dtOp)

	if err == nil {
		pc.metadata.WriteIndex, err = bytesToItemIndex(wiOp.Value)
	}

	if err == nil {
		pc.metadata.ItemsToDispatch, err = bytesToItemIndexArray(dtOp.Value)
	}

	if err != nil {
		if errors.Is(err, errValueNotSet) {
			pc.logger.Info("Initializing new persistent cache with mode: " + pc.set.mode.val)
		} else {
			pc.logger.Error("Failed getting read/write index, starting with new ones", zap.Error(err))
		}
		pc.metadata.WriteIndex = 0
	}

	cacheSize := uint64(len(pc.metadata.ItemsToDispatch))

	// If the cache is sized by the number of requests, no need to read the cache size from storage.
	if cacheSize > 0 && !pc.isRequestSized {
		if restoredCacheSize, err := pc.restoreCacheSizeFromStorage(ctx); err == nil {
			cacheSize = restoredCacheSize
		}
	}
	//nolint:gosec
	pc.metadata.CacheSize = int64(cacheSize)
}

// restoreCacheSizeFromStorage restores the cache size from storage.
func (pc *persistentCache[T]) restoreCacheSizeFromStorage(ctx context.Context) (uint64, error) {
	val, err := pc.client.Get(ctx, cacheSizeKey)
	if err != nil {
		if errors.Is(err, errValueNotSet) {
			pc.logger.Warn("Cannot read the cache size snapshot from storage. "+
				"The reported cache size will be inaccurate until the initial cache is drained. "+
				"It's expected when the items sized cache enabled for the first time", zap.Error(err))
		} else {
			pc.logger.Error("Failed to read the cache size snapshot from storage. "+
				"The reported cache size will be inaccurate until the initial cache is drained.", zap.Error(err))
		}
		return 0, err
	}
	return bytesToItemIndex(val)
}

func (pc *persistentCache[T]) Shutdown(ctx context.Context) error {
	// If the cache is not initialized, there is nothing to shut down.
	if pc.client == nil {
		return nil
	}

	pc.mu.Lock()
	defer pc.mu.Unlock()
	backupErr := pc.backupCacheSize(ctx)
	// Mark this cache as stopped, so consumer don't start any more work.
	pc.stopped = true
	pc.hasMoreElements.Broadcast()
	return errors.Join(backupErr, pc.unrefClient(ctx))
}

// backupCacheSize writes the current cache size to storage. The value is used to recover the cache size
// in case if the collector is killed.
func (pc *persistentCache[T]) backupCacheSize(ctx context.Context) error {
	// No need to write the cache size if the cache is sized by the number of requests.
	// That information is already stored as difference between read and write indexes.
	if pc.isRequestSized {
		return nil
	}

	//nolint:gosec
	return pc.client.Set(ctx, cacheSizeKey, itemIndexToBytes(uint64(pc.metadata.CacheSize)))
}

// unrefClient unrefs the client, and closes if no more references. Callers MUST hold the mutex.
// This is needed because consumers of the cache may still process the requests while the cache is shutting down or immediately after.
func (pc *persistentCache[T]) unrefClient(ctx context.Context) error {
	pc.refClient--
	if pc.refClient == 0 {
		return pc.client.Close(ctx)
	}
	return nil
}

// Offer inserts the specified element into this cache if it is possible to do so immediately
// without violating capacity restrictions. If success returns no error.
// It returns ErrCacheIsFull if no space is currently available.
func (pc *persistentCache[T]) Offer(ctx context.Context, req T) error {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	return pc.putInternal(ctx, req)
}

// putInternal is the internal version that requires caller to hold the mutex lock.
func (pc *persistentCache[T]) putInternal(ctx context.Context, req T) error {
	reqSize := pc.set.sizer.Sizeof(req)
	for pc.metadata.CacheSize+reqSize > pc.set.capacity {
		if !pc.set.blockOnOverflow {
			return ErrCacheIsFull
		}
		if err := pc.hasMoreSpace.Wait(ctx); err != nil {
			return err
		}
	}

	reqBuf, err := pc.set.encoding.Marshal(ctx, req)
	if err != nil {
		return err
	}

	// Carry out a transaction where we both add the item and update the write index
	ops := []*storage.Operation{
		storage.SetOperation(writeIndexKey, itemIndexToBytes(pc.metadata.WriteIndex+1)),
		storage.SetOperation(getItemKey(pc.metadata.WriteIndex), reqBuf),
	}
	if err = pc.client.Batch(ctx, ops...); err != nil {
		return err
	}

	// If we are running in stack mode we need to add the index of the item to the list of items to dispatch.
	pc.metadata.ItemsToDispatch = append(pc.metadata.ItemsToDispatch, pc.metadata.WriteIndex)
	pc.logger.Info("Items to dispatch after write", zap.String("items", fmt.Sprintf("%v", pc.metadata.ItemsToDispatch)))
	pc.metadata.WriteIndex++
	pc.metadata.CacheSize += reqSize
	pc.hasMoreElements.Signal()

	// Back up the cache size to storage every 10 writes. The stored value is used to recover the cache size
	// in case if the collector is killed. The recovered cache size is allowed to be inaccurate.
	if (pc.metadata.WriteIndex % 10) == 5 {
		if err := pc.backupCacheSize(ctx); err != nil {
			pc.logger.Error("Error writing cache size to storage", zap.Error(err))
		}
	}

	return nil
}

func (pc *persistentCache[T]) Read(ctx context.Context) (context.Context, T, Done, bool) {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	for {
		if pc.stopped {
			var req T
			return context.Background(), req, nil, false
		}

		// Read until either a successful retrieved element or no more elements in the storage.
		for len(pc.metadata.ItemsToDispatch) != 0 {
			index, req, reqCtx, consumed := pc.getNextItem(ctx)
			// Ensure the used size and the channel size are in sync.
			if len(pc.metadata.ItemsToDispatch) == 0 {
				pc.metadata.CacheSize = 0
				pc.hasMoreSpace.Signal()
			}
			if consumed {
				id := indexDonePool.Get().(*indexDone)
				id.reset(index, pc.set.sizer.Sizeof(req), pc)
				return reqCtx, req, id, true
			}
		}

		// TODO: Need to change the Queue interface to return an error to allow distinguish between shutdown and context canceled.
		//  Until then use the sync.Cond.
		pc.hasMoreElements.Wait()
	}
}

// getNextItem pulls the next available item from the persistent storage along with its index. Once processing is
// finished, the index should be called with onDone to clean up the storage. If no new item is available,
// returns false.
func (pc *persistentCache[T]) getNextItem(ctx context.Context) (uint64, T, context.Context, bool) {
	pc.readCount++

	var index uint64
	switch pc.set.mode {
	case ModeQueue:
		index = pc.metadata.ItemsToDispatch[0]
		// Remove the first item from the list of items to dispatch.
		pc.metadata.ItemsToDispatch = pc.metadata.ItemsToDispatch[1:]
	case ModeStack:
		index = pc.metadata.ItemsToDispatch[len(pc.metadata.ItemsToDispatch)-1]
		// Remove the last item from the list of items to dispatch.
		pc.metadata.ItemsToDispatch = pc.metadata.ItemsToDispatch[:len(pc.metadata.ItemsToDispatch)-1]
	default:
		pc.logger.Error("Unknown cache mode", zap.String("mode", pc.set.mode.val))
		return 0, *new(T), context.Background(), false
	}

	pc.logger.Info("Items to dispatch after read", zap.String("items", fmt.Sprintf("%v", pc.metadata.ItemsToDispatch)))

	pc.metadata.CurrentlyDispatchedItems = append(pc.metadata.CurrentlyDispatchedItems, index)
	pc.logger.Info("Dispatched items", zap.String("items", fmt.Sprintf("%v", pc.metadata.CurrentlyDispatchedItems)))
	getOp := storage.GetOperation(getItemKey(index))
	err := pc.client.Batch(ctx,
		storage.SetOperation(itemsToDispatchKey, itemIndexArrayToBytes(pc.metadata.ItemsToDispatch)),
		storage.SetOperation(currentlyDispatchedItemsKey, itemIndexArrayToBytes(pc.metadata.CurrentlyDispatchedItems)),
		getOp)

	var request T
	restoredCtx := context.Background()
	if err == nil {
		restoredCtx, request, err = pc.set.encoding.Unmarshal(getOp.Value)
	}

	if err != nil {
		pc.logger.Debug("Failed to dispatch item", zap.Error(err))
		// We need to make sure that currently dispatched items list is cleaned
		if err = pc.itemDispatchingFinish(ctx, index); err != nil {
			pc.logger.Error("Error deleting item from cache", zap.Error(err))
		}

		return 0, request, restoredCtx, false
	}

	// Increase the reference count, so the client is not closed while the request is being processed.
	// The client cannot be closed because we hold the lock since last we checked `stopped`.
	pc.refClient++

	return index, request, restoredCtx, true
}

// onDone should be called to remove the item of the given index from the cache once processing is finished.
func (pc *persistentCache[T]) onDone(index uint64, elSize int64, consumeErr error) {
	// Delete the item from the persistent storage after it was processed.
	pc.mu.Lock()
	// Always unref client even if the consumer is shutdown because we always ref it for every valid request.
	defer func() {
		if err := pc.unrefClient(context.Background()); err != nil {
			pc.logger.Error("Error closing the storage client", zap.Error(err))
		}
		pc.mu.Unlock()
	}()

	pc.metadata.CacheSize -= elSize
	// The size might be not in sync with the cache in case it's restored from the disk
	// because we don't flush the current cache size on the disk on every read/write.
	// In that case we need to make sure it doesn't go below 0.
	if pc.metadata.CacheSize < 0 {
		pc.metadata.CacheSize = 0
	}
	pc.hasMoreSpace.Signal()

	if experr.IsShutdownErr(consumeErr) {
		// The cache is shutting down, don't mark the item as dispatched, so it's picked up again after restart.
		// TODO: Handle partially delivered requests by updating their values in the storage.
		return
	}

	if err := pc.itemDispatchingFinish(context.Background(), index); err != nil {
		pc.logger.Error("Error deleting item from cache", zap.Error(err))
	}

	// Back up the cache size to storage on every 10 reads. The stored value is used to recover the cache size
	// in case if the collector is killed. The recovered cache size is allowed to be inaccurate.
	if (pc.readCount % 10) == 0 {
		if qsErr := pc.backupCacheSize(context.Background()); qsErr != nil {
			pc.logger.Error("Error writing cache size to storage", zap.Error(qsErr))
		}
	}
}

// retrieveAndProcessNotDispatchedReqs gets the items for which sending was not finished, cleans the storage
// and moves the items at the back of the queue or top of the stack (depending on cache mode).
func (pc *persistentCache[T]) retrieveAndProcessNotDispatchedReqs(ctx context.Context) {
	var dispatchedItems []uint64

	pc.mu.Lock()
	defer pc.mu.Unlock()
	pc.logger.Debug("Checking if there are items left for dispatch by consumers")
	itemKeysBuf, err := pc.client.Get(ctx, currentlyDispatchedItemsKey)
	if err == nil {
		dispatchedItems, err = bytesToItemIndexArray(itemKeysBuf)
	}
	if err != nil {
		pc.logger.Error("Could not fetch items left for dispatch by consumers", zap.Error(err))
		return
	}

	if len(dispatchedItems) == 0 {
		pc.logger.Debug("No items left for dispatch by consumers")
		return
	}

	pc.logger.Info("Fetching items left for dispatch by consumers", zap.Int(zapNumberOfItems,
		len(dispatchedItems)))
	retrieveBatch := make([]*storage.Operation, len(dispatchedItems))
	cleanupBatch := make([]*storage.Operation, len(dispatchedItems))
	for i, it := range dispatchedItems {
		key := getItemKey(it)
		retrieveBatch[i] = storage.GetOperation(key)
		cleanupBatch[i] = storage.DeleteOperation(key)
	}
	retrieveErr := pc.client.Batch(ctx, retrieveBatch...)
	cleanupErr := pc.client.Batch(ctx, cleanupBatch...)

	if cleanupErr != nil {
		pc.logger.Debug("Failed cleaning items left by consumers", zap.Error(cleanupErr))
	}

	if retrieveErr != nil {
		pc.logger.Warn("Failed retrieving items left by consumers", zap.Error(retrieveErr))
		return
	}

	errCount := 0
	for _, op := range retrieveBatch {
		if op.Value == nil {
			pc.logger.Warn("Failed retrieving item", zap.String(zapKey, op.Key), zap.Error(errValueNotSet))
			continue
		}
		reqCtx, req, err := pc.set.encoding.Unmarshal(op.Value)
		// If error happened or item is nil, it will be efficiently ignored
		if err != nil {
			pc.logger.Warn("Failed unmarshalling item", zap.String(zapKey, op.Key), zap.Error(err))
			continue
		}
		if pc.putInternal(reqCtx, req) != nil {
			errCount++
		}
	}

	if errCount > 0 {
		pc.logger.Error("Errors occurred while moving items for dispatching back to cache",
			zap.Int(zapNumberOfItems, len(retrieveBatch)), zap.Int(zapErrorCount, errCount))
	} else {
		pc.logger.Info("Moved items for dispatching back to cache",
			zap.Int(zapNumberOfItems, len(retrieveBatch)))
	}
}

// itemDispatchingFinish removes the item from the list of currently dispatched items and deletes it from the persistent cache
func (pc *persistentCache[T]) itemDispatchingFinish(ctx context.Context, index uint64) error {
	lenCDI := len(pc.metadata.CurrentlyDispatchedItems)
	for i := 0; i < lenCDI; i++ {
		if pc.metadata.CurrentlyDispatchedItems[i] == index {
			pc.metadata.CurrentlyDispatchedItems[i] = pc.metadata.CurrentlyDispatchedItems[lenCDI-1]
			pc.metadata.CurrentlyDispatchedItems = pc.metadata.CurrentlyDispatchedItems[:lenCDI-1]
			break
		}
	}

	setOp := storage.SetOperation(currentlyDispatchedItemsKey, itemIndexArrayToBytes(pc.metadata.CurrentlyDispatchedItems))
	deleteOp := storage.DeleteOperation(getItemKey(index))
	if err := pc.client.Batch(ctx, setOp, deleteOp); err != nil {
		// got an error, try to gracefully handle it
		pc.logger.Warn("Failed updating currently dispatched items, trying to delete the item first",
			zap.Error(err))
	} else {
		// Everything ok, exit
		return nil
	}

	if err := pc.client.Batch(ctx, deleteOp); err != nil {
		// Return an error here, as this indicates an issue with the underlying storage medium
		return fmt.Errorf("failed deleting item from cache, got error from storage: %w", err)
	}

	if err := pc.client.Batch(ctx, setOp); err != nil {
		// even if this fails, we still have the right dispatched items in memory
		// at worst, we'll have the wrong list in storage, and we'll discard the nonexistent items during startup
		return fmt.Errorf("failed updating currently dispatched items, but deleted item successfully: %w", err)
	}

	return nil
}

func toStorageClient(ctx context.Context, storageID component.ID, host component.Host, ownerID component.ID, signal pipeline.Signal) (storage.Client, error) {
	ext, found := host.GetExtensions()[storageID]
	if !found {
		return nil, errNoStorageClient
	}

	storageExt, ok := ext.(storage.Extension)
	if !ok {
		return nil, errWrongExtensionType
	}

	return storageExt.GetClient(ctx, component.KindExporter, ownerID, signal.String())
}

func getItemKey(index uint64) string {
	return strconv.FormatUint(index, 10)
}

func itemIndexToBytes(value uint64) []byte {
	return binary.LittleEndian.AppendUint64([]byte{}, value)
}

func bytesToItemIndex(buf []byte) (uint64, error) {
	if buf == nil {
		return uint64(0), errValueNotSet
	}
	// The sizeof uint64 in binary is 8.
	if len(buf) < 8 {
		return 0, errInvalidValue
	}
	return binary.LittleEndian.Uint64(buf), nil
}

func itemIndexArrayToBytes(arr []uint64) []byte {
	size := len(arr)
	buf := make([]byte, 0, 4+size*8)
	//nolint:gosec
	buf = binary.LittleEndian.AppendUint32(buf, uint32(size))
	for _, item := range arr {
		buf = binary.LittleEndian.AppendUint64(buf, item)
	}
	return buf
}

func bytesToItemIndexArray(buf []byte) ([]uint64, error) {
	if len(buf) == 0 {
		return nil, nil
	}

	// The sizeof uint32 in binary is 4.
	if len(buf) < 4 {
		return nil, errInvalidValue
	}
	size := int(binary.LittleEndian.Uint32(buf))
	if size == 0 {
		return nil, nil
	}

	buf = buf[4:]
	// The sizeof uint64 in binary is 8, so we need to have size*8 bytes.
	if len(buf) < size*8 {
		return nil, errInvalidValue
	}

	val := make([]uint64, size)
	for i := 0; i < size; i++ {
		val[i] = binary.LittleEndian.Uint64(buf)
		buf = buf[8:]
	}
	return val, nil
}

type indexDone struct {
	index uint64
	size  int64
	cache interface {
		onDone(uint64, int64, error)
	}
}

func (id *indexDone) reset(index uint64, size int64, cache interface{ onDone(uint64, int64, error) }) {
	id.index = index
	id.size = size
	id.cache = cache
}

func (id *indexDone) OnDone(err error) {
	id.cache.onDone(id.index, id.size, err)
}
