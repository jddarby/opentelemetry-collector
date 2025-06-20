// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package cache // import "go.opentelemetry.io/collector/exporter/exporterhelper/internal/cache"

import (
	"context"
	"errors"
	"fmt"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter/exporterhelper/internal/request"
	"go.opentelemetry.io/collector/pipeline"
)

type Encoding[T any] interface {
	// Marshal is a function that can marshal a request into bytes.
	Marshal(context.Context, T) ([]byte, error)

	// Unmarshal is a function that can unmarshal bytes into a request.
	Unmarshal([]byte) (context.Context, T, error)
}

// ErrCacheIsFull is the error returned when an item is offered to the Cache and the cache is full and setup to
// not block.
// Experimental: This API is at the early stage of development and may change without backward compatibility
// until https://github.com/open-telemetry/opentelemetry-collector/issues/8122 is resolved.
var ErrCacheIsFull = errors.New("sending cache is full")

// Done represents the callback that will be called when the read request is completely processed by the
// downstream components.
// Experimental: This API is at the early stage of development and may change without backward compatibility
// until https://github.com/open-telemetry/opentelemetry-collector/issues/8122 is resolved.
type Done interface {
	// OnDone needs to be called when processing of the cache item is done.
	OnDone(error)
}

type ConsumeFunc[T any] func(context.Context, T, Done)

// Cache defines a producer-consumer exchange which can be backed by e.g. the memory-based ring buffer queue
// (boundedMemoryQueue) or via a disk-based queue (persistentQueue)
// Experimental: This API is at the early stage of development and may change without backward compatibility
// until https://github.com/open-telemetry/opentelemetry-collector/issues/8122 is resolved.
type Cache[T any] interface {
	component.Component
	// Offer inserts the specified element into this cache if it is possible to do so immediately
	// without violating capacity restrictions. If success returns no error.
	// It returns ErrCacheIsFull if no space is currently available.
	Offer(ctx context.Context, item T) error
	// Size returns the current Size of the cache
	Size() int64
	// Capacity returns the capacity of the cache.
	Capacity() int64
}

// CacheMode defines the type of cache to be created.
// It can be either a queue or a stack.
type CacheMode struct {
	val string
}

const (
	cacheModeQueue string = "queue"
	cacheModeStack string = "stack"
)

var (
	ModeQueue CacheMode = CacheMode{val: cacheModeQueue}
	ModeStack CacheMode = CacheMode{val: cacheModeStack}
)

// UnmarshalText implements TextUnmarshaler interface.
func (s *CacheMode) UnmarshalText(text []byte) error {
	switch str := string(text); str {
	case cacheModeQueue:
		*s = ModeQueue
	case cacheModeStack:
		*s = ModeStack
	default:
		return fmt.Errorf("invalid cache mode: %q", str)
	}
	return nil
}

func (s *CacheMode) MarshalText() ([]byte, error) {
	return []byte(s.val), nil
}

func (s *CacheMode) String() string {
	return s.val
}

// Settings define internal parameters for a new Cache creation.
type Settings[T any] struct {
	Mode            CacheMode
	Sizer           request.Sizer[T]
	SizerType       request.SizerType
	Capacity        int64
	NumConsumers    int
	WaitForResult   bool
	BlockOnOverflow bool
	Signal          pipeline.Signal
	StorageID       *component.ID
	Encoding        Encoding[T]
	ID              component.ID
	Telemetry       component.TelemetrySettings
}

func NewCache[T any](set Settings[T], next ConsumeFunc[T]) Cache[T] {
	// Configure in-memory or persistent cache based on the config.
	if set.StorageID == nil {
		return newAsyncCache(newMemoryQueue[T](memoryQueueSettings[T]{
			sizer:           set.Sizer,
			capacity:        set.Capacity,
			waitForResult:   set.WaitForResult,
			blockOnOverflow: set.BlockOnOverflow,
		}), set.NumConsumers, next)
	}
	return newAsyncCache(newPersistentCache[T](persistentCacheSettings[T]{
		sizer:           set.Sizer,
		sizerType:       set.SizerType,
		capacity:        set.Capacity,
		mode:            set.Mode,
		blockOnOverflow: set.BlockOnOverflow,
		signal:          set.Signal,
		storageID:       *set.StorageID,
		encoding:        set.Encoding,
		id:              set.ID,
		telemetry:       set.Telemetry,
	}), set.NumConsumers, next)
}

// TODO: Investigate why linter "unused" fails if add a private "read" func on the Cache.
type readableCache[T any] interface {
	Cache[T]
	// Read pulls the next available item from the cache along with its done callback. Once processing is
	// finished, the done callback must be called to clean up the storage.
	// The function blocks until an item is available or if the cache is stopped.
	// If the cache is stopped returns false, otherwise true.
	Read(context.Context) (context.Context, T, Done, bool)
}
