// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package cachebatch // import "go.opentelemetry.io/collector/exporter/exporterhelper/internal/cachebatch"

import (
	"context"
	"errors"
	"fmt"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter/exporterhelper/internal/cache"
	"go.opentelemetry.io/collector/exporter/exporterhelper/internal/request"
	"go.opentelemetry.io/collector/exporter/exporterhelper/internal/sender"
	"go.opentelemetry.io/collector/pipeline"
)

// Settings defines settings for creating a CacheBatch.
type Settings[T any] struct {
	Signal      pipeline.Signal
	ID          component.ID
	Telemetry   component.TelemetrySettings
	Encoding    cache.Encoding[T]
	Sizers      map[request.SizerType]request.Sizer[T]
	Partitioner Partitioner[T]
}

type CacheBatch struct {
	cache   cache.Cache[request.Request]
	batcher Batcher[request.Request]
}

func NewCacheBatch(
	set Settings[request.Request],
	cfg Config,
	next sender.SendFunc[request.Request],
) (*CacheBatch, error) {
	return newCacheBatch(set, cfg, next, false)
}

func NewCacheBatchLegacyBatcher(
	set Settings[request.Request],
	cfg Config,
	next sender.SendFunc[request.Request],
) (*CacheBatch, error) {
	set.Telemetry.Logger.Warn("Configuring the exporter batcher capability separately is now deprecated. " +
		"Use sending_cache::batch instead.")
	return newCacheBatch(set, cfg, next, true)
}

func newCacheBatch(
	set Settings[request.Request],
	cfg Config,
	next sender.SendFunc[request.Request],
	oldBatcher bool,
) (*CacheBatch, error) {
	if cfg.hasBlocking {
		set.Telemetry.Logger.Error("using deprecated field `blocking`")
	}

	sizer, ok := set.Sizers[cfg.Sizer]
	if !ok {
		return nil, fmt.Errorf("cache_batch: unsupported sizer %q", cfg.Sizer)
	}

	bSet := batcherSettings[request.Request]{
		sizerType:   cfg.Sizer,
		sizer:       sizer,
		partitioner: set.Partitioner,
		next:        next,
		maxWorkers:  cfg.NumConsumers,
	}
	if oldBatcher {
		// If a user configures the old batcher, we only can support "items" sizer.
		bSet.sizerType = request.SizerTypeItems
		bSet.sizer = request.NewItemsSizer()
	}
	b := NewBatcher(cfg.Batch, bSet)
	if cfg.Batch != nil {
		// If batching is enabled, keep the number of cache consumers to 1 if batching is enabled until we support
		// sharding as described in https://github.com/open-telemetry/opentelemetry-collector/issues/12473
		cfg.NumConsumers = 1
	}

	q := cache.NewCache[request.Request](cache.Settings[request.Request]{
		Sizer:           sizer,
		SizerType:       cfg.Sizer,
		Capacity:        cfg.CacheSize,
		Mode:            cfg.CacheMode,
		NumConsumers:    cfg.NumConsumers,
		WaitForResult:   cfg.WaitForResult,
		BlockOnOverflow: cfg.BlockOnOverflow,
		Signal:          set.Signal,
		StorageID:       cfg.StorageID,
		Encoding:        set.Encoding,
		ID:              set.ID,
		Telemetry:       set.Telemetry,
	}, b.Consume)

	oq, err := newObsCache(set, q)
	if err != nil {
		return nil, err
	}

	return &CacheBatch{cache: oq, batcher: b}, nil
}

// Start is invoked during service startup.
func (qs *CacheBatch) Start(ctx context.Context, host component.Host) error {
	if err := qs.batcher.Start(ctx, host); err != nil {
		return err
	}
	if err := qs.cache.Start(ctx, host); err != nil {
		return errors.Join(err, qs.batcher.Shutdown(ctx))
	}
	return nil
}

// Shutdown is invoked during service shutdown.
func (qs *CacheBatch) Shutdown(ctx context.Context) error {
	// Stop the cache and batcher, this will drain the cache and will call the retry (which is stopped) that will only
	// try once every request.
	return errors.Join(qs.cache.Shutdown(ctx), qs.batcher.Shutdown(ctx))
}

// Send implements the requestSender interface. It puts the request in the cache.
func (qs *CacheBatch) Send(ctx context.Context, req request.Request) error {
	return qs.cache.Offer(ctx, req)
}
