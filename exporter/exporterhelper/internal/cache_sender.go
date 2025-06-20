// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package internal // import "go.opentelemetry.io/collector/exporter/exporterhelper/internal"

import (
	"context"
	"math"
	"runtime"

	"go.uber.org/zap"

	"go.opentelemetry.io/collector/exporter/exporterhelper/internal/cache"
	"go.opentelemetry.io/collector/exporter/exporterhelper/internal/cachebatch"
	"go.opentelemetry.io/collector/exporter/exporterhelper/internal/request"
	"go.opentelemetry.io/collector/exporter/exporterhelper/internal/sender"
)

// CacheBatchSettings is a subset of the cachebatch.Settings that are needed when used within an Exporter.
type CacheBatchSettings[T any] struct {
	Encoding    cache.Encoding[T]
	Sizers      map[request.SizerType]request.Sizer[T]
	Partitioner cachebatch.Partitioner[T]
}

// NewDefaultCacheConfig returns the default config for cachebatch.Config.
// By default, the cache stores 1000 requests of telemetry and is non-blocking when full.
func NewDefaultCacheConfig() cachebatch.Config {
	return cachebatch.Config{
		Enabled:      true,
		Sizer:        request.SizerTypeRequests,
		NumConsumers: 10,
		// By default, batches are 8192 spans, for a total of up to 8 million spans in the cache
		// This can be estimated at 1-4 GB worth of maximum memory usage
		// This default is probably still too high, and may be adjusted further down in a future release
		CacheSize:       1_000,
		CacheMode:       cache.ModeQueue,
		BlockOnOverflow: false,
		StorageID:       nil,
		Batch:           nil,
	}
}

func NewCacheSender(
	cSet cachebatch.Settings[request.Request],
	cCfg cachebatch.Config,
	bCfg BatcherConfig,
	exportFailureMessage string,
	next sender.Sender[request.Request],
) (sender.Sender[request.Request], error) {
	exportFunc := func(ctx context.Context, req request.Request) error {
		// Have to read the number of items before sending the request since the request can
		// be modified by the downstream components like the batcher.
		itemsCount := req.ItemsCount()
		if errSend := next.Send(ctx, req); errSend != nil {
			cSet.Telemetry.Logger.Error("Exporting failed. Dropping data."+exportFailureMessage,
				zap.Error(errSend), zap.Int("dropped_items", itemsCount))
			return errSend
		}
		return nil
	}

	// TODO: Remove this when WithBatcher is removed.
	if bCfg.Enabled {
		return cachebatch.NewCacheBatchLegacyBatcher(cSet, newCacheBatchConfig(cCfg, bCfg), exportFunc)
	}
	return cachebatch.NewCacheBatch(cSet, newCacheBatchConfig(cCfg, bCfg), exportFunc)
}

func newCacheBatchConfig(cCfg cachebatch.Config, bCfg BatcherConfig) cachebatch.Config {
	// Overwrite configuration with the legacy BatcherConfig configured via WithBatcher.
	// TODO: Remove this when WithBatcher is removed.
	if !bCfg.Enabled {
		return cCfg
	}

	// User configured cacheing, copy all config.
	if cCfg.Enabled {
		// Overwrite configuration with the legacy BatcherConfig configured via WithBatcher.
		// TODO: Remove this when WithBatcher is removed.
		cCfg.Batch = &cachebatch.BatchConfig{
			FlushTimeout: bCfg.FlushTimeout,
			MinSize:      bCfg.MinSize,
			MaxSize:      bCfg.MaxSize,
		}
		return cCfg
	}

	// This can happen only if the deprecated way to configure batching is used with a "disabled" cache.
	// TODO: Remove this when WithBatcher is removed.
	return cachebatch.Config{
		Enabled:         true,
		WaitForResult:   true,
		Sizer:           request.SizerTypeRequests,
		CacheSize:       math.MaxInt,
		NumConsumers:    runtime.NumCPU(),
		BlockOnOverflow: true,
		StorageID:       nil,
		Batch: &cachebatch.BatchConfig{
			FlushTimeout: bCfg.FlushTimeout,
			MinSize:      bCfg.MinSize,
			MaxSize:      bCfg.MaxSize,
		},
	}
}
