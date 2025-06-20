// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package cache // import "go.opentelemetry.io/collector/exporter/exporterhelper/internal/cache"

import (
	"context"
	"sync"

	"go.opentelemetry.io/otel/trace"

	"go.opentelemetry.io/collector/component"
)

type asyncCache[T any] struct {
	readableCache[T]
	numConsumers int
	consumeFunc  ConsumeFunc[T]
	stopWG       sync.WaitGroup
}

func newAsyncCache[T any](q readableCache[T], numConsumers int, consumeFunc ConsumeFunc[T]) Cache[T] {
	return &asyncCache[T]{
		readableCache: q,
		numConsumers:  numConsumers,
		consumeFunc:   consumeFunc,
	}
}

// Start ensures that cache and all consumers are started.
func (qc *asyncCache[T]) Start(ctx context.Context, host component.Host) error {
	if err := qc.readableCache.Start(ctx, host); err != nil {
		return err
	}
	var startWG sync.WaitGroup
	for i := 0; i < qc.numConsumers; i++ {
		qc.stopWG.Add(1)
		startWG.Add(1)
		go func() {
			startWG.Done()
			defer qc.stopWG.Done()
			for {
				ctx, req, done, ok := qc.Read(context.Background())
				if !ok {
					return
				}
				qc.consumeFunc(ctx, req, done)
			}
		}()
	}
	startWG.Wait()

	return nil
}

func (qc *asyncCache[T]) Offer(ctx context.Context, req T) error {
	span := trace.SpanFromContext(ctx)
	if err := qc.readableCache.Offer(ctx, req); err != nil {
		span.AddEvent("Failed to cache item.")
		return err
	}

	span.AddEvent("Cached item.")
	return nil
}

// Shutdown ensures that cache and all consumers are stopped.
func (qc *asyncCache[T]) Shutdown(ctx context.Context) error {
	err := qc.readableCache.Shutdown(ctx)
	qc.stopWG.Wait()
	return err
}
