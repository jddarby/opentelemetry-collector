// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package cachebatch // import "go.opentelemetry.io/collector/exporter/exporterhelper/internal/cachebatch"

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"go.opentelemetry.io/collector/exporter/exporterhelper/internal/cache"
	"go.opentelemetry.io/collector/exporter/exporterhelper/internal/metadata"
	"go.opentelemetry.io/collector/exporter/exporterhelper/internal/request"
	"go.opentelemetry.io/collector/pipeline"
)

const (
	// ExporterKey used to identify exporters in metrics and traces.
	exporterKey = "exporter"

	// DataTypeKey used to identify the data type in the cache size metric.
	dataTypeKey = "data_type"
)

// obsCache is a helper to add observability to a cache.
type obsCache[T request.Request] struct {
	cache.Cache[T]
	tb                *metadata.TelemetryBuilder
	metricAttr        metric.MeasurementOption
	cachedFailedInst  metric.Int64Counter
	tracer            trace.Tracer
}

func newObsCache[T request.Request](set Settings[T], delegate cache.Cache[T]) (cache.Cache[T], error) {
	tb, err := metadata.NewTelemetryBuilder(set.Telemetry)
	if err != nil {
		return nil, err
	}

	exporterAttr := attribute.String(exporterKey, set.ID.String())
	asyncAttr := metric.WithAttributeSet(attribute.NewSet(exporterAttr, attribute.String(dataTypeKey, set.Signal.String())))
	err = tb.RegisterExporterCacheSizeCallback(func(_ context.Context, o metric.Int64Observer) error {
		o.Observe(delegate.Size(), asyncAttr)
		return nil
	})
	if err != nil {
		return nil, err
	}

	err = tb.RegisterExporterCacheCapacityCallback(func(_ context.Context, o metric.Int64Observer) error {
		o.Observe(delegate.Capacity(), asyncAttr)
		return nil
	})
	if err != nil {
		return nil, err
	}

	tracer := metadata.Tracer(set.Telemetry)

	or := &obsCache[T]{
		Cache:     delegate,
		tb:       tb,
		metricAttr: metric.WithAttributeSet(attribute.NewSet(exporterAttr)),
		tracer:     tracer,
	}

	switch set.Signal {
	case pipeline.SignalTraces:
		or.cachedFailedInst = tb.ExporterCachedFailedSpans
	case pipeline.SignalMetrics:
		or.cachedFailedInst = tb.ExporterCachedFailedMetricPoints
	case pipeline.SignalLogs:
		or.cachedFailedInst = tb.ExporterCachedFailedLogRecords
	}

	return or, nil
}

func (or *obsCache[T]) Shutdown(ctx context.Context) error {
	defer or.tb.Shutdown()
	return or.Cache.Shutdown(ctx)
}

func (or *obsCache[T]) Offer(ctx context.Context, req T) error {
	// Have to read the number of items before sending the request since the request can
	// be modified by the downstream components like the batcher.
	numItems := req.ItemsCount()

	ctx, span := or.tracer.Start(ctx, "exporter/cached")
	err := or.Cache.Offer(ctx, req)
	span.End()

	// No metrics recorded for profiles, remove cachedFailedInst check with nil when profiles metrics available.
	if err != nil && or.cachedFailedInst != nil {
		or.cachedFailedInst.Add(ctx, int64(numItems), or.metricAttr)
	}
	return err
}
