// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package cachebatch

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/metric/metricdata/metricdatatest"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/exporter/exporterhelper/internal/cache"
	"go.opentelemetry.io/collector/exporter/exporterhelper/internal/metadatatest"
	"go.opentelemetry.io/collector/exporter/exporterhelper/internal/request"
	"go.opentelemetry.io/collector/exporter/exporterhelper/internal/requesttest"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/pipeline"
)

var exporterID = component.NewID(exportertest.NopType)

type fakeCache[T any] struct {
	cache.Cache[T]
	offerErr error
	size     int64
	capacity int64
}

func (fq *fakeCache[T]) Size() int64 {
	return fq.size
}

func (fq *fakeCache[T]) Capacity() int64 {
	return fq.capacity
}

func (fq *fakeCache[T]) Offer(context.Context, T) error {
	return fq.offerErr
}

func newFakeCache[T request.Request](offerErr error, size, capacity int64) cache.Cache[T] {
	return &fakeCache[T]{offerErr: offerErr, size: size, capacity: capacity}
}

func TestObsCacheLogsSizeCapacity(t *testing.T) {
	tt := componenttest.NewTelemetry()
	t.Cleanup(func() { require.NoError(t, tt.Shutdown(context.Background())) })

	te, err := newObsCache[request.Request](Settings[request.Request]{
		Signal:    pipeline.SignalLogs,
		ID:        exporterID,
		Telemetry: tt.NewTelemetrySettings(),
	}, newFakeCache[request.Request](nil, 7, 9))
	require.NoError(t, err)
	require.NoError(t, te.Offer(context.Background(), &requesttest.FakeRequest{Items: 2}))
	metadatatest.AssertEqualExporterCacheSize(t, tt,
		[]metricdata.DataPoint[int64]{
			{
				Attributes: attribute.NewSet(
					attribute.String(exporterKey, exporterID.String()),
					attribute.String(dataTypeKey, pipeline.SignalLogs.String())),
				Value: int64(7),
			},
		}, metricdatatest.IgnoreTimestamp())
	metadatatest.AssertEqualExporterCacheCapacity(t, tt,
		[]metricdata.DataPoint[int64]{
			{
				Attributes: attribute.NewSet(
					attribute.String(exporterKey, exporterID.String()),
					attribute.String(dataTypeKey, pipeline.SignalLogs.String())),
				Value: int64(9),
			},
		}, metricdatatest.IgnoreTimestamp())
}

func TestObsCacheLogsFailure(t *testing.T) {
	tt := componenttest.NewTelemetry()
	t.Cleanup(func() { require.NoError(t, tt.Shutdown(context.Background())) })

	te, err := newObsCache[request.Request](Settings[request.Request]{
		Signal:    pipeline.SignalLogs,
		ID:        exporterID,
		Telemetry: tt.NewTelemetrySettings(),
	}, newFakeCache[request.Request](errors.New("my error"), 7, 9))
	require.NoError(t, err)
	require.Error(t, te.Offer(context.Background(), &requesttest.FakeRequest{Items: 2}))
	metadatatest.AssertEqualExporterCachedFailedLogRecords(t, tt,
		[]metricdata.DataPoint[int64]{
			{
				Attributes: attribute.NewSet(
					attribute.String(exporterKey, exporterID.String())),
				Value: int64(2),
			},
		}, metricdatatest.IgnoreTimestamp(), metricdatatest.IgnoreExemplars())
}

func TestObsCacheTracesSizeCapacity(t *testing.T) {
	tt := componenttest.NewTelemetry()
	t.Cleanup(func() { require.NoError(t, tt.Shutdown(context.Background())) })

	te, err := newObsCache[request.Request](Settings[request.Request]{
		Signal:    pipeline.SignalTraces,
		ID:        exporterID,
		Telemetry: tt.NewTelemetrySettings(),
	}, newFakeCache[request.Request](nil, 17, 19))
	require.NoError(t, err)
	require.NoError(t, te.Offer(context.Background(), &requesttest.FakeRequest{Items: 12}))
	metadatatest.AssertEqualExporterCacheSize(t, tt,
		[]metricdata.DataPoint[int64]{
			{
				Attributes: attribute.NewSet(
					attribute.String(exporterKey, exporterID.String()),
					attribute.String(dataTypeKey, pipeline.SignalTraces.String())),
				Value: int64(17),
			},
		}, metricdatatest.IgnoreTimestamp())
	metadatatest.AssertEqualExporterCacheCapacity(t, tt,
		[]metricdata.DataPoint[int64]{
			{
				Attributes: attribute.NewSet(
					attribute.String(exporterKey, exporterID.String()),
					attribute.String(dataTypeKey, pipeline.SignalTraces.String())),
				Value: int64(19),
			},
		}, metricdatatest.IgnoreTimestamp())
}

func TestObsCacheTracesFailure(t *testing.T) {
	tt := componenttest.NewTelemetry()
	t.Cleanup(func() { require.NoError(t, tt.Shutdown(context.Background())) })

	te, err := newObsCache[request.Request](Settings[request.Request]{
		Signal:    pipeline.SignalTraces,
		ID:        exporterID,
		Telemetry: tt.NewTelemetrySettings(),
	}, newFakeCache[request.Request](errors.New("my error"), 0, 0))
	require.NoError(t, err)
	require.Error(t, te.Offer(context.Background(), &requesttest.FakeRequest{Items: 12}))
	metadatatest.AssertEqualExporterCachedFailedSpans(t, tt,
		[]metricdata.DataPoint[int64]{
			{
				Attributes: attribute.NewSet(
					attribute.String(exporterKey, exporterID.String())),
				Value: int64(12),
			},
		}, metricdatatest.IgnoreTimestamp(), metricdatatest.IgnoreExemplars())
}

func TestObsCacheMetrics(t *testing.T) {
	tt := componenttest.NewTelemetry()
	t.Cleanup(func() { require.NoError(t, tt.Shutdown(context.Background())) })

	te, err := newObsCache[request.Request](Settings[request.Request]{
		Signal:    pipeline.SignalMetrics,
		ID:        exporterID,
		Telemetry: tt.NewTelemetrySettings(),
	}, newFakeCache[request.Request](nil, 27, 29))
	require.NoError(t, err)
	require.NoError(t, te.Offer(context.Background(), &requesttest.FakeRequest{Items: 22}))
	metadatatest.AssertEqualExporterCacheSize(t, tt,
		[]metricdata.DataPoint[int64]{
			{
				Attributes: attribute.NewSet(
					attribute.String(exporterKey, exporterID.String()),
					attribute.String(dataTypeKey, pipeline.SignalMetrics.String())),
				Value: int64(27),
			},
		}, metricdatatest.IgnoreTimestamp())
	metadatatest.AssertEqualExporterCacheCapacity(t, tt,
		[]metricdata.DataPoint[int64]{
			{
				Attributes: attribute.NewSet(
					attribute.String(exporterKey, exporterID.String()),
					attribute.String(dataTypeKey, pipeline.SignalMetrics.String())),
				Value: int64(29),
			},
		}, metricdatatest.IgnoreTimestamp())
}

func TestObsCacheMetricsFailure(t *testing.T) {
	tt := componenttest.NewTelemetry()
	t.Cleanup(func() { require.NoError(t, tt.Shutdown(context.Background())) })

	te, err := newObsCache[request.Request](Settings[request.Request]{
		Signal:    pipeline.SignalMetrics,
		ID:        exporterID,
		Telemetry: tt.NewTelemetrySettings(),
	}, newFakeCache[request.Request](errors.New("my error"), 0, 0))
	require.NoError(t, err)
	require.Error(t, te.Offer(context.Background(), &requesttest.FakeRequest{Items: 22}))
	metadatatest.AssertEqualExporterCachedFailedMetricPoints(t, tt,
		[]metricdata.DataPoint[int64]{
			{
				Attributes: attribute.NewSet(
					attribute.String(exporterKey, exporterID.String())),
				Value: int64(22),
			},
		}, metricdatatest.IgnoreTimestamp(), metricdatatest.IgnoreExemplars())
}
