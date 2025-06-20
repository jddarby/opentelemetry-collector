package exporterhelper

import (
	"context"

	"go.opentelemetry.io/collector/exporter/exporterhelper/internal"
	"go.opentelemetry.io/collector/exporter/exporterhelper/internal/cachebatch"
)

type CacheBatchConfig = cachebatch.Config

// WithCache overrides the default CacheBatchConfig for an exporter.
// The default CacheBatchConfig is to disable caching.
// This option cannot be used with the new exporter helpers New[Traces|Metrics|Logs]RequestExporter.
func WithCache(config CacheBatchConfig) Option {
	return internal.WithCache(config)
}

// CacheBatchEncoding defines the encoding to be used if persistent queue is configured.
// Duplicate definition with queuebatch.Encoding since aliasing generics is not supported by default.
type CacheBatchEncoding[T any] interface {
	// Marshal is a function that can marshal a request and its context into bytes.
	Marshal(context.Context, T) ([]byte, error)

	// Unmarshal is a function that can unmarshal bytes into a request and its context.
	Unmarshal([]byte) (context.Context, T, error)
}

// CacheBatchSettings are settings for the CacheBatch component.
// They include things line Encoding to be used with persistent queue, or the available Sizers, etc.
type CacheBatchSettings = internal.CacheBatchSettings[Request]
