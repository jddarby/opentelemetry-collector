package internal

import (
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/exporter/exporterhelper/internal/request"
)

// BatcherConfig defines a configuration for batching requests based on a timeout and a minimum number of items.
// Experimental: This API is at the early stage of development and may change without backward compatibility
// until https://github.com/open-telemetry/opentelemetry-collector/issues/8122 is resolved.
type BatcherConfig struct {
	// Enabled indicates whether to not enqueue batches before sending to the consumerSender.
	Enabled bool `mapstructure:"enabled"`

	// FlushTimeout sets the time after which a batch will be sent regardless of its size.
	FlushTimeout time.Duration `mapstructure:"flush_timeout"`

	// SizeConfig sets the size limits for a batch.
	SizeConfig `mapstructure:",squash"`
}

// SizeConfig sets the size limits for a batch.
type SizeConfig struct {
	Sizer request.SizerType `mapstructure:"sizer"`

	// MinSize defines the configuration for the minimum size of a batch.
	MinSize int64 `mapstructure:"min_size"`
	// MaxSize defines the configuration for the maximum size of a batch.
	MaxSize int64 `mapstructure:"max_size"`
}

func (c *BatcherConfig) Validate() error {
	if !c.Enabled {
		return nil
	}

	if c.FlushTimeout <= 0 {
		return errors.New("`flush_timeout` must be greater than zero")
	}

	if c.Sizer != request.SizerTypeItems {
		return fmt.Errorf("unsupported sizer type: %q", c.Sizer)
	}
	if c.MinSize < 0 {
		return errors.New("`min_size` must be greater than or equal to zero")
	}
	if c.MaxSize < 0 {
		return errors.New("`max_size` must be greater than or equal to zero")
	}
	if c.MaxSize != 0 && c.MaxSize < c.MinSize {
		return errors.New("`max_size` must be greater than or equal to mix_size")
	}
	return nil
}

func NewDefaultBatcherConfig() BatcherConfig {
	return BatcherConfig{
		Enabled:      true,
		FlushTimeout: 200 * time.Millisecond,
		SizeConfig: SizeConfig{
			Sizer:   request.SizerTypeItems,
			MinSize: 8192,
		},
	}
}
