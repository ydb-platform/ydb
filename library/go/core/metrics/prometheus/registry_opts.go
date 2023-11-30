package prometheus

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/expfmt"
	"github.com/ydb-platform/ydb/library/go/core/metrics"
	"github.com/ydb-platform/ydb/library/go/core/metrics/collect"
	"github.com/ydb-platform/ydb/library/go/core/metrics/internal/pkg/registryutil"
)

type PrometheusRegistry interface {
	prometheus.Registerer
	prometheus.Gatherer
}

type RegistryOpts struct {
	Prefix        string
	Tags          map[string]string
	rg            PrometheusRegistry
	Collectors    []func(metrics.Registry)
	NameSanitizer func(string) string
	StreamFormat  expfmt.Format
}

// NewRegistryOpts returns new initialized instance of RegistryOpts.
func NewRegistryOpts() *RegistryOpts {
	return &RegistryOpts{
		Tags:         make(map[string]string),
		StreamFormat: StreamCompact,
	}
}

// SetTags overrides existing tags.
func (o *RegistryOpts) SetTags(tags map[string]string) *RegistryOpts {
	o.Tags = tags
	return o
}

// AddTags merges given tags with existing.
func (o *RegistryOpts) AddTags(tags map[string]string) *RegistryOpts {
	for k, v := range tags {
		o.Tags[k] = v
	}
	return o
}

// SetPrefix overrides existing prefix.
func (o *RegistryOpts) SetPrefix(prefix string) *RegistryOpts {
	o.Prefix = prefix
	return o
}

// AppendPrefix adds given prefix as postfix to existing using separator.
func (o *RegistryOpts) AppendPrefix(prefix string) *RegistryOpts {
	o.Prefix = registryutil.BuildFQName("_", o.Prefix, prefix)
	return o
}

// SetRegistry sets the given prometheus registry for further usage instead
// of creating a new one.
//
// This is primarily used to unite externally defined metrics with metrics kept
// in the core registry.
func (o *RegistryOpts) SetRegistry(rg PrometheusRegistry) *RegistryOpts {
	o.rg = rg
	return o
}

// AddCollectors adds collectors that handle their metrics automatically (e.g. system metrics).
func (o *RegistryOpts) AddCollectors(
	ctx context.Context, c metrics.CollectPolicy, collectors ...collect.Func,
) *RegistryOpts {
	if len(collectors) == 0 {
		return o
	}

	o.Collectors = append(o.Collectors, func(r metrics.Registry) {
		for _, collector := range collectors {
			collector(ctx, r, c)
		}
	})
	return o
}

// SetNameSanitizer sets a functions which will be called for each metric's name.
// It allows to alter names, for example to replace invalid characters
func (o *RegistryOpts) SetNameSanitizer(v func(string) string) *RegistryOpts {
	o.NameSanitizer = v
	return o
}

// SetStreamFormat sets default metrics stream format
func (o *RegistryOpts) SetStreamFormat(format expfmt.Format) *RegistryOpts {
	o.StreamFormat = format
	return o
}
