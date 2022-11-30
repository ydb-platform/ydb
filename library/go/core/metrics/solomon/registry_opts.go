package solomon

import (
	"context"

	"a.yandex-team.ru/library/go/core/metrics"
	"a.yandex-team.ru/library/go/core/metrics/collect"
	"a.yandex-team.ru/library/go/core/metrics/internal/pkg/registryutil"
)

type RegistryOpts struct {
	Separator  rune
	Prefix     string
	Tags       map[string]string
	Rated      bool
	UseNameTag bool
	Collectors []func(metrics.Registry)
}

// NewRegistryOpts returns new initialized instance of RegistryOpts
func NewRegistryOpts() *RegistryOpts {
	return &RegistryOpts{
		Separator:  '.',
		Tags:       make(map[string]string),
		UseNameTag: false,
	}
}

// SetUseNameTag overrides current UseNameTag opt
func (o *RegistryOpts) SetUseNameTag(useNameTag bool) *RegistryOpts {
	o.UseNameTag = useNameTag
	return o
}

// SetTags overrides existing tags
func (o *RegistryOpts) SetTags(tags map[string]string) *RegistryOpts {
	o.Tags = tags
	return o
}

// AddTags merges given tags with existing
func (o *RegistryOpts) AddTags(tags map[string]string) *RegistryOpts {
	for k, v := range tags {
		o.Tags[k] = v
	}
	return o
}

// SetPrefix overrides existing prefix
func (o *RegistryOpts) SetPrefix(prefix string) *RegistryOpts {
	o.Prefix = prefix
	return o
}

// AppendPrefix adds given prefix as postfix to existing using separator
func (o *RegistryOpts) AppendPrefix(prefix string) *RegistryOpts {
	o.Prefix = registryutil.BuildFQName(string(o.Separator), o.Prefix, prefix)
	return o
}

// SetSeparator overrides existing separator
func (o *RegistryOpts) SetSeparator(separator rune) *RegistryOpts {
	o.Separator = separator
	return o
}

// SetRated overrides existing rated flag
func (o *RegistryOpts) SetRated(rated bool) *RegistryOpts {
	o.Rated = rated
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
