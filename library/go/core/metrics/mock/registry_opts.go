package mock

import (
	"github.com/ydb-platform/ydb/library/go/core/metrics/internal/pkg/registryutil"
)

type RegistryOpts struct {
	Separator                  rune
	Prefix                     string
	Tags                       map[string]string
	AllowLoadRegisteredMetrics bool
}

// NewRegistryOpts returns new initialized instance of RegistryOpts
func NewRegistryOpts() *RegistryOpts {
	return &RegistryOpts{
		Separator: '.',
		Tags:      make(map[string]string),
	}
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
