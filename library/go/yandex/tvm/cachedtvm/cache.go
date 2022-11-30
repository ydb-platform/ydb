package cachedtvm

import (
	"time"

	"github.com/karlseguin/ccache/v2"
)

type cache struct {
	*ccache.Cache
	ttl time.Duration
}

func (c *cache) Fetch(key string, fn func() (interface{}, error)) (*ccache.Item, error) {
	return c.Cache.Fetch(key, c.ttl, fn)
}

func (c *cache) Stop() {
	if c.Cache != nil {
		c.Cache.Stop()
	}
}
