package inflight

import "time"

type Option func(*inflightPolicy)

func WithMinCollectInterval(interval time.Duration) Option {
	return func(c *inflightPolicy) {
		c.minUpdateInterval = interval
	}
}
