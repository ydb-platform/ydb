package cachedtvm

import "time"

type (
	Option interface{ isCachedOption() }

	cacheOptions struct {
		ttl      time.Duration
		maxItems int64
	}

	OptionServiceTicket struct {
		Option
		cacheOptions
	}

	OptionUserTicket struct {
		Option
		cacheOptions
	}
)

func WithCheckServiceTicket(ttl time.Duration, maxSize int) Option {
	return OptionServiceTicket{
		cacheOptions: cacheOptions{
			ttl:      ttl,
			maxItems: int64(maxSize),
		},
	}
}

func WithCheckUserTicket(ttl time.Duration, maxSize int) Option {
	return OptionUserTicket{
		cacheOptions: cacheOptions{
			ttl:      ttl,
			maxItems: int64(maxSize),
		},
	}
}
