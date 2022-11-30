package tvmtool

import (
	"context"
	"net/http"
	"strings"
	"time"

	"a.yandex-team.ru/library/go/core/log"
	"a.yandex-team.ru/library/go/core/xerrors"
	"a.yandex-team.ru/library/go/yandex/tvm"
	"a.yandex-team.ru/library/go/yandex/tvm/tvmtool/internal/cache"
)

type (
	Option func(tool *Client) error
)

// Source TVM client (id or alias)
//
// WARNING: id/alias must be configured in tvmtool. Documentation: https://wiki.yandex-team.ru/passport/tvm2/tvm-daemon/#konfig
func WithSrc(src string) Option {
	return func(tool *Client) error {
		tool.src = src
		return nil
	}
}

// Auth token
func WithAuthToken(token string) Option {
	return func(tool *Client) error {
		tool.authToken = token
		return nil
	}
}

// Use custom HTTP client
func WithHTTPClient(client *http.Client) Option {
	return func(tool *Client) error {
		tool.ownHTTPClient = false
		tool.httpClient = client
		return nil
	}
}

// Enable or disable service tickets cache
//
// Enabled by default
func WithCacheEnabled(enabled bool) Option {
	return func(tool *Client) error {
		switch {
		case enabled && tool.cache == nil:
			tool.cache = cache.New(cacheTTL, cacheMaxTTL)
		case !enabled:
			tool.cache = nil
		}
		return nil
	}
}

// Overrides blackbox environment defined in config.
//
// Documentation about environment overriding: https://wiki.yandex-team.ru/passport/tvm2/tvm-daemon/#/tvm/checkusr
func WithOverrideEnv(bbEnv tvm.BlackboxEnv) Option {
	return func(tool *Client) error {
		tool.bbEnv = strings.ToLower(bbEnv.String())
		return nil
	}
}

// WithLogger sets logger for tvm client.
func WithLogger(l log.Structured) Option {
	return func(tool *Client) error {
		tool.l = l
		return nil
	}
}

// WithRefreshFrequency sets service tickets refresh frequency.
// Frequency must be lower chan cacheTTL (10 min)
//
// Default: 8 min
func WithRefreshFrequency(freq time.Duration) Option {
	return func(tool *Client) error {
		if freq > cacheTTL {
			return xerrors.Errorf("refresh frequency must be lower than cacheTTL (%d > %d)", freq, cacheTTL)
		}

		tool.refreshFreq = int64(freq.Seconds())
		return nil
	}
}

// WithBackgroundUpdate force Client to update all service ticket at background.
// You must manually cancel given ctx to stops refreshing.
//
// Default: disabled
func WithBackgroundUpdate(ctx context.Context) Option {
	return func(tool *Client) error {
		tool.bgCtx, tool.bgCancel = context.WithCancel(ctx)
		return nil
	}
}
