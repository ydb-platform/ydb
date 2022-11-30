package cachedtvm

import (
	"context"
	"fmt"
	"time"

	"github.com/karlseguin/ccache/v2"

	"a.yandex-team.ru/library/go/yandex/tvm"
)

const (
	DefaultTTL          = 1 * time.Minute
	DefaultMaxItems     = 100
	MaxServiceTicketTTL = 5 * time.Minute
	MaxUserTicketTTL    = 1 * time.Minute
)

type CachedClient struct {
	tvm.Client
	serviceTicketCache cache
	userTicketCache    cache
	userTicketFn       func(ctx context.Context, ticket string, opts ...tvm.CheckUserTicketOption) (*tvm.CheckedUserTicket, error)
}

func NewClient(tvmClient tvm.Client, opts ...Option) (*CachedClient, error) {
	newCache := func(o cacheOptions) cache {
		return cache{
			Cache: ccache.New(
				ccache.Configure().MaxSize(o.maxItems),
			),
			ttl: o.ttl,
		}
	}

	out := &CachedClient{
		Client: tvmClient,
		serviceTicketCache: newCache(cacheOptions{
			ttl:      DefaultTTL,
			maxItems: DefaultMaxItems,
		}),
		userTicketFn: tvmClient.CheckUserTicket,
	}

	for _, opt := range opts {
		switch o := opt.(type) {
		case OptionServiceTicket:
			if o.ttl > MaxServiceTicketTTL {
				return nil, fmt.Errorf("maximum TTL for check service ticket exceed: %s > %s", o.ttl, MaxServiceTicketTTL)
			}

			out.serviceTicketCache = newCache(o.cacheOptions)
		case OptionUserTicket:
			if o.ttl > MaxUserTicketTTL {
				return nil, fmt.Errorf("maximum TTL for check user ticket exceed: %s > %s", o.ttl, MaxUserTicketTTL)
			}

			out.userTicketFn = out.cacheCheckUserTicket
			out.userTicketCache = newCache(o.cacheOptions)
		default:
			panic(fmt.Sprintf("unexpected cache option: %T", o))
		}
	}

	return out, nil
}

func (c *CachedClient) CheckServiceTicket(ctx context.Context, ticket string) (*tvm.CheckedServiceTicket, error) {
	out, err := c.serviceTicketCache.Fetch(ticket, func() (interface{}, error) {
		return c.Client.CheckServiceTicket(ctx, ticket)
	})

	if err != nil {
		return nil, err
	}

	return out.Value().(*tvm.CheckedServiceTicket), nil
}

func (c *CachedClient) CheckUserTicket(ctx context.Context, ticket string, opts ...tvm.CheckUserTicketOption) (*tvm.CheckedUserTicket, error) {
	return c.userTicketFn(ctx, ticket, opts...)
}

func (c *CachedClient) cacheCheckUserTicket(ctx context.Context, ticket string, opts ...tvm.CheckUserTicketOption) (*tvm.CheckedUserTicket, error) {
	cacheKey := func(ticket string, opts ...tvm.CheckUserTicketOption) string {
		if len(opts) == 0 {
			return ticket
		}

		var options tvm.CheckUserTicketOptions
		for _, opt := range opts {
			opt(&options)
		}

		if options.EnvOverride == nil {
			return ticket
		}

		return fmt.Sprintf("%d:%s", *options.EnvOverride, ticket)
	}

	out, err := c.userTicketCache.Fetch(cacheKey(ticket, opts...), func() (interface{}, error) {
		return c.Client.CheckUserTicket(ctx, ticket, opts...)
	})

	if err != nil {
		return nil, err
	}

	return out.Value().(*tvm.CheckedUserTicket), nil
}

func (c *CachedClient) Close() {
	c.serviceTicketCache.Stop()
	c.userTicketCache.Stop()
}
