package podagent

import (
	"context"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/ydb-platform/ydb/library/go/core/xerrors"
	"github.com/ydb-platform/ydb/library/go/httputil/headers"
)

const (
	EndpointURL = "http://127.0.0.1:1/"
	HTTPTimeout = 500 * time.Millisecond
)

type Client struct {
	httpc *resty.Client
}

func NewClient(opts ...Option) *Client {
	c := &Client{
		httpc: resty.New().
			SetBaseURL(EndpointURL).
			SetTimeout(HTTPTimeout),
	}

	for _, opt := range opts {
		opt(c)
	}
	return c
}

// PodAttributes returns current pod attributes.
//
// Documentation: https://deploy.yandex-team.ru/docs/reference/api/pod-agent-public-api#localhost:1pod_attributes
func (c *Client) PodAttributes(ctx context.Context) (rsp PodAttributesResponse, err error) {
	err = c.call(ctx, "/pod_attributes", &rsp)
	return
}

// PodStatus returns current pod status.
//
// Documentation: https://deploy.yandex-team.ru/docs/reference/api/pod-agent-public-api#localhost:1pod_status
func (c *Client) PodStatus(ctx context.Context) (rsp PodStatusResponse, err error) {
	err = c.call(ctx, "/pod_status", &rsp)
	return
}

func (c *Client) call(ctx context.Context, handler string, result interface{}) error {
	rsp, err := c.httpc.R().
		SetContext(ctx).
		ExpectContentType(headers.TypeApplicationJSON.String()).
		SetResult(&result).
		Get(handler)

	if err != nil {
		return xerrors.Errorf("failed to request pod agent API: %w", err)
	}

	if !rsp.IsSuccess() {
		return xerrors.Errorf("unexpected status code: %d", rsp.StatusCode())
	}

	return nil
}
