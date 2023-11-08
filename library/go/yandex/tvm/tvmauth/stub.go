//go:build !cgo
// +build !cgo

package tvmauth

//
// Pure 'go' stub to avoid linting CGO constrains violation errors on
// sandbox build stage of dependant projects.
//

import (
	"context"
	"errors"

	"github.com/ydb-platform/ydb/library/go/core/log"
	"github.com/ydb-platform/ydb/library/go/yandex/tvm"
)

// NewIDsOptions stub for tvmauth.NewIDsOptions.
func NewIDsOptions(secret string, dsts []tvm.ClientID) *TVMAPIOptions {
	return nil
}

// NewAliasesOptions stub for tvmauth.NewAliasesOptions
func NewAliasesOptions(secret string, dsts map[string]tvm.ClientID) *TVMAPIOptions {
	return nil
}

// NewAPIClient implemtation of tvm.Client interface.
// nolint: go-lint
func NewAPIClient(options TvmAPISettings, log log.Logger) (*Client, error) {
	return nil, tvm.ErrNotSupported
}

// NewDynamicApiClient implemtation of tvm.DynamicClient interface.
//
//nolint:st1003
func NewDynamicApiClient(options TvmAPISettings, log log.Logger) (*DynamicClient, error) {
	return nil, tvm.ErrNotSupported
}

// NewToolClient stub.
func NewToolClient(options TvmToolSettings, log log.Logger) (*Client, error) {
	return nil, tvm.ErrNotSupported
}

// NewUnittestClient stub.
func NewUnittestClient(options TvmUnittestSettings) (*Client, error) {
	return nil, tvm.ErrNotSupported
}

// CheckServiceTicket implementation of tvm.Client interface.
func (c *Client) CheckServiceTicket(ctx context.Context, ticketStr string) (*tvm.CheckedServiceTicket, error) {
	return nil, tvm.ErrNotSupported
}

// CheckUserTicket implemtation of tvm.Client interface.
func (c *Client) CheckUserTicket(ctx context.Context, ticketStr string, opts ...tvm.CheckUserTicketOption) (*tvm.CheckedUserTicket, error) {
	return nil, tvm.ErrNotSupported
}

// GetServiceTicketForAlias implemtation of tvm.Client interface.
func (c *Client) GetServiceTicketForAlias(ctx context.Context, alias string) (string, error) {
	return "", tvm.ErrNotSupported
}

// GetServiceTicketForID implemtation of tvm.Client interface.
func (c *Client) GetServiceTicketForID(ctx context.Context, dstID tvm.ClientID) (string, error) {
	return "", tvm.ErrNotSupported
}

// GetStatus implemtation of tvm.Client interface.
func (c *Client) GetStatus(ctx context.Context) (tvm.ClientStatusInfo, error) {
	return tvm.ClientStatusInfo{}, tvm.ErrNotSupported
}

func (c *Client) GetRoles(ctx context.Context) (*tvm.Roles, error) {
	return nil, errors.New("not implemented")
}

func (c *Client) GetOptionalServiceTicketForID(ctx context.Context, dstID tvm.ClientID) (*string, error) {
	return nil, tvm.ErrNotSupported
}

func (c *Client) AddDsts(ctx context.Context, dsts []tvm.ClientID) error {
	return tvm.ErrNotSupported
}

func (c *Client) Destroy() {
}
