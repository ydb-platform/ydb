package tvm

import (
	"context"
	"net/http"

	"github.com/ydb-platform/ydb/library/go/core/log"
	"github.com/ydb-platform/ydb/library/go/yandex/tvm"
	"golang.org/x/xerrors"
)

// WithAllowedClients sets list of allowed clients.
func WithAllowedClients(allowedClients []tvm.ClientID) MiddlewareOption {
	return func(m *middleware) {
		m.authClient = func(_ context.Context, src tvm.ClientID, dst tvm.ClientID) error {
			for _, allowed := range allowedClients {
				if allowed == src {
					return nil
				}
			}

			return xerrors.Errorf("client with tvm_id=%d is not whitelisted", dst)
		}
	}
}

// WithClientAuth sets custom function for client authorization.
func WithClientAuth(authClient func(ctx context.Context, src tvm.ClientID, dst tvm.ClientID) error) MiddlewareOption {
	return func(m *middleware) {
		m.authClient = authClient
	}
}

// WithErrorHandler sets http handler invoked for rejected requests.
func WithErrorHandler(h func(w http.ResponseWriter, r *http.Request, err error)) MiddlewareOption {
	return func(m *middleware) {
		m.onError = h
	}
}

// WithLogger sets logger.
func WithLogger(l log.Structured) MiddlewareOption {
	return func(m *middleware) {
		m.l = l
	}
}
