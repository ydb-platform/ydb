package tvm

import (
	"context"
	"net/http"

	"golang.org/x/xerrors"

	"a.yandex-team.ru/library/go/core/log"
	"a.yandex-team.ru/library/go/yandex/tvm"
)

// WithAllowedClients sets list of allowed clients.
func WithAllowedClients(allowedClients []tvm.ClientID) MiddlewareOption {
	return func(m *middleware) {
		m.authClient = func(_ context.Context, cid tvm.ClientID) error {
			for _, allowed := range allowedClients {
				if allowed == cid {
					return nil
				}
			}

			return xerrors.Errorf("client with tvm_id=%d is not whitelisted", cid)
		}
	}
}

// WithClientAuth sets custom function for client authorization.
func WithClientAuth(authClient func(context.Context, tvm.ClientID) error) MiddlewareOption {
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
