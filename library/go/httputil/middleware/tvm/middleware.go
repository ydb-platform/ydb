package tvm

import (
	"context"
	"net/http"

	"github.com/ydb-platform/ydb/library/go/core/log"
	"github.com/ydb-platform/ydb/library/go/core/log/ctxlog"
	"github.com/ydb-platform/ydb/library/go/core/log/nop"
	"github.com/ydb-platform/ydb/library/go/httputil/headers"
	"github.com/ydb-platform/ydb/library/go/yandex/tvm"
	"golang.org/x/xerrors"
)

const (
	// XYaServiceTicket is http header that should be used for service ticket transfer.
	XYaServiceTicket = headers.XYaServiceTicketKey
	// XYaUserTicket is http header that should be used for user ticket transfer.
	XYaUserTicket = headers.XYaUserTicketKey
)

type (
	MiddlewareOption func(*middleware)

	middleware struct {
		l log.Structured

		clients []tvm.Client

		authClient func(context.Context, tvm.ClientID, tvm.ClientID) error

		onError func(w http.ResponseWriter, r *http.Request, err error)
	}
)

func defaultErrorHandler(w http.ResponseWriter, r *http.Request, err error) {
	http.Error(w, err.Error(), http.StatusForbidden)
}

func getMiddleware(clients []tvm.Client, opts ...MiddlewareOption) middleware {
	m := middleware{
		clients: clients,
		onError: defaultErrorHandler,
	}

	for _, opt := range opts {
		opt(&m)
	}

	if m.authClient == nil {
		panic("must provide authorization policy")
	}

	if m.l == nil {
		m.l = &nop.Logger{}
	}

	return m
}

// CheckServiceTicketMultiClient returns http middleware that validates service tickets for all incoming requests.
// It tries to check ticket with all the given clients in the given order
// ServiceTicket is stored on request context. It might be retrieved by calling tvm.ContextServiceTicket.
func CheckServiceTicketMultiClient(clients []tvm.Client, opts ...MiddlewareOption) func(next http.Handler) http.Handler {
	m := getMiddleware(clients, opts...)
	return m.wrap
}

// CheckServiceTicket returns http middleware that validates service tickets for all incoming requests.
//
// ServiceTicket is stored on request context. It might be retrieved by calling tvm.ContextServiceTicket.
func CheckServiceTicket(client tvm.Client, opts ...MiddlewareOption) func(next http.Handler) http.Handler {
	m := getMiddleware([]tvm.Client{client}, opts...)
	return m.wrap
}

func (m *middleware) wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serviceTicket := r.Header.Get(XYaServiceTicket)
		if serviceTicket == "" {
			ctxlog.Error(r.Context(), m.l.Logger(), "missing service ticket")
			m.onError(w, r, xerrors.New("missing service ticket"))
			return
		}
		var (
			ticket *tvm.CheckedServiceTicket
			err    error
		)
		for _, client := range m.clients {
			ticket, err = client.CheckServiceTicket(r.Context(), serviceTicket)
			if err == nil {
				break
			}
		}
		if err != nil {
			ctxlog.Error(r.Context(), m.l.Logger(), "service ticket check failed", log.Error(err))
			m.onError(w, r, xerrors.Errorf("service ticket check failed: %w", err))
			return
		}

		if err := m.authClient(r.Context(), ticket.SrcID, ticket.DstID); err != nil {
			ctxlog.Error(r.Context(), m.l.Logger(), "client authorization failed",
				log.String("ticket", ticket.LogInfo),
				log.Error(err))
			m.onError(w, r, xerrors.Errorf("client authorization failed: %w", err))
			return
		}

		r = r.WithContext(tvm.WithServiceTicket(r.Context(), ticket))
		next.ServeHTTP(w, r)
	})
}
