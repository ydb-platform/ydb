package tvm

import (
	"context"
	"net/http"

	"golang.org/x/xerrors"

	"a.yandex-team.ru/library/go/core/log"
	"a.yandex-team.ru/library/go/core/log/ctxlog"
	"a.yandex-team.ru/library/go/core/log/nop"
	"a.yandex-team.ru/library/go/yandex/tvm"
)

const (
	// XYaServiceTicket is http header that should be used for service ticket transfer.
	XYaServiceTicket = "X-Ya-Service-Ticket"
	// XYaUserTicket is http header that should be used for user ticket transfer.
	XYaUserTicket = "X-Ya-User-Ticket"
)

type (
	MiddlewareOption func(*middleware)

	middleware struct {
		l log.Structured

		tvm tvm.Client

		authClient func(context.Context, tvm.ClientID) error

		onError func(w http.ResponseWriter, r *http.Request, err error)
	}
)

func defaultErrorHandler(w http.ResponseWriter, r *http.Request, err error) {
	http.Error(w, err.Error(), http.StatusForbidden)
}

// CheckServiceTicket returns http middleware that validates service tickets for all incoming requests.
//
// ServiceTicket is stored on request context. It might be retrieved by calling tvm.ContextServiceTicket.
func CheckServiceTicket(tvm tvm.Client, opts ...MiddlewareOption) func(next http.Handler) http.Handler {
	m := middleware{
		tvm:     tvm,
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

		ticket, err := m.tvm.CheckServiceTicket(r.Context(), serviceTicket)
		if err != nil {
			ctxlog.Error(r.Context(), m.l.Logger(), "service ticket check failed", log.Error(err))
			m.onError(w, r, xerrors.Errorf("service ticket check failed: %w", err))
			return
		}

		if err := m.authClient(r.Context(), ticket.SrcID); err != nil {
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
