package httppuller_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/ydb-platform/ydb/library/go/core/metrics/solomon"
	"github.com/ydb-platform/ydb/library/go/yandex/solomon/reporters/puller/httppuller"
	"github.com/ydb-platform/ydb/library/go/yandex/tvm"
)

type fakeTVMClient struct{}

func (f *fakeTVMClient) GetServiceTicketForAlias(ctx context.Context, alias string) (string, error) {
	return "", &tvm.Error{Code: tvm.ErrorMissingServiceTicket}
}

func (f *fakeTVMClient) GetServiceTicketForID(ctx context.Context, dstID tvm.ClientID) (string, error) {
	return "", &tvm.Error{Code: tvm.ErrorMissingServiceTicket}
}

func (f *fakeTVMClient) CheckServiceTicket(ctx context.Context, ticket string) (*tvm.CheckedServiceTicket, error) {
	if ticket == "qwerty" {
		return &tvm.CheckedServiceTicket{SrcID: httppuller.FetcherProdTVMID}, nil
	}

	return nil, &tvm.Error{Code: tvm.ErrorMissingServiceTicket}
}

func (f *fakeTVMClient) CheckUserTicket(ctx context.Context, ticket string, opts ...tvm.CheckUserTicketOption) (*tvm.CheckedUserTicket, error) {
	return nil, &tvm.Error{Code: tvm.ErrorMissingServiceTicket}
}

func (f *fakeTVMClient) GetStatus(ctx context.Context) (tvm.ClientStatusInfo, error) {
	return tvm.ClientStatusInfo{}, &tvm.Error{Code: tvm.ErrorMissingServiceTicket}
}

func (f *fakeTVMClient) GetRoles(ctx context.Context) (*tvm.Roles, error) {
	return nil, errors.New("not implemented")
}

var _ tvm.Client = &fakeTVMClient{}

func TestHandler_ServiceTicketValidation(t *testing.T) {
	registry := solomon.NewRegistry(solomon.NewRegistryOpts())
	h := httppuller.NewHandler(registry, httppuller.WithTVM(&fakeTVMClient{}))

	t.Run("MissingTicket", func(t *testing.T) {
		w := httptest.NewRecorder()
		r, _ := http.NewRequest("GET", "/metrics", nil)

		h.ServeHTTP(w, r)
		assert.Equal(t, 403, w.Code)
		assert.Equal(t, "missing service ticket\n", w.Body.String())
	})

	t.Run("InvalidTicket", func(t *testing.T) {
		w := httptest.NewRecorder()
		r, _ := http.NewRequest("GET", "/metrics", nil)
		r.Header.Add("X-Ya-Service-Ticket", "123456")

		h.ServeHTTP(w, r)
		assert.Equal(t, 403, w.Code)
		assert.Truef(t, strings.HasPrefix(w.Body.String(), "service ticket check failed"), "body=%q", w.Body.String())
	})

	t.Run("GoodTicket", func(t *testing.T) {
		w := httptest.NewRecorder()
		r, _ := http.NewRequest("GET", "/metrics", nil)
		r.Header.Add("X-Ya-Service-Ticket", "qwerty")

		h.ServeHTTP(w, r)
		assert.Equal(t, 200, w.Code)
	})
}
