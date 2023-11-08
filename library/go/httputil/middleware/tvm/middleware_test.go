package tvm

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb/library/go/yandex/tvm"
)

type fakeClient struct {
	ticket *tvm.CheckedServiceTicket
	err    error
}

func (f *fakeClient) GetServiceTicketForAlias(ctx context.Context, alias string) (string, error) {
	panic("implement me")
}

func (f *fakeClient) GetServiceTicketForID(ctx context.Context, dstID tvm.ClientID) (string, error) {
	panic("implement me")
}

func (f *fakeClient) CheckServiceTicket(ctx context.Context, ticket string) (*tvm.CheckedServiceTicket, error) {
	return f.ticket, f.err
}

func (f *fakeClient) CheckUserTicket(ctx context.Context, ticket string, opts ...tvm.CheckUserTicketOption) (*tvm.CheckedUserTicket, error) {
	panic("implement me")
}

func (f *fakeClient) GetStatus(ctx context.Context) (tvm.ClientStatusInfo, error) {
	panic("implement me")
}

func (f *fakeClient) GetRoles(ctx context.Context) (*tvm.Roles, error) {
	panic("implement me")
}

func TestMiddlewareOkTicket(t *testing.T) {
	var f fakeClient
	f.ticket = &tvm.CheckedServiceTicket{SrcID: 42}

	m := CheckServiceTicket(&f, WithAllowedClients([]tvm.ClientID{42}))

	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set(XYaServiceTicket, "123")

	var handlerCalled bool
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handlerCalled = true
		require.Equal(t, f.ticket, tvm.ContextServiceTicket(r.Context()))
	})

	m(handler).ServeHTTP(nil, r)
	require.True(t, handlerCalled)
}

func TestMiddlewareClientNotAllowed(t *testing.T) {
	var f fakeClient
	f.ticket = &tvm.CheckedServiceTicket{SrcID: 43}

	m := CheckServiceTicket(&f, WithAllowedClients([]tvm.ClientID{42}))

	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set(XYaServiceTicket, "123")
	w := httptest.NewRecorder()

	m(nil).ServeHTTP(w, r)
	require.Equal(t, 403, w.Code)
}

func TestMiddlewareMissingTicket(t *testing.T) {
	m := CheckServiceTicket(nil, WithAllowedClients([]tvm.ClientID{42}))

	r := httptest.NewRequest("GET", "/", nil)
	w := httptest.NewRecorder()

	m(nil).ServeHTTP(w, r)
	require.Equal(t, 403, w.Code)
}

func TestMiddlewareInvalidTicket(t *testing.T) {
	var f fakeClient
	f.err = &tvm.Error{}

	m := CheckServiceTicket(&f, WithAllowedClients([]tvm.ClientID{42}))

	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set(XYaServiceTicket, "123")
	w := httptest.NewRecorder()

	m(nil).ServeHTTP(w, r)
	require.Equal(t, 403, w.Code)
}

func TestMiddlewareMultipleDsts(t *testing.T) {
	var f1, f2, f3 fakeClient
	f1.err = &tvm.Error{}
	f2.err = &tvm.Error{}
	f3.ticket = &tvm.CheckedServiceTicket{SrcID: 42, DstID: 43}

	m := CheckServiceTicketMultiClient([]tvm.Client{
		&f1,
		&f3,
		&f2,
	}, WithClientAuth(func(ctx context.Context, src tvm.ClientID, dst tvm.ClientID) error {
		require.Equal(t, tvm.ClientID(43), dst)
		require.Equal(t, tvm.ClientID(42), src)
		return nil
	}))

	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set(XYaServiceTicket, "123")

	var handlerCalled bool
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handlerCalled = true
		require.Equal(t, f3.ticket, tvm.ContextServiceTicket(r.Context()))
	})

	m(handler).ServeHTTP(nil, r)
	require.True(t, handlerCalled)
}
