//go:build linux || darwin
// +build linux darwin

// tvmtool recipe exists only for linux & darwin so we skip another OSes
package tvmtool_test

import (
	"context"
	"fmt"
	"io/ioutil"
	"regexp"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"a.yandex-team.ru/library/go/core/log"
	"a.yandex-team.ru/library/go/core/log/zap"
	"a.yandex-team.ru/library/go/yandex/tvm"
	"a.yandex-team.ru/library/go/yandex/tvm/tvmtool"
)

const (
	tvmToolPortFile               = "tvmtool.port"
	tvmToolAuthTokenFile          = "tvmtool.authtoken"
	userTicketFor1120000000038691 = "3:user" +
		":CA4Q__________9_GjUKCQijrpqRpdT-ARCjrpqRpdT-ARoMYmI6c2Vzc2lvbmlkGgl0ZXN0OnRlc3Qg0oXY" +
		"zAQoAw:A-YI2yhoD7BbGU80_dKQ6vm7XADdvgD2QUFCeTI3XZ4MS4N8iENvsNDvYwsW89-vLQPv9pYqn8jxx" +
		"awkvu_ZS2aAfpU8vXtnEHvzUQfes2kMjweRJE71cyX8B0VjENdXC5QAfGyK7Y0b4elTDJzw8b28Ro7IFFbNe" +
		"qgcPInXndY"
	serviceTicketFor41_42 = "3:serv:CBAQ__________9_IgQIKRAq" +
		":VVXL3wkhpBHB7OXSeG0IhqM5AP2CP-gJRD31ksAb-q7pmssBJKtPNbH34BSyLpBllmM1dgOfwL8ICUOGUA3l" +
		"jOrwuxZ9H8ayfdrpM7q1-BVPE0sh0L9cd8lwZIW6yHejTe59s6wk1tG5MdSfncdaJpYiF3MwNHSRklNAkb6hx" +
		"vg"
	serviceTicketFor41_99 = "3:serv:CBAQ__________9_IgQIKRBj" +
		":PjJKDOsEk8VyxZFZwsVnKrW1bRyA82nGd0oIxnEFEf7DBTVZmNuxEejncDrMxnjkKwimrumV9POK4ptTo0ZPY" +
		"6Du9zHR5QxekZYwDzFkECVrv9YT2QI03odwZJX8_WCpmlgI8hUog_9yZ5YCYxrQpWaOwDXx4T7VVMwH_Z9YTZk"
)

var (
	srvTicketRe = regexp.MustCompile(`^3:serv:[A-Za-z0-9_\-]+:[A-Za-z0-9_\-]+$`)
)

func newTvmToolClient(src string, authToken ...string) (*tvmtool.Client, error) {
	raw, err := ioutil.ReadFile(tvmToolPortFile)
	if err != nil {
		return nil, err
	}

	port, err := strconv.Atoi(string(raw))
	if err != nil {
		return nil, err
	}

	var auth string
	if len(authToken) > 0 {
		auth = authToken[0]
	} else {
		raw, err = ioutil.ReadFile(tvmToolAuthTokenFile)
		if err != nil {
			return nil, err
		}
		auth = string(raw)
	}

	zlog, _ := zap.New(zap.ConsoleConfig(log.DebugLevel))

	return tvmtool.NewClient(
		fmt.Sprintf("http://localhost:%d", port),
		tvmtool.WithAuthToken(auth),
		tvmtool.WithCacheEnabled(false),
		tvmtool.WithSrc(src),
		tvmtool.WithLogger(zlog),
	)
}

func TestNewClient(t *testing.T) {
	client, err := newTvmToolClient("main")
	require.NoError(t, err)
	require.NotNil(t, client)
}

func TestClient_GetStatus(t *testing.T) {
	client, err := newTvmToolClient("main")
	require.NoError(t, err)
	status, err := client.GetStatus(context.Background())
	require.NoError(t, err, "ping must work")
	require.Equal(t, tvm.ClientOK, status.Status)
}

func TestClient_BadAuth(t *testing.T) {
	badClient, err := newTvmToolClient("main", "fake-auth")
	require.NoError(t, err)

	_, err = badClient.GetServiceTicketForAlias(context.Background(), "lala")
	require.Error(t, err)
	require.IsType(t, err, &tvmtool.Error{})
	srvTickerErr := err.(*tvmtool.Error)
	require.Equal(t, tvmtool.ErrorAuthFail, srvTickerErr.Code)
}

func TestClient_GetServiceTicket(t *testing.T) {
	tvmClient, err := newTvmToolClient("main")
	require.NoError(t, err)

	ctx := context.Background()

	t.Run("invalid_alias", func(t *testing.T) {
		// Ticket for invalid alias must fails
		t.Parallel()
		_, err := tvmClient.GetServiceTicketForAlias(ctx, "not_exists")
		require.Error(t, err, "ticket for invalid alias must fails")
		assert.IsType(t, err, &tvmtool.Error{}, "must return tvm err")
		assert.EqualError(t, err, "tvm: can't find in config destination tvmid for src = 42, dstparam = not_exists (strconv) (code ErrorBadRequest)")
	})

	t.Run("invalid_dst_id", func(t *testing.T) {
		// Ticket for invalid client id must fails
		t.Parallel()
		_, err := tvmClient.GetServiceTicketForID(ctx, 123123123)
		require.Error(t, err, "ticket for invalid ID must fails")
		assert.IsType(t, err, &tvmtool.Error{}, "must return tvm err")
		assert.EqualError(t, err, "tvm: can't find in config destination tvmid for src = 42, dstparam = 123123123 (by number) (code ErrorBadRequest)")
	})

	t.Run("by_alias", func(t *testing.T) {
		// Try to get ticket by alias
		t.Parallel()
		heTicketByAlias, err := tvmClient.GetServiceTicketForAlias(ctx, "he")
		if assert.NoError(t, err, "failed to get srv ticket to 'he'") {
			assert.Regexp(t, srvTicketRe, heTicketByAlias, "invalid 'he' srv ticket")
		}

		heCloneTicketAlias, err := tvmClient.GetServiceTicketForAlias(ctx, "he_clone")
		if assert.NoError(t, err, "failed to get srv ticket to 'he_clone'") {
			assert.Regexp(t, srvTicketRe, heCloneTicketAlias, "invalid 'he_clone' srv ticket")
		}
	})

	t.Run("by_dst_id", func(t *testing.T) {
		// Try to get ticket by id
		t.Parallel()
		heTicketByID, err := tvmClient.GetServiceTicketForID(ctx, 100500)
		if assert.NoError(t, err, "failed to get srv ticket to '100500'") {
			assert.Regexp(t, srvTicketRe, heTicketByID, "invalid '100500' srv ticket")
		}
	})
}

func TestClient_CheckServiceTicket(t *testing.T) {
	tvmClient, err := newTvmToolClient("main")
	require.NoError(t, err)

	ctx := context.Background()
	t.Run("self_to_self", func(t *testing.T) {
		t.Parallel()

		// Check from self to self
		selfTicket, err := tvmClient.GetServiceTicketForAlias(ctx, "self")
		require.NoError(t, err, "failed to get service ticket to 'self'")
		assert.Regexp(t, srvTicketRe, selfTicket, "invalid 'self' srv ticket")

		// Now we can check srv ticket
		ticketInfo, err := tvmClient.CheckServiceTicket(ctx, selfTicket)
		require.NoError(t, err, "failed to check srv ticket main -> self")

		assert.Equal(t, tvm.ClientID(42), ticketInfo.SrcID)
		assert.NotEmpty(t, ticketInfo.LogInfo)
		assert.NotEmpty(t, ticketInfo.DbgInfo)
	})

	t.Run("to_another", func(t *testing.T) {
		t.Parallel()

		// Check from another client (41) to self
		ticketInfo, err := tvmClient.CheckServiceTicket(ctx, serviceTicketFor41_42)
		require.NoError(t, err, "failed to check srv ticket 41 -> 42")

		assert.Equal(t, tvm.ClientID(41), ticketInfo.SrcID)
		assert.NotEmpty(t, ticketInfo.LogInfo)
		assert.NotEmpty(t, ticketInfo.DbgInfo)
	})

	t.Run("invalid_dst", func(t *testing.T) {
		t.Parallel()

		// Check from another client (41) to invalid dst (99)
		ticketInfo, err := tvmClient.CheckServiceTicket(ctx, serviceTicketFor41_99)
		require.Error(t, err, "srv ticket for 41 -> 99 must fails")
		assert.NotEmpty(t, ticketInfo.LogInfo)
		assert.NotEmpty(t, ticketInfo.DbgInfo)

		ticketErr := err.(*tvmtool.TicketError)
		require.IsType(t, err, &tvmtool.TicketError{})
		assert.Equal(t, tvmtool.TicketErrorOther, ticketErr.Status)
		assert.Equal(t, "Wrong ticket dst, expected 42, got 99", ticketErr.Msg)
	})

	t.Run("broken", func(t *testing.T) {
		t.Parallel()

		// Check with broken sign
		_, err := tvmClient.CheckServiceTicket(ctx, "lalala")
		require.Error(t, err, "srv ticket with broken sign must fails")
		ticketErr := err.(*tvmtool.TicketError)
		require.IsType(t, err, &tvmtool.TicketError{})
		assert.Equal(t, tvmtool.TicketErrorOther, ticketErr.Status)
		assert.Equal(t, "invalid ticket format", ticketErr.Msg)
	})
}

func TestClient_MultipleClients(t *testing.T) {
	tvmClient, err := newTvmToolClient("main")
	require.NoError(t, err)

	slaveClient, err := newTvmToolClient("slave")
	require.NoError(t, err)

	ctx := context.Background()

	ticket, err := tvmClient.GetServiceTicketForAlias(ctx, "slave")
	require.NoError(t, err, "failed to get service ticket to 'slave'")
	assert.Regexp(t, srvTicketRe, ticket, "invalid 'slave' srv ticket")

	ticketInfo, err := slaveClient.CheckServiceTicket(ctx, ticket)
	require.NoError(t, err, "failed to check srv ticket main -> self")

	assert.Equal(t, tvm.ClientID(42), ticketInfo.SrcID)
	assert.NotEmpty(t, ticketInfo.LogInfo)
	assert.NotEmpty(t, ticketInfo.DbgInfo)
}

func TestClient_CheckUserTicket(t *testing.T) {
	tvmClient, err := newTvmToolClient("main")
	require.NoError(t, err)

	ticketInfo, err := tvmClient.CheckUserTicket(context.Background(), userTicketFor1120000000038691)
	require.NoError(t, err, "failed to check user ticket")

	assert.Equal(t, tvm.UID(1120000000038691), ticketInfo.DefaultUID)
	assert.Subset(t, []tvm.UID{1120000000038691}, ticketInfo.UIDs)
	assert.Subset(t, []string{"bb:sessionid", "test:test"}, ticketInfo.Scopes)
	assert.NotEmpty(t, ticketInfo.LogInfo)
	assert.NotEmpty(t, ticketInfo.DbgInfo)
}

func TestClient_Version(t *testing.T) {
	tvmClient, err := newTvmToolClient("main")
	require.NoError(t, err)

	version, err := tvmClient.Version(context.Background())
	require.NoError(t, err)
	require.NotEmpty(t, version)
}
