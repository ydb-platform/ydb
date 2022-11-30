package tooltest

import (
	"context"
	"io/ioutil"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"a.yandex-team.ru/library/go/core/log/nop"
	"a.yandex-team.ru/library/go/yandex/tvm"
	"a.yandex-team.ru/library/go/yandex/tvm/tvmauth"
)

func recipeToolOptions(t *testing.T) tvmauth.TvmToolSettings {
	var portStr, token []byte
	portStr, err := ioutil.ReadFile("tvmtool.port")
	require.NoError(t, err)

	var port int
	port, err = strconv.Atoi(string(portStr))
	require.NoError(t, err)

	token, err = ioutil.ReadFile("tvmtool.authtoken")
	require.NoError(t, err)

	return tvmauth.TvmToolSettings{Alias: "me", Port: port, AuthToken: string(token)}
}

func TestToolClient(t *testing.T) {
	c, err := tvmauth.NewToolClient(recipeToolOptions(t), &nop.Logger{})
	require.NoError(t, err)
	defer c.Destroy()

	t.Run("GetServiceTicketForID", func(t *testing.T) {
		_, err := c.GetServiceTicketForID(context.Background(), 100500)
		require.NoError(t, err)
	})

	t.Run("GetInvalidTicket", func(t *testing.T) {
		_, err := c.GetServiceTicketForID(context.Background(), 100999)
		require.Error(t, err)
		require.IsType(t, &tvm.Error{}, err)
		require.Equal(t, tvm.ErrorBrokenTvmClientSettings, err.(*tvm.Error).Code)
	})

	t.Run("ClientStatus", func(t *testing.T) {
		status, err := c.GetStatus(context.Background())
		require.NoError(t, err)

		t.Logf("Got client status: %v", status)

		require.Equal(t, tvm.ClientStatus(0), status.Status)
		require.Equal(t, "OK", status.LastError)
	})
}
