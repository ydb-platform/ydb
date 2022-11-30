package apitest

import (
	"context"
	"io/ioutil"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	uzap "go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"a.yandex-team.ru/library/go/core/log"
	"a.yandex-team.ru/library/go/core/log/nop"
	"a.yandex-team.ru/library/go/core/log/zap"
	"a.yandex-team.ru/library/go/yandex/tvm"
	"a.yandex-team.ru/library/go/yandex/tvm/tvmauth"
)

func apiSettings(t testing.TB, client tvm.ClientID) tvmauth.TvmAPISettings {
	var portStr []byte
	portStr, err := ioutil.ReadFile("tvmapi.port")
	require.NoError(t, err)

	var port int
	port, err = strconv.Atoi(string(portStr))
	require.NoError(t, err)
	env := tvm.BlackboxProd

	if client == 1000501 {
		return tvmauth.TvmAPISettings{
			SelfID: 1000501,

			EnableServiceTicketChecking: true,
			BlackboxEnv:                 &env,

			ServiceTicketOptions: tvmauth.NewIDsOptions(
				"bAicxJVa5uVY7MjDlapthw",
				[]tvm.ClientID{1000502}),

			TVMHost: "localhost",
			TVMPort: port,
		}
	} else if client == 1000502 {
		return tvmauth.TvmAPISettings{
			SelfID: 1000502,

			EnableServiceTicketChecking: true,
			BlackboxEnv:                 &env,

			ServiceTicketOptions: tvmauth.NewAliasesOptions(
				"e5kL0vM3nP-nPf-388Hi6Q",
				map[string]tvm.ClientID{
					"cl1000501": 1000501,
					"cl1000503": 1000503,
				}),

			TVMHost: "localhost",
			TVMPort: port,
		}
	} else {
		t.Fatalf("Bad client id: %d", client)
		return tvmauth.TvmAPISettings{}
	}
}

func TestErrorPassing(t *testing.T) {
	_, err := tvmauth.NewAPIClient(tvmauth.TvmAPISettings{}, &nop.Logger{})
	require.Error(t, err)
}

func TestGetServiceTicketForID(t *testing.T) {
	c1000501, err := tvmauth.NewAPIClient(apiSettings(t, 1000501), &nop.Logger{})
	require.NoError(t, err)
	defer c1000501.Destroy()

	c1000502, err := tvmauth.NewAPIClient(apiSettings(t, 1000502), &nop.Logger{})
	require.NoError(t, err)
	defer c1000502.Destroy()

	ticketStr, err := c1000501.GetServiceTicketForID(context.Background(), 1000502)
	require.NoError(t, err)

	ticket, err := c1000502.CheckServiceTicket(context.Background(), ticketStr)
	require.NoError(t, err)
	require.Equal(t, tvm.ClientID(1000501), ticket.SrcID)

	ticketStrByAlias, err := c1000501.GetServiceTicketForAlias(context.Background(), "1000502")
	require.NoError(t, err)
	require.Equal(t, ticketStr, ticketStrByAlias)

	_, err = c1000501.CheckServiceTicket(context.Background(), ticketStr)
	require.Error(t, err)
	require.IsType(t, err, &tvm.TicketError{})
	require.Equal(t, tvm.TicketInvalidDst, err.(*tvm.TicketError).Status)

	_, err = c1000501.GetServiceTicketForID(context.Background(), 127)
	require.Error(t, err)
	require.IsType(t, err, &tvm.Error{})

	ticketStr, err = c1000502.GetServiceTicketForID(context.Background(), 1000501)
	require.NoError(t, err)
	ticketStrByAlias, err = c1000502.GetServiceTicketForAlias(context.Background(), "cl1000501")
	require.NoError(t, err)
	require.Equal(t, ticketStr, ticketStrByAlias)

	_, err = c1000502.GetServiceTicketForAlias(context.Background(), "1000501")
	require.Error(t, err)
	require.IsType(t, err, &tvm.Error{})
}

func TestLogger(t *testing.T) {
	logger, err := zap.New(zap.ConsoleConfig(log.DebugLevel))
	require.NoError(t, err)

	core, logs := observer.New(zap.ZapifyLevel(log.DebugLevel))
	logger.L = logger.L.WithOptions(uzap.WrapCore(func(_ zapcore.Core) zapcore.Core {
		return core
	}))

	c1000502, err := tvmauth.NewAPIClient(apiSettings(t, 1000502), logger)
	require.NoError(t, err)
	defer c1000502.Destroy()

	loggedEntries := logs.AllUntimed()
	for idx := 0; len(loggedEntries) < 7 && idx < 250; idx++ {
		time.Sleep(100 * time.Millisecond)
		loggedEntries = logs.AllUntimed()
	}

	var plainLog string
	for _, le := range loggedEntries {
		plainLog += le.Message + "\n"
	}

	require.Contains(
		t,
		plainLog,
		"Thread-worker started")
}

func BenchmarkServiceTicket(b *testing.B) {
	c1000501, err := tvmauth.NewAPIClient(apiSettings(b, 1000501), &nop.Logger{})
	require.NoError(b, err)
	defer c1000501.Destroy()

	c1000502, err := tvmauth.NewAPIClient(apiSettings(b, 1000502), &nop.Logger{})
	require.NoError(b, err)
	defer c1000502.Destroy()

	b.Run("GetServiceTicketForID", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_, err := c1000501.GetServiceTicketForID(context.Background(), 1000502)
				require.NoError(b, err)
			}
		})
	})

	ticketStr, err := c1000501.GetServiceTicketForID(context.Background(), 1000502)
	require.NoError(b, err)

	b.Run("CheckServiceTicket", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_, err := c1000502.CheckServiceTicket(context.Background(), ticketStr)
				require.NoError(b, err)
			}
		})
	})
}

const serviceTicketStr = "3:serv:CBAQ__________9_IggIlJEGELaIPQ:KC8zKTnoM7GQ8UkBixoAlDt7CAuNIO_6J4rzeqelj7wn7vCKBfsy1jlg2UIvBw0JKUUc6116s5aBw1-vr4BD1V0eh0z-k_CSGC4DKKlnBEEAwcpHRjOZUdW_5UJFe-l77KMObvZUPLckWUaQKybMSBYDGrAeo1TqHHmkumwSG5s"
const userTicketStr = "3:user:CAsQ__________9_GikKAgh7CgMIyAMQyAMaBmJiOmtlaxoLc29tZTpzY29wZXMg0oXYzAQoAA:LPpzn2ILhY1BHXA1a51mtU1emb2QSMH3UhTxsmL07iJ7m2AMc2xloXCKQOI7uK6JuLDf7aSWd9QQJpaRV0mfPzvFTnz2j78hvO3bY8KT_TshA3A-M5-t5gip8CfTVGPmEPwnuUhmKqAGkGSL-sCHyu1RIjHkGbJA250ThHHKgAY"

func TestDebugInfo(t *testing.T) {
	c1000502, err := tvmauth.NewAPIClient(apiSettings(t, 1000502), &nop.Logger{})
	require.NoError(t, err)
	defer c1000502.Destroy()

	ticketS, err := c1000502.CheckServiceTicket(context.Background(), serviceTicketStr)
	require.NoError(t, err)
	require.Equal(t, tvm.ClientID(100500), ticketS.SrcID)
	require.Equal(t, tvm.UID(0), ticketS.IssuerUID)
	require.Equal(t, "ticket_type=serv;expiration_time=9223372036854775807;src=100500;dst=1000502;", ticketS.DbgInfo)
	require.Equal(t, "3:serv:CBAQ__________9_IggIlJEGELaIPQ:", ticketS.LogInfo)

	ticketS, err = c1000502.CheckServiceTicket(context.Background(), serviceTicketStr[:len(serviceTicketStr)-1])
	require.Error(t, err)
	require.IsType(t, err, &tvm.TicketError{})
	require.Equal(t, err.(*tvm.TicketError).Status, tvm.TicketSignBroken)
	require.Equal(t, "ticket_type=serv;expiration_time=9223372036854775807;src=100500;dst=1000502;", ticketS.DbgInfo)
	require.Equal(t, "3:serv:CBAQ__________9_IggIlJEGELaIPQ:", ticketS.LogInfo)

	ticketU, err := c1000502.CheckUserTicket(context.Background(), userTicketStr)
	require.NoError(t, err)
	require.Equal(t, []tvm.UID{123, 456}, ticketU.UIDs)
	require.Equal(t, tvm.UID(456), ticketU.DefaultUID)
	require.Equal(t, []string{"bb:kek", "some:scopes"}, ticketU.Scopes)
	require.Equal(t, "ticket_type=user;expiration_time=9223372036854775807;scope=bb:kek;scope=some:scopes;default_uid=456;uid=123;uid=456;env=Prod;", ticketU.DbgInfo)
	require.Equal(t, "3:user:CAsQ__________9_GikKAgh7CgMIyAMQyAMaBmJiOmtlaxoLc29tZTpzY29wZXMg0oXYzAQoAA:", ticketU.LogInfo)

	_, err = c1000502.CheckUserTicket(context.Background(), userTicketStr, tvm.WithBlackboxOverride(tvm.BlackboxProdYateam))
	require.Error(t, err)
	require.IsType(t, err, &tvm.TicketError{})
	require.Equal(t, err.(*tvm.TicketError).Status, tvm.TicketInvalidBlackboxEnv)

	ticketU, err = c1000502.CheckUserTicket(context.Background(), userTicketStr[:len(userTicketStr)-1])
	require.Error(t, err)
	require.IsType(t, err, &tvm.TicketError{})
	require.Equal(t, err.(*tvm.TicketError).Status, tvm.TicketSignBroken)
	require.Equal(t, "ticket_type=user;expiration_time=9223372036854775807;scope=bb:kek;scope=some:scopes;default_uid=456;uid=123;uid=456;env=Prod;", ticketU.DbgInfo)
	require.Equal(t, "3:user:CAsQ__________9_GikKAgh7CgMIyAMQyAMaBmJiOmtlaxoLc29tZTpzY29wZXMg0oXYzAQoAA:", ticketU.LogInfo)
}

func TestUnittestClient(t *testing.T) {
	_, err := tvmauth.NewUnittestClient(tvmauth.TvmUnittestSettings{})
	require.NoError(t, err)

	client, err := tvmauth.NewUnittestClient(tvmauth.TvmUnittestSettings{
		SelfID: 1000502,
	})
	require.NoError(t, err)

	_, err = client.GetRoles(context.Background())
	require.ErrorContains(t, err, "Roles are not provided")
	_, err = client.GetServiceTicketForID(context.Background(), tvm.ClientID(42))
	require.ErrorContains(t, err, "Destination '42' was not specified in settings")

	status, err := client.GetStatus(context.Background())
	require.NoError(t, err)
	require.EqualValues(t, tvm.ClientOK, status.Status)

	st, err := client.CheckServiceTicket(context.Background(), serviceTicketStr)
	require.NoError(t, err)
	require.EqualValues(t, tvm.ClientID(100500), st.SrcID)

	ut, err := client.CheckUserTicket(context.Background(), userTicketStr)
	require.NoError(t, err)
	require.EqualValues(t, tvm.UID(456), ut.DefaultUID)
}
