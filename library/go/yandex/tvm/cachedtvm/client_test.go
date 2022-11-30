package cachedtvm_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"a.yandex-team.ru/library/go/yandex/tvm"
	"a.yandex-team.ru/library/go/yandex/tvm/cachedtvm"
)

const (
	checkPasses = 5
)

type mockTvmClient struct {
	tvm.Client
	checkServiceTicketCalls int
	checkUserTicketCalls    int
}

func (c *mockTvmClient) CheckServiceTicket(_ context.Context, ticket string) (*tvm.CheckedServiceTicket, error) {
	defer func() { c.checkServiceTicketCalls++ }()

	return &tvm.CheckedServiceTicket{
		LogInfo:   ticket,
		IssuerUID: tvm.UID(c.checkServiceTicketCalls),
	}, nil
}

func (c *mockTvmClient) CheckUserTicket(_ context.Context, ticket string, _ ...tvm.CheckUserTicketOption) (*tvm.CheckedUserTicket, error) {
	defer func() { c.checkUserTicketCalls++ }()

	return &tvm.CheckedUserTicket{
		LogInfo:    ticket,
		DefaultUID: tvm.UID(c.checkUserTicketCalls),
	}, nil
}

func (c *mockTvmClient) GetServiceTicketForAlias(_ context.Context, alias string) (string, error) {
	return alias, nil
}

func checkServiceTickets(t *testing.T, client tvm.Client, equal bool) {
	var prev *tvm.CheckedServiceTicket
	for i := 0; i < checkPasses; i++ {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			cur, err := client.CheckServiceTicket(context.Background(), "3:serv:tst")
			require.NoError(t, err)

			if prev == nil {
				return
			}

			if equal {
				require.Equal(t, *prev, *cur)
			} else {
				require.NotEqual(t, *prev, *cur)
			}
		})
	}
}

func runEqualServiceTickets(client tvm.Client) func(t *testing.T) {
	return func(t *testing.T) {
		checkServiceTickets(t, client, true)
	}
}

func runNotEqualServiceTickets(client tvm.Client) func(t *testing.T) {
	return func(t *testing.T) {
		checkServiceTickets(t, client, false)
	}
}

func checkUserTickets(t *testing.T, client tvm.Client, equal bool) {
	var prev *tvm.CheckedServiceTicket
	for i := 0; i < checkPasses; i++ {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			cur, err := client.CheckUserTicket(context.Background(), "3:user:tst")
			require.NoError(t, err)

			if prev == nil {
				return
			}

			if equal {
				require.Equal(t, *prev, *cur)
			} else {
				require.NotEqual(t, *prev, *cur)
			}
		})
	}
}

func runEqualUserTickets(client tvm.Client) func(t *testing.T) {
	return func(t *testing.T) {
		checkUserTickets(t, client, true)
	}
}

func runNotEqualUserTickets(client tvm.Client) func(t *testing.T) {
	return func(t *testing.T) {
		checkUserTickets(t, client, false)
	}
}
func TestDefaultBehavior(t *testing.T) {
	nestedClient := &mockTvmClient{}
	client, err := cachedtvm.NewClient(nestedClient)
	require.NoError(t, err)

	t.Run("first_pass_srv", runEqualServiceTickets(client))
	t.Run("first_pass_usr", runNotEqualUserTickets(client))

	require.Equal(t, 1, nestedClient.checkServiceTicketCalls)
	require.Equal(t, checkPasses, nestedClient.checkUserTicketCalls)

	ticket, err := client.GetServiceTicketForAlias(context.Background(), "tst")
	require.NoError(t, err)
	require.Equal(t, "tst", ticket)
}

func TestCheckServiceTicket(t *testing.T) {
	nestedClient := &mockTvmClient{}
	client, err := cachedtvm.NewClient(nestedClient, cachedtvm.WithCheckServiceTicket(10*time.Second, 10))
	require.NoError(t, err)

	t.Run("first_pass_srv", runEqualServiceTickets(client))
	t.Run("first_pass_usr", runNotEqualUserTickets(client))
	time.Sleep(20 * time.Second)
	t.Run("second_pass_srv", runEqualServiceTickets(client))
	t.Run("second_pass_usr", runNotEqualUserTickets(client))

	require.Equal(t, 2, nestedClient.checkServiceTicketCalls)
	require.Equal(t, 2*checkPasses, nestedClient.checkUserTicketCalls)

	ticket, err := client.GetServiceTicketForAlias(context.Background(), "tst")
	require.NoError(t, err)
	require.Equal(t, "tst", ticket)
}

func TestCheckUserTicket(t *testing.T) {
	nestedClient := &mockTvmClient{}
	client, err := cachedtvm.NewClient(nestedClient, cachedtvm.WithCheckUserTicket(10*time.Second, 10))
	require.NoError(t, err)

	t.Run("first_pass_usr", runEqualUserTickets(client))
	time.Sleep(20 * time.Second)
	t.Run("second_pass_usr", runEqualUserTickets(client))
	require.Equal(t, 2, nestedClient.checkUserTicketCalls)

	ticket, err := client.GetServiceTicketForAlias(context.Background(), "tst")
	require.NoError(t, err)
	require.Equal(t, "tst", ticket)
}

func TestCheckServiceAndUserTicket(t *testing.T) {
	nestedClient := &mockTvmClient{}
	client, err := cachedtvm.NewClient(nestedClient,
		cachedtvm.WithCheckServiceTicket(10*time.Second, 10),
		cachedtvm.WithCheckUserTicket(10*time.Second, 10),
	)
	require.NoError(t, err)

	t.Run("first_pass_srv", runEqualServiceTickets(client))
	t.Run("first_pass_usr", runEqualUserTickets(client))
	time.Sleep(20 * time.Second)
	t.Run("second_pass_srv", runEqualServiceTickets(client))
	t.Run("second_pass_usr", runEqualUserTickets(client))

	require.Equal(t, 2, nestedClient.checkUserTicketCalls)
	require.Equal(t, 2, nestedClient.checkServiceTicketCalls)

	ticket, err := client.GetServiceTicketForAlias(context.Background(), "tst")
	require.NoError(t, err)
	require.Equal(t, "tst", ticket)
}

func TestErrors(t *testing.T) {
	cases := []cachedtvm.Option{
		cachedtvm.WithCheckServiceTicket(12*time.Hour, 1),
		cachedtvm.WithCheckUserTicket(30*time.Minute, 1),
	}

	nestedClient := &mockTvmClient{}
	for i, tc := range cases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			_, err := cachedtvm.NewClient(nestedClient, tc)
			require.Error(t, err)
		})
	}
}
