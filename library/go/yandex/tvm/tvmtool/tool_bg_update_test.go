package tvmtool_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"a.yandex-team.ru/library/go/core/log"
	"a.yandex-team.ru/library/go/core/log/zap"
	"a.yandex-team.ru/library/go/yandex/tvm"
	"a.yandex-team.ru/library/go/yandex/tvm/tvmtool"
)

func newMockClient(upstream string, options ...tvmtool.Option) (*tvmtool.Client, error) {
	zlog, _ := zap.New(zap.ConsoleConfig(log.DebugLevel))
	options = append(options, tvmtool.WithLogger(zlog), tvmtool.WithAuthToken("token"))
	return tvmtool.NewClient(upstream, options...)
}

// TestClientBackgroundUpdate_Updatable checks that TVMTool client updates tickets state
func TestClientBackgroundUpdate_Updatable(t *testing.T) {
	type TestCase struct {
		client func(ctx context.Context, t *testing.T, url string) *tvmtool.Client
	}
	cases := map[string]TestCase{
		"async": {
			client: func(ctx context.Context, t *testing.T, url string) *tvmtool.Client {
				tvmClient, err := newMockClient(url, tvmtool.WithRefreshFrequency(500*time.Millisecond))
				require.NoError(t, err)
				return tvmClient
			},
		},
		"background": {
			client: func(ctx context.Context, t *testing.T, url string) *tvmtool.Client {
				tvmClient, err := newMockClient(
					url,
					tvmtool.WithRefreshFrequency(1*time.Second),
					tvmtool.WithBackgroundUpdate(ctx),
				)
				require.NoError(t, err)
				return tvmClient
			},
		},
	}

	tester := func(name string, tc TestCase) {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			var (
				testDstAlias    = "test"
				testDstID       = tvm.ClientID(2002456)
				testTicket      = atomic.NewString("3:serv:original-test-ticket:signature")
				testFooDstAlias = "test_foo"
				testFooDstID    = tvm.ClientID(2002457)
				testFooTicket   = atomic.NewString("3:serv:original-test-foo-ticket:signature")
			)

			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, "/tvm/tickets", r.URL.Path)
				assert.Equal(t, "token", r.Header.Get("Authorization"))
				switch r.URL.RawQuery {
				case "dsts=test", "dsts=test_foo", "dsts=test%2Ctest_foo", "dsts=test_foo%2Ctest":
					// ok
				case "dsts=2002456", "dsts=2002457", "dsts=2002456%2C2002457", "dsts=2002457%2C2002456":
					// ok
				default:
					t.Errorf("unknown tvm-request query: %q", r.URL.RawQuery)
				}

				w.Header().Set("Content-Type", "application/json")
				rsp := map[string]struct {
					Ticket string       `json:"ticket"`
					TVMID  tvm.ClientID `json:"tvm_id"`
				}{
					testDstAlias: {
						Ticket: testTicket.Load(),
						TVMID:  testDstID,
					},
					testFooDstAlias: {
						Ticket: testFooTicket.Load(),
						TVMID:  testFooDstID,
					},
				}

				err := json.NewEncoder(w).Encode(rsp)
				assert.NoError(t, err)
			}))
			defer srv.Close()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			tvmClient := tc.client(ctx, t, srv.URL)

			requestTickets := func(mustEquals bool) {
				ticket, err := tvmClient.GetServiceTicketForAlias(context.Background(), testDstAlias)
				require.NoError(t, err)
				if mustEquals {
					require.Equal(t, testTicket.Load(), ticket)
				}

				ticket, err = tvmClient.GetServiceTicketForID(context.Background(), testDstID)
				require.NoError(t, err)
				if mustEquals {
					require.Equal(t, testTicket.Load(), ticket)
				}

				ticket, err = tvmClient.GetServiceTicketForAlias(context.Background(), testFooDstAlias)
				require.NoError(t, err)
				if mustEquals {
					require.Equal(t, testFooTicket.Load(), ticket)
				}

				ticket, err = tvmClient.GetServiceTicketForID(context.Background(), testFooDstID)
				require.NoError(t, err)
				if mustEquals {
					require.Equal(t, testFooTicket.Load(), ticket)
				}
			}

			// populate tickets cache
			requestTickets(true)

			// now change tickets
			newTicket := "3:serv:changed-test-ticket:signature"
			testTicket.Store(newTicket)
			testFooTicket.Store("3:serv:changed-test-foo-ticket:signature")

			// wait some time
			time.Sleep(2 * time.Second)

			// request new tickets
			requestTickets(false)

			// and wait updates some time
			for idx := 0; idx < 250; idx++ {
				time.Sleep(100 * time.Millisecond)
				ticket, _ := tvmClient.GetServiceTicketForAlias(context.Background(), testDstAlias)
				if ticket == newTicket {
					break
				}
			}

			// now out tvmclient MUST returns new tickets
			requestTickets(true)
		})
	}

	for name, tc := range cases {
		tester(name, tc)
	}
}

// TestClientBackgroundUpdate_NotTooOften checks that TVMTool client request tvmtool not too often
func TestClientBackgroundUpdate_NotTooOften(t *testing.T) {
	type TestCase struct {
		client func(ctx context.Context, t *testing.T, url string) *tvmtool.Client
	}
	cases := map[string]TestCase{
		"async": {
			client: func(ctx context.Context, t *testing.T, url string) *tvmtool.Client {
				tvmClient, err := newMockClient(url, tvmtool.WithRefreshFrequency(20*time.Second))
				require.NoError(t, err)
				return tvmClient
			},
		},
		"background": {
			client: func(ctx context.Context, t *testing.T, url string) *tvmtool.Client {
				tvmClient, err := newMockClient(
					url,
					tvmtool.WithRefreshFrequency(20*time.Second),
					tvmtool.WithBackgroundUpdate(ctx),
				)
				require.NoError(t, err)
				return tvmClient
			},
		},
	}

	tester := func(name string, tc TestCase) {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			var (
				reqCount        = atomic.NewUint32(0)
				testDstAlias    = "test"
				testDstID       = tvm.ClientID(2002456)
				testTicket      = "3:serv:original-test-ticket:signature"
				testFooDstAlias = "test_foo"
				testFooDstID    = tvm.ClientID(2002457)
				testFooTicket   = "3:serv:original-test-foo-ticket:signature"
			)

			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				reqCount.Add(1)
				assert.Equal(t, "/tvm/tickets", r.URL.Path)
				assert.Equal(t, "token", r.Header.Get("Authorization"))
				switch r.URL.RawQuery {
				case "dsts=test", "dsts=test_foo", "dsts=test%2Ctest_foo", "dsts=test_foo%2Ctest":
					// ok
				case "dsts=2002456", "dsts=2002457", "dsts=2002456%2C2002457", "dsts=2002457%2C2002456":
					// ok
				default:
					t.Errorf("unknown tvm-request query: %q", r.URL.RawQuery)
				}

				w.Header().Set("Content-Type", "application/json")
				rsp := map[string]struct {
					Ticket string       `json:"ticket"`
					TVMID  tvm.ClientID `json:"tvm_id"`
				}{
					testDstAlias: {
						Ticket: testTicket,
						TVMID:  testDstID,
					},
					testFooDstAlias: {
						Ticket: testFooTicket,
						TVMID:  testFooDstID,
					},
				}

				err := json.NewEncoder(w).Encode(rsp)
				assert.NoError(t, err)
			}))
			defer srv.Close()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			tvmClient := tc.client(ctx, t, srv.URL)

			requestTickets := func() {
				ticket, err := tvmClient.GetServiceTicketForAlias(context.Background(), testDstAlias)
				require.NoError(t, err)
				require.Equal(t, testTicket, ticket)

				ticket, err = tvmClient.GetServiceTicketForID(context.Background(), testDstID)
				require.NoError(t, err)
				require.Equal(t, testTicket, ticket)

				ticket, err = tvmClient.GetServiceTicketForAlias(context.Background(), testFooDstAlias)
				require.NoError(t, err)
				require.Equal(t, testFooTicket, ticket)

				ticket, err = tvmClient.GetServiceTicketForID(context.Background(), testFooDstID)
				require.NoError(t, err)
				require.Equal(t, testFooTicket, ticket)
			}

			// populate cache
			requestTickets()

			// requests tickets some time that lower than refresh frequency
			for i := 0; i < 10; i++ {
				requestTickets()
				time.Sleep(200 * time.Millisecond)
			}

			require.Equal(t, uint32(2), reqCount.Load(), "tvmtool client calls tvmtool too many times")
		})
	}

	for name, tc := range cases {
		tester(name, tc)
	}
}

func TestClient_RefreshFrequency(t *testing.T) {
	cases := map[string]struct {
		freq time.Duration
		err  bool
	}{
		"too_high": {
			freq: 20 * time.Minute,
			err:  true,
		},
		"ok": {
			freq: 2 * time.Minute,
			err:  false,
		},
	}

	for name, cs := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := tvmtool.NewClient("fake", tvmtool.WithRefreshFrequency(cs.freq))
			if cs.err {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestClient_MultipleAliases(t *testing.T) {
	reqCount := atomic.NewUint32(0)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqCount.Add(1)

		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
"test": {"ticket": "3:serv:CNVRELOq1O0FIggIwON6EJiceg:signature","tvm_id": 2002456},
"test_alias": {"ticket": "3:serv:CNVRELOq1O0FIggIwON6EJiceg:signature","tvm_id": 2002456}
}`))
	}))
	defer srv.Close()

	bgCtx, bgCancel := context.WithCancel(context.Background())
	defer bgCancel()

	tvmClient, err := newMockClient(
		srv.URL,
		tvmtool.WithRefreshFrequency(2*time.Second),
		tvmtool.WithBackgroundUpdate(bgCtx),
	)
	require.NoError(t, err)

	requestTickets := func(t *testing.T) {
		ticket, err := tvmClient.GetServiceTicketForAlias(context.Background(), "test")
		require.NoError(t, err)
		require.Equal(t, "3:serv:CNVRELOq1O0FIggIwON6EJiceg:signature", ticket)

		ticket, err = tvmClient.GetServiceTicketForAlias(context.Background(), "test_alias")
		require.NoError(t, err)
		require.Equal(t, "3:serv:CNVRELOq1O0FIggIwON6EJiceg:signature", ticket)

		ticket, err = tvmClient.GetServiceTicketForID(context.Background(), tvm.ClientID(2002456))
		require.NoError(t, err)
		require.Equal(t, "3:serv:CNVRELOq1O0FIggIwON6EJiceg:signature", ticket)
	}

	t.Run("first", requestTickets)

	t.Run("check_requests", func(t *testing.T) {
		// reqCount must be 2 - one for each aliases
		require.Equal(t, uint32(2), reqCount.Load())
	})

	// now wait GC
	reqCount.Store(0)
	time.Sleep(3 * time.Second)

	t.Run("after_gc", requestTickets)
	t.Run("check_requests", func(t *testing.T) {
		// reqCount must be 1
		require.Equal(t, uint32(1), reqCount.Load())
	})
}
