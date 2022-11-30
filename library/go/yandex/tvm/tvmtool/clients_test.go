//go:build linux || darwin
// +build linux darwin

package tvmtool_test

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"a.yandex-team.ru/library/go/yandex/tvm/tvmtool"
)

func TestNewClients(t *testing.T) {
	type TestCase struct {
		env               map[string]string
		willFail          bool
		expectedErr       string
		expectedBaseURI   string
		expectedAuthToken string
	}

	cases := map[string]struct {
		constructor func(opts ...tvmtool.Option) (*tvmtool.Client, error)
		cases       map[string]TestCase
	}{
		"qloud": {
			constructor: tvmtool.NewQloudClient,
			cases: map[string]TestCase{
				"no-auth": {
					willFail:    true,
					expectedErr: "empty auth token (looked at ENV[QLOUD_TVM_TOKEN])",
				},
				"ok-default-origin": {
					env: map[string]string{
						"QLOUD_TVM_TOKEN": "ok-default-origin-token",
					},
					willFail:          false,
					expectedBaseURI:   "http://localhost:1/tvm",
					expectedAuthToken: "ok-default-origin-token",
				},
				"ok-custom-origin": {
					env: map[string]string{
						"QLOUD_TVM_INTERFACE_ORIGIN": "http://localhost:9000",
						"QLOUD_TVM_TOKEN":            "ok-custom-origin-token",
					},
					willFail:          false,
					expectedBaseURI:   "http://localhost:9000/tvm",
					expectedAuthToken: "ok-custom-origin-token",
				},
			},
		},
		"deploy": {
			constructor: tvmtool.NewDeployClient,
			cases: map[string]TestCase{
				"no-url": {
					willFail:    true,
					expectedErr: "empty tvmtool url (looked at ENV[DEPLOY_TVM_TOOL_URL])",
				},
				"no-auth": {
					env: map[string]string{
						"DEPLOY_TVM_TOOL_URL": "http://localhost:2",
					},
					willFail:    true,
					expectedErr: "empty auth token (looked at ENV[TVMTOOL_LOCAL_AUTHTOKEN])",
				},
				"ok": {
					env: map[string]string{
						"DEPLOY_TVM_TOOL_URL":     "http://localhost:1337",
						"TVMTOOL_LOCAL_AUTHTOKEN": "ok-token",
					},
					willFail:          false,
					expectedBaseURI:   "http://localhost:1337/tvm",
					expectedAuthToken: "ok-token",
				},
			},
		},
		"any": {
			constructor: tvmtool.NewAnyClient,
			cases: map[string]TestCase{
				"empty": {
					willFail:    true,
					expectedErr: "unknown tvmtool environment",
				},
				"ok-qloud": {
					env: map[string]string{
						"QLOUD_TVM_INTERFACE_ORIGIN": "http://qloud:9000",
						"QLOUD_TVM_TOKEN":            "ok-qloud",
					},
					expectedBaseURI:   "http://qloud:9000/tvm",
					expectedAuthToken: "ok-qloud",
				},
				"ok-deploy": {
					env: map[string]string{
						"DEPLOY_TVM_TOOL_URL":     "http://deploy:1337",
						"TVMTOOL_LOCAL_AUTHTOKEN": "ok-deploy",
					},
					expectedBaseURI:   "http://deploy:1337/tvm",
					expectedAuthToken: "ok-deploy",
				},
				"ok-local": {
					env: map[string]string{
						"TVMTOOL_URL":             "http://local:1338",
						"TVMTOOL_LOCAL_AUTHTOKEN": "ok-local",
					},
					willFail:          false,
					expectedBaseURI:   "http://local:1338/tvm",
					expectedAuthToken: "ok-local",
				},
			},
		},
	}

	// NB! this checks are not thread safe, never use t.Parallel() and so on
	for clientName, client := range cases {
		t.Run(clientName, func(t *testing.T) {
			for name, tc := range client.cases {
				t.Run(name, func(t *testing.T) {
					savedEnv := os.Environ()
					defer func() {
						os.Clearenv()
						for _, env := range savedEnv {
							parts := strings.SplitN(env, "=", 2)
							err := os.Setenv(parts[0], parts[1])
							require.NoError(t, err)
						}
					}()

					os.Clearenv()
					for key, val := range tc.env {
						_ = os.Setenv(key, val)
					}

					tvmClient, err := client.constructor()
					if tc.willFail {
						require.Error(t, err)
						if tc.expectedErr != "" {
							require.EqualError(t, err, tc.expectedErr)
						}

						require.Nil(t, tvmClient)
					} else {
						require.NoError(t, err)
						require.NotNil(t, tvmClient)
						require.Equal(t, tc.expectedBaseURI, tvmClient.BaseURI())
						require.Equal(t, tc.expectedAuthToken, tvmClient.AuthToken())
					}
				})
			}
		})
	}
}
