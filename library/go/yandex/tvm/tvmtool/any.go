package tvmtool

import (
	"os"

	"a.yandex-team.ru/library/go/core/xerrors"
)

const (
	LocalEndpointEnvKey = "TVMTOOL_URL"
	LocalTokenEnvKey    = "TVMTOOL_LOCAL_AUTHTOKEN"
)

var ErrUnknownTvmtoolEnvironment = xerrors.NewSentinel("unknown tvmtool environment")

// NewAnyClient method creates a new tvmtool client with environment auto-detection.
// You must reuse it to prevent connection/goroutines leakage.
func NewAnyClient(opts ...Option) (*Client, error) {
	switch {
	case os.Getenv(QloudEndpointEnvKey) != "":
		// it's Qloud
		return NewQloudClient(opts...)
	case os.Getenv(DeployEndpointEnvKey) != "":
		// it's Y.Deploy
		return NewDeployClient(opts...)
	case os.Getenv(LocalEndpointEnvKey) != "":
		passedOpts := append(
			[]Option{
				WithAuthToken(os.Getenv(LocalTokenEnvKey)),
			},
			opts...,
		)
		return NewClient(os.Getenv(LocalEndpointEnvKey), passedOpts...)
	default:
		return nil, ErrUnknownTvmtoolEnvironment.WithFrame()
	}
}
