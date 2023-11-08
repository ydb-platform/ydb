package tvmtool

import (
	"fmt"
	"os"
)

const (
	DeployEndpointEnvKey = "DEPLOY_TVM_TOOL_URL"
	DeployTokenEnvKey    = "TVMTOOL_LOCAL_AUTHTOKEN"
)

// NewDeployClient method creates a new tvmtool client for Deploy environment.
// You must reuse it to prevent connection/goroutines leakage.
func NewDeployClient(opts ...Option) (*Client, error) {
	baseURI := os.Getenv(DeployEndpointEnvKey)
	if baseURI == "" {
		return nil, fmt.Errorf("empty tvmtool url (looked at ENV[%s])", DeployEndpointEnvKey)
	}

	authToken := os.Getenv(DeployTokenEnvKey)
	if authToken == "" {
		return nil, fmt.Errorf("empty auth token (looked at ENV[%s])", DeployTokenEnvKey)
	}

	opts = append([]Option{WithAuthToken(authToken)}, opts...)
	return NewClient(
		baseURI,
		opts...,
	)
}
