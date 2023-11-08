package tvmtool

import (
	"fmt"
	"os"
)

const (
	QloudEndpointEnvKey  = "QLOUD_TVM_INTERFACE_ORIGIN"
	QloudTokenEnvKey     = "QLOUD_TVM_TOKEN"
	QloudDefaultEndpoint = "http://localhost:1"
)

// NewQloudClient method creates a new tvmtool client for Qloud environment.
// You must reuse it to prevent connection/goroutines leakage.
func NewQloudClient(opts ...Option) (*Client, error) {
	baseURI := os.Getenv(QloudEndpointEnvKey)
	if baseURI == "" {
		baseURI = QloudDefaultEndpoint
	}

	authToken := os.Getenv(QloudTokenEnvKey)
	if authToken == "" {
		return nil, fmt.Errorf("empty auth token (looked at ENV[%s])", QloudTokenEnvKey)
	}

	opts = append([]Option{WithAuthToken(authToken)}, opts...)
	return NewClient(
		baseURI,
		opts...,
	)
}
