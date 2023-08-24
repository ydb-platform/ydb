package server

import (
	"fmt"
	"io/ioutil"
	"math"
	"os"

	api_common "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/api/common"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/config"
	"google.golang.org/protobuf/encoding/prototext"
)

func validateServerConfig(c *config.ServerConfig) error {
	if err := validateEndpoint(c.Endpoint); err != nil {
		return fmt.Errorf("validate `Server`: %w", err)
	}

	if err := validateServerTLSConfig(c.Tls); err != nil {
		return fmt.Errorf("validate `TLS`: %w", err)
	}

	if err := validateServerReadLimit(c.ReadLimit); err != nil {
		return fmt.Errorf("validate `ReadLimit`: %w", err)
	}

	return nil
}

func validateEndpoint(c *api_common.TEndpoint) error {
	if c == nil {
		return fmt.Errorf("missing required field `Server`")
	}

	if c.Host == "" {
		return fmt.Errorf("invalid value of field `Server.Host`: %v", c.Host)
	}

	if c.Port == 0 || c.Port > math.MaxUint16 {
		return fmt.Errorf("invalid value of field `Server.Port`: %v", c.Port)
	}

	return nil
}

func validateServerTLSConfig(c *config.ServerTLSConfig) error {
	if c == nil {
		// It's OK not to have TLS config section
		return nil
	}

	if err := fileMustExist(c.Key); err != nil {
		return fmt.Errorf("invalid value of field `TLS.Key`: %w", err)
	}

	if err := fileMustExist(c.Cert); err != nil {
		return fmt.Errorf("invalid value of field `TLS.Cert`: %w", err)
	}

	return nil
}

func validateServerReadLimit(c *config.ServerReadLimit) error {
	if c == nil {
		// It's OK not to have read request memory limitation
		return nil
	}

	// but if it's not nil, one must set limits explicitly
	if c.GetRows() == 0 {
		return fmt.Errorf("invalid value of field `ServerReadLimit.Rows`")
	}

	return nil
}

func fileMustExist(path string) error {
	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		return fmt.Errorf("path '%s' does not exist", path)
	}

	if info.IsDir() {
		return fmt.Errorf("path '%s' is a directory", path)
	}

	return nil
}

func newConfigFromPath(configPath string) (*config.ServerConfig, error) {
	data, err := ioutil.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("read file %v: %w", configPath, err)
	}

	var cfg config.ServerConfig

	if err := prototext.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("prototext unmarshal `%v`: %w", string(data), err)
	}

	if err := validateServerConfig(&cfg); err != nil {
		return nil, fmt.Errorf("validate config: %w", err)
	}

	return &cfg, nil
}
