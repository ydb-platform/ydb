package server

import (
	"fmt"
	"math"
	"os"

	api_common "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/api/common"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/config"
	"google.golang.org/protobuf/encoding/prototext"
)

func fillServerConfigDefaults(c *config.TServerConfig) {
	if c.Paging == nil {
		c.Paging = &config.TPagingConfig{
			BytesPerPage:          4 * 1024 * 1024,
			PrefetchQueueCapacity: 2,
		}
	}
}

func validateServerConfig(c *config.TServerConfig) error {
	if err := validateConnectorServerConfig(c.ConnectorServer); err != nil {
		return fmt.Errorf("validate `connector_server`: %w", err)
	}

	if err := validateServerReadLimit(c.ReadLimit); err != nil {
		return fmt.Errorf("validate `read_limit`: %w", err)
	}

	if err := validatePprofServerConfig(c.PprofServer); err != nil {
		return fmt.Errorf("validate `pprof_server`: %w", err)
	}

	if err := validatePagingConfig(c.Paging); err != nil {
		return fmt.Errorf("validate `paging`: %w", err)
	}

	return nil
}

func validateConnectorServerConfig(c *config.TConnectorServerConfig) error {
	if c == nil {
		return fmt.Errorf("required section is missing")
	}

	if err := validateEndpoint(c.Endpoint); err != nil {
		return fmt.Errorf("validate `endpoint`: %w", err)
	}

	if err := validateServerTLSConfig(c.Tls); err != nil {
		return fmt.Errorf("validate `tls`: %w", err)
	}

	return nil
}

func validateEndpoint(c *api_common.TEndpoint) error {
	if c == nil {
		return fmt.Errorf("required section is missing")
	}

	if c.Host == "" {
		return fmt.Errorf("invalid value of field `host`: %v", c.Host)
	}

	if c.Port == 0 || c.Port > math.MaxUint16 {
		return fmt.Errorf("invalid value of field `port`: %v", c.Port)
	}

	return nil
}

func validateServerTLSConfig(c *config.TServerTLSConfig) error {
	if c == nil {
		// It's OK not to have TLS config section
		return nil
	}

	if err := fileMustExist(c.Key); err != nil {
		return fmt.Errorf("invalid value of field `key`: %w", err)
	}

	if err := fileMustExist(c.Cert); err != nil {
		return fmt.Errorf("invalid value of field `cert`: %w", err)
	}

	return nil
}

func validateServerReadLimit(c *config.TServerReadLimit) error {
	if c == nil {
		// It's OK not to have read request memory limitation
		return nil
	}

	// but if it's not nil, one must set limits explicitly
	if c.GetRows() == 0 {
		return fmt.Errorf("invalid value of field `rows`")
	}

	return nil
}

func validatePprofServerConfig(c *config.TPprofServerConfig) error {
	if c == nil {
		// It's OK to disable profiler
		return nil
	}

	if err := validateEndpoint(c.Endpoint); err != nil {
		return fmt.Errorf("validate `endpoint`: %w", err)
	}

	if err := validateServerTLSConfig(c.Tls); err != nil {
		return fmt.Errorf("validate `tls`: %w", err)
	}

	return nil
}

const maxInterconnectMessageSize = 50 * 1024 * 1024

func validatePagingConfig(c *config.TPagingConfig) error {
	if c == nil {
		return fmt.Errorf("required section is missing")
	}

	limitIsSet := c.BytesPerPage != 0 || c.RowsPerPage != 0
	if !limitIsSet {
		return fmt.Errorf("you must set either `bytes_per_page` or `rows_per_page` or both of them")
	}

	if c.BytesPerPage > maxInterconnectMessageSize {
		return fmt.Errorf("`bytes_per_page` limit exceeds the limits of interconnect system used by YDB engine")
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

func newConfigFromPath(configPath string) (*config.TServerConfig, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("read file %v: %w", configPath, err)
	}

	var cfg config.TServerConfig

	unmarshaller := prototext.UnmarshalOptions{
		// Do not emit an error if config contains outdated or too fresh fields
		DiscardUnknown: true,
	}

	if err := unmarshaller.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("prototext unmarshal `%v`: %w", string(data), err)
	}

	fillServerConfigDefaults(&cfg)

	if err := validateServerConfig(&cfg); err != nil {
		return nil, fmt.Errorf("validate config: %w", err)
	}

	return &cfg, nil
}
