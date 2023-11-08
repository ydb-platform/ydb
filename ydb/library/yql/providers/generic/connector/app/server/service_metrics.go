package server

import (
	"context"
	"fmt"
	"net/http"

	"github.com/ydb-platform/ydb/library/go/core/log"
	"github.com/ydb-platform/ydb/library/go/core/metrics/solomon"
	"github.com/ydb-platform/ydb/library/go/yandex/solomon/reporters/puller/httppuller"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/config"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/utils"
)

type serviceMetrics struct {
	httpServer *http.Server
	logger     log.Logger
	registry   *solomon.Registry
}

func (s *serviceMetrics) start() error {
	s.logger.Debug("starting HTTP metrics server", log.String("address", s.httpServer.Addr))

	if err := s.httpServer.ListenAndServe(); err != nil {
		return fmt.Errorf("http metrics server listen and serve: %w", err)
	}

	return nil
}

func (s *serviceMetrics) stop() {
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	err := s.httpServer.Shutdown(ctx)
	if err != nil {
		s.logger.Error("shutdown http metrics server", log.Error(err))
	}
}

func newServiceMetrics(logger log.Logger, cfg *config.TMetricsServerConfig, registry *solomon.Registry) service {
	mux := http.NewServeMux()
	mux.Handle("/metrics", httppuller.NewHandler(registry))

	httpServer := &http.Server{
		Addr:    utils.EndpointToString(cfg.Endpoint),
		Handler: mux,
	}

	// TODO: TLS
	logger.Warn("metrics server will use insecure connections")

	return &serviceMetrics{
		httpServer: httpServer,
		logger:     logger,
		registry:   registry,
	}
}
