package server

import (
	"context"
	"fmt"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/ydb-platform/ydb/library/go/core/log"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/config"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/utils"
)

type servicePprof struct {
	httpServer *http.Server
	mux        *http.ServeMux
	logger     log.Logger
}

func (s *servicePprof) start() error {
	s.logger.Debug("starting HTTP server", log.String("address", s.httpServer.Addr))

	if err := s.httpServer.ListenAndServe(); err != nil {
		return fmt.Errorf("http server listen and server: %w", err)
	}

	return nil
}

const shutdownTimeout = 5 * time.Second

func (s *servicePprof) stop() {
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	err := s.httpServer.Shutdown(ctx)
	if err != nil {
		s.logger.Error("shutdown http server", log.Error(err))
	}
}

func newServicePprof(logger log.Logger, cfg *config.TPprofServerConfig) service {
	httpServer := &http.Server{
		Addr: utils.EndpointToString(cfg.Endpoint),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	// TODO: TLS
	logger.Warn("server will use insecure connections")

	return &servicePprof{
		httpServer: httpServer,
		logger:     logger,
		mux:        mux,
	}
}
