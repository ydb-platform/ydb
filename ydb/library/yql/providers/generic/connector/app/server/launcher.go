package server

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/ydb-platform/ydb/library/go/core/log"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/config"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/utils"
)

type service interface {
	start() error
	stop()
}

type launcher struct {
	services map[string]service
	logger   log.Logger
}

func (l *launcher) start() <-chan error {
	errChan := make(chan error, len(l.services))

	for key := range l.services {
		key := key
		go func(key string) {
			l.logger.Info("starting service", log.String("service", key))

			// blocking call
			errChan <- l.services[key].start()
		}(key)
	}

	return errChan
}

func (l *launcher) stop() {
	// TODO: make it concurrent
	for key, s := range l.services {
		l.logger.Info("stopping service", log.String("service", key))
		s.stop()
	}
}

const (
	connectorServiceKey = "connector"
	pprofServiceKey     = "pprof"
)

func newLauncher(logger log.Logger, cfg *config.TServerConfig) (*launcher, error) {
	l := &launcher{
		services: make(map[string]service, 2),
		logger:   logger,
	}

	var err error

	// init GRPC server
	l.services[connectorServiceKey], err = newServiceConnector(
		log.With(logger, log.String("service", connectorServiceKey)),
		cfg)
	if err != nil {
		return nil, fmt.Errorf("new connector server: %w", err)
	}

	// init Pprof server
	if cfg.PprofServer != nil {
		l.services[pprofServiceKey] = newServicePprof(
			log.With(logger, log.String("service", pprofServiceKey)),
			cfg.PprofServer)
	}

	return l, nil
}

func run(cmd *cobra.Command, _ []string) error {
	configPath, err := cmd.Flags().GetString(configFlag)
	if err != nil {
		return fmt.Errorf("get config flag: %v", err)
	}

	cfg, err := newConfigFromPath(configPath)
	if err != nil {
		return fmt.Errorf("new config: %w", err)
	}

	logger, err := utils.NewLoggerFromConfig(cfg.Logger)
	if err != nil {
		return fmt.Errorf("new logger from config: %w", err)
	}

	l, err := newLauncher(logger, cfg)
	if err != nil {
		return fmt.Errorf("new launcher: %w", err)
	}

	errChan := l.start()

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)

	select {
	case err := <-errChan:
		logger.Error("service fatal error", log.Error(err))
	case sig := <-signalChan:
		logger.Info("interrupting signal", log.Any("value", sig))
		l.stop()
	}

	return nil
}
