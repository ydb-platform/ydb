package server

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"

	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/ydb-platform/ydb/library/go/core/log"
	"github.com/ydb-platform/ydb/library/go/core/metrics/solomon"
	api_common "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/api/common"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/config"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/paging"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/rdbms"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/streaming"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/utils"
	api_service "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/libgo/service"
	api_service_protos "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/libgo/service/protos"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/reflection"
)

type serviceConnector struct {
	api_service.UnimplementedConnectorServer
	handlerFactory     rdbms.HandlerFactory
	memoryAllocator    memory.Allocator
	readLimiterFactory *paging.ReadLimiterFactory
	cfg                *config.TServerConfig
	grpcServer         *grpc.Server
	listener           net.Listener
	logger             log.Logger
}

func (s *serviceConnector) ListTables(_ *api_service_protos.TListTablesRequest, _ api_service.Connector_ListTablesServer) error {
	return nil
}

func (s *serviceConnector) DescribeTable(
	ctx context.Context,
	request *api_service_protos.TDescribeTableRequest,
) (*api_service_protos.TDescribeTableResponse, error) {
	logger := utils.AnnotateLogger(s.logger, "DescribeTable", request.DataSourceInstance)
	logger.Info("request handling started", log.String("table", request.GetTable()))

	if err := ValidateDescribeTableRequest(logger, request); err != nil {
		logger.Error("request handling failed", log.Error(err))

		return &api_service_protos.TDescribeTableResponse{
			Error: utils.NewAPIErrorFromStdError(err),
		}, nil
	}

	handler, err := s.handlerFactory.Make(logger, request.DataSourceInstance.Kind)
	if err != nil {
		logger.Error("request handling failed", log.Error(err))

		return &api_service_protos.TDescribeTableResponse{
			Error: utils.NewAPIErrorFromStdError(err),
		}, nil
	}

	out, err := handler.DescribeTable(ctx, logger, request)
	if err != nil {
		logger.Error("request handling failed", log.Error(err))

		out = &api_service_protos.TDescribeTableResponse{Error: utils.NewAPIErrorFromStdError(err)}

		return out, nil
	}

	out.Error = utils.NewSuccess()
	logger.Info("request handling finished", log.String("response", out.String()))

	return out, nil
}

func (s *serviceConnector) ListSplits(request *api_service_protos.TListSplitsRequest, stream api_service.Connector_ListSplitsServer) error {
	logger := utils.AnnotateLogger(s.logger, "ListSplits", nil)
	logger.Info("request handling started", log.Int("total selects", len(request.Selects)))

	if err := ValidateListSplitsRequest(logger, request); err != nil {
		return s.doListSplitsResponse(logger, stream,
			&api_service_protos.TListSplitsResponse{Error: utils.NewAPIErrorFromStdError(err)})
	}

	// Make a trivial copy of requested selects
	totalSplits := 0

	for _, slct := range request.Selects {
		if err := s.doListSplitsHandleSelect(stream, slct, &totalSplits); err != nil {
			logger.Error("request handling failed", log.Error(err))

			return err
		}
	}

	logger.Info("request handling finished", log.Int("total_splits", totalSplits))

	return nil
}

func (s *serviceConnector) doListSplitsHandleSelect(
	stream api_service.Connector_ListSplitsServer,
	slct *api_service_protos.TSelect,
	totalSplits *int,
) error {
	logger := utils.AnnotateLogger(s.logger, "ListSplits", slct.DataSourceInstance)
	logger.Debug("responding selects", log.Int("split_id", *totalSplits), log.String("select", slct.String()))
	resp := &api_service_protos.TListSplitsResponse{
		Error:  utils.NewSuccess(),
		Splits: []*api_service_protos.TSplit{{Select: slct}},
	}

	for _, split := range resp.Splits {
		logger.Debug("responding split", log.Int("split_id", *totalSplits), log.String("split", split.Select.String()))

		*totalSplits++
	}

	if err := s.doListSplitsResponse(logger, stream, resp); err != nil {
		return err
	}

	return nil
}

func (s *serviceConnector) doListSplitsResponse(
	logger log.Logger,
	stream api_service.Connector_ListSplitsServer,
	response *api_service_protos.TListSplitsResponse,
) error {
	if !utils.IsSuccess(response.Error) {
		logger.Error("request handling failed", utils.APIErrorToLogFields(response.Error)...)
	}

	if err := stream.Send(response); err != nil {
		logger.Error("send channel failed", log.Error(err))

		return err
	}

	return nil
}

func (s *serviceConnector) ReadSplits(
	request *api_service_protos.TReadSplitsRequest,
	stream api_service.Connector_ReadSplitsServer) error {
	logger := utils.AnnotateLogger(s.logger, "ReadSplits", request.DataSourceInstance)
	logger.Info("request handling started", log.Int("total_splits", len(request.Splits)))

	totalBytes, err := s.doReadSplits(logger, request, stream)
	if err != nil {
		logger.Error("request handling failed", log.Error(err))

		response := &api_service_protos.TReadSplitsResponse{Error: utils.NewAPIErrorFromStdError(err)}

		if err := stream.Send(response); err != nil {
			return fmt.Errorf("stream send: %w", err)
		}
	}

	logger.Info("request handling finished", log.UInt64("total_bytes", totalBytes))

	return nil
}

func (s *serviceConnector) doReadSplits(
	logger log.Logger,
	request *api_service_protos.TReadSplitsRequest,
	stream api_service.Connector_ReadSplitsServer,
) (uint64, error) {
	if err := ValidateReadSplitsRequest(logger, request); err != nil {
		return 0, fmt.Errorf("validate read splits request: %w", err)
	}

	var totalBytes uint64

	handler, err := s.handlerFactory.Make(logger, request.DataSourceInstance.Kind)
	if err != nil {
		return 0, fmt.Errorf("make handler: %w", err)
	}

	for i, split := range request.Splits {
		columnarBufferFactory, err := paging.NewColumnarBufferFactory(
			logger,
			s.memoryAllocator,
			s.readLimiterFactory,
			request.Format,
			split.Select.What,
			handler.TypeMapper())
		if err != nil {
			return 0, fmt.Errorf("new columnar buffer factory: %w", err)
		}

		// TODO: use configs
		const (
			resultQueueCapacity = 10
			rowsPerBuffer       = 10000
		)

		sinkFactory := paging.NewSinkFactory(columnarBufferFactory, resultQueueCapacity, rowsPerBuffer)

		sink, err := sinkFactory.MakeSink(stream.Context(), logger, request.Pagination)
		if err != nil {
			return 0, fmt.Errorf("new sink: %w", err)
		}

		bytesInSplit, err := s.readSplit(logger, stream, request, split, sink, handler)
		if err != nil {
			return 0, fmt.Errorf("read split %d: %w", i, err)
		}

		totalBytes += bytesInSplit
	}

	return totalBytes, nil
}

func (s *serviceConnector) readSplit(
	logger log.Logger,
	stream api_service.Connector_ReadSplitsServer,
	request *api_service_protos.TReadSplitsRequest,
	split *api_service_protos.TSplit,
	sink paging.Sink,
	handler rdbms.Handler,
) (uint64, error) {
	logger.Debug("split reading started")

	streamer := streaming.NewStreamer(
		logger,
		stream,
		request,
		split,
		sink,
		handler,
	)

	totalBytesSent, err := streamer.Run()
	if err != nil {
		return 0, fmt.Errorf("run paging streamer: %w", err)
	}

	logger.Debug("split reading finished", log.UInt64("total_bytes_sent", totalBytesSent))

	return totalBytesSent, nil
}

func (s *serviceConnector) start() error {
	s.logger.Debug("starting GRPC server", log.String("address", s.listener.Addr().String()))

	if err := s.grpcServer.Serve(s.listener); err != nil {
		return fmt.Errorf("listener serve: %w", err)
	}

	return nil
}

func makeGRPCOptions(logger log.Logger, cfg *config.TServerConfig, registry *solomon.Registry) ([]grpc.ServerOption, error) {
	var (
		opts      []grpc.ServerOption
		tlsConfig *config.TServerTLSConfig
	)

	// TODO: drop deprecated fields after YQ-2057
	switch {
	case cfg.GetConnectorServer().GetTls() != nil:
		tlsConfig = cfg.GetConnectorServer().GetTls()
	case cfg.GetTls() != nil:
		tlsConfig = cfg.GetTls()
	default:
		logger.Warn("server will use insecure connections")

		return opts, nil
	}

	logger.Info("server will use TLS connections")

	logger.Debug("reading key pair", log.String("cert", tlsConfig.Cert), log.String("key", tlsConfig.Key))

	cert, err := tls.LoadX509KeyPair(tlsConfig.Cert, tlsConfig.Key)
	if err != nil {
		return nil, fmt.Errorf("LoadX509KeyPair: %w", err)
	}

	// for security reasons we do not allow TLS < 1.2, see YQ-1877
	creds := credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})

	unaryInterceptors := []grpc.UnaryServerInterceptor{UnaryServerMetrics(registry)}

	streamInterceptors := []grpc.StreamServerInterceptor{StreamServerMetrics(registry)}

	opts = append(opts, grpc.Creds(creds), grpc.ChainUnaryInterceptor(unaryInterceptors...), grpc.ChainStreamInterceptor(streamInterceptors...))

	return opts, nil
}

func (s *serviceConnector) stop() {
	s.grpcServer.GracefulStop()
}

func newServiceConnector(
	logger log.Logger,
	cfg *config.TServerConfig,
	registry *solomon.Registry,
) (service, error) {
	queryLoggerFactory := utils.NewQueryLoggerFactory(cfg.Logger)

	// TODO: drop deprecated fields after YQ-2057
	var endpoint *api_common.TEndpoint

	switch {
	case cfg.GetConnectorServer().GetEndpoint() != nil:
		endpoint = cfg.ConnectorServer.GetEndpoint()
	case cfg.GetEndpoint() != nil:
		logger.Warn("Using deprecated field `endpoint` from config. Please update your config.")

		endpoint = cfg.GetEndpoint()
	default:
		return nil, fmt.Errorf("invalid config: no endpoint")
	}

	listener, err := net.Listen("tcp", utils.EndpointToString(endpoint))
	if err != nil {
		return nil, fmt.Errorf("net listen: %w", err)
	}

	options, err := makeGRPCOptions(logger, cfg, registry)
	if err != nil {
		return nil, fmt.Errorf("make GRPC options: %w", err)
	}

	grpcServer := grpc.NewServer(options...)
	reflection.Register(grpcServer)

	s := &serviceConnector{
		handlerFactory:     rdbms.NewHandlerFactory(queryLoggerFactory),
		memoryAllocator:    memory.DefaultAllocator,
		readLimiterFactory: paging.NewReadLimiterFactory(cfg.ReadLimit),
		logger:             logger,
		grpcServer:         grpcServer,
		listener:           listener,
		cfg:                cfg,
	}

	api_service.RegisterConnectorServer(grpcServer, s)

	return s, nil
}
