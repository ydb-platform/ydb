package server

import (
	"context"
	"fmt"
	"net"

	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/spf13/cobra"
	"github.com/ydb-platform/ydb/library/go/core/log"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/config"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/rdbms"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/utils"
	api_service "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/libgo/service"
	api_service_protos "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/libgo/service/protos"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type Server struct {
	api_service.UnimplementedConnectorServer
	handlerFactory        *rdbms.HandlerFactory
	columnarBufferFactory *utils.ColumnarBufferFactory
	cfg                   *config.ServerConfig
	logger                log.Logger
}

func (s *Server) ListTables(_ *api_service_protos.TListTablesRequest, _ api_service.Connector_ListTablesServer) error {
	return nil
}

func (s *Server) DescribeTable(
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

func (s *Server) ListSplits(request *api_service_protos.TListSplitsRequest, stream api_service.Connector_ListSplitsServer) error {
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

func (s *Server) doListSplitsHandleSelect(
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

func (s *Server) doListSplitsResponse(
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

func (s *Server) ReadSplits(request *api_service_protos.TReadSplitsRequest, stream api_service.Connector_ReadSplitsServer) error {
	logger := utils.AnnotateLogger(s.logger, "ReadSplits", request.DataSourceInstance)
	logger.Info("request handling started", log.Int("total_splits", len(request.Splits)))

	if err := ValidateReadSplitsRequest(logger, request); err != nil {
		logger.Error("request handling failed", log.Error(err))

		response := &api_service_protos.TReadSplitsResponse{Error: utils.NewAPIErrorFromStdError(err)}

		if err := stream.Send(response); err != nil {
			logger.Error("send channel failed", log.Error(err))
			return err
		}
	}

	logger = log.With(logger, log.String("data source kind", request.DataSourceInstance.GetKind().String()))

	for i, split := range request.Splits {
		logger.Info("reading split", log.Int("split_ordered_num", i))

		err := s.readSplit(logger, stream, request, split)
		if err != nil {
			logger.Error("request handling failed", log.Error(err))

			response := &api_service_protos.TReadSplitsResponse{Error: utils.NewAPIErrorFromStdError(err)}

			if err := stream.Send(response); err != nil {
				logger.Error("send channel failed", log.Error(err))
				return err
			}
		}
	}

	logger.Info("request handling finished")

	return nil
}

func (s *Server) readSplit(
	logger log.Logger,
	stream api_service.Connector_ReadSplitsServer,
	request *api_service_protos.TReadSplitsRequest,
	split *api_service_protos.TSplit,
) error {
	logger.Debug("reading split", log.String("split", split.String()))

	handler, err := s.handlerFactory.Make(logger, request.DataSourceInstance.Kind)
	if err != nil {
		return fmt.Errorf("get handler: %w", err)
	}

	buf, err := s.columnarBufferFactory.MakeBuffer(logger, request.Format, split.Select.What, handler.TypeMapper())
	if err != nil {
		return fmt.Errorf("make buffer: %w", err)
	}

	pagingWriter, err := utils.NewPagingWriter(
		logger,
		buf,
		stream,
		request.GetPagination(),
	)
	if err != nil {
		return fmt.Errorf("new paging writer result set: %w", err)
	}

	if err = handler.ReadSplit(stream.Context(), logger, request.GetDataSourceInstance(), split, pagingWriter); err != nil {
		return fmt.Errorf("read split: %w", err)
	}

	rowsReceived, err := pagingWriter.Finish()
	if err != nil {
		return fmt.Errorf("finish paging writer: %w", err)
	}

	logger.Debug("reading split finished", log.UInt64("rows_received", rowsReceived))

	return nil
}

func (s *Server) run() error {
	endpoint := utils.EndpointToString(s.cfg.Endpoint)

	lis, err := net.Listen("tcp", endpoint)
	if err != nil {
		return fmt.Errorf("net listen: %w", err)
	}

	options, err := s.makeOptions()
	if err != nil {
		return fmt.Errorf(": %w", err)
	}

	grpcSrv := grpc.NewServer(options...)

	api_service.RegisterConnectorServer(grpcSrv, s)

	s.logger.Info("listener started", log.String("address", lis.Addr().String()))

	if err := grpcSrv.Serve(lis); err != nil {
		return fmt.Errorf("listener serve: %w", err)
	}

	return nil
}

func (s *Server) makeOptions() ([]grpc.ServerOption, error) {
	var opts []grpc.ServerOption

	if s.cfg.Tls != nil {
		s.logger.Info("server will use TLS connections")

		s.logger.Info("reading key pair", log.String("cert", s.cfg.Tls.Cert), log.String("key", s.cfg.Tls.Key))
		creds, err := credentials.NewServerTLSFromFile(s.cfg.Tls.Cert, s.cfg.Tls.Key)
		if err != nil {
			return nil, fmt.Errorf("new server TLS from file: %w", err)
		}

		opts = append(opts, grpc.Creds(creds))
	} else {
		s.logger.Warn("server will use insecure connections")
	}

	return opts, nil
}

func newServer(
	logger log.Logger,
	cfg *config.ServerConfig,
) (*Server, error) {
	return &Server{
		handlerFactory: rdbms.NewHandlerFactory(),
		columnarBufferFactory: utils.NewColumnarBufferFactory(
			memory.DefaultAllocator,
			utils.NewReadLimiterFactory(cfg.ReadLimit),
		),
		logger: logger,
		cfg:    cfg,
	}, nil
}

func runServer(cmd *cobra.Command, _ []string) error {
	logger, err := utils.NewDevelopmentLogger()
	if err != nil {
		return fmt.Errorf("new development logger: %w", err)
	}

	configPath, err := cmd.Flags().GetString(configFlag)
	if err != nil {
		return fmt.Errorf("get config flag: %v", err)
	}

	cfg, err := newConfigFromPath(configPath)
	if err != nil {
		return fmt.Errorf("new config: %w", err)
	}

	srv, err := newServer(logger, cfg)
	if err != nil {
		return fmt.Errorf("new server: %w", err)
	}

	if err := srv.run(); err != nil {
		return fmt.Errorf("server run: %w", err)
	}

	return nil
}
