package server

import (
	"context"
	"fmt"

	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/ydb-platform/ydb/library/go/core/log"
	api_common "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/api/common"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/config"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/datasource"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/datasource/rdbms"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/datasource/s3"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/paging"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/streaming"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/utils"
	api_service "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/libgo/service"
	api_service_protos "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/libgo/service/protos"
)

type DataSourceCollection struct {
	rdbms              datasource.DataSourceFactory[any]
	memoryAllocator    memory.Allocator
	readLimiterFactory *paging.ReadLimiterFactory
	cfg                *config.TServerConfig
}

func (dsc *DataSourceCollection) DescribeTable(
	ctx context.Context, logger log.Logger, request *api_service_protos.TDescribeTableRequest,
) (*api_service_protos.TDescribeTableResponse, error) {
	kind := request.GetDataSourceInstance().GetKind()

	switch kind {
	case api_common.EDataSourceKind_CLICKHOUSE, api_common.EDataSourceKind_POSTGRESQL:
		ds, err := dsc.rdbms.Make(logger, kind)
		if err != nil {
			return nil, err
		}

		return ds.DescribeTable(ctx, logger, request)
	case api_common.EDataSourceKind_S3:
		ds := s3.NewDataSource()

		return ds.DescribeTable(ctx, logger, request)
	default:
		return nil, fmt.Errorf("unsupported data source type '%v': %w", kind, utils.ErrDataSourceNotSupported)
	}
}

func (dsc *DataSourceCollection) DoReadSplit(
	logger log.Logger,
	stream api_service.Connector_ReadSplitsServer,
	request *api_service_protos.TReadSplitsRequest,
	split *api_service_protos.TSplit,
) error {
	switch kind := request.GetDataSourceInstance().GetKind(); kind {
	case api_common.EDataSourceKind_CLICKHOUSE, api_common.EDataSourceKind_POSTGRESQL:
		ds, err := dsc.rdbms.Make(logger, kind)
		if err != nil {
			return err
		}

		return readSplit[any](logger, stream, request, split, ds, dsc.memoryAllocator, dsc.readLimiterFactory, dsc.cfg)
	case api_common.EDataSourceKind_S3:
		ds := s3.NewDataSource()

		return readSplit[string](logger, stream, request, split, ds, dsc.memoryAllocator, dsc.readLimiterFactory, dsc.cfg)
	default:
		return fmt.Errorf("unsupported data source type '%v': %w", kind, utils.ErrDataSourceNotSupported)
	}
}

func readSplit[T utils.Acceptor](
	logger log.Logger,
	stream api_service.Connector_ReadSplitsServer,
	request *api_service_protos.TReadSplitsRequest,
	split *api_service_protos.TSplit,
	dataSource datasource.DataSource[T],
	memoryAllocator memory.Allocator,
	readLimiterFactory *paging.ReadLimiterFactory,
	cfg *config.TServerConfig,
) error {
	logger.Debug("split reading started", utils.SelectToFields(split.Select)...)

	columnarBufferFactory, err := paging.NewColumnarBufferFactory[T](
		logger,
		memoryAllocator,
		request.Format,
		split.Select.What)
	if err != nil {
		return fmt.Errorf("new columnar buffer factory: %w", err)
	}

	trafficTracker := paging.NewTrafficTracker[T](cfg.Paging)

	sink, err := paging.NewSink(
		stream.Context(),
		logger,
		trafficTracker,
		columnarBufferFactory,
		readLimiterFactory.MakeReadLimiter(logger),
		int(cfg.Paging.PrefetchQueueCapacity),
	)
	if err != nil {
		return fmt.Errorf("new sink: %w", err)
	}

	streamer := streaming.NewStreamer(
		logger,
		stream,
		request,
		split,
		sink,
		dataSource,
	)

	if err := streamer.Run(); err != nil {
		return fmt.Errorf("run paging streamer: %w", err)
	}

	readStats := trafficTracker.DumpStats(true)

	logger.Debug(
		"split reading finished",
		log.UInt64("total_bytes", readStats.GetBytes()),
		log.UInt64("total_rows", readStats.GetRows()),
	)

	return nil
}

func NewDataSourceCollection(
	queryLoggerFactory utils.QueryLoggerFactory,
	memoryAllocator memory.Allocator,
	readLimiterFactory *paging.ReadLimiterFactory,
	cfg *config.TServerConfig,
) *DataSourceCollection {
	return &DataSourceCollection{
		rdbms:              rdbms.NewDataSourceFactory(queryLoggerFactory),
		memoryAllocator:    memoryAllocator,
		readLimiterFactory: readLimiterFactory,
		cfg:                cfg,
	}
}
