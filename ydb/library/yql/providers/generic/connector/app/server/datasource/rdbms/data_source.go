package rdbms

import (
	"context"
	"fmt"

	"github.com/ydb-platform/ydb/library/go/core/log"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/datasource"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/paging"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/utils"
	api_service_protos "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/libgo/service/protos"
)

type Preset struct {
	SQLFormatter      utils.SQLFormatter
	ConnectionManager utils.ConnectionManager
	TypeMapper        utils.TypeMapper
}

var _ datasource.DataSource = (*dataSourceImpl)(nil)

type dataSourceImpl struct {
	typeMapper        utils.TypeMapper
	sqlFormatter      utils.SQLFormatter
	connectionManager utils.ConnectionManager
	logger            log.Logger
}

func (ds *dataSourceImpl) DescribeTable(
	ctx context.Context,
	logger log.Logger,
	request *api_service_protos.TDescribeTableRequest,
) (*api_service_protos.TDescribeTableResponse, error) {
	query, args := utils.MakeDescribeTableQuery(logger, ds.sqlFormatter, request)

	conn, err := ds.connectionManager.Make(ctx, logger, request.DataSourceInstance)
	if err != nil {
		return nil, fmt.Errorf("make connection: %w", err)
	}

	defer ds.connectionManager.Release(logger, conn)

	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query builder error: %w", err)
	}

	defer func() { utils.LogCloserError(logger, rows, "close rows") }()

	var (
		columnName string
		typeName   string
	)

	sb := &schemaBuilder{typeMapper: ds.typeMapper, typeMappingSettings: request.TypeMappingSettings}

	for rows.Next() {
		if err := rows.Scan(&columnName, &typeName); err != nil {
			return nil, fmt.Errorf("rows scan: %w", err)
		}

		if err := sb.addColumn(columnName, typeName); err != nil {
			return nil, fmt.Errorf("add column to schema builder: %w", err)
		}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration: %w", err)
	}

	schema, err := sb.build(logger)
	if err != nil {
		return nil, fmt.Errorf("build schema: %w", err)
	}

	return &api_service_protos.TDescribeTableResponse{Schema: schema}, nil
}

func (ds *dataSourceImpl) doReadSplit(
	ctx context.Context,
	logger log.Logger,
	split *api_service_protos.TSplit,
	sink paging.Sink,
) error {
	query, args, err := utils.MakeReadSplitQuery(logger, ds.sqlFormatter, split.Select)
	if err != nil {
		return fmt.Errorf("make read split query: %w", err)
	}

	conn, err := ds.connectionManager.Make(ctx, logger, split.Select.DataSourceInstance)
	if err != nil {
		return fmt.Errorf("make connection: %w", err)
	}

	defer ds.connectionManager.Release(logger, conn)

	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("query '%s' error: %w", query, err)
	}

	defer func() { utils.LogCloserError(logger, rows, "close rows") }()

	ydbTypes, err := utils.SelectWhatToYDBTypes(split.Select.What)
	if err != nil {
		return fmt.Errorf("convert Select.What to Ydb types: %w", err)
	}

	transformer, err := rows.MakeTransformer(ydbTypes)
	if err != nil {
		return fmt.Errorf("make transformer: %w", err)
	}

	for rows.Next() {
		if err := rows.Scan(transformer.GetAcceptors()...); err != nil {
			return fmt.Errorf("rows scan error: %w", err)
		}

		if err := sink.AddRow(transformer); err != nil {
			return fmt.Errorf("add row to paging writer: %w", err)
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("rows error: %w", err)
	}

	return nil
}

func (ds *dataSourceImpl) ReadSplit(
	ctx context.Context,
	logger log.Logger,
	split *api_service_protos.TSplit,
	sink paging.Sink,
) {
	err := ds.doReadSplit(ctx, logger, split, sink)
	if err != nil {
		sink.AddError(err)
	}

	sink.Finish()
}

func (ds *dataSourceImpl) TypeMapper() utils.TypeMapper { return ds.typeMapper }

func NewDataSource(
	logger log.Logger,
	preset *Preset,
) datasource.DataSource {
	return &dataSourceImpl{
		logger:            logger,
		sqlFormatter:      preset.SQLFormatter,
		connectionManager: preset.ConnectionManager,
		typeMapper:        preset.TypeMapper,
	}
}
