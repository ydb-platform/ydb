package rdbms

import (
	"context"
	"fmt"
	"strings"

	"github.com/ydb-platform/ydb/library/go/core/log"
	api_common "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/api/common"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/utils"
	api_service_protos "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/libgo/service/protos"
)

type Handler interface {
	DescribeTable(
		ctx context.Context,
		logger log.Logger,
		request *api_service_protos.TDescribeTableRequest,
	) (*api_service_protos.TDescribeTableResponse, error)

	ReadSplit(
		ctx context.Context,
		logger log.Logger,
		dataSourceInstance *api_common.TDataSourceInstance,
		split *api_service_protos.TSplit,
		pagingWriter *utils.PagingWriter,
	) error

	TypeMapper() utils.TypeMapper
}

type handlerImpl[CONN utils.Connection] struct {
	typeMapper        utils.TypeMapper
	queryBuilder      utils.QueryExecutor[CONN]
	connectionManager utils.ConnectionManager[CONN]
	logger            log.Logger
}

func (h *handlerImpl[CONN]) DescribeTable(
	ctx context.Context,
	logger log.Logger,
	request *api_service_protos.TDescribeTableRequest,
) (*api_service_protos.TDescribeTableResponse, error) {
	conn, err := h.connectionManager.Make(ctx, logger, request.DataSourceInstance)
	if err != nil {
		return nil, fmt.Errorf("make connection: %w", err)
	}

	defer h.connectionManager.Release(logger, conn)

	rows, err := h.queryBuilder.DescribeTable(ctx, conn, request)
	if err != nil {
		return nil, fmt.Errorf("query builder error: %w", err)
	}

	defer func() { utils.LogCloserError(logger, rows, "close rows") }()

	var (
		columnName string
		typeName   string
	)

	sb := &schemaBuilder{typeMapper: h.typeMapper}

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

func (h *handlerImpl[CONN]) ReadSplit(
	ctx context.Context,
	logger log.Logger,
	dataSourceInstance *api_common.TDataSourceInstance,
	split *api_service_protos.TSplit,
	pagingWriter *utils.PagingWriter,
) error {
	conn, err := h.connectionManager.Make(ctx, logger, dataSourceInstance)
	if err != nil {
		return fmt.Errorf("make connection: %w", err)
	}

	defer h.connectionManager.Release(logger, conn)

	// SELECT $columns

	// interpolate request
	var sb strings.Builder

	sb.WriteString("SELECT ")

	// accumulate acceptors
	var acceptors []any

	columns, err := utils.SelectWhatToYDBColumns(split.Select.What)
	if err != nil {
		return fmt.Errorf("convert Select.What.Items to Ydb.Columns: %w", err)
	}

	for i, column := range columns {
		sb.WriteString(column.GetName())

		if i != len(columns)-1 {
			sb.WriteString(", ")
		}

		var acceptor any

		acceptor, err = h.typeMapper.YDBTypeToAcceptor(column.GetType())
		if err != nil {
			return fmt.Errorf("map ydb column to acceptor: %w", err)
		}

		acceptors = append(acceptors, acceptor)
	}

	// SELECT $columns FROM $from
	tableName := split.GetSelect().GetFrom().GetTable()
	if tableName == "" {
		return fmt.Errorf("empty table name")
	}

	sb.WriteString(" FROM ")
	sb.WriteString(tableName)

	// execute query

	query := sb.String()

	logger.Debug("execute query", log.String("query", query))

	rows, err := conn.Query(ctx, query)
	if err != nil {
		return fmt.Errorf("query '%s' error: %w", query, err)
	}

	defer func() { utils.LogCloserError(logger, rows, "close rows") }()

	for rows.Next() {
		if err := rows.Scan(acceptors...); err != nil {
			return fmt.Errorf("rows scan error: %w", err)
		}

		if err := pagingWriter.AddRow(acceptors); err != nil {
			return fmt.Errorf("add row to paging writer: %w", err)
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("rows error: %w", err)
	}

	return nil
}

func (h *handlerImpl[CONN]) TypeMapper() utils.TypeMapper { return h.typeMapper }

func newHandler[CONN utils.Connection](
	logger log.Logger,
	preset *preset[CONN],
) Handler {
	return &handlerImpl[CONN]{
		logger:            logger,
		queryBuilder:      preset.queryExecutor,
		connectionManager: preset.connectionManager,
		typeMapper:        preset.typeMapper,
	}
}
