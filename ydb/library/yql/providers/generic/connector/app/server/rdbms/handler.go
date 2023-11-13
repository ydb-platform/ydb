package rdbms

import (
	"context"
	"fmt"
	"strings"

	"github.com/ydb-platform/ydb/library/go/core/log"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/paging"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/utils"
	api_service_protos "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/libgo/service/protos"
)

type handlerImpl struct {
	typeMapper        utils.TypeMapper
	queryBuilder      utils.QueryExecutor
	connectionManager utils.ConnectionManager
	logger            log.Logger
}

func (h *handlerImpl) DescribeTable(
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

	sb := &schemaBuilder{typeMapper: h.typeMapper, typeMappingSettings: request.TypeMappingSettings}

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

func (h *handlerImpl) makeReadSplitQuery(
	logger log.Logger,
	split *api_service_protos.TSplit,
) (string, error) {
	// SELECT $columns
	// interpolate request
	var sb strings.Builder

	sb.WriteString("SELECT ")

	columns, err := utils.SelectWhatToYDBColumns(split.Select.What)
	if err != nil {
		return "", fmt.Errorf("convert Select.What.Items to Ydb.Columns: %w", err)
	}

	// for the case of empty column set select some constant for constructing a valid sql statement
	if len(columns) == 0 {
		sb.WriteString("0")
	} else {
		for i, column := range columns {
			sb.WriteString(column.GetName())

			if i != len(columns)-1 {
				sb.WriteString(", ")
			}
		}
	}

	// SELECT $columns FROM $from
	tableName := split.GetSelect().GetFrom().GetTable()
	if tableName == "" {
		return "", fmt.Errorf("empty table name")
	}

	sb.WriteString(" FROM ")
	sb.WriteString(tableName)

	if split.Select.Where != nil {
		clause, err := FormatWhereClause(split.Select.Where)
		if err != nil {
			logger.Error("Failed to format WHERE clause", log.Error(err), log.String("where", split.Select.Where.String()))
		} else {
			sb.WriteString(" ")
			sb.WriteString(clause)
		}
	}

	// execute query

	return sb.String(), nil
}

func (h *handlerImpl) doReadSplit(
	ctx context.Context,
	logger log.Logger,
	split *api_service_protos.TSplit,
	sink paging.Sink,
) error {
	query, err := h.makeReadSplitQuery(logger, split)
	if err != nil {
		return fmt.Errorf("make read split query: %w", err)
	}

	conn, err := h.connectionManager.Make(ctx, logger, split.Select.DataSourceInstance)
	if err != nil {
		return fmt.Errorf("make connection: %w", err)
	}

	defer h.connectionManager.Release(logger, conn)

	rows, err := conn.Query(ctx, query)
	if err != nil {
		return fmt.Errorf("query '%s' error: %w", query, err)
	}

	defer func() { utils.LogCloserError(logger, rows, "close rows") }()

	acceptors, err := rows.MakeAcceptors()
	if err != nil {
		return fmt.Errorf("make acceptors: %w", err)
	}

	for rows.Next() {
		if err := rows.Scan(acceptors...); err != nil {
			return fmt.Errorf("rows scan error: %w", err)
		}

		if err := sink.AddRow(acceptors); err != nil {
			return fmt.Errorf("add row to paging writer: %w", err)
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("rows error: %w", err)
	}

	return nil
}

func (h *handlerImpl) ReadSplit(
	ctx context.Context,
	logger log.Logger,
	split *api_service_protos.TSplit,
	sink paging.Sink,
) {
	err := h.doReadSplit(ctx, logger, split, sink)
	if err != nil {
		sink.AddError(err)
	}

	sink.Finish()
}

func (h *handlerImpl) TypeMapper() utils.TypeMapper { return h.typeMapper }

func newHandler(
	logger log.Logger,
	preset *handlerPreset,
) Handler {
	return &handlerImpl{
		logger:            logger,
		queryBuilder:      preset.queryExecutor,
		connectionManager: preset.connectionManager,
		typeMapper:        preset.typeMapper,
	}
}
