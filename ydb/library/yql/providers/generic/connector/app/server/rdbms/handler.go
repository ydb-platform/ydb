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

type Handler struct {
	typeMapper        utils.TypeMapper
	queryBuilder      QueryBuilder
	connectionManager ConnectionManager
	logger            log.Logger
}

func (h *Handler) DescribeTable(
	ctx context.Context,
	logger log.Logger,
	request *api_service_protos.TDescribeTableRequest,
) (*api_service_protos.TDescribeTableResponse, error) {
	query := h.queryBuilder.DescribeTable(request)

	conn, err := h.connectionManager.Make(ctx, logger, request.DataSourceInstance)
	if err != nil {
		return nil, fmt.Errorf("make connection: %w", err)
	}

	defer h.connectionManager.Release(logger, conn)

	logger.Debug("execute query", log.String("query", query))

	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query '%s' error: %w", query, err)
	}

	defer func() { utils.LogCloserError(logger, rows, "close rows") }()

	var (
		columnName string
		typeName   string
		schema     api_service_protos.TSchema
	)

	for rows.Next() {
		if err := rows.Scan(&columnName, &typeName); err != nil {
			return nil, fmt.Errorf("rows scan: %w", err)
		}

		column, err := h.typeMapper.SQLTypeToYDBColumn(columnName, typeName)
		if err != nil {
			return nil, fmt.Errorf("sql type to ydb column (%s, %s): %w", columnName, typeName, err)
		}

		schema.Columns = append(schema.Columns, column)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}

	if len(schema.Columns) == 0 {
		return nil, utils.ErrTableDoesNotExist
	}

	return &api_service_protos.TDescribeTableResponse{Schema: &schema}, nil
}

func (h *Handler) ReadSplit(
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

func (h *Handler) TypeMapper() utils.TypeMapper { return h.typeMapper }

func NewHandler(logger log.Logger, queryBuilder QueryBuilder, connectionManager ConnectionManager, typeMapper utils.TypeMapper) *Handler {
	return &Handler{logger: logger, queryBuilder: queryBuilder, connectionManager: connectionManager, typeMapper: typeMapper}
}
