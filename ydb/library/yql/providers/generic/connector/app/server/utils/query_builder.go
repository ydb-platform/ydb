package utils

import (
	"fmt"
	"strings"

	"github.com/ydb-platform/ydb/library/go/core/log"
	api_service_protos "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/libgo/service/protos"
)

func MakeDescribeTableQuery(logger log.Logger, formatter SQLFormatter, request *api_service_protos.TDescribeTableRequest) (string, []any) {
	query, args := formatter.GetDescribeTableQuery(request)

	return query, args
}

func MakeReadSplitQuery(logger log.Logger, formatter SQLFormatter, request *api_service_protos.TSelect) (string, []any, error) {
	var sb strings.Builder

	selectPart, err := formatSelectColumns(formatter, request.What, request.GetFrom().GetTable(), true)
	if err != nil {
		return "", nil, fmt.Errorf("failed to format select statement: %w", err)
	}

	sb.WriteString(selectPart)

	if request.Where != nil {
		clause, err := formatWhereClause(formatter, request.Where)
		if err != nil {
			logger.Error("Failed to format WHERE clause", log.Error(err), log.String("where", request.Where.String()))
		} else {
			sb.WriteString(" ")
			sb.WriteString(clause)
		}
	}

	query := sb.String()

	return query, nil, nil
}
