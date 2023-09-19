package clickhouse

import (
	"context"
	"fmt"

	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/utils"
	api_service_protos "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/libgo/service/protos"
)

type queryExecutor struct {
}

func (qm queryExecutor) DescribeTable(ctx context.Context, conn *Connection, request *api_service_protos.TDescribeTableRequest) (utils.Rows, error) {
	out, err := conn.QueryContext(
		ctx,
		"SELECT name, type FROM system.columns WHERE table = ? and database = ?",
		request.Table,
		request.DataSourceInstance.Database,
	)

	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}

	return out, nil
}

func NewQueryExecutor() utils.QueryExecutor[*Connection] {
	return queryExecutor{}
}
