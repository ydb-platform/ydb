package postgresql

import (
	"context"
	"fmt"

	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/utils"
	api_service_protos "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/libgo/service/protos"
)

type queryExecutor struct {
}

func (qm queryExecutor) DescribeTable(ctx context.Context, conn *Connection, request *api_service_protos.TDescribeTableRequest) (utils.Rows, error) {
	out, err := conn.Query(
		ctx,
		"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = $1 AND table_schema ='public'",
		request.Table,
	)

	if err != nil {
		return nil, fmt.Errorf("connection query: %w", err)
	}

	return out, nil
}

func NewQueryExecutor() utils.QueryExecutor[*Connection] {
	return queryExecutor{}
}
