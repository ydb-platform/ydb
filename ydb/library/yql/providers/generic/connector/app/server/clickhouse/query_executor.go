package clickhouse

import (
	"context"
	"fmt"

	"github.com/ydb-platform/ydb/library/go/core/log"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/utils"
	api_service_protos "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/libgo/service/protos"
)

type queryExecutor struct {
}

func (qm queryExecutor) DescribeTable(
	ctx context.Context,
	conn *Connection,
	request *api_service_protos.TDescribeTableRequest,
) (utils.Rows, error) {
	out, err := conn.QueryContext(
		ctx,
		"SELECT name, type FROM system.columns WHERE table = ? and database = ?",
		request.Table,
		request.DataSourceInstance.Database,
	)

	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}

	if err := out.Err(); err != nil {
		defer func() {
			if err := out.Close(); err != nil {
				conn.logger.Error("close rows", log.Error(err))
			}
		}()

		return nil, fmt.Errorf("rows err: %w", err)
	}

	return rows{Rows: out}, nil
}

func NewQueryExecutor() utils.QueryExecutor[*Connection] {
	return queryExecutor{}
}
