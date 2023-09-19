package utils

import (
	"context"

	api_service_protos "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/libgo/service/protos"
)

type QueryExecutor[CONN Connection] interface {
	DescribeTable(ctx context.Context, conn CONN, request *api_service_protos.TDescribeTableRequest) (Rows, error)
}
