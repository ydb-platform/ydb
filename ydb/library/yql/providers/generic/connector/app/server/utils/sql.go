package utils

import (
	"context"

	"github.com/ydb-platform/ydb/library/go/core/log"
	api_common "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/api/common"
	api_service_protos "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/libgo/service/protos"
)

type Connection interface {
	Query(ctx context.Context, query string, args ...any) (Rows, error)
	Close() error
}

type Rows interface {
	Close() error
	Err() error
	Next() bool
	Scan(dest ...any) error
	MakeAcceptors() ([]any, error)
}

type ConnectionManager interface {
	Make(ctx context.Context, logger log.Logger, dataSourceInstance *api_common.TDataSourceInstance) (Connection, error)
	Release(logger log.Logger, connection Connection)
}

type ConnectionManagerBase struct {
	QueryLoggerFactory QueryLoggerFactory
}

type QueryExecutor interface {
	DescribeTable(ctx context.Context, conn Connection, request *api_service_protos.TDescribeTableRequest) (Rows, error)
}
