package utils

import (
	"context"

	"github.com/ydb-platform/ydb/library/go/core/log"
	api_common "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/api/common"
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
}

type ConnectionManager[CONN any] interface {
	Make(ctx context.Context, logger log.Logger, dataSourceInstance *api_common.TDataSourceInstance) (CONN, error)
	Release(logger log.Logger, conn CONN)
}
