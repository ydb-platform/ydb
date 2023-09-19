package rdbms

import (
	"fmt"

	"github.com/ydb-platform/ydb/library/go/core/log"
	api_common "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/api/common"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/clickhouse"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/postgresql"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/utils"
)

type preset[CONN utils.Connection] struct {
	queryExecutor     utils.QueryExecutor[CONN]
	connectionManager utils.ConnectionManager[CONN]
	typeMapper        utils.TypeMapper
}

type HandlerFactory struct {
	clickhouse preset[*clickhouse.Connection]
	postgresql preset[*postgresql.Connection]
}

func (hf *HandlerFactory) Make(
	logger log.Logger,
	dataSourceType api_common.EDataSourceKind,
) (Handler, error) {

	switch dataSourceType {
	case api_common.EDataSourceKind_CLICKHOUSE:
		return newHandler[*clickhouse.Connection](logger, &hf.clickhouse), nil
	case api_common.EDataSourceKind_POSTGRESQL:
		return newHandler[*postgresql.Connection](logger, &hf.postgresql), nil
	default:
		return nil, fmt.Errorf("pick handler for data source type '%v': %w", dataSourceType, utils.ErrDataSourceNotSupported)
	}
}

func NewHandlerFactory() *HandlerFactory {
	return &HandlerFactory{
		clickhouse: preset[*clickhouse.Connection]{
			queryExecutor:     clickhouse.NewQueryExecutor(),
			connectionManager: clickhouse.NewConnectionManager(),
			typeMapper:        clickhouse.NewTypeMapper(),
		},
		postgresql: preset[*postgresql.Connection]{
			queryExecutor:     postgresql.NewQueryExecutor(),
			connectionManager: postgresql.NewConnectionManager(),
			typeMapper:        postgresql.NewTypeMapper(),
		},
	}
}
