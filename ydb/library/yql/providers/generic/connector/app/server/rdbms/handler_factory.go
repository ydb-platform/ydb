package rdbms

import (
	"fmt"

	"github.com/ydb-platform/ydb/library/go/core/log"
	api_common "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/api/common"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/clickhouse"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/postgresql"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/utils"
)

type handlerPreset struct {
	sqlFormatter      utils.SQLFormatter
	connectionManager utils.ConnectionManager
	typeMapper        utils.TypeMapper
}

type handlerFactoryImpl struct {
	clickhouse handlerPreset
	postgresql handlerPreset
}

func (hf *handlerFactoryImpl) Make(
	logger log.Logger,
	dataSourceType api_common.EDataSourceKind,
) (Handler, error) {
	switch dataSourceType {
	case api_common.EDataSourceKind_CLICKHOUSE:
		return newHandler(logger, &hf.clickhouse), nil
	case api_common.EDataSourceKind_POSTGRESQL:
		return newHandler(logger, &hf.postgresql), nil
	default:
		return nil, fmt.Errorf("pick handler for data source type '%v': %w", dataSourceType, utils.ErrDataSourceNotSupported)
	}
}

func NewHandlerFactory(qlf utils.QueryLoggerFactory) HandlerFactory {
	connManagerCfg := utils.ConnectionManagerBase{
		QueryLoggerFactory: qlf,
	}

	return &handlerFactoryImpl{
		clickhouse: handlerPreset{
			sqlFormatter:      clickhouse.NewSQLFormatter(),
			connectionManager: clickhouse.NewConnectionManager(connManagerCfg),
			typeMapper:        clickhouse.NewTypeMapper(),
		},
		postgresql: handlerPreset{
			sqlFormatter:      postgresql.NewSQLFormatter(),
			connectionManager: postgresql.NewConnectionManager(connManagerCfg),
			typeMapper:        postgresql.NewTypeMapper(),
		},
	}
}
