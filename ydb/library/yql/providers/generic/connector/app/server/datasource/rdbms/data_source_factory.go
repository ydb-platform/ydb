package rdbms

import (
	"fmt"

	"github.com/ydb-platform/ydb/library/go/core/log"
	api_common "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/api/common"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/datasource"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/datasource/rdbms/clickhouse"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/datasource/rdbms/postgresql"
	rdbms_utils "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/datasource/rdbms/utils"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/utils"
)

var _ datasource.DataSourceFactory[any] = (*dataSourceFactory)(nil)

type dataSourceFactory struct {
	clickhouse Preset
	postgresql Preset
}

func (dsf *dataSourceFactory) Make(
	logger log.Logger,
	dataSourceType api_common.EDataSourceKind,
) (datasource.DataSource[any], error) {
	switch dataSourceType {
	case api_common.EDataSourceKind_CLICKHOUSE:
		return NewDataSource(logger, &dsf.clickhouse), nil
	case api_common.EDataSourceKind_POSTGRESQL:
		return NewDataSource(logger, &dsf.postgresql), nil
	default:
		return nil, fmt.Errorf("pick handler for data source type '%v': %w", dataSourceType, utils.ErrDataSourceNotSupported)
	}
}

func NewDataSourceFactory(qlf utils.QueryLoggerFactory) datasource.DataSourceFactory[any] {
	connManagerCfg := rdbms_utils.ConnectionManagerBase{
		QueryLoggerFactory: qlf,
	}

	return &dataSourceFactory{
		clickhouse: Preset{
			SQLFormatter:      clickhouse.NewSQLFormatter(),
			ConnectionManager: clickhouse.NewConnectionManager(connManagerCfg),
			TypeMapper:        clickhouse.NewTypeMapper(),
		},
		postgresql: Preset{
			SQLFormatter:      postgresql.NewSQLFormatter(),
			ConnectionManager: postgresql.NewConnectionManager(connManagerCfg),
			TypeMapper:        postgresql.NewTypeMapper(),
		},
	}
}
