package clickhouse

import (
	"fmt"

	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/rdbms"
	api_service_protos "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/libgo/service/protos"
)

var _ rdbms.QueryBuilder = (*queryBuilder)(nil)

type queryBuilder struct {
}

func (qb queryBuilder) DescribeTable(request *api_service_protos.TDescribeTableRequest) string {
	return fmt.Sprintf(
		"SELECT name, type FROM system.columns WHERE table = '%s' and database ='%s'",
		request.Table,
		request.DataSourceInstance.Database)
}

func NewQueryBuilder() rdbms.QueryBuilder { return queryBuilder{} }
