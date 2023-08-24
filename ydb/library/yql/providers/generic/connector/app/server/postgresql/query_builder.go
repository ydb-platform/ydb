package postgresql

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
		// TODO: is hardconing schema correct?
		"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '%s' AND table_schema ='public'",
		request.Table)
}

func NewQueryBuilder() rdbms.QueryBuilder { return queryBuilder{} }
