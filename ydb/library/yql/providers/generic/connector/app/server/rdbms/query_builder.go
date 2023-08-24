package rdbms

import (
	api_service_protos "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/libgo/service/protos"
)

type QueryBuilder interface {
	DescribeTable(request *api_service_protos.TDescribeTableRequest) string
}
