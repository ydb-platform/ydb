package utils

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	api_service_protos "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/libgo/service/protos"
)

type TypeMapper interface {
	SQLTypeToYDBColumn(columnName, typeName string, rules *api_service_protos.TTypeMappingSettings) (*Ydb.Column, error)
}
