package utils

import (
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
)

type TypeMapper interface {
	SQLTypeToYDBColumn(columnName, typeName string) (*Ydb.Column, error)
	YDBTypeToAcceptor(ydbType *Ydb.Type) (any, error)
	AddRowToArrowIPCStreaming(
		ydbTypes []*Ydb.Type,
		acceptors []any,
		builders []array.Builder) error
}
