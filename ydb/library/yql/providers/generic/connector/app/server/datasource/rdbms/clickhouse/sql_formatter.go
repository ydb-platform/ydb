package clickhouse

import (
	"strings"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/utils"
	api_service_protos "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/libgo/service/protos"
)

var _ utils.SQLFormatter = (*sqlFormatter)(nil)

type sqlFormatter struct {
}

func (f *sqlFormatter) supportsType(typeID Ydb.Type_PrimitiveTypeId) bool {
	switch typeID {
	case Ydb.Type_BOOL:
		return true
	case Ydb.Type_INT8:
		return true
	case Ydb.Type_UINT8:
		return true
	case Ydb.Type_INT16:
		return true
	case Ydb.Type_UINT16:
		return true
	case Ydb.Type_INT32:
		return true
	case Ydb.Type_UINT32:
		return true
	case Ydb.Type_INT64:
		return true
	case Ydb.Type_UINT64:
		return true
	case Ydb.Type_FLOAT:
		return true
	case Ydb.Type_DOUBLE:
		return true
	default:
		return false
	}
}

func (f *sqlFormatter) supportsConstantValueExpression(t *Ydb.Type) bool {
	switch v := t.Type.(type) {
	case *Ydb.Type_TypeId:
		return f.supportsType(v.TypeId)
	case *Ydb.Type_OptionalType:
		return f.supportsConstantValueExpression(v.OptionalType.Item)
	default:
		return false
	}
}

func (f sqlFormatter) SupportsPushdownExpression(expression *api_service_protos.TExpression) bool {
	switch e := expression.Payload.(type) {
	case *api_service_protos.TExpression_Column:
		return true
	case *api_service_protos.TExpression_TypedValue:
		return f.supportsConstantValueExpression(e.TypedValue.Type)
	case *api_service_protos.TExpression_ArithmeticalExpression:
		return false
	case *api_service_protos.TExpression_Null:
		return true
	default:
		return false
	}
}

func (f sqlFormatter) GetDescribeTableQuery(request *api_service_protos.TDescribeTableRequest) (string, []any) {
	query := "SELECT name, type FROM system.columns WHERE table = ? and database = ?"
	args := []any{request.Table, request.DataSourceInstance.Database}

	return query, args
}

func (f sqlFormatter) GetPlaceholder(_ int) string {
	return "?"
}

func (f sqlFormatter) SanitiseIdentifier(ident string) string {
	// https: //clickhouse.com/docs/en/sql-reference/syntax#identifiers
	sanitizedIdent := strings.ReplaceAll(ident, string([]byte{0}), "")
	sanitizedIdent = `"` + strings.ReplaceAll(sanitizedIdent, `"`, `""`) + `"`

	return sanitizedIdent
}

func NewSQLFormatter() utils.SQLFormatter {
	return sqlFormatter{}
}
