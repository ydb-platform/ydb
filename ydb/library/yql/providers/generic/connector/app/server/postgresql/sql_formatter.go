package postgresql

import (
	"fmt"
	"strings"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb/library/go/core/log"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/utils"
	api_service_protos "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/libgo/service/protos"
)

type sqlFormatter struct {
}

type predicateBuilderFeatures struct {
}

func (f predicateBuilderFeatures) SupportsType(typeID Ydb.Type_PrimitiveTypeId) bool {
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

func (f predicateBuilderFeatures) SupportsConstantValueExpression(t *Ydb.Type) bool {
	switch v := t.Type.(type) {
	case *Ydb.Type_TypeId:
		return f.SupportsType(v.TypeId)
	case *Ydb.Type_OptionalType:
		return f.SupportsConstantValueExpression(v.OptionalType.Item)
	default:
		return false
	}
}

func (f predicateBuilderFeatures) SupportsExpression(expression *api_service_protos.TExpression) bool {
	switch e := expression.Payload.(type) {
	case *api_service_protos.TExpression_Column:
		return true
	case *api_service_protos.TExpression_TypedValue:
		return f.SupportsConstantValueExpression(e.TypedValue.Type)
	case *api_service_protos.TExpression_ArithmeticalExpression:
		return false
	case *api_service_protos.TExpression_Null:
		return true
	default:
		return false
	}
}

func (formatter sqlFormatter) FormatRead(logger log.Logger, selectReq *api_service_protos.TSelect) (string, error) {
	var sb strings.Builder

	selectPart, err := utils.FormatSelectColumns(selectReq.What, selectReq.GetFrom().GetTable(), true)
	if err != nil {
		return "", fmt.Errorf("failed to format select statement: %w", err)
	}

	sb.WriteString(selectPart)

	if selectReq.Where != nil {
		var features predicateBuilderFeatures

		clause, err := utils.FormatWhereClause(selectReq.Where, features)
		if err != nil {
			logger.Error("Failed to format WHERE clause", log.Error(err), log.String("where", selectReq.Where.String()))
		} else {
			sb.WriteString(" ")
			sb.WriteString(clause)
		}
	}

	query := sb.String()

	return query, nil
}

func NewSQLFormatter() utils.SQLFormatter {
	return sqlFormatter{}
}
