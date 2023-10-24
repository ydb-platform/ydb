package rdbms

import (
	"fmt"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/utils"
	api_service_protos "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/libgo/service/protos"
)

func FormatValue(value *Ydb.TypedValue) (string, error) {
	switch v := value.Value.Value.(type) {
	case *Ydb.Value_BoolValue:
		return fmt.Sprintf("%t", v.BoolValue), nil
	case *Ydb.Value_Int32Value:
		return fmt.Sprintf("%d", v.Int32Value), nil
	case *Ydb.Value_Uint32Value:
		return fmt.Sprintf("%d", v.Uint32Value), nil
	default:
		return "", fmt.Errorf("%w, type: %T", utils.ErrUnimplementedTypedValue, v)
	}
}

func FormatExpression(expression *api_service_protos.TExpression) (string, error) {
	switch e := expression.Payload.(type) {
	case *api_service_protos.TExpression_Column:
		return e.Column, nil
	case *api_service_protos.TExpression_TypedValue:
		return FormatValue(e.TypedValue)
	default:
		return "", fmt.Errorf("%w, type: %T", utils.ErrUnimplementedExpression, e)
	}
}

func FormatComparison(comparison *api_service_protos.TPredicate_TComparison) (string, error) {
	var operation string

	switch op := comparison.Operation; op {
	case api_service_protos.TPredicate_TComparison_EQ:
		operation = " = "
	default:
		return "", fmt.Errorf("%w, op: %d", utils.ErrUnimplementedOperation, op)
	}

	var (
		left  string
		right string
		err   error
	)

	left, err = FormatExpression(comparison.LeftValue)
	if err != nil {
		return "", fmt.Errorf("failed to format left argument: %w", err)
	}

	right, err = FormatExpression(comparison.RightValue)
	if err != nil {
		return "", fmt.Errorf("failed to format right argument: %w", err)
	}

	return fmt.Sprintf("(%s%s%s)", left, operation, right), nil
}

func FormatPredicate(predicate *api_service_protos.TPredicate) (string, error) {
	switch p := predicate.Payload.(type) {
	case *api_service_protos.TPredicate_Comparison:
		return FormatComparison(p.Comparison)
	default:
		return "", fmt.Errorf("%w, type: %T", utils.ErrUnimplementedPredicateType, p)
	}
}

func FormatWhereStatement(where *api_service_protos.TSelect_TWhere) (string, error) {
	if where.FilterTyped == nil {
		return "", utils.ErrUnimplemented
	}

	formatted, err := FormatPredicate(where.FilterTyped)
	if err != nil {
		return "", err
	}

	result := "WHERE " + formatted

	return result, nil
}
