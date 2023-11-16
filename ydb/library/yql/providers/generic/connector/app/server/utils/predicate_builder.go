package utils

import (
	"fmt"
	"strings"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	api_service_protos "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/libgo/service/protos"
)

type PredicateBuilderFeatures interface {
	// Support for high level expression (without subexpressions, they are checked separately)
	SupportsExpression(expression *api_service_protos.TExpression) bool
}

func FormatValue(value *Ydb.TypedValue) (string, error) {
	switch v := value.Value.Value.(type) {
	case *Ydb.Value_BoolValue:
		return fmt.Sprintf("%t", v.BoolValue), nil
	case *Ydb.Value_Int32Value:
		return fmt.Sprintf("%d", v.Int32Value), nil
	case *Ydb.Value_Uint32Value:
		return fmt.Sprintf("%d", v.Uint32Value), nil
	case *Ydb.Value_Int64Value:
		return fmt.Sprintf("%d", v.Int64Value), nil
	case *Ydb.Value_Uint64Value:
		return fmt.Sprintf("%d", v.Uint64Value), nil
	case *Ydb.Value_FloatValue:
		return fmt.Sprintf("%e", v.FloatValue), nil // TODO: support query parameters
	case *Ydb.Value_DoubleValue:
		return fmt.Sprintf("%e", v.DoubleValue), nil // TODO: support query parameters
	default:
		return "", fmt.Errorf("%w, type: %T", ErrUnimplementedTypedValue, v)
	}
}

func FormatColumn(col string) (string, error) {
	return col, nil
}

func FormatNull(n *api_service_protos.TExpression_TNull) (string, error) {
	return "NULL", nil
}

func FormatArithmeticalExpression(expression *api_service_protos.TExpression_TArithmeticalExpression, features PredicateBuilderFeatures) (string, error) {
	var operation string

	switch op := expression.Operation; op {
	case api_service_protos.TExpression_TArithmeticalExpression_MUL:
		operation = " * "
	case api_service_protos.TExpression_TArithmeticalExpression_ADD:
		operation = " + "
	case api_service_protos.TExpression_TArithmeticalExpression_SUB:
		operation = " - "
	case api_service_protos.TExpression_TArithmeticalExpression_BIT_AND:
		operation = " & "
	case api_service_protos.TExpression_TArithmeticalExpression_BIT_OR:
		operation = " | "
	case api_service_protos.TExpression_TArithmeticalExpression_BIT_XOR:
		operation = " ^ "
	default:
		return "", fmt.Errorf("%w, op: %d", ErrUnimplementedArithmeticalExpression, op)
	}

	var (
		left  string
		right string
		err   error
	)

	left, err = FormatExpression(expression.LeftValue, features)
	if err != nil {
		return "", fmt.Errorf("failed to format left argument: %w", err)
	}

	right, err = FormatExpression(expression.RightValue, features)
	if err != nil {
		return "", fmt.Errorf("failed to format right argument: %w", err)
	}

	return fmt.Sprintf("(%s%s%s)", left, operation, right), nil
}

func FormatExpression(expression *api_service_protos.TExpression, features PredicateBuilderFeatures) (string, error) {
	if !features.SupportsExpression(expression) {
		return "", ErrUnsupportedExpression
	}

	switch e := expression.Payload.(type) {
	case *api_service_protos.TExpression_Column:
		return FormatColumn(e.Column)
	case *api_service_protos.TExpression_TypedValue:
		return FormatValue(e.TypedValue)
	case *api_service_protos.TExpression_ArithmeticalExpression:
		return FormatArithmeticalExpression(e.ArithmeticalExpression, features)
	case *api_service_protos.TExpression_Null:
		return FormatNull(e.Null)
	default:
		return "", fmt.Errorf("%w, type: %T", ErrUnimplementedExpression, e)
	}
}

func FormatComparison(comparison *api_service_protos.TPredicate_TComparison, features PredicateBuilderFeatures) (string, error) {
	var operation string

	switch op := comparison.Operation; op {
	case api_service_protos.TPredicate_TComparison_L:
		operation = " < "
	case api_service_protos.TPredicate_TComparison_LE:
		operation = " <= "
	case api_service_protos.TPredicate_TComparison_EQ:
		operation = " = "
	case api_service_protos.TPredicate_TComparison_NE:
		operation = " <> "
	case api_service_protos.TPredicate_TComparison_GE:
		operation = " >= "
	case api_service_protos.TPredicate_TComparison_G:
		operation = " > "
	default:
		return "", fmt.Errorf("%w, op: %d", ErrUnimplementedOperation, op)
	}

	var (
		left  string
		right string
		err   error
	)

	left, err = FormatExpression(comparison.LeftValue, features)
	if err != nil {
		return "", fmt.Errorf("failed to format left argument: %w", err)
	}

	right, err = FormatExpression(comparison.RightValue, features)
	if err != nil {
		return "", fmt.Errorf("failed to format right argument: %w", err)
	}

	return fmt.Sprintf("(%s%s%s)", left, operation, right), nil
}

func FormatNegation(negation *api_service_protos.TPredicate_TNegation, features PredicateBuilderFeatures) (string, error) {
	pred, err := FormatPredicate(negation.Operand, features, false)
	if err != nil {
		return "", fmt.Errorf("failed to format NOT statement: %w", err)
	}

	return fmt.Sprintf("(NOT %s)", pred), nil
}

func FormatConjunction(conjunction *api_service_protos.TPredicate_TConjunction, features PredicateBuilderFeatures, topLevel bool) (string, error) {
	var (
		sb        strings.Builder
		succeeded int32 = 0
		statement string
		err       error
		first     string
	)

	for _, predicate := range conjunction.Operands {
		statement, err = FormatPredicate(predicate, features, false)
		if err != nil {
			if !topLevel {
				return "", fmt.Errorf("failed to format AND statement: %w", err)
			}
		} else {
			if succeeded > 0 {
				if succeeded == 1 {
					sb.WriteString("(")
					sb.WriteString(first)
				}

				sb.WriteString(" AND ")
				sb.WriteString(statement)
			} else {
				first = statement
			}

			succeeded++
		}
	}

	if succeeded == 0 {
		return "", fmt.Errorf("failed to format AND statement: %w", err)
	}

	if succeeded == 1 {
		sb.WriteString(first)
	} else {
		sb.WriteString(")")
	}

	return sb.String(), nil
}

func FormatDisjunction(disjunction *api_service_protos.TPredicate_TDisjunction, features PredicateBuilderFeatures) (string, error) {
	var (
		sb        strings.Builder
		cnt       int32 = 0
		statement string
		err       error
		first     string
	)

	for _, predicate := range disjunction.Operands {
		statement, err = FormatPredicate(predicate, features, false)
		if err != nil {
			return "", fmt.Errorf("failed to format OR statement: %w", err)
		} else {
			if cnt > 0 {
				if cnt == 1 {
					sb.WriteString("(")
					sb.WriteString(first)
				}

				sb.WriteString(" OR ")
				sb.WriteString(statement)
			} else {
				first = statement
			}

			cnt++
		}
	}

	if cnt == 0 {
		return "", fmt.Errorf("failed to format OR statement: no operands")
	}

	if cnt == 1 {
		sb.WriteString(first)
	} else {
		sb.WriteString(")")
	}

	return sb.String(), nil
}

func FormatIsNull(isNull *api_service_protos.TPredicate_TIsNull, features PredicateBuilderFeatures) (string, error) {
	statement, err := FormatExpression(isNull.Value, features)
	if err != nil {
		return "", fmt.Errorf("failed to format IS NULL statement: %w", err)
	}

	return fmt.Sprintf("(%s IS NULL)", statement), nil
}

func FormatIsNotNull(isNotNull *api_service_protos.TPredicate_TIsNotNull, features PredicateBuilderFeatures) (string, error) {
	statement, err := FormatExpression(isNotNull.Value, features)
	if err != nil {
		return "", fmt.Errorf("failed to format IS NOT NULL statement: %w", err)
	}

	return fmt.Sprintf("(%s IS NOT NULL)", statement), nil
}

func FormatPredicate(predicate *api_service_protos.TPredicate, features PredicateBuilderFeatures, topLevel bool) (string, error) {
	switch p := predicate.Payload.(type) {
	case *api_service_protos.TPredicate_Negation:
		return FormatNegation(p.Negation, features)
	case *api_service_protos.TPredicate_Conjunction:
		return FormatConjunction(p.Conjunction, features, topLevel)
	case *api_service_protos.TPredicate_Disjunction:
		return FormatDisjunction(p.Disjunction, features)
	case *api_service_protos.TPredicate_IsNull:
		return FormatIsNull(p.IsNull, features)
	case *api_service_protos.TPredicate_IsNotNull:
		return FormatIsNotNull(p.IsNotNull, features)
	case *api_service_protos.TPredicate_Comparison:
		return FormatComparison(p.Comparison, features)
	case *api_service_protos.TPredicate_BoolExpression:
		return FormatExpression(p.BoolExpression.Value, features)
	default:
		return "", fmt.Errorf("%w, type: %T", ErrUnimplementedPredicateType, p)
	}
}

func FormatWhereClause(where *api_service_protos.TSelect_TWhere, features PredicateBuilderFeatures) (string, error) {
	if where.FilterTyped == nil {
		return "", ErrUnimplemented
	}

	formatted, err := FormatPredicate(where.FilterTyped, features, true)
	if err != nil {
		return "", err
	}

	result := "WHERE " + formatted

	return result, nil
}
