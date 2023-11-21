package utils

import (
	"fmt"
	"strings"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	api_service_protos "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/libgo/service/protos"
)

func formatValue(formatter SQLFormatter, value *Ydb.TypedValue) (string, error) {
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

func formatColumn(formatter SQLFormatter, col string) (string, error) {
	return formatter.SanitiseIdentifier(col), nil
}

func formatNull(formatter SQLFormatter, n *api_service_protos.TExpression_TNull) (string, error) {
	return "NULL", nil
}

func formatArithmeticalExpression(formatter SQLFormatter, expression *api_service_protos.TExpression_TArithmeticalExpression) (string, error) {
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

	left, err = formatExpression(formatter, expression.LeftValue)
	if err != nil {
		return "", fmt.Errorf("failed to format left argument: %w", err)
	}

	right, err = formatExpression(formatter, expression.RightValue)
	if err != nil {
		return "", fmt.Errorf("failed to format right argument: %w", err)
	}

	return fmt.Sprintf("(%s%s%s)", left, operation, right), nil
}

func formatExpression(formatter SQLFormatter, expression *api_service_protos.TExpression) (string, error) {
	if !formatter.SupportsPushdownExpression(expression) {
		return "", ErrUnsupportedExpression
	}

	switch e := expression.Payload.(type) {
	case *api_service_protos.TExpression_Column:
		return formatColumn(formatter, e.Column)
	case *api_service_protos.TExpression_TypedValue:
		return formatValue(formatter, e.TypedValue)
	case *api_service_protos.TExpression_ArithmeticalExpression:
		return formatArithmeticalExpression(formatter, e.ArithmeticalExpression)
	case *api_service_protos.TExpression_Null:
		return formatNull(formatter, e.Null)
	default:
		return "", fmt.Errorf("%w, type: %T", ErrUnimplementedExpression, e)
	}
}

func formatComparison(formatter SQLFormatter, comparison *api_service_protos.TPredicate_TComparison) (string, error) {
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

	left, err = formatExpression(formatter, comparison.LeftValue)
	if err != nil {
		return "", fmt.Errorf("failed to format left argument: %w", err)
	}

	right, err = formatExpression(formatter, comparison.RightValue)
	if err != nil {
		return "", fmt.Errorf("failed to format right argument: %w", err)
	}

	return fmt.Sprintf("(%s%s%s)", left, operation, right), nil
}

func formatNegation(formatter SQLFormatter, negation *api_service_protos.TPredicate_TNegation) (string, error) {
	pred, err := formatPredicate(formatter, negation.Operand, false)
	if err != nil {
		return "", fmt.Errorf("failed to format NOT statement: %w", err)
	}

	return fmt.Sprintf("(NOT %s)", pred), nil
}

func formatConjunction(formatter SQLFormatter, conjunction *api_service_protos.TPredicate_TConjunction, topLevel bool) (string, error) {
	var (
		sb        strings.Builder
		succeeded int32 = 0
		statement string
		err       error
		first     string
	)

	for _, predicate := range conjunction.Operands {
		statement, err = formatPredicate(formatter, predicate, false)
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

func formatDisjunction(formatter SQLFormatter, disjunction *api_service_protos.TPredicate_TDisjunction) (string, error) {
	var (
		sb        strings.Builder
		cnt       int32 = 0
		statement string
		err       error
		first     string
	)

	for _, predicate := range disjunction.Operands {
		statement, err = formatPredicate(formatter, predicate, false)
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

func formatIsNull(formatter SQLFormatter, isNull *api_service_protos.TPredicate_TIsNull) (string, error) {
	statement, err := formatExpression(formatter, isNull.Value)
	if err != nil {
		return "", fmt.Errorf("failed to format IS NULL statement: %w", err)
	}

	return fmt.Sprintf("(%s IS NULL)", statement), nil
}

func formatIsNotNull(formatter SQLFormatter, isNotNull *api_service_protos.TPredicate_TIsNotNull) (string, error) {
	statement, err := formatExpression(formatter, isNotNull.Value)
	if err != nil {
		return "", fmt.Errorf("failed to format IS NOT NULL statement: %w", err)
	}

	return fmt.Sprintf("(%s IS NOT NULL)", statement), nil
}

func formatPredicate(formatter SQLFormatter, predicate *api_service_protos.TPredicate, topLevel bool) (string, error) {
	switch p := predicate.Payload.(type) {
	case *api_service_protos.TPredicate_Negation:
		return formatNegation(formatter, p.Negation)
	case *api_service_protos.TPredicate_Conjunction:
		return formatConjunction(formatter, p.Conjunction, topLevel)
	case *api_service_protos.TPredicate_Disjunction:
		return formatDisjunction(formatter, p.Disjunction)
	case *api_service_protos.TPredicate_IsNull:
		return formatIsNull(formatter, p.IsNull)
	case *api_service_protos.TPredicate_IsNotNull:
		return formatIsNotNull(formatter, p.IsNotNull)
	case *api_service_protos.TPredicate_Comparison:
		return formatComparison(formatter, p.Comparison)
	case *api_service_protos.TPredicate_BoolExpression:
		return formatExpression(formatter, p.BoolExpression.Value)
	default:
		return "", fmt.Errorf("%w, type: %T", ErrUnimplementedPredicateType, p)
	}
}

func formatWhereClause(formatter SQLFormatter, where *api_service_protos.TSelect_TWhere) (string, error) {
	if where.FilterTyped == nil {
		return "", ErrUnimplemented
	}

	formatted, err := formatPredicate(formatter, where.FilterTyped, true)
	if err != nil {
		return "", err
	}

	result := "WHERE " + formatted

	return result, nil
}
