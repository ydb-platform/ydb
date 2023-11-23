package utils

import (
	"fmt"
	"strings"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	api_service_protos "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/libgo/service/protos"
)

func formatValue(formatter SQLFormatter, args []any, value *Ydb.TypedValue) (string, []any, error) {
	switch v := value.Value.Value.(type) {
	case *Ydb.Value_BoolValue:
		return formatter.GetPlaceholder(len(args)), append(args, v.BoolValue), nil
	case *Ydb.Value_Int32Value:
		return formatter.GetPlaceholder(len(args)), append(args, v.Int32Value), nil
	case *Ydb.Value_Uint32Value:
		return formatter.GetPlaceholder(len(args)), append(args, v.Uint32Value), nil
	case *Ydb.Value_Int64Value:
		return formatter.GetPlaceholder(len(args)), append(args, v.Int64Value), nil
	case *Ydb.Value_Uint64Value:
		return formatter.GetPlaceholder(len(args)), append(args, v.Uint64Value), nil
	case *Ydb.Value_FloatValue:
		return formatter.GetPlaceholder(len(args)), append(args, v.FloatValue), nil
	case *Ydb.Value_DoubleValue:
		return formatter.GetPlaceholder(len(args)), append(args, v.DoubleValue), nil
	default:
		return "", args, fmt.Errorf("%w, type: %T", ErrUnimplementedTypedValue, v)
	}
}

func formatColumn(formatter SQLFormatter, args []any, col string) (string, []any, error) {
	return formatter.SanitiseIdentifier(col), args, nil
}

func formatNull(formatter SQLFormatter, args []any, n *api_service_protos.TExpression_TNull) (string, []any, error) {
	return "NULL", args, nil
}

func formatArithmeticalExpression(formatter SQLFormatter, args []any, expression *api_service_protos.TExpression_TArithmeticalExpression) (string, []any, error) {
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
		return "", args, fmt.Errorf("%w, op: %d", ErrUnimplementedArithmeticalExpression, op)
	}

	left, args, err := formatExpression(formatter, args, expression.LeftValue)
	if err != nil {
		return "", args, fmt.Errorf("failed to format left argument: %w", err)
	}

	right, args, err := formatExpression(formatter, args, expression.RightValue)
	if err != nil {
		return "", args, fmt.Errorf("failed to format right argument: %w", err)
	}

	return fmt.Sprintf("(%s%s%s)", left, operation, right), args, nil
}

func formatExpression(formatter SQLFormatter, args []any, expression *api_service_protos.TExpression) (string, []any, error) {
	if !formatter.SupportsPushdownExpression(expression) {
		return "", args, ErrUnsupportedExpression
	}

	switch e := expression.Payload.(type) {
	case *api_service_protos.TExpression_Column:
		return formatColumn(formatter, args, e.Column)
	case *api_service_protos.TExpression_TypedValue:
		return formatValue(formatter, args, e.TypedValue)
	case *api_service_protos.TExpression_ArithmeticalExpression:
		return formatArithmeticalExpression(formatter, args, e.ArithmeticalExpression)
	case *api_service_protos.TExpression_Null:
		return formatNull(formatter, args, e.Null)
	default:
		return "", args, fmt.Errorf("%w, type: %T", ErrUnimplementedExpression, e)
	}
}

func formatComparison(formatter SQLFormatter, args []any, comparison *api_service_protos.TPredicate_TComparison) (string, []any, error) {
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
		return "", args, fmt.Errorf("%w, op: %d", ErrUnimplementedOperation, op)
	}

	left, args, err := formatExpression(formatter, args, comparison.LeftValue)
	if err != nil {
		return "", args, fmt.Errorf("failed to format left argument: %w", err)
	}

	right, args, err := formatExpression(formatter, args, comparison.RightValue)
	if err != nil {
		return "", args, fmt.Errorf("failed to format right argument: %w", err)
	}

	return fmt.Sprintf("(%s%s%s)", left, operation, right), args, nil
}

func formatNegation(formatter SQLFormatter, args []any, negation *api_service_protos.TPredicate_TNegation) (string, []any, error) {
	pred, args, err := formatPredicate(formatter, args, negation.Operand, false)
	if err != nil {
		return "", args, fmt.Errorf("failed to format NOT statement: %w", err)
	}

	return fmt.Sprintf("(NOT %s)", pred), args, nil
}

func formatConjunction(formatter SQLFormatter, args []any, conjunction *api_service_protos.TPredicate_TConjunction, topLevel bool) (string, []any, error) {
	var (
		sb        strings.Builder
		succeeded int32 = 0
		statement string
		err       error
		first     string
	)

	for _, predicate := range conjunction.Operands {
		argsCut := args
		statement, args, err = formatPredicate(formatter, args, predicate, false)

		if err != nil {
			if !topLevel {
				return "", args, fmt.Errorf("failed to format AND statement: %w", err)
			} else {
				args = argsCut
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
		return "", args, fmt.Errorf("failed to format AND statement: %w", err)
	}

	if succeeded == 1 {
		sb.WriteString(first)
	} else {
		sb.WriteString(")")
	}

	return sb.String(), args, nil
}

func formatDisjunction(formatter SQLFormatter, args []any, disjunction *api_service_protos.TPredicate_TDisjunction) (string, []any, error) {
	var (
		sb        strings.Builder
		cnt       int32 = 0
		statement string
		err       error
		first     string
	)

	for _, predicate := range disjunction.Operands {
		statement, args, err = formatPredicate(formatter, args, predicate, false)
		if err != nil {
			return "", args, fmt.Errorf("failed to format OR statement: %w", err)
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
		return "", args, fmt.Errorf("failed to format OR statement: no operands")
	}

	if cnt == 1 {
		sb.WriteString(first)
	} else {
		sb.WriteString(")")
	}

	return sb.String(), args, nil
}

func formatIsNull(formatter SQLFormatter, args []any, isNull *api_service_protos.TPredicate_TIsNull) (string, []any, error) {
	statement, args, err := formatExpression(formatter, args, isNull.Value)
	if err != nil {
		return "", args, fmt.Errorf("failed to format IS NULL statement: %w", err)
	}

	return fmt.Sprintf("(%s IS NULL)", statement), args, nil
}

func formatIsNotNull(formatter SQLFormatter, args []any, isNotNull *api_service_protos.TPredicate_TIsNotNull) (string, []any, error) {
	statement, args, err := formatExpression(formatter, args, isNotNull.Value)
	if err != nil {
		return "", args, fmt.Errorf("failed to format IS NOT NULL statement: %w", err)
	}

	return fmt.Sprintf("(%s IS NOT NULL)", statement), args, nil
}

func formatPredicate(formatter SQLFormatter, args []any, predicate *api_service_protos.TPredicate, topLevel bool) (string, []any, error) {
	switch p := predicate.Payload.(type) {
	case *api_service_protos.TPredicate_Negation:
		return formatNegation(formatter, args, p.Negation)
	case *api_service_protos.TPredicate_Conjunction:
		return formatConjunction(formatter, args, p.Conjunction, topLevel)
	case *api_service_protos.TPredicate_Disjunction:
		return formatDisjunction(formatter, args, p.Disjunction)
	case *api_service_protos.TPredicate_IsNull:
		return formatIsNull(formatter, args, p.IsNull)
	case *api_service_protos.TPredicate_IsNotNull:
		return formatIsNotNull(formatter, args, p.IsNotNull)
	case *api_service_protos.TPredicate_Comparison:
		return formatComparison(formatter, args, p.Comparison)
	case *api_service_protos.TPredicate_BoolExpression:
		return formatExpression(formatter, args, p.BoolExpression.Value)
	default:
		return "", args, fmt.Errorf("%w, type: %T", ErrUnimplementedPredicateType, p)
	}
}

func formatWhereClause(formatter SQLFormatter, where *api_service_protos.TSelect_TWhere) (string, []any, error) {
	if where.FilterTyped == nil {
		return "", nil, ErrUnimplemented
	}

	args := make([]any, 0)
	formatted, args, err := formatPredicate(formatter, args, where.FilterTyped, true)

	if err != nil {
		return "", nil, err
	}

	result := "WHERE " + formatted

	return result, args, nil
}
