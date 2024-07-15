#include "predicate_builder.h"

#include <util/string/cast.h>

namespace NFq {

TString FormatColumn(const TString& value);
TString FormatValue(/*formatter SQLFormatter, args []any,*/ Ydb::TypedValue value) ;
TString FormatNull(NYql::NPq::NProto::TExpression_TNull);
TString FormatExpression(/*formatter SQLFormatter, */ NYql::NPq::NProto::TExpression expression);
TString FormatArithmeticalExpression(NYql::NPq::NProto::TExpression_TArithmeticalExpression expression);
TString FormatComparison(/*formatter SQLFormatter, args []any, */NYql::NPq::NProto::TPredicate_TComparison comparison);
TString FormatPredicate(const NYql::NPq::NProto::TPredicate& predicate, bool /*topLevel*/ );

TString FormatColumn(const TString& value) {
	//return formatter.SanitiseIdentifier(col), args, nil
    return value;
}

TString FormatValue(/*formatter SQLFormatter, args []any,*/ Ydb::TypedValue value) {
	switch (value.value().value_case()) {
	case  Ydb::Value::kBoolValue:
		return ToString(value.value().bool_value());
	case Ydb::Value::kInt32Value:
		return ToString(value.value().int32_value());
	case Ydb::Value::kUint32Value:
		return ToString(value.value().uint32_value());
	case Ydb::Value::kInt64Value:
		return ToString(value.value().int64_value());
	case Ydb::Value::kUint64Value:
		return ToString(value.value().uint64_value());
	case Ydb::Value::kFloatValue:
		return ToString(value.value().float_value());
	case Ydb::Value::kDoubleValue:
		return ToString(value.value().double_value());
	case Ydb::Value::kBytesValue:
		return "\"" + ToString(value.value().bytes_value()) + "\"";
	case Ydb::Value::kTextValue:
		return "\"" + ToString(value.value().text_value()) + "\"";
	default:
        ythrow yexception() << "ErrUnimplementedTypedValue";
	}
}

TString FormatNull(NYql::NPq::NProto::TExpression_TNull) {
	return "NULL";
}

TString FormatExpression(/*formatter SQLFormatter, */ NYql::NPq::NProto::TExpression expression) {
	// if !formatter.SupportsPushdownExpression(expression) {
	// 	return "", args, common.ErrUnsupportedExpression
	// }
    switch (expression.payload_case()) {
	case NYql::NPq::NProto::TExpression::kColumn:
		return FormatColumn(expression.column());
	case NYql::NPq::NProto::TExpression::kTypedValue:
	 	return FormatValue(expression.typed_value());
    case NYql::NPq::NProto::TExpression::kArithmeticalExpression:
	  	return FormatArithmeticalExpression(expression.arithmetical_expression());
	case NYql::NPq::NProto::TExpression::kNull:
	 	return FormatNull(expression.null());
	default:
		ythrow yexception() << "UnimplementedExpression";
	}
}

TString FormatArithmeticalExpression(NYql::NPq::NProto::TExpression_TArithmeticalExpression expression) {
    TString operation;
	switch (expression.operation()) {
	case NYql::NPq::NProto::TExpression_TArithmeticalExpression::MUL:
		operation = " * ";
        break;
	case NYql::NPq::NProto::TExpression_TArithmeticalExpression::ADD:
		operation = " + ";
        break;
	case NYql::NPq::NProto::TExpression_TArithmeticalExpression::SUB:
		operation = " - ";
        break;
	case NYql::NPq::NProto::TExpression_TArithmeticalExpression::BIT_AND:
		operation = " & ";
        break;
	case NYql::NPq::NProto::TExpression_TArithmeticalExpression::BIT_OR:
		operation = " | ";
        break;
	case NYql::NPq::NProto::TExpression_TArithmeticalExpression::BIT_XOR:
		operation = " ^ ";
        break;
	default:
		ythrow yexception() << "ErrUnimplementedArithmeticalExpression";
	}

	auto left = FormatExpression(/*formatter, args,*/ expression.left_value());
	auto right = FormatExpression(/*formatter, args, */expression.right_value());
	return left + operation + right;
}

TString FormatComparison(/*formatter SQLFormatter, args []any, */NYql::NPq::NProto::TPredicate_TComparison comparison) {
	TString operation;

    switch (comparison.operation()) {
	case NYql::NPq::NProto::TPredicate_TComparison::L:
		operation = " < ";
        break;
	case NYql::NPq::NProto::TPredicate_TComparison::LE:
		operation = " <= ";
        break;
	case NYql::NPq::NProto::TPredicate_TComparison::EQ:
		operation = " = ";
        break;
	case NYql::NPq::NProto::TPredicate_TComparison::NE:
		operation = " <> ";
        break;
	case NYql::NPq::NProto::TPredicate_TComparison::GE:
		operation = " >= ";
        break;
	case NYql::NPq::NProto::TPredicate_TComparison::G:
		operation = " > ";
        break;
	default:
		ythrow yexception() << "UnimplementedOperation";
	}

	auto left = FormatExpression(/*formatter, args,*/ comparison.left_value());
	auto right = FormatExpression(/*formatter, args, */comparison.right_value());

	return left + operation + right;
}

TString FormatPredicate(const NYql::NPq::NProto::TPredicate& predicate, bool /*topLevel*/ ) {
    switch (predicate.payload_case()) {
        case NYql::NPq::NProto::TPredicate::PAYLOAD_NOT_SET:
                return {};
       /* case NYql::NPq::NProto::::TPredicate::kNegation:
    		return formatNegation(formatter, args, predicate.GetNegation())
        case NYql::NPq::NProto::::TPredicate::kConjunction:
            return formatConjunction(formatter, args, p.Conjunction, topLevel)
        case NYql::NPq::NProto::::TPredicate::kDisjunction:
            return formatDisjunction(formatter, args, p.Disjunction)
        case NYql::NPq::NProto::::TPredicate::kIsNull:
            return formatIsNull(formatter, args, p.IsNull)
        case NYql::NPq::NProto::::TPredicate::kIsNotNull:
            return formatIsNotNull(formatter, args, p.IsNotNull)*/
        case NYql::NPq::NProto::TPredicate::kComparison:
            return FormatComparison(/*formatter, args, */predicate.comparison());
        // case NYql::NPq::NProto::::TPredicate::kBoolExpression:
        //     return formatExpression(formatter, args, p.BoolExpression.Value)
        default:
            ythrow yexception() << "UnimplementedPredicateType";
	}
}

TString FormatWhere(const NYql::NPq::NProto::TPredicate& predicate) {
    auto stream = FormatPredicate(predicate, true);
	if (stream.empty()) {
		return "";
	}
    return "WHERE " + stream;
}

} // namespace NFq
