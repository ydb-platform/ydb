#include "predicate_builder.h"

#include <util/string/cast.h>

namespace NFq {

TString FormatColumn(const TString& value);
TString FormatValue(Ydb::TypedValue value) ;
TString FormatNull(NYql::NPq::NProto::TExpression_TNull);
TString FormatExpression(NYql::NPq::NProto::TExpression expression);
TString FormatArithmeticalExpression(NYql::NPq::NProto::TExpression_TArithmeticalExpression expression);
TString FormatNegation(const NYql::NPq::NProto::TPredicate_TNegation& negation);
TString FormatComparison(NYql::NPq::NProto::TPredicate_TComparison comparison);
TString FormatConjunction(const NYql::NPq::NProto::TPredicate_TConjunction& conjunction, bool topLevel);
TString FormatDisjunction(const NYql::NPq::NProto::TPredicate_TDisjunction& disjunction);
TString FormatIsNull(NYql::NPq::NProto::TPredicate_TIsNull isNull);
TString FormatIsNotNull(NYql::NPq::NProto::TPredicate_TIsNotNull isNotNull);
TString FormatPredicate(const NYql::NPq::NProto::TPredicate& predicate, bool topLevel);

TString FormatColumn(const TString& value) {
    return value;
}

TString FormatValue(Ydb::TypedValue value) {
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

TString FormatExpression(NYql::NPq::NProto::TExpression expression) {
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

	auto left = FormatExpression(expression.left_value());
	auto right = FormatExpression(expression.right_value());
	return left + operation + right;
}

TString FormatNegation(const NYql::NPq::NProto::TPredicate_TNegation& negation) {
	auto pred = FormatPredicate(negation.operand(), false);
	return "(NOT " + pred + ")";
}

TString FormatConjunction(const NYql::NPq::NProto::TPredicate_TConjunction& conjunction, bool /*topLevel*/) {
	ui32 succeeded = 0;
	TStringStream stream;
	TString first;

	for (const auto& predicate : conjunction.operands()) {
		auto statement = FormatPredicate(predicate, false);

		if (succeeded > 0) {
			if (succeeded == 1) {
				stream << "(";
				stream << first;
			}
			stream << " AND ";
			stream << statement;
		} else {
			first = statement;
		}
		succeeded++;
	}

	if (succeeded == 0) {
		ythrow yexception() << "failed to format AND statement, no operands";
	}

	if (succeeded == 1) {
		stream << first;
	} else {
		stream << ")";
	}

	return stream.Str();
}

TString FormatDisjunction(const NYql::NPq::NProto::TPredicate_TDisjunction& disjunction) {
	TStringStream stream;
	TString first;
	ui32 cnt = 0;

	for (const auto& predicate : disjunction.operands()) {
		auto statement = FormatPredicate(predicate, false);

		if (cnt > 0) {
			if (cnt == 1) {
				stream << "(";
				stream << first;
			}

			stream << " OR ";
			stream << statement;
		} else {
			first = statement;
		}
		cnt++;
	}

	if (cnt == 0) {
		ythrow yexception() << "failed to format OR statement: no operands";
	}

	if (cnt == 1) {
		stream << first;
	} else {
		stream << ")";
	}

	return stream.Str();
}

TString FormatIsNull(NYql::NPq::NProto::TPredicate_TIsNull isNull) {
	auto statement = FormatExpression(isNull.value());
	return "(" + statement + " IS NULL)";
}

TString FormatIsNotNull(NYql::NPq::NProto::TPredicate_TIsNotNull isNotNull) {
	auto statement = FormatExpression(isNotNull.value());
	return "(" + statement + " IS NOT NULL)";
}

TString FormatComparison(NYql::NPq::NProto::TPredicate_TComparison comparison) {
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

	auto left = FormatExpression(comparison.left_value());
	auto right = FormatExpression(comparison.right_value());

	return left + operation + right;
}

TString FormatPredicate(const NYql::NPq::NProto::TPredicate& predicate, bool topLevel ) {
    switch (predicate.payload_case()) {
        case NYql::NPq::NProto::TPredicate::PAYLOAD_NOT_SET:
                return {};
        case NYql::NPq::NProto::TPredicate::kNegation:
    		return FormatNegation(predicate.negation());
        case NYql::NPq::NProto::TPredicate::kConjunction:
            return FormatConjunction(predicate.conjunction(), topLevel);
        case NYql::NPq::NProto::TPredicate::kDisjunction:
            return FormatDisjunction(predicate.disjunction());
        case NYql::NPq::NProto::TPredicate::kIsNull:
            return FormatIsNull(predicate.is_null());
        case NYql::NPq::NProto::TPredicate::kIsNotNull:
            return FormatIsNotNull(predicate.is_not_null());
        case NYql::NPq::NProto::TPredicate::kComparison:
            return FormatComparison(predicate.comparison());
        case NYql::NPq::NProto::TPredicate::kBoolExpression:
            return FormatExpression(predicate.bool_expression().value());
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
