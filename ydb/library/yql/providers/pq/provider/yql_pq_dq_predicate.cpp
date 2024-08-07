#include "yql_pq_dq_predicate.h"
#include "yql_pq_helpers.h"
#include "yql_pq_mkql_compiler.h"

#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/providers/pq/expr_nodes/yql_pq_expr_nodes.h>
#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>
#include <ydb/library/yql/providers/common/dq/yql_dq_integration_impl.h>
#include <ydb/library/yql/providers/dq/common/yql_dq_settings.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <ydb/library/yql/providers/pq/common/pq_meta_fields.h>
#include <ydb/library/yql/providers/pq/common/yql_names.h>
#include <ydb/library/yql/providers/pq/expr_nodes/yql_pq_expr_nodes.h>
#include <ydb/library/yql/providers/pq/proto/dq_io.pb.h>
#include <ydb/library/yql/providers/pq/proto/dq_task_params.pb.h>

#include <ydb/library/yql/utils/log/log.h>

#include <util/string/builder.h>

namespace NYql::NDqPredicate {

using namespace NNodes;

namespace {

bool SerializePredicate(const TExprBase& predicate, NPq::NProto::TPredicate* proto, const TCoArgument& arg, TStringBuilder& err);

bool SerializeMember(const TCoMember& member, NPq::NProto::TExpression* proto, const TCoArgument& arg, TStringBuilder& err) {
    if (member.Struct().Raw() != arg.Raw()) { // member callable called not for lambda argument
        err << "member callable called not for lambda argument";
        return false;
    }
    proto->set_column(member.Name().StringValue());
    return true;
}

template <class T>
T Cast(const TStringBuf& from) {
    return FromString<T>(from);
}

// Special convertation from TStringBuf to TString
template <>
TString Cast<TString>(const TStringBuf& from) {
    return TString(from);
}

#define MATCH_ATOM(AtomType, ATOM_ENUM, proto_name, cpp_type)                             \
if (auto atom = expression.Maybe<Y_CAT(TCo, AtomType)>()) {                           \
    auto* value = proto->mutable_typed_value();                                       \
    auto* t = value->mutable_type();                                                  \
    t->set_type_id(Ydb::Type::ATOM_ENUM);                                             \
    auto* v = value->mutable_value();                                                 \
    v->Y_CAT(Y_CAT(set_, proto_name), _value)(Cast<cpp_type>(atom.Cast().Literal())); \
    return true;                                                                      \
}

#define MATCH_ARITHMETICAL(OpType, OP_ENUM)                                                                                                                                  \
if (auto maybeExpr = expression.Maybe<Y_CAT(TCo, OpType)>()) {                                                                                                           \
    auto expr = maybeExpr.Cast();                                                                                                                                        \
    auto* exprProto = proto->mutable_arithmetical_expression();                                                                                                          \
    exprProto->set_operation(NPq::NProto::TExpression::TArithmeticalExpression::OP_ENUM);                                                                                             \
    return SerializeExpression(expr.Left(), exprProto->mutable_left_value(), arg, err) && SerializeExpression(expr.Right(), exprProto->mutable_right_value(), arg, err); \
}

bool SerializeExpression(const TExprBase& expression, NPq::NProto::TExpression* proto, const TCoArgument& arg, TStringBuilder& err) {
    if (auto member = expression.Maybe<TCoMember>()) {
        return SerializeMember(member.Cast(), proto, arg, err);
    }

    // data
    MATCH_ATOM(Int8, INT8, int32, i8);
    MATCH_ATOM(Uint8, UINT8, uint32, ui8);
    MATCH_ATOM(Int16, INT16, int32, i16);
    MATCH_ATOM(Uint16, UINT16, uint32, ui16);
    MATCH_ATOM(Int32, INT32, int32, i32);
    MATCH_ATOM(Uint32, UINT32, uint32, ui32);
    MATCH_ATOM(Int64, INT64, int64, i64);
    MATCH_ATOM(Uint64, UINT64, uint64, ui64);
    MATCH_ATOM(Float, FLOAT, float, float);
    MATCH_ATOM(Double, DOUBLE, double, double);
    MATCH_ATOM(String, STRING, bytes, TString);
    MATCH_ATOM(Utf8, UTF8, text, TString);
    MATCH_ARITHMETICAL(Sub, SUB);
    MATCH_ARITHMETICAL(Add, ADD);
    MATCH_ARITHMETICAL(Mul, MUL);

    if (auto maybeNull = expression.Maybe<TCoNull>()) {
        proto->mutable_null();
        return true;
    }

    err << "unknown expression: " << expression.Raw()->Content();
    return false;
}

#undef MATCH_ATOM

#define EXPR_NODE_TO_COMPARE_TYPE(TExprNodeType, COMPARE_TYPE)       \
if (!opMatched && compare.Maybe<TExprNodeType>()) {              \
    opMatched = true;                                            \
    proto->set_operation(NPq::NProto::TPredicate::TComparison::COMPARE_TYPE); \
}

bool SerializeCompare(const TCoCompare& compare, NPq::NProto::TPredicate* predicateProto, const TCoArgument& arg, TStringBuilder& err) {
    NPq::NProto::TPredicate::TComparison* proto = predicateProto->mutable_comparison();
    bool opMatched = false;

    EXPR_NODE_TO_COMPARE_TYPE(TCoCmpEqual, EQ);
    EXPR_NODE_TO_COMPARE_TYPE(TCoCmpNotEqual, NE);
    EXPR_NODE_TO_COMPARE_TYPE(TCoCmpLess, L);
    EXPR_NODE_TO_COMPARE_TYPE(TCoCmpLessOrEqual, LE);
    EXPR_NODE_TO_COMPARE_TYPE(TCoCmpGreater, G);
    EXPR_NODE_TO_COMPARE_TYPE(TCoCmpGreaterOrEqual, GE);

    if (proto->operation() == NPq::NProto::TPredicate::TComparison::COMPARISON_OPERATION_UNSPECIFIED) {
        err << "unknown operation: " << compare.Raw()->Content();
        return false;
    }
    return SerializeExpression(compare.Left(), proto->mutable_left_value(), arg, err) && SerializeExpression(compare.Right(), proto->mutable_right_value(), arg, err);
}

#undef EXPR_NODE_TO_COMPARE_TYPE

bool SerializeCoalesce(const TCoCoalesce& coalesce, NPq::NProto::TPredicate* proto, const TCoArgument& arg, TStringBuilder& err) {
    auto predicate = coalesce.Predicate();
    if (auto compare = predicate.Maybe<TCoCompare>()) {
        return SerializeCompare(compare.Cast(), proto, arg, err);
    }

    err << "unknown coalesce predicate: " << predicate.Raw()->Content();
    return false;
}


bool SerializeExists(const TCoExists& exists, NPq::NProto::TPredicate* proto, const TCoArgument& arg, TStringBuilder& err, bool withNot = false) {
    auto* expressionProto = withNot ? proto->mutable_is_null()->mutable_value() : proto->mutable_is_not_null()->mutable_value();
    return SerializeExpression(exists.Optional(), expressionProto, arg, err);
}

bool SerializeAnd(const TCoAnd& andExpr, NPq::NProto::TPredicate* proto, const TCoArgument& arg, TStringBuilder& err) {
    auto* dstProto = proto->mutable_conjunction();
    for (const auto& child : andExpr.Ptr()->Children()) {
        if (!SerializePredicate(TExprBase(child), dstProto->add_operands(), arg, err)) {
            return false;
        }
    }
    return true;
}

bool SerializeOr(const TCoOr& orExpr, NPq::NProto::TPredicate* proto, const TCoArgument& arg, TStringBuilder& err) {
    auto* dstProto = proto->mutable_disjunction();
    for (const auto& child : orExpr.Ptr()->Children()) {
        if (!SerializePredicate(TExprBase(child), dstProto->add_operands(), arg, err)) {
            return false;
        }
    }
    return true;
}

bool SerializeNot(const TCoNot& notExpr, NPq::NProto::TPredicate* proto, const TCoArgument& arg, TStringBuilder& err) {
    // Special case: (Not (Exists ...))
    if (auto exists = notExpr.Value().Maybe<TCoExists>()) {
        return SerializeExists(exists.Cast(), proto, arg, err, true);
    }
    auto* dstProto = proto->mutable_negation();
    return SerializePredicate(notExpr.Value(), dstProto->mutable_operand(), arg, err);
}

bool SerializeMember(const TCoMember& member, NPq::NProto::TPredicate* proto, const TCoArgument& arg, TStringBuilder& err) {
    return SerializeMember(member, proto->mutable_bool_expression()->mutable_value(), arg, err);
}

bool SerializePredicate(const TExprBase& predicate, NPq::NProto::TPredicate* proto, const TCoArgument& arg, TStringBuilder& err) {
    if (auto compare = predicate.Maybe<TCoCompare>()) {
        return SerializeCompare(compare.Cast(), proto, arg, err);
    }
    if (auto coalesce = predicate.Maybe<TCoCoalesce>()) {
        return SerializeCoalesce(coalesce.Cast(), proto, arg, err);
    }
    if (auto andExpr = predicate.Maybe<TCoAnd>()) {
        return SerializeAnd(andExpr.Cast(), proto, arg, err);
    }
    if (auto orExpr = predicate.Maybe<TCoOr>()) {
        return SerializeOr(orExpr.Cast(), proto, arg, err);
    }
    if (auto notExpr = predicate.Maybe<TCoNot>()) {
        return SerializeNot(notExpr.Cast(), proto, arg, err);
    }
    if (auto member = predicate.Maybe<TCoMember>()) {
        return SerializeMember(member.Cast(), proto, arg, err);
    }
    if (auto exists = predicate.Maybe<TCoExists>()) {
        return SerializeExists(exists.Cast(), proto, arg, err);
    }

    err << "unknown predicate: " << predicate.Raw()->Content();
    return false;
}


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

}

bool IsEmptyFilterPredicate(const TCoLambda& lambda) {
    auto maybeBool = lambda.Body().Maybe<TCoBool>();
    if (!maybeBool) {
        return false;
    }
    return TStringBuf(maybeBool.Cast().Literal()) == "true"sv;
}

bool SerializeFilterPredicate(const TCoLambda& predicate, NPq::NProto::TPredicate* proto, TStringBuilder& err) {
    return SerializePredicate(predicate.Body(), proto, predicate.Args().Arg(0), err);
}

TString FormatWhere(const NYql::NPq::NProto::TPredicate& predicate) {
    auto stream = FormatPredicate(predicate, true);
	if (stream.empty()) {
		return "";
	}
    return "WHERE " + stream;
}

}
