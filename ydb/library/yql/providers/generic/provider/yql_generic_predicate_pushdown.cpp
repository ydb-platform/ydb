#include "yql_generic_predicate_pushdown.h"

#include <ydb/library/yql/providers/generic/connector/api/service/protos/connector.pb.h>
#include <ydb/core/fq/libs/common/util.h>
#include <util/string/cast.h>

namespace NYql {

    using namespace NNodes;
    using namespace NConnector::NApi;

    TString FormatColumn(const TString& value);
    TString FormatValue(const Ydb::TypedValue& value);
    TString FormatNull(const TExpression_TNull&);
    TString FormatExpression(const TExpression& expression);
    TString FormatArithmeticalExpression(const TExpression_TArithmeticalExpression& expression);
    TString FormatNegation(const TPredicate_TNegation& negation);
    TString FormatComparison(const TPredicate_TComparison comparison);
    TString FormatConjunction(const TPredicate_TConjunction& conjunction, bool topLevel);
    TString FormatDisjunction(const TPredicate_TDisjunction& disjunction);
    TString FormatIsNull(const TPredicate_TIsNull& isNull);
    TString FormatIsNotNull(const TPredicate_TIsNotNull& isNotNull);
    TString FormatPredicate(const TPredicate& predicate, bool topLevel);
    TString FormatIn(const TPredicate_TIn& in);

    namespace {

        bool SerializeMember(const TCoMember& member, TExpression* proto, const TCoArgument& arg, TStringBuilder& err) {
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
        exprProto->set_operation(TExpression::TArithmeticalExpression::OP_ENUM);                                                                                             \
        return SerializeExpression(expr.Left(), exprProto->mutable_left_value(), arg, err) && SerializeExpression(expr.Right(), exprProto->mutable_right_value(), arg, err); \
    }

        bool SerializeExpression(const TExprBase& expression, TExpression* proto, const TCoArgument& arg, TStringBuilder& err) {
            if (auto member = expression.Maybe<TCoMember>()) {
                return SerializeMember(member.Cast(), proto, arg, err);
            }

            // data
            MATCH_ATOM(Bool, BOOL, bool, bool);
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
            MATCH_ATOM(Timestamp, TIMESTAMP, int64, i64);
            MATCH_ARITHMETICAL(Sub, SUB);
            MATCH_ARITHMETICAL(Add, ADD);
            MATCH_ARITHMETICAL(Mul, MUL);
            MATCH_ARITHMETICAL(Div, DIV);
            MATCH_ARITHMETICAL(Mod, MOD);

            if (auto maybeNull = expression.Maybe<TCoNull>()) {
                proto->mutable_null();
                return true;
            }

            err << "unknown expression: " << expression.Raw()->Content();
            return false;
        }

#undef MATCH_ATOM
#undef MATCH_ARITHMETICAL

#define EXPR_NODE_TO_COMPARE_TYPE(TExprNodeType, COMPARE_TYPE)       \
    if (!opMatched && compare.Maybe<TExprNodeType>()) {              \
        opMatched = true;                                            \
        proto->set_operation(TPredicate::TComparison::COMPARE_TYPE); \
    }

        bool SerializeCompare(const TCoCompare& compare, TPredicate* predicateProto, const TCoArgument& arg, TStringBuilder& err) {
            TPredicate::TComparison* proto = predicateProto->mutable_comparison();
            bool opMatched = false;

            EXPR_NODE_TO_COMPARE_TYPE(TCoCmpEqual, EQ);
            EXPR_NODE_TO_COMPARE_TYPE(TCoCmpNotEqual, NE);
            EXPR_NODE_TO_COMPARE_TYPE(TCoCmpLess, L);
            EXPR_NODE_TO_COMPARE_TYPE(TCoCmpLessOrEqual, LE);
            EXPR_NODE_TO_COMPARE_TYPE(TCoCmpGreater, G);
            EXPR_NODE_TO_COMPARE_TYPE(TCoCmpGreaterOrEqual, GE);
            EXPR_NODE_TO_COMPARE_TYPE(TCoAggrEqual, IND);
            EXPR_NODE_TO_COMPARE_TYPE(TCoAggrNotEqual, ID);

            if (proto->operation() == TPredicate::TComparison::COMPARISON_OPERATION_UNSPECIFIED) {
                err << "unknown compare operation: " << compare.Raw()->Content();
                return false;
            }
            return SerializeExpression(compare.Left(), proto->mutable_left_value(), arg, err) && SerializeExpression(compare.Right(), proto->mutable_right_value(), arg, err);
        }

#undef EXPR_NODE_TO_COMPARE_TYPE

        bool SerializePredicate(const TExprBase& predicate, TPredicate* proto, const TCoArgument& arg, TStringBuilder& err, ui64 depth);

        bool SerializeSqlIf(const TCoIf& sqlIf, TPredicate* proto, const TCoArgument& arg, TStringBuilder& err, ui64 depth) {
            auto* dstProto = proto->mutable_if_();
            return SerializePredicate(TExprBase(sqlIf.Predicate()), dstProto->mutable_predicate(), arg, err, depth + 1)
                && SerializePredicate(TExprBase(sqlIf.ThenValue()), dstProto->mutable_then_predicate(), arg, err, depth + 1)
                && SerializePredicate(TExprBase(sqlIf.ElseValue()), dstProto->mutable_else_predicate(), arg, err, depth + 1);
        }

        bool SerializeCoalesce(const TCoCoalesce& coalesce, TPredicate* proto, const TCoArgument& arg, TStringBuilder& err, ui64 depth) {
            // Special case for top level COALESCE: COALESCE(Predicat, FALSE)
            // We can assume NULL as FALSE and skip COALESCE
            if (depth == 0) {
                auto value = coalesce.Value().Maybe<TCoBool>();
                if (value && TStringBuf(value.Cast().Literal()) == "false"sv) {
                    return SerializePredicate(TExprBase(coalesce.Predicate()), proto, arg, err, 0);
                }
            }

            auto* dstProto = proto->mutable_coalesce();
            for (const auto& child : coalesce.Ptr()->Children()) {
                if (!SerializePredicate(TExprBase(child), dstProto->add_operands(), arg, err, depth + 1)) {
                    return false;
                }

                // We can unwrap nested COALESCE:
                // COALESCE(..., COALESCE(Predicat_1, Predicat_2), ...) -> COALESCE(..., Predicat_1, Predicat_2, ...)
                if (dstProto->operands().rbegin()->has_coalesce()) {
                    auto coalesceOperands = std::move(*dstProto->mutable_operands()->rbegin()->mutable_coalesce()->mutable_operands());
                    dstProto->mutable_operands()->RemoveLast();
                    dstProto->mutable_operands()->Add(coalesceOperands.begin(), coalesceOperands.end());
                }
            }
            return true;
        }

        bool SerializeExists(const TCoExists& exists, TPredicate* proto, const TCoArgument& arg, TStringBuilder& err, bool withNot = false) {
            auto* expressionProto = withNot ? proto->mutable_is_null()->mutable_value() : proto->mutable_is_not_null()->mutable_value();
            return SerializeExpression(exists.Optional(), expressionProto, arg, err);
        }

        bool SerializeSqlIn(const TCoSqlIn& sqlIn, TPredicate* proto, const TCoArgument& arg, TStringBuilder& err) {
            auto* dstProto = proto->mutable_in();
            const TExprBase& expr = sqlIn.Collection();
            const TExprBase& lookup = sqlIn.Lookup();

            auto* expressionProto = dstProto->mutable_value();
            SerializeExpression(lookup, expressionProto, arg, err);

            TExprNode::TPtr collection;
            if (expr.Ref().IsList()) {
                collection = expr.Ptr();
            } else if (auto maybeAsList = expr.Maybe<TCoAsList>()) {
                collection = maybeAsList.Cast().Ptr();
            } else {
                err << "unknown source for in: " << expr.Ref().Content();
                return false;
            }

            for (auto& child : collection->Children()) {
                if (!SerializeExpression(TExprBase(child), dstProto->add_set(), arg, err)) {
                    return false;
                }
            }
            return true;
        }

        bool SerializeIsNotDistinctFrom(const TExprBase& predicate, TPredicate* predicateProto, const TCoArgument& arg, TStringBuilder& err, bool invert) {
            if (predicate.Ref().ChildrenSize() != 2) {
                err << "invalid IsNotDistinctFrom predicate, expected 2 children but got " << predicate.Ref().ChildrenSize();
                return false;
            }
            TPredicate::TComparison* proto = predicateProto->mutable_comparison();
            proto->set_operation(!invert ? TPredicate::TComparison::IND : TPredicate::TComparison::ID);
            return SerializeExpression(TExprBase(predicate.Ref().Child(0)), proto->mutable_left_value(), arg, err)
                && SerializeExpression(TExprBase(predicate.Ref().Child(1)), proto->mutable_right_value(), arg, err);
        }

        bool SerializeAnd(const TCoAnd& andExpr, TPredicate* proto, const TCoArgument& arg, TStringBuilder& err, ui64 depth) {
            auto* dstProto = proto->mutable_conjunction();
            for (const auto& child : andExpr.Ptr()->Children()) {
                if (!SerializePredicate(TExprBase(child), dstProto->add_operands(), arg, err, depth + 1)) {
                    return false;
                }
            }
            return true;
        }

        bool SerializeOr(const TCoOr& orExpr, TPredicate* proto, const TCoArgument& arg, TStringBuilder& err, ui64 depth) {
            auto* dstProto = proto->mutable_disjunction();
            for (const auto& child : orExpr.Ptr()->Children()) {
                if (!SerializePredicate(TExprBase(child), dstProto->add_operands(), arg, err, depth + 1)) {
                    return false;
                }
            }
            return true;
        }

        bool SerializeNot(const TCoNot& notExpr, TPredicate* proto, const TCoArgument& arg, TStringBuilder& err, ui64 depth) {
            // Special case: (Not (Exists ...))
            if (auto exists = notExpr.Value().Maybe<TCoExists>()) {
                return SerializeExists(exists.Cast(), proto, arg, err, true);
            }
            auto* dstProto = proto->mutable_negation();
            return SerializePredicate(notExpr.Value(), dstProto->mutable_operand(), arg, err, depth + 1);
        }

        bool SerializeMember(const TCoMember& member, TPredicate* proto, const TCoArgument& arg, TStringBuilder& err) {
            return SerializeMember(member, proto->mutable_bool_expression()->mutable_value(), arg, err);
        }

        bool SerializePredicate(const TExprBase& predicate, TPredicate* proto, const TCoArgument& arg, TStringBuilder& err, ui64 depth) {
            if (auto compare = predicate.Maybe<TCoCompare>()) {
                return SerializeCompare(compare.Cast(), proto, arg, err);
            }
            if (auto coalesce = predicate.Maybe<TCoCoalesce>()) {
                return SerializeCoalesce(coalesce.Cast(), proto, arg, err, depth);
            }
            if (auto andExpr = predicate.Maybe<TCoAnd>()) {
                return SerializeAnd(andExpr.Cast(), proto, arg, err, depth);
            }
            if (auto orExpr = predicate.Maybe<TCoOr>()) {
                return SerializeOr(orExpr.Cast(), proto, arg, err, depth);
            }
            if (auto notExpr = predicate.Maybe<TCoNot>()) {
                return SerializeNot(notExpr.Cast(), proto, arg, err, depth);
            }
            if (auto member = predicate.Maybe<TCoMember>()) {
                return SerializeMember(member.Cast(), proto, arg, err);
            }
            if (auto exists = predicate.Maybe<TCoExists>()) {
                return SerializeExists(exists.Cast(), proto, arg, err);
            }
            if (auto sqlIn = predicate.Maybe<TCoSqlIn>()) {
                return SerializeSqlIn(sqlIn.Cast(), proto, arg, err);
            }
            if (predicate.Ref().IsCallable("IsNotDistinctFrom")) {
                return SerializeIsNotDistinctFrom(predicate, proto, arg, err, false);
            }
            if (predicate.Ref().IsCallable("IsDistinctFrom")) {
                return SerializeIsNotDistinctFrom(predicate, proto, arg, err, true);
            }
            if (auto sqlIf = predicate.Maybe<TCoIf>()) {
                return SerializeSqlIf(sqlIf.Cast(), proto, arg, err, depth);
            }
            if (auto just = predicate.Maybe<TCoJust>()) {
                return SerializePredicate(TExprBase(just.Cast().Input()), proto, arg, err, depth + 1);
            }

            // Try to serialize predicate as boolean expression
            // For example single bool value TRUE in COALESCE or IF
            return SerializeExpression(predicate, proto->mutable_bool_expression()->mutable_value(), arg, err);
        }
    }

    TString FormatColumn(const TString& value) {
        return NFq::EncloseAndEscapeString(value, '`');
    }

    TString FormatValue(const Ydb::TypedValue& value) {
        switch (value.value().value_case()) {
            case  Ydb::Value::kBoolValue:
                return value.value().bool_value() ? "TRUE" : "FALSE";
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
                return NFq::EncloseAndEscapeString(value.value().bytes_value(), '"');
            case Ydb::Value::kTextValue:
                return NFq::EncloseAndEscapeString(value.value().text_value(), '"');
            default:
                throw yexception() << "ErrUnimplementedTypedValue, value case " << static_cast<ui64>(value.value().value_case());
        }
    }

    TString FormatNull(const TExpression_TNull&) {
        return "NULL";
    }

    TString FormatExpression(const TExpression& expression) {
        switch (expression.payload_case()) {
            case TExpression::kColumn:
                return FormatColumn(expression.column());
            case TExpression::kTypedValue:
                return FormatValue(expression.typed_value());
            case TExpression::kArithmeticalExpression:
                return FormatArithmeticalExpression(expression.arithmetical_expression());
            case TExpression::kNull:
                return FormatNull(expression.null());
            default:
                throw yexception() << "UnimplementedExpression, payload_case " << static_cast<ui64>(expression.payload_case());
        }
    }

    TString FormatArithmeticalExpression(const TExpression_TArithmeticalExpression& expression) {
        TString operation;
        switch (expression.operation()) {
            case TExpression_TArithmeticalExpression::MUL:
                operation = " * ";
                break;
            case TExpression_TArithmeticalExpression::ADD:
                operation = " + ";
                break;
            case TExpression_TArithmeticalExpression::SUB:
                operation = " - ";
                break;
            case TExpression_TArithmeticalExpression::DIV:
                operation = " / ";
                break;
            case TExpression_TArithmeticalExpression::MOD:
                operation = " % ";
                break;
            case TExpression_TArithmeticalExpression::BIT_AND:
                operation = " & ";
                break;
            case TExpression_TArithmeticalExpression::BIT_OR:
                operation = " | ";
                break;
            case TExpression_TArithmeticalExpression::BIT_XOR:
                operation = " ^ ";
                break;
            default:
                throw yexception() << "ErrUnimplementedArithmeticalExpression, operation " << static_cast<ui64>(expression.operation());
        }

        auto left = FormatExpression(expression.left_value());
        auto right = FormatExpression(expression.right_value());
        return TStringBuilder() << "(" << left << operation << right << ")";
    }

    TString FormatNegation(const TPredicate_TNegation& negation) {
        auto pred = FormatPredicate(negation.operand(), false);
        return "(NOT " + pred + ")";
    }

    TString FormatConjunction(const TPredicate_TConjunction& conjunction, bool /*topLevel*/) {
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
            throw yexception() << "failed to format AND statement, no operands";
        }

        if (succeeded == 1) {
            stream << first;
        } else {
            stream << ")";
        }
        return stream.Str();
    }

    TString FormatDisjunction(const TPredicate_TDisjunction& disjunction) {
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
            throw yexception() << "failed to format OR statement: no operands";
        }

        if (cnt == 1) {
            stream << first;
        } else {
            stream << ")";
        }

        return stream.Str();
    }

    TString FormatCoalesce(const TPredicate::TCoalesce& coalesce) {
        TStringStream stream;
        TString first;
        ui32 cnt = 0;

        for (const auto& predicate : coalesce.operands()) {
            auto statement = FormatPredicate(predicate, false);

            if (cnt > 0) {
                if (cnt == 1) {
                    stream << "COALESCE(";
                    stream << first;
                }

                stream << ", ";
                stream << statement;
            } else {
                first = statement;
            }
            cnt++;
        }

        if (cnt == 0) {
            throw yexception() << "failed to format COALESCE statement: no operands";
        }

        if (cnt == 1) {
            stream << first;
        } else {
            stream << ")";
        }

        return stream.Str();
    }

    TString FormatIf(const TPredicate::TIf& sqlIf) {
        TStringStream stream;
        stream << "IF(" << FormatPredicate(sqlIf.predicate(), false);
        stream << ", " << FormatPredicate(sqlIf.then_predicate(), false);
        stream << ", " << FormatPredicate(sqlIf.else_predicate(), false) << ")";
        return stream.Str();
    }

    TString FormatIsNull(const TPredicate_TIsNull& isNull) {
        auto statement = FormatExpression(isNull.value());
        return "(" + statement + " IS NULL)";
    }

    TString FormatIsNotNull(const TPredicate_TIsNotNull& isNotNull) {
        auto statement = FormatExpression(isNotNull.value());
        return "(" + statement + " IS NOT NULL)";
    }

    TString FormatComparison(TPredicate_TComparison comparison) {
        TString operation;

        switch (comparison.operation()) {
        case TPredicate_TComparison::L:
            operation = " < ";
            break;
        case TPredicate_TComparison::LE:
            operation = " <= ";
            break;
        case TPredicate_TComparison::EQ:
            operation = " = ";
            break;
        case TPredicate_TComparison::NE:
            operation = " <> ";
            break;
        case TPredicate_TComparison::GE:
            operation = " >= ";
            break;
        case TPredicate_TComparison::G:
            operation = " > ";
            break;
        case TPredicate_TComparison::IND:
            operation = " IS NOT DISTINCT FROM ";
            break;
        case TPredicate_TComparison::ID:
            operation = " IS DISTINCT FROM ";
            break;
        default:
            throw yexception() << "UnimplementedOperation, operation " << static_cast<ui64>(comparison.operation());
        }

        auto left = FormatExpression(comparison.left_value());
        auto right = FormatExpression(comparison.right_value());

        return left + operation + right;
    }

    TString FormatIn(const TPredicate_TIn& in) {
        auto value = FormatExpression(in.value());
        TStringStream list;
        for (const auto& expr : in.set()) {
            if (!list.empty()) {
                list << ", ";
            } else {
                list << value << " IN (";
            }
            list << FormatExpression(expr);
        }

        if (list.empty()) {
            throw yexception() << "failed to format IN statement, no operands";
        }

        list << ")";
        return list.Str();
    }

    TString FormatPredicate(const TPredicate& predicate, bool topLevel ) {
        switch (predicate.payload_case()) {
            case TPredicate::PAYLOAD_NOT_SET:
                return {};
            case TPredicate::kNegation:
                return FormatNegation(predicate.negation());
            case TPredicate::kConjunction:
                return FormatConjunction(predicate.conjunction(), topLevel);
            case TPredicate::kDisjunction:
                return FormatDisjunction(predicate.disjunction());
            case TPredicate::kCoalesce:
                return FormatCoalesce(predicate.coalesce());
            case TPredicate::kIf:
                return FormatIf(predicate.if_());
            case TPredicate::kIsNull:
                return FormatIsNull(predicate.is_null());
            case TPredicate::kIsNotNull:
                return FormatIsNotNull(predicate.is_not_null());
            case TPredicate::kComparison:
                return FormatComparison(predicate.comparison());
            case TPredicate::kBoolExpression:
                return FormatExpression(predicate.bool_expression().value());
            case TPredicate::kIn:
                return FormatIn(predicate.in());
            default:
                throw yexception() << "UnimplementedPredicateType, payload_case " << static_cast<ui64>(predicate.payload_case());
        }
    }

    bool IsEmptyFilterPredicate(const TCoLambda& lambda) {
        auto maybeBool = lambda.Body().Maybe<TCoBool>();
        if (!maybeBool) {
            return false;
        }
        return TStringBuf(maybeBool.Cast().Literal()) == "true"sv;
    }

    bool SerializeFilterPredicate(const TCoLambda& predicate, TPredicate* proto, TStringBuilder& err) {
        return SerializePredicate(predicate.Body(), proto, predicate.Args().Arg(0), err, 0);
    }

    TString FormatWhere(const TPredicate& predicate) {
        auto stream = FormatPredicate(predicate, true);
        if (stream.empty()) {
            return "";
        }
        return "WHERE " + stream;
    }
} // namespace NYql
