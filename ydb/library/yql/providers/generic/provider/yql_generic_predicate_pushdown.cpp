#include "yql_generic_predicate_pushdown.h"

#include <ydb/library/yql/providers/generic/connector/api/service/protos/connector.pb.h>
#include <ydb/core/fq/libs/common/util.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <util/string/cast.h>

namespace NYql {

    using namespace NNodes;
    using namespace NConnector::NApi;

    TString FormatColumn(const TString& value);
    TString FormatType(const Ydb::Type& type);
    TString FormatTypedValue(const Ydb::TypedValue& typedValue);
    TString FormatValue(const Ydb::Value& value);
    TString FormatNull(const TExpression_TNull&);
    TString FormatExpression(const TExpression& expression);
    TString FormatArithmeticalExpression(const TExpression_TArithmeticalExpression& expression);
    TString FormatNegation(const TPredicate_TNegation& negation);
    TString FormatComparison(const TPredicate_TComparison comparison);
    TString FormatConjunction(const TPredicate_TConjunction& conjunction);
    TString FormatDisjunction(const TPredicate_TDisjunction& disjunction);
    TString FormatIsNull(const TPredicate_TIsNull& isNull);
    TString FormatIsNotNull(const TPredicate_TIsNotNull& isNotNull);
    TString FormatPredicate(const TPredicate& predicate);
    TString FormatIn(const TPredicate_TIn& in);
    TString FormatCoalesce(const TExpression::TCoalesce& coalesce);
    TString FormatIfExpression(const TExpression::TIf& sqlIf);
    TString FormatUnaryOperation(const TExpression::TUnaryOperation& expression);
    TString FormatVarArgOperation(const TExpression::TVarArgOperation& expression);

    namespace {
        TString ToIso8601(TDuration duration) {
            if (duration == TDuration::Zero()) {
                return "PT0S";
            }
            TStringBuilder result;
            result << "PT" << duration.Seconds();
            auto fraction = duration.MicroSecondsOfSecond();
            if (fraction != 0) {
                result << ".";
                result << Sprintf("%06d", fraction);
            }
            result << "S";
            return result;
        }

        struct TSerializationContext {
            const TCoArgument& Arg;
            TStringBuilder& Err;
            std::unordered_map<const TExprNode*, TExpression> LambdaArgs = {};
            TExprContext& Ctx;
        };

        bool SerializeMember(const TCoMember& member, TExpression* proto, TSerializationContext& ctx) {
            if (member.Struct().Raw() != ctx.Arg.Raw()) { // member callable called not for lambda argument
                ctx.Err << "member callable called not for lambda argument";
                return false;
            }
            proto->set_column(member.Name().StringValue());
            return true;
        }

        bool SerializeLambdaArgument(const TExprBase& node, TExpression* proto, TSerializationContext& ctx) {
            const auto it = ctx.LambdaArgs.find(node.Raw());
            if (it == ctx.LambdaArgs.end()) { // node is not lambda argument
                return false;
            }
            *proto = it->second;
            return true;
        }

        template <class T>
        T Cast(const TStringBuf& from) {
            return FromString<T>(from);
        }

        // Special conversion from TStringBuf to TString
        template <>
        TString Cast<TString>(const TStringBuf& from) {
            return TString(from);
        }

        bool SerializeExpression(const TExprBase& expression, TExpression* proto, TSerializationContext& ctx, ui64 depth);

#define MATCH_TYPE(DataType, PROTO_TYPE)                                                  \
    if (dataSlot == NUdf::EDataSlot::DataType) {                                          \
        dstType->set_type_id(Ydb::Type::PROTO_TYPE);                                      \
        return true;                                                                      \
    }

        bool SerializeCastExpression(const TCoSafeCast& safeCast, TExpression* proto, TSerializationContext& ctx, ui64 depth) {
            const auto typeAnnotation = safeCast.Type().Ref().GetTypeAnn();
            if (!typeAnnotation) {
                ctx.Err << "expected non empty type annotation for safe cast";
                return false;
            }
            if (typeAnnotation->GetKind() != ETypeAnnotationKind::Type) {
                ctx.Err << "expected only type expression for safe cast";
                return false;
            }

            auto* dstProto = proto->mutable_cast();
            if (!SerializeExpression(safeCast.Value(), dstProto->mutable_value(), ctx, depth + 1)) {
                return false;
            }

            auto type = typeAnnotation->Cast<TTypeExprType>()->GetType();
            auto* dstType = dstProto->mutable_type();
            if (type->GetKind() == ETypeAnnotationKind::Optional) {
                type = type->Cast<TOptionalExprType>()->GetItemType();
                dstType = dstType->mutable_optional_type()->mutable_item();
            }
            if (type->GetKind() != ETypeAnnotationKind::Data) {
                ctx.Err << "expected only data type or optional data type for safe cast";
                return false;
            }
            const auto dataSlot = type->Cast<TDataExprType>()->GetSlot();

            MATCH_TYPE(Bool, BOOL);
            MATCH_TYPE(Int8, INT8);
            MATCH_TYPE(Int16, INT16);
            MATCH_TYPE(Int32, INT32);
            MATCH_TYPE(Int64, INT64);
            MATCH_TYPE(Uint8, UINT8);
            MATCH_TYPE(Uint16, UINT16);
            MATCH_TYPE(Uint32, UINT32);
            MATCH_TYPE(Uint64, UINT64);
            MATCH_TYPE(Float, FLOAT);
            MATCH_TYPE(Double, DOUBLE);
            MATCH_TYPE(String, STRING);
            MATCH_TYPE(Utf8, UTF8);
            MATCH_TYPE(Json, JSON);
            MATCH_TYPE(Timestamp, TIMESTAMP);
            MATCH_TYPE(Interval, INTERVAL);

            ctx.Err << "unknown data slot " << static_cast<ui64>(dataSlot) << " for safe cast";
            return false;
        }

#undef MATCH_TYPE

        bool SerializeToBytesExpression(const TExprBase& toBytes, TExpression* proto, TSerializationContext& ctx, ui64 depth) {
            if (toBytes.Ref().ChildrenSize() != 1) {
                ctx.Err << "invalid ToBytes expression, expected 1 child but got " << toBytes.Ref().ChildrenSize();
                return false;
            }

            const auto toBytesExpr = TExprBase(toBytes.Ref().Child(0));
            auto typeAnnotation = toBytesExpr.Ref().GetTypeAnn();
            if (!typeAnnotation) {
                ctx.Err << "expected non empty type annotation for ToBytes";
                return false;
            }
            if (typeAnnotation->GetKind() == ETypeAnnotationKind::Optional) {
                typeAnnotation = typeAnnotation->Cast<TOptionalExprType>()->GetItemType();
            }
            if (typeAnnotation->GetKind() != ETypeAnnotationKind::Data) {
                ctx.Err << "expected data type or optional from data type in ToBytes";
                return false;
            }

            const auto dataSlot = typeAnnotation->Cast<TDataExprType>()->GetSlot();
            if (!IsDataTypeString(dataSlot) && dataSlot != NUdf::EDataSlot::JsonDocument) {
                ctx.Err << "expected only string like input type for ToBytes";
                return false;
            }

            auto* dstProto = proto->mutable_cast();
            dstProto->mutable_type()->set_type_id(Ydb::Type::STRING);
            return SerializeExpression(toBytesExpr, dstProto->mutable_value(), ctx, depth + 1);
        }

        bool SerializeToStringExpression(const TExprBase& toString, TExpression* proto, TSerializationContext& ctx, ui64 depth) {
            if (toString.Ref().ChildrenSize() != 1) {
                ctx.Err << "invalid ToString expression, expected 1 child but got " << toString.Ref().ChildrenSize();
                return false;
            }

            const auto toStringExpr = TExprBase(toString.Ref().Child(0));
            auto typeAnnotation = toStringExpr.Ref().GetTypeAnn();
            if (!typeAnnotation) {
                ctx.Err << "expected non empty type annotation for ToString";
                return false;
            }

            if (typeAnnotation->GetKind() == ETypeAnnotationKind::Optional) {
                typeAnnotation = typeAnnotation->Cast<TOptionalExprType>()->GetItemType();
            }

            if (typeAnnotation->GetKind() != ETypeAnnotationKind::Data) {
                ctx.Err << "expected data type or optional from data type in ToString";
                return false;
            }

            const auto dataSlot = typeAnnotation->Cast<TDataExprType>()->GetSlot();
            if (!IsDataTypeString(dataSlot) && dataSlot != NUdf::EDataSlot::JsonDocument) {
                ctx.Err << "expected only string like input type for ToString";
                return false;
            }

            auto* dstProto = proto->mutable_cast();
            dstProto->mutable_type()->set_type_id(Ydb::Type::STRING);
            return SerializeExpression(toStringExpr, dstProto->mutable_value(), ctx, depth + 1);
        }

        bool SerializeFlatMap(const TCoFlatMap& flatMap, TExpression* proto, TSerializationContext& ctx, ui64 depth) {
            const auto lambda = flatMap.Lambda();
            const auto lambdaArgs = lambda.Args();
            if (lambdaArgs.Size() != 1) {
                ctx.Err << "expected only one argument for flat map lambda";
                return false;
            }

            auto* dstProto = proto->mutable_if_();
            dstProto->mutable_else_expression()->mutable_null();
            auto* dstInput = dstProto->mutable_predicate()->mutable_is_not_null()->mutable_value();
            if (!SerializeExpression(flatMap.Input(), dstInput, ctx, depth + 1)) {
                return false;
            }

            // Duplicated arguments is ok, maybe one lambda was used twice
            ctx.LambdaArgs.insert({lambdaArgs.Ref().Child(0), *dstInput});
            return SerializeExpression(lambda.Body(), dstProto->mutable_then_expression(), ctx, depth + 1);
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
        return SerializeExpression(expr.Left(), exprProto->mutable_left_value(), ctx, depth + 1) && SerializeExpression(expr.Right(), exprProto->mutable_right_value(), ctx, depth + 1); \
    }

#define MATCH_UNARY_OP(OpType, OP_ENUM)                                                             \
    if (auto maybeExpr = expression.Maybe<Y_CAT(TCo, OpType)>()) {                                  \
        auto expr = maybeExpr.Cast();                                                               \
        auto* exprProto = proto->mutable_unary_op();                                                \
        exprProto->set_operation(TExpression::TUnaryOperation::OP_ENUM);                            \
        const auto child = expression.Ptr()->Child(0);                                              \
        if (!SerializeExpression(TExprBase(child), exprProto->mutable_operand(), ctx, depth + 1)) { \
            return false;                                                                           \
        }                                                                                           \
        return true;                                                                                \
    }

#define MATCH_VAR_ARG_OP(OpType, OP_ENUM)                                                            \
    if (auto maybeExpr = expression.Maybe<Y_CAT(TCo, OpType)>()) {                                   \
        auto expr = maybeExpr.Cast();                                                                \
        auto* exprProto = proto->mutable_var_arg_op();                                               \
        exprProto->set_operation(TExpression::TVarArgOperation::OP_ENUM);                            \
        for (const auto& child : expr.Args()) {                                                      \
            if (!SerializeExpression(TExprBase(child), exprProto->add_operands(), ctx, depth + 1)) { \
                return false;                                                                        \
            }                                                                                        \
        }                                                                                            \
        return true;                                                                                 \
    }

        bool SerializeSqlIfExpression(const TCoIf& sqlIf, TExpression* proto, TSerializationContext& ctx, ui64 depth);

        bool SerializeCoalesceExpression(const TCoCoalesce& coalesce, TExpression* proto, TSerializationContext& ctx, ui64 depth);

        bool SerializeExpression(const TExprBase& expression, TExpression* proto, TSerializationContext& ctx, ui64 depth) {
            if (auto member = expression.Maybe<TCoMember>()) {
                return SerializeMember(member.Cast(), proto, ctx);
            }
            if (auto coalesce = expression.Maybe<TCoCoalesce>()) {
                return SerializeCoalesceExpression(coalesce.Cast(), proto, ctx, depth);
            }
            if (auto sqlIf = expression.Maybe<TCoIf>()) {
                return SerializeSqlIfExpression(sqlIf.Cast(), proto, ctx, depth);
            }
            if (auto just = expression.Maybe<TCoJust>()) {
                return SerializeExpression(TExprBase(just.Cast().Input()), proto, ctx, depth + 1);
            }
            if (auto safeCast = expression.Maybe<TCoSafeCast>()) {
                return SerializeCastExpression(safeCast.Cast(), proto, ctx, depth);
            }
            if (expression.Ref().IsCallable("ToBytes")) {
                return SerializeToBytesExpression(expression, proto, ctx, depth);
            }
            if (expression.Ref().IsCallable("ToString")) {
                return SerializeToStringExpression(expression, proto, ctx, depth);
            }
            if (auto flatMap = expression.Maybe<TCoFlatMap>()) {
                return SerializeFlatMap(flatMap.Cast(), proto, ctx, depth);
            }
            if (auto dependsOn = expression.Maybe<TCoDependsOn>()) {
                return SerializeExpression(dependsOn.Cast().Input(), proto, ctx, depth + 1);
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
            MATCH_ATOM(Interval, INTERVAL, int64, i64);
            MATCH_ARITHMETICAL(Sub, SUB);
            MATCH_ARITHMETICAL(Add, ADD);
            MATCH_ARITHMETICAL(Mul, MUL);
            MATCH_ARITHMETICAL(Div, DIV);
            MATCH_ARITHMETICAL(Mod, MOD);
            MATCH_UNARY_OP(Unwrap, UNWRAP);
            MATCH_VAR_ARG_OP(Max, MAX);
            MATCH_VAR_ARG_OP(Min, MIN);
            MATCH_VAR_ARG_OP(CurrentUtcTimestamp, CURRENT_UTC_TIMESTAMP);

            if (auto maybeNull = expression.Maybe<TCoNull>()) {
                proto->mutable_null();
                return true;
            }

            // Try to serialize as lambda argument
            if (SerializeLambdaArgument(expression, proto, ctx)) {
                return true;
            }

            ctx.Err << "unknown expression: " << expression.Raw()->Content();
            return false;
        }

#undef MATCH_ATOM
#undef MATCH_ARITHMETICAL
#undef MATCH_UNARY_OP
#undef MATCH_VAR_ARG_OP

#define EXPR_NODE_TO_COMPARE_TYPE(TExprNodeType, COMPARE_TYPE)       \
    if (!opMatched && compare.Maybe<TExprNodeType>()) {              \
        opMatched = true;                                            \
        proto->set_operation(TPredicate::TComparison::COMPARE_TYPE); \
    }

        bool SerializeCompare(const TCoCompare& compare, TPredicate* predicateProto, TSerializationContext& ctx, ui64 depth) {
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
            EXPR_NODE_TO_COMPARE_TYPE(TCoCmpStartsWith, STARTS_WITH);
            EXPR_NODE_TO_COMPARE_TYPE(TCoCmpEndsWith, ENDS_WITH);
            EXPR_NODE_TO_COMPARE_TYPE(TCoCmpStringContains, CONTAINS);

            if (proto->operation() == TPredicate::TComparison::COMPARISON_OPERATION_UNSPECIFIED) {
                ctx.Err << "unknown compare operation: " << compare.Raw()->Content();
                return false;
            }
            return SerializeExpression(compare.Left(), proto->mutable_left_value(), ctx, depth + 1) && SerializeExpression(compare.Right(), proto->mutable_right_value(), ctx, depth + 1);
        }

#undef EXPR_NODE_TO_COMPARE_TYPE

        bool SerializePredicate(const TExprBase& predicate, TPredicate* proto, TSerializationContext& ctx, ui64 depth);

        bool SerializeSqlIfExpression(const TCoIf& sqlIf, TExpression* proto, TSerializationContext& ctx, ui64 depth) {
            auto* dstProto = proto->mutable_if_();
            return SerializePredicate(TExprBase(sqlIf.Predicate()), dstProto->mutable_predicate(), ctx, depth + 1)
                && SerializeExpression(TExprBase(sqlIf.ThenValue()), dstProto->mutable_then_expression(), ctx, depth + 1)
                && SerializeExpression(TExprBase(sqlIf.ElseValue()), dstProto->mutable_else_expression(), ctx, depth + 1);
        }

        bool SerializeSqlIfPredicate(const TCoIf& sqlIf, TPredicate* proto, TSerializationContext& ctx, ui64 depth) {
            auto* dstProto = proto->mutable_if_();
            return SerializePredicate(TExprBase(sqlIf.Predicate()), dstProto->mutable_predicate(), ctx, depth + 1)
                && SerializePredicate(TExprBase(sqlIf.ThenValue()), dstProto->mutable_then_predicate(), ctx, depth + 1)
                && SerializePredicate(TExprBase(sqlIf.ElseValue()), dstProto->mutable_else_predicate(), ctx, depth + 1);
        }

        template <typename TProto>
        void UnwrapNestedCoalesce(TProto* proto) {
            // We can unwrap nested COALESCE:
            // COALESCE(..., COALESCE(Predicate_1, Predicate_2), ...) -> COALESCE(..., Predicate_1, Predicate_2, ...)
            if (proto->operands().rbegin()->has_coalesce()) {
                auto coalesceOperands = std::move(*proto->mutable_operands()->rbegin()->mutable_coalesce()->mutable_operands());
                proto->mutable_operands()->RemoveLast();
                proto->mutable_operands()->Add(coalesceOperands.begin(), coalesceOperands.end());
            }
        }

        bool SerializeCoalesceExpression(const TCoCoalesce& coalesce, TExpression* proto, TSerializationContext& ctx, ui64 depth) {
            auto* dstProto = proto->mutable_coalesce();
            for (const auto& child : coalesce.Ptr()->Children()) {
                if (!SerializeExpression(TExprBase(child), dstProto->add_operands(), ctx, depth + 1)) {
                    return false;
                }
                UnwrapNestedCoalesce(dstProto);
            }
            return true;
        }

        bool SerializeCoalescePredicate(const TCoCoalesce& coalesce, TPredicate* proto, TSerializationContext& ctx, ui64 depth) {
            // Special case for top level COALESCE: COALESCE(Predicate, FALSE)
            // We can assume NULL as FALSE and skip COALESCE
            if (depth == 0) {
                auto value = coalesce.Value().Maybe<TCoBool>();
                if (value && TStringBuf(value.Cast().Literal()) == "false"sv) {
                    return SerializePredicate(TExprBase(coalesce.Predicate()), proto, ctx, 0);
                }
            }

            auto* dstProto = proto->mutable_coalesce();
            for (const auto& child : coalesce.Ptr()->Children()) {
                if (!SerializePredicate(TExprBase(child), dstProto->add_operands(), ctx, depth + 1)) {
                    return false;
                }
                UnwrapNestedCoalesce(dstProto);
            }
            return true;
        }

        bool SerializeExists(const TCoExists& exists, TPredicate* proto, TSerializationContext& ctx, bool withNot, ui64 depth) {
            auto* expressionProto = withNot ? proto->mutable_is_null()->mutable_value() : proto->mutable_is_not_null()->mutable_value();
            return SerializeExpression(exists.Optional(), expressionProto, ctx, depth + 1);
        }

        bool SerializeSqlIn(const TCoSqlIn& sqlIn, TPredicate* proto, TSerializationContext& ctx, ui64 depth) {
            auto* dstProto = proto->mutable_in();
            const TExprBase& expr = sqlIn.Collection();
            const TExprBase& lookup = sqlIn.Lookup();

            auto* expressionProto = dstProto->mutable_value();
            SerializeExpression(lookup, expressionProto, ctx, depth + 1);

            TExprNode::TPtr collection;
            if (expr.Ref().IsList()) {
                collection = expr.Ptr();
            } else if (auto maybeAsList = expr.Maybe<TCoAsList>()) {
                collection = maybeAsList.Cast().Ptr();
            } else {
                ctx.Err << "unknown source for in: " << expr.Ref().Content();
                return false;
            }

            for (auto& child : collection->Children()) {
                if (!SerializeExpression(TExprBase(child), dstProto->add_set(), ctx, depth + 1)) {
                    return false;
                }
            }
            return true;
        }

        bool SerializeIsNotDistinctFrom(const TExprBase& predicate, TPredicate* predicateProto, TSerializationContext& ctx, bool invert, ui64 depth) {
            if (predicate.Ref().ChildrenSize() != 2) {
                ctx.Err << "invalid IsNotDistinctFrom predicate, expected 2 children but got " << predicate.Ref().ChildrenSize();
                return false;
            }
            TPredicate::TComparison* proto = predicateProto->mutable_comparison();
            proto->set_operation(!invert ? TPredicate::TComparison::IND : TPredicate::TComparison::ID);
            return SerializeExpression(TExprBase(predicate.Ref().Child(0)), proto->mutable_left_value(), ctx, depth + 1)
                && SerializeExpression(TExprBase(predicate.Ref().Child(1)), proto->mutable_right_value(), ctx, depth + 1);
        }

        bool SerializeAnd(const TCoAnd& andExpr, TPredicate* proto, TSerializationContext& ctx, ui64 depth) {
            auto* dstProto = proto->mutable_conjunction();
            for (const auto& child : andExpr.Ptr()->Children()) {
                if (!SerializePredicate(TExprBase(child), dstProto->add_operands(), ctx, depth + 1)) {
                    return false;
                }
            }
            return true;
        }

        bool SerializeOr(const TCoOr& orExpr, TPredicate* proto, TSerializationContext& ctx, ui64 depth) {
            auto* dstProto = proto->mutable_disjunction();
            for (const auto& child : orExpr.Ptr()->Children()) {
                if (!SerializePredicate(TExprBase(child), dstProto->add_operands(), ctx, depth + 1)) {
                    return false;
                }
            }
            return true;
        }

        bool SerializeNot(const TCoNot& notExpr, TPredicate* proto, TSerializationContext& ctx, ui64 depth) {
            // Special case: (Not (Exists ...))
            if (auto exists = notExpr.Value().Maybe<TCoExists>()) {
                return SerializeExists(exists.Cast(), proto, ctx, true, depth + 1);
            }
            auto* dstProto = proto->mutable_negation();
            return SerializePredicate(notExpr.Value(), dstProto->mutable_operand(), ctx, depth + 1);
        }

        bool SerializeMember(const TCoMember& member, TPredicate* proto, TSerializationContext& ctx) {
            return SerializeMember(member, proto->mutable_bool_expression()->mutable_value(), ctx);
        }

        bool SerializeRegexp(const TCoUdf& regexp, const TExprNode::TListType& children, TPredicate* proto, TSerializationContext& ctx, ui64 depth) {
            if (children.size() != 2) {
                ctx.Err << "expected exactly one argument for UDF function Re2.Grep, but got: " << children.size() - 1;
                return false;
            }

            const auto& maybeRunConfig = regexp.RunConfigValue();
            if (!maybeRunConfig) {
                ctx.Err << "predicate for REGEXP can't be empty";
                return false;
            }
            const auto& runConfig = maybeRunConfig.Cast().Ref();

            if (runConfig.ChildrenSize() != 2) {
                ctx.Err << "expected exactly two run config options for UDF Re2.Grep, but got: " << runConfig.ChildrenSize();
                return false;
            }

            auto* dstProto = proto->mutable_regexp();
            return SerializeExpression(TExprBase(runConfig.ChildPtr(0)), dstProto->mutable_pattern(), ctx, depth + 1)
                && SerializeExpression(TExprBase(children[1]), dstProto->mutable_value(), ctx, depth + 1);
        }

        bool SerializeApply(const TCoApply& apply, TPredicate* proto, TSerializationContext& ctx, ui64 depth) {
            const auto& maybeUdf = apply.Callable().Maybe<TCoUdf>();
            if (!maybeUdf) {
                ctx.Err << "expected only UDF apply, but got: " << apply.Callable().Ref().Content();
                return false;
            }
            const auto& udf = maybeUdf.Cast();

            if (TStringBuf(udf.MethodName()) == "Re2.Grep"sv) {
                return SerializeRegexp(udf, apply.Ref().ChildrenList(), proto, ctx, depth);
            }

            ctx.Err << "unknown UDF in apply: " << TStringBuf(udf.MethodName());
            return false;
        }

        bool SerializePredicate(const TExprBase& predicate, TPredicate* proto, TSerializationContext& ctx, ui64 depth) {
            if (auto compare = predicate.Maybe<TCoCompare>()) {
                return SerializeCompare(compare.Cast(), proto, ctx, depth);
            }
            if (auto coalesce = predicate.Maybe<TCoCoalesce>()) {
                return SerializeCoalescePredicate(coalesce.Cast(), proto, ctx, depth);
            }
            if (auto andExpr = predicate.Maybe<TCoAnd>()) {
                return SerializeAnd(andExpr.Cast(), proto, ctx, depth);
            }
            if (auto orExpr = predicate.Maybe<TCoOr>()) {
                return SerializeOr(orExpr.Cast(), proto, ctx, depth);
            }
            if (auto notExpr = predicate.Maybe<TCoNot>()) {
                return SerializeNot(notExpr.Cast(), proto, ctx, depth);
            }
            if (auto member = predicate.Maybe<TCoMember>()) {
                return SerializeMember(member.Cast(), proto, ctx);
            }
            if (auto exists = predicate.Maybe<TCoExists>()) {
                return SerializeExists(exists.Cast(), proto, ctx, false, depth);
            }
            if (auto sqlIn = predicate.Maybe<TCoSqlIn>()) {
                return SerializeSqlIn(sqlIn.Cast(), proto, ctx, depth);
            }
            if (predicate.Ref().IsCallable("IsNotDistinctFrom")) {
                return SerializeIsNotDistinctFrom(predicate, proto, ctx, false, depth);
            }
            if (predicate.Ref().IsCallable("IsDistinctFrom")) {
                return SerializeIsNotDistinctFrom(predicate, proto, ctx, true, depth);
            }
            if (auto sqlIf = predicate.Maybe<TCoIf>()) {
                return SerializeSqlIfPredicate(sqlIf.Cast(), proto, ctx, depth);
            }
            if (auto just = predicate.Maybe<TCoJust>()) {
                return SerializePredicate(TExprBase(just.Cast().Input()), proto, ctx, depth + 1);
            }
            if (auto apply = predicate.Maybe<TCoApply>()) {
                return SerializeApply(apply.Cast(), proto, ctx, depth);
            }

            // Try to serialize predicate as boolean expression
            // For example single bool value TRUE in COALESCE or IF
            return SerializeExpression(predicate, proto->mutable_bool_expression()->mutable_value(), ctx, depth);
        }
    }

    TString FormatColumn(const TString& value) {
        return NFq::EncloseAndEscapeString(value, '`');
    }

    TString FormatValue(const Ydb::Value& value) {
        switch (value.value_case()) {
            case  Ydb::Value::kBoolValue:
                return value.bool_value() ? "TRUE" : "FALSE";
            case Ydb::Value::kInt32Value:
                return ToString(value.int32_value());
            case Ydb::Value::kUint32Value:
                return ToString(value.uint32_value());
            case Ydb::Value::kInt64Value:
                return ToString(value.int64_value());
            case Ydb::Value::kUint64Value:
                return ToString(value.uint64_value());
            case Ydb::Value::kFloatValue:
                return ToString(value.float_value());
            case Ydb::Value::kDoubleValue:
                return ToString(value.double_value());
            case Ydb::Value::kBytesValue:
                return NFq::EncloseAndEscapeString(value.bytes_value(), '"');
            case Ydb::Value::kTextValue:
                return NFq::EncloseAndEscapeString(value.text_value(), '"');
            default:
                throw yexception() << "Failed to format ydb value, value case " << static_cast<ui64>(value.value_case()) << " is not supported";
        }
    }

    TString FormatPrimitiveType(const Ydb::Type::PrimitiveTypeId& typeId) {
        switch (typeId) {
            case Ydb::Type::BOOL:
                return "Bool";
            case Ydb::Type::INT8:
                return "Int8";
            case Ydb::Type::INT16:
                return "Int16";
            case Ydb::Type::INT32:
                return "Int32";
            case Ydb::Type::INT64:
                return "Int64";
            case Ydb::Type::UINT8:
                return "Uint8";
            case Ydb::Type::UINT16:
                return "Uint16";
            case Ydb::Type::UINT32:
                return "Uint32";
            case Ydb::Type::UINT64:
                return "Uint64";
            case Ydb::Type::FLOAT:
                return "Float";
            case Ydb::Type::DOUBLE:
                return "Double";
            case Ydb::Type::TIMESTAMP:
                return "Timestamp";
            case Ydb::Type::INTERVAL:
                return "Interval";
            case Ydb::Type::STRING:
                return "String";
            case Ydb::Type::UTF8:
                return "Utf8";
            case Ydb::Type::JSON:
                return "Json";
            default:
                throw yexception() << "Failed to format primitive type, type case " << static_cast<ui64>(typeId) << " is not supported";
        }
    }

    TString FormatType(const Ydb::Type& type) {
        switch (type.type_case()) {
            case Ydb::Type::kTypeId:
                return FormatPrimitiveType(type.type_id());
            case Ydb::Type::kOptionalType:
                return TStringBuilder() << FormatType(type.optional_type().item()) << "?";
            default:
                throw yexception() << "Failed to format ydb type, type id " << static_cast<ui64>(type.type_case()) << " is not supported";
        }
    }

    TString FormatTypedValue(const Ydb::TypedValue& typedValue) {
        const auto& type = typedValue.type();
        switch (type.type_case()) {
        case Ydb::Type::kTypeId: {
            const auto& typeId = type.type_id();
            switch (typeId) {
            case Ydb::Type::INTERVAL: {
                const auto& value = typedValue.value();
                switch (value.value_case()) {
                case Ydb::Value::kInt64Value: {
                    const auto duration = TDuration::MicroSeconds(value.int64_value());
                    return TStringBuilder() << FormatType(typedValue.type()) << "(\"" << ToIso8601(duration) << "\")";
                }
                default:
                    [[fallthrough]];
                }
            }
            default:
                [[fallthrough]];
            }
        }
        default:
            // return TStringBuilder() << FormatType(typedValue.type()) << "(\"" << FormatValue(typedValue.value()) << "\")";
            return FormatValue(typedValue.value());
        }
    }

    TString FormatNull(const TExpression_TNull&) {
        return "NULL";
    }

    TString FormatCast(const TExpression::TCast& cast) {
        auto value = FormatExpression(cast.value());
        auto type = FormatType(cast.type());
        return TStringBuilder() << "CAST(" << value << " AS " << type << ")";
    }

    TString FormatUnaryOperation(const TExpression_TUnaryOperation& expression) {
        TStringBuilder sb;
        switch (expression.operation()) {
            case TExpression_TUnaryOperation::UNWRAP:
                sb << "Unwrap";
                break;
            default:
                throw yexception() << "ErrUnimplementedUnaryOperation, operation " << static_cast<ui64>(expression.operation());
        }
        sb << "(" << FormatExpression(expression.operand()) << ")";
        return sb;
    }

    TString FormatVarArgOperation(const TExpression_TVarArgOperation& expression) {
        TStringBuilder sb;
        switch (expression.operation()) {
            case TExpression_TVarArgOperation::COALESCE:
                sb << "COALESCE";
                break;
            case TExpression_TVarArgOperation::MIN:
                sb << "MIN_OF";
                break;
            case TExpression_TVarArgOperation::MAX:
                sb << "MAX_OF";
                break;
            case TExpression_TVarArgOperation::CURRENT_UTC_TIMESTAMP:
                sb << "CurrentUtcTimestamp";
                break;
            default:
                throw yexception() << "ErrUnimplementedVarArgOperation, operation " << static_cast<ui64>(expression.operation());
        }
        sb << "(";

        bool isFirst = true;
        for (const auto& operand : expression.operands()) {
            if (!isFirst) {
                sb << ", ";
            }
            sb << FormatExpression(operand);
            isFirst = false;
        }
        sb << ")";

        return sb;
    }

    TString FormatExpression(const TExpression& expression) {
        switch (expression.payload_case()) {
            case TExpression::PAYLOAD_NOT_SET:
                return {};
            case TExpression::kColumn:
                return FormatColumn(expression.column());
            case TExpression::kTypedValue:
                return FormatTypedValue(expression.typed_value());
            case TExpression::kArithmeticalExpression:
                return FormatArithmeticalExpression(expression.arithmetical_expression());
            case TExpression::kNull:
                return FormatNull(expression.null());
            case TExpression::kCoalesce:
                return FormatCoalesce(expression.coalesce());
            case TExpression::kIf:
                return FormatIfExpression(expression.if_());
            case TExpression::kCast:
                return FormatCast(expression.cast());
            case TExpression::kUnaryOp:
                return FormatUnaryOperation(expression.unary_op());
            case TExpression::kVarArgOp:
                return FormatVarArgOperation(expression.var_arg_op());
            default:
                throw yexception() << "Failed to format expression, unimplemented payload_case " << static_cast<ui64>(expression.payload_case());
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
        auto pred = FormatPredicate(negation.operand());
        return "(NOT " + pred + ")";
    }

    TString FormatConjunction(const TPredicate_TConjunction& conjunction) {
        ui32 succeeded = 0;
        TStringStream stream;
        TString first;

        for (const auto& predicate : conjunction.operands()) {
            auto statement = FormatPredicate(predicate);

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
            auto statement = FormatPredicate(predicate);

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

    template <typename TProto>
    TString FormatCoalesce(const typename TProto::TCoalesce& coalesce, std::function<TString(const TProto&)> operandsCallback) {
        TStringStream stream;
        TString first;
        ui32 cnt = 0;

        for (const auto& predicate : coalesce.operands()) {
            auto statement = operandsCallback(predicate);

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

    TString FormatCoalesce(const TExpression::TCoalesce& coalesce) {
        return FormatCoalesce<TExpression>(coalesce, &FormatExpression);
    }

    TString FormatCoalesce(const TPredicate::TCoalesce& coalesce) {
        return FormatCoalesce<TPredicate>(coalesce, &FormatPredicate);
    }

    TString FormatIfExpression(const TExpression::TIf& sqlIf) {
        TStringStream stream;
        stream << "IF(" << FormatPredicate(sqlIf.predicate());
        stream << ", " << FormatExpression(sqlIf.then_expression());
        stream << ", " << FormatExpression(sqlIf.else_expression()) << ")";
        return stream.Str();
    }

    TString FormatIfPredicate(const TPredicate::TIf& sqlIf) {
        TStringStream stream;
        stream << "IF(" << FormatPredicate(sqlIf.predicate());
        stream << ", " << FormatPredicate(sqlIf.then_predicate());
        stream << ", " << FormatPredicate(sqlIf.else_predicate()) << ")";
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

        auto left = FormatExpression(comparison.left_value());
        auto right = FormatExpression(comparison.right_value());

        // Distinct branch handling LIKE operator
        switch (comparison.operation()) {
            case TPredicate_TComparison::STARTS_WITH:
                return TStringBuilder() << "StartsWith(" << left << ", " << right << ")";
            case TPredicate_TComparison::ENDS_WITH:
                return TStringBuilder() << "EndsWith(" << left << ", " << right << ")";
            case TPredicate_TComparison::CONTAINS:
                return TStringBuilder() << "String::Contains(" << left << ", " << right << ")";
            default:
                break;
        }

        // General comparisons
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

        return TStringBuilder() << "(" << left << operation << right << ")";
    }

    TString FormatIn(const TPredicate_TIn& in) {
        auto value = FormatExpression(in.value());
        TStringStream list;
        for (const auto& expr : in.set()) {
            if (!list.empty()) {
                list << ", ";
            } else {
                list << "(" << value << " IN (";
            }
            list << FormatExpression(expr);
        }

        if (list.empty()) {
            throw yexception() << "failed to format IN statement, no operands";
        }

        list << "))";
        return list.Str();
    }

    TString FormatRegexp(const TPredicate::TRegexp& regexp) {
        const auto& value = FormatExpression(regexp.value());
        const auto& pattern = FormatExpression(regexp.pattern());
        return TStringBuilder() << "(" << value << " REGEXP " << pattern << ")";
    }

    TString FormatPredicate(const TPredicate& predicate) {
        switch (predicate.payload_case()) {
            case TPredicate::PAYLOAD_NOT_SET:
                return {};
            case TPredicate::kNegation:
                return FormatNegation(predicate.negation());
            case TPredicate::kConjunction:
                return FormatConjunction(predicate.conjunction());
            case TPredicate::kDisjunction:
                return FormatDisjunction(predicate.disjunction());
            case TPredicate::kCoalesce:
                return FormatCoalesce(predicate.coalesce());
            case TPredicate::kIf:
                return FormatIfPredicate(predicate.if_());
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
            case TPredicate::kRegexp:
                return FormatRegexp(predicate.regexp());
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

    bool SerializeFilterPredicate(
        TExprContext& ctx,
        const TExprBase& predicateBody,
        const TCoArgument& predicateArgument,
        NConnector::NApi::TPredicate* proto,
        TStringBuilder& err
    ) {
        TSerializationContext serializationContext = {.Arg = predicateArgument, .Err = err, .Ctx = ctx};
        return SerializePredicate(predicateBody, proto, serializationContext, 0);
    }

    bool SerializeFilterPredicate(
        TExprContext& ctx,
        const TCoLambda& predicate,
        TPredicate* proto,
        TStringBuilder& err
    ) {
        return SerializeFilterPredicate(ctx, predicate.Body(), predicate.Args().Arg(0), proto, err);
    }

    bool SerializeWatermarkExpr(
        TExprContext& ctx,
        const TCoLambda& predicate,
        TExpression* proto,
        TStringBuilder& err
    ) {
        TSerializationContext serializationContext = {.Arg = predicate.Args().Arg(0), .Err = err, .Ctx = ctx};
        return SerializeExpression(predicate.Body(), proto, serializationContext, 0);
    }

    TString FormatWhere(const TPredicate& predicate) {
        auto stream = FormatPredicate(predicate);
        if (stream.empty()) {
            return "";
        }
        return "WHERE " + stream;
    }
} // namespace NYql
