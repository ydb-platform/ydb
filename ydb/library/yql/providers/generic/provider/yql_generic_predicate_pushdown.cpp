#include "yql_generic_predicate_pushdown.h"

#include <ydb/library/yql/providers/generic/connector/api/service/protos/connector.pb.h>

#include <util/string/cast.h>

namespace NYql {

    using namespace NNodes;
    using namespace NConnector::NApi;

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

            if (proto->operation() == TPredicate::TComparison::COMPARISON_OPERATION_UNSPECIFIED) {
                err << "unknown operation: " << compare.Raw()->Content();
                return false;
            }
            return SerializeExpression(compare.Left(), proto->mutable_left_value(), arg, err) && SerializeExpression(compare.Right(), proto->mutable_right_value(), arg, err);
        }

#undef EXPR_NODE_TO_COMPARE_TYPE

        bool SerializeCoalesce(const TCoCoalesce& coalesce, TPredicate* proto, const TCoArgument& arg, TStringBuilder& err) {
            auto predicate = coalesce.Predicate();
            if (auto compare = predicate.Maybe<TCoCompare>()) {
                return SerializeCompare(compare.Cast(), proto, arg, err);
            }

            err << "unknown coalesce predicate: " << predicate.Raw()->Content();
            return false;
        }

        bool SerializePredicate(const TExprBase& predicate, TPredicate* proto, const TCoArgument& arg, TStringBuilder& err);

        bool SerializeExists(const TCoExists& exists, TPredicate* proto, const TCoArgument& arg, TStringBuilder& err, bool withNot = false) {
            auto* expressionProto = withNot ? proto->mutable_is_null()->mutable_value() : proto->mutable_is_not_null()->mutable_value();
            return SerializeExpression(exists.Optional(), expressionProto, arg, err);
        }

        bool SerializeAnd(const TCoAnd& andExpr, TPredicate* proto, const TCoArgument& arg, TStringBuilder& err) {
            auto* dstProto = proto->mutable_conjunction();
            for (const auto& child : andExpr.Ptr()->Children()) {
                if (!SerializePredicate(TExprBase(child), dstProto->add_operands(), arg, err)) {
                    return false;
                }
            }
            return true;
        }

        bool SerializeOr(const TCoOr& orExpr, TPredicate* proto, const TCoArgument& arg, TStringBuilder& err) {
            auto* dstProto = proto->mutable_disjunction();
            for (const auto& child : orExpr.Ptr()->Children()) {
                if (!SerializePredicate(TExprBase(child), dstProto->add_operands(), arg, err)) {
                    return false;
                }
            }
            return true;
        }

        bool SerializeNot(const TCoNot& notExpr, TPredicate* proto, const TCoArgument& arg, TStringBuilder& err) {
            // Special case: (Not (Exists ...))
            if (auto exists = notExpr.Value().Maybe<TCoExists>()) {
                return SerializeExists(exists.Cast(), proto, arg, err, true);
            }
            auto* dstProto = proto->mutable_negation();
            return SerializePredicate(notExpr.Value(), dstProto->mutable_operand(), arg, err);
        }

        bool SerializeMember(const TCoMember& member, TPredicate* proto, const TCoArgument& arg, TStringBuilder& err) {
            return SerializeMember(member, proto->mutable_bool_expression()->mutable_value(), arg, err);
        }

        bool SerializePredicate(const TExprBase& predicate, TPredicate* proto, const TCoArgument& arg, TStringBuilder& err) {
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

    }

    bool IsEmptyFilterPredicate(const TCoLambda& lambda) {
        auto maybeBool = lambda.Body().Maybe<TCoBool>();
        if (!maybeBool) {
            return false;
        }
        return TStringBuf(maybeBool.Cast().Literal()) == "true"sv;
    }

    bool SerializeFilterPredicate(const TCoLambda& predicate, TPredicate* proto, TStringBuilder& err) {
        return SerializePredicate(predicate.Body(), proto, predicate.Args().Arg(0), err);
    }

} // namespace NYql
