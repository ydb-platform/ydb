#include "yql_generic_predicate_pushdown.h"

#include <ydb/library/yql/providers/generic/connector/api/service/protos/connector.pb.h>

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

        bool SerializeInt32(const TCoInt32& constant, TExpression* proto, TStringBuilder&) {
            auto* value = proto->mutable_typed_value();
            auto* t = value->mutable_type();
            t->set_type_id(Ydb::Type::INT32);
            auto* v = value->mutable_value();
            v->set_int32_value(FromString<i32>(constant.Literal()));
            return true;
        }

        bool SerializeExpression(const TExprBase& expression, TExpression* proto, const TCoArgument& arg, TStringBuilder& err) {
            if (auto member = expression.Maybe<TCoMember>()) {
                return SerializeMember(member.Cast(), proto, arg, err);
            }

            // data
            if (auto int32Atom = expression.Maybe<TCoInt32>()) {
                return SerializeInt32(int32Atom.Cast(), proto, err);
            }

            err << "unknown expression: " << expression.Raw()->Content();
            return false;
        }

        bool SerializeCompare(const TCoCompare& compare, TPredicate* predicateProto, const TCoArgument& arg, TStringBuilder& err) {
            TPredicate::TComparison* proto = predicateProto->mutable_comparison();
            if (compare.Maybe<TCoCmpEqual>()) {
                proto->set_operation(TPredicate::TComparison::EQ);
            }
            if (proto->operation() == TPredicate::TComparison::RESERVED) { // Unknown operation
                err << "unknown operation: " << compare.Raw()->Content();
                return false;
            }
            return SerializeExpression(compare.Left(), proto->mutable_left_value(), arg, err) && SerializeExpression(compare.Right(), proto->mutable_right_value(), arg, err);
        }

        bool SerializeCoalesce(const TCoCoalesce& coalesce, TPredicate* proto, const TCoArgument& arg, TStringBuilder& err) {
            auto predicate = coalesce.Predicate();
            if (auto compare = predicate.Maybe<TCoCompare>()) {
                return SerializeCompare(compare.Cast(), proto, arg, err);
            }

            err << "unknown coalesce predicate: " << predicate.Raw()->Content();
            return false;
        }

        bool SerializePredicate(const TExprBase& predicate, TPredicate* proto, const TCoArgument& arg, TStringBuilder& err) {
            if (auto compare = predicate.Maybe<TCoCompare>()) {
                return SerializeCompare(compare.Cast(), proto, arg, err);
            }
            if (auto coalesce = predicate.Maybe<TCoCoalesce>()) {
                return SerializeCoalesce(coalesce.Cast(), proto, arg, err);
            }

            err << "unknown predicate: " << predicate.Raw()->Content();
            return false;
        }

    } // namespace

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
