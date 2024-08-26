#include "dq_opt_stat.h"

#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/utils/log/log.h>

using namespace NYql;
using namespace NYql::NNodes;

namespace {

    using namespace NYql::NDq;

    THashSet<TString> PgInequalityPreds = {
        "<", "<=", ">", ">="};

    /**
     * Check if a callable is an attribute of some table
     * Currently just return a boolean and cover only basic cases
     */
    TMaybe<TCoMember> IsAttribute(const TExprBase& input) {
        if (auto member = input.Maybe<TCoMember>()) {
            return member.Cast();
        } else if (auto cast = input.Maybe<TCoSafeCast>()) {
            return IsAttribute(cast.Cast().Value());
        } else if (auto ifPresent = input.Maybe<TCoIfPresent>()) {
            return IsAttribute(ifPresent.Cast().Optional());
        } else if (auto just = input.Maybe<TCoJust>()) {
            return IsAttribute(just.Cast().Input());
        } else if (input.Ptr()->IsCallable("PgCast")) {
            auto child = TExprBase(input.Ptr()->ChildRef(0));
            return IsAttribute(child);
        } else if (input.Ptr()->IsCallable("FromPg")) {
            auto child = TExprBase(input.Ptr()->ChildRef(0));
            return IsAttribute(child);
        } else if (auto exists = input.Maybe<TCoExists>()) {
            auto child = TExprBase(input.Ptr()->ChildRef(0));
            return IsAttribute(child);
        }

        return Nothing();
    }

    double DefaultSelectivity(const std::shared_ptr<TOptimizerStatistics>& stats, const TString& attributeName) {
        if (stats == nullptr) {
            return 1.0;
        }

        if (stats->KeyColumns && stats->KeyColumns->Data.size() == 1 && attributeName == stats->KeyColumns->Data[0]) {
            return 1.0 / std::max<double>(stats->Nrows, 1.0);
        } else {
            if (stats->Nrows > 1) {
                return 0.1;
            }
                
            return 1.0;
        }
    }

    std::optional<ui32> EstimateCountMin(NYql::NNodes::TExprBase maybeLiteral, TString columnType, const std::shared_ptr<NKikimr::TCountMinSketch>& countMinSketch) {
        if (auto maybeJust = maybeLiteral.Maybe<NYql::NNodes::TCoJust>() ) {
            maybeLiteral = maybeJust.Cast().Input();
        }

        if (maybeLiteral.Maybe<NYql::NNodes::TCoDataCtor>()) {
            auto literal = maybeLiteral.Maybe<NYql::NNodes::TCoDataCtor>().Cast();
            auto value = literal.Literal().Value();

            if (columnType == "Bool") {
                ui8 v = FromString<bool>(value);
                return countMinSketch->Probe(reinterpret_cast<const char*>(&v), sizeof(v));
            } else if (columnType == "Uint8") {
                ui8 v = FromString<ui8>(value);
                return countMinSketch->Probe(reinterpret_cast<const char*>(&v), sizeof(v));
            } else if (columnType == "Int8") {
                i8 v = FromString<i8>(value);
                return countMinSketch->Probe(reinterpret_cast<const char*>(&v), sizeof(v));
            } else if (columnType == "Uint32") {
                ui32 v = FromString<ui32>(value);
                return countMinSketch->Probe(reinterpret_cast<const char*>(&v), sizeof(v));
            } else if (columnType == "Int32") {
                i32 v = FromString<i32>(value);
                return countMinSketch->Probe(reinterpret_cast<const char*>(&v), sizeof(v));
            } else if (columnType == "Uint64") {
                ui64 v = FromString<ui64>(value);
                return countMinSketch->Probe(reinterpret_cast<const char*>(&v), sizeof(v));
            } else if (columnType == "Int64") {
                i64 v = FromString<i64>(value);
                return countMinSketch->Probe(reinterpret_cast<const char*>(&v), sizeof(v));
            } else if (columnType == "Float") {
                float v = FromString<float>(value);
                return countMinSketch->Probe(reinterpret_cast<const char*>(&v), sizeof(v));
            } else if (columnType == "Double") {
                double v = FromString<double>(value);
                return countMinSketch->Probe(reinterpret_cast<const char*>(&v), sizeof(v));
            } else if (columnType == "Date") {
                ui16 v = FromString<ui32>(value);
                return countMinSketch->Probe(reinterpret_cast<const char*>(&v), sizeof(v));
            } else if (columnType == "Datetime") {
                ui32 v = FromString<ui32>(value);
                return countMinSketch->Probe(reinterpret_cast<const char*>(&v), sizeof(v));
            } else if (columnType == "Utf8" || columnType == "String" || columnType == "Yson" || columnType == "Json") {
                return countMinSketch->Probe(value.Data(), value.Size());
            } else if (columnType == "Interval" || columnType == "Timestamp64" || columnType == "Interval64") {
                i64 v = FromString<i64>(value);
                return countMinSketch->Probe(reinterpret_cast<const char*>(&v), sizeof(v));
            } else if (columnType == "Timestamp") {
                ui64 v = FromString<ui64>(value);
                return countMinSketch->Probe(reinterpret_cast<const char*>(&v), sizeof(v));
            } else if (columnType == "Uuid") {
                const ui64* uuidData = reinterpret_cast<const ui64*>(value.Data());
                std::pair<ui64, ui64> v{};
                v.first = uuidData[0]; // low128
                v.second = uuidData[1]; // high128
                return countMinSketch->Probe(reinterpret_cast<const char*>(&v), sizeof(v));
            } else {
                return std::nullopt;
            }

        }

        return std::nullopt;
    }
}

template<typename T>
TExprNode::TPtr FindNode(const TExprBase& input) {
    for (const auto& child : input.Ptr()->Children()) {
        if (TExprBase(child).Maybe<T>()) {
            return child;
        }

        auto tmp = FindNode<T>(TExprBase(child));
        if (tmp != nullptr) {
            return tmp;
        }
    }

    return nullptr;
}

double NYql::NDq::TPredicateSelectivityComputer::ComputeEqualitySelectivity(const TExprBase& left, const TExprBase& right) {
    if (IsAttribute(right) && IsConstantExprWithParams(left.Ptr())) {
        return ComputeEqualitySelectivity(right, left);
    }

    if (auto maybeMember = IsAttribute(left)) {
        // In case both arguments refer to an attribute, return 0.2
        if (IsAttribute(right)) {
            return 0.3;
        }
        // In case the right side is a constant that can be extracted, compute the selectivity using statistics
        // Currently, with the basic statistics we just return 1/nRows

        else if (IsConstantExprWithParams(right.Ptr())) {
            TString attributeName = maybeMember.Get()->Name().StringValue();
            if (!IsConstantExpr(right.Ptr())) {
                return DefaultSelectivity(Stats, attributeName);
            }

            if (Stats == nullptr || Stats->ColumnStatistics == nullptr) {
                if (CollectColumnsStatUsedMembers) {
                    ColumnStatsUsedMembers.AddEquality(*maybeMember.Get());
                }
                return DefaultSelectivity(Stats, attributeName);
            }
            
            if (auto countMinSketch = Stats->ColumnStatistics->Data[attributeName].CountMinSketch; countMinSketch != nullptr) {
                auto columnType = Stats->ColumnStatistics->Data[attributeName].Type;
                std::optional<ui32> countMinEstimation = EstimateCountMin(right, columnType,  countMinSketch);
                if (!countMinEstimation.has_value()) {
                    return DefaultSelectivity(Stats, attributeName);
                }
                return countMinEstimation.value() / Stats->Nrows;
            }
            
            return DefaultSelectivity(Stats, attributeName);
        }
    }

    return 1.0;
}

double NYql::NDq::TPredicateSelectivityComputer::ComputeComparisonSelectivity(const TExprBase& left, const TExprBase& right) {
    if (IsAttribute(right) && IsConstantExprWithParams(left.Ptr())) {
        return ComputeComparisonSelectivity(right, left);
    }

    if (IsAttribute(left)) {
        // In case both arguments refer to an attribute, return 0.2
        if (IsAttribute(right)) {
            return 0.3;
        }
        // In case the right side is a constant that can be extracted, compute the selectivity using statistics
        // Currently, with the basic statistics we just return 0.5
        else if (IsConstantExprWithParams(right.Ptr())) {
            return 0.5;
        }
    }

    return 1.0;
}

/**
 * Compute the selectivity of a predicate given statistics about the input it operates on
 */
double TPredicateSelectivityComputer::Compute(
    const TExprBase& input
) {
    std::optional<double> resSelectivity;

    // Process OptionalIf, just return the predicate statistics
    if (auto optIf = input.Maybe<TCoOptionalIf>()) {
        resSelectivity = Compute(optIf.Cast().Predicate());
    }

    // Same with Coalesce
    else if (auto coalesce = input.Maybe<TCoCoalesce>()) {
        resSelectivity = Compute(coalesce.Cast().Predicate());
    }

    else if (
        input.Ptr()->IsCallable("FromPg") ||
        input.Ptr()->IsCallable("Exists") ||
        input.Ptr()->IsCallable("AssumeStrict") ||
        input.Ptr()->IsCallable("Apply") ||
        input.Ptr()->IsCallable("Udf")
    ) {
        auto child = TExprBase(input.Ptr()->ChildRef(0));
        resSelectivity = Compute(child);
    }

    else if(input.Ptr()->IsCallable("Find") || input.Ptr()->IsCallable("StringContains")) {
        auto member =  TExprBase(input.Ptr()->ChildRef(0));
        auto stringPred = TExprBase(input.Ptr()->ChildRef(1));

        if (IsAttribute(member) && IsConstantExpr(stringPred.Ptr())) {
            resSelectivity = 0.1;
        }
    }

    // Process AND, OR and NOT logical operators.
    // In case of AND we multiply the selectivities, since we assume them to be independent
    // In case of OR we sum them up, again assuming independence and disjointness, but make sure its at most 1.0
    // In case of NOT we subtract the argument's selectivity from 1.0

    else if (auto andNode = input.Maybe<TCoAnd>()) {
        double tmpSelectivity = 1.0;
        for (size_t i = 0; i < andNode.Cast().ArgCount(); i++) {
            tmpSelectivity *= Compute(andNode.Cast().Arg(i));
        }
        resSelectivity = tmpSelectivity;
    } else if (auto orNode = input.Maybe<TCoOr>()) {
        double tmpSelectivity = 0.0;
        for (size_t i = 0; i < orNode.Cast().ArgCount(); i++) {
            tmpSelectivity += Compute(orNode.Cast().Arg(i));
        }
        resSelectivity = tmpSelectivity;
    } else if (auto notNode = input.Maybe<TCoNot>()) {
        double argSel = Compute(notNode.Cast().Value());
        resSelectivity = 1.0 - (argSel == 1.0 ? 0.95 : argSel);
    }

    // Process the equality predicate
    else if (auto equality = input.Maybe<TCoCmpEqual>()) {
        auto left = equality.Cast().Left();
        auto right = equality.Cast().Right();

        resSelectivity = ComputeEqualitySelectivity(left, right);
    }

    else if (input.Ptr()->IsCallable("PgResolvedOp") && input.Ptr()->ChildPtr(0)->Content()=="=") {
        auto left = TExprBase(input.Ptr()->ChildPtr(2));
        auto right = TExprBase(input.Ptr()->ChildPtr(3));

        resSelectivity = ComputeEqualitySelectivity(left, right);
    }

    // Process the not equal predicate
    else if (auto equality = input.Maybe<TCoCmpNotEqual>()) {
        auto left = equality.Cast().Left();
        auto right = equality.Cast().Right();

        double eqSel = ComputeEqualitySelectivity(left, right);
        resSelectivity = 1.0 - (eqSel == 1.0 ? 0.95 : eqSel);
    }

    else if (input.Ptr()->IsCallable("PgResolvedOp") && input.Ptr()->ChildPtr(0)->Content()=="<>") {
        auto left = TExprBase(input.Ptr()->ChildPtr(2));
        auto right = TExprBase(input.Ptr()->ChildPtr(3));

        double eqSel = ComputeEqualitySelectivity(left, right);
        resSelectivity = 1.0 - (eqSel == 1.0 ? 0.95 : eqSel);
    }

    // Process all other comparison predicates
    else if (auto comparison = input.Maybe<TCoCompare>()) {
        auto left = comparison.Cast().Left();
        auto right = comparison.Cast().Right();

        resSelectivity = ComputeComparisonSelectivity(left, right);
    }

    else if (input.Ptr()->IsCallable("PgResolvedOp") && PgInequalityPreds.contains(input.Ptr()->ChildPtr(0)->Content())){
        auto left = TExprBase(input.Ptr()->ChildPtr(2));
        auto right = TExprBase(input.Ptr()->ChildPtr(3));

        resSelectivity = ComputeComparisonSelectivity(left, right);
    }

    // Process SqlIn
    else if(input.Ptr()->IsCallable("SqlIn")) {
        auto list = input.Ptr()->ChildPtr(0);

        double tmpSelectivity = 0.0;
        auto lhs = TExprBase(input.Ptr()->ChildPtr(1));
        for (const auto& child: list->Children()) {
            TExprBase rhs = TExprBase(child);
            tmpSelectivity += ComputeEqualitySelectivity(lhs, rhs);
        }
        resSelectivity = tmpSelectivity;
    }

    else if (input.Maybe<TCoAtom>()) {
        auto atom = input.Cast<TCoAtom>();
        // regexp
        if (atom.StringValue().StartsWith("Re2")) {
            resSelectivity = 0.5;
        }
    }

    else if (auto maybeIfExpr = input.Maybe<TCoIf>()) {
        auto ifExpr = maybeIfExpr.Cast();
        
        // attr in ('a', 'b', 'c' ...)
        if (ifExpr.Predicate().Maybe<TCoExists>() && ifExpr.ThenValue().Maybe<TCoJust>() && ifExpr.ElseValue().Maybe<TCoNothing>()) {
            auto list = FindNode<TExprList>(ifExpr.ThenValue());

            if (list != nullptr) {
                double tmpSelectivity = 0.0;
                TExprBase lhs = ifExpr.Predicate();
                for (const auto& child: list->Children()) {
                    TExprBase rhs = TExprBase(child);
                    tmpSelectivity += ComputeEqualitySelectivity(lhs, rhs);
                }

                resSelectivity = tmpSelectivity;
            }
        }
    }

    if (!resSelectivity.has_value()) {
        auto dumped = input.Raw()->Dump();
        YQL_CLOG(TRACE, CoreDq) << "ComputePredicateSelectivity NOT FOUND : " << dumped;
        return 1.0;
    }

    return std::min(1.0, resSelectivity.value());
}
