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
    bool IsAttribute(const TExprBase& input, TString& attributeName) {
        if (auto member = input.Maybe<TCoMember>()) {
            attributeName = member.Cast().Name().StringValue();
            return true;
        } else if (auto cast = input.Maybe<TCoSafeCast>()) {
            return IsAttribute(cast.Cast().Value(), attributeName);
        } else if (auto ifPresent = input.Maybe<TCoIfPresent>()) {
            return IsAttribute(ifPresent.Cast().Optional(), attributeName);
        } else if (auto just = input.Maybe<TCoJust>()) {
            return IsAttribute(just.Cast().Input(), attributeName);
        } else if (input.Ptr()->IsCallable("PgCast")) {
            auto child = TExprBase(input.Ptr()->ChildRef(0));
            return IsAttribute(child, attributeName);
        } else if (input.Ptr()->IsCallable("FromPg")) {
            auto child = TExprBase(input.Ptr()->ChildRef(0));
            return IsAttribute(child, attributeName);
        }

        return false;
    }

    double DefaultSelectivity(const std::shared_ptr<TOptimizerStatistics>& stats, const TString& attributeName) {
        if (stats->KeyColumns && stats->KeyColumns->Data.size() == 1 && attributeName == stats->KeyColumns->Data[0]) {
            if (stats->Nrows > 1) {
                return 1.0 / stats->Nrows;
            }
            
            return 1.0;
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

    double ComputeEqualitySelectivity(TExprBase& left, TExprBase& right, const std::shared_ptr<TOptimizerStatistics>& stats) {

        TString attributeName;

        if (IsAttribute(right, attributeName) && IsConstantExpr(left.Ptr())) {
            std::swap(left, right);
        }

        if (IsAttribute(left, attributeName)) {
            // In case both arguments refer to an attribute, return 0.2
            TString rightAttributeName;
            if (IsAttribute(right, rightAttributeName)) {
                return 0.3;
            }
            // In case the right side is a constant that can be extracted, compute the selectivity using statistics
            // Currently, with the basic statistics we just return 1/nRows

            else if (IsConstantExpr(right.Ptr())) {
                if (stats->ColumnStatistics == nullptr) {
                    return DefaultSelectivity(stats, attributeName);
                }
                
                if (auto countMinSketch = stats->ColumnStatistics->Data[attributeName].CountMinSketch; countMinSketch != nullptr) {
                    auto columnType = stats->ColumnStatistics->Data[attributeName].Type;
                    std::optional<ui32> countMinEstimation = EstimateCountMin(right, columnType,  countMinSketch);
                    if (!countMinEstimation.has_value()) {
                        return DefaultSelectivity(stats, attributeName);
                    }
                    return countMinEstimation.value() / stats->Nrows;
                }
                
                return DefaultSelectivity(stats, attributeName);
            }
        }

        return 1.0;
    }

    double ComputeComparisonSelectivity(TExprBase& left, TExprBase& right, const std::shared_ptr<TOptimizerStatistics>& stats) {

        Y_UNUSED(stats);

        TString attributeName;
        if (IsAttribute(right, attributeName) && IsConstantExpr(left.Ptr())) {
            std::swap(left, right);
        }

        if (IsAttribute(left, attributeName)) {
            // In case both arguments refer to an attribute, return 0.2
            if (IsAttribute(right, attributeName)) {
                return 0.3;
            }
            // In case the right side is a constant that can be extracted, compute the selectivity using statistics
            // Currently, with the basic statistics we just return 0.5
            else if (IsConstantExpr(right.Ptr())) {
                return 0.5;
            }
        }

        return 1.0;
    }
}

/**
 * Compute the selectivity of a predicate given statistics about the input it operates on
 */
double NYql::NDq::ComputePredicateSelectivity(const TExprBase& input, const std::shared_ptr<TOptimizerStatistics>& stats) {
    double result = 1.0;

    // Process OptionalIf, just return the predicate statistics
    if (auto optIf = input.Maybe<TCoOptionalIf>()) {
        result = ComputePredicateSelectivity(optIf.Cast().Predicate(), stats);
    }

    // Same with Coalesce
    else if (auto coalesce = input.Maybe<TCoCoalesce>()) {
        result = ComputePredicateSelectivity(coalesce.Cast().Predicate(), stats);
    }

    else if (input.Ptr()->IsCallable("FromPg")) {
        auto child = TExprBase(input.Ptr()->ChildRef(0));
        result = ComputePredicateSelectivity(child, stats);
    }

    else if (input.Ptr()->IsCallable("Exists")) {
        auto child = TExprBase(input.Ptr()->ChildRef(0));
        result = ComputePredicateSelectivity(child, stats);
    }

    else if(input.Ptr()->IsCallable("Find") || input.Ptr()->IsCallable("StringContains")) {
        auto member =  TExprBase(input.Ptr()->ChildRef(0));
        auto stringPred = TExprBase(input.Ptr()->ChildRef(1));

        TString attributeName;
        if (IsAttribute(member, attributeName) && IsConstantExpr(stringPred.Ptr())) {
            result = 0.1;
        }
    }

    // Process AND, OR and NOT logical operators.
    // In case of AND we multiply the selectivities, since we assume them to be independent
    // In case of OR we sum them up, again assuming independence and disjointness, but make sure its at most 1.0
    // In case of NOT we subtract the argument's selectivity from 1.0

    else if (auto andNode = input.Maybe<TCoAnd>()) {
        double res = 1.0;
        for (size_t i = 0; i < andNode.Cast().ArgCount(); i++) {
            res *= ComputePredicateSelectivity(andNode.Cast().Arg(i), stats);
        }
        result = res;
    } else if (auto orNode = input.Maybe<TCoOr>()) {
        double res = 0.0;
        for (size_t i = 0; i < orNode.Cast().ArgCount(); i++) {
            res += ComputePredicateSelectivity(orNode.Cast().Arg(i), stats);
        }
        result = std::max(res, 1.0);
    } else if (auto notNode = input.Maybe<TCoNot>()) {
        double argSel = ComputePredicateSelectivity(notNode.Cast().Value(), stats);
        result = 1.0 - (argSel == 1.0 ? 0.95 : argSel);
    }

    // Process the equality predicate
    else if (auto equality = input.Maybe<TCoCmpEqual>()) {
        auto left = equality.Cast().Left();
        auto right = equality.Cast().Right();

        result = ComputeEqualitySelectivity(left, right, stats);
    }

    else if (input.Ptr()->IsCallable("PgResolvedOp") && input.Ptr()->ChildPtr(0)->Content()=="=") {
        auto left = TExprBase(input.Ptr()->ChildPtr(2));
        auto right = TExprBase(input.Ptr()->ChildPtr(3));

        result = ComputeEqualitySelectivity(left, right, stats);
    }

    // Process the not equal predicate
    else if (auto equality = input.Maybe<TCoCmpNotEqual>()) {
        auto left = equality.Cast().Left();
        auto right = equality.Cast().Right();

        double eqSel = ComputeEqualitySelectivity(left, right, stats);
        result = 1.0 - (eqSel == 1.0 ? 0.95 : eqSel);
    }

    else if (input.Ptr()->IsCallable("PgResolvedOp") && input.Ptr()->ChildPtr(0)->Content()=="<>") {
        auto left = TExprBase(input.Ptr()->ChildPtr(2));
        auto right = TExprBase(input.Ptr()->ChildPtr(3));

        double eqSel = ComputeEqualitySelectivity(left, right, stats);
        result = 1.0 - (eqSel == 1.0 ? 0.95 : eqSel);
    }

    // Process all other comparison predicates
    else if (auto comparison = input.Maybe<TCoCompare>()) {
        auto left = comparison.Cast().Left();
        auto right = comparison.Cast().Right();

        result = ComputeComparisonSelectivity(left, right, stats);
    }

    else if (input.Ptr()->IsCallable("PgResolvedOp") && PgInequalityPreds.contains(input.Ptr()->ChildPtr(0)->Content())){
        auto left = TExprBase(input.Ptr()->ChildPtr(2));
        auto right = TExprBase(input.Ptr()->ChildPtr(3));

        result = ComputeComparisonSelectivity(left, right, stats);
    }

    // Process SqlIn
    else if(input.Ptr()->IsCallable("SqlIn")) {
        auto left = TExprBase(input.Ptr()->ChildPtr(0));
        auto right = TExprBase(input.Ptr()->ChildPtr(1));

        TString attributeName;

        if (IsAttribute(right, attributeName) && IsConstantExpr(left.Ptr())) {
            std::swap(left, right);
        }

        if (IsAttribute(left, attributeName) && IsConstantExpr(right.Ptr())) {
            if (right.Ptr()->IsCallable("AsList")) {
                auto size = right.Ptr()->Child(0)->ChildrenSize();
                if (stats->KeyColumns && stats->KeyColumns->Data.size()==1 && attributeName==stats->KeyColumns->Data[0]) {
                    result = size / stats->Nrows;
                } else {
                    result = 0.1 + 0.2 / (1 + std::exp(size));
                }

            }
        }
    }

    return result;
}
