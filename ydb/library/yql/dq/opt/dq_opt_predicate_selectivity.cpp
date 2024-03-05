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
            attributeName = member.Cast().Raw()->Content();
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
                if (stats->KeyColumns.size()==1 && attributeName==stats->KeyColumns[0]) {
                    if (stats->Nrows > 1) {
                        return 1.0 / stats->Nrows;
                    }
                    else {
                        return 1.0;
                    }
                } else {
                    if (stats->Nrows > 1) {
                        return 0.1;
                    }
                    else {
                        return 1.0;
                    }
                }
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
                if (stats->KeyColumns.size()==1 && attributeName==stats->KeyColumns[0]) {
                    result = size / stats->Nrows;
                } else {
                    result = 0.1 + 0.2 / (1 + std::exp(size));
                }

            }
        }
    }

    return result;
}
