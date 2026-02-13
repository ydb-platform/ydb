#pragma once
#include "predicate_collector.h"

#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/utils/log/log.h>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NNodes;

struct TFilterOpsLevels {
    TFilterOpsLevels(const TMaybeNode<TExprBase>& firstLevel, const TMaybeNode<TExprBase>& secondLevel)
        : FirstLevelOps(firstLevel)
        , SecondLevelOps(secondLevel) {
    }

    TFilterOpsLevels(const TMaybeNode<TExprBase>& predicate)
        : FirstLevelOps(predicate)
        , SecondLevelOps(TMaybeNode<TExprBase>()) {
        if (IsSecondLevelOp(predicate)) {
            FirstLevelOps = TMaybeNode<TExprBase>();
            SecondLevelOps = predicate;
        }
    }

    bool IsValid() const {
        return FirstLevelOps.IsValid() || SecondLevelOps.IsValid();
    }

    bool IsSecondLevelOp(const TMaybeNode<TExprBase>& predicate);
    void WrapToNotOp(TExprContext& ctx, TPositionHandle pos);
    static TFilterOpsLevels Merge(TVector<TFilterOpsLevels> predicates, TExprContext& ctx, TPositionHandle pos);

    TMaybeNode<TExprBase> FirstLevelOps;
    TMaybeNode<TExprBase> SecondLevelOps;
};

std::pair<TVector<TOLAPPredicateNode>, TVector<TOLAPPredicateNode>> SplitForPartialPushdown(const TOLAPPredicateNode& predicateTree, bool allowApply);
TFilterOpsLevels PredicatePushdown(const TExprBase& predicate, const TExprNode& argument, TExprContext& ctx, TPositionHandle pos, const TPushdownOptions& pushdownOptions);
TMaybeNode<TExprBase> CombinePredicatesWithAnd(const TVector<TOLAPPredicateNode>& conjuncts, TExprContext& ctx, TPositionHandle pos, bool useOlapAnd,
                                               bool trueForEmpty);

} // namespace NKikimr::NKqp::NOpt
