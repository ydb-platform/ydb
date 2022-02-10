#include "kqp_opt_impl.h"

#include <ydb/core/kqp/common/kqp_yql.h>

#include <ydb/library/yql/dq/opt/dq_opt_phy.h> 

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NDq;
using namespace NYql::NNodes;

namespace {

// left input should be DqCnUnionAll (with single usage)
// right input should be either DqCnUnionAll (with single usage) or DqPure expression
bool ValidateJoinInputs(const TExprBase& left, const TExprBase& right, const TParentsMap& parentsMap,
    bool allowStageMultiUsage)
{
    if (!left.Maybe<TDqCnUnionAll>()) {
        return false;
    }
    if (!IsSingleConsumerConnection(left.Cast<TDqCnUnionAll>(), parentsMap, allowStageMultiUsage)) {
        return false;
    }

    if (right.Maybe<TDqCnUnionAll>()) {
        if (!IsSingleConsumerConnection(right.Cast<TDqCnUnionAll>(), parentsMap, allowStageMultiUsage)) {
            return false;
        }
    } else if (IsDqPureExpr(right, /* isPrecomputePure */ true)) {
        // pass
    } else {
        return false;
    }

    return true;
}

TMaybeNode<TDqJoin> FlipJoin(const TDqJoin& join, TExprContext& ctx) {
    auto joinType = join.JoinType().Value();

    if (joinType == "Inner"sv || joinType == "Full"sv || joinType == "Exclusion"sv || joinType == "Cross"sv) {
        // pass
    } else if (joinType == "Right"sv) {
        joinType = "Left"sv;
    } else if (joinType == "Left"sv) {
        joinType = "Right"sv;
    } else if (joinType == "RightSemi"sv) {
        joinType = "LeftSemi"sv;
    } else if (joinType == "LeftSemi"sv) {
        joinType = "RightSemi"sv;
    } else if (joinType == "RightOnly"sv) {
        joinType = "LeftOnly"sv;
    } else if (joinType == "LeftOnly"sv) {
        joinType = "RightOnly"sv;
    } else {
        return {};
    }

    auto joinKeysBuilder = Build<TDqJoinKeyTupleList>(ctx, join.Pos());
    for (const auto& keys : join.JoinKeys()) {
        joinKeysBuilder.Add<TDqJoinKeyTuple>()
            .LeftLabel(keys.RightLabel())
            .LeftColumn(keys.RightColumn())
            .RightLabel(keys.LeftLabel())
            .RightColumn(keys.LeftColumn())
            .Build();
    }

    return Build<TDqJoin>(ctx, join.Pos())
        .LeftInput(join.RightInput())
        .LeftLabel(join.RightLabel())
        .RightInput(join.LeftInput())
        .RightLabel(join.LeftLabel())
        .JoinType().Build(joinType)
        .JoinKeys(joinKeysBuilder.Done())
        .Done();
}

} // anonymous namespace

TExprBase KqpBuildJoin(const TExprBase& node, TExprContext& ctx, const TKqpOptimizeContext& kqpCtx,
    IOptimizationContext& optCtx, const TParentsMap& parentsMap, bool allowStageMultiUsage)
{
    if (!node.Maybe<TDqJoin>()) {
        return node;
    }

    auto join = node.Cast<TDqJoin>();

    if (ValidateJoinInputs(join.LeftInput(), join.RightInput(), parentsMap, allowStageMultiUsage)) {
        // pass
    } else if (ValidateJoinInputs(join.RightInput(), join.LeftInput(), parentsMap, allowStageMultiUsage)) {
        auto maybeFlipJoin = FlipJoin(join, ctx);
        if (!maybeFlipJoin) {
            return node;
        }
        join = maybeFlipJoin.Cast();
    } else {
        return node;
    }

    auto joinType = join.JoinType().Value();

    if (joinType == "Full"sv || joinType == "Exclusion"sv) {
        return DqBuildJoinDict(join, ctx);
    }

    // NOTE: We don't want to broadcast table data via readsets for data queries, so we need to create a
    // separate stage to receive data from both sides of join.
    // TODO: We can push MapJoin to existing stage for data query, if it doesn't have table reads. This
    //       requires some additional knowledge, probably with use of constraints.
    bool pushLeftStage = !kqpCtx.IsDataQuery();
    return DqBuildPhyJoin(join, pushLeftStage, ctx, optCtx);
}

} // namespace NKikimr::NKqp::NOpt
