#pragma once

#include "kqp_opt_predicate_selectivity.h"

#include <ydb/library/yql/dq/opt/dq_opt.h>

#include <ydb/core/kqp/opt/cbo/cbo_optimizer_new.h>
#include <ydb/core/kqp/opt/cbo/kqp_statistics.h>

namespace NKikimr::NKqp {

using namespace NYql::NNodes;
using NYql::TExprNode;

void InferStatisticsForFlatMap(const TExprNode::TPtr& input, TKqpStatsStore* kqpStats);
void InferStatisticsForFilter(const TExprNode::TPtr& input, TKqpStatsStore* kqpStats);
void InferStatisticsForSkipNullMembers(const TExprNode::TPtr& input, TKqpStatsStore* kqpStats);
void InferStatisticsForExtendBase(const TExprNode::TPtr& input, TKqpStatsStore* kqpStats);
void InferStatisticsForAggregateBase(const TExprNode::TPtr& input, TKqpStatsStore* kqpStats);
void InferStatisticsForAggregateMergeFinalize(const TExprNode::TPtr& input, TKqpStatsStore* kqpStats);
void PropagateStatisticsToLambdaArgument(const TExprNode::TPtr& input, TKqpStatsStore* kqpStats);
void PropagateStatisticsToStageArguments(const TExprNode::TPtr& input, TKqpStatsStore* kqpStats);
void InferStatisticsForStage(const TExprNode::TPtr& input, TKqpStatsStore* kqpStats);
void InferStatisticsForDqSource(const TExprNode::TPtr& input, TKqpStatsStore* kqpStats);
void InferStatisticsForDqMerge(const TExprNode::TPtr& input, TKqpStatsStore* kqpStats);
void InferStatisticsForGraceJoin(const TExprNode::TPtr& input, TKqpStatsStore* kqpStats, const IProviderContext& ctx, TOptimizerHints hints = {}, TShufflingOrderingsByJoinLabels* shufflingOrderingsByJoinLabels = nullptr);
void InferStatisticsForBlockHashJoin(const TExprNode::TPtr& input, TKqpStatsStore* kqpStats, const IProviderContext& ctx, TOptimizerHints hints = {});
void InferStatisticsForMapJoin(const TExprNode::TPtr& input, TKqpStatsStore* kqpStats, const IProviderContext& ctx, TOptimizerHints hints = {});
void InferStatisticsForDqJoinBase(const TExprNode::TPtr& input, TKqpStatsStore* kqpStats, const IProviderContext& ctx, TOptimizerHints hints = {});
void InferStatisticsForDqPhyCrossJoin(const TExprNode::TPtr& input, TKqpStatsStore* kqpStats);
void InferStatisticsForAsList(const TExprNode::TPtr& input, TKqpStatsStore* kqpStats);
void InferStatisticsForAsStruct(const TExprNode::TPtr& input, TKqpStatsStore* kqpStats);
void InferStatisticsForTopBase(const TExprNode::TPtr& input, TKqpStatsStore* kqpStats);
void InferStatisticsForSortBase(const TExprNode::TPtr& input, TKqpStatsStore* kqpStats);
bool InferStatisticsForListParam(const TExprNode::TPtr& input, TKqpStatsStore* kqpStats);
void InferStatisticsForEquiJoin(const TExprNode::TPtr& input, TKqpStatsStore* kqpStats);
void InferStatisticsForUnionAll(const TExprNode::TPtr& input, TKqpStatsStore* kqpStats);

template <typename TAggregationCallable>
void InferStatisticsForAggregationCallable(const TExprNode::TPtr& input, TKqpStatsStore* kqpStats);
extern template void InferStatisticsForAggregationCallable<TCoShuffleByKeys>(const TExprNode::TPtr& input, TKqpStatsStore* kqpStats);


std::shared_ptr<TOptimizerStatistics> RemoveSorting(const std::shared_ptr<TOptimizerStatistics>& stats);
std::shared_ptr<TOptimizerStatistics> RemoveSorting(const std::shared_ptr<TOptimizerStatistics>& stats, const TExprNode::TPtr& input);
std::shared_ptr<TOptimizerStatistics> RemoveShuffling(const std::shared_ptr<TOptimizerStatistics>& stats);
std::shared_ptr<TOptimizerStatistics> RemoveShuffling(const std::shared_ptr<TOptimizerStatistics>& stats, const TExprNode::TPtr& input);

std::shared_ptr<TOptimizerStatistics> RemoveOrderings(const std::shared_ptr<TOptimizerStatistics>& stats, const TExprNode::TPtr& input);

struct TOrderingInfo {
    std::int64_t OrderingIdx = -1;
    std::vector<TOrdering::TItem::EDirection> Directions{};
    TVector<TJoinColumn> Ordering{};
};

TOrderingInfo GetTopBaseSortingOrderingInfo(const TCoTopBase&, const TSimpleSharedPtr<TOrderingsStateMachine>& sortingsFSM, TTableAliasMap*);
TOrderingInfo GetSortBaseSortingOrderingInfo(const TCoSortBase&, const TSimpleSharedPtr<TOrderingsStateMachine>& sortingsFSM, TTableAliasMap*);
TOrderingInfo GetAggregationBaseShuffleOrderingInfo(const TCoAggregateBase&, const TSimpleSharedPtr<TOrderingsStateMachine>& shufflingsFSM, TTableAliasMap*);
TVector<TJoinColumn> GetKeySelectorOrdering(const TCoLambda& keySelector);

bool NeedCalc(TExprBase node);
// Returns true if the expression is already a fully-evaluated literal
// (TCoDataCtor, TCoNothing, or TCoJust wrapping a literal) and
// does not need further evaluation via EvaluateExpr.
bool IsLiteralDataExpr(TExprBase node);
bool IsConstantExpr(const TExprNode::TPtr& input, bool foldUdfs = true);
bool IsConstantExprWithParams(const TExprNode::TPtr& input);

} // namespace NKikimr::NKqp
