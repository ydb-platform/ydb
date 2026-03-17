#include "dq_opt_stat_transformer_base.h"

#include <ydb/core/kqp/opt/cbo/solver/dq_opt_stat.h>
#include <yql/essentials/core/yql_expr_optimize.h>

#include <yql/essentials/utils/log/log.h>


namespace NKikimr::NKqp {

using namespace NYql;
using namespace NYql::NDq;
using namespace NYql::NNodes;

TDqStatisticsTransformerBase::TDqStatisticsTransformerBase(
    TTypeAnnotationContext* typeCtx,
    const IProviderContext& ctx,
    const TOptimizerHints& hints,
    TShufflingOrderingsByJoinLabels* shufflingOrderingsByJoinLabels,
    const bool useFSMForSortElimination,
    TKqpStatsStore* kqpStats
)
    : TypeCtx(typeCtx)
    , KqpStats(kqpStats)
    , Pctx(ctx)
    , Hints(hints)
    , ShufflingOrderingsByJoinLabels(shufflingOrderingsByJoinLabels)
    , UseFSMForSortElimination(useFSMForSortElimination)
{ }

namespace {

void PropogateTableAliasesFromChildren(const TExprNode::TPtr& input, TKqpStatsStore* kqpStats) {
    auto inputNode = TExprBase(input);
    auto stats = kqpStats->GetStats(inputNode.Raw());

    // Don't process these, already processed at the InferStatistics stage
    if (
        stats && stats->TableAliases &&
        (
            TCoAsStruct::Match(inputNode.Raw()) ||
            TCoEquiJoin::Match(inputNode.Raw()) ||
            input->Content().Contains("ReadTable")
        )
    ) {
        return;
    }

    TTableAliasMap tableAliases;
    for (const auto& child: input->Children()) {
        auto childStats = kqpStats->GetStats(TExprBase(child).Raw());
        if (childStats && childStats->TableAliases) {
            tableAliases.Merge(*childStats->TableAliases);
        }
    }

    if (tableAliases.Empty()) {
        return;
    }

    if (stats == nullptr) {
        stats = std::make_shared<TOptimizerStatistics>();
    } else {
        stats = std::make_shared<TOptimizerStatistics>(*stats);
    }

    stats->TableAliases = MakeIntrusive<TTableAliasMap>(std::move(tableAliases));
    kqpStats->SetStats(inputNode.Raw(), std::move(stats));
}

} // namespace

IGraphTransformer::TStatus TDqStatisticsTransformerBase::DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) {
    output = input;

    VisitExprLambdasLast(
        input, [&](const TExprNode::TPtr& input) {
            BeforeLambdas(input, ctx) || BeforeLambdasSpecific(input, ctx) || BeforeLambdasUnmatched(input, ctx);

            // We have a separate rule for all callables that may use a lambda
            // we need to take each generic callable and see if it includes a lambda
            // if so - we will map the input to the callable to the argument of the lambda
            if (input->IsCallable()) {
                NDq::PropagateStatisticsToLambdaArgument(input, KqpStats);
            }

            return true;
        },
        [&](const TExprNode::TPtr& input) {
            AfterLambdas(input, ctx) || AfterLambdasSpecific(input, ctx);
            if (UseFSMForSortElimination) {
                PropogateTableAliasesFromChildren(input, KqpStats);
            }

            return true;
        });

    return IGraphTransformer::TStatus::Ok;
}

bool TDqStatisticsTransformerBase::BeforeLambdas(const TExprNode::TPtr& input, TExprContext& ctx)
{
    Y_UNUSED(ctx);
    bool matched = true;
    // Generic matchers
    if (TCoFilterBase::Match(input.Get())){
        NDq::InferStatisticsForFilter(input, KqpStats);
    }
    else if(TCoSkipNullMembers::Match(input.Get())){
        NDq::InferStatisticsForSkipNullMembers(input, KqpStats);
    }
    else if(auto aggregateBase = TMaybeNode<TCoAggregateBase>(input.Get())){
        NDq::InferStatisticsForAggregateBase(input, KqpStats, TypeCtx);
    }
    else if(TCoAggregateMergeFinalize::Match(input.Get())){
        NDq::InferStatisticsForAggregateMergeFinalize(input, KqpStats);
    }
    else if (TCoAsList::Match(input.Get())){
        NDq::InferStatisticsForAsList(input, KqpStats);
    }
    else if (TCoParameter::Match(input.Get()) && NDq::InferStatisticsForListParam(input, KqpStats)) {
    }

    // Join matchers (use NKikimr::NKqp::IProviderContext overloads from dq_opt_stat_kqp.cpp)
    else if(TCoMapJoinCore::Match(input.Get())) {
        NDq::InferStatisticsForMapJoin(input, KqpStats, Pctx, Hints);
    }
    else if(TCoGraceJoinCore::Match(input.Get())) {
        NDq::InferStatisticsForGraceJoin(input, KqpStats, Pctx, Hints, ShufflingOrderingsByJoinLabels);
    }
    else if (auto dqJoinBase = TMaybeNode<TDqJoinBase>(input.Get())) {
        NDq::InferStatisticsForDqJoinBase(input, KqpStats, Pctx, Hints);
    }
    // Do nothing in case of EquiJoin, otherwise the EquiJoin rule won't fire
    else if(TCoEquiJoin::Match(input.Get())){
        NDq::InferStatisticsForEquiJoin(input, KqpStats);
    }
    // In case of DqSource, propagate the statistics from the correct argument
    else if (TDqSource::Match(input.Get())) {
        NDq::InferStatisticsForDqSource(input, KqpStats);
    }
    else if (TDqCnMerge::Match(input.Get())) {
        NDq::InferStatisticsForDqMerge(input, KqpStats);
    }
    else if (auto extendBase = TMaybeNode<TCoExtendBase>(input)) { // == union all
        NDq::InferStatisticsForExtendBase(input, KqpStats, TypeCtx);
    }
    else if (TCoAsStruct::Match(input.Get())) {
        NDq::InferStatisticsForAsStruct(input, KqpStats);
    }
    else if (auto topBase = TMaybeNode<TCoTopBase>(input)) {
        NDq::InferStatisticsForTopBase(input, KqpStats, TypeCtx);
    }
    else if (auto sortBase = TMaybeNode<TCoSortBase>(input)) {
        NDq::InferStatisticsForSortBase(input, KqpStats, TypeCtx);
    }
    else if (TCoUnionAll::Match(input.Get())) {
        NDq::InferStatisticsForUnionAll(input, KqpStats);
    }
    else if (TCoShuffleByKeys::Match(input.Get())) {
        NDq::InferStatisticsForAggregationCallable<TCoShuffleByKeys>(input, KqpStats, TypeCtx);
    }
    else {
        matched = false;
    }

    return matched;
}

bool TDqStatisticsTransformerBase::BeforeLambdasUnmatched(const TExprNode::TPtr& input, TExprContext& ctx)
{
    Y_UNUSED(ctx);
    if (input->ChildrenSize() >= 1) {
        auto stats = KqpStats->GetStats(input->ChildRef(0).Get());
        if (stats) {
            KqpStats->SetStats(input.Get(), NDq::RemoveOrderings(stats, input));
        }
    }
    return true;
}

bool TDqStatisticsTransformerBase::AfterLambdas(const TExprNode::TPtr& input, TExprContext& ctx) {
    Y_UNUSED(ctx);
    bool matched = true;
    if (TDqStageBase::Match(input.Get())) {
        NDq::InferStatisticsForStage(input, KqpStats);
    } else if (TCoFlatMapBase::Match(input.Get())) {
        NDq::InferStatisticsForFlatMap(input, KqpStats);
    } else {
        matched = false;
    }
    return matched;
}

void TDqStatisticsTransformerBase::Rewind() { }

} // namespace NKikimr::NKqp
