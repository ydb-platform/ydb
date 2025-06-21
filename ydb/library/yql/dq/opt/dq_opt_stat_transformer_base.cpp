#include "dq_opt_stat_transformer_base.h"

#include <ydb/library/yql/dq/opt/dq_opt_stat.h>
#include <yql/essentials/core/yql_expr_optimize.h>

#include <yql/essentials/utils/log/log.h>


namespace NYql::NDq {

using namespace NNodes;

TDqStatisticsTransformerBase::TDqStatisticsTransformerBase(
    TTypeAnnotationContext* typeCtx,
    const IProviderContext& ctx,
    const TOptimizerHints& hints
)
    : TypeCtx(typeCtx)
    , Pctx(ctx)
    , Hints(hints)
{ }

void PropogateTableAliasesFromChildren(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx) {
    auto inputNode = TExprBase(input);
    auto stats = typeCtx->GetStats(inputNode.Raw());

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
        auto childStats = typeCtx->GetStats(TExprBase(child).Raw());
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
    typeCtx->SetStats(inputNode.Raw(), std::move(stats));
}

IGraphTransformer::TStatus TDqStatisticsTransformerBase::DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) {
    output = input;

    VisitExprLambdasLast(
        input, [&](const TExprNode::TPtr& input) {
            BeforeLambdas(input, ctx) || BeforeLambdasSpecific(input, ctx) || BeforeLambdasUnmatched(input, ctx);

            // We have a separate rule for all callables that may use a lambda
            // we need to take each generic callable and see if it includes a lambda
            // if so - we will map the input to the callable to the argument of the lambda
            if (input->IsCallable()) {
                PropagateStatisticsToLambdaArgument(input, TypeCtx);
            }

            return true;
        },
        [&](const TExprNode::TPtr& input) {
            AfterLambdas(input, ctx) || AfterLambdasSpecific(input, ctx);

            PropogateTableAliasesFromChildren(input, TypeCtx);

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
        InferStatisticsForFilter(input, TypeCtx);
    }
    else if(TCoSkipNullMembers::Match(input.Get())){
        InferStatisticsForSkipNullMembers(input, TypeCtx);
    }
    else if(auto aggregateBase = TMaybeNode<TCoAggregateBase>(input.Get())){
        InferStatisticsForAggregateBase(input, TypeCtx);
    }
    else if(TCoAggregateMergeFinalize::Match(input.Get())){
        InferStatisticsForAggregateMergeFinalize(input, TypeCtx);
    }
    else if (TCoAsList::Match(input.Get())){
        InferStatisticsForAsList(input, TypeCtx);
    }
    else if (TCoParameter::Match(input.Get()) && InferStatisticsForListParam(input, TypeCtx)) {
    }

    // Join matchers
    else if(TCoMapJoinCore::Match(input.Get())) {
        InferStatisticsForMapJoin(input, TypeCtx, Pctx, Hints);
    }
    else if(TCoGraceJoinCore::Match(input.Get())) {
        InferStatisticsForGraceJoin(input, TypeCtx, Pctx, Hints);
    }
    else if (auto dqJoinBase = TMaybeNode<TDqJoinBase>(input.Get())) {
        InferStatisticsForDqJoinBase(input, TypeCtx, Pctx, Hints);
    }
    // Do nothing in case of EquiJoin, otherwise the EquiJoin rule won't fire
    else if(TCoEquiJoin::Match(input.Get())){
        InferStatisticsForEquiJoin(input, TypeCtx);
    }
    // In case of DqSource, propagate the statistics from the correct argument
    else if (TDqSource::Match(input.Get())) {
        InferStatisticsForDqSource(input, TypeCtx);
    }
    else if (TDqCnMerge::Match(input.Get())) {
        InferStatisticsForDqMerge(input, TypeCtx);
    }
    else if (auto extendBase = TMaybeNode<TCoExtendBase>(input)) { // == union all
        InferStatisticsForExtendBase(input, TypeCtx);
    }
    else if (TCoAsStruct::Match(input.Get())) {
        InferStatisticsForAsStruct(input, TypeCtx);
    }
    else if (auto topBase = TMaybeNode<TCoTopBase>(input)) {
        InferStatisticsForTopBase(input, TypeCtx);
    }
    else if (auto sortBase = TMaybeNode<TCoSortBase>(input)) {
        InferStatisticsForSortBase(input, TypeCtx);
    }
    else if (TCoUnionAll::Match(input.Get())) {
        InferStatisticsForUnionAll(input, TypeCtx);
    }
    else if (TCoShuffleByKeys::Match(input.Get())) {
        InferStatisticsForAggregationCallable<TCoShuffleByKeys>(input, TypeCtx);
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
        auto stats = TypeCtx->GetStats(input->ChildRef(0).Get());
        if (stats) {
            TypeCtx->SetStats(input.Get(), RemoveOrderings(stats, input));
        }
    }
    return true;
}

bool TDqStatisticsTransformerBase::AfterLambdas(const TExprNode::TPtr& input, TExprContext& ctx) {
    Y_UNUSED(ctx);
    bool matched = true;
    if (TDqStageBase::Match(input.Get())) {
        InferStatisticsForStage(input, TypeCtx);
    } else if (TCoFlatMapBase::Match(input.Get())) {
        InferStatisticsForFlatMap(input, TypeCtx);
    } else {
        matched = false;
    }
    return matched;
}

void TDqStatisticsTransformerBase::Rewind() { }

} // namespace NYql::NDq
