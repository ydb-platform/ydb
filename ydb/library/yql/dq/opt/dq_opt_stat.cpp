#include "dq_opt_stat.h"

#include <ydb/library/yql/core/yql_opt_utils.h>

namespace NYql::NDq {

using namespace NNodes;

/**
 * For Flatmap we check the input and fetch the statistcs and cost from below
 * Then we analyze the filter predicate and compute it's selectivity and apply it
 * to the result.
 */
void InferStatisticsForFlatMap(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx) {

    auto inputNode = TExprBase(input);
    auto flatmap = inputNode.Cast<TCoFlatMap>();
    if (!IsPredicateFlatMap(flatmap.Lambda().Body().Ref())) {
        return;
    }

    auto flatmapInput = flatmap.Input();
    auto inputStats = typeCtx->GetStats(flatmapInput.Raw());

    if (! inputStats ) {
        return;
    }

    // Selectivity is the fraction of tuples that are selected by this predicate
    // Currently we just set the number to 10% before we have statistics and parse
    // the predicate
    double selectivity = 0.1;

    auto outputStats = TOptimizerStatistics(inputStats->Nrows * selectivity, inputStats->Ncols);

    typeCtx->SetStats(input.Get(), std::make_shared<TOptimizerStatistics>(outputStats) );
    typeCtx->SetCost(input.Get(), typeCtx->GetCost(flatmapInput.Raw()));
}

/**
 * Infer statistics and costs for SkipNullMembers
 * We don't have a good idea at this time how many nulls will be discarded, so we just return the
 * input statistics.
 */
void InferStatisticsForSkipNullMembers(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx) {

    auto inputNode = TExprBase(input);
    auto skipNullMembers = inputNode.Cast<TCoSkipNullMembers>();
    auto skipNullMembersInput = skipNullMembers.Input();

    auto inputStats = typeCtx->GetStats(skipNullMembersInput.Raw());
    if (!inputStats) {
        return;
    }

    typeCtx->SetStats( input.Get(), inputStats );
    typeCtx->SetCost( input.Get(), typeCtx->GetCost( skipNullMembersInput.Raw() ) );
}

/**
 * Infer statistics and costs for ExtractlMembers
 * We just return the input statistics.
*/
void InferStatisticsForExtractMembers(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx) {

    auto inputNode = TExprBase(input);
    auto extractMembers = inputNode.Cast<TCoExtractMembers>();
    auto extractMembersInput = extractMembers.Input();

    auto inputStats = typeCtx->GetStats(extractMembersInput.Raw() );
    if (!inputStats) {
        return;
    }

    typeCtx->SetStats( input.Get(), inputStats );
    typeCtx->SetCost( input.Get(), typeCtx->GetCost( extractMembersInput.Raw() ) );
}

/**
 * Infer statistics and costs for AggregateCombine
 * We just return the input statistics.
*/
void InferStatisticsForAggregateCombine(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx) {

    auto inputNode = TExprBase(input);
    auto agg = inputNode.Cast<TCoAggregateCombine>();
    auto aggInput = agg.Input();

    auto inputStats = typeCtx->GetStats(aggInput.Raw());
    if (!inputStats) {
        return;
    }

    typeCtx->SetStats( input.Get(), inputStats );
    typeCtx->SetCost( input.Get(), typeCtx->GetCost( aggInput.Raw() ) );
}

/**
 * Infer statistics and costs for AggregateMergeFinalize
 * Just return input stats
*/
void InferStatisticsForAggregateMergeFinalize(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx) {

    auto inputNode = TExprBase(input);
    auto agg = inputNode.Cast<TCoAggregateMergeFinalize>();
    auto aggInput = agg.Input();

    auto inputStats = typeCtx->GetStats(aggInput.Raw() );
    if (!inputStats) {
        return;
    }

    typeCtx->SetStats( input.Get(), inputStats );
    typeCtx->SetCost( input.Get(), typeCtx->GetCost( aggInput.Raw() ) );
}

} // namespace NYql::NDq {
