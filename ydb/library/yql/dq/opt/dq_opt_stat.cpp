#include "dq_opt_stat.h"

#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/core/yql_cost_function.h>
#include <ydb/library/yql/utils/log/log.h>


namespace NYql::NDq {

using namespace NNodes;

/**
 * Compute statistics for map join
 * FIX: Currently we treat all join the same from the cost perspective, need to refine cost function
 */
void InferStatisticsForMapJoin(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx) {
    auto inputNode = TExprBase(input);
    auto join = inputNode.Cast<TCoMapJoinCore>();

    auto leftArg = join.LeftInput();
    auto rightArg = join.RightDict();

    auto leftStats = typeCtx->GetStats(leftArg.Raw());
    auto rightStats = typeCtx->GetStats(rightArg.Raw());

    if (!leftStats || !rightStats) {
        return;
    }

    typeCtx->SetStats(join.Raw(), std::make_shared<TOptimizerStatistics>(
                                      ComputeJoinStats(*leftStats, *rightStats, MapJoin)));
}

/**
 * Compute statistics for grace join
 * FIX: Currently we treat all join the same from the cost perspective, need to refine cost function
 */
void InferStatisticsForGraceJoin(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx) {
    auto inputNode = TExprBase(input);
    auto join = inputNode.Cast<TCoGraceJoinCore>();

    auto leftArg = join.LeftInput();
    auto rightArg = join.RightInput();

    auto leftStats = typeCtx->GetStats(leftArg.Raw());
    auto rightStats = typeCtx->GetStats(rightArg.Raw());

    if (!leftStats || !rightStats) {
        return;
    }

    typeCtx->SetStats(join.Raw(), std::make_shared<TOptimizerStatistics>(
                                      ComputeJoinStats(*leftStats, *rightStats, GraceJoin)));
}

/**
 * Infer statistics for DqSource
 *
 * We just pass up the statistics from the Settings of the DqSource
 */
void InferStatisticsForDqSource(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx) {
    auto inputNode = TExprBase(input);
    auto dqSource = inputNode.Cast<TDqSource>();
    auto inputStats = typeCtx->GetStats(dqSource.Settings().Raw());
    if (!inputStats) {
        return;
    }

    typeCtx->SetStats(input.Get(), inputStats);
    typeCtx->SetCost(input.Get(), typeCtx->GetCost(dqSource.Settings().Raw()));
}

/**
 * For Flatmap we check the input and fetch the statistcs and cost from below
 * Then we analyze the filter predicate and compute it's selectivity and apply it
 * to the result.
 * 
 * If this flatmap's lambda is a join, we propagate the join result as the output of FlatMap
 */
void InferStatisticsForFlatMap(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx) {

    auto inputNode = TExprBase(input);
    auto flatmap = inputNode.Cast<TCoFlatMapBase>();
    auto flatmapInput = flatmap.Input();
    auto inputStats = typeCtx->GetStats(flatmapInput.Raw());

    if (! inputStats ) {
        return;
    }

    if (IsPredicateFlatMap(flatmap.Lambda().Body().Ref())) {
        // Selectivity is the fraction of tuples that are selected by this predicate
        // Currently we just set the number to 10% before we have statistics and parse
        // the predicate
        double selectivity = 0.1;

        auto outputStats = TOptimizerStatistics(inputStats->Nrows * selectivity, inputStats->Ncols);

        typeCtx->SetStats(input.Get(), std::make_shared<TOptimizerStatistics>(outputStats) );
        typeCtx->SetCost(input.Get(), typeCtx->GetCost(flatmapInput.Raw()));

    }
    else if (flatmap.Lambda().Body().Maybe<TCoMapJoinCore>() || 
            flatmap.Lambda().Body().Maybe<TCoMap>().Input().Maybe<TCoMapJoinCore>() ||
            flatmap.Lambda().Body().Maybe<TCoJoinDict>() ||
            flatmap.Lambda().Body().Maybe<TCoMap>().Input().Maybe<TCoJoinDict>()){

        typeCtx->SetStats(input.Get(), typeCtx->GetStats(flatmap.Lambda().Body().Raw()));
    }
    else {
        typeCtx->SetStats(input.Get(), typeCtx->GetStats(flatmapInput.Raw()));
    }
}

/**
 * For Filter we check the input and fetch the statistcs and cost from below
 * Then we analyze the filter predicate and compute it's selectivity and apply it
 * to the result, just like in FlatMap, except we check for a specific pattern:
 * If the filter's lambda is an Exists callable with a Member callable, we set the
 * selectivity to 1 to be consistent with SkipNullMembers in the logical plan
 */
void InferStatisticsForFilter(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx) {

    auto inputNode = TExprBase(input);
    auto filter = inputNode.Cast<TCoFilterBase>();
    auto filterInput = filter.Input();
    auto inputStats = typeCtx->GetStats(filterInput.Raw());

    if (!inputStats){
        return;
    }

    // Selectivity is the fraction of tuples that are selected by this predicate
    // Currently we just set the number to 10% before we have statistics and parse
    // the predicate
    double selectivity = 0.1;

    auto filterLambda = filter.Lambda();
    if (auto exists = filterLambda.Body().Maybe<TCoExists>()) {
        if (exists.Cast().Optional().Maybe<TCoMember>()) {
            selectivity = 1.0;
        }
    }

    auto outputStats = TOptimizerStatistics(inputStats->Nrows * selectivity, inputStats->Ncols);

    typeCtx->SetStats(input.Get(), std::make_shared<TOptimizerStatistics>(outputStats) );
    typeCtx->SetCost(input.Get(), typeCtx->GetCost(filterInput.Raw()));

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

/***
 * For callables that include lambdas, we want to propagate the statistics from lambda's input to its argument, so
 * that the operators inside lambda receive the correct statistics
*/
void PropagateStatisticsToLambdaArgument(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx) {

    if (input->ChildrenSize()<2) {
        return;
    }

    auto callableInput = input->ChildRef(0);

    // Iterate over all children except for the input
    // Check if the child is a lambda and propagate the statistics into it
    for (size_t i=1; i<input->ChildrenSize(); i++) {
        auto maybeLambda = TExprBase(input->ChildRef(i));
        if (!maybeLambda.Maybe<TCoLambda>()) {
            continue;
        }

        auto lambda = maybeLambda.Cast<TCoLambda>();
        if (!lambda.Args().Size()){
            continue;
        }

        // If the input to the callable is a list, then lambda also takes a list of arguments
        // So we need to propagate corresponding arguments

        if (callableInput->IsList()){
            for(size_t j=0; j<callableInput->ChildrenSize(); j++){
                auto inputStats = typeCtx->GetStats(callableInput->Child(j) );
                if (inputStats){
                    typeCtx->SetStats( lambda.Args().Arg(j).Raw(), inputStats );
                    typeCtx->SetCost( lambda.Args().Arg(j).Raw(), typeCtx->GetCost( callableInput->Child(j) ));
                }
            }
            
        }
        else {
            auto inputStats = typeCtx->GetStats(callableInput.Get());
            if (!inputStats) {
                return;
            }

            typeCtx->SetStats( lambda.Args().Arg(0).Raw(), inputStats );
            typeCtx->SetCost( lambda.Args().Arg(0).Raw(), typeCtx->GetCost( callableInput.Get() ));
        }
    }
}

/**
 * After processing the lambda for the stage we set the stage output to the result of the lambda
*/
void InferStatisticsForStage(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx) {
    auto inputNode = TExprBase(input);
    auto stage = inputNode.Cast<TDqStageBase>();

    auto lambdaStats = typeCtx->GetStats( stage.Program().Body().Raw());
    if (lambdaStats){
        typeCtx->SetStats( stage.Raw(), lambdaStats );
        typeCtx->SetCost( stage.Raw(), typeCtx->GetCost( stage.Program().Body().Raw()));
    }
}

} // namespace NYql::NDq {
