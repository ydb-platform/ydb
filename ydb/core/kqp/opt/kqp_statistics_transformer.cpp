#include "kqp_statistics_transformer.h"
#include <ydb/library/yql/utils/log/log.h>


using namespace NYql;
using namespace NYql::NNodes;
using namespace NKikimr::NKqp;

namespace {

/**
 * Helper method to fetch statistics from type annotation context
*/
std::shared_ptr<TOptimizerStatistics> GetStats( const TExprNode* input, TTypeAnnotationContext* typeCtx ) {

    return typeCtx->StatisticsMap.Value(input, std::shared_ptr<TOptimizerStatistics>(nullptr));
}

/**
 * Helper method to set statistics in type annotation context
*/
void SetStats( const TExprNode* input, TTypeAnnotationContext* typeCtx, std::shared_ptr<TOptimizerStatistics> stats ) {

    typeCtx->StatisticsMap[input] = stats;
}

/**
 * Helper method to get cost from type annotation context
 * Doesn't check if the cost is in the mapping
*/
std::optional<double> GetCost( const TExprNode* input, TTypeAnnotationContext* typeCtx ) {
    return typeCtx->StatisticsMap[input]->Cost;
}

/**
 * Helper method to set the cost in type annotation context
*/
void SetCost( const TExprNode* input, TTypeAnnotationContext* typeCtx, std::optional<double> cost ) {
    typeCtx->StatisticsMap[input]->Cost = cost;
}
}

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
    auto inputStats = GetStats(flatmapInput.Raw(), typeCtx);

    if (! inputStats ) {
        return;
    }

    // Selectivity is the fraction of tuples that are selected by this predicate
    // Currently we just set the number to 10% before we have statistics and parse
    // the predicate
    double selectivity = 0.1;

    auto outputStats = TOptimizerStatistics(inputStats->Nrows * selectivity, inputStats->Ncols);

    SetStats(input.Get(), typeCtx, std::make_shared<TOptimizerStatistics>(outputStats) );
    SetCost(input.Get(), typeCtx, GetCost(flatmapInput.Raw(), typeCtx));
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

    auto inputStats = GetStats(skipNullMembersInput.Raw(), typeCtx);
    if (!inputStats) {
        return;
    }

    SetStats( input.Get(), typeCtx, inputStats );
    SetCost( input.Get(), typeCtx, GetCost( skipNullMembersInput.Raw(), typeCtx ) );
}

/**
 * Compute statistics and cost for read table
 * Currently we just make up a number for the cardinality (100000) and set cost to 0
*/
void InferStatisticsForReadTable(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx) {

    YQL_CLOG(TRACE, CoreDq) << "Infer statistics for read table";

    auto outputStats = TOptimizerStatistics(100000, 5, 0.0);
    SetStats( input.Get(), typeCtx, std::make_shared<TOptimizerStatistics>(outputStats) );
}

/**
 * Compute sstatistics for index lookup
 * Currently we just make up a number for cardinality (5) and set cost to 0
*/
void InferStatisticsForIndexLookup(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx) {

    auto outputStats = TOptimizerStatistics(5, 5, 0.0);
    SetStats( input.Get(), typeCtx, std::make_shared<TOptimizerStatistics>(outputStats) );
}

/**
 * DoTransform method matches operators and callables in the query DAG and
 * uses pre-computed statistics and costs of the children to compute their cost.
*/
IGraphTransformer::TStatus TKqpStatisticsTransformer::DoTransform(TExprNode::TPtr input, 
    TExprNode::TPtr& output, TExprContext& ctx) {

    output = input;
    if (!Config->HasOptEnableCostBasedOptimization()) {
        return IGraphTransformer::TStatus::Ok;
    }
          
    TOptimizeExprSettings settings(nullptr);

    auto ret = OptimizeExpr(input, output, [*this](const TExprNode::TPtr& input, TExprContext& ctx) {
        Y_UNUSED(ctx);
        auto output = input;

        if (TCoFlatMap::Match(input.Get())){
            InferStatisticsForFlatMap(input, typeCtx);
        }
        else if(TCoSkipNullMembers::Match(input.Get())){
            InferStatisticsForSkipNullMembers(input, typeCtx);
        }
        else if(TKqlReadTableBase::Match(input.Get()) || TKqlReadTableRangesBase::Match(input.Get())){
            InferStatisticsForReadTable(input, typeCtx);
        }
        else if(TKqlLookupTableBase::Match(input.Get()) || TKqlLookupIndexBase::Match(input.Get())){
            InferStatisticsForIndexLookup(input, typeCtx);
        }

        return output;
    }, ctx, settings);

    return ret;
}

TAutoPtr<IGraphTransformer> NKikimr::NKqp::CreateKqpStatisticsTransformer(TTypeAnnotationContext& typeCtx, 
    const TKikimrConfiguration::TPtr& config) {

    return THolder<IGraphTransformer>(new TKqpStatisticsTransformer(typeCtx, config));
}