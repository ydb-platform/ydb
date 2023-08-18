#include "kqp_statistics_transformer.h"
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/dq/opt/dq_opt_stat.h>


using namespace NYql;
using namespace NYql::NNodes;
using namespace NKikimr::NKqp;

/**
 * Compute statistics and cost for read table
 * Currently we just make up a number for the cardinality (100000) and set cost to 0
*/
void InferStatisticsForReadTable(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx) {

    YQL_CLOG(TRACE, CoreDq) << "Infer statistics for read table";

    auto outputStats = TOptimizerStatistics(100000, 5, 0.0);
    typeCtx->SetStats( input.Get(), std::make_shared<TOptimizerStatistics>(outputStats) );
}

/**
 * Compute sstatistics for index lookup
 * Currently we just make up a number for cardinality (5) and set cost to 0
*/
void InferStatisticsForIndexLookup(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx) {

    auto outputStats = TOptimizerStatistics(5, 5, 0.0);
    typeCtx->SetStats( input.Get(), std::make_shared<TOptimizerStatistics>(outputStats) );
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
            NDq::InferStatisticsForFlatMap(input, typeCtx);
        }
        else if(TCoSkipNullMembers::Match(input.Get())){
            NDq::InferStatisticsForSkipNullMembers(input, typeCtx);
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
