#include "kqp_statistics_transformer.h"
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/dq/opt/dq_opt_stat.h>


using namespace NYql;
using namespace NYql::NNodes;
using namespace NKikimr::NKqp;
using namespace NYql::NDq;

/**
 * Compute statistics and cost for read table
 * Currently we look up the number of rows and attributes in the statistics service
*/
void InferStatisticsForReadTable(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx,
    const TKqpOptimizeContext& kqpCtx) {

    auto inputNode = TExprBase(input);
    double nRows = 0;
    int nAttrs = 0;

    const TExprNode* path;

    if ( auto readTable = inputNode.Maybe<TKqlReadTableBase>()){
        path = readTable.Cast().Table().Path().Raw();
        nAttrs = readTable.Cast().Columns().Size();
    } else if(auto readRanges = inputNode.Maybe<TKqlReadTableRangesBase>()){
        path = readRanges.Cast().Table().Path().Raw();
        nAttrs = readRanges.Cast().Columns().Size();
    } else {
        Y_ENSURE(false,"Invalid node type for InferStatisticsForReadTable");
    }

    const auto& tableData = kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, path->Content());
    nRows = tableData.Metadata->RecordsCount;
    YQL_CLOG(TRACE, CoreDq) << "Infer statistics for read table, nrows:" << nRows << ", nattrs: " << nAttrs;

    auto outputStats = TOptimizerStatistics(nRows, nAttrs, 0.0);
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
            InferStatisticsForFlatMap(input, TypeCtx);
        }
        else if(TCoSkipNullMembers::Match(input.Get())){
            InferStatisticsForSkipNullMembers(input, TypeCtx);
        }
        else if(TCoExtractMembers::Match(input.Get())){
            InferStatisticsForExtractMembers(input, TypeCtx);
        }
        else if(TCoAggregateCombine::Match(input.Get())){
            InferStatisticsForAggregateCombine(input, TypeCtx);
        }
        else if(TCoAggregateMergeFinalize::Match(input.Get())){
            InferStatisticsForAggregateMergeFinalize(input, TypeCtx);
        }
        else if(TKqlReadTableBase::Match(input.Get()) || TKqlReadTableRangesBase::Match(input.Get())){
            InferStatisticsForReadTable(input, TypeCtx, KqpCtx);
        }
        else if(TKqlLookupTableBase::Match(input.Get()) || TKqlLookupIndexBase::Match(input.Get())){
            InferStatisticsForIndexLookup(input, TypeCtx);
        }

        return output;
    }, ctx, settings);

    return ret;
}

TAutoPtr<IGraphTransformer> NKikimr::NKqp::CreateKqpStatisticsTransformer(const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx,
    TTypeAnnotationContext& typeCtx, const TKikimrConfiguration::TPtr& config) {

    return THolder<IGraphTransformer>(new TKqpStatisticsTransformer(kqpCtx, typeCtx, config));
}
