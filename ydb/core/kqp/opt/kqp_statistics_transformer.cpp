#include "kqp_statistics_transformer.h"
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/dq/opt/dq_opt_stat.h>
#include <ydb/library/yql/core/yql_cost_function.h>

#include <charconv>

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

    if (auto readTable = inputNode.Maybe<TKqlReadTableBase>()) {
        path = readTable.Cast().Table().Path().Raw();
        nAttrs = readTable.Cast().Columns().Size();
    } else if (auto readRanges = inputNode.Maybe<TKqlReadTableRangesBase>()) {
        path = readRanges.Cast().Table().Path().Raw();
        nAttrs = readRanges.Cast().Columns().Size();
    } else {
        Y_ENSURE(false, "Invalid node type for InferStatisticsForReadTable");
    }

    const auto& tableData = kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, path->Content());
    nRows = tableData.Metadata->RecordsCount;

    YQL_CLOG(TRACE, CoreDq) << "Infer statistics for read table, nrows:" << nRows << ", nattrs: " << nAttrs;

    auto outputStats = TOptimizerStatistics(EStatisticsType::BaseTable, nRows, nAttrs, 0.0, tableData.Metadata->KeyColumnNames);
    typeCtx->SetStats(input.Get(), std::make_shared<TOptimizerStatistics>(outputStats));
}

/**
 * Infer statistics for KQP table
 */
void InferStatisticsForKqpTable(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx,
    const TKqpOptimizeContext& kqpCtx) {

    auto inputNode = TExprBase(input);
    auto readTable = inputNode.Cast<TKqpTable>();
    auto path = readTable.Path();

    const auto& tableData = kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, path.Value());
    double nRows = tableData.Metadata->RecordsCount;
    int nAttrs = tableData.Metadata->Columns.size();
    YQL_CLOG(TRACE, CoreDq) << "Infer statistics for table: " << path.Value() << ", nrows: " << nRows << ", nattrs: " << nAttrs;

    auto outputStats = TOptimizerStatistics(EStatisticsType::BaseTable, nRows, nAttrs, 0.0, tableData.Metadata->KeyColumnNames);
    typeCtx->SetStats(input.Get(), std::make_shared<TOptimizerStatistics>(outputStats));
}

/**
 * Infer statistics for TableLookup
 *
 * Table lookup can be done with an Iterator, in which case we treat it as a full scan
 * We don't differentiate between a small range and full scan at this time
 */
void InferStatisticsForLookupTable(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx) {
    auto inputNode = TExprBase(input);
    auto lookupTable = inputNode.Cast<TKqlLookupTableBase>();

    int nAttrs = lookupTable.Columns().Size();
    double nRows = 0;

    auto inputStats = typeCtx->GetStats(lookupTable.Table().Raw());

    if (lookupTable.LookupKeys().Maybe<TCoIterator>()) {
        if (inputStats) {
            nRows = inputStats->Nrows;
        } else {
            return;
        }
    } else {
        nRows = 1;
    }

    auto outputStats = TOptimizerStatistics(EStatisticsType::BaseTable, nRows, nAttrs, 0, inputStats->KeyColumns);
    typeCtx->SetStats(input.Get(), std::make_shared<TOptimizerStatistics>(outputStats));
}

/**
 * Compute statistics for RowsSourceSetting
 * We look into range expression to check if its a point lookup or a full scan
 * We currently don't try to figure out whether this is a small range vs full scan
 */
void InferStatisticsForRowsSourceSettings(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx) {
    auto inputNode = TExprBase(input);
    auto sourceSettings = inputNode.Cast<TKqpReadRangesSourceSettings>();

    auto inputStats = typeCtx->GetStats(sourceSettings.Table().Raw());
    if (!inputStats) {
        return;
    }

    double nRows = inputStats->Nrows;

    // Check if we have a range expression, in that case just assign a single row to this read
    // We don't currently check the size of an index lookup
    if (sourceSettings.RangesExpr().Maybe<TKqlKeyRange>()) {
        auto range = sourceSettings.RangesExpr().Cast<TKqlKeyRange>();
        auto maybeFromKey = range.From().Maybe<TKqlKeyTuple>();
        auto maybeToKey = range.To().Maybe<TKqlKeyTuple>();
        if (maybeFromKey && maybeToKey) {
            nRows = 1;
        }
    }

    int nAttrs = sourceSettings.Columns().Size();
    double cost = inputStats->Cost;

    auto outputStats = TOptimizerStatistics(EStatisticsType::BaseTable, nRows, nAttrs, cost, inputStats->KeyColumns);
    typeCtx->SetStats(input.Get(), std::make_shared<TOptimizerStatistics>(outputStats));
}

/**
 * Compute statistics for index lookup
 * Currently we just make up a number for cardinality (5) and set cost to 0
 */
void InferStatisticsForIndexLookup(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx) {
    auto outputStats = TOptimizerStatistics(5, 5, 0.0);
    typeCtx->SetStats(input.Get(), std::make_shared<TOptimizerStatistics>(outputStats));
}

/***
 * Infer statistics for result binding of a stage
 */
void InferStatisticsForResultBinding(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx, 
    TVector<TVector<std::shared_ptr<TOptimizerStatistics>>>& txStats) {

    auto inputNode = TExprBase(input);
    auto param = inputNode.Cast<TCoParameter>();

    if (param.Name().Maybe<TCoAtom>()) {
        auto atom = param.Name().Cast<TCoAtom>();
        if (atom.Value().StartsWith("%kqp%tx_result_binding")) {
            TStringBuf suffix;
            atom.Value().AfterPrefix("%kqp%tx_result_binding_", suffix);
            TStringBuf bindingNoStr, resultNoStr;
            suffix.Split('_', bindingNoStr, resultNoStr);

            int bindingNo;
            int resultNo;
            std::from_chars(bindingNoStr.data(), bindingNoStr.data() + bindingNoStr.size(), bindingNo);
            std::from_chars(resultNoStr.data(), resultNoStr.data() + resultNoStr.size(), resultNo);

            typeCtx->SetStats(param.Name().Raw(), txStats[bindingNo][resultNo]);
            typeCtx->SetStats(inputNode.Raw(), txStats[bindingNo][resultNo]);
        }
    }
}

/**
 * When encountering a KqpPhysicalTx, we save the results of the stage in a vector
 * where it can later be accessed via binding parameters
 */
void AppendTxStats(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx, 
    TVector<TVector<std::shared_ptr<TOptimizerStatistics>>>& txStats) {

    auto inputNode = TExprBase(input);
    auto tx = inputNode.Cast<TKqpPhysicalTx>();
    TVector<std::shared_ptr<TOptimizerStatistics>> vec;

    for (size_t i = 0; i < tx.Results().Size(); i++) {
        vec.push_back(typeCtx->GetStats(tx.Results().Item(i).Raw()));
    }

    txStats.push_back(vec);
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

    TxStats.clear();
    return TDqStatisticsTransformerBase::DoTransform(input, output, ctx);
}

bool TKqpStatisticsTransformer::BeforeLambdasSpecific(const TExprNode::TPtr& input, TExprContext& ctx) {
    Y_UNUSED(ctx);
    bool matched = true;
    // KQP Matchers
    if(TKqlReadTableBase::Match(input.Get()) || TKqlReadTableRangesBase::Match(input.Get())){
        InferStatisticsForReadTable(input, TypeCtx, KqpCtx);
    }
    else if(TKqlLookupTableBase::Match(input.Get())) {
        InferStatisticsForLookupTable(input, TypeCtx);
    }
    else if(TKqlLookupIndexBase::Match(input.Get())){
        InferStatisticsForIndexLookup(input, TypeCtx);
    }
    else if(TKqpTable::Match(input.Get())) {
        InferStatisticsForKqpTable(input, TypeCtx, KqpCtx);
    }
    else if (TKqpReadRangesSourceSettings::Match(input.Get())) {
        InferStatisticsForRowsSourceSettings(input, TypeCtx);
    }

    // Match a result binding atom and connect it to a stage
    else if(TCoParameter::Match(input.Get())) {
        InferStatisticsForResultBinding(input, TypeCtx, TxStats);
    }
    else {
        matched = false;
    }

    return matched;
}

bool TKqpStatisticsTransformer::AfterLambdasSpecific(const TExprNode::TPtr& input, TExprContext& ctx) {
    Y_UNUSED(ctx);
    bool matched = true;
    if (TKqpPhysicalTx::Match(input.Get())) {
        AppendTxStats(input, TypeCtx, TxStats);
    } else {
        matched = false;
    }
    return matched;
}

TAutoPtr<IGraphTransformer> NKikimr::NKqp::CreateKqpStatisticsTransformer(const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx,
    TTypeAnnotationContext& typeCtx, const TKikimrConfiguration::TPtr& config) {
    return THolder<IGraphTransformer>(new TKqpStatisticsTransformer(kqpCtx, typeCtx, config));
}
