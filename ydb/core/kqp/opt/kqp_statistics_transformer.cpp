#include "kqp_statistics_transformer.h"
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/dq/opt/dq_opt_stat.h>
#include <ydb/library/yql/core/yql_cost_function.h>

#include <ydb/library/yql/providers/dq/common/yql_dq_settings.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <ydb/library/yql/providers/s3/expr_nodes/yql_s3_expr_nodes.h>

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
    std::shared_ptr<TOptimizerStatistics> inputStats;

    int nAttrs = 0;
    bool readRange = false;

    if (auto readTable = inputNode.Maybe<TKqlReadTableBase>()) {
        inputStats = typeCtx->GetStats(readTable.Cast().Table().Raw());
        nAttrs = readTable.Cast().Columns().Size();

        auto range = readTable.Cast().Range();
        auto rangeFrom = range.From().Maybe<TKqlKeyTuple>();
        auto rangeTo = range.To().Maybe<TKqlKeyTuple>();
        if (rangeFrom && rangeTo) {
            readRange = true;
        }
    } else if (auto readRanges = inputNode.Maybe<TKqlReadTableRangesBase>()) {
        inputStats = typeCtx->GetStats(readRanges.Cast().Table().Raw());
        nAttrs = readRanges.Cast().Columns().Size();
    } else {
        Y_ENSURE(false, "Invalid node type for InferStatisticsForReadTable");
    }

    if (!inputStats) {
        return;
    }

    auto keyColumns = inputStats->KeyColumns;
    if (auto indexRead = inputNode.Maybe<TKqlReadTableIndex>()) {
        const auto& tableData = kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, indexRead.Cast().Table().Path().Value());
        const auto& [indexMeta, _] = tableData.Metadata->GetIndexMetadata(indexRead.Cast().Index().StringValue());

        keyColumns = TIntrusivePtr<TOptimizerStatistics::TKeyColumns>(
            new TOptimizerStatistics::TKeyColumns(indexMeta->KeyColumnNames));
    }

    /**
     * We need index statistics to calculate this in the future
     * Right now we use very small estimates to make sure CBO picks Lookup Joins
     * I.e. there can be a chain of lookup joins in OLTP scenario and we want to make
     * sure the cardinality doesn't blow up and lookup joins are still being picked
     */
    double inputRows = inputStats->Nrows;
    double nRows = inputRows;
    if (readRange) {
        nRows = 1;
    }

    double sizePerRow = inputStats->ByteSize / (inputRows==0?1:inputRows);
    double byteSize = nRows * sizePerRow * (nAttrs / (double)inputStats->Ncols);

    auto stats = std::make_shared<TOptimizerStatistics>(
        EStatisticsType::BaseTable, 
        nRows, 
        nAttrs, 
        byteSize, 
        0.0, 
        keyColumns,
        inputStats->ColumnStatistics);

    YQL_CLOG(TRACE, CoreDq) << "Infer statistics for read table" << stats->ToString();

    typeCtx->SetStats(input.Get(), stats);
}

/**
 * Infer statistics for KQP table
 */
void InferStatisticsForKqpTable(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx,
    TKqpOptimizeContext& kqpCtx) {

    auto inputNode = TExprBase(input);
    auto readTable = inputNode.Cast<TKqpTable>();
    auto path = readTable.Path();

    const auto& tableData = kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, path.Value());
    if (!tableData.Metadata->StatsLoaded && !kqpCtx.Config->OptOverrideStatistics.Get()) {
        return;
    }

    double nRows = tableData.Metadata->RecordsCount;
    double byteSize = tableData.Metadata->DataSize;
    int nAttrs = tableData.Metadata->Columns.size();

    auto keyColumns = TIntrusivePtr<TOptimizerStatistics::TKeyColumns>(new TOptimizerStatistics::TKeyColumns(tableData.Metadata->KeyColumnNames));
    auto stats = std::make_shared<TOptimizerStatistics>(EStatisticsType::BaseTable, nRows, nAttrs, byteSize, 0.0, keyColumns);
    if (typeCtx->ColumnStatisticsByTableName.contains(path.StringValue())) {
        stats->ColumnStatistics = typeCtx->ColumnStatisticsByTableName[path.StringValue()];
    }
    if (kqpCtx.Config->OptOverrideStatistics.Get()) {
        stats = OverrideStatistics(*stats, path.Value(), kqpCtx.GetOverrideStatistics());
    }
    if (stats->ColumnStatistics) {
        for (const auto& [columnName, metaData]: tableData.Metadata->Columns) {
            stats->ColumnStatistics->Data[columnName].Type = metaData.Type;
        }
    }

    YQL_CLOG(TRACE, CoreDq) << "Infer statistics for table: " << path.Value() << ": " << stats->ToString();

    typeCtx->SetStats(input.Get(), stats);
}

/**
 * Infer statistic for Kqp steam lookup operator
 * 
 * In reality we want to compute the number of rows and cost that the lookyup actually performed.
 * But currently we just take the data from the base table, and join cardinality will still work correctly,
 * because it considers joins on PK.
 * 
 * In the future it would be better to compute the actual cardinality
*/
void InferStatisticsForSteamLookup(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx) {
    auto inputNode = TExprBase(input);
    auto streamLookup = inputNode.Cast<TKqpCnStreamLookup>();

    int nAttrs = streamLookup.Columns().Size();
    auto inputStats = typeCtx->GetStats(streamLookup.Table().Raw());
    if (!inputStats) {
        return;
    }
    auto byteSize = inputStats->ByteSize * (nAttrs / (double) inputStats->Ncols);

    auto res = std::make_shared<TOptimizerStatistics>(
        EStatisticsType::BaseTable, 
        inputStats->Nrows, 
        nAttrs, 
        byteSize, 
        0, 
        inputStats->KeyColumns,
        inputStats->ColumnStatistics);

    typeCtx->SetStats(input.Get(), res); 

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
    double byteSize = 0;

    auto inputStats = typeCtx->GetStats(lookupTable.Table().Raw());
    if (!inputStats) {
        return;
    }

    if (lookupTable.LookupKeys().Maybe<TCoIterator>()) {
        if (inputStats) {
            nRows = inputStats->Nrows;
            byteSize = inputStats->ByteSize * (nAttrs / (double) inputStats->Ncols);
        } else {
            return;
        }
    } else {
        nRows = 1;
        byteSize = 10;
    }

    typeCtx->SetStats(input.Get(), std::make_shared<TOptimizerStatistics>(
        EStatisticsType::BaseTable, 
        nRows,
        nAttrs, 
        byteSize, 
        0, 
        inputStats->KeyColumns,
        inputStats->ColumnStatistics));
}

/**
 * Compute statistics for RowsSourceSetting
 * We look into range expression to check if its a point lookup or a full scan
 * We currently don't try to figure out whether this is a small range vs full scan
 */
void InferStatisticsForRowsSourceSettings(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx,
    const TKqpOptimizeContext& kqpCtx) {

    auto inputNode = TExprBase(input);
    auto sourceSettings = inputNode.Cast<TKqpReadRangesSourceSettings>();

    auto inputStats = typeCtx->GetStats(sourceSettings.Table().Raw());
    if (!inputStats) {
        return;
    }

    double inputRows =  inputStats->Nrows;
    double nRows = inputRows;

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

    auto keyColumns = inputStats->KeyColumns;
    if (auto indexRead = inputNode.Maybe<TKqlReadTableIndexRanges>()) {
        const auto& tableData = kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, indexRead.Cast().Table().Path().Value());
        const auto& [indexMeta, _] = tableData.Metadata->GetIndexMetadata(indexRead.Cast().Index().StringValue());

        keyColumns = TIntrusivePtr<TOptimizerStatistics::TKeyColumns>(
            new TOptimizerStatistics::TKeyColumns(indexMeta->KeyColumnNames));
    }

    int nAttrs = sourceSettings.Columns().Size();

    double sizePerRow = inputStats->ByteSize / (inputRows==0?1:inputRows);
    double byteSize = nRows * sizePerRow * (nAttrs / (double)inputStats->Ncols);
    double cost = inputStats->Cost;

    typeCtx->SetStats(input.Get(), std::make_shared<TOptimizerStatistics>(
        EStatisticsType::BaseTable, 
        nRows, 
        nAttrs, 
        byteSize, 
        cost, 
        keyColumns, 
        inputStats->ColumnStatistics));
}

/**
 * Compute statistics for index lookup
 * Currently we just make up a number for cardinality (5) and set cost to 0
 */
void InferStatisticsForIndexLookup(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx) {
    typeCtx->SetStats(input.Get(), std::make_shared<TOptimizerStatistics>(EStatisticsType::BaseTable, 5, 5, 20, 0.0));
}

void InferStatisticsForReadTableIndexRanges(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx) {
    auto indexRanges = TKqlReadTableIndexRanges(input);

    auto inputStats = typeCtx->GetStats(indexRanges.Table().Raw());
    if (!inputStats) {
        return;
    }
    
    TVector<TString> indexColumns;
    for (auto c : indexRanges.Columns()) {
        indexColumns.push_back(c.StringValue());
    }

    auto indexColumnsPtr = TIntrusivePtr<TOptimizerStatistics::TKeyColumns>(new TOptimizerStatistics::TKeyColumns(indexColumns));
    auto stats = std::make_shared<TOptimizerStatistics>(
        inputStats->Type, 
        inputStats->Nrows, 
        inputStats->Ncols, 
        inputStats->ByteSize, 
        inputStats->Cost, 
        indexColumnsPtr,
        inputStats->ColumnStatistics);

    typeCtx->SetStats(input.Get(), stats);

    YQL_CLOG(TRACE, CoreDq) << "Infer statistics for index: " << stats->ToString();
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

            auto resStats = txStats[bindingNo][resultNo];
            typeCtx->SetStats(param.Name().Raw(), resStats);
            typeCtx->SetStats(inputNode.Raw(), resStats);
        }
    }
}

void InferStatisticsForDqSourceWrap(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx,
    TKqpOptimizeContext& kqpCtx) {
    auto inputNode = TExprBase(input);
    if (auto wrapBase = inputNode.Maybe<TDqSourceWrapBase>()) {
        if (auto maybeS3DataSource = wrapBase.Cast().DataSource().Maybe<TS3DataSource>()) {
            auto s3DataSource = maybeS3DataSource.Cast();
            if (s3DataSource.Name()) {
                auto path = s3DataSource.Name().Cast().StringValue();
                if (kqpCtx.Config->OptOverrideStatistics.Get() && path) {
                    auto stats = std::make_shared<TOptimizerStatistics>(EStatisticsType::BaseTable, 0.0, 0, 0, 0.0, TIntrusivePtr<TOptimizerStatistics::TKeyColumns>());
                    stats = OverrideStatistics(*stats, path, kqpCtx.GetOverrideStatistics());
                    if (stats->ByteSize == 0.0) {
                        auto n = path.find_last_of('/');
                        if (n != path.npos) {
                            stats = OverrideStatistics(*stats, path.substr(n + 1), kqpCtx.GetOverrideStatistics());
                        }
                    }
                    if (stats->ByteSize != 0.0) {
                        if (stats->Ncols) {
                            auto n = wrapBase.Cast().RowType().Ref().GetTypeAnn()->Cast<TTypeExprType>()->GetType()->Cast<TStructExprType>()->GetSize();
                            stats->ByteSize = stats->ByteSize * n / stats->Ncols;
                        }
                        YQL_CLOG(TRACE, CoreDq) << "Infer statistics for s3 data source " << path;
                        typeCtx->SetStats(input.Get(), stats);
                        typeCtx->SetStats(s3DataSource.Raw(), stats);
                    }
                }
            }
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
    if (Config->CostBasedOptimizationLevel.Get().GetOrElse(TDqSettings::TDefault::CostBasedOptimizationLevel) == 0) {
        return IGraphTransformer::TStatus::Ok;
    }

    TxStats.clear();
    return TDqStatisticsTransformerBase::DoTransform(input, output, ctx);
}

bool TKqpStatisticsTransformer::BeforeLambdasSpecific(const TExprNode::TPtr& input, TExprContext& ctx) {
    Y_UNUSED(ctx);
    bool matched = true;
    // KQP Matchers
    if(TKqlReadTableIndexRanges::Match(input.Get())) {
        InferStatisticsForReadTableIndexRanges(input, TypeCtx);
    }
    else if(TKqlReadTableBase::Match(input.Get()) || TKqlReadTableRangesBase::Match(input.Get())){
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
        InferStatisticsForRowsSourceSettings(input, TypeCtx, KqpCtx);
    }
    else if (TKqpCnStreamLookup::Match(input.Get())) {
        InferStatisticsForSteamLookup(input, TypeCtx);
    }

    // Match a result binding atom and connect it to a stage
    else if(TCoParameter::Match(input.Get())) {
        InferStatisticsForResultBinding(input, TypeCtx, TxStats);
    }
    else if(TDqSourceWrapBase::Match(input.Get())) {
        InferStatisticsForDqSourceWrap(input, TypeCtx, KqpCtx);
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
    TTypeAnnotationContext& typeCtx, const TKikimrConfiguration::TPtr& config, const TKqpProviderContext& pctx) {
    return THolder<IGraphTransformer>(new TKqpStatisticsTransformer(kqpCtx, typeCtx, config, pctx));
}
