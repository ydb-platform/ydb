#include "kqp_statistics_transformer.h"
#include <yql/essentials/utils/log/log.h>
#include <ydb/core/kqp/opt/cbo/solver/kqp_opt_stat.h>
#include <yql/essentials/core/yql_cost_function.h>
#include <yql/essentials/core/yql_join.h>
#include <ydb/core/kqp/opt/cbo/cbo_interesting_orderings.h>
#include <yql/essentials/core/cbo/cbo_interesting_orderings.h>

#include <ydb/core/kqp/opt/cbo/solver/kqp_opt_join_cost_based.h>

#include <ydb/library/yql/providers/dq/common/yql_dq_settings.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <ydb/library/yql/providers/s3/expr_nodes/yql_s3_expr_nodes.h>
#include <ydb/library/yql/providers/s3/statistics/yql_s3_statistics.h>

#include <charconv>

namespace NKikimr::NKqp {

using namespace NYql;
using namespace NYql::NNodes;
using namespace NYql::NDq;

static TIntrusivePtr<TOptimizerStatistics::TColumnStatMap> FromYqlColumnStatMap(
    const TIntrusivePtr<NYql::TOptimizerStatistics::TColumnStatMap>& src)
{
    auto result = MakeIntrusive<TOptimizerStatistics::TColumnStatMap>();
    for (const auto& [name, s] : src->Data) {
        TColumnStatistics cs;
        cs.NumUniqueVals = s.NumUniqueVals;
        cs.HyperLogLog = s.HyperLogLog;
        cs.CountMinSketch = s.CountMinSketch;
        cs.EqWidthHistogramEstimator = s.EqWidthHistogramEstimator;
        cs.Type = s.Type;
        result->Data[name] = std::move(cs);
    }
    return result;
}

// Dual-inheritance wrapper: stored as NKikimr::NKqp::IProviderStatistics,
// but dynamic_cast<NYql::TS3ProviderStatistics*> still works.
struct TKqpS3ProviderStatistics
    : public NKikimr::NKqp::IProviderStatistics
    , public NYql::TS3ProviderStatistics
{
    TKqpS3ProviderStatistics() = default;
    explicit TKqpS3ProviderStatistics(const NYql::TS3ProviderStatistics& s3)
        : NYql::TS3ProviderStatistics(s3) {}
};

/**
 * Compute statistics and cost for read table
 * Currently we look up the number of rows and attributes in the statistics service
 */
void InferStatisticsForReadTable(const TExprNode::TPtr& input, TTypeAnnotationContext* /*typeCtx*/,
    const TKqpOptimizeContext& kqpCtx, TKqpStatsStore* kqpStats) {

    auto inputNode = TExprBase(input);
    std::shared_ptr<TOptimizerStatistics> inputStats;

    int nAttrs = 0;
    bool readRange = false;

    TMaybe<TCoAtomList> columns;
    if (auto readTable = inputNode.Maybe<TKqlReadTableBase>()) {
        inputStats = kqpStats->GetStats(readTable.Cast().Table().Raw());
        nAttrs = readTable.Cast().Columns().Size();
        columns = readTable.Cast().Columns();

        auto range = readTable.Cast().Range();
        auto rangeFrom = range.From().Maybe<TKqlKeyTuple>();
        auto rangeTo = range.To().Maybe<TKqlKeyTuple>();
        if (rangeFrom && rangeTo) {
            readRange = true;
        }
    } else if (auto readRanges = inputNode.Maybe<TKqlReadTableRangesBase>()) {
        inputStats = kqpStats->GetStats(readRanges.Cast().Table().Raw());
        nAttrs = readRanges.Cast().Columns().Size();

        columns = readRanges.Cast().Columns();
    } else {
        Y_ENSURE(false, "Invalid node type for InferStatisticsForReadTable");
    }

    if (!inputStats) {
        return;
    }

    TTableAliasMap tableAlias;
    if (columns) {
        for (const auto& column: *columns) {
            TString alias;
            if (inputStats->Aliases && inputStats->Aliases->size() == 1) {
                alias = *inputStats->Aliases->begin();
            }
            TString from = alias + "." + column.StringValue();
            TString to = column.StringValue();
            tableAlias.AddRename(from, to);
        }
    }

    auto keyColumns = inputStats->KeyColumns;
    if (auto indexRead = inputNode.Maybe<TKqlReadTableIndex>()) {
        const auto& tableData = kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, indexRead.Cast().Table().Path().Value());
        const auto& [indexMeta, _] = tableData.Metadata->GetIndexMetadata(indexRead.Cast().Index().Value());

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
        inputStats->ColumnStatistics,
        inputStats->StorageType
    );
    stats->ShuffledByColumns = inputStats->ShuffledByColumns;
    stats->LogicalOrderings = inputStats->LogicalOrderings;
    stats->SortingOrderings = inputStats->SortingOrderings;
    stats->Aliases = inputStats->Aliases;
    stats->TableAliases = MakeIntrusive<TTableAliasMap>(std::move(tableAlias));
    stats->SourceTableName = inputStats->SourceTableName;

    YQL_CLOG(TRACE, CoreDq) << "Infer statistics for read table" << stats->ToString();

    kqpStats->SetStats(input.Get(), stats);
}

std::vector<TOrdering::TItem::EDirection> GetAscDirections(std::size_t n) {
    return std::vector<TOrdering::TItem::EDirection>(n, TOrdering::TItem::EDirection::EAscending);
}

std::vector<TOrdering::TItem::EDirection> GetDescDirections(std::size_t n) {
    return std::vector<TOrdering::TItem::EDirection>(n, TOrdering::TItem::EDirection::EDescending);
}

/**
 * Infer statistics for KQP table
 */
void InferStatisticsForKqpTable(
    const TExprNode::TPtr& input,
    TTypeAnnotationContext* typeCtx,
    TKqpOptimizeContext& kqpCtx,
    TKqpStatsStore* kqpStats
) {
    auto inputNode = TExprBase(input);

    auto readTable = inputNode.Cast<TKqpTable>();
    auto path = readTable.Path();

    if (readTable.PathId() == "") {
        // CTAS don't have created table during compilation.
        return;
    }

    const auto& tableData = kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, path.Value());
    TSimpleSharedPtr<THashSet<TString>> aliases;
    if (auto tablePrevStats = kqpStats->GetStats(inputNode.Raw())) {
        aliases = tablePrevStats->Aliases;
    } else {
        aliases = MakeSimpleShared<THashSet<TString>>();
    }

    double nRows = tableData.Metadata->RecordsCount;
    double byteSize = tableData.Metadata->DataSize;
    int nAttrs = tableData.Metadata->Columns.size();

    auto keyColumns = TIntrusivePtr<TOptimizerStatistics::TKeyColumns>(new TOptimizerStatistics::TKeyColumns(tableData.Metadata->KeyColumnNames));
    auto stats = std::make_shared<TOptimizerStatistics>(EStatisticsType::BaseTable, nRows, nAttrs, byteSize, 0.0, keyColumns);
    if (typeCtx->ColumnStatisticsByTableName.contains(path.StringValue())) {
        stats->ColumnStatistics = FromYqlColumnStatMap(typeCtx->ColumnStatisticsByTableName[path.StringValue()]);
    }
    if (kqpCtx.Config->OptOverrideStatistics.Get()) {
        stats = OverrideStatistics(*stats, path.Value(), kqpCtx.GetOverrideStatistics());
    }
    if (stats->ColumnStatistics) {
        for (const auto& [columnName, metaData]: tableData.Metadata->Columns) {
            stats->ColumnStatistics->Data[columnName].Type = metaData.Type;
        }
    }

    EStorageType storageType = EStorageType::NA;
    switch (tableData.Metadata->Kind) {
        case EKikimrTableKind::Datashard:
            storageType = EStorageType::RowStorage;
            break;
        case EKikimrTableKind::Olap:
            storageType = EStorageType::ColumnStorage;
            break;
        default:
            break;
    }
    stats->StorageType = storageType;

    TString alias;
    if (aliases && aliases->size() == 1) {
        alias = *aliases->begin();;
    }

    if (!tableData.Metadata->PartitionedByColumns.empty()) {
        TVector<TJoinColumn> shuffledByColumns;
        for (const auto& columnName: tableData.Metadata->PartitionedByColumns) {
            shuffledByColumns.emplace_back(alias, columnName);
        }

        stats->ShuffledByColumns = TIntrusivePtr<TOptimizerStatistics::TShuffledByColumns>(
            new TOptimizerStatistics::TShuffledByColumns(std::move(shuffledByColumns))
        );
    }

    stats->TableAliases = MakeIntrusive<TTableAliasMap>();
    stats->TableAliases->AddMapping(path.StringValue(), path.StringValue());
    stats->SourceTableName = path.StringValue();

    auto& shufflingsFSM = kqpStats->ShufflingsFSM;
    if (shufflingsFSM && stats && stats->ShuffledByColumns) {
        auto shuffledBy = stats->ShuffledByColumns->Data;
        for (auto& column: shuffledBy) {
            column.RelName = alias;
        }
        auto shuffling = TShuffling(shuffledBy);
        std::int64_t orderingIdx = shufflingsFSM->FDStorage.FindShuffling(shuffling, nullptr);
        stats->LogicalOrderings = shufflingsFSM->CreateState(orderingIdx);
    }

    auto& sortingsFSM = kqpStats->SortingsFSM;
    if (sortingsFSM && stats && stats->KeyColumns && stats->StorageType == EStorageType::RowStorage) {
        const TVector<TString>& keyColumns = stats->KeyColumns->Data;

        TVector<TJoinColumn> sortedBy(keyColumns.size());
        for (std::size_t i = 0; i < sortedBy.size(); ++i) {
            sortedBy[i].RelName = alias;
            sortedBy[i].AttributeName = keyColumns[i];
        }

        auto sorting = TSorting(sortedBy, GetAscDirections(sortedBy.size()));
        std::int64_t orderingIdx = sortingsFSM->FDStorage.FindSorting(sorting, nullptr);
        stats->SortingOrderings = sortingsFSM->CreateState(orderingIdx);

        auto reversedSorting = TSorting(sortedBy, GetDescDirections(sortedBy.size()));
        std::int64_t reversedOrderingIdx = sortingsFSM->FDStorage.FindSorting(reversedSorting, nullptr);
        stats->ReversedSortingOrderings = sortingsFSM->CreateState(reversedOrderingIdx);
    }

    stats->Aliases = std::move(aliases);

    YQL_CLOG(TRACE, CoreDq) << "Infer statistics for table: " << path.Value() << ": " << stats->ToString();

    kqpStats->SetStats(input.Get(), stats);
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
void InferStatisticsForSteamLookup(
    const TExprNode::TPtr& input,
    TTypeAnnotationContext* /*typeCtx*/,
    const TKqpOptimizeContext& kqpCtx,
    TKqpStatsStore* kqpStats
) {
    auto inputNode = TExprBase(input);
    auto streamLookup = inputNode.Cast<TKqpCnStreamLookup>();

    auto columns = streamLookup.Columns();
    int nAttrs = columns.Size();
    auto tableStats = kqpStats->GetStats(streamLookup.Table().Raw());
    auto inputStats = kqpStats->GetStats(streamLookup.Output().Raw());

    if (!inputStats || !tableStats) {
        return;
    }
    auto byteSize = tableStats->ByteSize * (nAttrs / (double) tableStats->Ncols) * inputStats->Selectivity;

    auto res = std::make_shared<TOptimizerStatistics>(
        EStatisticsType::BaseTable,
        inputStats->Nrows,
        nAttrs,
        byteSize,
        0,
        inputStats->KeyColumns,
        inputStats->ColumnStatistics,
        inputStats->StorageType
    );
    res->SortingOrderings = inputStats->SortingOrderings;

    if (!kqpCtx.Config->OrderPreservingLookupJoinEnabled()) {
        res->SortingOrderings.RemoveState();
    }

    YQL_CLOG(TRACE, CoreDq) << "Infer statistics for KqpCnStreamLookup: " << res->ToString();
    kqpStats->SetStats(input.Get(), std::move(res));
}

/**
 * Infer statistics for TableLookup
 *
 * Table lookup can be done with an Iterator, in which case we treat it as a full scan
 * We don't differentiate between a small range and full scan at this time
 */
void InferStatisticsForLookupTable(const TExprNode::TPtr& input, TTypeAnnotationContext* /*typeCtx*/, TKqpStatsStore* kqpStats) {
    auto inputNode = TExprBase(input);
    auto lookupTable = inputNode.Cast<TKqlLookupTableBase>();
    auto lookupKeys = lookupTable.LookupKeys();

    auto inputTableStats = kqpStats->GetStats(lookupTable.Table().Raw());
    auto inputLookupStats = kqpStats->GetStats(lookupKeys.Raw());
    if (!inputTableStats || !inputLookupStats) {
        return;
    }

    kqpStats->SetStats(input.Get(), inputLookupStats);
}

/**
 * Compute statistics for RowsSourceSetting
 * We look into range expression to check if its a point lookup or a full scan
 * We currently don't try to figure out whether this is a small range vs full scan
 */
void InferStatisticsForRowsSourceSettings(
    const TExprNode::TPtr& input,
    TTypeAnnotationContext* /*typeCtx*/,
    const TKqpOptimizeContext& kqpCtx,
    TKqpStatsStore* kqpStats
) {

    auto inputNode = TExprBase(input);
    auto sourceSettings = inputNode.Cast<TKqpReadRangesSourceSettings>();

    auto inputStats = kqpStats->GetStats(sourceSettings.Table().Raw());
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
        const auto& [indexMeta, _] = tableData.Metadata->GetIndexMetadata(indexRead.Cast().Index().Value());

        keyColumns = TIntrusivePtr<TOptimizerStatistics::TKeyColumns>(
            new TOptimizerStatistics::TKeyColumns(indexMeta->KeyColumnNames)
        );
    }

    int nAttrs = sourceSettings.Columns().Size();

    double sizePerRow = inputStats->ByteSize / (inputRows==0?1:inputRows);
    double byteSize = nRows * sizePerRow * (nAttrs / (double)inputStats->Ncols);
    double cost = inputStats->Cost;

    auto outputStats = std::make_shared<TOptimizerStatistics>(
        EStatisticsType::BaseTable,
        nRows,
        nAttrs,
        byteSize,
        cost,
        keyColumns,
        inputStats->ColumnStatistics,
        inputStats->StorageType
    );
    outputStats->SortingOrderings = inputStats->SortingOrderings;
    outputStats->ShuffledByColumns = inputStats->ShuffledByColumns;
    outputStats->LogicalOrderings = inputStats->LogicalOrderings;
    outputStats->Aliases = inputStats->Aliases;
    outputStats->SourceTableName = inputStats->SourceTableName;

    auto settings = NYql::TKqpReadTableSettings::Parse(sourceSettings.Settings());
    if (!settings.IsSorted()) {
        outputStats->SortingOrderings.RemoveState();
    }

    if (settings.IsReverse()) {
        outputStats->SortingOrderings = inputStats->ReversedSortingOrderings;
    }

    YQL_CLOG(TRACE, CoreDq) << "Infer statistics for source settings: " << outputStats->ToString();

    kqpStats->SetStats(input.Get(), outputStats);
}

/**
 * Compute statistics for index lookup
 * Currently we just make up a number for cardinality (5) and set cost to 0
 */
void InferStatisticsForIndexLookup(const TExprNode::TPtr& input, TTypeAnnotationContext* /*typeCtx*/, TKqpStatsStore* kqpStats) {
    auto inputNode = TExprBase(input);
    auto lookupIndex = inputNode.Cast<TKqlStreamLookupIndex>();

    auto inputStats = kqpStats->GetStats(lookupIndex.LookupKeys().Raw());
    if (!inputStats) {
        return;
    }

    kqpStats->SetStats(input.Get(), inputStats);
}

void InferStatisticsForReadTableIndexRanges(
    const TExprNode::TPtr& input,
    TTypeAnnotationContext* /*typeCtx*/,
    const TKqpOptimizeContext& kqpCtx,
    TKqpStatsStore* kqpStats
) {
    auto indexRanges = TKqlReadTableIndexRanges(input);

    auto inputStats = kqpStats->GetStats(indexRanges.Table().Raw());
    if (!inputStats) {
        return;
    }

    TString alias;
    if (auto prevStats = kqpStats->GetStats(TExprBase(input).Raw()); prevStats && prevStats->Aliases && !prevStats->Aliases->empty()) {
        alias = *prevStats->Aliases->begin();
    }


    auto tablePath = indexRanges.Table().Path();
    const auto& tableDesc = kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, tablePath);
    const auto& [indexMeta, _ ] = tableDesc.Metadata->GetIndexMetadata(indexRanges.Index().StringValue());

    auto indexColumnsPtr = TIntrusivePtr<TOptimizerStatistics::TKeyColumns>(new TOptimizerStatistics::TKeyColumns(indexMeta->KeyColumnNames));
    auto stats = std::make_shared<TOptimizerStatistics>(
        inputStats->Type,
        inputStats->Nrows,
        inputStats->Ncols,
        inputStats->ByteSize,
        inputStats->Cost,
        indexColumnsPtr,
        inputStats->ColumnStatistics,
        inputStats->StorageType
    );

    if (kqpStats->SortingsFSM) {
        auto sortedBy = indexColumnsPtr->ToJoinColumns(alias);
        auto sorting = TSorting(sortedBy, GetAscDirections(sortedBy.size()));
        std::int64_t orderingIdx = kqpStats->SortingsFSM->FDStorage.FindSorting(sorting);
        stats->SortingOrderings = kqpStats->SortingsFSM->CreateState(orderingIdx);
    }

    stats->ShuffledByColumns = inputStats->ShuffledByColumns;
    stats->LogicalOrderings = inputStats->LogicalOrderings;
    stats->Aliases = inputStats->Aliases;
    stats->SourceTableName = inputStats->SourceTableName;

    kqpStats->SetStats(input.Get(), stats);

    YQL_CLOG(TRACE, CoreDq) << "Infer statistics for index: " << stats->ToString();
}

void InferStatisticsForLookupJoin(
    const TExprNode::TPtr& input,
    TTypeAnnotationContext* /*typeCtx*/,
    TKqpStatsStore* kqpStats
) {
    auto lookupJoin = TKqlIndexLookupJoinBase(input);

    auto inputStats = kqpStats->GetStats(lookupJoin.Input().Raw());
    if (!inputStats) {
        return;
    }

    auto propagateAliases = [kqpStats, lookupJoin](auto&& thisLambda, const TExprNode::TPtr& input) -> void {
        auto exprNode = TExprBase(input).Raw();
        if (auto maybeKqlLookupTableBase = TMaybeNode<TKqlLookupTableBase>(exprNode)) {
            auto lookupBase = maybeKqlLookupTableBase.Cast();

            if (auto leftStats = kqpStats->GetStats(lookupBase.LookupKeys().Raw()); leftStats && leftStats->Aliases) {
                if (auto leftLabel = lookupJoin.LeftLabel().StringValue()) {
                    leftStats->Aliases->insert(std::move(leftLabel));
                }
            }

            if (auto rightStats = kqpStats->GetStats(lookupBase.Table().Raw()); rightStats && rightStats->Aliases) {
                if (auto rightLabel = lookupJoin.RightLabel().StringValue()) {
                    rightStats->Aliases->insert(std::move(rightLabel));
                }
            }
        } else if (auto maybeFlatMapBase = TMaybeNode<TCoFlatMapBase>(exprNode)) {
            thisLambda(thisLambda, maybeFlatMapBase.Cast().Input().Ptr());
        }
    };

    propagateAliases(propagateAliases, lookupJoin.Input().Ptr());
    auto outputStats = *inputStats;

    YQL_CLOG(TRACE, CoreDq) << "Infer statistics for lookup join: " << outputStats.ToString();
    kqpStats->SetStats(input.Get(), std::make_shared<TOptimizerStatistics>(std::move(outputStats)));
}

/***
 * Infer statistics for result binding of a stage
 */
void InferStatisticsForResultBinding(const TExprNode::TPtr& input, TTypeAnnotationContext* /*typeCtx*/,
    TVector<TVector<std::shared_ptr<TOptimizerStatistics>>>& txStats, TKqpStatsStore* kqpStats) {

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
            kqpStats->SetStats(param.Name().Raw(), resStats);
            kqpStats->SetStats(inputNode.Raw(), resStats);
        }
    }
}

class TKqpOlapPredicateSelectivityComputer: public TPredicateSelectivityComputer {
public:
    TKqpOlapPredicateSelectivityComputer(const std::shared_ptr<TOptimizerStatistics>& stats)
        : TPredicateSelectivityComputer(stats)
    {}

    double Compute(const NNodes::TExprBase& input) {
        std::optional<double> resSelectivity;

        if (auto andNode = input.Maybe<TKqpOlapAnd>()) {
            double tmpSelectivity = 1.0;
            for (size_t i = 0; i < andNode.Cast().ArgCount(); i++) {
                tmpSelectivity *= Compute(andNode.Cast().Arg(i));
            }
            resSelectivity = tmpSelectivity;
        } else if (auto olapApply = input.Maybe<TKqpOlapApply>()) {
            resSelectivity = TPredicateSelectivityComputer::Compute(olapApply.Cast().Lambda().Body());
        } else if (auto orNode = input.Maybe<TKqpOlapOr>()) {
            double tmpSelectivity = 0.0;
            for (size_t i = 0; i < orNode.Cast().ArgCount(); i++) {
                tmpSelectivity += Compute(orNode.Cast().Arg(i));
            }
            resSelectivity = tmpSelectivity;
        } else if (auto notNode = input.Maybe<TKqpOlapNot>()) {
            resSelectivity = 1 - Compute(notNode.Cast().Value());
        } else if ((input.Maybe<TCoList>() || input.Maybe<TCoAtomList>()) && input.Ptr()->ChildrenSize() >= 1) {
            TExprNode::TPtr listPtr = input.Ptr();
            if (listPtr->ChildrenSize() >= 2 && listPtr->Child(0)->Content() == "??") {
                listPtr = listPtr->Child(1);
            }

            size_t listSize = listPtr->ChildrenSize();
            if (listSize == 3 || listSize == 4/*OpType optional field*/) {
                TString compSign = TString(listPtr->Child(0)->Content());
                auto left = listPtr->ChildPtr(1);
                auto right = listPtr->ChildPtr(2);
                if (IsConstantExpr(left) && OlapOppositeCompSigns.contains(compSign)) {
                    compSign = OlapOppositeCompSigns[compSign];
                    std::swap(left, right);
                }

                TString attr = TString(left->Content());

                TExprContext dummyCtx;
                TPositionHandle dummyPos;

                auto rowArg =
                    Build<TCoArgument>(dummyCtx, dummyPos)
                        .Name("row")
                    .Done();

                auto member =
                        Build<TCoMember>(dummyCtx, dummyPos)
                            .Struct(rowArg)
                            .Name().Build(attr)
                        .Done();

                auto value = TExprBase(right);
                if (listPtr->ChildPtr(2)->ChildrenSize() >= 2 && listPtr->ChildPtr(2)->ChildPtr(0)->Content() == "just") {
                    value = TExprBase(listPtr->ChildPtr(2)->ChildPtr(1));
                }
                if (OlapCompSigns.contains(compSign)) {
                    resSelectivity = this->ComputeInequalitySelectivity(member, value, OlapCompStrToEInequalityPredicate[compSign], false);
                } else if (compSign == "eq") {
                    resSelectivity = this->ComputeEqualitySelectivity(member, value, false);
                } else if (compSign == "neq") {
                    resSelectivity = 1 - this->ComputeEqualitySelectivity(member, value, false);
                } else if (RegexpSigns.contains(compSign)) {
                    return 0.5;
                }
            }
        }

        if (!resSelectivity.has_value()) {
            auto dumped = input.Raw()->Dump();
            YQL_CLOG(TRACE, ProviderKqp) << "ComputePredicateSelectivity NOT FOUND : " << dumped;
            return 1.0;
        }

        return std::min(1.0, resSelectivity.value());
    }

private:
    THashSet<TString> OlapCompSigns = {
        {"lt"},
        {"lte"},
        {"gt"},
        {"gte"}
    };

    THashSet<TString> RegexpSigns = {
        "string_contains",
        "starts_with",
        "ends_with"
    };

    THashMap<TString, EInequalityPredicateType> OlapCompStrToEInequalityPredicate = {
        {"lt", EInequalityPredicateType::Less},
        {"lte", EInequalityPredicateType::LessOrEqual},
        {"gt", EInequalityPredicateType::GreaterOrEqual},
        {"gte", EInequalityPredicateType::GreaterOrEqual},
    };

    THashMap<TString, TString> OlapOppositeCompSigns = {{"lt", "gt"},   {"lte", "gte"}, {"gt", "lt"},
                                                        {"gte", "lte"}, {"eq", "neq"},  {"neq", "eq"}};
};

void InferStatisticsForOlapFilter(const TExprNode::TPtr& input, TTypeAnnotationContext* /*typeCtx*/, TKqpStatsStore* kqpStats) {
    auto inputNode = TExprBase(input);
    auto filter = inputNode.Cast<TKqpOlapFilter>();
    auto filterInput = filter.Input();
    auto inputStats = kqpStats->GetStats(filterInput.Raw());

    if (!inputStats) {
        return;
    }

    double selectivity = TKqpOlapPredicateSelectivityComputer(inputStats).Compute(filter.Condition());

    auto outputStats = TOptimizerStatistics(inputStats->Type, inputStats->Nrows * selectivity, inputStats->Ncols, inputStats->ByteSize * selectivity, inputStats->Cost, inputStats->KeyColumns, inputStats->ColumnStatistics );
    outputStats.Labels = inputStats->Labels;
    outputStats.Selectivity *= selectivity;

    YQL_CLOG(TRACE, CoreDq) << "Infer statistics for OLAP Filter: " << outputStats.ToString();


    kqpStats->SetStats(input.Get(), std::make_shared<TOptimizerStatistics>(std::move(outputStats)) );
}

void InferStatisticsForOlapRead(const TExprNode::TPtr& input, TTypeAnnotationContext* /*typeCtx*/, TKqpStatsStore* kqpStats) {
    auto inputNode = TExprBase(input);
    auto olapRead = inputNode.Cast<TKqpReadOlapTableRangesBase>();

    auto process = olapRead.Process();
    auto lambdaStats = kqpStats->GetStats(process.Body().Raw());
    if (lambdaStats) {
        YQL_CLOG(TRACE, CoreDq) << "Infer statistics for OLAP table: " << lambdaStats->ToString();
        kqpStats->SetStats(input.Get(), lambdaStats);
    }
}

double EstimateRowSize(const TStructExprType& rowType, const TString& format, const TString& compression, bool decoded) {
    double result = 0.0;
    for (auto item : rowType.GetItems()) {
        auto itemType = item->GetItemType();
        if (itemType->GetKind() == ETypeAnnotationKind::Data) {
            switch(itemType->Cast<TDataExprType>()->GetSlot()) {
                case EDataSlot::Bool:
                    result += decoded ? 1.0 : 0.2;
                    break;
                case EDataSlot::Int8:
                    [[fallthrough]];
                case EDataSlot::Uint8:
                    result += decoded ? 1.0 : 0.72;
                    break;
                case EDataSlot::Int16:
                    [[fallthrough]];
                case EDataSlot::Uint16:
                    result += decoded ? 2.0 : 1.44;
                    break;
                case EDataSlot::Int32:
                    [[fallthrough]];
                case EDataSlot::Uint32:
                    result += decoded ? 4.0 : 2.88;
                    break;
                case EDataSlot::Int64:
                    [[fallthrough]];
                case EDataSlot::Uint64:
                    [[fallthrough]];
                case EDataSlot::Double:
                    result += decoded ? 8.0 : 3.88;
                    break;
                case EDataSlot::Float:
                    result += decoded ? 4.0 : 2.88;
                    break;
                case EDataSlot::String:
                    [[fallthrough]];
                case EDataSlot::Utf8:
                    result += decoded ? 28.0 : 8.0;
                    break;
                case EDataSlot::Yson:
                    [[fallthrough]];
                case EDataSlot::Json:
                    result += decoded ? 56.0 : 16.0;
                    break;
                case EDataSlot::Uuid:
                    break;
                case EDataSlot::Date:
                    result += decoded ? 2.0 : 1.51;
                    break;
                case EDataSlot::Datetime:
                    [[fallthrough]];
                case EDataSlot::Timestamp:
                    result += decoded ? 8.0 : 6.04;
                    break;
                case EDataSlot::Interval:
                    break;
                case EDataSlot::TzDate:
                    break;
                case EDataSlot::TzDatetime:
                    break;
                case EDataSlot::TzTimestamp:
                    break;
                case EDataSlot::Decimal:
                    result += decoded ? 16.0 : 7.76;
                    break;
                case EDataSlot::DyNumber:
                    break;
                case EDataSlot::JsonDocument:
                    break;
                case EDataSlot::Date32:
                    result += decoded ? 4.0 : 2.88;
                    break;
                case EDataSlot::Datetime64:
                    [[fallthrough]];
                case EDataSlot::Timestamp64:
                case EDataSlot::Interval64:
                    result += decoded ? 8.0 : 3.88;
                    break;
                case EDataSlot::TzDate32:
                    break;
                case EDataSlot::TzDatetime64:
                    break;
                case EDataSlot::TzTimestamp64:
                    break;
            }
        }
    }

    if (result == 0.0) {
        result = 1000.0;
    }

    if (format != "parquet" && !decoded) {
        double compressionRatio = 1.0;
        if (format == "csv_with_names" || format == "tsv_with_names") {
            result *= 5.0;
            compressionRatio = 4.5;   // gzip
        } else if (format != "raw") { // json's
            result *= 12.0;
            compressionRatio = 14.0;   // gzip
        }
        if (compression) {
            if (compression == "gzip") {
                // 1.00
            } else if (compression == "zstd") {
                compressionRatio *= 1.05;
            } else if (compression == "lz4") {
                compressionRatio *= 1.43;
            } else if (compression == "brotli") {
                compressionRatio *= 1.20;
            } else if (compression == "bzip2") {
                compressionRatio *= 1.24;
            } else if (compression == "xz") {
                compressionRatio *= 1.45;
            }
            result /= compressionRatio;
        }
    }

    return result;
}

void InferStatisticsForDqSourceWrap(
    const TExprNode::TPtr& input,
    TTypeAnnotationContext* /*typeCtx*/,
    TKqpOptimizeContext& kqpCtx,
    TKqpStatsStore* kqpStats
) {
    auto inputNode = TExprBase(input);
    if (auto wrapBase = inputNode.Maybe<TDqSourceWrapBase>()) {
        if (auto maybeS3DataSource = wrapBase.Cast().DataSource().Maybe<TS3DataSource>()) {
            auto s3DataSource = maybeS3DataSource.Cast();
            if (s3DataSource.Name()) {
                auto stats = kqpStats->GetStats(s3DataSource.Raw());
                if (!stats) {
                    stats = std::make_shared<TOptimizerStatistics>(EStatisticsType::BaseTable, 0.0, 0, 0, 0.0, TIntrusivePtr<TOptimizerStatistics::TKeyColumns>());
                }
                if (!stats->Specific) {
                    stats->Specific = std::make_shared<TKqpS3ProviderStatistics>();
                }

                const TS3ProviderStatistics* specific = dynamic_cast<const TS3ProviderStatistics*>((stats->Specific.get()));

                if (!specific->OverrideApplied && kqpCtx.Config->OptOverrideStatistics.Get()) {
                    auto path = s3DataSource.Name().Cast().StringValue();
                    auto dbStats = kqpCtx.GetOverrideStatistics()->GetMapSafe();
                    if (!dbStats.contains(path)) {
                        auto n = path.find_last_of('/');
                        if (n != path.npos) {
                            path = path.substr(n + 1);
                        }
                    }
                    if (dbStats.contains(path)) {
                        YQL_CLOG(TRACE, CoreDq) << "Override statistics for s3 data source " << path;
                        stats = OverrideStatistics(*stats, path, kqpCtx.GetOverrideStatistics());
                        auto newSpecific = std::make_shared<TKqpS3ProviderStatistics>(*specific);
                        newSpecific->OverrideApplied = true;
                        stats->Specific = newSpecific;
                        specific = newSpecific.get();
                        kqpStats->SetStats(s3DataSource.Raw(), stats);
                    }
                }

                auto dataSourceStats = stats;

                auto rowType = wrapBase.Cast().RowType().Ref().GetTypeAnn()->Cast<TTypeExprType>()->GetType()->Cast<TStructExprType>();
                if (specific->FullRawRowAvgSize == 0.0) {
                    auto newSpecific = std::make_shared<TKqpS3ProviderStatistics>(*specific);
                    stats = std::make_shared<TOptimizerStatistics>(stats->Type, stats->Nrows, stats->Ncols, stats->ByteSize, stats->Cost, stats->KeyColumns, stats->ColumnStatistics, stats->StorageType, newSpecific);
                    newSpecific->FullRawRowAvgSize = EstimateRowSize(*rowType, newSpecific->Format, newSpecific->Compression, false);
                    newSpecific->FullDecodedRowAvgSize = EstimateRowSize(*rowType, newSpecific->Format, newSpecific->Compression, true);
                    specific = newSpecific.get();
                    kqpStats->SetStats(s3DataSource.Raw(), stats);
                }

                auto wrapStats = kqpStats->GetStats(input.Get());
                if (!wrapStats) {
                    kqpStats->SetStats(input.Get(), stats);
                } else {
                    stats = wrapStats;
                }

                if (stats->Ncols == 0 || stats->Ncols > static_cast<int>(rowType->GetSize()) || stats->Nrows == 0 || stats->ByteSize == 0.0 || stats->Cost == 0.0) {
                    auto newSpecific = std::make_shared<TKqpS3ProviderStatistics>(*specific);

                    stats = std::make_shared<TOptimizerStatistics>(stats->Type, stats->Nrows, stats->Ncols, stats->ByteSize, stats->Cost, stats->KeyColumns, stats->ColumnStatistics, stats->StorageType, newSpecific);

                    if (stats->Nrows == 0 && newSpecific->FullRawRowAvgSize) {
                        stats->Nrows = newSpecific->RawByteSize / newSpecific->FullRawRowAvgSize;
                    }
                    if (stats->Ncols == 0 || stats->Ncols > static_cast<int>(rowType->GetSize())) {
                        stats->Ncols = rowType->GetSize();
                        newSpecific->PrunedRawRowAvgSize = EstimateRowSize(*rowType, newSpecific->Format, newSpecific->Compression, false);
                        newSpecific->PrunedDecodedRowAvgSize = EstimateRowSize(*rowType, newSpecific->Format, newSpecific->Compression, true);
                        stats->ByteSize = 0.0;
                    }
                    if (stats->ByteSize == 0.0) {
                        stats->ByteSize = stats->Nrows * newSpecific->PrunedDecodedRowAvgSize;
                    }
                    double rowSize = 0.0;
                    if (stats->Cost == 0.0) {
                        if (newSpecific->Format == "parquet") {
                            rowSize = newSpecific->PrunedRawRowAvgSize;
                        } else {
                            rowSize = newSpecific->FullRawRowAvgSize;
                        }
                        stats->Cost = rowSize * stats->Nrows;
                        if (newSpecific->Compression) {
                            stats->Cost *= 1.5;
                        }
                        {
                            auto specific = const_cast<TS3ProviderStatistics*>(dynamic_cast<const TS3ProviderStatistics*>((dataSourceStats->Specific.get())));
                            specific->Costs[TStructExprType::MakeHash(rowType->GetItems())] = stats->Cost;
                        }
                    }
                    kqpStats->SetStats(input.Get(), stats);
                }
            }
        }
    }
}

/**
 * When encountering a KqpPhysicalTx, we save the results of the stage in a vector
 * where it can later be accessed via binding parameters
 */
void AppendTxStats(const TExprNode::TPtr& input, TTypeAnnotationContext* /*typeCtx*/,
    TVector<TVector<std::shared_ptr<TOptimizerStatistics>>>& txStats, TKqpStatsStore* kqpStats) {

    auto inputNode = TExprBase(input);
    auto tx = inputNode.Cast<TKqpPhysicalTx>();
    TVector<std::shared_ptr<TOptimizerStatistics>> vec;

    for (size_t i = 0; i < tx.Results().Size(); i++) {
        vec.push_back(kqpStats->GetStats(tx.Results().Item(i).Raw()));
    }

    txStats.push_back(vec);
}

TJoinColumn GetColumn(const TString& column) {
    return TJoinColumn::FromString(column);
}

TString TableAliasToString(TTableAliasMap* tableAlias) {
    if (tableAlias) {
        return tableAlias->ToString();
    }

    return "";
}

// Build a YQL FDStorage that mirrors the KQP FDStorage.
// Orderings are added in the same sequence so interesting-ordering indices match.
static NYql::NDq::TFDStorage ConvertToYqlFDStorage(const TFDStorage& kqpFDS) {
    NYql::NDq::TFDStorage yqlFDS;
    const auto& cols = kqpFDS.GetColumns();

    auto toYqlCol = [&cols](std::size_t idx) {
        const auto& c = cols[idx];
        return NYql::NDq::TJoinColumn(c.RelName, c.AttributeName);
    };

    for (const auto& fd : kqpFDS.FDs) {
        if (fd.IsConstant()) {
            yqlFDS.AddConstant(toYqlCol(fd.ConsequentItem), fd.AlwaysActive);
        } else if (fd.IsEquivalence()) {
            yqlFDS.AddEquivalence(
                toYqlCol(fd.AntecedentItems[0]),
                toYqlCol(fd.ConsequentItem),
                fd.AlwaysActive
            );
        } else {
            TVector<NYql::NDq::TJoinColumn> antecedents;
            antecedents.reserve(fd.AntecedentItems.size());
            for (auto i : fd.AntecedentItems) {
                antecedents.push_back(toYqlCol(i));
            }
            yqlFDS.AddImplication(antecedents, toYqlCol(fd.ConsequentItem), fd.AlwaysActive);
        }
    }

    for (const auto& ordering : kqpFDS.InterestingOrderings) {
        std::vector<NYql::NDq::TJoinColumn> yqlCols;
        yqlCols.reserve(ordering.Items.size());
        for (auto i : ordering.Items) {
            yqlCols.push_back(toYqlCol(i));
        }
        if (ordering.Type == TOrdering::EShuffle) {
            NYql::NDq::TShuffling shuffling(std::move(yqlCols));
            yqlFDS.AddShuffling(shuffling);
        } else {
            std::vector<NYql::NDq::TOrdering::TItem::EDirection> dirs;
            dirs.reserve(ordering.Directions.size());
            for (auto d : ordering.Directions) {
                dirs.push_back(static_cast<NYql::NDq::TOrdering::TItem::EDirection>(d));
            }
            NYql::NDq::TSorting sorting(std::move(yqlCols), std::move(dirs));
            yqlFDS.AddSorting(sorting);
        }
    }

    return yqlFDS;
}

class TInterestingOrderingsFSMBuilder {
public:
    TInterestingOrderingsFSMBuilder(
        TTypeAnnotationContext& typeCtx,
        TKqpStatsStore* kqpStats
    )
        : InterestingOrderingsCollector(typeCtx, kqpStats)
    {}

public:
    void Build(const TExprNode::TPtr& node) {
        YQL_CLOG(TRACE, CoreDq) << "Building Orderings FSM";

        VisitExpr(
            node,
            {},
            [this](const TExprNode::TPtr& node){
                return this->InterestingOrderingsCollector.Collect(node);
            }
        );

        YQL_CLOG(TRACE, CoreDq) << InterestingOrderingsCollector.FDStorage.ToString();

        // Build YQL FDStorage mirror before moving the KQP FDStorage.
        auto yqlFDStorage = ConvertToYqlFDStorage(InterestingOrderingsCollector.FDStorage);

        // Build KQP FSMs and store in KqpStats.
        auto& kqpStats = *InterestingOrderingsCollector.KqpStats;
        kqpStats.ShufflingsFSM = MakeSimpleShared<TOrderingsStateMachine>(
            InterestingOrderingsCollector.FDStorage, TOrdering::EType::EShuffle);
        kqpStats.SortingsFSM = MakeSimpleShared<TOrderingsStateMachine>(
            std::move(InterestingOrderingsCollector.FDStorage), TOrdering::EType::ESorting);

        LogReport(kqpStats.ShufflingsFSM, kqpStats.SortingsFSM);

        // Build YQL FSMs and store in TypeCtx (used by shared YQL infrastructure).
        auto& typeCtx = InterestingOrderingsCollector.TypeCtx;
        typeCtx.OrderingsFSM = MakeSimpleShared<NYql::NDq::TOrderingsStateMachine>(
            yqlFDStorage, NYql::NDq::TOrdering::EType::EShuffle);
        typeCtx.SortingsFSM = MakeSimpleShared<NYql::NDq::TOrderingsStateMachine>(
            std::move(yqlFDStorage), NYql::NDq::TOrdering::EType::ESorting);
    }

private:
    void LogReport(
        const TSimpleSharedPtr<TOrderingsStateMachine>& shufflingsFsm,
        const TSimpleSharedPtr<TOrderingsStateMachine>& sortingsFsm
    ) {
        if (shufflingsFsm) {
            YQL_CLOG(TRACE, CoreDq) << "\nShufflings FSM: " << shufflingsFsm->ToString();
        }

        if (sortingsFsm) {
            YQL_CLOG(TRACE, CoreDq) << "\nSortings FSM: " << sortingsFsm->ToString();
        }
    }

private:
    class TInterestingOrderingsCollector {
    public:
        TInterestingOrderingsCollector(
            TTypeAnnotationContext& typeCtx,
            TKqpStatsStore* kqpStats
        )
            : TypeCtx(typeCtx)
            , KqpStats(kqpStats)
        {}

        bool Collect(const TExprNode::TPtr& node) {
            if (auto equiJoin = TMaybeNode<TCoEquiJoin>(node)) {
                CollectEquiJoin(equiJoin.Cast());
            } else if (TMaybeNode<TKqlReadTableRangesBase>(node) || TMaybeNode<TKqlReadTableBase>(node)) {
                CollectKqpReadTable<GetAscDirections>(TExprBase(node));
            } else if (auto aggregateBase = TMaybeNode<TCoAggregateBase>(node)) {
                CollectAggregateBase(aggregateBase.Cast());
            } else if (auto topBase = TMaybeNode<TCoTopBase>(node)) {
                CollectSort<TCoTopBase>(topBase.Cast());
            } else if (auto sort = TMaybeNode<TCoSortBase>(node)) {
                CollectSort<TCoSortBase>(sort.Cast());
            } else if (auto kqlReadTableIndexRanges = TMaybeNode<TKqlReadTableIndexRanges>(node)) {
                CollectKqlReadTableIndexRanges<GetAscDirections>(kqlReadTableIndexRanges.Cast());
            } else if (auto flatMapBase = TMaybeNode<TCoFlatMapBase>(node)) {
                CollectFlatMapBase(flatMapBase.Cast());
            }

            return true;
        }

    public:
        TFDStorage FDStorage;
        TTypeAnnotationContext& TypeCtx;
        TKqpStatsStore* KqpStats;

    private:
        void CollectEquiJoin(const TCoEquiJoin& equiJoin) {
            CollectInterestingOrderingsFromJoinTree(equiJoin, FDStorage, TypeCtx, *KqpStats);
        }

    private:
        template <auto GetDirs>
        void CollectKqpReadTable(const TExprBase& readTable) {
            Y_ENSURE(readTable.Maybe<TKqlReadTableRangesBase>() || readTable.Maybe<TKqlReadTableBase>());

            auto stats = KqpStats->GetStats(readTable.Raw());
            if (!stats) {
                return;
            }

            TVector<TString> shufflingOrderingIdxes;
            TVector<TString> sortingsOrderingIdxes;
            if (stats->Aliases && stats->Aliases->size() == 1) {
                if (stats->ShuffledByColumns) {
                    auto shuffledBy = stats->ShuffledByColumns->Data;
                    for (auto& column: shuffledBy) {
                        column.RelName = *stats->Aliases->begin();
                    }
                    auto shuffling = TShuffling(shuffledBy);
                    TString idx = ToString(FDStorage.AddShuffling(shuffling, nullptr));
                    shufflingOrderingIdxes.push_back(std::move(idx));
                }

                if (stats->KeyColumns) {
                    TVector<TJoinColumn> sortedBy = stats->KeyColumns->ToJoinColumns(*stats->Aliases->begin());
                    auto sorting = TSorting(sortedBy, GetDirs(sortedBy.size()));
                    TString idx = ToString(FDStorage.AddSorting(sorting, nullptr));
                    sortingsOrderingIdxes.push_back(std::move(idx));
                }
            } else {
                if (stats->ShuffledByColumns) {
                    auto shuffling = TShuffling(stats->ShuffledByColumns->Data);
                    TString idx = ToString(FDStorage.AddShuffling(shuffling, nullptr));
                    shufflingOrderingIdxes.push_back(std::move(idx));
                }

                if (stats->KeyColumns) {
                    auto sortedBy = stats->KeyColumns->ToJoinColumns("");
                    auto sorting = TSorting(sortedBy, GetDirs(sortedBy.size()));
                    TString idx = ToString(FDStorage.AddSorting(sorting, nullptr));
                    sortingsOrderingIdxes.push_back(std::move(idx));
                }
            }

            TKqpTable table =
                readTable.Maybe<TKqlReadTableRangesBase>().IsValid()?
                    readTable.Maybe<TKqlReadTableRangesBase>().Cast().Table() :
                    readTable.Maybe<TKqlReadTableBase>().Cast().Table();

            std::stringstream ss;
                ss << "Collected KqpReadTable interesting ordering idx,"
                   << "shufflings: " << "[" << JoinSeq(", ", shufflingOrderingIdxes) << "]" << ", "
                   << "sortings: " << "[" << JoinSeq(", ", sortingsOrderingIdxes) << "]" << ", "
                   << "Path: " << table.Path().StringValue() << ", ";
            if (stats->Aliases) {
                ss << "Aliases: " << "[" << JoinSeq(", ", *stats->Aliases) << "]";
            }

            YQL_CLOG(TRACE, CoreDq) << ss.str();
        }

    private:
        void CollectAggregateBase(const TCoAggregateBase& aggregationBase) {
            if (aggregationBase.Keys().Empty()) {
                return;
            }

            TTableAliasMap* tableAliases = nullptr;
            if (auto stats = KqpStats->GetStats(aggregationBase.Raw())) {
                tableAliases = stats->TableAliases.Get();
            }

            auto orderingInfo = GetAggregationBaseShuffleOrderingInfo(aggregationBase, nullptr, tableAliases);
            auto shuffling = TShuffling(orderingInfo.Ordering);
            std::size_t shuffleOrderingIdx = FDStorage.AddShuffling(shuffling, tableAliases);

            TString aliasesStr;
            if (tableAliases) {
                aliasesStr = tableAliases->ToString();
            }
            YQL_CLOG(TRACE, CoreDq) << "Collected AggregateBase interesting ordering idx: " << shuffleOrderingIdx << ", TableAliases: " << aliasesStr;
        }

    private:
        template <typename TSortCallable>
        void CollectSort(const TSortCallable& sortCallable) {
            TTableAliasMap* tableAliases = nullptr;
            if (auto stats = KqpStats->GetStats(sortCallable.Raw())) {
                tableAliases = stats->TableAliases.Get();
            }

            TOrderingInfo orderingInfo;
            if constexpr (std::is_same_v<TSortCallable, TCoTopBase>) {
                orderingInfo = GetTopBaseSortingOrderingInfo(sortCallable, nullptr, tableAliases);
            } else if constexpr (std::is_same_v<TSortCallable, TCoSortBase>) {
                orderingInfo = GetSortBaseSortingOrderingInfo(sortCallable, nullptr, tableAliases);
            } else {
                static_assert(false, "There's no such callable");
            }

            bool ascOnly =
                    std::all_of(
                        orderingInfo.Directions.begin(),
                        orderingInfo.Directions.end(),
                        [](auto dir) { return dir == TOrdering::TItem::EAscending; }
                    );

            if (!ascOnly) { // we may have desc direction in topsort - so we will consider two cases : asc and desc table reads
                if (auto maybeReadTable = GetReadTable(sortCallable.Input().Raw())) {
                    CollectKqpReadTable<GetDescDirections>(*maybeReadTable);
                }
            }

            auto sorting = TSorting(orderingInfo.Ordering, orderingInfo.Directions);
            std::size_t sortingsOrderingIdx = FDStorage.AddSorting(sorting, tableAliases);

            YQL_CLOG(TRACE, CoreDq) << "Collected " << sortCallable.CallableName() << " interesting ordering idx: " << sortingsOrderingIdx << ", TableAliases: " << TableAliasToString(tableAliases);
        }

        TMaybe<TExprBase> GetReadTable(const TExprNode* const input) {
            if (auto maybeFlatMapBase = TMaybeNode<TCoFlatMapBase>(input)) {
                return GetReadTable(maybeFlatMapBase.Cast().Input().Raw());
            }

            if (auto maybeExtractMembers = TMaybeNode<TCoExtractMembers>(input)) {
                return GetReadTable(maybeExtractMembers.Input().Raw());
            }

            if (auto maybeKqlReadTableRangesBase = TMaybeNode<TKqlReadTableRangesBase>(input)) {
                return maybeKqlReadTableRangesBase.Cast();
            }

            if (auto maybeKqlReadTableBase = TMaybeNode<TKqlReadTableBase>(input)) {
                return maybeKqlReadTableBase.Cast();
            }

            return Nothing();
        }

    private:
        template <auto GetDirs>
        void CollectKqlReadTableIndexRanges(const TKqlReadTableIndexRanges& readTableIndexRanges) {
            auto stats = KqpStats->GetStats(readTableIndexRanges.Raw());
            if (!stats) {
                return;
            }

            TString alias;
            if (stats->Aliases && stats->Aliases->size() == 1) {
                alias = *stats->Aliases->begin();
            }

            auto sortedBy = stats->KeyColumns->ToJoinColumns(alias);
            auto sorting = TSorting(sortedBy, GetDirs(sortedBy.size()));
            std::size_t orderingIdx = FDStorage.AddSorting(sorting);
            YQL_CLOG(TRACE, CoreDq) << "Collected KqlReadTableIndexRanges interesting ordering idx: " << orderingIdx;
        }
    private:
        // collect functional dependencies from the filter
        void CollectFlatMapBase(const TCoFlatMapBase& flatMapBase) {
            const auto& lambdaBody = flatMapBase.Lambda().Body();
            if (!IsPredicateFlatMap(lambdaBody.Ref())) {
                return;
            }

            auto computer = TPredicateSelectivityComputer(
                nullptr,
                false,
                true,
                true
            );

            auto lambdaStats = KqpStats->GetStats(lambdaBody.Raw());
            computer.Compute(lambdaBody);

            TTableAliasMap* tableAliases = lambdaStats? lambdaStats->TableAliases.Get(): nullptr;
            bool alwaysActive = IsRead(flatMapBase.Input().Raw());
            for (const auto& [lMember, rMember]: computer.GetMemberEqualities()) {
                auto lhs = GetColumnFromMember(lMember);
                auto rhs = GetColumnFromMember(rMember);
                FDStorage.AddEquivalence(lhs, rhs, alwaysActive, tableAliases);
            }

            for (const auto& member: computer.GetConstantMembers()) {
                TJoinColumn constant = GetColumnFromMember(member);
                FDStorage.AddConstant(constant, alwaysActive, tableAliases);
            }
        }

        bool IsRead(
            const TExprNode* const input
        ) {
            if (auto maybeExtractMembers = TMaybeNode<TCoExtractMembers>(input)) {
                return IsRead(maybeExtractMembers.Input().Raw());
            }

            return
                TMaybeNode<TKqlReadTableRangesBase>(input) ||
                TMaybeNode<TKqlReadTableBase>(input);
        }

        TJoinColumn GetColumnFromMember(const TCoMember& member) {
            TJoinColumn column = TJoinColumn::FromString(member.Name().StringValue());
            if (auto stats = KqpStats->GetStats(member.Raw()); stats && column.RelName.empty()) {
                if (stats->Aliases && stats->Aliases->size() == 1) {
                    column.RelName = *stats->Aliases->begin();
                }
            }
            return column;
        }
    };

private:
    TInterestingOrderingsCollector InterestingOrderingsCollector;
};

// Forward declaration (defined below)
static void PropogateTableAliasesFromChildren(const TExprNode::TPtr& input, TKqpStatsStore* kqpStats);

/**
 * DoTransform method matches operators and callables in the query DAG and
 * uses pre-computed statistics and costs of the children to compute their cost.
 */
IGraphTransformer::TStatus TKqpStatisticsTransformer::DoTransform(
    TExprNode::TPtr input,
    TExprNode::TPtr& output,
    TExprContext& ctx
) {
    output = input;
    if (Config->CostBasedOptimizationLevel.Get().GetOrElse(TDqSettings::TDefault::CostBasedOptimizationLevel) == 0) {
        return IGraphTransformer::TStatus::Ok;
    }

    auto runPass = [&] {
        VisitExprLambdasLast(
            input,
            [&](const TExprNode::TPtr& input) {
                BeforeLambdas(input, ctx) || BeforeLambdasSpecific(input, ctx) || BeforeLambdasUnmatched(input, ctx);
                if (input->IsCallable()) {
                    PropagateStatisticsToLambdaArgument(input, KqpStats);
                }
                return true;
            },
            [&](const TExprNode::TPtr& input) {
                AfterLambdas(input, ctx) || AfterLambdasSpecific(input, ctx);
                PropogateTableAliasesFromChildren(input, KqpStats);
                return true;
            });
    };

    if (!KqpStats->ShufflingsFSM) {
        runPass();
        /* ^ we have to propogate statistics to work with aliases */

        auto fsmBuilder = TInterestingOrderingsFSMBuilder(*TypeCtx, KqpStats);
        fsmBuilder.Build(input);
    }

    TxStats.clear();
    runPass();

    return IGraphTransformer::TStatus::Ok;
}

bool TKqpStatisticsTransformer::BeforeLambdasSpecific(const TExprNode::TPtr& input, TExprContext& ctx) {
    Y_UNUSED(ctx);
    bool matched = true;
    // KQP Matchers
    if(TKqlReadTableIndexRanges::Match(input.Get())) {
        InferStatisticsForReadTableIndexRanges(input, TypeCtx, KqpCtx, KqpStats);
    }
    else if(TKqlReadTableBase::Match(input.Get()) || TKqlReadTableRangesBase::Match(input.Get())){
        InferStatisticsForReadTable(input, TypeCtx, KqpCtx, KqpStats);
    }
    else if(TKqlStreamLookupIndex::Match(input.Get())){
        InferStatisticsForIndexLookup(input, TypeCtx, KqpStats);
    }
    else if(TKqlLookupTableBase::Match(input.Get())) {
        InferStatisticsForLookupTable(input, TypeCtx, KqpStats);
    }
    else if(TKqpTable::Match(input.Get())) {
        InferStatisticsForKqpTable(input, TypeCtx, KqpCtx, KqpStats);
    }
    else if (TKqpReadRangesSourceSettings::Match(input.Get())) {
        InferStatisticsForRowsSourceSettings(input, TypeCtx, KqpCtx, KqpStats);
    }
    else if (TKqpCnStreamLookup::Match(input.Get())) {
        InferStatisticsForSteamLookup(input, TypeCtx, KqpCtx, KqpStats);
    }
    else if (TKqlIndexLookupJoinBase::Match(input.Get())) {
        InferStatisticsForLookupJoin(input, TypeCtx, KqpStats);
    }

    // Match a result binding atom and connect it to a stage
    else if(TCoParameter::Match(input.Get())) {
        InferStatisticsForResultBinding(input, TypeCtx, TxStats, KqpStats);
    }
    else if(TDqSourceWrapBase::Match(input.Get())) {
        InferStatisticsForDqSourceWrap(input, TypeCtx, KqpCtx, KqpStats);
    }
    else if (TKqpOlapFilter::Match(input.Get())) {
        InferStatisticsForOlapFilter(input, TypeCtx, KqpStats);
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
        AppendTxStats(input, TypeCtx, TxStats, KqpStats);
    } else if (TKqpReadOlapTableRangesBase::Match(input.Get())) {
        InferStatisticsForOlapRead(input, TypeCtx, KqpStats);
    } else {
        matched = false;
    }

    return matched;
}

// KQP version of table-alias propagation (uses TKqpStatsStore instead of TTypeAnnotationContext).
static void PropogateTableAliasesFromChildren(const TExprNode::TPtr& input, TKqpStatsStore* kqpStats) {
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

bool TKqpStatisticsTransformer::BeforeLambdas(const TExprNode::TPtr& input, TExprContext& ctx) {
    Y_UNUSED(ctx);
    bool matched = true;
    auto hints = KqpCtx.GetOptimizerHints();

    if (TCoFilterBase::Match(input.Get())) {
        InferStatisticsForFilter(input, KqpStats);
    }
    else if (TCoSkipNullMembers::Match(input.Get())) {
        InferStatisticsForSkipNullMembers(input, KqpStats);
    }
    else if (auto aggregateBase = TMaybeNode<TCoAggregateBase>(input.Get())) {
        InferStatisticsForAggregateBase(input, KqpStats);
    }
    else if (TCoAggregateMergeFinalize::Match(input.Get())) {
        InferStatisticsForAggregateMergeFinalize(input, KqpStats);
    }
    else if (TCoAsList::Match(input.Get())) {
        InferStatisticsForAsList(input, KqpStats);
    }
    else if (TCoParameter::Match(input.Get()) && InferStatisticsForListParam(input, KqpStats)) {
    }
    else if (TCoMapJoinCore::Match(input.Get())) {
        InferStatisticsForMapJoin(input, KqpStats, KqpPctx, hints);
    }
    else if (TCoGraceJoinCore::Match(input.Get())) {
        InferStatisticsForGraceJoin(input, KqpStats, KqpPctx, hints, &KqpCtx.ShufflingOrderingsByJoinLabels);
    }
    else if (TDqBlockHashJoinCore::Match(input.Get())) {
        InferStatisticsForBlockHashJoin(input, KqpStats, KqpPctx, hints);
    }
    else if (TDqScalarHashJoinCore::Match(input.Get())) {
        InferStatisticsForScalarHashJoin(input, KqpStats, KqpPctx, hints);
    }
    else if (auto dqJoinBase = TMaybeNode<TDqJoinBase>(input.Get())) {
        InferStatisticsForDqJoinBase(input, KqpStats, KqpPctx, hints);
    }
    // Do nothing in case of EquiJoin, otherwise the EquiJoin rule won't fire
    else if (TCoEquiJoin::Match(input.Get())) {
        InferStatisticsForEquiJoin(input, KqpStats);
    }
    // In case of DqSource, propagate the statistics from the correct argument
    else if (TDqSource::Match(input.Get())) {
        InferStatisticsForDqSource(input, KqpStats);
    }
    else if (TDqCnMerge::Match(input.Get())) {
        InferStatisticsForDqMerge(input, KqpStats);
    }
    else if (auto extendBase = TMaybeNode<TCoExtendBase>(input)) {
        InferStatisticsForExtendBase(input, KqpStats);
    }
    else if (TCoAsStruct::Match(input.Get())) {
        InferStatisticsForAsStruct(input, KqpStats);
    }
    else if (auto topBase = TMaybeNode<TCoTopBase>(input)) {
        InferStatisticsForTopBase(input, KqpStats);
    }
    else if (auto sortBase = TMaybeNode<TCoSortBase>(input)) {
        InferStatisticsForSortBase(input, KqpStats);
    }
    else if (TCoUnionAll::Match(input.Get())) {
        InferStatisticsForUnionAll(input, KqpStats);
    }
    else if (TCoShuffleByKeys::Match(input.Get())) {
        InferStatisticsForAggregationCallable<TCoShuffleByKeys>(input, KqpStats);
    }
    else {
        matched = false;
    }

    return matched;
}

bool TKqpStatisticsTransformer::BeforeLambdasUnmatched(const TExprNode::TPtr& input, TExprContext& ctx) {
    Y_UNUSED(ctx);
    if (input->ChildrenSize() >= 1) {
        auto stats = KqpStats->GetStats(input->ChildRef(0).Get());
        if (stats) {
            KqpStats->SetStats(input.Get(), RemoveOrderings(stats, input));
        }
    }
    return true;
}

bool TKqpStatisticsTransformer::AfterLambdas(const TExprNode::TPtr& input, TExprContext& ctx) {
    Y_UNUSED(ctx);
    bool matched = true;
    if (TDqStageBase::Match(input.Get())) {
        InferStatisticsForStage(input, KqpStats);
    } else if (TCoFlatMapBase::Match(input.Get())) {
        InferStatisticsForFlatMap(input, KqpStats);
    } else {
        matched = false;
    }
    return matched;
}

TAutoPtr<IGraphTransformer> CreateKqpStatisticsTransformer(const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx,
    TTypeAnnotationContext& typeCtx, const TKikimrConfiguration::TPtr& config, const TKqpProviderContext& pctx) {
    return THolder<IGraphTransformer>(new TKqpStatisticsTransformer(kqpCtx, typeCtx, config, pctx));
}

} // namespace NKikimr::NKqp
