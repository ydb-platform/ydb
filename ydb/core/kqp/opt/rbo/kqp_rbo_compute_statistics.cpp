#include "kqp_operator.h"

#include <yql/essentials/core/yql_cost_function.h>
#include <yql/essentials/core/cbo/cbo_optimizer_new.h>
#include <ydb/library/yql/dq/opt/dq_opt_stat.h>

/***
 * All the methods to compute metadata and statistics are collected in this file
 */

namespace {

using namespace NKikimr;
using namespace NKikimr::NKqp;
using namespace NYql;
using namespace NYql::NDq;

TOptimizerStatistics BuildOptimizerStatistics(TPhysicalOpProps & props, bool withStatsAndCosts) {
    TVector<TString> keyColumnNames;
    for (auto iu: props.Metadata->KeyColumns) {
        keyColumnNames.push_back(iu.ColumnName);
    }

    return TOptimizerStatistics(props.Metadata->Type, 
        withStatsAndCosts ? props.Statistics->DataSize : 0.0,
        props.Metadata->ColumnsCount,
        withStatsAndCosts ? props.Statistics->DataSize : 0.0,
        withStatsAndCosts ? *props.Cost : 0.0,
        TIntrusivePtr<TOptimizerStatistics::TKeyColumns>(
            new TOptimizerStatistics::TKeyColumns(keyColumnNames)));
}

}

namespace NKikimr {
namespace NKqp {

using namespace NYql;
using namespace NYql::NNodes;

/**
 * Default metadata computation for unary operators
 */
void IUnaryOperator::ComputeMetadata(TRBOContext & ctx, TPlanProps & planProps) {
    Y_UNUSED(ctx);
    Y_UNUSED(planProps);
    Props.Metadata = GetInput()->Props.Metadata;
}

/**
 * Default statistics and cost computation for unary operators
 */
void IUnaryOperator::ComputeStatistics(TRBOContext & ctx, TPlanProps & planProps) {
    Y_UNUSED(ctx);
    Y_UNUSED(planProps);
    Props.Statistics = GetInput()->Props.Statistics;
    Props.Cost = GetInput()->Props.Cost;
}

/***
 * Compute metadata for empty source
 */
void TOpEmptySource::ComputeMetadata(TRBOContext & ctx, TPlanProps & planProps) {
    Y_UNUSED(ctx);
    Y_UNUSED(planProps);
    Props.Metadata = TRBOMetadata();
}

/***
 * Compute costs and statistics for empty source
 */
void TOpEmptySource::ComputeStatistics(TRBOContext & ctx, TPlanProps & planProps) {
    Y_UNUSED(ctx);
    Y_UNUSED(planProps);
    Y_ENSURE(Props.Metadata.has_value());
    Props.Statistics = TRBOStatistics();
    Props.Statistics->RecordsCount = 1;
    Props.Statistics->DataSize = 1;
    Props.Cost = 0;
}

/***
 * Compute metadata for source operator
 * This method also fetches Nrows and ByteSize statistics
 */
void TOpRead::ComputeMetadata(TRBOContext & ctx, TPlanProps & planProps) {
    Y_UNUSED(planProps);

    auto readTable = TKqpTable(TableCallable);
    auto path = readTable.Path();

    if (readTable.PathId() == "") {
        // CTAS don't have created table during compilation.
        return;
    }

    Props.Metadata = TRBOMetadata();

    const auto& tableData = ctx.KqpCtx.Tables->ExistingTable(ctx.KqpCtx.Cluster, path.Value());
    Props.Metadata->ColumnsCount = tableData.Metadata->Columns.size();
    Props.Metadata->KeyColumns = {};
    for( auto col : tableData.Metadata->KeyColumnNames) {
        Props.Metadata->KeyColumns.push_back(TInfoUnit(Alias, col));
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
    Props.Metadata->StorageType = storageType;

    /***
     * Will add support for shuffling and ordering later
     

    if (!tableData.Metadata->PartitionedByColumns.empty()) {
        TVector<TJoinColumn> shuffledByColumns;
        for (const auto& columnName: tableData.Metadata->PartitionedByColumns) {
            shuffledByColumns.emplace_back(Alias, columnName);
        }

        stats->ShuffledByColumns = TIntrusivePtr<TOptimizerStatistics::TShuffledByColumns>(
            new TOptimizerStatistics::TShuffledByColumns(std::move(shuffledByColumns))
        );
    }

    auto& shufflingFSM = planProps.ShufflingFSM;
    if (shufflingFSM && stats && stats->ShuffledByColumns) {
        auto shuffledBy = stats->ShuffledByColumns->Data;
        for (auto& column: shuffledBy) {
            column.RelName = Alias;
        }
        auto shuffling = TShuffling(shuffledBy).SetNatural();
        std::int64_t orderingIdx = shufflingFSM->FDStorage.FindShuffling(shuffling, nullptr);
        stats->LogicalOrderings = shufflingFSM->CreateState(orderingIdx);
    }

    auto& sortingsFSM = planProps.SortingsFSM;
    if (sortingsFSM && stats && stats->KeyColumns && stats->StorageType == EStorageType::RowStorage) {
        const TVector<TString>& keyColumns = stats->KeyColumns->Data;

        TVector<TJoinColumn> sortedBy(keyColumns.size());
        for (std::size_t i = 0; i < sortedBy.size(); ++i) {
            sortedBy[i].RelName = Alias;
            sortedBy[i].AttributeName = keyColumns[i];
        }

        auto sorting = TSorting(sortedBy, GetAscDirections(sortedBy.size()));
        std::int64_t orderingIdx = sortingsFSM->FDStorage.FindSorting(sorting, nullptr);
        stats->SortingOrderings = sortingsFSM->CreateState(orderingIdx);

        auto reversedSorting = TSorting(sortedBy, GetDescDirections(sortedBy.size()));
        std::int64_t reversedOrderingIdx = sortingsFSM->FDStorage.FindSorting(reversedSorting, nullptr);
        stats->ReversedSortingOrderings = sortingsFSM->CreateState(reversedOrderingIdx);
    }
*/

    Props.Metadata->Aliases = {Alias};

    YQL_CLOG(TRACE, CoreDq) << "Inferred metadata for table: " << path.Value();
}

/***
 * Add cost and statistics info for read operator
 */
void TOpRead::ComputeStatistics(TRBOContext & ctx, TPlanProps & planProps) {
    Y_UNUSED(planProps);
    if (!Props.Metadata.has_value()) {
        return;
    }

    auto readTable = TKqpTable(TableCallable);
    auto path = readTable.Path();
    const auto& tableData = ctx.KqpCtx.Tables->ExistingTable(ctx.KqpCtx.Cluster, path.Value());

    Props.Statistics = TRBOStatistics();
    Props.Statistics->RecordsCount = tableData.Metadata->RecordsCount;
    Props.Statistics->DataSize = tableData.Metadata->DataSize;
    Props.Cost = 0;
}

/**
 * Compute statistics and costs for Filter
 */
void TOpFilter::ComputeStatistics(TRBOContext & ctx, TPlanProps & planProps) {
    Y_UNUSED(ctx);
    Y_UNUSED(planProps);
    if (!GetInput()->Props.Statistics.has_value() || !Props.Metadata.has_value()) {
        return;
    }

    Props.Statistics = GetInput()->Props.Statistics;
    auto inputStats = std::make_shared<TOptimizerStatistics>(BuildOptimizerStatistics(GetInput()->Props, true));
    auto lambda = TCoLambda(FilterLambda);
    double selectivity = TPredicateSelectivityComputer(inputStats).Compute(lambda.Body());

    double filterSelectivity = selectivity * Props.Statistics->Selectivity;
    Props.Statistics->DataSize = filterSelectivity * Props.Statistics->DataSize;
    Props.Statistics->Selectivity = filterSelectivity;
}

/**
 * Compute metadata for map operator. 
 */
void TOpMap::ComputeMetadata(TRBOContext & ctx, TPlanProps & planProps) {
    Y_UNUSED(ctx);
    Y_UNUSED(planProps);
    if (GetInput()->Props.Metadata.has_value()) {
        return;
    }
    Props.Metadata = *GetInput()->Props.Metadata;

    if (Project) {
        Props.Metadata->ColumnsCount = MapElements.size();

        TVector<TInfoUnit> newKeyColumns;

        // FIXME: Computed columns in many cases preserve key property: e.g. FromPg, Cast, etc.

        auto renames = GetRenames();
        for (auto column : Props.Metadata->KeyColumns) {
            auto it = std::find_if(renames.begin(), renames.end(), 
                [&column](const std::pair<TInfoUnit, TInfoUnit> & rename){
                    return column == rename.first || column == rename.second;
                });
            
            if (it != renames.end()) {
                newKeyColumns.push_back(it->first);
            }
            else {
                newKeyColumns = {};
                break;
            }
        }

        Props.Metadata->KeyColumns = newKeyColumns;

        for (auto ui : GetOutputIUs()){
            if (Props.Metadata->Aliases.contains(ui.Alias)){ 
                    Props.Metadata->Aliases.insert(ui.Alias);
            }
        }
    } 
    else {
        Props.Metadata->ColumnsCount += MapElements.size();
    }
}

/**
 * Compute costs and statistics for map operator
 * We only modify ByteSize based on old and new number of columns
 */
void TOpMap::ComputeStatistics(TRBOContext & ctx, TPlanProps & planProps) {
    Y_UNUSED(ctx);
    Y_UNUSED(planProps);
    if (!GetInput()->Props.Statistics.has_value() || !Props.Metadata.has_value()) {
        return;
    }

    Props.Statistics = GetInput()->Props.Statistics;
    Props.Cost = GetInput()->Props.Cost;

    int inputColumnsCount = GetInput()->Props.Metadata->ColumnsCount;
    if (Props.Metadata->ColumnsCount != inputColumnsCount) {
        double inputDataSize = Props.Statistics->DataSize;
        Props.Statistics->DataSize = inputDataSize * Props.Metadata->ColumnsCount / (double)inputColumnsCount;
    }
}

/**
 * Compute metadata for aggregare operator
 */
void TOpAggregate::ComputeMetadata(TRBOContext & ctx, TPlanProps & planProps) {
    Y_UNUSED(ctx);
    Y_UNUSED(planProps);
    if (!GetInput()->Props.Metadata.has_value()) {
        return;
    }

    Props.Metadata = GetInput()->Props.Metadata;
    Props.Metadata->Type = EStatisticsType::BaseTable;
    Props.Metadata->KeyColumns = KeyColumns;
    Props.Metadata->ColumnsCount = GetOutputIUs().size();
}

/**
 * Compute cost and statistics for aggregate
 * TODO: Need real cardinality and cost here
 */
void TOpAggregate::ComputeStatistics(TRBOContext & ctx, TPlanProps & planProps) {
    Y_UNUSED(ctx);
    Y_UNUSED(planProps);
    if (!GetInput()->Props.Statistics.has_value() || !Props.Metadata.has_value()) {
        return;
    }

    Props.Statistics = GetInput()->Props.Statistics;
    Props.Cost = GetInput()->Props.Cost;

    int inputColumnsCount = GetInput()->Props.Metadata->ColumnsCount;
    if (Props.Metadata->ColumnsCount != inputColumnsCount) {
        double inputDataSize = Props.Statistics->DataSize;
        Props.Statistics->DataSize = inputDataSize * Props.Metadata->ColumnsCount / (double)inputColumnsCount;
    }
}

/**
 * Compute metadata for join operator
 * Currently we make use of current CBO method that computes statistics for joins
 */
void TOpJoin::ComputeMetadata(TRBOContext & ctx, TPlanProps & planProps) {
    Y_UNUSED(ctx);
    Y_UNUSED(planProps);
    if (!GetLeftInput()->Props.Metadata.has_value() || !GetRightInput()->Props.Metadata.has_value()) {
        return;
    }

    Props.Metadata = TRBOMetadata();
    
    auto leftStats = BuildOptimizerStatistics(GetLeftInput()->Props, false);
    auto rightStats = BuildOptimizerStatistics(GetRightInput()->Props, false);

    TVector<TString> leftLabels;
    TVector<TString> rightLabels;
    TVector<TJoinColumn> leftJoinKeys;
    TVector<TJoinColumn> rightJoinKeys;

    for (auto & [leftKey, rightKey] : JoinKeys) {
        leftLabels.push_back(leftKey.Alias);
        rightLabels.push_back(rightKey.Alias);
        leftJoinKeys.push_back(TJoinColumn(leftKey.Alias, leftKey.ColumnName));
        rightJoinKeys.push_back(TJoinColumn(rightKey.Alias, rightKey.ColumnName));
    }

    Props.Metadata->Aliases = GetLeftInput()->Props.Metadata->Aliases;
    Props.Metadata->Aliases.emplace(GetRightInput()->Props.Metadata->Aliases.begin(), GetRightInput()->Props.Metadata->Aliases.end());

    TVector<TString> unionOfLabels;
    unionOfLabels.insert(Props.Metadata->Aliases.begin(), Props.Metadata->Aliases.end());
    EJoinAlgoType joinAlgo = Props.JoinAlgo.has_value() ? *Props.JoinAlgo : EJoinAlgoType::MapJoin;

    auto hints = ctx.KqpCtx.GetOptimizerHints();
    auto CBOStats = ctx.CBOCtx.ComputeJoinStatsV2(leftStats, 
        rightStats, 
        leftJoinKeys, 
        rightJoinKeys,
        joinAlgo,
        ConvertToJoinKind(JoinKind),
        FindCardHint(unionOfLabels, *hints.CardinalityHints)
        false,
        false,
        FindCardHint(unionOfLabels, *hints.BytesHints));

    Props.Metadata->ColumnsCount = GetLeftInput()->Props.Metadata->ColumnsCount + GetRightInput()->Props.Metadata->ColumnsCount;
    Props.Metadata->KeyColumns = CBOStats.KeyColumns;
    Props.Metadata->StorageType = CBOStats.StorageType;
    Props.Metadata->Type = CBOStats.Type;
}

void TOpJoin::ComputeStatistics(TRBOContext & ctx, TPlanProps & planProps) {
    Y_UNUSED(ctx);
    Y_UNUSED(planProps);
    if (!GetLeftInput()->Props.Statistics.has_value() || !GetRightInput()->Props.Statistics.has_value()) {
        return;
    }

    Props.Statistics = TRBOStatistics();
    
    auto leftStats = BuildOptimizerStatistics(GetLeftInput()->Props, true);
    auto rightStats = BuildOptimizerStatitics(GetRightInput()->Props, true);

    TVector<TString> leftLabels;
    TVector<TString> rightLabels;
    TVector<TJoinColumn> leftJoinKeys;
    TVector<TJoinColumn> rightJoinKeys;

    for (auto & [leftKey, rightKey] : JoinKeys) {
        leftLabels.push_back(leftKey.Alias);
        rightLabels.push_back(rightKey.Alias);
        leftJoinKeys.push_back(TJoinColumn(leftKey.Alias, leftKey.ColumnName));
        rightJoinKeys.push_back(TJoinColumn(rightKey.Alias, rightKey.ColumnName));
    }

    leftStats = ApplyRowsHints(leftStats, leftLabels, *hints.CardinalityHints);
    rightStats = ApplyRowsHints(rightStats, rightLabels, *hints.CardinalityHints);

    leftStats = ApplyBytesHints(leftStats, leftLabels, *hints.BytesHints);
    rightStats = ApplyBytesHints(rightStats, rightLabels, *hints.BytesHints);

    Props.Metadata->Aliases = GetLeftInput()->Props.Metadata->Aliases;
    Props.Metadata->Aliases.emplace(GetRightInput()->Props.Metadata->Aliases.begin(), GetRightInput()->Props.Metadata->Aliases.end());

    TVector<TString> unionOfLabels = TVector<TString>(Props.Metadata->Aliases);

    auto hints = ctx.KqpCtx.GetOptimizerHints();
    auto CBOStats = ctx.CBOCtx.ComputeJoinStatsV2(leftStats, 
        rightStats, 
        leftJoinKeys, 
        rightJoinKeys,
        Props.JoinAlgo.has_value() ? *Props.JoinAlgo : EJoinAlgoType::MapJoin,
        ConvertToJoinKind(JoinKind),
        FindCardHint(unionOfLabels, *hints.CardinalityHints)
        false,
        false,
        FindCardHint(unionOfLabels, *hints.BytesHints));

    Props.Statistics->DataSize = CBOStats.ByteSize;
    Props.Statistics->RecordsCount = CBOStats.Nrows;
    Props.Statistics->Selectivity = CBOStats.Selectivity;

    if (Props.JoinAlgo.has_value()) {
        Props.Cost = CBOStats.Cost;
    }
}

void TOpUnionAll::ComputeMetadata(TRBOContext & ctx, TPlanProps & planProps) {
    Y_UNUSED(ctx);
    Y_UNUSED(planProps);
    if (!GetLeftInput()->Props.Metadata.has_value() || !GetRightInput()->Props.Metadata.has_value()) {
        return;
    }

    Props.Metadata = TRBOMetadata();
    Props.Metadata->ColumnsCount = GetLeftInput()->Props.Metadata->ColumnsCount + GetRightInput()->Props.Metadata->ColumnsCount;
}

void TOpUnionAll::ComputeStatistics(TRBOContext & ctx, TPlanProps & planProps) {
    Y_UNUSED(ctx);
    Y_UNUSED(planProps);
    if (!GetLeftInput()->Props.Statistics.has_value() || !GetRightInput()->Props.Statistics.has_value()) {
        return;
    }

    Props.Statistics = TRBOStatistics();
    Props.Statistics->DataSize = GetLeftInput()->Props.Statistics->DataSize + GetRightInput()->Props.Statistics->DataSize;
    Props.Statistics->RecordsCount = GetLeftInput()->Props.Statistics->RecordsCount + GetRightInput()->Props.Statistics->RecordsCount;
}

void TOpRoot::ComputePlanMetadata(TRBOContext & ctx) {
    for (auto it : *this) {
        it.Current->ComputePlanMetadata(ctx);
    }
}

void TOpRoot::ComputePlanStatistics(TRBOContext & ctx) {
    for (auto it : *this) {
        it.Current->ComputePlanStatistics(ctx);
    }
}

}
}