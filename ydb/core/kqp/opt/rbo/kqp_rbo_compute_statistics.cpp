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

TVector<TInfoUnit> ConvertKeyColumns(TIntrusivePtr<NYql::TOptimizerStatistics::TKeyColumns> keyColumns, const TVector<TInfoUnit>& outputColumns) {
    if (!keyColumns) {
        return {};
    }

    TVector<TInfoUnit> result;
    for (const auto& key : keyColumns->Data) {
        auto it = std::find_if(outputColumns.begin(), outputColumns.end(), [&key](const TInfoUnit& iu) {
            return key == iu.GetColumnName();
        });

        Y_ENSURE(it != outputColumns.end());
        result.push_back(*it);
    }
    return result;
}

void ComputeAlisesForJoin(const std::shared_ptr<IOperator>& left, const std::shared_ptr<IOperator>& right, TVector<TString>& leftAliases,
                          TVector<TString>& rightAliases, TVector<TString>& unionOfAliases) {
    THashSet<TString> leftAliasSet;
    THashSet<TString> rightAliasSet;

    for (const auto& iu : left->GetOutputIUs()) {
        if (auto lineage = left->Props.Metadata->ColumnLineage.Mapping.find(iu); lineage != left->Props.Metadata->ColumnLineage.Mapping.end()) {
            TString alias = lineage->second.GetSourceAlias();
            if (alias == "") {
                alias = lineage->second.TableName;
            }
            leftAliasSet.insert(alias);
        }
        if (auto lineage = right->Props.Metadata->ColumnLineage.Mapping.find(iu); lineage != right->Props.Metadata->ColumnLineage.Mapping.end()) {
            TString alias = lineage->second.GetSourceAlias();
            if (alias == "") {
                alias = lineage->second.TableName;
            }
            rightAliasSet.insert(alias);
        }
    }

    leftAliases.insert(leftAliases.begin(), leftAliasSet.begin(), leftAliasSet.end());
    std::sort(leftAliases.begin(), leftAliases.end());
    rightAliases.insert(rightAliases.begin(), rightAliasSet.begin(), rightAliasSet.end());
    std::sort(rightAliases.begin(), rightAliases.end());
    std::set_union(leftAliasSet.begin(), leftAliasSet.end(), rightAliasSet.begin(), rightAliasSet.end(),
            std::back_inserter(unionOfAliases));
}
}

namespace NKikimr {
namespace NKqp {

using namespace NYql;
using namespace NYql::NNodes;

/**
 * Default metadata computation for unary operators
 */
void IUnaryOperator::ComputeMetadata(TRBOContext& ctx, TPlanProps& planProps) {
    Y_UNUSED(ctx);
    Y_UNUSED(planProps);
    Props.Metadata = GetInput()->Props.Metadata;
}

/**
 * Default statistics and cost computation for unary operators
 */
void IUnaryOperator::ComputeStatistics(TRBOContext& ctx, TPlanProps& planProps) {
    Y_UNUSED(ctx);
    Y_UNUSED(planProps);
    Props.Statistics = GetInput()->Props.Statistics;
    Props.Cost = GetInput()->Props.Cost;
}

/***
 * Compute metadata for empty source
 */
void TOpEmptySource::ComputeMetadata(TRBOContext& ctx, TPlanProps& planProps) {
    Y_UNUSED(ctx);
    Y_UNUSED(planProps);
    Props.Metadata = TRBOMetadata();
}

/***
 * Compute costs and statistics for empty source
 */
void TOpEmptySource::ComputeStatistics(TRBOContext& ctx, TPlanProps& planProps) {
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
void TOpRead::ComputeMetadata(TRBOContext& ctx, TPlanProps& planProps) {
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

    for(const auto& column : tableData.Metadata->KeyColumnNames) {
        if (std::find(Columns.begin(), Columns.end(), column) == Columns.end()) {
            Props.Metadata->KeyColumns = {};
            break;
        }
        Props.Metadata->KeyColumns.emplace_back(Alias, column);
    }

    // Record lineage: source can rename its columns, so already we need to record that
    auto outputIUs = GetOutputIUs();

    const int duplicateId = Props.Metadata->ColumnLineage.AddAlias(Alias, path.StringValue());
    for (size_t i = 0; i < outputIUs.size(); i++) {
        Props.Metadata->ColumnLineage.AddMapping(outputIUs[i], TColumnLineageEntry(Alias, path.StringValue(), Columns[i], duplicateId));
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

    YQL_CLOG(TRACE, CoreDq) << "Inferred metadata for table: " << path.Value();
}

/***
 * Add cost and statistics info for read operator
 */
void TOpRead::ComputeStatistics(TRBOContext& ctx, TPlanProps& planProps) {
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
void TOpFilter::ComputeStatistics(TRBOContext& ctx, TPlanProps& planProps) {
    Y_UNUSED(ctx);
    Y_UNUSED(planProps);
    if (!GetInput()->Props.Statistics.has_value() || !Props.Metadata.has_value()) {
        return;
    }

    Props.Statistics = GetInput()->Props.Statistics;
    Props.Cost = GetInput()->Props.Cost;

    auto inputStats = std::make_shared<TOptimizerStatistics>(BuildOptimizerStatistics(GetInput()->Props, true));
    auto lambda = TCoLambda(FilterExpr.Node);
    double selectivity = TPredicateSelectivityComputer(inputStats).Compute(lambda.Body());

    double filterSelectivity = selectivity * Props.Statistics->Selectivity;
    Props.Statistics->DataSize = filterSelectivity * Props.Statistics->DataSize;
    Props.Statistics->Selectivity = filterSelectivity;
}

/**
 * Compute metadata for map operator. 
 */
void TOpMap::ComputeMetadata(TRBOContext& ctx, TPlanProps& planProps) {
    Y_UNUSED(ctx);
    Y_UNUSED(planProps);
    if (!GetInput()->Props.Metadata.has_value()) {
        return;
    }
    auto inputMetadata = *GetInput()->Props.Metadata;
    Props.Metadata = TRBOMetadata();

    Props.Metadata->Type = inputMetadata.Type;
    Props.Metadata->StorageType = inputMetadata.StorageType;

    if (Project) {
        Props.Metadata->ColumnsCount = MapElements.size();
    } else {
        Props.Metadata->ColumnsCount += inputMetadata.ColumnsCount + MapElements.size();
    }

    auto renamesWithTransform = GetRenamesWithTransforms(planProps);

    for (const auto& column : inputMetadata.KeyColumns) {
        const auto it = std::find_if(renamesWithTransform.begin(), renamesWithTransform.end(), [&column](const std::pair<TInfoUnit, TInfoUnit>& rename) {
            // Check that a key column has been renamed into something new
            return column == rename.second;
        });

        if (it != renamesWithTransform.end()) {
            // Add the new name to column list
            Props.Metadata->KeyColumns.push_back(it->first);
        } else {
            Props.Metadata->KeyColumns.push_back(column);
        }

        if (Project && it == renamesWithTransform.end()) {
            Props.Metadata->KeyColumns = {};
            break;
        }
    }

    // Build lineage data
    Props.Metadata->ColumnLineage = {};
    auto renames = GetRenames();

    for (const auto& iu : GetOutputIUs()) {
        const auto it = std::find_if(renames.begin(), renames.end(), [&iu](const std::pair<TInfoUnit, TInfoUnit>& rename) { return iu == rename.first; });

        if (it != renames.end() && inputMetadata.ColumnLineage.Mapping.contains(it->second)) {
            Props.Metadata->ColumnLineage.AddMapping(iu, inputMetadata.ColumnLineage.Mapping.at(it->second));
        } else if (it == renames.end() && inputMetadata.ColumnLineage.Mapping.contains(iu)) {
            Props.Metadata->ColumnLineage.AddMapping(iu, inputMetadata.ColumnLineage.Mapping.at(iu));
        }
    }
}

/**
 * Compute costs and statistics for map operator
 * We only modify ByteSize based on old and new number of columns
 */
void TOpMap::ComputeStatistics(TRBOContext& ctx, TPlanProps& planProps) {
    Y_UNUSED(ctx);
    Y_UNUSED(planProps);
    if (!GetInput()->Props.Statistics.has_value() || !Props.Metadata.has_value()) {
        return;
    }

    Props.Statistics = GetInput()->Props.Statistics;
    Props.Cost = GetInput()->Props.Cost;

    const auto inputColumnsCount = GetInput()->Props.Metadata->ColumnsCount;
    if (Props.Metadata->ColumnsCount != inputColumnsCount) {
        double inputDataSize = Props.Statistics->DataSize;
        if (inputColumnsCount!=0) {
            Props.Statistics->DataSize = inputDataSize * Props.Metadata->ColumnsCount / (double)inputColumnsCount;
        }
        // Input may have 0 columns (e.g. EmptySource), in such case the data size depends on the number of records
        // and the number of columns in the output. We just assume each column contains 8 bytes
        else {
            Props.Statistics->DataSize = Props.Statistics->RecordsCount * Props.Metadata->ColumnsCount * 8;
        }
    }
}

/**
 * Compute metadata for aggregare operator
 */
void TOpAggregate::ComputeMetadata(TRBOContext& ctx, TPlanProps& planProps) {
    Y_UNUSED(ctx);
    Y_UNUSED(planProps);
    if (!GetInput()->Props.Metadata.has_value()) {
        return;
    }

    Props.Metadata = GetInput()->Props.Metadata;
    Props.Metadata->Type = EStatisticsType::BaseTable;
    Props.Metadata->KeyColumns = KeyColumns;
    Props.Metadata->ColumnsCount = GetOutputIUs().size();

    // Aggregate acts list a source in terms of lineage
    // FIXME: We currently delete all lineage of columns before Aggregate, maybe this is suboptimal in some future cases?
    Props.Metadata->ColumnLineage = {};
    TString alias = "_aggregate";
    int duplicateId = Props.Metadata->ColumnLineage.AddAlias(alias, alias);
    for (const auto & iu : GetOutputIUs()) {
        Props.Metadata->ColumnLineage.AddMapping(iu, TColumnLineageEntry(alias, alias, iu.GetColumnName(), duplicateId));
    }
}

/**
 * Compute cost and statistics for aggregate
 * TODO: Need real cardinality and cost here
 */
void TOpAggregate::ComputeStatistics(TRBOContext& ctx, TPlanProps& planProps) {
    Y_UNUSED(ctx);
    Y_UNUSED(planProps);
    if (!GetInput()->Props.Statistics.has_value() || !Props.Metadata.has_value()) {
        return;
    }

    Props.Statistics = GetInput()->Props.Statistics;
    Props.Cost = GetInput()->Props.Cost;

    const auto inputColumnsCount = GetInput()->Props.Metadata->ColumnsCount;
    if (Props.Metadata->ColumnsCount != inputColumnsCount) {
        double inputDataSize = Props.Statistics->DataSize;
        Props.Statistics->DataSize = inputDataSize * Props.Metadata->ColumnsCount / (double)inputColumnsCount;
    }
}

/**
 * Compute metadata for join operator
 * Currently we make use of current CBO method that computes statistics for joins
 */
void TOpJoin::ComputeMetadata(TRBOContext& ctx, TPlanProps& planProps) {
    Y_UNUSED(ctx);
    Y_UNUSED(planProps);
    if (!GetLeftInput()->Props.Metadata.has_value() || !GetRightInput()->Props.Metadata.has_value()) {
        return;
    }

    Props.Metadata = TRBOMetadata();
    
    auto leftStats = std::make_shared<TOptimizerStatistics>(BuildOptimizerStatistics(GetLeftInput()->Props, false));
    auto rightStats = std::make_shared<TOptimizerStatistics>(BuildOptimizerStatistics(GetRightInput()->Props, false));

    TVector<TJoinColumn> leftJoinKeys;
    TVector<TJoinColumn> rightJoinKeys;

    for (const auto& [leftKey, rightKey] : JoinKeys) {
        leftJoinKeys.push_back(TJoinColumn(leftKey.GetAlias(), leftKey.GetColumnName()));
        rightJoinKeys.push_back(TJoinColumn(rightKey.GetAlias(), rightKey.GetColumnName()));
    }

    TVector<TString> leftAliases;
    TVector<TString> rightAliases;
    TVector<TString> unionOfAliases;
    ComputeAlisesForJoin(GetLeftInput(), GetRightInput(), leftAliases, rightAliases, unionOfAliases);
    
    EJoinAlgoType joinAlgo = Props.JoinAlgo.has_value() ? *Props.JoinAlgo : EJoinAlgoType::Undefined;

    auto hints = ctx.KqpCtx.GetOptimizerHints();
    auto CBOStats = ctx.CBOCtx.ComputeJoinStatsV2(*leftStats, 
        *rightStats, 
        leftJoinKeys, 
        rightJoinKeys,
        joinAlgo,
        ConvertToJoinKind(JoinKind),
        FindCardHint(unionOfAliases, *hints.CardinalityHints),
        false,
        false,
        FindCardHint(unionOfAliases, *hints.BytesHints));

    Props.Metadata->ColumnsCount = GetLeftInput()->Props.Metadata->ColumnsCount + GetRightInput()->Props.Metadata->ColumnsCount;
    Props.Metadata->KeyColumns = ConvertKeyColumns(CBOStats.KeyColumns, GetOutputIUs());
    Props.Metadata->StorageType = CBOStats.StorageType;
    Props.Metadata->Type = CBOStats.Type;

    if (JoinKind == "LeftOnly" || JoinKind == "LeftSemi") {
        Props.Metadata->ColumnLineage = GetLeftInput()->Props.Metadata->ColumnLineage;
    } else if (JoinKind == "RightOnly" || JoinKind == "RightSemi") {
        Props.Metadata->ColumnLineage = GetRightInput()->Props.Metadata->ColumnLineage;
    } else {
        Props.Metadata->ColumnLineage = GetLeftInput()->Props.Metadata->ColumnLineage;
        Props.Metadata->ColumnLineage.Merge(GetRightInput()->Props.Metadata->ColumnLineage);
    }
}

void TOpJoin::ComputeStatistics(TRBOContext& ctx, TPlanProps& planProps) {
    Y_UNUSED(ctx);
    Y_UNUSED(planProps);
    if (!GetLeftInput()->Props.Statistics.has_value() || !GetRightInput()->Props.Statistics.has_value()) {
        return;
    }

    Props.Statistics = TRBOStatistics();
    
    auto leftStats = std::make_shared<TOptimizerStatistics>(BuildOptimizerStatistics(GetLeftInput()->Props, true));
    auto rightStats = std::make_shared<TOptimizerStatistics>(BuildOptimizerStatistics(GetRightInput()->Props, true));

    TVector<TJoinColumn> leftJoinKeys;
    TVector<TJoinColumn> rightJoinKeys;

    for (const auto& [leftKey, rightKey] : JoinKeys) {
        leftJoinKeys.push_back(TJoinColumn(leftKey.GetAlias(), leftKey.GetColumnName()));
        rightJoinKeys.push_back(TJoinColumn(rightKey.GetAlias(), rightKey.GetColumnName()));
    }

    TVector<TString> leftAliases;
    TVector<TString> rightAliases;
    TVector<TString> unionOfAliases;
    ComputeAlisesForJoin(GetLeftInput(), GetRightInput(), leftAliases, rightAliases, unionOfAliases);

    auto hints = ctx.KqpCtx.GetOptimizerHints();

    leftStats = ApplyRowsHints(leftStats, leftAliases, *hints.CardinalityHints);
    rightStats = ApplyRowsHints(rightStats, rightAliases, *hints.CardinalityHints);

    leftStats = ApplyBytesHints(leftStats, leftAliases, *hints.BytesHints);
    rightStats = ApplyBytesHints(rightStats, rightAliases, *hints.BytesHints);

    auto CBOStats = ctx.CBOCtx.ComputeJoinStatsV2(*leftStats, 
        *rightStats, 
        leftJoinKeys, 
        rightJoinKeys,
        Props.JoinAlgo.has_value() ? *Props.JoinAlgo : EJoinAlgoType::Undefined,
        ConvertToJoinKind(JoinKind),
        FindCardHint(unionOfAliases, *hints.CardinalityHints),
        false,
        false,
        FindCardHint(unionOfAliases, *hints.BytesHints));

    Props.Statistics->DataSize = CBOStats.ByteSize;
    Props.Statistics->RecordsCount = CBOStats.Nrows;
    Props.Statistics->Selectivity = CBOStats.Selectivity;

    if (Props.JoinAlgo.has_value()) {
        Props.Cost = CBOStats.Cost;
    } else {
        Props.Cost = std::nullopt;
    }
}

void TOpUnionAll::ComputeMetadata(TRBOContext& ctx, TPlanProps& planProps) {
    Y_UNUSED(ctx);
    Y_UNUSED(planProps);
    if (!GetLeftInput()->Props.Metadata.has_value() || !GetRightInput()->Props.Metadata.has_value()) {
        return;
    }

    Props.Metadata = TRBOMetadata();
    Props.Metadata->ColumnsCount = GetLeftInput()->Props.Metadata->ColumnsCount + GetRightInput()->Props.Metadata->ColumnsCount;
}

void TOpUnionAll::ComputeStatistics(TRBOContext& ctx, TPlanProps& planProps) {
    Y_UNUSED(ctx);
    Y_UNUSED(planProps);
    if (!GetLeftInput()->Props.Statistics.has_value() || !GetRightInput()->Props.Statistics.has_value()) {
        return;
    }

    Props.Statistics = TRBOStatistics();
    Props.Statistics->DataSize = GetLeftInput()->Props.Statistics->DataSize + GetRightInput()->Props.Statistics->DataSize;
    Props.Statistics->RecordsCount = GetLeftInput()->Props.Statistics->RecordsCount + GetRightInput()->Props.Statistics->RecordsCount;

    if (GetLeftInput()->Props.Cost.has_value() && GetRightInput()->Props.Cost.has_value()) {
        Props.Cost = *GetLeftInput()->Props.Cost + *GetRightInput()->Props.Cost;
    } else {
        Props.Cost = std::nullopt;
    }
}

void TOpCBOTree::ComputeMetadata(TRBOContext& ctx, TPlanProps& planProps) {
    for (auto op: TreeNodes) {
        op->ComputeMetadata(ctx, planProps);
    }

    Props.Metadata = TreeRoot->Props.Metadata;
}

void TOpCBOTree::ComputeStatistics(TRBOContext& ctx, TPlanProps& planProps) {
    for (auto op: TreeNodes) {
        op->ComputeStatistics(ctx, planProps);
    }

    Props.Statistics = TreeRoot->Props.Statistics;
    Props.Cost = TreeRoot->Props.Cost;
}

void TOpRoot::ComputePlanMetadata(TRBOContext& ctx) {
    for (auto it : *this) {
        it.Current->ComputeMetadata(ctx, PlanProps);
    }
}

void TOpRoot::ComputePlanStatistics(TRBOContext& ctx) {
    for (auto it : *this) {
        it.Current->ComputeStatistics(ctx, PlanProps);
    }
}

}
}