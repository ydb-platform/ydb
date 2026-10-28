#include "kqp_operator.h"

#include <ydb/core/kqp/opt/cbo/cbo_optimizer_hints.h>
#include <ydb/core/kqp/opt/cbo/cbo_optimizer_new.h>
#include <ydb/core/kqp/opt/cbo/solver/kqp_opt_predicate_selectivity.h>
#include <ydb/core/kqp/opt/cbo/solver/kqp_opt_stat_kqp.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo_utils.h>

#include <yql/essentials/utils/log/log.h>

namespace NKikimr::NKqp {

/***
 * All the methods to compute metadata and statistics are collected in this file
 */

namespace {

using namespace NKikimr;
using namespace NKikimr::NKqp;
using namespace NYql;
using namespace NYql::NNodes;
using namespace NYql::NDq;

// Known limitation: only the left input's aliases are collected; the right
// side contributes none.
void ComputeAlisesForJoin(IOperator* left, const TColumnLineage& lineage, TVector<TString>& leftAliases,
                          TVector<TString>& rightAliases, TVector<TString>& unionOfAliases) {
    leftAliases = lineage.GetAliases(left->Props.Metadata->HintRelations);
    rightAliases.clear();
    unionOfAliases = leftAliases;
}

TOrderedIUs<> ComputeKeysAfterJoin(TOpJoin* join) {
    auto leftKeys = join->GetLeftInput()->Props.Metadata->KeyColumns;
    if (!JoinOutputsRight(join->JoinKind)) {
        return leftKeys;
    }

    auto rightKeys = join->GetRightInput()->Props.Metadata->KeyColumns;
    if (!JoinOutputsLeft(join->JoinKind)) {
        return rightKeys;
    }
    
    if (leftKeys.Items().empty() || rightKeys.Items().empty()) {
        return {};
    }

    // If right join keys covers all the keys of the right hand side,
    // we don't need the key of the right side at all
    if (rightKeys.Unordered().IsSubsetOf(join->JoinKeys.Right())) {
        return leftKeys;
    }
    // Same for the left side
    else if (leftKeys.Unordered().IsSubsetOf(join->JoinKeys.Left())) {
        return rightKeys;
    }

    else {
        leftKeys.AppendMissing(rightKeys.Items());
        return leftKeys;
    }
}

using TStorageBindings = THashMap<TString, TInfoUnitId>;

TStorageBindings AddSourceLineage(TColumnLineage& lineage, TRBOMetadata& metadata, const TUnorderedIUs& columns,
    const TInfoUnitRegistry& registry, const TString& table, const TString& alias)
{
    TStorageBindings bindings;
    bindings.reserve(columns.Size());
    const auto relation = lineage.AddRelation(alias, table);
    metadata.SourceStatsColumns.UnionWith(columns);
    for (const auto id : columns) {
        const auto name = registry.Get(id).GetColumnName();
        // Several IDs may fetch one field. Retain one representative for keys;
        // every binding still receives its own lineage entry.
        bindings.try_emplace(name, id);
        lineage.Add(id, TColumnLineageEntry{.SourceAlias = alias, .TableName = table, .ColumnName = name, .Relation = relation});
        metadata.HintRelations.Add(id, relation);
    }
    return bindings;
}

TOrderedIUs<> ResolveStorageColumns(const TVector<TString>& names, const TStorageBindings& bindings) {
    TOrderedIUs<> result;
    result.Reserve(names.size());
    for (const auto& name : names) {
        const auto it = bindings.find(name);
        if (it == bindings.end()) {
            return {};
        }
        result.Append(it->second);
    }
    return result;
}

template <typename TValue, typename TRebind>
TOrderedIUs<TValue> RebindColumns(const TOrderedIUs<TValue>& columns, const TRebind& rebind) {
    TOrderedIUs<TValue> result;
    result.Reserve(columns.Items().size());
    for (const auto& entry : columns.Items()) {
        if constexpr (std::is_void_v<TValue>) {
            result.Append(rebind(entry));
        } else {
            result.Append(rebind(entry.first), entry.second);
        }
    }
    return result;
}

TJoinColumn StatisticsColumn(const TColumnLineage& lineage, TInfoUnitId id) {
    const auto* entry = lineage.Find(id);
    return TJoinColumn(entry ? entry->GetRawAlias() : TString{}, ToString(id));
}

} // anonymous namespace

/**
 * Default metadata computation for unary operators
 */
void IUnaryOperator::ComputeMetadata(TRBOContext& ctx, TPlanProps& planProps) {
    Y_UNUSED(ctx);
    Y_UNUSED(planProps);
    Props.Metadata = GetInput()->Props.Metadata;
    if (Props.Metadata) {
        Props.Metadata->SourceStatsColumns.IntersectWith(GetOutputIUs());
        Props.Metadata->HintRelations.RetainKeys(GetOutputIUs());
    }
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

void TOpReplicate::ComputeMetadata(TRBOContext& ctx, TPlanProps& planProps) {
    Y_UNUSED(ctx);
    Props.Metadata = GetInput()->Props.Metadata;
    if (IsPrimary() || !Props.Metadata) {
        return;
    }
    const auto& bindings = GetRebindings();
    const auto& inputs = GetInput()->GetOutputIUs();
    const auto rebind = [&](TInfoUnitId id) {
        const auto* output = bindings.Find(id);
        Y_ENSURE(output, "Missing Replicate metadata binding");
        return *output;
    };
    // Producer metadata may mention IDs it does not output (e.g. columns a
    // semi lookup join fetched). Keep only what this port can rebind.
    const auto rebindAll = [&](auto& columns) {
        columns = columns.Unordered().IsSubsetOf(inputs)
            ? RebindColumns(columns, rebind) : std::remove_reference_t<decltype(columns)>{};
    };
    auto& metadata = *Props.Metadata;
    rebindAll(metadata.KeyColumns);
    rebindAll(metadata.ShuffledByColumns);
    TUnorderedIUs sourceStats;
    for (const auto id : metadata.SourceStatsColumns) {
        sourceStats.Add(rebind(id));
    }
    metadata.SourceStatsColumns = std::move(sourceStats);
    // The port is another instance of every relation it reads from.
    auto& lineage = planProps.ColumnLineage;
    THashMap<ui32, ui32> relations;
    const auto rebindRelation = [&](ui32 relation) {
        const auto [it, inserted] = relations.try_emplace(relation);
        if (inserted) {
            it->second = lineage.CopyRelation(relation);
        }
        return it->second;
    };
    TMappedIUs<ui32> hints;
    for (const auto& [id, relation] : metadata.HintRelations.Items()) {
        hints.Add(rebind(id), rebindRelation(relation));
    }
    metadata.HintRelations = std::move(hints);
    for (const auto id : inputs) {
        if (const auto* source = lineage.Find(id)) {
            auto entry = *source;
            entry.Relation = rebindRelation(entry.Relation);
            lineage.Add(rebind(id), std::move(entry));
        }
    }
}

void TOpReplicate::ComputeStatistics(TRBOContext& ctx, TPlanProps& planProps) {
    Y_UNUSED(ctx);
    Y_UNUSED(planProps);
    Props.Statistics = GetInput()->Props.Statistics;
    Props.Cost = GetInput()->Props.Cost;
}

/***
 * Compute metadata for table lookup
 */
void TOpTableLookup::ComputeMetadata(TRBOContext& ctx, TPlanProps& planProps) {
    auto path = TKqpTable(Table).Path();
    const auto& tableData = ctx.KqpCtx.Tables->ExistingTable(ctx.KqpCtx.Cluster, path.Value());

    Props.Metadata = TRBOMetadata();
    if (IsJoin() && GetInput()->Props.Metadata.has_value()) {
        Props.Metadata = GetInput()->Props.Metadata;
    }
    Props.Metadata->ColumnsCount += GetColumns().Size();
    Props.Metadata->StorageType = EStorageType::RowStorage;

    const auto& registry = planProps.InfoUnitRegistry;
    const TString alias = GetColumns().Empty() ? TString{} : registry.Get(*GetColumns().begin()).GetAlias();
    const auto bindings = AddSourceLineage(planProps.ColumnLineage, *Props.Metadata, GetColumns(), registry, path.StringValue(), alias);
    Props.Metadata->SourceStatsColumns.IntersectWith(GetOutputIUs());
    Props.Metadata->HintRelations.RetainKeys(GetOutputIUs());

    if (IsJoin()) {
        Props.Metadata->KeyColumns = {};
        return;
    }

    TOrderedIUs<> keyColumns;
    for (const auto& key : tableData.Metadata->KeyColumnNames) {
        if (const auto it = bindings.find(key); it != bindings.end()) {
            keyColumns.Append(it->second);
        }
    }
    Props.Metadata->KeyColumns = std::move(keyColumns);
}

/***
 * Compute metadata for empty source
 */
void TOpEmptySource::ComputeMetadata(TRBOContext& ctx, TPlanProps& planProps) {
    Y_UNUSED(ctx);
    Y_UNUSED(planProps);
    Props.Metadata = TRBOMetadata();
    Props.Metadata->LogicalCard = Input ? ELogicalCardinality::ZeroOrMore : ELogicalCardinality::One;
}

/***
 * Compute costs and statistics for empty source
 */
void TOpEmptySource::ComputeStatistics(TRBOContext& ctx, TPlanProps& planProps) {
    Y_UNUSED(ctx);
    Y_UNUSED(planProps);
    Y_ENSURE(Props.Metadata.has_value());
    Props.Statistics = TRBOStatistics();
    Props.Statistics->ERows = 1;
    Props.Statistics->EBytes = 1;
    Props.Cost = 0;
}

/***
 * Compute metadata for source operator
 * This method also fetches Nrows and ByteSize statistics
 */
void TOpRead::ComputeMetadata(TRBOContext& ctx, TPlanProps& planProps) {
    auto readTable = TKqpTable(TableCallable);
    auto path = readTable.Path();

    if (readTable.PathId() == "") {
        // CTAS don't have created table during compilation.
        return;
    }

    Props.Metadata = TRBOMetadata();

    const auto& tableData = ctx.KqpCtx.Tables->ExistingTable(ctx.KqpCtx.Cluster, path.Value());
    Props.Metadata->ColumnsCount = GetColumns().Size();
    const auto bindings = AddSourceLineage(planProps.ColumnLineage, *Props.Metadata, GetColumns(), planProps.InfoUnitRegistry, path.StringValue(), Alias);
    Props.Metadata->KeyColumns = ResolveStorageColumns(tableData.Metadata->KeyColumnNames, bindings);

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

    if (storageType == EStorageType::ColumnStorage && !tableData.Metadata->PartitionedByColumns.empty()) {
        Props.Metadata->ShuffledByColumns = ResolveStorageColumns(tableData.Metadata->PartitionedByColumns, bindings);
    }

    YQL_CLOG(TRACE, CoreDq) << "Inferred metadata for table: " << path.Value();
}

/***
 * Add cost and statistics info for read operator
 */
void TOpRead::ComputeStatistics(TRBOContext& ctx, TPlanProps& planProps) {
    if (!Props.Metadata.has_value()) {
        return;
    }

    auto readTable = TKqpTable(TableCallable);
    auto path = readTable.Path();
    const auto& tableData = ctx.KqpCtx.Tables->ExistingTable(ctx.KqpCtx.Cluster, path.Value());

    Props.Statistics = TRBOStatistics();
    Props.Statistics->ERows = tableData.Metadata->RecordsCount;
    Props.Statistics->EBytes = tableData.Metadata->DataSize;
    Props.Cost = 0;

    auto overrideStats = ctx.KqpCtx.GetOverrideStatistics();
    if (overrideStats) {
        auto dbStats = overrideStats->GetMapSafe();
        if (auto it = dbStats.find(path.Value()); it != dbStats.end()) {
            auto tableStats = it->second.GetMapSafe();
            if (auto nrows = tableStats.find("n_rows"); nrows != tableStats.end()) {
                Props.Statistics->ERows = nrows->second.GetDoubleSafe();
            }
            if (auto byteSize = tableStats.find("byte_size"); byteSize != tableStats.end()) {
                Props.Statistics->EBytes = byteSize->second.GetDoubleSafe();
            }
        }
    }

    auto hints = ctx.KqpCtx.GetOptimizerHints();
    auto hintCandidates = BuildTableHintCandidates(Alias, path.StringValue());
    if (hints.CardinalityHints) {
        ApplySingleLabelHint(*hints.CardinalityHints, hintCandidates, Props.Statistics->ERows);
    }
    if (hints.BytesHints) {
        ApplySingleLabelHint(*hints.BytesHints, hintCandidates, Props.Statistics->EBytes);
    }

    const auto totalColumns = tableData.Metadata->Columns.size();
    const auto readColumns = GetColumns().Size();
    if (totalColumns > 0 && readColumns < totalColumns) {
        Props.Statistics->EBytes *= static_cast<double>(readColumns) / static_cast<double>(totalColumns);
    }

    // Overwrite with selectivity for successfully pushed-down filters within the read operator.
    if (OriginalPredicate.has_value()) {
        auto inputStats = std::make_shared<TOptimizerStatistics>(BuildOptimizerStatistics(*this, planProps.ColumnLineage, true, ctx.TypeCtx));
        auto lambda = TCoLambda(OriginalPredicate->Node);
        double selectivity = TPredicateSelectivityComputer(inputStats).Compute(lambda.Body());

        double filterSelectivity = selectivity * Props.Statistics->Selectivity;
        Props.Statistics->EBytes = filterSelectivity * Props.Statistics->EBytes;
        Props.Statistics->ERows = filterSelectivity * Props.Statistics->ERows;
        Props.Statistics->Selectivity = filterSelectivity;
    }
}

/**
 * Compute metadata for Filter
 */
void TOpFilter::ComputeMetadata(TRBOContext& ctx, TPlanProps& planProps) {
    Y_UNUSED(ctx);
    Y_UNUSED(planProps);
    if (!GetInput()->Props.Metadata.has_value()) {
        return;
    }

    Props.Metadata = GetInput()->Props.Metadata;

    auto newCard = Props.Metadata->LogicalCard;

    switch( Props.Metadata->LogicalCard) {
        case ELogicalCardinality::OneOrMore:
            newCard = ELogicalCardinality::ZeroOrMore;
            break;
        case ELogicalCardinality::One:
        case ELogicalCardinality::ZeroOrOne:
            newCard = ELogicalCardinality::ZeroOrOne;
            break;
        default:
            break;
    }

    Props.Metadata->LogicalCard = newCard;
}

/**
 * Compute statistics and costs for Filter
 */
void TOpFilter::ComputeStatistics(TRBOContext& ctx, TPlanProps& planProps) {
    Y_UNUSED(ctx);
    if (!GetInput()->Props.Statistics.has_value() || !Props.Metadata.has_value()) {
        return;
    }

    Props.Statistics = GetInput()->Props.Statistics;
    Props.Cost = GetInput()->Props.Cost;

    if (PartiallyPushedDown) {
        return;
    }

    auto inputStats = std::make_shared<TOptimizerStatistics>(BuildOptimizerStatistics((*GetInput()), planProps.ColumnLineage, true, ctx.TypeCtx));
    auto lambda = TCoLambda(FilterExpr.Node);
    double selectivity = TPredicateSelectivityComputer(inputStats).Compute(lambda.Body());

    double filterSelectivity = selectivity * Props.Statistics->Selectivity;
    Props.Statistics->EBytes = filterSelectivity * Props.Statistics->EBytes;
    Props.Statistics->ERows = filterSelectivity * Props.Statistics->ERows;
    Props.Statistics->Selectivity = filterSelectivity;
}

/**
 * Compute metadata for map operator. 
 */
void TOpMap::ComputeMetadata(TRBOContext& ctx, TPlanProps& planProps) {
    Y_UNUSED(ctx);
    if (!GetInput()->Props.Metadata.has_value()) {
        return;
    }
    const auto& inputMetadata = *GetInput()->Props.Metadata;
    Props.Metadata = TRBOMetadata();

    Props.Metadata->Type = inputMetadata.Type;
    Props.Metadata->StorageType = inputMetadata.StorageType;
    Props.Metadata->ColumnsCount = GetOutputIUs().Size();
    // A Map only appends: the input rows, their keys and distribution survive.
    Props.Metadata->KeyColumns = inputMetadata.KeyColumns;
    Props.Metadata->ShuffledByColumns = inputMetadata.ShuffledByColumns;
    Props.Metadata->SourceStatsColumns = inputMetadata.SourceStatsColumns;
    Props.Metadata->HintRelations = inputMetadata.HintRelations;

    // A copy has the lineage of its source
    auto& lineage = planProps.ColumnLineage;
    for (const auto& [id, element] : MapElements.Items()) {
        if (element.IsColumnAccess()) {
            const auto input = element.GetColumnAccess();
            if (inputMetadata.SourceStatsColumns.Contains(input)) {
                Props.Metadata->SourceStatsColumns.Add(id);
            }
            if (const auto* relation = inputMetadata.HintRelations.Find(input)) {
                Props.Metadata->HintRelations.Add(id, *relation);
            }
            if (const auto* source = lineage.Find(element.GetColumnAccess())) {
                lineage.Add(id, *source);
            }
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
        double inputDataSize = Props.Statistics->EBytes;
        if (inputColumnsCount!=0) {
            Props.Statistics->EBytes = inputDataSize * Props.Metadata->ColumnsCount / (double)inputColumnsCount;
        }
        // Input may have 0 columns (e.g. EmptySource), in such case the data size depends on the number of records
        // and the number of columns in the output. We just assume each column contains 8 bytes
        else {
            Props.Statistics->EBytes = Props.Statistics->ERows * Props.Metadata->ColumnsCount * 8;
        }
    }
}

/**
 * Compute metadata for aggregare operator
 */
void TOpAggregate::ComputeMetadata(TRBOContext& ctx, TPlanProps& planProps) {
    if (!GetInput()->Props.Metadata.has_value()) {
        return;
    }

    const auto& inputMetadata = *GetInput()->Props.Metadata;

    Props.Metadata = TRBOMetadata();

    Props.Metadata->StorageType = inputMetadata.StorageType;
    // Compute logical cardinality info. Its the same as input cardinality, except in the case
    // where the group-by list is empty, then we always produce a single tuple
    Props.Metadata->LogicalCard = KeyColumns.Items().empty() ? ELogicalCardinality::One : inputMetadata.LogicalCard;
    Props.Metadata->Type = EStatisticsType::BaseTable;

    const auto& outputIUs = GetOutputIUs();
    // If the aggregate just adds more columns to existing key columns, use original key columns
    if (DistinctAll) {
        Props.Metadata->KeyColumns = TOrderedIUs<>(outputIUs.begin(), outputIUs.end());
    } else if (IsDeduplication()) {
        // Like DistinctAll: the whole grouping tuple is unique, even when the
        // input has no known key.
        Props.Metadata->KeyColumns = KeyColumns;
    } else if (inputMetadata.KeyColumns.Unordered().IsSubsetOf(KeyColumns.Unordered()))
    {
        Props.Metadata->KeyColumns = inputMetadata.KeyColumns;
    } else {
        Props.Metadata->KeyColumns = KeyColumns;
    }
    Props.Metadata->ColumnsCount = outputIUs.Size();

    Props.Metadata->ShuffledByColumns = GetAggregatePreservedShuffling(*this, ctx);

    // Aggregate acts like a source for the columns it computes. Grouping keys
    // pass through under their input IDs and keep their input lineage.
    TString alias = "_aggregate";
    const auto relation = planProps.ColumnLineage.AddRelation(alias);
    // Statistical and hint boundaries are independent of the keys' SSA IDs.
    for (const auto id : outputIUs) {
        Props.Metadata->HintRelations.Add(id, relation);
    }
    for (const auto id : Aggregations.Keys()) {
        planProps.ColumnLineage.Add(id, TColumnLineageEntry{.SourceAlias = alias, .ColumnName = ::ToString(id), .Relation = relation});
    }
}

void TOpGroupingSets::ComputeMetadata(TRBOContext& ctx, TPlanProps& planProps) {
    Y_UNUSED(ctx);
    Props.Metadata = GetInput()->Props.Metadata;
    if (!Props.Metadata) {
        return;
    }
    auto& metadata = *Props.Metadata;
    TMappedIUs<TInfoUnitId> outputs;
    for (const auto output : Columns.Keys()) {
        const auto input = *Columns.Find(output);
        if (!outputs.Find(input)) {
            outputs.Add(input, output);
        }
    }
    // Keep the input metadata, rebound to the output IDs. A pruned component
    // invalidates the whole key, not just that component.
    const auto rebind = [&](const auto& columns) {
        return columns.Unordered().IsSubsetOf(outputs.Keys())
            ? RebindColumns(columns, [&](TInfoUnitId id) { return *outputs.Find(id); })
            : std::decay_t<decltype(columns)>{};
    };
    metadata.KeyColumns = rebind(metadata.KeyColumns);
    metadata.ShuffledByColumns = rebind(metadata.ShuffledByColumns);
    metadata.SourceStatsColumns.Clear();
    metadata.HintRelations.Clear();
    const auto& inputMetadata = *GetInput()->Props.Metadata;
    auto& lineage = planProps.ColumnLineage;
    for (const auto& [output, input] : Columns.Items()) {
        if (inputMetadata.SourceStatsColumns.Contains(input)) {
            metadata.SourceStatsColumns.Add(output);
        }
        if (const auto* relation = inputMetadata.HintRelations.Find(input)) {
            metadata.HintRelations.Add(output, *relation);
        }
        if (const auto* source = lineage.Find(input)) {
            lineage.Add(output, *source);
        }
    }
    metadata.ColumnsCount = GetOutputIUs().Size();
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
        double inputDataSize = Props.Statistics->EBytes;
        Props.Statistics->EBytes = inputDataSize * Props.Metadata->ColumnsCount / (double)inputColumnsCount;
    }
}

/**
 * Compute metadata for join operator
 * Currently we make use of current CBO method that computes statistics for joins
 */
void TOpJoin::ComputeMetadata(TRBOContext& ctx, TPlanProps& planProps) {
    Y_UNUSED(ctx);
    if (!GetLeftInput()->Props.Metadata.has_value() || !GetRightInput()->Props.Metadata.has_value()) {
        return;
    }

    Props.Metadata = TRBOMetadata();

    // FIXME: Compute decent logical cardinality
    Props.Metadata->LogicalCard = ELogicalCardinality::ZeroOrMore;
    
    auto leftStats = std::make_shared<TOptimizerStatistics>(BuildOptimizerStatistics((*GetLeftInput()), planProps.ColumnLineage, false, ctx.TypeCtx));
    auto rightStats = std::make_shared<TOptimizerStatistics>(BuildOptimizerStatistics((*GetRightInput()), planProps.ColumnLineage, false, ctx.TypeCtx));

    TVector<TJoinColumn> leftJoinKeys;
    TVector<TJoinColumn> rightJoinKeys;

    for (const auto& [leftKey, rightKey, equalNulls] : JoinKeys.Items()) {
        leftJoinKeys.push_back(StatisticsColumn(planProps.ColumnLineage, leftKey));
        rightJoinKeys.push_back(StatisticsColumn(planProps.ColumnLineage, rightKey));
        leftJoinKeys.back().EqualNulls = rightJoinKeys.back().EqualNulls = equalNulls;
    }

    TVector<TString> leftAliases;
    TVector<TString> rightAliases;
    TVector<TString> unionOfAliases;
    ComputeAlisesForJoin(GetLeftInput().Get(), planProps.ColumnLineage, leftAliases, rightAliases, unionOfAliases);
    
    NKqp::EJoinAlgoType joinAlgo = Props.JoinAlgo.has_value() ? *Props.JoinAlgo : NKqp::EJoinAlgoType::Undefined;

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
    Props.Metadata->StorageType = CBOStats.StorageType;
    Props.Metadata->Type = CBOStats.Type;

    Props.Metadata->KeyColumns = ComputeKeysAfterJoin(this);
    const auto& outputs = GetOutputIUs();
    for (const auto* input : GetChildren()) {
        Props.Metadata->SourceStatsColumns.UnionWith(input->Props.Metadata->SourceStatsColumns);
        for (const auto& [id, relation] : input->Props.Metadata->HintRelations.Items()) {
            if (outputs.Contains(id)) {
                Props.Metadata->HintRelations.Add(id, relation);
            }
        }
    }
    Props.Metadata->SourceStatsColumns.IntersectWith(outputs);

    NKqp::EJoinAlgoType algo = Props.JoinAlgo.has_value() ? *Props.JoinAlgo : NKqp::EJoinAlgoType::Undefined;
    if (algo == NKqp::EJoinAlgoType::MapJoin) {
        // Build side is always right (broadcast), so left-family output keeps the left distribution.
        // For right-family joins be conservative: output rows are right-side rows, so left-side
        // shuffling is not a valid distribution claim for the result.
        const bool rightSided = (JoinKind == "Right" || JoinKind == "RightSemi" || JoinKind == "RightOnly");
        if (!rightSided) {
            Props.Metadata->ShuffledByColumns = GetLeftInput()->Props.Metadata->ShuffledByColumns;
        }
    } else if (algo == NKqp::EJoinAlgoType::GraceJoin) {
        // Both sides will be partitioned by their respective join keys,
        // but the columns by which they are shuffled may not survive after the join.

        // For example, if you do a right semi join, the columns from the right
        // table won't be available.

        // For consistency, we'll take join keys from the right table for the "right"
        // family of joins (including ones like a simple right join in which
        // we can probably take either) and columns from the left keys otherwise.

        // TODO: Later down the line, we should store ordering id instead of a column
        // list. This will allow us to not discard compatible joins, when one of
        // the equal columns is dropped by a projection later.

        bool rightSided = (JoinKind == "Right" || JoinKind == "RightSemi" || JoinKind == "RightOnly");
        // Describe the shuffle stage assignment performs: CBO's key order when it
        // chose one (join key pairs have no order), the input's own distribution
        // when that side's shuffle was eliminated.
        const auto& shuffleBy = rightSided ? Props.RightShuffleBy : Props.LeftShuffleBy;
        if (shuffleBy && shuffleBy->Items().empty()) {
            Props.Metadata->ShuffledByColumns = (rightSided ? (*GetRightInput()) : (*GetLeftInput())).Props.Metadata->ShuffledByColumns;
        } else if (shuffleBy) {
            Props.Metadata->ShuffledByColumns = *shuffleBy;
        } else {
            for (const auto& [leftKey, rightKey, equalNulls] : JoinKeys.Items()) {
                Props.Metadata->ShuffledByColumns.Append(rightSided ? rightKey : leftKey);
            }
        }
    }
    // Currently there are no other algos.
    // If any other are added, leave ShuffledByColumns empty (unknown distribution) - the safest default.
}

void TOpJoin::ComputeStatistics(TRBOContext& ctx, TPlanProps& planProps) {
    Y_UNUSED(ctx);
    if (!GetLeftInput()->Props.Statistics.has_value() || !GetRightInput()->Props.Statistics.has_value()) {
        return;
    }

    Props.Statistics = TRBOStatistics();
    
    auto leftStats = std::make_shared<TOptimizerStatistics>(BuildOptimizerStatistics((*GetLeftInput()), planProps.ColumnLineage, true, ctx.TypeCtx));
    auto rightStats = std::make_shared<TOptimizerStatistics>(BuildOptimizerStatistics((*GetRightInput()), planProps.ColumnLineage, true, ctx.TypeCtx));

    TVector<TJoinColumn> leftJoinKeys;
    TVector<TJoinColumn> rightJoinKeys;

    for (const auto& [leftKey, rightKey, equalNulls] : JoinKeys.Items()) {
        leftJoinKeys.push_back(StatisticsColumn(planProps.ColumnLineage, leftKey));
        rightJoinKeys.push_back(StatisticsColumn(planProps.ColumnLineage, rightKey));
        leftJoinKeys.back().EqualNulls = rightJoinKeys.back().EqualNulls = equalNulls;
    }

    TVector<TString> leftAliases;
    TVector<TString> rightAliases;
    TVector<TString> unionOfAliases;
    ComputeAlisesForJoin(GetLeftInput().Get(), planProps.ColumnLineage, leftAliases, rightAliases, unionOfAliases);

    auto hints = ctx.KqpCtx.GetOptimizerHints();

    leftStats = ApplyRowsHints(leftStats, leftAliases, *hints.CardinalityHints);
    rightStats = ApplyRowsHints(rightStats, rightAliases, *hints.CardinalityHints);

    leftStats = ApplyBytesHints(leftStats, leftAliases, *hints.BytesHints);
    rightStats = ApplyBytesHints(rightStats, rightAliases, *hints.BytesHints);

    auto CBOStats = ctx.CBOCtx.ComputeJoinStatsV2(*leftStats, 
        *rightStats, 
        leftJoinKeys, 
        rightJoinKeys,
        Props.JoinAlgo.has_value() ? *Props.JoinAlgo : NKqp::EJoinAlgoType::Undefined,
        ConvertToJoinKind(JoinKind),
        FindCardHint(unionOfAliases, *hints.CardinalityHints),
        false,
        false,
        FindCardHint(unionOfAliases, *hints.BytesHints));

    Props.Statistics->EBytes = CBOStats.ByteSize;
    Props.Statistics->ERows = CBOStats.Nrows;
    Props.Statistics->Selectivity = CBOStats.Selectivity;

    if (Props.JoinAlgo.has_value()) {
        Props.Cost = CBOStats.Cost;
    } else {
        Props.Cost = std::nullopt;
    }
}

// It does not have runtime support, it could be eliminated or we will rewrite it into cross join.
void TOpDependentJoin::ComputeMetadata(TRBOContext& ctx, TPlanProps& planProps) {
    Y_UNUSED(ctx);
    Y_UNUSED(planProps);
    if (!GetDomain()->Props.Metadata.has_value() || !GetInput()->Props.Metadata.has_value()) {
        return;
    }

    Props.Metadata = TRBOMetadata();
    Props.Metadata->LogicalCard = ELogicalCardinality::ZeroOrMore;
    Props.Metadata->ColumnsCount = GetOutputIUs().Size();
}

void TOpDependentJoin::ComputeStatistics(TRBOContext& ctx, TPlanProps& planProps) {
    Y_UNUSED(ctx);
    Y_UNUSED(planProps);
    if (!GetDomain()->Props.Statistics.has_value() || !GetInput()->Props.Statistics.has_value()) {
        return;
    }

    Props.Statistics = TRBOStatistics();
    // Just a workaround, we do not have runtime support anyway.
    Props.Statistics->ERows = GetDomain()->Props.Statistics->ERows * GetInput()->Props.Statistics->ERows;
    Props.Statistics->EBytes = GetDomain()->Props.Statistics->EBytes + GetInput()->Props.Statistics->EBytes;
    Props.Cost = std::nullopt;
}

void TOpUnionAll::ComputeMetadata(TRBOContext& ctx, TPlanProps& planProps) {
    Y_UNUSED(ctx);
    Y_UNUSED(planProps);
    for (const auto& input : GetChildren()) {
        if (!input->Props.Metadata.has_value()) {
            return;
        }
    }

    Props.Metadata = TRBOMetadata();
    Props.Metadata->ColumnsCount = GetOutputIUs().Size();
}

void TOpUnionAll::ComputeStatistics(TRBOContext& ctx, TPlanProps& planProps) {
    Y_UNUSED(ctx);
    Y_UNUSED(planProps);
    for (const auto& input : GetChildren()) {
        if (!input->Props.Statistics.has_value()) {
            return;
        }
    }

    Props.Statistics = TRBOStatistics();

    double cost = 0.0;
    bool allInputsHaveCost = true;
    for (const auto& input : GetChildren()) {
        Props.Statistics->EBytes += input->Props.Statistics->EBytes;
        Props.Statistics->ERows += input->Props.Statistics->ERows;
        if (input->Props.Cost.has_value()) {
            cost += *input->Props.Cost;
        } else {
            allInputsHaveCost = false;
        }
    }

    if (allInputsHaveCost) {
        Props.Cost = cost;
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
    PlanProps.ColumnLineage.Clear();
    for (const auto& it : *this) {
        it.Current->ComputeMetadata(ctx, PlanProps);
        if (const auto& metadata = it.Current->Props.Metadata) {
            const auto& outputs = it.Current->GetOutputIUs();
            Y_ENSURE(metadata->SourceStatsColumns.IsSubsetOf(outputs));
            Y_ENSURE(metadata->HintRelations.Keys().IsSubsetOf(outputs));
        }
    }
}

void TOpRoot::ComputePlanStatistics(TRBOContext& ctx) {
    for (const auto& it : *this) {
        it.Current->ComputeStatistics(ctx, PlanProps);
    }
}

} // namespace NKikimr::NKqp
