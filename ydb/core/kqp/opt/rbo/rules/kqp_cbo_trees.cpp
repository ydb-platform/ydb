#include "kqp_cbo_trees.h"

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/opt/cbo/solver/kqp_opt_make_join_hypergraph.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo_cbo.h>

#include <library/cpp/iterator/zip.h>

#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/core/yql_type_annotation.h>

#include <bitset>
#include <limits>
#include <sstream>

namespace NKikimr::NKqp {

namespace {

TIntrusivePtr<TOpRead> FindReadThroughMapFilter(const TIntrusivePtr<IOperator>& op) {
    if (op->Kind == EOperator::Source) {
        return CastOperator<TOpRead>(op);
    }

    if (op->Kind == EOperator::Map) {
        return FindReadThroughMapFilter(CastOperator<TOpMap>(op)->GetInput());
    }

    if (op->Kind == EOperator::Filter) {
        return FindReadThroughMapFilter(CastOperator<TOpFilter>(op)->GetInput());
    }

    return {};
}

TString GetReadTableName(const TIntrusivePtr<TOpRead>& read) {
    if (!read || !read->TableCallable) {
        return {};
    }

    return NYql::NNodes::TKqpTable(read->TableCallable).Path().StringValue();
}

TString GetReadRelationName(const TIntrusivePtr<TOpRead>& read) {
    if (!read->Alias.empty()) {
        return read->Alias;
    }

    return GetReadTableName(read);
}

TString MakeUniqueName(const TString& preferred, THashSet<TString>& usedNames) {
    if (usedNames.insert(preferred).second) {
        return preferred;
    }

    for (ui32 suffix = 1;; ++suffix) {
        TString candidate = TStringBuilder() << preferred << suffix;
        if (usedNames.insert(candidate).second) {
            return candidate;
        }
    }
}

TString MakeSyntheticRelationName(ui32& syntheticId, THashSet<TString>& usedNames) {
    constexpr TStringBuf prefix = "_kqp_rbo_cbo_leaf_";
    for (;;) {
        TString candidate = TStringBuilder() << prefix << syntheticId++;
        if (usedNames.insert(candidate).second) {
            return candidate;
        }
    }
}

TString MakeUniqueColumnName(const TString& preferred, THashSet<TString>& usedNames) {
    return MakeUniqueName(preferred.empty() ? TString("_col") : preferred, usedNames);
}

// CBO leaves are boundary operators of the packed join island, not necessarily
// base reads:
//
//       Join ABCD        TreeNodes = [Join AB, Join ABCD]
//      /         \       Leaves    = [Aggregate CD, Map A, Filter B]
//   Join AB   Aggregate CD
//   /    \        |
// Map A Filter B ...
//
// Map/Filter chains over a read use the read alias/table name as the CBO
// relation name. Other boundary subtrees, e.g. Aggregate CD, use generated
// _kqp_rbo_cbo_leaf_N names. Column names come from lineage when available,
// otherwise from output IUs, and are uniquified per leaf.
TCBOLeaf BuildCBOLeaf(
    const TIntrusivePtr<IOperator>& op,
    TCBOBoundaryEdge edge,
    THashSet<TString>& usedRelationNames,
    ui32& syntheticRelationId)
{
    TCBOLeaf leaf = {
        .Op = op,
        .Edge = edge,
    };

    if (auto read = FindReadThroughMapFilter(op)) {
        const auto relationName = GetReadRelationName(read);
        leaf.RelationName = relationName.empty()
            ? MakeSyntheticRelationName(syntheticRelationId, usedRelationNames)
            : MakeUniqueName(relationName, usedRelationNames);
        leaf.SourceTableName = GetReadTableName(read);
    } else {
        leaf.RelationName = MakeSyntheticRelationName(syntheticRelationId, usedRelationNames);
    }

    THashSet<TString> usedColumnNames;
    for (const auto& column : op->GetOutputIUs()) {
        TString cboColumnName;
        if (op->Props.Metadata) {
            const auto& lineage = op->Props.Metadata->ColumnLineage.Mapping;
            if (const auto it = lineage.find(column); it != lineage.end() && !it->second.ColumnName.empty()) {
                cboColumnName = it->second.ColumnName;
            }
        }

        if (cboColumnName.empty()) {
            cboColumnName = column.GetColumnName();
        }

        const auto cboColumn = TInfoUnit(leaf.RelationName, MakeUniqueColumnName(cboColumnName, usedColumnNames));
        leaf.ColumnsToCBO[column] = cboColumn;
        leaf.CBOToColumns[cboColumn] = column;
    }

    return leaf;
}

NKqp::TColumnStatistics ConvertYqlColumnStatistics(const NYql::TColumnStatistics& src) {
    NKqp::TColumnStatistics result;
    result.NumUniqueVals = src.NumUniqueVals;
    result.HyperLogLog = src.HyperLogLog;
    result.CountMinSketch = src.CountMinSketch;
    result.EqWidthHistogramEstimator = src.EqWidthHistogramEstimator;
    result.Type = src.Type;
    return result;
}

TVector<TString> BuildTranslatedKeyColumns(const TCBOLeaf& leaf) {
    TVector<TString> keyColumns;
    if (!leaf.Op->Props.Metadata) {
        return keyColumns;
    }

    for (const auto& key : leaf.Op->Props.Metadata->KeyColumns) {
        if (const auto it = leaf.ColumnsToCBO.find(key); it != leaf.ColumnsToCBO.end()) {
            keyColumns.push_back(it->second.GetColumnName());
        }
    }
    return keyColumns;
}

TIntrusivePtr<TOptimizerStatistics::TColumnStatMap> BuildTranslatedColumnStatistics(
    const TCBOLeaf& leaf,
    NYql::TTypeAnnotationContext& typeCtx)
{
    if (!leaf.Op->Props.Metadata) {
        return {};
    }

    auto result = MakeIntrusive<TOptimizerStatistics::TColumnStatMap>();
    const auto& lineage = leaf.Op->Props.Metadata->ColumnLineage.Mapping;

    for (const auto& [rboColumn, cboColumn] : leaf.ColumnsToCBO) {
        const auto lineageIt = lineage.find(rboColumn);
        if (lineageIt == lineage.end() || lineageIt->second.TableName.empty()) {
            continue;
        }

        const auto tableStatsIt = typeCtx.ColumnStatisticsByTableName.find(lineageIt->second.TableName);
        if (tableStatsIt == typeCtx.ColumnStatisticsByTableName.end()) {
            continue;
        }

        const auto columnStatsIt = tableStatsIt->second->Data.find(lineageIt->second.ColumnName);
        if (columnStatsIt == tableStatsIt->second->Data.end()) {
            continue;
        }

        result->Data[cboColumn.GetColumnName()] = ConvertYqlColumnStatistics(columnStatsIt->second);
    }

    if (result->Data.empty()) {
        return {};
    }
    return result;
}

TOptimizerStatistics BuildLeafOptimizerStatistics(const TCBOLeaf& leaf, NYql::TTypeAnnotationContext& typeCtx) {
    auto stats = BuildOptimizerStatistics(leaf.Op->Props, true, typeCtx);
    stats.KeyColumns = MakeIntrusive<TOptimizerStatistics::TKeyColumns>(BuildTranslatedKeyColumns(leaf));

    if (leaf.Op->Props.Metadata) {
        stats.StorageType = leaf.Op->Props.Metadata->StorageType;
    }

    stats.Aliases = MakeSimpleShared<THashSet<TString>>();
    stats.Aliases->insert(leaf.RelationName);
    if (!leaf.SourceTableName.empty()) {
        stats.SourceTableName = leaf.SourceTableName;
        stats.TableAliases = MakeIntrusive<TTableAliasMap>();
        stats.TableAliases->AddMapping(leaf.SourceTableName, leaf.RelationName);
    }

    stats.ColumnStatistics = BuildTranslatedColumnStatistics(leaf, typeCtx);
    return stats;
}

TVector<const TCBOLeaf*> FindLeavesByRelation(const TVector<TCBOLeaf>& leaves, const TString& relationName) {
    TVector<const TCBOLeaf*> result;
    for (const auto& leaf : leaves) {
        if (leaf.RelationName == relationName || leaf.SourceTableName == relationName) {
            result.push_back(&leaf);
        }
    }
    return result;
}

const TCBOLeaf& FindLeafForRBOColumn(
    const TVector<TCBOLeaf>& leaves,
    const TInfoUnit& column,
    const std::shared_ptr<IBaseOptimizerNode>& side)
{
    THashSet<TString> sideLabels;
    for (const auto& label : side->Labels()) {
        sideLabels.insert(label);
    }

    for (const auto& leaf : leaves) {
        if (sideLabels.contains(leaf.RelationName) && leaf.ColumnsToCBO.contains(column)) {
            return leaf;
        }
    }

    Y_ENSURE(false, TStringBuilder() << "Could not map NEW RBO column "
        << column.GetFullName() << " to a CBO leaf");
    return leaves.front();
}

TJoinColumn ConvertRBOColumnToCBO(
    const TVector<TCBOLeaf>& leaves,
    const TInfoUnit& column,
    const std::shared_ptr<IBaseOptimizerNode>& side)
{
    const auto& leaf = FindLeafForRBOColumn(leaves, column, side);
    const auto it = leaf.ColumnsToCBO.find(column);
    Y_ENSURE(it != leaf.ColumnsToCBO.end());
    return TJoinColumn(it->second.GetAlias(), it->second.GetColumnName());
}

TInfoUnit ConvertCBOColumnToRBO(const TVector<TCBOLeaf>& leaves, const TJoinColumn& column) {
    for (const auto* leafPtr : FindLeavesByRelation(leaves, column.RelName)) {
        const auto& leaf = *leafPtr;
        const auto cboColumn = TInfoUnit(leaf.RelationName, column.AttributeName);
        if (const auto it = leaf.CBOToColumns.find(cboColumn); it != leaf.CBOToColumns.end()) {
            return it->second;
        }
    }

    Y_ENSURE(false, TStringBuilder() << "Could not map CBO column "
        << column.RelName << "." << column.AttributeName
        << " back to NEW RBO input");
    return {};
}

TVector<TInfoUnit> ConvertCBOColumnsToRBO(const TVector<TCBOLeaf>& leaves, const TVector<TJoinColumn>& columns) {
    TVector<TInfoUnit> result;
    result.reserve(columns.size());
    for (const auto& column : columns) {
        result.push_back(ConvertCBOColumnToRBO(leaves, column));
    }
    return result;
}

} // anonymous namespace

TVector<TCBOLeaf> BuildCBOLeaves(const TOpCBOTree& cboTree) {
    TVector<TCBOLeaf> leaves;

    THashSet<IOperator*> treeNodeSet;
    for (const auto& node : cboTree.TreeNodes) {
        treeNodeSet.insert(node.Get());
    }

    THashSet<TString> usedRelationNames;
    ui32 syntheticRelationId = 0;
    for (const auto& node : cboTree.TreeNodes) {
        for (ui32 childIndex = 0; childIndex < node->Children.size(); ++childIndex) {
            const auto& child = node->Children[childIndex];
            if (treeNodeSet.contains(child.Get())) {
                continue;
            }

            leaves.push_back(BuildCBOLeaf(child, TCBOBoundaryEdge{node.Get(), childIndex}, usedRelationNames, syntheticRelationId));
        }
    }

    return leaves;
}

TShuffleEliminationContext BuildShuffleEliminationContext(
    const std::shared_ptr<TJoinOptimizerNode>& joinTree,
    TVector<std::shared_ptr<TRelOptimizerNode>>& rels,
    const TVector<TCBOLeaf>& leaves)
{
    TFDStorage fdStorage;
    TTableAliasMap tableAliasMap;

    // Collect interesting orderings and FDs from the hypergraph shape that DPHyp sees.
    // The original CBO tree can group several join predicates in one operator,
    // while MakeJoinHypergraph splits them by relation pair and adds transitive
    // closure edges. DPHyp edge ordering indexes must be looked up in an FSM
    // built from that same shape.
    auto hypergraph = MakeJoinHypergraph<std::bitset<256>>(joinTree, {}, false);
    for (const auto& edge : hypergraph.GetEdges()) {
        for (const auto& [lhs, rhs] : Zip(edge.LeftJoinKeys, edge.RightJoinKeys)) {
            fdStorage.AddFD(lhs, rhs, TFunctionalDependency::EEquivalence, false, &tableAliasMap);
        }

        fdStorage.AddInterestingOrdering(edge.LeftJoinKeys, TOrdering::EShuffle, &tableAliasMap);
        fdStorage.AddInterestingOrdering(edge.RightJoinKeys, TOrdering::EShuffle, &tableAliasMap);
    }

    TVector<TVector<TJoinColumn>> resolvedLeafShufflings(leaves.size());

    // Translate existing leaf shufflings and sortings into the CBO relation namespace.
    for (size_t i = 0; i < leaves.size(); ++i) {
        const auto& leaf = leaves[i];
        if (!leaf.Op->Props.Metadata.has_value()) {
            continue;
        }
        const auto& metadata = *leaf.Op->Props.Metadata;

        if (!metadata.ShuffledByColumns.empty()) {
            auto& shuffledBy = resolvedLeafShufflings[i];
            shuffledBy.reserve(metadata.ShuffledByColumns.size());
            bool allShufflingColumnsResolved = true;
            for (const auto& col : metadata.ShuffledByColumns) {
                if (const auto it = leaf.ColumnsToCBO.find(col); it != leaf.ColumnsToCBO.end()) {
                    shuffledBy.emplace_back(it->second.GetAlias(), it->second.GetColumnName());
                } else {
                    allShufflingColumnsResolved = false;
                    break;
                }
            }
            if (allShufflingColumnsResolved && !shuffledBy.empty()) {
                fdStorage.AddShuffling(TShuffling(shuffledBy), &tableAliasMap);
            } else {
                shuffledBy.clear();
            }
        }

        if (!metadata.KeyColumns.empty()) {
            TVector<TJoinColumn> sortedBy;
            sortedBy.reserve(metadata.KeyColumns.size());
            for (const auto& col : metadata.KeyColumns) {
                if (const auto it = leaf.ColumnsToCBO.find(col); it != leaf.ColumnsToCBO.end()) {
                    sortedBy.emplace_back(it->second.GetAlias(), it->second.GetColumnName());
                }
            }
            if (!sortedBy.empty()) {
                TVector<TOrdering::TItem::EDirection> dirs(
                    sortedBy.size(), TOrdering::TItem::EDirection::EAscending);
                fdStorage.AddSorting(TSorting(sortedBy, dirs), &tableAliasMap);
            }
        }
    }

    // Build the FSM and seed each rel's LogicalOrderings from cached leaf shufflings.
    auto fsm = MakeSimpleShared<TOrderingsStateMachine>(
        std::move(fdStorage), TOrdering::EType::EShuffle);

    for (size_t i = 0; i < resolvedLeafShufflings.size(); ++i) {
        if (resolvedLeafShufflings[i].empty()) {
            continue;
        }
        auto orderingIdx = fsm->FDStorage.FindShuffling(
            TShuffling(resolvedLeafShufflings[i]), &tableAliasMap);
        if (orderingIdx != std::numeric_limits<std::size_t>::max()) {
            rels[i]->Stats.LogicalOrderings = fsm->CreateState(orderingIdx);
            rels[i]->Stats.LogicalOrderings.SetShuffleHashFuncArgsCount(resolvedLeafShufflings[i].size());
        }
    }

    // Log the orderings FSM that CBO will use for shuffle elimination.
    if (NYql::NLog::YqlLogger().NeedToLog(
            NYql::NLog::EComponent::CoreDq, NYql::NLog::ELevel::TRACE)) {
        YQL_CLOG(TRACE, CoreDq) << "\nShufflings FSM: " << fsm->ToString();
    }

    return {std::move(fsm), std::move(tableAliasMap)};
}

std::shared_ptr<TJoinOptimizerNode> ConvertJoinTree(
    TIntrusivePtr<TOpCBOTree>& cboTree,
    NYql::TTypeAnnotationContext& typeCtx,
    TVector<std::shared_ptr<TRelOptimizerNode>>& rels,
    const TVector<TCBOLeaf>& leaves)
{
    std::shared_ptr<TJoinOptimizerNode> result;

    THashMap<TCBOBoundaryEdge, std::shared_ptr<IBaseOptimizerNode>, TCBOBoundaryEdge::THashFunction> leafNodeMap;
    THashMap<IOperator*, std::shared_ptr<IBaseOptimizerNode>> nodeMap;

    // Build one CBO relation per boundary input. Each relation carries the
    // leaf-scoped column aliases and translated statistics.
    for (const auto& leaf : leaves) {
        auto stats = BuildLeafOptimizerStatistics(leaf, typeCtx);
        auto relNode = std::make_shared<NOpt::TRBORelOptimizerNode>(
            TVector<TString>{leaf.RelationName}, stats, leaf.Op);
        rels.push_back(relNode);
        leafNodeMap.insert({leaf.Edge, relNode});
    }

    auto resolveChildNode = [&nodeMap, &leafNodeMap](const TIntrusivePtr<TOpJoin>& join, ui32 childIndex) {
        const auto& child = join->Children[childIndex];
        if (const auto it = nodeMap.find(child.get()); it != nodeMap.end()) {
            return it->second;
        }
        return leafNodeMap.at(TCBOBoundaryEdge{join.get(), childIndex});
    };

    for (auto node : cboTree->TreeNodes) {
        auto join = CastOperator<TOpJoin>(node);
        auto leftNode = resolveChildNode(join, 0);
        auto rightNode = resolveChildNode(join, 1);
        TVector<TJoinColumn> leftKeys;
        TVector<TJoinColumn> rightKeys;

        for (auto [leftKey, rightKey] : join->JoinKeys) {
            leftKeys.push_back(ConvertRBOColumnToCBO(leaves, leftKey, leftNode));
            rightKeys.push_back(ConvertRBOColumnToCBO(leaves, rightKey, rightNode));
        }

        result = std::make_shared<TJoinOptimizerNode>(leftNode,
            rightNode,
            leftKeys,
            rightKeys,
            ConvertToJoinKind(join->JoinKind),
            NKikimr::NKqp::EJoinAlgoType::Undefined,
            false,
            false,
            false);

        nodeMap.insert({join.get(), result});
    }

    return result;
}

TIntrusivePtr<IOperator> ConvertOptimizedTree(
    std::shared_ptr<IBaseOptimizerNode> tree,
    const TVector<TCBOLeaf>& leaves,
    TPositionHandle pos)
{
    if (tree->Kind == RelNodeType) {
        auto rel = std::static_pointer_cast<NOpt::TRBORelOptimizerNode>(tree);
        return rel->Op;
    } else {
        auto join = std::static_pointer_cast<TJoinOptimizerNode>(tree);
        auto leftArg = ConvertOptimizedTree(join->LeftArg, leaves, pos);
        auto rightArg = ConvertOptimizedTree(join->RightArg, leaves, pos);

        Y_ENSURE(join->LeftJoinKeys.size() == join->RightJoinKeys.size());

        TVector<std::pair<TInfoUnit, TInfoUnit>> joinKeys;
        for (size_t i=0; i<join->LeftJoinKeys.size(); i++) {
            auto leftKey = ConvertCBOColumnToRBO(leaves, join->LeftJoinKeys[i]);
            auto rightKey = ConvertCBOColumnToRBO(leaves, join->RightJoinKeys[i]);
            joinKeys.push_back(std::make_pair(leftKey, rightKey));
        }

        auto joinKind = ConvertToJoinString(join->JoinType);

        auto res = MakeIntrusive<TOpJoin>(leftArg, rightArg, pos, joinKind, joinKeys);

        // JoinAlgo is optional, set it only if CBO ran and decided on an algo.
        // Otherwise MaybeSetJoinAlgo can see that it's std::nullopt and set it to the default.
        if (join->JoinAlgo != NKikimr::NKqp::EJoinAlgoType::Undefined) {
            res->Props.JoinAlgo = join->JoinAlgo;
        }

        if (join->JoinAlgo == NKikimr::NKqp::EJoinAlgoType::GraceJoin) {
            res->Props.LeftShuffleBy = ConvertCBOColumnsToRBO(leaves, join->ShuffleLeftSideBy);
            res->Props.RightShuffleBy = ConvertCBOColumnsToRBO(leaves, join->ShuffleRightSideBy);
        }
        return res;
    }
}

std::string FormatJoinTree(const char* title, const std::shared_ptr<IBaseOptimizerNode>& joinTree) {
    std::stringstream str;
    str << title << ":\n";
    joinTree->Print(str);
    return str.str();
}

} // namespace NKikimr::NKqp
