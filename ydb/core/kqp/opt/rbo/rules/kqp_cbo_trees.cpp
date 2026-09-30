#include "kqp_cbo_trees.h"

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/opt/cbo/solver/kqp_opt_make_join_hypergraph.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo_cbo.h>

#include <library/cpp/iterator/zip.h>

#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/core/yql_type_annotation.h>

#include <util/generic/hash_set.h>
#include <util/generic/strbuf.h>
#include <util/string/builder.h>

#include <bitset>
#include <limits>
#include <sstream>

namespace NKikimr::NKqp {

namespace {

TOpRead* FindReadThroughMapFilter(IOperator* op) {
    if (op->Kind == EOperator::Source) {
        return CastOperator<TOpRead>(op);
    }

    if (op->Kind == EOperator::Map) {
        return FindReadThroughMapFilter(CastOperator<TOpMap>(op)->GetInput().Get());
    }

    if (op->Kind == EOperator::Filter) {
        return FindReadThroughMapFilter(CastOperator<TOpFilter>(op)->GetInput().Get());
    }
    if (op->Kind == EOperator::Replicate) {
        return FindReadThroughMapFilter(CastOperator<TOpReplicate>(op)->GetReplicate().GetInput().Get());
    }

    return {};
}

TString GetReadTableName(TOpRead* read) {
    if (!read || !read->TableCallable) {
        return {};
    }

    return NYql::NNodes::TKqpTable(read->TableCallable).Path().StringValue();
}

TString GetReadRelationName(TOpRead* read) {
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
// _kqp_rbo_cbo_leaf_N names. Columns are the leaf's output IDs.
TCBOLeaf BuildCBOLeaf(
    TIntrusivePtr<IOperator> op,
    TCBOBoundaryEdge edge,
    THashSet<TString>& usedRelationNames,
    ui32& syntheticRelationId)
{
    TCBOLeaf leaf = {
        .Op = op,
        .Edge = edge,
    };

    if (auto read = FindReadThroughMapFilter(op.Get())) {
        const auto relationName = GetReadRelationName(read);
        leaf.RelationName = relationName.empty()
            ? MakeSyntheticRelationName(syntheticRelationId, usedRelationNames)
            : MakeUniqueName(relationName, usedRelationNames);
        leaf.SourceTableName = GetReadTableName(read);
    } else {
        leaf.RelationName = MakeSyntheticRelationName(syntheticRelationId, usedRelationNames);
    }
    return leaf;
}

TVector<TString> BuildTranslatedKeyColumns(const TCBOLeaf& leaf) {
    TVector<TString> keyColumns;
    if (!leaf.Op->Props.Metadata) {
        return keyColumns;
    }

    const auto& outputs = leaf.Op->GetOutputIUs();
    for (const auto key : leaf.Op->Props.Metadata->KeyColumns.Items()) {
        if (outputs.Contains(key)) {
            keyColumns.push_back(ToString(key));
        }
    }
    return keyColumns;
}

TIntrusivePtr<TOptimizerStatistics::TColumnStatMap> BuildTranslatedColumnStatistics(
    const TCBOLeaf& leaf,
    const TColumnLineage& lineage,
    NYql::TTypeAnnotationContext& typeCtx)
{
    if (!leaf.Op->Props.Metadata) {
        return {};
    }

    auto result = MakeIntrusive<TOptimizerStatistics::TColumnStatMap>();

    THashMap<TString, THashMap<TString, TString>> cboColumnByTableColumn;

    for (const auto column : leaf.Op->GetOutputIUs()) {
        const auto* source = FindSourceStatistics(*leaf.Op, column, lineage);
        if (!source || source->TableName.empty()) {
            continue;
        }

        cboColumnByTableColumn[source->TableName][source->ColumnName] = ToString(column);

        const auto tableStatsIt = typeCtx.ColumnStatisticsByTableName.find(source->TableName);
        if (tableStatsIt == typeCtx.ColumnStatisticsByTableName.end()) {
            continue;
        }

        const auto columnStatsIt = tableStatsIt->second->Data.find(source->ColumnName);
        if (columnStatsIt == tableStatsIt->second->Data.end()) {
            continue;
        }

        result->Data[ToString(column)] = NKqp::TColumnStatistics(columnStatsIt->second);
    }

    for (const auto& [tableName, cboColumnByColumn] : cboColumnByTableColumn) {
        const auto tableStatsIt = typeCtx.ColumnStatisticsByTableName.find(tableName);
        if (tableStatsIt == typeCtx.ColumnStatisticsByTableName.end()) {
            continue;
        }

        for (const auto& [_, multiColumnStats] : tableStatsIt->second->MultiData) {
            TVector<TString> translatedColumns;
            for (const auto& column : multiColumnStats.Columns) {
                const auto it = cboColumnByColumn.find(column);
                if (it == cboColumnByColumn.end()) {
                    translatedColumns.clear();
                    break;
                }
                translatedColumns.push_back(it->second);
            }

            if (translatedColumns.empty()) {
                continue;
            }

            NKqp::TMultiColumnStatistics translated(multiColumnStats);
            translated.Columns = translatedColumns;
            result->MultiData[MakeMultiColumnKey(translatedColumns)] = std::move(translated);
        }
    }

    if (result->Data.empty() && result->MultiData.empty()) {
        return {};
    }
    return result;
}

TOptimizerStatistics BuildLeafOptimizerStatistics(const TCBOLeaf& leaf, const TColumnLineage& lineage, NYql::TTypeAnnotationContext& typeCtx) {
    auto stats = BuildOptimizerStatistics(*leaf.Op, lineage, true, typeCtx);
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

    stats.ColumnStatistics = BuildTranslatedColumnStatistics(leaf, lineage, typeCtx);
    return stats;
}

TOrderedIUs<> ConvertCBOColumnsToRBO(const TVector<TJoinColumn>& columns) {
    TOrderedIUs<> result;
    result.Reserve(columns.size());
    for (const auto& column : columns) {
        result.Append(GetCBOColumnId(column));
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
        for (ui32 childIndex = 0; childIndex < node->GetChildCount(); ++childIndex) {
            auto child = node->GetChild(childIndex);
            if (treeNodeSet.contains(child.Get())) {
                continue;
            }

            leaves.push_back(BuildCBOLeaf(
                child,
                TCBOBoundaryEdge{node.Get(), childIndex},
                usedRelationNames,
                syntheticRelationId));
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
            if (IsEqualNullsKey(lhs, rhs)) {
                continue;
            }
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

        if (!metadata.ShuffledByColumns.Items().empty()) {
            auto& shuffledBy = resolvedLeafShufflings[i];
            shuffledBy.reserve(metadata.ShuffledByColumns.Items().size());
            bool allShufflingColumnsResolved = true;
            for (const auto col : metadata.ShuffledByColumns.Items()) {
                if (leaf.Op->GetOutputIUs().Contains(col)) {
                    shuffledBy.push_back(MakeCBOColumn(leaf.RelationName, col));
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

        if (!metadata.KeyColumns.Items().empty()) {
            TVector<TJoinColumn> sortedBy;
            sortedBy.reserve(metadata.KeyColumns.Items().size());
            for (const auto col : metadata.KeyColumns.Items()) {
                if (leaf.Op->GetOutputIUs().Contains(col)) {
                    sortedBy.push_back(MakeCBOColumn(leaf.RelationName, col));
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
    const TColumnLineage& lineage,
    TVector<std::shared_ptr<TRelOptimizerNode>>& rels,
    const TVector<TCBOLeaf>& leaves)
{
    std::shared_ptr<TJoinOptimizerNode> result;

    THashMap<TCBOBoundaryEdge, std::shared_ptr<IBaseOptimizerNode>, TCBOBoundaryEdge::THashFunction> leafNodeMap;
    THashMap<IOperator*, std::shared_ptr<IBaseOptimizerNode>> nodeMap;

    // Build one CBO relation per boundary input. Leaf outputs are disjoint,
    // so every ID names a column of exactly one relation.
    TMappedIUs<ui32> leafOf;
    for (ui32 index = 0; index < leaves.size(); ++index) {
        const auto& leaf = leaves[index];
        for (const auto id : leaf.Op->GetOutputIUs()) {
            leafOf.Add(id, index);
        }
        auto stats = BuildLeafOptimizerStatistics(leaf, lineage, typeCtx);
        auto relNode = std::make_shared<NOpt::TRBORelOptimizerNode>(
            TVector<TString>{leaf.RelationName}, stats, leaf.Op);
        rels.push_back(relNode);
        leafNodeMap.insert({leaf.Edge, relNode});
    }
    const auto toCBO = [&](TInfoUnitId id) {
        return MakeCBOColumn(leaves[leafOf.At(id)].RelationName, id);
    };

    auto resolveChildNode = [&nodeMap, &leafNodeMap](TOpJoin* join, ui32 childIndex) {
        auto* child = join->GetChild(childIndex).Get();
        if (const auto it = nodeMap.find(child); it != nodeMap.end()) {
            return it->second;
        }
        return leafNodeMap.at(TCBOBoundaryEdge{join, childIndex});
    };

    for (auto node : cboTree->TreeNodes) {
        auto join = CastOperator<TOpJoin>(node);
        auto leftNode = resolveChildNode(join.Get(), 0);
        auto rightNode = resolveChildNode(join.Get(), 1);
        TVector<TJoinColumn> leftKeys;
        TVector<TJoinColumn> rightKeys;

        for (const auto& [leftKey, rightKey, equalNulls] : join->JoinKeys.Items()) {
            leftKeys.push_back(toCBO(leftKey));
            rightKeys.push_back(toCBO(rightKey));
            leftKeys.back().EqualNulls = equalNulls;
            rightKeys.back().EqualNulls = equalNulls;
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

        nodeMap.insert({join.Get(), result});
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

        TJoinIUs joinKeys;
        for (size_t i=0; i<join->LeftJoinKeys.size(); i++) {
            auto leftKey = GetCBOColumnId(join->LeftJoinKeys[i]);
            auto rightKey = GetCBOColumnId(join->RightJoinKeys[i]);
            Y_ENSURE(join->LeftJoinKeys[i].EqualNulls == join->RightJoinKeys[i].EqualNulls, "Join keys have different IS NOT DISTINCT FROM semantics.");
            joinKeys.Add({leftKey, rightKey, join->LeftJoinKeys[i].EqualNulls});
        }

        auto joinKind = ConvertToJoinString(join->JoinType);

        auto res = MakeIntrusive<TOpJoin>(leftArg, rightArg, pos, joinKind, joinKeys);

        // JoinAlgo is optional, set it only if CBO ran and decided on an algo.
        // Otherwise MaybeSetJoinAlgo can see that it's std::nullopt and set it to the default.
        if (join->JoinAlgo != NKikimr::NKqp::EJoinAlgoType::Undefined) {
            res->Props.JoinAlgo = join->JoinAlgo;
        }

        if (join->JoinAlgo == NKikimr::NKqp::EJoinAlgoType::GraceJoin) {
            res->Props.LeftShuffleBy = ConvertCBOColumnsToRBO(join->ShuffleLeftSideBy);
            res->Props.RightShuffleBy = ConvertCBOColumnsToRBO(join->ShuffleRightSideBy);
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
