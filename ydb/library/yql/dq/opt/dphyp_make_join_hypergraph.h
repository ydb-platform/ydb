#pragma once

#include "dphyp_join_hypergraph.h"
#include "dphyp_conflict_rules_collector.h"

#include <ydb/library/yql/core/cbo/cbo_optimizer_new.h>

#include <memory.h>

namespace NYql::NDq::NDphyp {

inline TVector<TString> GetConditionUsedRelationNames(const std::shared_ptr<TJoinOptimizerNode>& joinNode) {
    TVector<TString> res;
    res.reserve(joinNode->JoinConditions.size());

    for (const auto& [lhsTable, rhsTable]: joinNode->JoinConditions) {
        res.push_back(lhsTable.RelName);
        res.push_back(rhsTable.RelName);
    }

    return res;
}

template <typename TNodeSet>
TJoinHypergraph<TNodeSet>::TEdge MakeHyperedge(
    const std::shared_ptr<TJoinOptimizerNode>& joinNode,
    TNodeSet conditionUsedRels,
    std::unordered_map<std::shared_ptr<IBaseOptimizerNode>, TNodeSet>& subtreeNodes
) {
    auto conflictRulesCollector = TConflictRulesCollector<TNodeSet>(joinNode, subtreeNodes);
    auto conflictRules = conflictRulesCollector.CollectConflicts();

    TNodeSet TES = ConvertConflictRulesIntoTES(conditionUsedRels, std::move(conflictRules));

    TNodeSet left = TES & subtreeNodes[joinNode->LeftArg];
    TNodeSet right = TES & subtreeNodes[joinNode->RightArg];
    
    return typename TJoinHypergraph<TNodeSet>::TEdge(left, right, joinNode->JoinType, OperatorIsCommut(joinNode->JoinType), joinNode->JoinConditions);
}

template<typename TNodeSet>
void MakeJoinHypergraphRec(
    TJoinHypergraph<TNodeSet>& graph,
    const std::shared_ptr<IBaseOptimizerNode>& joinTree,
    std::unordered_map<std::shared_ptr<IBaseOptimizerNode>, TNodeSet>& subtreeNodes
) {
    if (joinTree->Kind == RelNodeType) {
        size_t nodeId = graph.AddNode(joinTree);
        TNodeSet node{};
        node[nodeId] = 1;
        subtreeNodes[joinTree] = node;
        return;
    }

    auto joinNode = std::static_pointer_cast<TJoinOptimizerNode>(joinTree);
    MakeJoinHypergraphRec(graph, joinNode->LeftArg, subtreeNodes);
    MakeJoinHypergraphRec(graph, joinNode->RightArg, subtreeNodes);

    subtreeNodes[joinTree] = subtreeNodes[joinNode->LeftArg] | subtreeNodes[joinNode->RightArg];

    TNodeSet conditionUsedRels{};
    if (joinNode->JoinType == EJoinKind::Cross) {
        conditionUsedRels = graph.GetNodesByRelNames(joinTree->Labels());
    } else {
        conditionUsedRels = graph.GetNodesByRelNames(GetConditionUsedRelationNames(joinNode));
    }

    graph.AddEdge(MakeHyperedge<TNodeSet>(joinNode, conditionUsedRels, subtreeNodes));
}

template <typename TNodeSet>
TJoinHypergraph<TNodeSet> MakeJoinHypergraph(
    const std::shared_ptr<IBaseOptimizerNode>& joinTree
) {
    TJoinHypergraph<TNodeSet> graph{};
    std::unordered_map<std::shared_ptr<IBaseOptimizerNode>, TNodeSet> subtreeNodes{};
    MakeJoinHypergraphRec(graph, joinTree, subtreeNodes);
    return graph;
}

} // namespace NYql::NDq
