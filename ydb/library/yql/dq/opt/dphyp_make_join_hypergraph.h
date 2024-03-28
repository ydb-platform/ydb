#pragma once

#include "dphyp_join_hypergraph.h"
#include "dphyp_conflict_rules_collector.h"

#include <ydb/library/yql/core/cbo/cbo_optimizer_new.h>

#include <memory.h>

namespace NYql::NDq::NDphyp {

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
        TNodeSet node = graph.AddNode(joinTree);
        subtreeNodes[joinTree] = node;
        return;
    }

    auto joinNode = std::static_pointer_cast<TJoinOptimizerNode>(joinTree);
    MakeJoinHypergraphRec(graph, joinNode->LeftArg, subtreeNodes);
    MakeJoinHypergraphRec(graph, joinNode->RightArg, subtreeNodes);

    subtreeNodes[joinTree] = subtreeNodes[joinNode->LeftArg] | subtreeNodes[joinNode->RightArg];

    TNodeSet conditionUsedRels = graph.GetNodesByRelNames(joinNode->Labels());
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
