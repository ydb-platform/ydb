#pragma once

#include "dphyp_join_hypergraph.h"
#include "dphyp_conflict_rules_collector.h"

#include <ydb/library/yql/core/cbo/cbo_optimizer_new.h>

#include <memory.h>

namespace NYql::NDq {

template <typename TNodeSet>
TJoinHypergraph<TNodeSet>::TEdge MakeHyperedge(
    const std::shared_ptr<TJoinOptimizerNode>& joinNode,
    TNodeSet conditionUsedRels,
    THashMap<std::shared_ptr<TJoinOptimizerNode>, TNodeSet>& subtreeNodes
) {
    auto conflictRulesCollector = TConflictRulesCollector<TNodeSet>(joinNode, subtreeNodes);
    auto conflictRules = conflictRulesCollector.CollectConflicts();

    TNodeSet TES = ConvertConflictRulesIntoTES(conditionUsedRels, std::move(conflictRules));

    TNodeSet left = TES & subtreeNodes[joinNode->LeftArg];
    TNodeSet right = TES & subtreeNodes[joinNode->RightArg];
    
    return TJoinHypergraph<TNodeSet>::TEdge(left, right, joinNode->JoinType, OperatorIsCommut(joinNode->JoinType), joinNode->JoinConditions);
}

template<typename TNodeSet>
void MakeJoinHypergraphRec(
    TJoinHypergraph<TNodeSet>& graph,
    const std::shared_ptr<IBaseOptimizerNode>& joinTree,
    THashMap<std::shared_ptr<TJoinOptimizerNode>, TNodeSet>& subtreeNodes
) {
    if (joinTree->Kind == RelNodeType) {
        TNodeSet node = graph.AddNode(joinTree);
        subtreeNodes[joinTree] = node;
        return;
    }

    auto joinNode = std::static_pointer_cast<TJoinOptimizerNode>(joinTree);
    MakeJoinHypergraphRec(graph, joinNode->LeftArg, subtreeNodes);
    MakeJoinHypergraphRec(graph, joinNode->RightArg, subtreeNodes);

    subtreeNodes[joinNode] = subtreeNodes[joinNode->LeftArg] | subtreeNodes[joinNode->RightArg];

    TNodeSet conditionUsedRels = graph.GetNodesByRelNames(joinNode->Labels());
    graph.AddEdge(MakeHyperedge<TNodeSet>(joinNode, conditionUsedRels, subtreeNodes));
}

template <typename TNodeSet>
TJoinHypergraph<TNodeSet> MakeHypergraph(
    const std::shared_ptr<IBaseOptimizerNode>& joinTree
) {
    TJoinHypergraph<TNodeSet>& graph{};
    THashMap<std::shared_ptr<TJoinOptimizerNode>, TNodeSet>& subtreeNodes{};
    MakeJoinHypergraphRec(graph, joinTree, subtreeNodes);
}

} // namespace NYql::NDq
