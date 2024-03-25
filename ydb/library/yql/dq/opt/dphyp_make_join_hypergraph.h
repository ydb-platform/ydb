#include "dphyp_join_hypergraph.h"
#include "dphyp_join_tree_node.h"
#include "dphyp_conflict_rules_collector.h"

#include <ydb/library/yql/core/cbo/cbo_optimizer_new.h>

#include <memory.h>

namespace NYql::NDq {

template <typename TNodeSet>
TJoinHypergraph<TNodeSet>::TEdge FindHyperedge(
    const std::shared_ptr<TJoinOptimizerNode>& joinNode,
    THashMap<std::shared_ptr<TJoinOptimizerNode>, TNodeSet>& subtreeNodes
) {
    auto conflictRulesCollector = TConflictRulesCollector<TNodeSet>(joinNode, subtreeNodes);
    auto conflictRules = conflictRulesCollector.CollectConflicts();
    TNodeSet TES = ConvertConflictRulesIntoTES(, std::move(conflictRules));

    TNodeSet left = TES & subtreeNodes[joinNode->LeftArg];
    TNodeSet right = TES & subtreeNodes[joinNode->RightArg];
    
    return TJoinHypergraph<TNodeSet>::TEdge(left, right);
}

template<typename TNodeSet>
void MakeJoinHypergraph(
    TJoinHypergraph<TNodeSet>& graph,
    const std::shared_ptr<IBaseOptimizerNode>& joinTree,
    THashMap<std::shared_ptr<TJoinOptimizerNode>, TNodeSet>& subtreeNodes
) {
    if (joinTree->Kind == RelNodeType) {
        auto relNode = std::static_pointer_cast<TRelOptimizerNode>(joinTree);
        graph.AddNode(relNode->Label);
        return;
    }

    auto joinNode = std::static_pointer_cast<TJoinOptimizerNode>(joinTree);
    MakeJoinHypergraph(graph, joinNode->LeftArg, subtreeNodes);
    MakeJoinHypergraph(graph, joinNode->RightArg, subtreeNodes);

    graph.AddEdge(FindHyperedge<TNodeSet>(joinNode, subtreeNodes));
}

} // namespace NYql::NDq
