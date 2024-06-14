#pragma once

#include "dq_opt_join_hypergraph.h"
#include "dq_opt_conflict_rules_collector.h"

#include <ydb/library/yql/core/cbo/cbo_optimizer_new.h>
#include <ydb/library/yql/utils/log/log.h>

#include <memory.h>

/* 
 * This header contains MakeJoinHypergraph function to construct the hypergraph from inner optimizer nodes.
 * Pipeline works as follows:
 *      1) MakeJoinHypergraph calls MakeJoinHypergraphRec recursively.
 *      2) MakeJoinHypergraphRec calls MakeHyperedge for each join node.
 *      3) MakeHyperedge finds conflicts with TConflictRulesCollector and collect them into TES.
 * If join has conflicts or complex predicate, then MakeHyperedge will create a complex edge.
 */

namespace NYql::NDq {

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
typename TJoinHypergraph<TNodeSet>::TEdge MakeHyperedge(
    const std::shared_ptr<TJoinOptimizerNode>& joinNode,
    const TNodeSet& conditionUsedRels,
    std::unordered_map<std::shared_ptr<IBaseOptimizerNode>, TNodeSet>& subtreeNodes
) {
    auto conflictRulesCollector = TConflictRulesCollector<TNodeSet>(joinNode, subtreeNodes);
    auto conflictRules = conflictRulesCollector.CollectConflicts();

    TNodeSet TES = ConvertConflictRulesIntoTES(conditionUsedRels, conflictRules);


    /* For CROSS Join and degenerate predicates (if subtree tables and joinCondition tables do not intersect) */
    if (!Overlaps(TES, subtreeNodes[joinNode->LeftArg])) {
        TES |= subtreeNodes[joinNode->LeftArg];
        TES = ConvertConflictRulesIntoTES(TES, conflictRules);
    }

    if (!Overlaps(TES, subtreeNodes[joinNode->RightArg])) {
        TES |= subtreeNodes[joinNode->RightArg];
        TES = ConvertConflictRulesIntoTES(TES, conflictRules);
    }

    TNodeSet left = TES & subtreeNodes[joinNode->LeftArg];
    TNodeSet right = TES & subtreeNodes[joinNode->RightArg];
    
    return typename TJoinHypergraph<TNodeSet>::TEdge(left, right, joinNode->JoinType, OperatorIsCommutative(joinNode->JoinType) && joinNode->IsReorderable, joinNode->JoinConditions);
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
    conditionUsedRels = graph.GetNodesByRelNames(GetConditionUsedRelationNames(joinNode));

    graph.AddEdge(MakeHyperedge<TNodeSet>(joinNode, conditionUsedRels, subtreeNodes));
}

template <typename TNodeSet>
TJoinHypergraph<TNodeSet> MakeJoinHypergraph(
    const std::shared_ptr<IBaseOptimizerNode>& joinTree
) {
    TJoinHypergraph<TNodeSet> graph{};
    std::unordered_map<std::shared_ptr<IBaseOptimizerNode>, TNodeSet> subtreeNodes{};
    MakeJoinHypergraphRec(graph, joinTree, subtreeNodes);

    if (NYql::NLog::YqlLogger().NeedToLog(NYql::NLog::EComponent::CoreDq, NYql::NLog::ELevel::TRACE)) {
        YQL_CLOG(TRACE, CoreDq) << "Hypergraph build: ";
        YQL_CLOG(TRACE, CoreDq) << graph.String();
    }

    TTransitiveClosureConstructor transitveClosure(graph);
    transitveClosure.Construct();

    if (NYql::NLog::YqlLogger().NeedToLog(NYql::NLog::EComponent::CoreDq, NYql::NLog::ELevel::TRACE)) {
        YQL_CLOG(TRACE, CoreDq) << "Hypergraph after transitive closure: ";
        YQL_CLOG(TRACE, CoreDq) << graph.String();
    }

    return graph;
}

} // namespace NYql::NDq
