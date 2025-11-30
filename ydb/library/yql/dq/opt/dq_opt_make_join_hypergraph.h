#pragma once

#include "dq_opt_join_hypergraph.h"
#include "dq_opt_conflict_rules_collector.h"

#include <yql/essentials/core/cbo/cbo_optimizer_new.h>
#include <yql/essentials/utils/log/log.h>

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

inline TVector<TString> GetConditionUsedRelationNames(const TVector<TJoinColumn>& lhs, const TVector<TJoinColumn>& rhs) {
    TVector<TString> res;
    res.reserve(lhs.size());

    for (const auto& [lhsTable, rhsTable]: Zip(lhs, rhs)) {
        res.push_back(lhsTable.RelName);
        res.push_back(rhsTable.RelName);
    }

    return res;
}

template <typename TNodeSet>
void AddHyperedge(
    TJoinHypergraph<TNodeSet>& graph,
    const std::shared_ptr<TJoinOptimizerNode>& joinNode,
    std::unordered_map<std::shared_ptr<IBaseOptimizerNode>, TNodeSet>& subtreeNodes,
    const TVector<TJoinColumn>& leftJoinKeys,
    const TVector<TJoinColumn>& rightJoinKeys
) {
    if (joinNode->JoinType == EJoinKind::Cross) {
        return;
    }

    TNodeSet conditionUsedRels = graph.GetNodesByRelNames(GetConditionUsedRelationNames(leftJoinKeys, rightJoinKeys));

    auto conflictRulesCollector = TConflictRulesCollector<TNodeSet>(joinNode, subtreeNodes);
    auto conflictRules = conflictRulesCollector.CollectConflicts();

    TNodeSet TES = ConvertConflictRulesIntoTES(conditionUsedRels, conflictRules);

    /* For CROSS, Non-Reorderable, ANY Joins and degenerate predicates (if subtree tables and joinCondition tables do not intersect) */
    if (!Overlaps(TES, subtreeNodes[joinNode->LeftArg]) || !joinNode->IsReorderable || joinNode->LeftAny) {
        TES |= subtreeNodes[joinNode->LeftArg];
        TES = ConvertConflictRulesIntoTES(TES, conflictRules);
    }

    if (!Overlaps(TES, subtreeNodes[joinNode->RightArg]) || !joinNode->IsReorderable || joinNode->RightAny) {
        TES |= subtreeNodes[joinNode->RightArg];
        TES = ConvertConflictRulesIntoTES(TES, conflictRules);
    }

    TNodeSet left = TES & subtreeNodes[joinNode->LeftArg];
    TNodeSet right = TES & subtreeNodes[joinNode->RightArg];

    bool isCommutative = OperatorIsCommutative(joinNode->JoinType) && (joinNode->IsReorderable);

    typename TJoinHypergraph<TNodeSet>::TEdge edge(left, right, joinNode->JoinType, joinNode->LeftAny, joinNode->RightAny, isCommutative, leftJoinKeys, rightJoinKeys);
    graph.AddEdge(edge);
}

template <typename TNodeSet>
void AddHyperedges(
    TJoinHypergraph<TNodeSet>& graph,
    const std::shared_ptr<TJoinOptimizerNode>& joinNode,
    std::unordered_map<std::shared_ptr<IBaseOptimizerNode>, TNodeSet>& subtreeNodes,
    const TVector<TJoinColumn>& leftJoinKeys,
    const TVector<TJoinColumn>& rightJoinKeys
) {
    if (joinNode->JoinType != EJoinKind::InnerJoin || leftJoinKeys.size() <= 1) {
        AddHyperedge(graph, joinNode, subtreeNodes, leftJoinKeys, rightJoinKeys);
        return;
    }

    auto zip = Zip(joinNode->LeftJoinKeys, joinNode->RightJoinKeys);

    using TJoinCondition = std::pair<TJoinColumn, TJoinColumn>;
    std::vector<TJoinCondition> joinConditions{zip.begin(), zip.end()};

    std::sort(joinConditions.begin(), joinConditions.end());

    auto isOneGroup = [](const TJoinCondition& lhs, const TJoinCondition& rhs) -> bool {
        return lhs.first.RelName == rhs.first.RelName
            && lhs.second.RelName == rhs.second.RelName;
    };

    for (ui32 i = 0; i < joinConditions.size(); ) {
        TVector<TJoinColumn> currentGroupLhsJoinKeys, currentGroupRhsJoinKeys;

        ui32 groupBegin = i;
        while (i < joinConditions.size() &&
               isOneGroup(joinConditions[groupBegin], joinConditions[i])) {

            const auto &[lhs, rhs] = joinConditions[i];

            currentGroupLhsJoinKeys.push_back(lhs);
            currentGroupRhsJoinKeys.push_back(rhs);
            ++ i;
        }

        AddHyperedge(graph, joinNode, subtreeNodes,
                     currentGroupLhsJoinKeys, currentGroupRhsJoinKeys);
    }
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
    AddHyperedges<TNodeSet>(graph, joinNode, subtreeNodes, joinNode->LeftJoinKeys, joinNode->RightJoinKeys);
}

template <typename TNodeSet>
TJoinHypergraph<TNodeSet> MakeJoinHypergraph(
    const std::shared_ptr<IBaseOptimizerNode>& joinTree,
    const TOptimizerHints& hints = {},
    bool logGraph = true
) {
    TJoinHypergraph<TNodeSet> graph{};
    std::unordered_map<std::shared_ptr<IBaseOptimizerNode>, TNodeSet> subtreeNodes{};
    MakeJoinHypergraphRec(graph, joinTree, subtreeNodes);

    if (logGraph && NYql::NLog::YqlLogger().NeedToLog(NYql::NLog::EComponent::CoreDq, NYql::NLog::ELevel::TRACE)) {
        YQL_CLOG(TRACE, CoreDq) << "Hypergraph build: ";
        YQL_CLOG(TRACE, CoreDq) << graph.String();
    }

    if (!hints.JoinOrderHints->Hints.empty()) {
        TJoinOrderHintsApplier joinHints(graph);
        joinHints.Apply(*hints.JoinOrderHints);
        if (logGraph && NYql::NLog::YqlLogger().NeedToLog(NYql::NLog::EComponent::CoreDq, NYql::NLog::ELevel::TRACE)) {
            YQL_CLOG(TRACE, CoreDq) << "Hypergraph after hints: ";
            YQL_CLOG(TRACE, CoreDq) << graph.String();
        }
    }

    TTransitiveClosureConstructor transitveClosure(graph);
    transitveClosure.Construct();

    if (logGraph && NYql::NLog::YqlLogger().NeedToLog(NYql::NLog::EComponent::CoreDq, NYql::NLog::ELevel::TRACE)) {
        YQL_CLOG(TRACE, CoreDq) << "Hypergraph after transitive closure: ";
        YQL_CLOG(TRACE, CoreDq) << graph.String();
    }

    return graph;
}

} // namespace NYql::NDq
