#pragma once

#include "dq_opt_join_hypergraph.h"
#include "dq_opt_conflict_rules_collector.h"

#include <library/cpp/disjoint_sets/disjoint_sets.h>
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
typename TJoinHypergraph<TNodeSet>::TEdge MakeHyperedge(
    TJoinHypergraph<TNodeSet>& graph,
    const std::shared_ptr<TJoinOptimizerNode>& joinNode,
    std::unordered_map<std::shared_ptr<IBaseOptimizerNode>, TNodeSet>& subtreeNodes,
    const TVector<TJoinColumn>& leftJoinKeys,
    const TVector<TJoinColumn>& rightJoinKeys
) {
    TVector<TString> relations = GetConditionUsedRelationNames(leftJoinKeys, rightJoinKeys);
    TNodeSet SES = graph.GetNodesByRelNames(relations);

    auto conflictRulesCollector = TConflictRulesCollector<TNodeSet>(joinNode, subtreeNodes);
    auto conflictRules = conflictRulesCollector.CollectConflicts();

    bool isLHSReorderable = Overlaps(SES, subtreeNodes[joinNode->LeftArg])  && !joinNode->LeftAny;
    bool isRHSReorderable = Overlaps(SES, subtreeNodes[joinNode->RightArg]) && !joinNode->RightAny;

    if (!isLHSReorderable || !joinNode->IsReorderable) {
        SES |= subtreeNodes[joinNode->LeftArg];
    }

    if (!isRHSReorderable || !joinNode->IsReorderable) {
        SES |= subtreeNodes[joinNode->RightArg];
    }

    TNodeSet TES = ConvertConflictRulesIntoTES(SES, conflictRules);

    TNodeSet left = TES & subtreeNodes[joinNode->LeftArg];
    TNodeSet right = TES & subtreeNodes[joinNode->RightArg];

    bool isCommutative = OperatorIsCommutative(joinNode->JoinType) && (joinNode->IsReorderable);

    typename TJoinHypergraph<TNodeSet>::TEdge edge(
        left, right,
        joinNode->JoinType,
        joinNode->LeftAny, joinNode->RightAny,
        isCommutative,
        leftJoinKeys, rightJoinKeys
    );

    return edge;
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
        graph.AddEdge(MakeHyperedge(graph, joinNode, subtreeNodes, leftJoinKeys, rightJoinKeys));
        return;
    }

    auto zip = Zip(leftJoinKeys, rightJoinKeys);

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

        graph.AddEdge(
            MakeHyperedge(graph, joinNode, subtreeNodes,
                          currentGroupLhsJoinKeys, currentGroupRhsJoinKeys));
    }
}

template<typename TNodeSet>
void MakeJoinHypergraphRec(
    TJoinHypergraph<TNodeSet>& graph,
    const std::shared_ptr<IBaseOptimizerNode>& joinTree,
    std::unordered_map<std::shared_ptr<IBaseOptimizerNode>, TNodeSet>& subtreeNodes,
    TVector<typename TJoinHypergraph<TNodeSet>::TEdge>& crossJoins
) {
    if (joinTree->Kind == RelNodeType) {
        size_t nodeId = graph.AddNode(joinTree);
        TNodeSet node{};
        node[nodeId] = 1;
        subtreeNodes[joinTree] = node;
        return;
    }

    auto joinNode = std::static_pointer_cast<TJoinOptimizerNode>(joinTree);

    MakeJoinHypergraphRec(graph, joinNode->LeftArg, subtreeNodes, crossJoins);
    MakeJoinHypergraphRec(graph, joinNode->RightArg, subtreeNodes, crossJoins);

    subtreeNodes[joinTree] = subtreeNodes[joinNode->LeftArg] | subtreeNodes[joinNode->RightArg];

    if (joinNode->JoinType == EJoinKind::Cross) {
        auto edge = MakeHyperedge(
            graph, joinNode, subtreeNodes,
            joinNode->LeftJoinKeys, joinNode->RightJoinKeys
        );

        crossJoins.emplace_back(std::move(edge));
        return;
    }

    AddHyperedges<TNodeSet>(graph, joinNode, subtreeNodes, joinNode->LeftJoinKeys, joinNode->RightJoinKeys);
}

template<typename TNodeSet>
void AddCrossJoins(TJoinHypergraph<TNodeSet>& graph,
                   TVector<typename TJoinHypergraph<TNodeSet>::TEdge>& crossJoins) {

    TDisjointSets connectedComponents(graph.GetNodes().size());

    // If all nodes in nodeSet lie in the same connected component returns
    // canonical element of that component, otherwise -1
    auto getCommonComponent = [&connectedComponents](TNodeSet nodeSet) -> i32 {
        i32 previousComponent = -1;
        for (ui32 i = 0; i < nodeSet.size(); ++ i) {
            if (nodeSet[i]) {
                i32 component = connectedComponents.CanonicSetElement(i);
                if (previousComponent != -1 && previousComponent != component) {
                    return -1;
                }

                previousComponent = component;
            }
        }

        return previousComponent;
    };

    auto applyEdgeIfEnabled = [&](const typename TJoinHypergraph<TNodeSet>::TEdge& edge) {
        // For edge to become "active", lhs nodes have to form a connected
        // hypersubgraph and rhs nodes also have to form connected hypersubgraph

        i32 componentLHS = getCommonComponent(edge.Left);
        if (componentLHS == -1) {
            return false; // lhs nodes are not connected, can't enable the edge
        }

        i32 componentRHS = getCommonComponent(edge.Right);
        if (componentRHS == -1) {
            return false; // rhs nodes are not connected, can't enable the edge
        }

        if (componentLHS == componentRHS) {
            // Enabling the edge wouldn't change anything, both sides are already
            // in the same connected component -- nothing changes
            return false;
        }

        connectedComponents.UnionSets(componentLHS, componentRHS);
        return true;
    };

    TVector<typename TJoinHypergraph<TNodeSet>::TEdge> enabledCrossJoins;

    std::vector<bool> isEdgeActive(graph.GetEdges().size(), false);
    bool hasChanged{};
    do {
        hasChanged = false;

        // Try to re-enable any of the hyperedges that might have become active
        for (ui32 i = 0; i < graph.GetEdges().size(); ++ i) {
            if (isEdgeActive[i]) {
                continue;
            }

            auto& edge = graph.GetEdge(i);
            bool wasEdgeApplied = applyEdgeIfEnabled(edge);

            // Mark edge as active not to recheck on every iteration:
            isEdgeActive[i] |= wasEdgeApplied;
            hasChanged |= wasEdgeApplied;
        }

        // Check if any of the cross joins reduce number of connected components
        for (auto& edge : crossJoins) {
            bool wasEdgeApplied = applyEdgeIfEnabled(edge);
            if (wasEdgeApplied) {
                enabledCrossJoins.push_back(edge);
            }

            hasChanged |= wasEdgeApplied;
        }
    } while (hasChanged);


    // After all missing cross joins have been added, the graph
    // has to become fully connected:
    Y_ENSURE(connectedComponents.SetCount() == 1);

    // Add all missing cross joins to the hypergraph
    for (auto& edge : enabledCrossJoins) {
        graph.AddEdge(edge);
    }
}

template <typename TNodeSet>
TJoinHypergraph<TNodeSet> MakeJoinHypergraph(
    const std::shared_ptr<IBaseOptimizerNode>& joinTree,
    const TOptimizerHints& hints = {},
    bool logGraph = true
) {
    TJoinHypergraph<TNodeSet> graph{};
    std::unordered_map<std::shared_ptr<IBaseOptimizerNode>, TNodeSet> subtreeNodes{};
    TVector<typename TJoinHypergraph<TNodeSet>::TEdge> crossJoins;
    MakeJoinHypergraphRec(graph, joinTree, subtreeNodes, crossJoins);

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

    AddCrossJoins(graph, crossJoins);

    if (logGraph && NYql::NLog::YqlLogger().NeedToLog(NYql::NLog::EComponent::CoreDq, NYql::NLog::ELevel::TRACE)) {
        YQL_CLOG(TRACE, CoreDq) << "Hypergraph after adding cross joins: ";
        YQL_CLOG(TRACE, CoreDq) << graph.String();
    }

    return graph;
}

} // namespace NYql::NDq
