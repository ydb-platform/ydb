#pragma once

#include "kqp_opt_join_hypergraph.h"

#include <unordered_map>

namespace NKikimr::NKqp {

namespace NJoinConditionCompletion {

// Completion only adds predicates, so each join's input relations stay fixed.
template <typename TNodeSet>
using TJoinInputs = std::unordered_map<const TJoinOptimizerNode*, std::pair<TNodeSet, TNodeSet>>;

template <typename TNodeSet>
TNodeSet CollectJoinInputs(TJoinHypergraph<TNodeSet>& graph, const std::shared_ptr<IBaseOptimizerNode>& node, TJoinInputs<TNodeSet>& inputs) {
    if (node->Kind != EOptimizerNodeKind::JoinNodeType) {
        return graph.GetNodesByRelNames(node->Labels());
    }
    auto* join = static_cast<TJoinOptimizerNode*>(node.get());
    const auto lhs = CollectJoinInputs(graph, join->LeftArg, inputs);
    const auto rhs = CollectJoinInputs(graph, join->RightArg, inputs);
    inputs.emplace(join, std::make_pair(lhs, rhs));
    return lhs | rhs;
}

inline bool ContainsCondition(const auto& conditions, const TJoinColumn& left, const TJoinColumn& right) {
    for (const auto& [actualLeft, actualRight] : conditions) {
        if (actualLeft == left && actualRight == right &&
            IsEqualNullsKey(actualLeft, actualRight) == IsEqualNullsKey(left, right)) {
            return true;
        }
    }
    return false;
}

inline void AddJoinKeys(TJoinOptimizerNode& join, const TVector<TJoinColumn>& leftKeys, const TVector<TJoinColumn>& rightKeys) {
    for (const auto& [left, right] : Zip(leftKeys, rightKeys)) {
        if (!ContainsCondition(Zip(join.LeftJoinKeys, join.RightJoinKeys), left, right)) {
            join.LeftJoinKeys.push_back(left);
            join.RightJoinKeys.push_back(right);
        }
    }
}

template <typename TNodeSet>
bool AddEdgeConditions(const TJoinInputs<TNodeSet>& inputs, IBaseOptimizerNode* node, const typename TJoinHypergraph<TNodeSet>::TEdge& edge) {
    const auto required = edge.Left | edge.Right;
    while (node->Kind == EOptimizerNodeKind::JoinNodeType) {
        auto& join = *static_cast<TJoinOptimizerNode*>(node);
        const auto& [lhs, rhs] = inputs.at(&join);
        if (IsSubset(required, lhs)) {
            node = join.LeftArg.get();
            continue;
        }
        if (IsSubset(required, rhs)) {
            node = join.RightArg.get();
            continue;
        }
        if (!IsSubset(required, lhs | rhs)) {
            return false;
        }

        if (edge.JoinKind == join.JoinType) {
            if (edge.Bridges(lhs, rhs) && edge.LeftAny == join.LeftAny && edge.RightAny == join.RightAny) {
                AddJoinKeys(join, edge.LeftJoinKeys, edge.RightJoinKeys);
                return true;
            }
            if (edge.IsCommutative && edge.Bridges(rhs, lhs) && edge.LeftAny == join.RightAny && edge.RightAny == join.LeftAny) {
                AddJoinKeys(join, edge.RightJoinKeys, edge.LeftJoinKeys);
                return true;
            }
        }
        // A displaced INNER equality can filter this output once its eligibility set is present.
        if (edge.JoinKind == EJoinKind::InnerJoin && edge.IsCommutative && !edge.LeftJoinKeys.empty() &&
            !edge.LeftAny && !edge.RightAny && !join.LeftAny && !join.RightAny) {
            for (const auto& [left, right] : Zip(edge.LeftJoinKeys, edge.RightJoinKeys)) {
                if (!ContainsCondition(join.PostJoinConditions, left, right)) {
                    join.PostJoinConditions.emplace_back(left, right);
                }
            }
            return true;
        }
        return false;
    }
    return false;
}

} // namespace NJoinConditionCompletion

// On success every graph predicate is present at its placement as a join key or a filter.
template <typename TNodeSet>
void AddMissingConditions(TJoinHypergraph<TNodeSet>& graph, const std::shared_ptr<IBaseOptimizerNode>& result) {
    using namespace NJoinConditionCompletion;
    TJoinInputs<TNodeSet> inputs;
    inputs.reserve(graph.GetNodes().size());
    CollectJoinInputs(graph, result, inputs);
    for (const auto& edge : graph.GetEdges()) {
        if (edge.IsReversed) {
            continue;
        }
        // Enumeration can eliminate a cartesian product when other joins connect its inputs.
        if (edge.JoinKind == EJoinKind::Cross && edge.LeftJoinKeys.empty() &&
            edge.IsCommutative && !edge.LeftAny && !edge.RightAny) {
            continue;
        }
        Y_ENSURE(AddEdgeConditions(inputs, result.get(), edge), "Cannot place join conditions in the selected tree");
    }
}

} // namespace NKikimr::NKqp
