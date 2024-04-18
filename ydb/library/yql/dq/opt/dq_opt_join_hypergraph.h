#pragma once

#include <vector>
#include <util/string/printf.h>
#include "bitset.h"

#include <ydb/library/yql/core/cbo/cbo_optimizer_new.h> 
#include <ydb/library/yql/core/yql_cost_function.h>

namespace NYql::NDq {

/* 
 * JoinHypergraph - a graph, whose edge connects two sets of nodes.
 * It represents relation between tables and ordering constraints.
 * Graph is undirected, so it stores each edge twice (original and reversed) for DPHyp algorithm.
 */
template <typename TNodeSet>
class TJoinHypergraph {
public:
    struct TEdge {
        TEdge(
            const TNodeSet& left,
            const TNodeSet& right,
            EJoinKind joinKind,
            bool isCommutative,
            const std::set<std::pair<TJoinColumn, TJoinColumn>>& joinConditions
        )
            : Left(left)
            , Right(right)
            , JoinKind(joinKind)
            , IsCommutative(isCommutative)
            , JoinConditions(joinConditions)
            , IsReversed(false)
        {
            BuildCondVectors();
        }

        inline bool IsSimpleEdge() {
            return HasSingleBit(Left) && HasSingleBit(Right);
        }

        TNodeSet Left;
        TNodeSet Right;
        EJoinKind JoinKind;
        bool IsCommutative;
        std::set<std::pair<TJoinColumn, TJoinColumn>> JoinConditions;
        TVector<TString> LeftJoinKeys;
        TVector<TString> RightJoinKeys;

        // JoinKind may not be commutative, so we need to know which edge is original and which is reversed.
        bool IsReversed;
        int64_t ReversedEdgeId = -1;

        void BuildCondVectors() {
            LeftJoinKeys.clear();
            RightJoinKeys.clear();

            for (auto [left, right] : JoinConditions) {
                auto leftKey = left.AttributeName;
                auto rightKey = right.AttributeName;

                for (size_t i = leftKey.size() - 1; i > 0; --i) {
                    if (leftKey[i] == '.') {
                        leftKey = leftKey.substr(i + 1);
                        break;
                    }
                }

                for (size_t i = rightKey.size() - 1; i > 0; --i) {
                    if (rightKey[i] == '.') {
                        rightKey = rightKey.substr(i + 1);
                        break;
                    }
                }

                LeftJoinKeys.emplace_back(leftKey);
                RightJoinKeys.emplace_back(rightKey);
            }
        }
    };

    struct TNode {
        TNodeSet SimpleNeighborhood;
        TVector<size_t> ComplexEdgesId;
        std::shared_ptr<IBaseOptimizerNode> RelationOptimizerNode;
    };

public:
    /* Add node to the hypergraph and returns its id */
    size_t AddNode(const std::shared_ptr<IBaseOptimizerNode>& relationNode) {
        size_t nodeId = Nodes_.size(); 
        NodeIdByRelationOptimizerNode_.insert({relationNode, nodeId});

        Nodes_.push_back({});
        Nodes_.back().RelationOptimizerNode = relationNode;

        return nodeId;
    }

    /* Adds an edge, and its reversed version. */
    void AddEdge(TEdge edge) {
        size_t edgeId = Edges_.size();
        size_t reversedEdgeId = edgeId + 1;
        edge.ReversedEdgeId = reversedEdgeId;

        AddEdgeImpl(edge);

        std::set<std::pair<TJoinColumn, TJoinColumn>> reversedJoinConditions;
        for (const auto& [lhs, rhs]: edge.JoinConditions) {
            reversedJoinConditions.insert({rhs, lhs});
        }

        TEdge reversedEdge = std::move(edge);
        std::swap(reversedEdge.Left, reversedEdge.Right);
        reversedEdge.JoinConditions = std::move(reversedJoinConditions);
        reversedEdge.IsReversed = true;
        reversedEdge.ReversedEdgeId = edgeId;
    
        AddEdgeImpl(reversedEdge);
    }

    TNodeSet GetNodesByRelNamesInSubtree(const std::shared_ptr<IBaseOptimizerNode>& subtreeRoot, const TVector<TString>& relationNames) {
        if (subtreeRoot->Kind == RelNodeType) {
            TString relationName = subtreeRoot->Labels()[0];

            TNodeSet nodeSet{};
            if (std::find(relationNames.begin(), relationNames.end(), relationName) != relationNames.end()) {
                nodeSet[NodeIdByRelationOptimizerNode_[subtreeRoot]] = 1;
            }
            return nodeSet;
        }

        auto joinNode = std::static_pointer_cast<TJoinOptimizerNode>(subtreeRoot);
        
        auto leftNodeSet = GetNodesByRelNamesInSubtree(joinNode->LeftArg, relationNames);
        auto rightNodeSet = GetNodesByRelNamesInSubtree(joinNode->RightArg, relationNames);

        TNodeSet nodeSet = leftNodeSet | rightNodeSet;

        return nodeSet;
    }

    TEdge& GetEdge(size_t edgeId) {
        Y_ASSERT(edgeId < Edges_.size());
        return Edges_[edgeId];
    }

    inline TVector<TNode>& GetNodes() {
        return Nodes_;
    }

    const TEdge* FindEdgeBetween(const TNodeSet& lhs, const TNodeSet& rhs) {
        for (const auto& edge: Edges_) {
            if (
                IsSubset(edge.Left, lhs) &&
                !Overlaps(edge.Left, rhs) &&
                IsSubset(edge.Right, rhs) &&
                !Overlaps(edge.Right, lhs)
            ) {
                return &edge;
            }
        }

        return nullptr;
    }

private:
    /* Attach edges to nodes */
    void AddEdgeImpl(TEdge edge) {
        Edges_.push_back(edge);

        if (edge.IsSimpleEdge()) {
            Nodes_[GetLowestSetBit(edge.Left)].SimpleNeighborhood |= edge.Right;
            return;
        }

        auto setBitsIt = TSetBitsIt(edge.Left);
        while (setBitsIt.HasNext()) {
            Nodes_[setBitsIt.Next()].ComplexEdgesId.push_back(Edges_.size() - 1);
        }
    }

private:
    std::unordered_map<std::shared_ptr<IBaseOptimizerNode>, size_t> NodeIdByRelationOptimizerNode_;

    TVector<TNode> Nodes_;
    TVector<TEdge> Edges_;
};

} // namespace NYql::NDq
