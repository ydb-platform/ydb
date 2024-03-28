#pragma once

#include <vector>

#include "dphyp_bitset.h"

#include <ydb/library/yql/core/cbo/cbo_optimizer_new.h> 
#include <ydb/library/yql/core/yql_cost_function.h>

namespace NYql::NDq::NDphyp {

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

        void BuildCondVectors() {
            LeftJoinKeys.clear();
            RightJoinKeys.clear();

            for (auto [left, right] : JoinConditions) {
                auto leftKey = left.AttributeName;
                auto rightKey = right.AttributeName;

                for (size_t i = leftKey.size() - 1; i>0; i--) {
                    if (leftKey[i]=='.') {
                        leftKey = leftKey.substr(i+1);
                        break;
                    }
                }

                for (size_t i = rightKey.size() - 1; i>0; i--) {
                    if (rightKey[i]=='.') {
                        rightKey = rightKey.substr(i+1);
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
        TVector<TEdge*> ComplexEdges;
        std::shared_ptr<IBaseOptimizerNode> RelationOptimizerNode;
    };

public:
    TNodeSet AddNode(const std::shared_ptr<IBaseOptimizerNode>& relationNode) {
        Y_ASSERT(relationNode->Labels().size() == 1);

        auto it = NodeIdByRelationName_.find(relationNode->Labels()[0]); 

        if (it != NodeIdByRelationName_.end()) {
            return it->second;
        }

        size_t nodeId = Nodes_.size(); 
        NodeIdByRelationName_.insert_noresize({relationNode->Labels()[0], nodeId});

        Nodes_.push_back({});
        Nodes_.back().RelationOptimizerNode = relationNode;

        return nodeId;
    }

    void AddEdge(TEdge edge) {
        AddEdgeImpl(edge);

        TEdge reversedEdge = std::move(edge);
        std::swap(reversedEdge.Left, reversedEdge.Right);
        AddEdgeImpl(reversedEdge);
    }

    TNodeSet GetNodesByRelNames(const TVector<TString>& relationNames) {
        TNodeSet nodeSet{};

        for (const auto& relationName: relationNames) {
            nodeSet |= NodeIdByRelationName_[relationName];
        }

        return nodeSet;
    }

    inline size_t GetNodeCount() {
        return Nodes_.size();
    }

    inline TVector<TNode>& GetNodes() {
        return Nodes_;
    }

    const TEdge* FindEdgeBetween(const TNodeSet& lhs, const TNodeSet& rhs) {
        for (const auto& edge: Edges_) {
            if (IsSubset(edge.Left, lhs) && !AreOverlaps(edge.Left, rhs) && IsSubset(edge.Right, rhs) && !AreOverlaps(edge.Right, lhs)) {
                return &edge;
            }
        }

        return nullptr;
    }

private:
    void AddEdgeImpl(TEdge edge) {
        Edges_.push_back(edge);

        if (edge.IsSimpleEdge()) {
            Nodes_[GetLowestSetBit(edge.Left)].SimpleNeighborhood |= edge.Right;
            return;
        }

        auto setBitsIt = TSetBitsIt(edge.Left);
        while (setBitsIt.HasNext()) {
            Nodes_[setBitsIt.Next()].ComplexEdges.push_back(&Edges_.back());
        }
    }

private:
    THashMap<TString, size_t> NodeIdByRelationName_;

    TVector<TNode> Nodes_;
    TVector<TEdge> Edges_;
};

} // namespace NYql::NDq
