#pragma once

#include <vector>
#include <util/string/printf.h>
#include "bitset.h"

#include <ydb/library/yql/core/cbo/cbo_optimizer_new.h> 
#include <ydb/library/yql/core/yql_cost_function.h>
#include <library/cpp/disjoint_sets/disjoint_sets.h>


#include "dq_opt_conflict_rules_collector.h"

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

        bool AreCondVectorEqual() const {
            return LeftJoinKeys == RightJoinKeys;
        }

        inline bool IsSimple() const {
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

                if (auto idx = leftKey.find_last_of('.'); idx != TString::npos) {
                    leftKey =  leftKey.substr(idx+1);
                }

                if (auto idx = rightKey.find_last_of('.'); idx != TString::npos) {
                    rightKey =  rightKey.substr(idx+1);
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
    /* For debug purposes */
    TString String() {
        TString res;

        res.append("Edges: ").append("\n");
        for (const auto& edge: Edges_) {
            res.append("{");

            auto left = TSetBitsIt(edge.Left);
            while (left.HasNext()) {
                res.append(ToString(left.Next())).append(", ");
            }
            res.pop_back();
            res.pop_back();

            res.append("}");
            
            res.append(" -> ");

            res.append("{");

            auto right = TSetBitsIt(edge.Right);
            while (right.HasNext()) {
                res.append(ToString(right.Next())).append(", ");
            }
            res.pop_back();
            res.pop_back();

            res.append("}, ");

            for (auto l : edge.LeftJoinKeys) {
                res.append(l).append(",");
            }
            res.append("=");
            for (auto r : edge.RightJoinKeys) {
                res.append(r).append(",");
            }

            res.append("\n");
        }
        
        return res;
    }

    /* Add node to the hypergraph and returns its id */
    size_t AddNode(const std::shared_ptr<IBaseOptimizerNode>& relationNode) {
        Y_ASSERT(relationNode->Labels().size() == 1);

        size_t nodeId = Nodes_.size(); 
        NodeIdByRelationName_.insert({relationNode->Labels()[0], nodeId});

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
        reversedEdge.BuildCondVectors();
    
        AddEdgeImpl(reversedEdge);
    }

    TNodeSet GetNodesByRelNames(const TVector<TString>& relationNames) {
        TNodeSet nodeSet{};

        for (const auto& relationName: relationNames) {
            nodeSet[NodeIdByRelationName_[relationName]] = 1;
        }

        return nodeSet;
    }


    TEdge& GetEdge(size_t edgeId) {
        Y_ASSERT(edgeId < Edges_.size());
        return Edges_[edgeId];
    }

    TVector<TEdge> GetSimpleEdges() {
        TVector<TEdge> simpleEdges;
        simpleEdges.reserve(Edges_.size());

        for (const auto& edge: Edges_) {
            if (edge.IsSimple()) {
                simpleEdges.push_back(edge);
            }
        }
        
        return simpleEdges;
    }

    inline const TVector<TNode>& GetNodes() const {
        return Nodes_;
    }

    inline const TVector<TEdge>& GetEdges() const {
        return Edges_;
    }

    const TEdge* FindEdgeBetween(const TNodeSet& lhs, const TNodeSet& rhs) const {
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

        if (edge.IsSimple()) {
            Nodes_[GetLowestSetBit(edge.Left)].SimpleNeighborhood |= edge.Right;
            return;
        }

        auto setBitsIt = TSetBitsIt(edge.Left);
        while (setBitsIt.HasNext()) {
            Nodes_[setBitsIt.Next()].ComplexEdgesId.push_back(Edges_.size() - 1);
        }
    }

private:
    THashMap<TString, size_t> NodeIdByRelationName_;

    TVector<TNode> Nodes_;
    TVector<TEdge> Edges_;
};

/* 
 *  This class construct transitive closure between nodes in hypergraph. 
 *  Transitive closure means that if we have an edge from (1,2) with join
 *  condition R.A = S.A and we have an edge from (2,3) with join condition
 *  S.A = T.A, we will find out that the join conditions form an equivalence set
 *  and add an edge (1,3) with join condition R.A = T.A.
 *  Algorithm works as follows:
 *      1) We leave only edges that do not conflict with themselves and 
 *      in join condition equality attributes on left and right side must be equal by name.
 *      (e.g. a.id = b.id && a.kek = b.kek)
 *      2) We group edges by attribute names in equality and joinKind
 *      3) In each group we build connected components and in each components we add missing edges. 
 */
template <typename TNodeSet>
class TTransitiveClosureConstructor {
private:
    using THyperedge = typename TJoinHypergraph<TNodeSet>::TEdge;

public:
    TTransitiveClosureConstructor(TJoinHypergraph<TNodeSet>& graph)
        : Graph_(graph)
    {}

    void Construct() {
        auto edges = Graph_.GetSimpleEdges();

        EraseIf(
            edges, 
            [this](const THyperedge& edge) {
                return 
                    edge.IsReversed || 
                    !(IsJoinTransitiveClosureSupported(edge.JoinKind) && edge.AreCondVectorEqual());
            }
        );
        
        std::sort(
            edges.begin(),
            edges.end(),
            [](const THyperedge& lhs, const THyperedge& rhs) {    
                auto lhsAttributeNames = lhs.LeftJoinKeys;
                auto rhsAttributeNames = rhs.LeftJoinKeys;

                std::sort(lhsAttributeNames.begin(), lhsAttributeNames.end());
                std::sort(rhsAttributeNames.begin(), rhsAttributeNames.end());

                return 
                    std::tie(lhsAttributeNames, lhs.JoinKind) < 
                    std::tie(rhsAttributeNames, rhs.JoinKind);
            }
        );
        
        size_t groupBegin = 0;
        for (size_t groupEnd = 0; groupEnd < edges.size();) {
            while (groupEnd < edges.size() && HasOneGroup(edges[groupBegin], edges[groupEnd])) {
                ++groupEnd;
            }

            if (groupEnd - groupBegin >= 2) {
                ComputeTransitiveClosureInGroup(edges, groupBegin, groupEnd);
            }

            groupBegin = groupEnd;
        }
    }

private:
    void ComputeTransitiveClosureInGroup(const TVector<THyperedge>& edges, size_t groupBegin, size_t groupEnd) {
        size_t nodeSetSize = TNodeSet{}.size();
        const auto& nodes = Graph_.GetNodes();

        EJoinKind groupJoinKind = edges[groupBegin].JoinKind;
        bool isJoinCommutative = edges[groupBegin].IsCommutative;

        TVector<TString> groupConditionUsedAttributes;
        for (const auto& [lhs, rhs]:  edges[groupBegin].JoinConditions) {
            groupConditionUsedAttributes.push_back(lhs.AttributeName);
        }

        TDisjointSets connectedComponents(nodeSetSize);
        for (size_t edgeId = groupBegin; edgeId < groupEnd; ++edgeId) {
            const auto& edge = edges[edgeId];
            connectedComponents.UnionSets(GetLowestSetBit(edge.Left), GetLowestSetBit(edge.Right));
        }

        for (size_t i = 0; i < nodeSetSize; ++i) {
            for (size_t j = 0; j < i; ++j) {
                if (
                    connectedComponents.CanonicSetElement(i) == 
                    connectedComponents.CanonicSetElement(j)
                ) {
                    TNodeSet lhs;
                    lhs[i] = 1;

                    TNodeSet rhs;
                    rhs[j] = 1;

                    const auto* edge = Graph_.FindEdgeBetween(lhs, rhs);

                    if (edge != nullptr) {
                        continue;
                    }

                    TString lhsRelName = nodes[i].RelationOptimizerNode->Labels()[0];
                    TString rhsRelName = nodes[j].RelationOptimizerNode->Labels()[0];

                    std::set<std::pair<TJoinColumn, TJoinColumn>> joinConditions;
                    for (const auto& attributeName: groupConditionUsedAttributes){
                        joinConditions.insert(
                            {
                                TJoinColumn(lhsRelName, attributeName), 
                                TJoinColumn(rhsRelName, attributeName)
                                }
                        );
                    }

                    Graph_.AddEdge(
                        THyperedge(
                            lhs,
                            rhs,
                            groupJoinKind,
                            isJoinCommutative,
                            joinConditions
                        )
                    );
                }
            }
        }
    }

    bool HasOneGroup(const THyperedge& lhs, const THyperedge& rhs) {
        auto lhsAttributeNames = lhs.LeftJoinKeys;
        auto rhsAttributeNames = rhs.LeftJoinKeys;

        std::sort(lhsAttributeNames.begin(), lhsAttributeNames.end());
        std::sort(rhsAttributeNames.begin(), rhsAttributeNames.end());

        return lhsAttributeNames == rhsAttributeNames && lhs.JoinKind == rhs.JoinKind;
    }

    bool IsJoinTransitiveClosureSupported(EJoinKind joinKind)  {
        return 
            OperatorsAreAssociative(joinKind, joinKind) &&
            OperatorsAreLeftAsscom(joinKind, joinKind) &&
            OperatorsAreRightAsscom(joinKind, joinKind);
    }

private:
    TJoinHypergraph<TNodeSet>& Graph_;
};

} // namespace NYql::NDq
