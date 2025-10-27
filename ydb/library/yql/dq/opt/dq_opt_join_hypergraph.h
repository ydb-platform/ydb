#pragma once

#include <util/string/join.h>
#include <util/string/printf.h>
#include <util/string/builder.h>

#include "bitset.h"


#include "dq_opt_join_tree_node.h"

#include <yql/essentials/core/cbo/cbo_optimizer_new.h>
#include <yql/essentials/core/yql_cost_function.h>
#include <library/cpp/iterator/zip.h>
#include <library/cpp/disjoint_sets/disjoint_sets.h>
#include <yql/essentials/core/cbo/cbo_interesting_orderings.h>
#include <yql/essentials/utils/log/log.h>


namespace NYql::NDq {

/*
 * JoinHypergraph - a graph, whose edge connects two sets of nodes.
 * It represents relation between tables and ordering constraints.
 * Graph is directed, so it stores each edge twice (original and reversed) for DPHyp algorithm.
 */
template <typename TNodeSet>
class TJoinHypergraph {
public:
    struct TEdge {
        TEdge(
            const TNodeSet& left,
            const TNodeSet& right,
            EJoinKind joinKind,
            bool leftAny,
            bool rightAny,
            bool isCommutative,
            const TVector<TJoinColumn>& leftJoinKeys,
            const TVector<TJoinColumn>& rightJoinKeys
        )
            : Left(left)
            , Right(right)
            , JoinKind(joinKind)
            , LeftAny(leftAny)
            , RightAny(rightAny)
            , IsCommutative(isCommutative)
            , LeftJoinKeys(leftJoinKeys)
            , RightJoinKeys(rightJoinKeys)
            , IsReversed(false)
        {
            Y_ASSERT(LeftJoinKeys.size() == RightJoinKeys.size());
        }

        inline bool IsSimple() const {
            return HasSingleBit(Left) && HasSingleBit(Right);
        }

        TNodeSet Left;
        TNodeSet Right;
        EJoinKind JoinKind;
        bool LeftAny, RightAny;
        bool IsCommutative;
        TVector<TJoinColumn> LeftJoinKeys;
        TVector<TJoinColumn> RightJoinKeys;

        // for interesting orderings framework
        TOrderingsStateMachine::TFDSet FDs;
        std::int64_t LeftJoinKeysShuffleOrderingIdx = TJoinOptimizerNodeInternal::NoOrdering;
        std::int64_t RightJoinKeysShuffleOrderingIdx = TJoinOptimizerNodeInternal::NoOrdering;

        // JoinKind may not be commutative, so we need to know which edge is original and which is reversed.
        bool IsReversed;
        int64_t ReversedEdgeId = -1;

        TEdge CreateReversed(int64_t reversedEdgeId) const {
            auto reversedEdge = TEdge(Right, Left, JoinKind, RightAny, LeftAny, IsCommutative, RightJoinKeys, LeftJoinKeys);
            reversedEdge.IsReversed = true; reversedEdge.ReversedEdgeId = reversedEdgeId;
            std::swap(reversedEdge.LeftJoinKeysShuffleOrderingIdx, reversedEdge.RightJoinKeysShuffleOrderingIdx);
            return reversedEdge;
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

        TVector<TString> relNameByNodeId(Nodes_.size());
        res.append("Nodes: ").append("\n");
        for (const auto& [name, idx]: NodeIdByRelationName_) {
            relNameByNodeId[idx] = name;
        }

        for (size_t idx = 0; idx < relNameByNodeId.size(); ++idx) {
            res.append(Sprintf("%ld: %s\n", idx, relNameByNodeId[idx].c_str()));
        }

        res.append(Sprintf("Edges(%ld): ", Edges_.size())).append("\n");

        auto edgeSideToString =
            [&relNameByNodeId](const TNodeSet& edgeSide) {
                TString res;
                res.append("{");

                for (size_t i = 0; i < edgeSide.size(); ++i) {
                    if (edgeSide[i]) {
                        res.append(relNameByNodeId[i]).append(", ");
                    }
                }

                if (res != "{") {
                    res.pop_back();
                    res.pop_back();
                }

                res.append("}");

                return res;
            };

        for (const auto& edge: Edges_) {
            TVector<TString> conds;
            for (const auto& [lhsCond, rhsCond]: Zip(edge.LeftJoinKeys, edge.RightJoinKeys)) {
                TString cond = Sprintf(
                    "%s.%s = %s.%s",
                    lhsCond.RelName.c_str(), lhsCond.AttributeName.c_str(), rhsCond.RelName.c_str(), rhsCond.AttributeName.c_str()
                );

                conds.push_back(std::move(cond));
            }

            res
                .append(edgeSideToString(edge.Left))
                .append(" -> ")
                .append(edgeSideToString(edge.Right))
                .append("\t").append(JoinSeq(", ", conds))
                .append("\n");
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

        TEdge reversedEdge = edge.CreateReversed(edgeId);
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

    bool HasLabels(const TVector<TString>& labels) const  {
        for (const auto& label: labels) {
            if (!NodeIdByRelationName_.contains(label)) {
                return false;
            }
        }

        return true;
    }

    inline const TVector<TNode>& GetNodes() const {
        return Nodes_;
    }

    inline TVector<TEdge>& GetEdges() {
        return Edges_;
    }

    /* Find any edge between lhs and rhs. (It can skip conditions and generate invalid plan in case of cycles) */
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

    /*
     * This functions returns all conditions without redundancy between lhs and rhs
     * Many conditions can cause in a graph with cycles, but transitive closure conditions in one eq. class
     * will be redudant, so we consider only one of condition from eq. class.
     */
    void FindAllConditionsBetween(
        const TNodeSet& lhs,
        const TNodeSet& rhs,
        TVector<TJoinColumn>& resLeftJoinKeys,
        TVector<TJoinColumn>& resRightJoinKeys
    ) {
        for (const auto& edge: Edges_) {
            if (
                IsSubset(edge.Left, lhs) &&
                !Overlaps(edge.Left, rhs) &&
                IsSubset(edge.Right, rhs) &&
                !Overlaps(edge.Right, lhs)
            ) {
                for (const auto& [lhsEdgeCond, rhsEdgeCond]: Zip(edge.LeftJoinKeys, edge.RightJoinKeys)) {
                    bool hasSameEquivClass = false;
                    for (const auto& lhsResJoinKey: resLeftJoinKeys) {
                        if (lhsEdgeCond.EquivalenceClass.has_value() && lhsEdgeCond.EquivalenceClass == lhsResJoinKey.EquivalenceClass || lhsEdgeCond == lhsResJoinKey) {
                            hasSameEquivClass = true; break;
                        }
                    }

                    if (!hasSameEquivClass) {
                        resLeftJoinKeys.push_back(lhsEdgeCond);
                        resRightJoinKeys.push_back(rhsEdgeCond);
                    }
                }
            }
        }
    }

    /*
     * Updates the left and right node sets of an existing edge in the hypergraph saving its invariants.
     *
     * This method is typically used during join optimization when we need to expand
     * the scope of a join edge to include additional nodes (e.g., when applying
     * join hints or reordering operations).
     */
    void UpdateEdgeSides(size_t idx, TNodeSet newLeft, TNodeSet newRight) {
        auto& edge = Edges_[idx];

        Y_ENSURE(IsSubset(edge.Left, newLeft) && IsSubset(edge.Right, newRight), "Hint violates the join order restriction!");

        if (edge.IsSimple() && !(HasSingleBit(newLeft) && HasSingleBit(newRight))) {
            size_t lhsNodeIdx = GetLowestSetBit(edge.Left);
            Nodes_[lhsNodeIdx].SimpleNeighborhood &= ~edge.Right;
            Nodes_[lhsNodeIdx].ComplexEdgesId.push_back(idx);
        }
        edge.Left = newLeft;
        edge.Right = newRight;
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
 * This class applies join order hints to a hypergraph.
 * It traverses a hints tree and modifies the edges to restrict the join order in the subgraph, which has all the nodes from the hints tree.
 * Then, it restricts the join order of the edges, which connect the subgraph with the all graph.
 */
template <typename TNodeSet>
class TJoinOrderHintsApplier {
public:
    TJoinOrderHintsApplier(TJoinHypergraph<TNodeSet>& graph)
        : Graph_(graph)
    {}

    void Apply(TJoinOrderHints& hints) {
        for (auto& hint: hints.Hints) {
            if (!Graph_.HasLabels(hint.Tree->Labels())) {
                continue;
            }

            auto labels = ApplyHintsToSubgraph(hint.Tree);
            auto nodes = Graph_.GetNodesByRelNames(labels);

            for (size_t i = 0; i < Graph_.GetEdges().size(); ++i) {
                TNodeSet newLeft = Graph_.GetEdge(i).Left;
                if (Overlaps(Graph_.GetEdge(i).Left, nodes) && !IsSubset(Graph_.GetEdge(i).Right, nodes)) {
                    newLeft |= nodes;
                }

                TNodeSet newRight = Graph_.GetEdge(i).Right;
                if (Overlaps(Graph_.GetEdge(i).Right, nodes) && !IsSubset(Graph_.GetEdge(i).Left, nodes)) {
                    newRight |= nodes;
                }

                Graph_.UpdateEdgeSides(i, newLeft, newRight);
            }

            hint.Applied = true;
        }
    }

private:
    TVector<TString> ApplyHintsToSubgraph(const std::shared_ptr<TJoinOrderHints::ITreeNode>& node) {
        if (node->IsJoin()) {
            auto join = std::static_pointer_cast<TJoinOrderHints::TJoinNode>(node);
            TVector<TString> lhsLabels = ApplyHintsToSubgraph(join->Lhs);
            TVector<TString> rhsLabels = ApplyHintsToSubgraph(join->Rhs);

            auto lhs = Graph_.GetNodesByRelNames(lhsLabels);
            auto rhs = Graph_.GetNodesByRelNames(rhsLabels);

            auto* maybeEdge = Graph_.FindEdgeBetween(lhs, rhs);
            if (maybeEdge == nullptr) {
                const char* errStr = "There is no edge between {%s}, {%s}. The graf: %s";
                Y_ENSURE(false, Sprintf(errStr, JoinSeq(", ", lhsLabels).c_str(), JoinSeq(", ", rhsLabels).c_str(), Graph_.String().c_str()));
            }

            size_t revEdgeIdx = maybeEdge->ReversedEdgeId;
            auto& revEdge = Graph_.GetEdge(revEdgeIdx);
            size_t edgeIdx = revEdge.ReversedEdgeId;
            auto& edge = Graph_.GetEdge(edgeIdx);

            edge.IsReversed = false;
            revEdge.IsReversed = true;

            edge.IsCommutative = false;
            revEdge.IsCommutative = false;

            Graph_.UpdateEdgeSides(edgeIdx, lhs, rhs);
            Graph_.UpdateEdgeSides(revEdgeIdx, rhs, lhs);

            TVector<TString> joinLabels = std::move(lhsLabels);
            joinLabels.insert(
                joinLabels.end(),
                std::make_move_iterator(rhsLabels.begin()),
                std::make_move_iterator(rhsLabels.end())
            );
            return joinLabels;
        }

        auto relation = std::static_pointer_cast<TJoinOrderHints::TRelationNode>(node);
        return {relation->Label};
    }

private:
    TJoinHypergraph<TNodeSet>& Graph_;
};

/*
 *  This class construct transitive closure between nodes in hypergraph.
 *  Transitive closure means that if we have an edge from (1,2) with join
 *  condition R.Z = S.A and we have an edge from (2,3) with join condition
 *  S.A = T.V, we will find out that the join conditions form an equivalence set
 *  and add an edge (1,3) with join condition R.Z = T.V.
 *  Algorithm works as follows:
 *      1) We leave only inner-join simple edges
 *      2) We build connected components (by join conditions) and in each components we add missing edges.
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
        auto edges = Graph_.GetEdges();

        EraseIf(
            edges,
            [](const THyperedge& edge) {
                return edge.IsReversed || !edge.IsSimple() || edge.JoinKind != InnerJoin || edge.LeftAny || edge.RightAny;
            }
        );

        ConstructImpl(edges);
    }

private:
    void ConstructImpl(const TVector<THyperedge>& edges) {
        std::vector<TJoinColumn> joinCondById;
        for (const auto& edge: edges) {
            for (const auto& [lhs, rhs]: Zip(edge.LeftJoinKeys, edge.RightJoinKeys)) {
                joinCondById.push_back(lhs);
                joinCondById.push_back(rhs);
            }
        }
        std::sort(joinCondById.begin(), joinCondById.end());
        joinCondById.erase(std::unique(joinCondById.begin(), joinCondById.end()), joinCondById.end());

        THashMap<TJoinColumn, size_t, TJoinColumn::THashFunction> idByJoinCond;
        for (size_t i = 0; i < joinCondById.size(); ++i) {
            idByJoinCond[joinCondById[i]] = i;
        }

        TDisjointSets connectedComponents(joinCondById.size());
        for (const auto& edge: edges) {
            for (const auto& [lhs, rhs]: Zip(edge.LeftJoinKeys, edge.RightJoinKeys)) {
                connectedComponents.UnionSets(idByJoinCond[lhs], idByJoinCond[rhs]);
            }
        }

        for (auto& edge: Graph_.GetEdges()) {
            for (auto& lhs : edge.LeftJoinKeys) {
                if (idByJoinCond.contains(lhs)) {
                    lhs.EquivalenceClass = connectedComponents.CanonicSetElement(idByJoinCond[lhs]);
                }
            }

            for (auto& rhs : edge.RightJoinKeys) {
                if (idByJoinCond.contains(rhs)) {
                    rhs.EquivalenceClass = connectedComponents.CanonicSetElement(idByJoinCond[rhs]);
                }
            }
        }

        for (size_t i = 0; i < joinCondById.size(); ++i) {
            joinCondById[i].EquivalenceClass = connectedComponents.CanonicSetElement(i);
        }

        for (size_t i = 0; i < joinCondById.size(); ++i) {
            for (size_t j = 0; j < i; ++j) {
                if (joinCondById[i].EquivalenceClass == joinCondById[j].EquivalenceClass && joinCondById[i].RelName != joinCondById[j].RelName) {
                    auto iNode = Graph_.GetNodesByRelNames({joinCondById[i].RelName});
                    auto jNode = Graph_.GetNodesByRelNames({joinCondById[j].RelName});

                    if (auto* maybeEdge = Graph_.FindEdgeBetween(iNode, jNode)) {
                        auto addUniqueKey = [](auto& vector, const auto& key) {
                            if (std::find(vector.begin(), vector.end(), key) == vector.end()) {
                                vector.push_back(key);
                            }
                        };

                        auto& revEdge = Graph_.GetEdge(maybeEdge->ReversedEdgeId);
                        addUniqueKey(revEdge.LeftJoinKeys, joinCondById[j]);
                        addUniqueKey(revEdge.RightJoinKeys, joinCondById[i]);

                        auto& edge = Graph_.GetEdge(revEdge.ReversedEdgeId);
                        addUniqueKey(edge.LeftJoinKeys, joinCondById[i]);
                        addUniqueKey(edge.RightJoinKeys, joinCondById[j]);
                        continue;
                    }

                    Graph_.AddEdge(THyperedge(iNode, jNode, InnerJoin, false, false, true, {joinCondById[i]}, {joinCondById[j]}));
                }
            }
        }
    }

private:
    TJoinHypergraph<TNodeSet>& Graph_;
};

/*
 * This class builds FSM for CBO join tree which is used for the DPHypElimination algorithm. It fills edges with information of orderings and FD's
 * which they have. Also, it converts orderings (vector of shuffles) into inner representation (just indexes) for faster enumeration.
 */
template <typename TNodeSet>
class TOrderingsStateMachineConstructor {
private:
    using THyperedge = typename TJoinHypergraph<TNodeSet>::TEdge;

public:
    TOrderingsStateMachineConstructor(TJoinHypergraph<TNodeSet>& graph)
        : Graph_(graph)
    {}

    TOrderingsStateMachine Construct() {
        auto& edges = Graph_.GetEdges();

        TFDStorage fdStorage;
        std::vector<std::vector<std::size_t>> fdsByEdgeIdx(edges.size());
        for (std::size_t i = 0; i < edges.size(); ++i) {
            if (edges[i].IsReversed) {
                continue;
            }

            std::size_t edgeIdx = i;
            std::size_t revEdgeIdx = edges[i].ReversedEdgeId;
            for (const auto& [lhs, rhs]: Zip(edges[i].LeftJoinKeys, edges[i].RightJoinKeys)) {
                std::size_t fdIdx = fdStorage.AddFD(lhs, rhs, TFunctionalDependency::EEquivalence, false, nullptr);
                fdsByEdgeIdx[edgeIdx].push_back(fdIdx);
                fdsByEdgeIdx[revEdgeIdx].push_back(fdIdx);
            }

            std::size_t orderingIdx;
            orderingIdx = fdStorage.AddInterestingOrdering(edges[i].LeftJoinKeys , TOrdering::EShuffle, nullptr);
            edges[edgeIdx].LeftJoinKeysShuffleOrderingIdx = orderingIdx;
            edges[revEdgeIdx].RightJoinKeysShuffleOrderingIdx = orderingIdx; // reversed edge
            orderingIdx = fdStorage.AddInterestingOrdering(edges[i].RightJoinKeys, TOrdering::EShuffle, nullptr);
            edges[edgeIdx].RightJoinKeysShuffleOrderingIdx = orderingIdx;
            edges[revEdgeIdx].LeftJoinKeysShuffleOrderingIdx = orderingIdx; // reversed edge
        }

        std::vector<std::int64_t> shuffleOrderingIdxByNodeIdx(Graph_.GetNodes().size(), -1);
        for (std::size_t i = 0; i < Graph_.GetNodes().size(); ++i) {
            auto relNode = std::static_pointer_cast<TRelOptimizerNode>(Graph_.GetNodes()[i].RelationOptimizerNode);

            if (!relNode->Stats.ShuffledByColumns) {
                YQL_CLOG(TRACE, CoreDq) << "No shuffle in stats for table: " << relNode->Labels()[0];
                continue;
            }

            std::vector<TJoinColumn> shuffledBy;
            shuffledBy.reserve(relNode->Stats.ShuffledByColumns->Data.size());
            for (const auto& column: relNode->Stats.ShuffledByColumns->Data) {
                shuffledBy.emplace_back(relNode->Label, column.AttributeName);
            }
            shuffleOrderingIdxByNodeIdx[i] = fdStorage.AddInterestingOrdering(shuffledBy, TOrdering::EShuffle, nullptr);
        }

        TOrderingsStateMachine orderingsFSM(std::move(fdStorage), TOrdering::EShuffle);

        for (std::size_t i = 0; i < edges.size(); ++i) {
            edges[i].FDs = orderingsFSM.GetFDSet(fdsByEdgeIdx[i]);
        }

        for (std::size_t i = 0; i < Graph_.GetNodes().size(); ++i) {
            auto& node = Graph_.GetNodes()[i].RelationOptimizerNode;
            node->Stats.LogicalOrderings = orderingsFSM.CreateState();
            if (shuffleOrderingIdxByNodeIdx[i] == -1) {
                continue;
            }
            node->Stats.LogicalOrderings.SetOrdering(shuffleOrderingIdxByNodeIdx[i]);
        }

        return orderingsFSM;
    }

private:
    TJoinHypergraph<TNodeSet>& Graph_;
};

/*
 * Assigns inner representation of the orderings (orderingIdx) and FD sets to edges of the hypergraph and to their nodes.
 */
template <typename TNodeSet>
class TOrderingStatesAssigner {
public:
    TOrderingStatesAssigner(
        TJoinHypergraph<TNodeSet>& graph,
        TTableAliasMap*
    )
        : Graph_(graph)
        , TableAliases_(nullptr)
    {}

    void Assign(TOrderingsStateMachine& fsm) {
        auto& edges = Graph_.GetEdges();
        auto& fdStorage = fsm.FDStorage;

        for (auto& e: edges) {
            if (e.JoinKind == EJoinKind::Cross) {
                continue;
            }

            e.LeftJoinKeysShuffleOrderingIdx =
                fdStorage.FindInterestingOrderingIdx(e.LeftJoinKeys, TOrdering::EShuffle, TableAliases_);

            e.RightJoinKeysShuffleOrderingIdx =
                fdStorage.FindInterestingOrderingIdx(e.RightJoinKeys, TOrdering::EShuffle, TableAliases_);

            for (const auto& [lhs, rhs]: Zip(e.LeftJoinKeys, e.RightJoinKeys)) {
                auto fdIdx = fdStorage.FindFDIdx(lhs, rhs, TFunctionalDependency::EEquivalence, TableAliases_);
                auto fdIdxRev = fdStorage.FindFDIdx(rhs, lhs, TFunctionalDependency::EEquivalence, TableAliases_);
                e.FDs |= fsm.GetFDSet(fdIdx);
                e.FDs |= fsm.GetFDSet(fdIdxRev);
            }
        }
    }

private:
    TJoinHypergraph<TNodeSet>& Graph_;
    TTableAliasMap* TableAliases_;
};

} // namespace NYql::NDq
