#pragma once

#include "dq_opt_join_hypergraph.h"
#include "dq_opt_join_tree_node.h"
#include "bitset.h"

namespace NYql::NDq {

TString GetJoinOrderString(const std::shared_ptr<IBaseOptimizerNode>& node) {
    if (node->Kind == EOptimizerNodeKind::RelNodeType) {
        return node->Labels()[0];
    }

    auto joinNode = std::static_pointer_cast<TJoinOptimizerNodeInternal>(node);
    return  "(" + GetJoinOrderString(joinNode->LeftArg) + "," + GetJoinOrderString(joinNode->RightArg) + ")";
}

struct TBestJoin {
    TOptimizerStatistics Stats;
    EJoinAlgoType Algo;
    bool IsReversed; // todo ticket #12291
};

/*
 * DPHyp (Dynamic Programming with Hypergraph) is a graph-aware
 * join eumeration algorithm that only considers CSGs (Connected Sub-Graphs) of
 * the join graph and computes CMPs (Complement pairs) that are also connected
 * subgraphs of the join graph. It enumerates CSGs in the order, such that subsets
 * are enumerated first and no duplicates are ever enumerated. Then, for each emitted
 * CSG it computes the complements with the same conditions - they much already be
 * present in the dynamic programming table and no pair should be enumerated twice.
 * Details are described in white papper - "Dynamic Programming Strikes Back".
 *
 * This class is templated by std::bitset with the largest number of joins we can process
 * or std::bitset<64>, which has a more efficient implementation of enumerating subsets of set.
 *
 * TDPHypSolverBase class contains core enumeration logic. It is templated with TDerived parameter.
 * TDerived is a class which is derived from TDPHypSolverBase. This class contains DPTable and
 * saving the lowest cost plan logic (EmitCsgCmp method).
 *
 * Also, it has a bool ProcessCycles template parameter, which makes algorithm consider all edges
 * between csg-cmp. It makes dphyp slower, but without it we can miss a condition in case of cycles
 */
template <typename TNodeSet, typename TDerived>
class TDPHypSolverBase {
public:
    TDPHypSolverBase(
        TJoinHypergraph<TNodeSet>& graph,
        IProviderContext& ctx,
        TDerived& derived
    )
        : Graph_(graph)
        , NNodes_(graph.GetNodes().size())
        , Pctx_(ctx)
        , Derived(derived)
    {}

    // Run DPHyp algorithm and produce the join tree in CBO's internal representation
    std::shared_ptr<TJoinOptimizerNodeInternal> Solve(const TOptimizerHints& hints);

    // Calculate the size of a dynamic programming table with a budget
    ui32 CountCC(ui32 budget);

private:
    void EnumerateCsgRec(const TNodeSet& s1, const TNodeSet& x);

    void EmitCsg(const TNodeSet& s1);

    void EnumerateCmpRec(const TNodeSet& s1, const TNodeSet& s2, const TNodeSet& x);

    void EmitCsgCmp(const TNodeSet& s1, const TNodeSet& s2, const typename TJoinHypergraph<TNodeSet>::TEdge* csgCmpEdge, const typename TJoinHypergraph<TNodeSet>::TEdge* reversedCsgCmpEdge);

private:
    // Create an exclusion set that contains all the nodes of the graph that are smaller or equal to
    // the smallest node in the provided bitset
    inline TNodeSet MakeBiMin(const TNodeSet& s);

    // Create an exclusion set that contains all the nodes of the bitset that are smaller or equal to
    // the provided integer
    inline TNodeSet MakeB(const TNodeSet& s, size_t v);

    // Compute the neighbors of a set of nodes, excluding the nodes in exclusion set
    TNodeSet Neighs(TNodeSet s, TNodeSet x);

    // Compute the next subset of relations, given by the final bitset
    TNodeSet NextBitset(const TNodeSet& current, const TNodeSet& final);

    // Count the size of the dynamic programming table recursively
    ui32 CountCCRec(const TNodeSet&, const TNodeSet&, ui32, ui32);

protected:
    TJoinHypergraph<TNodeSet>& Graph_;
    size_t NNodes_;
    IProviderContext& Pctx_;  // Provider specific contexts?
    // FIXME: This is a temporary structure that needs to be extended to multiple providers, also we need to remove virtual functions, they are really expensive #10578.

    TDerived& Derived; // this class owns DPTable and chose best plan in EmitCsgCmp method. It exists for avoiding slow virtual functions

    #ifndef NDEBUG
        struct TPairHash {
            template <class T1, class T2>
            std::size_t operator () (const std::pair<T1,T2> &p) const {
                auto h1 = std::hash<T1>{}(p.first);
                auto h2 = std::hash<T2>{}(p.second);

                // Mainly for demonstration purposes, i.e. works but is overly simple
                // In the real world, use sth. like boost.hash_combine
                return h1 ^ h2;
            }
        };
        THashMap<std::pair<TNodeSet, TNodeSet>, bool, TPairHash> CheckTable_;
    #endif
protected:
    THashMap<TNodeSet, TCardinalityHints::TCardinalityHint*, std::hash<TNodeSet>> BytesHintsTable_;
    THashMap<TNodeSet, TCardinalityHints::TCardinalityHint*, std::hash<TNodeSet>> CardHintsTable_;
    THashMap<TNodeSet, TJoinAlgoHints::TJoinAlgoHint*, std::hash<TNodeSet>> JoinAlgoHintsTable_;
};

template <typename TNodeSet>
class TDPHypSolverShuffleElimination : public TDPHypSolverBase<TNodeSet, TDPHypSolverShuffleElimination<TNodeSet>> {
public:
    TDPHypSolverShuffleElimination(
        TJoinHypergraph<TNodeSet>& graph,
        IProviderContext& ctx,
        TOrderingsStateMachine& orderingFSM
    )
        : TDPHypSolverBase<TNodeSet, TDPHypSolverShuffleElimination<TNodeSet>>(graph, ctx, *this)
        , OrderingsFSM(orderingFSM)
    {}

    std::string Type() const {
        return "DPHypShuffleElimination";
    }

    void InitDpEntry(const TNodeSet& nodes, const std::shared_ptr<IBaseOptimizerNode>& relNode) {
        DpTable_[nodes].emplace_back(relNode);
    }

    std::shared_ptr<IBaseOptimizerNode> GetLowestCostTree(const TNodeSet& nodes) {
        Y_ASSERT(DpTable_.contains(nodes));


        #ifndef NDEBUG
            std::size_t candidateIdx = 0;
            YQL_CLOG(TRACE, CoreDq) << "GetLowestCostTree candidates:";
            for (const auto& joinTree: DpTable_[nodes]) {
                std::stringstream ss;
                joinTree->Print(ss);
                YQL_CLOG(TRACE, CoreDq) << "Candidate #" << candidateIdx << "\n" << ss.str();
            }
        #endif

        auto minCost = std::min_element(
            DpTable_[nodes].begin(),
            DpTable_[nodes].end(),
            [](const auto& lhs, const auto& rhs) { return lhs->Stats.Cost < rhs->Stats.Cost; }
        );
        return *minCost;
    }

    void EmitCsgCmp(
        const TNodeSet& s1,
        const TNodeSet& s2,
        const typename TJoinHypergraph<TNodeSet>::TEdge* csgCmpEdge,
        const typename TJoinHypergraph<TNodeSet>::TEdge* reversedCsgCmpEdge
    );

    TBestJoin PickBestJoin(
        const std::shared_ptr<IBaseOptimizerNode>& left,
        const std::shared_ptr<IBaseOptimizerNode>& right,
        const TJoinHypergraph<TNodeSet>::TEdge& edge,
        bool shuffleLeftSide,
        bool shuffleRightSide,
        TCardinalityHints::TCardinalityHint* maybeCardHint,
        TJoinAlgoHints::TJoinAlgoHint* maybeJoinAlgoHint,
        TCardinalityHints::TCardinalityHint* maybeBytesHint
    );

    std::shared_ptr<IBaseOptimizerNode> PickBestJoinBothSidesShuffled(
        const std::shared_ptr<IBaseOptimizerNode>& left,
        const std::shared_ptr<IBaseOptimizerNode>& right,
        const TJoinHypergraph<TNodeSet>::TEdge& edge,
        TCardinalityHints::TCardinalityHint* maybeCardHint,
        TJoinAlgoHints::TJoinAlgoHint* maybeJoinAlgoHint,
        TCardinalityHints::TCardinalityHint* maybeBytesHint
    );

    std::array<std::shared_ptr<IBaseOptimizerNode>, 2> PickBestJoinRightSideShuffled(
        const std::shared_ptr<IBaseOptimizerNode>& left,
        const std::shared_ptr<IBaseOptimizerNode>& right,
        const TJoinHypergraph<TNodeSet>::TEdge& edge,
        TCardinalityHints::TCardinalityHint* maybeCardHint,
        TJoinAlgoHints::TJoinAlgoHint* maybeJoinAlgoHint,
        TCardinalityHints::TCardinalityHint* maybeBytesHint
    );

    std::array<std::shared_ptr<IBaseOptimizerNode>, 2> PickBestJoinLeftSideShuffled(
        const std::shared_ptr<IBaseOptimizerNode>& left,
        const std::shared_ptr<IBaseOptimizerNode>& right,
        const TJoinHypergraph<TNodeSet>::TEdge& edge,
        TCardinalityHints::TCardinalityHint* maybeCardHint,
        TJoinAlgoHints::TJoinAlgoHint* maybeJoinAlgoHint,
        TCardinalityHints::TCardinalityHint* maybeBytesHint
    );

    std::array<std::shared_ptr<IBaseOptimizerNode>, 3> PickBestJoinNoSidesShuffled(
        const std::shared_ptr<IBaseOptimizerNode>& left,
        const std::shared_ptr<IBaseOptimizerNode>& right,
        const TJoinHypergraph<TNodeSet>::TEdge& edge,
        TCardinalityHints::TCardinalityHint* maybeCardHint,
        TJoinAlgoHints::TJoinAlgoHint* maybeJoinAlgoHint,
        TCardinalityHints::TCardinalityHint* maybeBytesHint
    );

    std::array<std::shared_ptr<IBaseOptimizerNode>, 2> PickCrossJoinTrees(
        const std::shared_ptr<IBaseOptimizerNode>& left,
        const std::shared_ptr<IBaseOptimizerNode>& right,
        const TJoinHypergraph<TNodeSet>::TEdge& edge,
        TCardinalityHints::TCardinalityHint* maybeCardHint,
        TCardinalityHints::TCardinalityHint* maybeBytesHint
    );

    THashMap<TNodeSet, std::vector<std::shared_ptr<IBaseOptimizerNode>>, std::hash<TNodeSet>> DpTable_;
    TOrderingsStateMachine& OrderingsFSM;
};

template <typename TNodeSet>
class TDPHypSolverClassic : public TDPHypSolverBase<TNodeSet, TDPHypSolverClassic<TNodeSet>> {
public:
    TDPHypSolverClassic(
        TJoinHypergraph<TNodeSet>& graph,
        IProviderContext& ctx
    )
        : TDPHypSolverBase<TNodeSet, TDPHypSolverClassic<TNodeSet>>(graph, ctx, *this)
    {}

    std::string Type() const {
        return "DPHypClassic";
    }

    void InitDpEntry(const TNodeSet& nodes, const std::shared_ptr<IBaseOptimizerNode>& relNode) {
        DpTable_[nodes] = relNode;
    }

    std::shared_ptr<IBaseOptimizerNode> GetLowestCostTree(const TNodeSet& nodes) {
        Y_ASSERT(DpTable_.contains(nodes));
        return DpTable_[nodes];
    }

    void EmitCsgCmp(
        const TNodeSet& s1,
        const TNodeSet& s2,
        const typename TJoinHypergraph<TNodeSet>::TEdge* csgCmpEdge,
        const typename TJoinHypergraph<TNodeSet>::TEdge* reversedCsgCmpEdge
    );

    THashMap<TNodeSet, std::shared_ptr<IBaseOptimizerNode>, std::hash<TNodeSet>> DpTable_;
private:
    TBestJoin PickBestJoin(
        const std::shared_ptr<IBaseOptimizerNode>& left,
        const std::shared_ptr<IBaseOptimizerNode>& right,
        EJoinKind joinKind,
        bool isCommutative,
        const TVector<TJoinColumn>& leftJoinKeys,
        const TVector<TJoinColumn>& rightJoinKeys,
        IProviderContext& ctx,
        TCardinalityHints::TCardinalityHint* maybeCardHint,
        TCardinalityHints::TCardinalityHint* maybeBytesHint,
        TJoinAlgoHints::TJoinAlgoHint* maybeJoinAlgoHint
    );

};

/*
 * Emit a single CSG + CMP pair
 */
 template<typename TNodeSet> void TDPHypSolverClassic<TNodeSet>::EmitCsgCmp(
    const TNodeSet& s1,
    const TNodeSet& s2,
    const typename TJoinHypergraph<TNodeSet>::TEdge* csgCmpEdge,
    const typename TJoinHypergraph<TNodeSet>::TEdge* reversedCsgCmpEdge
) {
    // Here we actually build the join and choose and compare the
    // new plan to what's in the dpTable, if it there

    Y_ENSURE(DpTable_.contains(s1), "DP Table does not contain S1");
    Y_ENSURE(DpTable_.contains(s2), "DP Table does not conaint S2");

    auto leftNodes = DpTable_[s1];
    auto rightNodes = DpTable_[s2];

    if (csgCmpEdge->IsReversed) {
        std::swap(csgCmpEdge, reversedCsgCmpEdge);
        std::swap(leftNodes, rightNodes);
    }

    TNodeSet joined = s1 | s2;

    auto maybeCardHint = this->CardHintsTable_.contains(joined) ? this->CardHintsTable_[joined] : nullptr;
    auto maybeBytesHint = this->BytesHintsTable_.contains(joined) ? this->BytesHintsTable_[joined] : nullptr;
    auto maybeJoinAlgoHint = this->JoinAlgoHintsTable_.contains(joined) ? this->JoinAlgoHintsTable_[joined] : nullptr;

    auto bestJoin = PickBestJoin(
        leftNodes,
        rightNodes,
        csgCmpEdge->JoinKind,
        csgCmpEdge->IsCommutative,
        csgCmpEdge->LeftJoinKeys,
        csgCmpEdge->RightJoinKeys,
        this->Pctx_,
        maybeCardHint,
        maybeBytesHint,
        maybeJoinAlgoHint
    );

    if (!DpTable_.contains(joined) || bestJoin.Stats.Cost < DpTable_[joined]->Stats.Cost) {
        DpTable_[joined] =
            bestJoin.IsReversed?
            MakeJoinInternal(std::move(bestJoin.Stats), rightNodes, leftNodes, csgCmpEdge->RightJoinKeys, csgCmpEdge->LeftJoinKeys, csgCmpEdge->JoinKind, bestJoin.Algo, csgCmpEdge->RightAny, csgCmpEdge->LeftAny, std::nullopt) :
            MakeJoinInternal(std::move(bestJoin.Stats), leftNodes, rightNodes, csgCmpEdge->LeftJoinKeys, csgCmpEdge->RightJoinKeys, csgCmpEdge->JoinKind, bestJoin.Algo, csgCmpEdge->LeftAny, csgCmpEdge->RightAny, std::nullopt);
    }

    #ifndef NDEBUG
        auto pair = std::make_pair(s1, s2);
        Y_ENSURE (!this->CheckTable_.contains(pair), "Check table already contains pair S1|S2");
        this->CheckTable_[pair] = true;
    #endif
}

template <typename TNodeSet> TBestJoin TDPHypSolverClassic<TNodeSet>::PickBestJoin(
    const std::shared_ptr<IBaseOptimizerNode>& left,
    const std::shared_ptr<IBaseOptimizerNode>& right,
    EJoinKind joinKind,
    bool isCommutative,
    const TVector<TJoinColumn>& leftJoinKeys,
    const TVector<TJoinColumn>& rightJoinKeys,
    IProviderContext& ctx,
    TCardinalityHints::TCardinalityHint* maybeCardHint,
    TCardinalityHints::TCardinalityHint* maybeBytesHint,
    TJoinAlgoHints::TJoinAlgoHint* maybeJoinAlgoHint
) {
    if (maybeJoinAlgoHint) {
        maybeJoinAlgoHint->Applied = true;
        auto stats = ctx.ComputeJoinStatsV2(left->Stats, right->Stats, leftJoinKeys, rightJoinKeys, maybeJoinAlgoHint->Algo, joinKind, maybeCardHint, false, false, maybeBytesHint);
        return TBestJoin{.Stats = std::move(stats), .Algo = maybeJoinAlgoHint->Algo, .IsReversed = false};
    }

    TOptimizerStatistics bestJoinStats;
    double bestCost = std::numeric_limits<double>::infinity();
    EJoinAlgoType bestAlgo = EJoinAlgoType::Undefined;
    bool bestJoinIsReversed = false;

    for (auto joinAlgo : AllJoinAlgos) {
        if (ctx.IsJoinApplicable(left, right, leftJoinKeys, rightJoinKeys, joinAlgo, joinKind)){
            auto stats = ctx.ComputeJoinStatsV2(left->Stats, right->Stats, leftJoinKeys, rightJoinKeys, joinAlgo, joinKind, maybeCardHint, false, false, maybeBytesHint);
            if (stats.Cost < bestCost) {
                bestCost = stats.Cost;
                bestAlgo = joinAlgo;
                bestJoinStats = std::move(stats);
                bestJoinIsReversed = false;
            }
        }

        if (isCommutative) {
            if (ctx.IsJoinApplicable(right, left, rightJoinKeys, leftJoinKeys, joinAlgo, joinKind)){
                auto stats = ctx.ComputeJoinStatsV2(right->Stats, left->Stats,  rightJoinKeys, leftJoinKeys, joinAlgo, joinKind, maybeCardHint, false, false, maybeBytesHint);
                if (stats.Cost < bestCost) {
                    bestCost = stats.Cost;
                    bestAlgo = joinAlgo;
                    bestJoinStats = std::move(stats);
                    bestJoinIsReversed = true;
                }
            }
        }
    }

    Y_ENSURE(bestAlgo != EJoinAlgoType::Undefined, "No join was chosen!");
    return TBestJoin{.Stats = std::move(bestJoinStats), .Algo = bestAlgo, .IsReversed = bestJoinIsReversed};
}

/*
 * Count the number of items in the DP table of DPHyp
 */
template <typename TNodeSet, typename TDerived> ui32 TDPHypSolverBase<TNodeSet, TDerived>::CountCC(ui32 budget) {
    TNodeSet allNodes;
    allNodes.set();
    ui32 cost = 0;

    for (int i = NNodes_ - 1; i >= 0; --i) {
        ++cost;
        if (cost > budget) {
            return cost;
        }
        TNodeSet s;
        s[i] = 1;
        TNodeSet x = MakeB(allNodes,i);
        cost = CountCCRec(s, x, cost, budget);
    }

    return cost;
}

/**
 * Recursively count the nuber of items in the DP table of DPccp
*/
template <typename TNodeSet, typename TDerived> ui32 TDPHypSolverBase<TNodeSet, TDerived>::CountCCRec(const TNodeSet& s, const TNodeSet& x, ui32 cost, ui32 budget) {
    TNodeSet neighs = Neighs(s, x);

    if (neighs == TNodeSet{}) {
        return cost;
    }

    TNodeSet prev;
    TNodeSet next;

    while(true) {
        next = NextBitset(prev, neighs);
        cost += 1;
        if (cost > budget) {
            return cost;
        }
        cost = CountCCRec(s | next, x | neighs, cost, budget);
        if (next == neighs) {
            break;
        }
        prev = next;
    }

    return cost;
}

template<typename TNodeSet, typename TDerived> TNodeSet TDPHypSolverBase<TNodeSet, TDerived>::Neighs(TNodeSet s, TNodeSet x) {
    TNodeSet neighs{};

    auto& nodes = Graph_.GetNodes();

    TSetBitsIt<TNodeSet> setBitsIt(s);
    while (setBitsIt.HasNext()) {
        size_t nodeId = setBitsIt.Next();

        neighs |= nodes[nodeId].SimpleNeighborhood;

        for (const auto& edgeId: nodes[nodeId].ComplexEdgesId) {
            auto& edge = Graph_.GetEdge(edgeId);
            if (
                IsSubset(edge.Left, s) &&
                !Overlaps(edge.Right, x) &&
                !Overlaps(edge.Right, s) &&
                !Overlaps(edge.Right, neighs)
            ) {
                neighs[GetLowestSetBit(edge.Right)] = 1;
            }
        }
    }

    neighs &= ~x;
    return neighs;
}

template<>
inline std::bitset<64> TDPHypSolverBase<std::bitset<64>, TDPHypSolverShuffleElimination<std::bitset<64>>>::NextBitset(const std::bitset<64>& prev, const std::bitset<64>& final) {
    return std::bitset<64>((prev | ~final).to_ulong() + 1) & final;
}

template<>
inline std::bitset<64> TDPHypSolverBase<std::bitset<64>, TDPHypSolverClassic<std::bitset<64>>>::NextBitset(const std::bitset<64>& prev, const std::bitset<64>& final) {
    return std::bitset<64>((prev | ~final).to_ulong() + 1) & final;
}

template<typename TNodeSet, typename TDerived> TNodeSet TDPHypSolverBase<TNodeSet, TDerived>::NextBitset(const TNodeSet& prev, const TNodeSet& final) {
    if (prev == final) {
        return final;
    }

    TNodeSet res = prev;

    bool carry = true;
    for (size_t i = 0; i < NNodes_; i++)
    {
        if (!carry) {
            break;
        }

        if (!final[i]) {
            continue;
        }

        if (res[i] == 1 && carry) {
            res[i] = 0;
        } else if (res[i] == 0 && carry) {
            res[i] = 1;
            carry = false;
        }
    }

    return res;
}

template<typename TNodeSet,  typename TDerived> std::shared_ptr<TJoinOptimizerNodeInternal> TDPHypSolverBase<TNodeSet, TDerived>::Solve(const TOptimizerHints& hints) {
    for (auto& h : hints.CardinalityHints->Hints) {
        TNodeSet hintSet = Graph_.GetNodesByRelNames(h.JoinLabels);
        CardHintsTable_[hintSet] = &h;
    }

    for (auto& h : hints.JoinAlgoHints->Hints) {
        TNodeSet hintSet = Graph_.GetNodesByRelNames(h.JoinLabels);
        JoinAlgoHintsTable_[hintSet] = &h;
    }

    for (auto& h : hints.BytesHints->Hints) {
        TNodeSet hintSet = Graph_.GetNodesByRelNames(h.JoinLabels);
        BytesHintsTable_[hintSet] = &h;
    }

    auto& nodes = Graph_.GetNodes();
    for (int i = NNodes_ - 1; i >= 0; --i) {
        TNodeSet s{};
        s[i] = 1;

        Derived.InitDpEntry(s, nodes[i].RelationOptimizerNode);

        if (CardHintsTable_.contains(s)){
            double& nRows = nodes[i].RelationOptimizerNode->Stats.Nrows;
            nRows = CardHintsTable_.at(s)->ApplyHint(nRows);
        }

        if (BytesHintsTable_.contains(s)){
            double& nBytes = nodes[i].RelationOptimizerNode->Stats.ByteSize;
            nBytes = BytesHintsTable_.at(s)->ApplyHint(nBytes);
        }
    }

    for (int i = NNodes_ - 1; i >= 0; --i) {
        TNodeSet s{};
        s[i] = 1;
        EmitCsg(s);
        auto bi = MakeBiMin(s);
        EnumerateCsgRec(s, bi);
    }

    TNodeSet allNodes{};
    for (size_t i = 0; i < NNodes_; ++i) {
        allNodes[i] = 1;
    }

    auto minCostTree = Derived.GetLowestCostTree(allNodes);
    return std::static_pointer_cast<TJoinOptimizerNodeInternal>(minCostTree);
}

/*
 * Enumerates connected subgraphs
 * First it emits CSGs that are created by adding neighbors of S to S
 * Then it recurses on the S fused with its neighbors.
 */
template <typename TNodeSet, typename TDerived> void TDPHypSolverBase<TNodeSet, TDerived>::EnumerateCsgRec(const TNodeSet& s1, const TNodeSet& x) {
    TNodeSet neighs =  Neighs(s1, x);

    if (neighs == TNodeSet{}) {
        return;
    }

    TNodeSet prev{};
    TNodeSet next{};

    while (true) {
        next = NextBitset(prev, neighs);

        if (Derived.DpTable_.contains(s1 | next)) {
            EmitCsg(s1 | next);
        }

        if (next == neighs) {
            break;
        }
        prev = next;
    }

    prev.reset();
    while (true) {
        next = NextBitset(prev, neighs);

        EnumerateCsgRec(s1 | next, x | neighs);

        if (next == neighs) {
            break;

        }

        prev = next;
    }
}

/*
 * EmitCsg emits Connected SubGraphs
 * First it iterates through neighbors of the initial set S and emits pairs
 * (S,S2), where S2 is the neighbor of S. Then it recursively emits complement pairs
 */
template <typename TNodeSet, typename TDerived> void TDPHypSolverBase<TNodeSet, TDerived>::EmitCsg(const TNodeSet& s1) {
    TNodeSet x = s1 | MakeBiMin(s1);
    TNodeSet neighs = Neighs(s1, x);

    if (neighs == TNodeSet{}) {
        return;
    }

    for (int i = NNodes_ - 1; i >= 0; i--) {
        if (neighs[i]) {
            TNodeSet s2{};
            s2[i] = 1;

            if (auto* edge = Graph_.FindEdgeBetween(s1, s2)) {
                Derived.EmitCsgCmp(s1, s2, edge, &Graph_.GetEdge(edge->ReversedEdgeId));
            }

            EnumerateCmpRec(s1, s2, x | MakeB(neighs, GetLowestSetBit(s2)));
        }
    }
}

/*
 * Enumerates complement pairs
 * First it emits the pairs (S1,S2+next) where S2+next is the set of relation sets
 * that are obtained by adding S2's neighbors to itself
 * Then it recusrses into pairs (S1,S2+next)
 */
template <typename TNodeSet, typename TDerived> void TDPHypSolverBase<TNodeSet, TDerived>::EnumerateCmpRec(const TNodeSet& s1, const TNodeSet& s2, const TNodeSet& x) {
    TNodeSet neighs = Neighs(s2, x);

    if (neighs == TNodeSet{}) {
        return;
    }

    TNodeSet prev{};
    TNodeSet next{};

    while (true) {
        next = NextBitset(prev, neighs);

        if (Derived.DpTable_.contains(s2 | next)) {
            if (auto* edge = Graph_.FindEdgeBetween(s1, s2 | next)) {
                Derived.EmitCsgCmp(s1, s2 | next, edge, &Graph_.GetEdge(edge->ReversedEdgeId));
            }
        }

        if (next == neighs) {
            break;
        }

        prev = next;
    }

    prev.reset();
    while (true) {
        next = NextBitset(prev, neighs);

        EnumerateCmpRec(s1, s2 | next, x | neighs);

        if (next == neighs) {
            break;
        }

        prev = next;
    }
}

template <typename TNodeSet, typename TDerived> TNodeSet TDPHypSolverBase<TNodeSet, TDerived>::MakeBiMin(const TNodeSet& s) {
    TNodeSet res{};

    for (size_t i = 0; i < NNodes_; i++) {
        if (s[i]) {
            for (size_t j = 0; j <= i; j++) {
                res[j] = 1;
            }
            break;
        }
    }
    return res;
}

template <typename TNodeSet, typename TDerived> TNodeSet TDPHypSolverBase<TNodeSet, TDerived>::MakeB(const TNodeSet& s, size_t v) {
    TNodeSet res{};

    for (size_t i = 0; i < NNodes_; i++) {
        if (s[i] && i <= v) {
            res[i] = 1;
        }
    }

    return res;
}

template <typename TNodeSet> TBestJoin TDPHypSolverShuffleElimination<TNodeSet>::PickBestJoin(
    const std::shared_ptr<IBaseOptimizerNode>& left,
    const std::shared_ptr<IBaseOptimizerNode>& right,
    const TJoinHypergraph<TNodeSet>::TEdge& edge,
    bool shuffleLeftSide,
    bool shuffleRightSide,
    TCardinalityHints::TCardinalityHint* maybeCardHint,
    TJoinAlgoHints::TJoinAlgoHint* maybeJoinAlgoHint,
    TCardinalityHints::TCardinalityHint* maybeBytesHint
) {
    if (maybeJoinAlgoHint) {
        auto stats = this->Pctx_.ComputeJoinStatsV2(left->Stats, right->Stats, edge.LeftJoinKeys, edge.RightJoinKeys, maybeJoinAlgoHint->Algo, edge.JoinKind, maybeCardHint, shuffleLeftSide, shuffleRightSide, maybeBytesHint);
        if (!edge.IsCommutative) {
            return {.Stats = std::move(stats), .Algo = maybeJoinAlgoHint->Algo, .IsReversed = false};
        }

        auto reversedStats = this->Pctx_.ComputeJoinStatsV2(right->Stats, left->Stats,  edge.RightJoinKeys, edge.LeftJoinKeys, maybeJoinAlgoHint->Algo, edge.JoinKind, maybeCardHint, shuffleRightSide, shuffleLeftSide, maybeBytesHint);
        if (stats.Cost < reversedStats.Cost) {
            return {.Stats = std::move(stats), .Algo = maybeJoinAlgoHint->Algo, .IsReversed = false};
        } else {
            return {.Stats = std::move(reversedStats), .Algo = maybeJoinAlgoHint->Algo, .IsReversed = true};
        }
    }

    if (shuffleLeftSide || shuffleRightSide) { // we don't have rules to put shuffles into not grace join yet.
        auto stats = this->Pctx_.ComputeJoinStatsV2(left->Stats, right->Stats, edge.LeftJoinKeys, edge.RightJoinKeys, EJoinAlgoType::GraceJoin, edge.JoinKind, maybeCardHint, shuffleLeftSide, shuffleRightSide, maybeBytesHint);
        return TBestJoin {
            .Stats = std::move(stats),
            .Algo = EJoinAlgoType::GraceJoin,
            .IsReversed = false
        };
    }

    TOptimizerStatistics bestJoinStats;
    double bestCost = std::numeric_limits<double>::max();
    EJoinAlgoType bestAlgo = EJoinAlgoType::Undefined;
    bool bestJoinIsReversed = false;

    for (auto joinAlgo : AllJoinAlgos) {
        if (this->Pctx_.IsJoinApplicable(left, right, edge.LeftJoinKeys, edge.RightJoinKeys, joinAlgo, edge.JoinKind)){
            auto stats = this->Pctx_.ComputeJoinStatsV2(left->Stats, right->Stats, edge.LeftJoinKeys, edge.RightJoinKeys, joinAlgo, edge.JoinKind, maybeCardHint, shuffleLeftSide, shuffleRightSide, maybeBytesHint);
            if (stats.Cost < bestCost) {
                bestCost = stats.Cost;
                bestAlgo = joinAlgo;
                bestJoinStats = std::move(stats);
                bestJoinIsReversed = false;
            }
        }


        if (edge.IsCommutative) {
            if (this->Pctx_.IsJoinApplicable(right, left, edge.RightJoinKeys, edge.LeftJoinKeys, joinAlgo, edge.JoinKind)){
                auto stats = this->Pctx_.ComputeJoinStatsV2(right->Stats, left->Stats,  edge.RightJoinKeys, edge.LeftJoinKeys, joinAlgo, edge.JoinKind, maybeCardHint, shuffleRightSide, shuffleLeftSide, maybeBytesHint);
                if (stats.Cost < bestCost) {
                    bestCost = stats.Cost;
                    bestAlgo = joinAlgo;
                    bestJoinStats = std::move(stats);
                    bestJoinIsReversed = true;
                }
            }
        }
    }

    Y_ENSURE(bestAlgo != EJoinAlgoType::Undefined, "No join was chosen!");

    return {.Stats = std::move(bestJoinStats), .Algo = bestAlgo, .IsReversed = bestJoinIsReversed };
}

template <typename TNodeSet> std::shared_ptr<IBaseOptimizerNode> TDPHypSolverShuffleElimination<TNodeSet>::PickBestJoinBothSidesShuffled(
    const std::shared_ptr<IBaseOptimizerNode>& left,
    const std::shared_ptr<IBaseOptimizerNode>& right,
    const TJoinHypergraph<TNodeSet>::TEdge& edge,
    TCardinalityHints::TCardinalityHint* maybeCardHint,
    TJoinAlgoHints::TJoinAlgoHint* maybeJoinAlgoHint,
    TCardinalityHints::TCardinalityHint* maybeBytesHint
) {
    auto bestJoin = PickBestJoin(left, right, edge, false, false, maybeCardHint, maybeJoinAlgoHint, maybeBytesHint);

    if (!bestJoin.IsReversed) {
        auto tree = MakeJoinInternal(std::move(bestJoin.Stats), left, right, edge.LeftJoinKeys, edge.RightJoinKeys, edge.JoinKind, bestJoin.Algo, edge.LeftAny, edge.RightAny, left->Stats.LogicalOrderings);
        tree->Stats.LogicalOrderings.InduceNewOrderings(edge.FDs | right->Stats.LogicalOrderings.GetFDs());
        return tree;
    } else {
        auto tree = MakeJoinInternal(std::move(bestJoin.Stats), right, left, edge.RightJoinKeys, edge.LeftJoinKeys, edge.JoinKind, bestJoin.Algo, edge.RightAny, edge.LeftAny, right->Stats.LogicalOrderings);
        tree->Stats.LogicalOrderings.InduceNewOrderings(edge.FDs | left->Stats.LogicalOrderings.GetFDs());
        return tree;
    }
}

template <typename TNodeSet> std::array<std::shared_ptr<IBaseOptimizerNode>, 2> TDPHypSolverShuffleElimination<TNodeSet>::PickBestJoinRightSideShuffled(
    const std::shared_ptr<IBaseOptimizerNode>& left,
    const std::shared_ptr<IBaseOptimizerNode>& right,
    const TJoinHypergraph<TNodeSet>::TEdge& edge,
    TCardinalityHints::TCardinalityHint* maybeCardHint,
    TJoinAlgoHints::TJoinAlgoHint* maybeAlgoHint,
    TCardinalityHints::TCardinalityHint* maybeBytesHint
) {
    std::array<std::shared_ptr<IBaseOptimizerNode>, 2> trees = { nullptr, nullptr };
    std::size_t treeCount = 0;

    if ((this->Pctx_.IsJoinApplicable(left, right, edge.LeftJoinKeys, edge.RightJoinKeys, EJoinAlgoType::MapJoin, edge.JoinKind) && !maybeAlgoHint) || (maybeAlgoHint && maybeAlgoHint->Algo == EJoinAlgoType::MapJoin)) {
        auto stats = this->Pctx_.ComputeJoinStatsV2(left->Stats, right->Stats, edge.LeftJoinKeys, edge.RightJoinKeys, EJoinAlgoType::MapJoin, edge.JoinKind, maybeCardHint, false, false, maybeBytesHint);
        auto tree = MakeJoinInternal(std::move(stats), left, right, edge.LeftJoinKeys, edge.RightJoinKeys, edge.JoinKind, EJoinAlgoType::MapJoin, edge.LeftAny, edge.RightAny, left->Stats.LogicalOrderings);
        tree->Stats.LogicalOrderings.InduceNewOrderings(edge.FDs | right->Stats.LogicalOrderings.GetFDs());
        trees[treeCount++] = std::move(tree);
    }

    TOptimizerStatistics reversedMapJoinStatistics;
    reversedMapJoinStatistics.Cost = std::numeric_limits<double>::max();
    if ((edge.IsCommutative && this->Pctx_.IsJoinApplicable(right, left, edge.RightJoinKeys, edge.LeftJoinKeys, EJoinAlgoType::MapJoin, edge.JoinKind) && !maybeAlgoHint) || (edge.IsCommutative && maybeAlgoHint && maybeAlgoHint->Algo == EJoinAlgoType::MapJoin)) {
        reversedMapJoinStatistics = this->Pctx_.ComputeJoinStatsV2(right->Stats, left->Stats, edge.RightJoinKeys, edge.LeftJoinKeys, EJoinAlgoType::MapJoin, edge.JoinKind, maybeCardHint, false, false, maybeBytesHint);
    }

    std::shared_ptr<TJoinOptimizerNodeInternal> tree;
    auto shuffleLeftSideBestJoin = PickBestJoin(left, right, edge, true, false, maybeCardHint, maybeAlgoHint, maybeBytesHint);
    if (reversedMapJoinStatistics.Cost <= shuffleLeftSideBestJoin.Stats.Cost) {
        tree = MakeJoinInternal(std::move(reversedMapJoinStatistics), right, left, edge.RightJoinKeys, edge.LeftJoinKeys, edge.JoinKind, EJoinAlgoType::MapJoin, edge.RightAny, edge.LeftAny, right->Stats.LogicalOrderings);
        tree->Stats.LogicalOrderings.InduceNewOrderings(edge.FDs | left->Stats.LogicalOrderings.GetFDs());
    } else {
        if (!shuffleLeftSideBestJoin.IsReversed) {
            tree = MakeJoinInternal(std::move(shuffleLeftSideBestJoin.Stats), left, right, edge.LeftJoinKeys, edge.RightJoinKeys, edge.JoinKind, shuffleLeftSideBestJoin.Algo, edge.LeftAny, edge.RightAny, right->Stats.LogicalOrderings);
            tree->Stats.LogicalOrderings.InduceNewOrderings(edge.FDs | left->Stats.LogicalOrderings.GetFDs());
            tree->ShuffleLeftSideByOrderingIdx = edge.LeftJoinKeysShuffleOrderingIdx;
        } else {
            tree = MakeJoinInternal(std::move(shuffleLeftSideBestJoin.Stats), right, left, edge.RightJoinKeys, edge.LeftJoinKeys, edge.JoinKind, shuffleLeftSideBestJoin.Algo, edge.RightAny, edge.LeftAny, left->Stats.LogicalOrderings);
            tree->Stats.LogicalOrderings.InduceNewOrderings(edge.FDs | right->Stats.LogicalOrderings.GetFDs());
            tree->ShuffleRightSideByOrderingIdx = edge.LeftJoinKeysShuffleOrderingIdx;
        }
    }
    trees[treeCount++] = std::move(tree);

    return trees;
}

template <typename TNodeSet> std::array<std::shared_ptr<IBaseOptimizerNode>, 2> TDPHypSolverShuffleElimination<TNodeSet>::PickBestJoinLeftSideShuffled(
    const std::shared_ptr<IBaseOptimizerNode>& left,
    const std::shared_ptr<IBaseOptimizerNode>& right,
    const TJoinHypergraph<TNodeSet>::TEdge& edge,
    TCardinalityHints::TCardinalityHint* maybeCardHint,
    TJoinAlgoHints::TJoinAlgoHint* maybeAlgoHint,
    TCardinalityHints::TCardinalityHint* maybeBytesHint
) {
    std::array<std::shared_ptr<IBaseOptimizerNode>, 2> trees = { nullptr, nullptr };
    std::size_t treeCount = 0;

    if ((edge.IsCommutative && this->Pctx_.IsJoinApplicable(right, left, edge.RightJoinKeys, edge.LeftJoinKeys, EJoinAlgoType::MapJoin, edge.JoinKind) && !maybeAlgoHint) || (edge.IsCommutative && maybeAlgoHint && maybeAlgoHint->Algo == EJoinAlgoType::MapJoin)) {
        auto stats = this->Pctx_.ComputeJoinStatsV2(right->Stats, left->Stats, edge.RightJoinKeys, edge.LeftJoinKeys, EJoinAlgoType::MapJoin, edge.JoinKind, maybeCardHint, false, false, maybeBytesHint);
        auto tree = MakeJoinInternal(std::move(stats), right, left, edge.RightJoinKeys, edge.LeftJoinKeys, edge.JoinKind, EJoinAlgoType::MapJoin, edge.RightAny, edge.LeftAny, right->Stats.LogicalOrderings);
        tree->Stats.LogicalOrderings.InduceNewOrderings(edge.FDs | left->Stats.LogicalOrderings.GetFDs());
        trees[treeCount++] = std::move(tree);
    }

    TOptimizerStatistics mapJoinStatistics;
    mapJoinStatistics.Cost = std::numeric_limits<double>::max();
    if ((this->Pctx_.IsJoinApplicable(left, right, edge.LeftJoinKeys, edge.RightJoinKeys, EJoinAlgoType::MapJoin, edge.JoinKind) && !maybeAlgoHint) || (maybeAlgoHint && maybeAlgoHint->Algo == EJoinAlgoType::MapJoin)) {
        mapJoinStatistics = this->Pctx_.ComputeJoinStatsV2(left->Stats, right->Stats, edge.LeftJoinKeys, edge.RightJoinKeys, EJoinAlgoType::MapJoin, edge.JoinKind, maybeCardHint, false, false, maybeBytesHint);
    }

    std::shared_ptr<TJoinOptimizerNodeInternal> tree;
    auto shuffleRightSideBestJoin = PickBestJoin(left, right, edge, false, true, maybeCardHint, maybeAlgoHint, maybeBytesHint);
    if (mapJoinStatistics.Cost <= shuffleRightSideBestJoin.Stats.Cost) {
        tree = MakeJoinInternal(std::move(mapJoinStatistics), left, right, edge.LeftJoinKeys, edge.RightJoinKeys, edge.JoinKind, EJoinAlgoType::MapJoin, edge.LeftAny, edge.RightAny, left->Stats.LogicalOrderings);
        tree->Stats.LogicalOrderings.InduceNewOrderings(edge.FDs | left->Stats.LogicalOrderings.GetFDs());
    } else {
        if (!shuffleRightSideBestJoin.IsReversed) {
            tree = MakeJoinInternal(std::move(shuffleRightSideBestJoin.Stats), left, right, edge.LeftJoinKeys, edge.RightJoinKeys, edge.JoinKind, shuffleRightSideBestJoin.Algo, edge.LeftAny, edge.RightAny, left->Stats.LogicalOrderings);
            tree->Stats.LogicalOrderings.InduceNewOrderings(edge.FDs | right->Stats.LogicalOrderings.GetFDs());
            tree->ShuffleRightSideByOrderingIdx = edge.RightJoinKeysShuffleOrderingIdx;
        } else {
            tree = MakeJoinInternal(std::move(shuffleRightSideBestJoin.Stats), right, left, edge.RightJoinKeys, edge.LeftJoinKeys, edge.JoinKind, shuffleRightSideBestJoin.Algo, edge.RightAny, edge.LeftAny, right->Stats.LogicalOrderings);
            tree->Stats.LogicalOrderings.InduceNewOrderings(edge.FDs | left->Stats.LogicalOrderings.GetFDs());
            tree->ShuffleLeftSideByOrderingIdx = edge.RightJoinKeysShuffleOrderingIdx;
        }
    }
    trees[treeCount++] = std::move(tree);

    return trees;
}

template <typename TNodeSet> std::array<std::shared_ptr<IBaseOptimizerNode>, 3> TDPHypSolverShuffleElimination<TNodeSet>::PickBestJoinNoSidesShuffled(
    const std::shared_ptr<IBaseOptimizerNode>& left,
    const std::shared_ptr<IBaseOptimizerNode>& right,
    const TJoinHypergraph<TNodeSet>::TEdge& edge,
    TCardinalityHints::TCardinalityHint* maybeCardHint,
    TJoinAlgoHints::TJoinAlgoHint* maybeAlgoHint,
    TCardinalityHints::TCardinalityHint* maybeBytesHint
) {
    std::array<std::shared_ptr<IBaseOptimizerNode>, 3> trees = { nullptr, nullptr, nullptr };
    std::size_t treeCount = 0;

    if ((this->Pctx_.IsJoinApplicable(left, right, edge.LeftJoinKeys, edge.RightJoinKeys, EJoinAlgoType::MapJoin, edge.JoinKind) && !maybeAlgoHint) || (maybeAlgoHint && maybeAlgoHint->Algo == EJoinAlgoType::MapJoin)) {
        auto stats = this->Pctx_.ComputeJoinStatsV2(left->Stats, right->Stats, edge.LeftJoinKeys, edge.RightJoinKeys, EJoinAlgoType::MapJoin, edge.JoinKind, maybeCardHint, false, false, maybeBytesHint);
        auto tree = MakeJoinInternal(std::move(stats), left, right, edge.LeftJoinKeys, edge.RightJoinKeys, edge.JoinKind, EJoinAlgoType::MapJoin, edge.LeftAny, edge.RightAny, left->Stats.LogicalOrderings);
        tree->Stats.LogicalOrderings.InduceNewOrderings(edge.FDs | right->Stats.LogicalOrderings.GetFDs());
        trees[treeCount++] = std::move(tree);
    }


    if ((edge.IsCommutative && this->Pctx_.IsJoinApplicable(right, left, edge.RightJoinKeys, edge.LeftJoinKeys, EJoinAlgoType::MapJoin, edge.JoinKind) && !maybeAlgoHint) || (edge.IsCommutative && maybeAlgoHint && maybeAlgoHint->Algo == EJoinAlgoType::MapJoin)) {
        auto stats = this->Pctx_.ComputeJoinStatsV2(right->Stats, left->Stats, edge.RightJoinKeys, edge.LeftJoinKeys, EJoinAlgoType::MapJoin, edge.JoinKind, maybeCardHint, false, false, maybeBytesHint);
        auto tree = MakeJoinInternal(std::move(stats), right, left, edge.RightJoinKeys, edge.LeftJoinKeys, edge.JoinKind, EJoinAlgoType::MapJoin, edge.RightAny, edge.LeftAny, right->Stats.LogicalOrderings);
        tree->Stats.LogicalOrderings.InduceNewOrderings(edge.FDs | left->Stats.LogicalOrderings.GetFDs());
        trees[treeCount++] = std::move(tree);
    }

    enum EMinCostTree {
        EShuffleLeftSideAndMapJoin,
        EShuffleRightSideAndReversedMapJoin,
        EShuffleBothSides
    };
    EMinCostTree minCostTree;
    double minCost = std::numeric_limits<double>::max();

    // V Now we don't support shuffling sides not of the GraceJoin

    // TOptimizerStatistics shuffleLeftSideAndMapJoinStats;
    // shuffleLeftSideAndMapJoinStats.Cost = std::numeric_limits<double>::max();
    // if (this->Pctx_.IsJoinApplicable(left, right, edge.LeftJoinKeys, edge.RightJoinKeys, EJoinAlgoType::MapJoin, edge.JoinKind)) {
    //     shuffleLeftSideAndMapJoinStats = this->Pctx_.ComputeJoinStatsV1(left->Stats, right->Stats, edge.LeftJoinKeys, edge.RightJoinKeys, EJoinAlgoType::MapJoin, edge.JoinKind, maybeCardHint, true, false);
    //     minCost = shuffleLeftSideAndMapJoinStats.Cost;
    //     minCostTree = EShuffleBothSides;
    // }

    // TOptimizerStatistics shuffleRightSideAndReversedMapJoinStats;
    // shuffleRightSideAndReversedMapJoinStats.Cost = std::numeric_limits<double>::max();
    // if ((edge.IsCommutative && this->Pctx_.IsJoinApplicable(left, right, edge.LeftJoinKeys, edge.RightJoinKeys, EJoinAlgoType::MapJoin, edge.JoinKind) && !maybeAlgoHint) || (maybeAlgoHint && maybeAlgoHint->Algo == EJoinAlgoType::MapJoin)) {
    //     shuffleRightSideAndReversedMapJoinStats = this->Pctx_.ComputeJoinStatsV1(right->Stats, left->Stats, edge.RightJoinKeys, edge.LeftJoinKeys, EJoinAlgoType::MapJoin, edge.JoinKind, maybeCardHint, false, true);
    //     if (shuffleRightSideAndReversedMapJoinStats.Cost < minCost) {
    //         minCost = shuffleRightSideAndReversedMapJoinStats.Cost;
    //         minCostTree = EShuffleRightSideAndReversedMapJoin;
    //     }
    // }

    TBestJoin shuffleBothSidesBestJoin = PickBestJoin(left, right, edge, true, true, maybeCardHint, maybeAlgoHint, maybeBytesHint);
    if (shuffleBothSidesBestJoin.Stats.Cost < minCost) {
        minCost = shuffleBothSidesBestJoin.Stats.Cost;
        minCostTree = EShuffleBothSides;
    }

    std::shared_ptr<TJoinOptimizerNodeInternal> tree;
    // switch (minCostTree) {
        // case EMinCostTree::EShuffleLeftSideAndMapJoin: {
        //     tree = MakeJoinInternal(std::move(shuffleLeftSideAndMapJoinStats), left, right, edge.LeftJoinKeys, edge.RightJoinKeys, edge.JoinKind, EJoinAlgoType::MapJoin, edge.LeftAny, edge.RightAny, OrderingsFSM.CreateState());
        //     tree->Stats.LogicalOrderings.SetOrdering(edge.LeftJoinKeysShuffleOrderingIdx);
        //     tree->Stats.LogicalOrderings.InduceNewOrderings(edge.FDs | left->Stats.LogicalOrderings.GetFDs() | right->Stats.LogicalOrderings.GetFDs());
        //     tree->ShuffleLeftSideByOrderingIdx = edge.LeftJoinKeysShuffleOrderingIdx;
        //     break;
        // }
        // case EMinCostTree::EShuffleRightSideAndReversedMapJoin: {
        //     tree = MakeJoinInternal(std::move(shuffleRightSideAndReversedMapJoinStats), right, left, edge.RightJoinKeys, edge.LeftJoinKeys, edge.JoinKind, EJoinAlgoType::MapJoin, edge.RightAny, edge.LeftAny, OrderingsFSM.CreateState());
        //     tree->Stats.LogicalOrderings.SetOrdering(edge.RightJoinKeysShuffleOrderingIdx);
        //     tree->Stats.LogicalOrderings.InduceNewOrderings(edge.FDs | left->Stats.LogicalOrderings.GetFDs() | right->Stats.LogicalOrderings.GetFDs());
        //     tree->ShuffleLeftSideByOrderingIdx = edge.RightJoinKeysShuffleOrderingIdx;
        //     break;
        // }
        // case EMinCostTree::EShuffleBothSides: {
            if (!shuffleBothSidesBestJoin.IsReversed) {
                tree = MakeJoinInternal(std::move(shuffleBothSidesBestJoin.Stats), left, right, edge.LeftJoinKeys, edge.RightJoinKeys, edge.JoinKind, shuffleBothSidesBestJoin.Algo, edge.LeftAny, edge.RightAny, OrderingsFSM.CreateState());
                tree->Stats.LogicalOrderings.SetOrdering(edge.LeftJoinKeysShuffleOrderingIdx);
                tree->Stats.LogicalOrderings.InduceNewOrderings(edge.FDs | left->Stats.LogicalOrderings.GetFDs() | right->Stats.LogicalOrderings.GetFDs());
                tree->ShuffleLeftSideByOrderingIdx = edge.LeftJoinKeysShuffleOrderingIdx;
                tree->ShuffleRightSideByOrderingIdx = edge.RightJoinKeysShuffleOrderingIdx;
            } else {
                tree = MakeJoinInternal(std::move(shuffleBothSidesBestJoin.Stats), right, left, edge.RightJoinKeys, edge.LeftJoinKeys, edge.JoinKind, shuffleBothSidesBestJoin.Algo, edge.RightAny, edge.LeftAny, OrderingsFSM.CreateState());
                tree->Stats.LogicalOrderings.SetOrdering(edge.LeftJoinKeysShuffleOrderingIdx);
                tree->Stats.LogicalOrderings.InduceNewOrderings(edge.FDs | left->Stats.LogicalOrderings.GetFDs() | right->Stats.LogicalOrderings.GetFDs());
                tree->ShuffleLeftSideByOrderingIdx = edge.RightJoinKeysShuffleOrderingIdx;
                tree->ShuffleRightSideByOrderingIdx = edge.LeftJoinKeysShuffleOrderingIdx;
            }
            // break;
        // }
    // }

    trees[treeCount++] = std::move(tree);
    return trees;
}


template <typename TNodeSet>  std::array<std::shared_ptr<IBaseOptimizerNode>, 2> TDPHypSolverShuffleElimination<TNodeSet>::PickCrossJoinTrees(
    const std::shared_ptr<IBaseOptimizerNode>& left,
    const std::shared_ptr<IBaseOptimizerNode>& right,
    const TJoinHypergraph<TNodeSet>::TEdge& edge,
    TCardinalityHints::TCardinalityHint* maybeCardHint,
    TCardinalityHints::TCardinalityHint* maybeBytesHint
) {
    std::array<std::shared_ptr<IBaseOptimizerNode>, 2> trees = { nullptr, nullptr };

    auto stats = this->Pctx_.ComputeJoinStatsV2(left->Stats, right->Stats, edge.LeftJoinKeys, edge.RightJoinKeys, EJoinAlgoType::Undefined, edge.JoinKind, maybeCardHint, false, false, maybeBytesHint);
    trees[0] = MakeJoinInternal(std::move(stats), left, right, edge.LeftJoinKeys, edge.RightJoinKeys, edge.JoinKind, EJoinAlgoType::Undefined, edge.LeftAny, edge.RightAny, left->Stats.LogicalOrderings);

    auto reversedStats = this->Pctx_.ComputeJoinStatsV2(right->Stats, left->Stats, edge.RightJoinKeys, edge.LeftJoinKeys, EJoinAlgoType::Undefined, edge.JoinKind, maybeCardHint, false, false, maybeBytesHint);
    trees[1] = MakeJoinInternal(std::move(reversedStats), right, left, edge.RightJoinKeys, edge.LeftJoinKeys, edge.JoinKind, EJoinAlgoType::Undefined, edge.RightAny, edge.LeftAny, right->Stats.LogicalOrderings);

    return trees;
}

inline void AddNodeToDpTableEntries(
    std::shared_ptr<IBaseOptimizerNode>&& node,
    std::vector<std::shared_ptr<IBaseOptimizerNode>>& dpTableEntries
) {
    bool wasFound = false;

    for (auto& entry: dpTableEntries) {
        if (entry->Stats.LogicalOrderings.IsSubsetOf(node->Stats.LogicalOrderings)) {
            if (node->Stats.Cost < entry->Stats.Cost) {
                entry = std::move(node);
                return;
            }

            wasFound = true;
        }
    }

    if (wasFound) { return; }
    dpTableEntries.push_back(std::move(node));
}

/*
 * Emit a single CSG + CMP pair
 */
template<typename TNodeSet> void TDPHypSolverShuffleElimination<TNodeSet>::EmitCsgCmp(
    const TNodeSet& s1,
    const TNodeSet& s2,
    const typename TJoinHypergraph<TNodeSet>::TEdge* csgCmpEdge,
    const typename TJoinHypergraph<TNodeSet>::TEdge* reversedCsgCmpEdge
) {
    // Here we actually build the join and choose and compare the
    // new plan to what's in the dpTable, if it there

    Y_ENSURE(DpTable_.contains(s1), "DP Table does not contain S1");
    Y_ENSURE(DpTable_.contains(s2), "DP Table does not conaint S2");

    const auto* leftNodes =  &DpTable_[s1];
    const auto* rightNodes = &DpTable_[s2];

    if (csgCmpEdge->IsReversed) {
        std::swap(csgCmpEdge, reversedCsgCmpEdge);
        std::swap(leftNodes, rightNodes);
    }

    TNodeSet joined = s1 | s2;

    auto maybeCardHint = this->CardHintsTable_.contains(joined) ? this->CardHintsTable_[joined] : nullptr;
    auto maybeBytesHint = this->BytesHintsTable_.contains(joined) ? this->BytesHintsTable_[joined] : nullptr;
    auto maybeJoinAlgoHint = this->JoinAlgoHintsTable_.contains(joined) ? this->JoinAlgoHintsTable_[joined] : nullptr;

    for (const auto& leftNode: *leftNodes) {
        for (const auto& rightNode: *rightNodes) {
            if (csgCmpEdge->JoinKind == EJoinKind::Cross) {
                for (auto&& tree: PickCrossJoinTrees(leftNode, rightNode, *csgCmpEdge, maybeCardHint, maybeBytesHint)) {
                    AddNodeToDpTableEntries(std::move(tree), this->DpTable_[joined]);
                }
                continue;
            }

            i64 lhsHashFuncArgCnt = leftNode->Stats.LogicalOrderings.GetShuffleHashFuncArgsCount();
            i64 rhsHashFuncArgCnt = rightNode->Stats.LogicalOrderings.GetShuffleHashFuncArgsCount();

            bool lhsShuffled =
                leftNode->Stats.LogicalOrderings.ContainsShuffle(csgCmpEdge->LeftJoinKeysShuffleOrderingIdx) &&
                lhsHashFuncArgCnt == static_cast<std::int64_t>(csgCmpEdge->LeftJoinKeys.size());
            bool rhsShuffled =
                rightNode->Stats.LogicalOrderings.ContainsShuffle(csgCmpEdge->RightJoinKeysShuffleOrderingIdx) &&
                rhsHashFuncArgCnt == static_cast<std::int64_t>(csgCmpEdge->RightJoinKeys.size());

            // TODO: we can remove shuffle from here, joinkeys.size() == getshufflehashargscount() isn't nescesary condition. GetShuffleHashFuncArgsCount must be equal, otherwise we will reshuffle.
            // bool sameHashFuncArgCount = (lhsHashFuncArgCnt == rhsHashFuncArgCnt);
            if (lhsShuffled && rhsShuffled /* we don't support not shuffling two inputs in the execution, so we must shuffle at least one*/) {
                if (leftNode->Stats.Nrows < rightNode->Stats.Nrows) {
                    lhsShuffled = false;
                } else {
                    rhsShuffled = false;
                }
            }

            // TODO: don't add shuffle, if it won't be used
            /* if (lhsShuffled && rhsShuffled) { // we don't support not shuffling two inputs in the execution
                ++bothSidesShuffled;
                auto bestJoin = PickBestJoinBothSidesShuffled(leftNode, rightNode, *csgCmpEdge, maybeCardHint, maybeJoinAlgoHint);
                AddNodeToDpTableEntries(std::move(bestJoin), DpTable_[joined]);
            }  else */ if (!lhsShuffled && !rhsShuffled) {
                auto trees = PickBestJoinNoSidesShuffled(leftNode, rightNode, *csgCmpEdge, maybeCardHint, maybeJoinAlgoHint, maybeBytesHint);
                for (std::size_t i = 0; i < trees.size() && trees[i] != nullptr; ++i) {
                    AddNodeToDpTableEntries(std::move(trees[i]), this->DpTable_[joined]);
                }
            } else if (!lhsShuffled && rhsShuffled) {
                auto trees = PickBestJoinRightSideShuffled(leftNode, rightNode, *csgCmpEdge, maybeCardHint, maybeJoinAlgoHint, maybeBytesHint);
                for (std::size_t i = 0; i < trees.size() && trees[i] != nullptr; ++i) {
                    AddNodeToDpTableEntries(std::move(trees[i]), this->DpTable_[joined]);
                }
            } else  {
                auto trees = PickBestJoinLeftSideShuffled(leftNode, rightNode, *csgCmpEdge, maybeCardHint, maybeJoinAlgoHint, maybeBytesHint);
                for (std::size_t i = 0; i < trees.size() && trees[i] != nullptr; ++i) {
                    AddNodeToDpTableEntries(std::move(trees[i]), this->DpTable_[joined]);
                }
            }
        }
    }

    #ifndef NDEBUG
        auto pair = std::make_pair(s1, s2);
        Y_ENSURE (!this->CheckTable_.contains(pair), "Check table already contains pair S1|S2");
        this->CheckTable_[pair] = true;
    #endif
}

} // namespace NYql::NDq
