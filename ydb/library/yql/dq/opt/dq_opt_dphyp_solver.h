#pragma once

#include "dq_opt_join_hypergraph.h"
#include "dq_opt_join_tree_node.h"
#include "bitset.h"

namespace NYql::NDq {

#ifndef NDEBUG
    struct pair_hash {
        template <class T1, class T2>
        std::size_t operator () (const std::pair<T1,T2> &p) const {
            auto h1 = std::hash<T1>{}(p.first);
            auto h2 = std::hash<T2>{}(p.second);

            // Mainly for demonstration purposes, i.e. works but is overly simple
            // In the real world, use sth. like boost.hash_combine
            return h1 ^ h2;
        }
    };
#endif

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
 */
template <typename TNodeSet>
class TDPHypSolver {
public:
    TDPHypSolver(
        TJoinHypergraph<TNodeSet>& graph,
        IProviderContext& ctx,
        const TCardinalityHints& hints,
        const TJoinAlgoHints& joinHints
    ) 
        : Graph_(graph) 
        , NNodes_(graph.GetNodes().size())
        , Pctx_(ctx)
    {
        for (const auto& h : hints.Hints) {
            TNodeSet hintSet = Graph_.GetNodesByRelNames(h.JoinLabels);
            CardHintsTable_[hintSet] = h;
        }
        for (const auto& h : joinHints.Hints) {
            TNodeSet hintSet = Graph_.GetNodesByRelNames(h.JoinLabels);
            JoinAlgoHintsTable_[hintSet] = h;
        }
    }

    // Run DPHyp algorithm and produce the join tree in CBO's internal representation
    std::shared_ptr<TJoinOptimizerNodeInternal> Solve();

    // Calculate the size of a dynamic programming table with a budget
    ui32 CountCC(ui32 budget);

private:
    void EnumerateCsgRec(const TNodeSet& s1, const TNodeSet& x);

    void EmitCsg(const TNodeSet& s1);

    void EnumerateCmpRec(const TNodeSet& s1, const TNodeSet& s2, const TNodeSet& x);

    void EmitCsgCmp(const TNodeSet& s1, const TNodeSet& s2, const typename TJoinHypergraph<TNodeSet>::TEdge* csgCmpEdge);

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

    // Iterate over all join algorithms and pick the best join that is applicable.
    // Also considers commuting joins
    std::shared_ptr<TJoinOptimizerNodeInternal> PickBestJoin(
        const std::shared_ptr<IBaseOptimizerNode>& left,
        const std::shared_ptr<IBaseOptimizerNode>& right,
        EJoinKind joinKind,
        bool isCommutative,
        const std::set<std::pair<TJoinColumn, TJoinColumn>>& joinConditions,
        const std::set<std::pair<TJoinColumn, TJoinColumn>>& reversedJoinConditions,
        const TVector<TString>& leftJoinKeys,
        const TVector<TString>& rightJoinKeys,
        IProviderContext& ctx,
        TCardinalityHints::TCardinalityHint* maybeHint,
        TJoinAlgoHints::TJoinAlgoHint* maybeJoinHint
    );

    // Count the size of the dynamic programming table recursively
    ui32 CountCCRec(const TNodeSet&, const TNodeSet&, ui32, ui32);

private:
    TJoinHypergraph<TNodeSet>& Graph_;
    size_t NNodes_;
    IProviderContext& Pctx_;  // Provider specific contexts?
    // FIXME: This is a temporary structure that needs to be extended to multiple providers.
    #ifndef NDEBUG
        THashMap<std::pair<TNodeSet, TNodeSet>, bool, pair_hash> CheckTable_;
    #endif
private:
    THashMap<TNodeSet, std::shared_ptr<IBaseOptimizerNode>, std::hash<TNodeSet>> DpTable_;
    THashMap<TNodeSet, TCardinalityHints::TCardinalityHint, std::hash<TNodeSet>> CardHintsTable_;
    THashMap<TNodeSet, TJoinAlgoHints::TJoinAlgoHint, std::hash<TNodeSet>> JoinAlgoHintsTable_;
};

/*
 * Count the number of items in the DP table of DPHyp
 */
template <typename TNodeSet> ui32 TDPHypSolver<TNodeSet>::CountCC(ui32 budget) {
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
template <typename TNodeSet> ui32 TDPHypSolver<TNodeSet>::CountCCRec(const TNodeSet& s, const TNodeSet& x, ui32 cost, ui32 budget) {
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

template<typename TNodeSet> TNodeSet TDPHypSolver<TNodeSet>::Neighs(TNodeSet s, TNodeSet x) {
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
inline std::bitset<64> TDPHypSolver<std::bitset<64>>::NextBitset(const std::bitset<64>& prev, const std::bitset<64>& final) {
    return std::bitset<64>((prev | ~final).to_ulong() + 1) & final;
}

template<typename TNodeSet> TNodeSet TDPHypSolver<TNodeSet>::NextBitset(const TNodeSet& prev, const TNodeSet& final) {
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

template<typename TNodeSet> std::shared_ptr<TJoinOptimizerNodeInternal> TDPHypSolver<TNodeSet>::Solve() {
    auto& nodes = Graph_.GetNodes();

    Y_ASSERT(nodes.size() == NNodes_);

    for (int i = NNodes_ - 1; i >= 0; --i) {
        TNodeSet s{};
        s[i] = 1;
        DpTable_[s] = nodes[i].RelationOptimizerNode;
        if (CardHintsTable_.contains(s)){
            DpTable_[s]->Stats->Nrows = CardHintsTable_.at(s).ApplyHint(DpTable_[s]->Stats->Nrows);
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

    Y_ASSERT(DpTable_.contains(allNodes));

    return std::static_pointer_cast<TJoinOptimizerNodeInternal>(DpTable_[allNodes]);
}

/*
 * Enumerates connected subgraphs
 * First it emits CSGs that are created by adding neighbors of S to S
 * Then it recurses on the S fused with its neighbors.
 */
template <typename TNodeSet> void TDPHypSolver<TNodeSet>::EnumerateCsgRec(const TNodeSet& s1, const TNodeSet& x) {
    TNodeSet neighs =  Neighs(s1, x);

    if (neighs == TNodeSet{}) {
        return;
    }

    TNodeSet prev{};
    TNodeSet next{};

    while (true) {
        next = NextBitset(prev, neighs);

        if (DpTable_.contains(s1 | next)) {
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
template <typename TNodeSet> void TDPHypSolver<TNodeSet>::EmitCsg(const TNodeSet& s1) {
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
                EmitCsgCmp(s1, s2, edge);
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
template <typename TNodeSet> void TDPHypSolver<TNodeSet>::EnumerateCmpRec(const TNodeSet& s1, const TNodeSet& s2, const TNodeSet& x) {
    TNodeSet neighs = Neighs(s2, x);

    if (neighs == TNodeSet{}) {
        return;
    }

    TNodeSet prev{};
    TNodeSet next{};

    while (true) {
        next = NextBitset(prev, neighs);

        if (DpTable_.contains(s2 | next)) {
            if (auto* edge = Graph_.FindEdgeBetween(s1, s2 | next)) {
                EmitCsgCmp(s1, s2 | next, edge);
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

template <typename TNodeSet> TNodeSet TDPHypSolver<TNodeSet>::MakeBiMin(const TNodeSet& s) {
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

template <typename TNodeSet> TNodeSet TDPHypSolver<TNodeSet>::MakeB(const TNodeSet& s, size_t v) {
    TNodeSet res{};

    for (size_t i = 0; i < NNodes_; i++) {
        if (s[i] && i <= v) {
            res[i] = 1;
        }
    }

    return res;
}

template <typename TNodeSet> std::shared_ptr<TJoinOptimizerNodeInternal> TDPHypSolver<TNodeSet>::PickBestJoin(
    const std::shared_ptr<IBaseOptimizerNode>& left,
    const std::shared_ptr<IBaseOptimizerNode>& right,
    EJoinKind joinKind,
    bool isCommutative,
    const std::set<std::pair<TJoinColumn, TJoinColumn>>& joinConditions,
    const std::set<std::pair<TJoinColumn, TJoinColumn>>& reversedJoinConditions,
    const TVector<TString>& leftJoinKeys,
    const TVector<TString>& rightJoinKeys,
    IProviderContext& ctx,
    TCardinalityHints::TCardinalityHint* maybeHint,
    TJoinAlgoHints::TJoinAlgoHint* maybeJoinHint
) {
    double bestCost = std::numeric_limits<double>::infinity();
    EJoinAlgoType bestAlgo{};
    bool bestJoinIsReversed = false;

    for (auto joinAlgo : AllJoinAlgos) {
        if (ctx.IsJoinApplicable(left, right, joinConditions, leftJoinKeys, rightJoinKeys, joinAlgo, joinKind)){
            auto cost = ctx.ComputeJoinStats(*left->Stats, *right->Stats, leftJoinKeys, rightJoinKeys, joinAlgo, joinKind, maybeHint).Cost;
            if (maybeJoinHint) {
                if (joinAlgo == maybeJoinHint->JoinHint) {
                    cost = -1;
                }
            }
            if (cost < bestCost) {
                bestCost = cost;
                bestAlgo = joinAlgo;
                bestJoinIsReversed = false;
            }
        }

        if (isCommutative) {
            if (ctx.IsJoinApplicable(right, left, reversedJoinConditions, rightJoinKeys, leftJoinKeys, joinAlgo, joinKind)){
                auto cost = ctx.ComputeJoinStats(*right->Stats, *left->Stats,  rightJoinKeys, leftJoinKeys, joinAlgo, joinKind, maybeHint).Cost;
                if (maybeJoinHint) {
                    if (joinAlgo == maybeJoinHint->JoinHint) {
                        cost = -1;
                    }
                }
                if (cost < bestCost) {
                    bestCost = cost;
                    bestAlgo = joinAlgo;
                    bestJoinIsReversed = true;
                }
            }
        }
    }

    Y_ENSURE(bestCost != std::numeric_limits<double>::infinity(), "No join was chosen!");

    if (bestJoinIsReversed) {
        return MakeJoinInternal(right, left, reversedJoinConditions, rightJoinKeys, leftJoinKeys, joinKind, bestAlgo, ctx, maybeHint);
    }
    
    return MakeJoinInternal(left, right, joinConditions, leftJoinKeys, rightJoinKeys, joinKind, bestAlgo, ctx, maybeHint);
}

/* 
 * Emit a single CSG + CMP pair
 */
template<typename TNodeSet> void TDPHypSolver<TNodeSet>::EmitCsgCmp(const TNodeSet& s1, const TNodeSet& s2, const typename TJoinHypergraph<TNodeSet>::TEdge* csgCmpEdge) {
    // Here we actually build the join and choose and compare the
    // new plan to what's in the dpTable, if it there

    Y_ENSURE(DpTable_.contains(s1), "DP Table does not contain S1");
    Y_ENSURE(DpTable_.contains(s2), "DP Table does not conaint S2");

    const auto* reversedEdge = &Graph_.GetEdge(csgCmpEdge->ReversedEdgeId);
    auto leftNodes = DpTable_[s1];
    auto rightNodes = DpTable_[s2];
    
    if (csgCmpEdge->IsReversed) {
        std::swap(csgCmpEdge, reversedEdge);
        std::swap(leftNodes, rightNodes);
    }

    TNodeSet joined = s1 | s2;

    auto maybeCardHint = CardHintsTable_.contains(joined) ? & CardHintsTable_[joined] : nullptr;
    auto maybeJoinAlgoHint = JoinAlgoHintsTable_.contains(joined) ? & JoinAlgoHintsTable_[joined] : nullptr;

    auto bestJoin = PickBestJoin(
        leftNodes,
        rightNodes,
        csgCmpEdge->JoinKind,
        csgCmpEdge->IsCommutative,
        csgCmpEdge->JoinConditions,
        reversedEdge->JoinConditions,
        csgCmpEdge->LeftJoinKeys,
        csgCmpEdge->RightJoinKeys,
        Pctx_,
        maybeCardHint,
        maybeJoinAlgoHint
    );

    if (!DpTable_.contains(joined) || bestJoin->Stats->Cost < DpTable_[joined]->Stats->Cost) {
        DpTable_[joined] = bestJoin;
    }

    #ifndef NDEBUG
        auto pair = std::make_pair(s1, s2);
        Y_ENSURE (!CheckTable_.contains(pair), "Check table already contains pair S1|S2");
        CheckTable_[ std::pair<TNodeSet,TNodeSet>(s1, s2) ] = true;
    #endif
}

} // namespace NYql::NDq
