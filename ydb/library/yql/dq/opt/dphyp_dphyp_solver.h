#pragma once

#include "dphyp_join_hypergraph.h"
#include "dphyp_join_tree_node.h"
#include "dphyp_bitset.h"

namespace NYql::NDq::NDphyp {

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

template <typename TNodeSet>
class TDPHypSolver {
public:
    TDPHypSolver(
        TJoinHypergraph<TNodeSet>& graph,
        IProviderContext& ctx
    ) 
        : Graph_(graph) 
        , NNodes_(graph.GetNodeCount())
        , Pctx_(ctx)
    {}

    std::shared_ptr<TJoinOptimizerNodeInternal> Solve();


private:
    void EnumerateCsgRec(const TNodeSet& s1, const TNodeSet& x);

    void EmitCsg(const TNodeSet& s1);

    void EnumerateCmpRec(const TNodeSet& s1, const TNodeSet& s2, const TNodeSet& x);

    void EmitCsgCmp(const TNodeSet& s1, const TNodeSet& s2, const TJoinHypergraph<TNodeSet>::TEdge& csgCmpEdge);

private:
    inline TNodeSet MakeBiMin(const TNodeSet& s);

    inline TNodeSet MakeB(const TNodeSet& s, size_t v);

    TNodeSet Neighs(TNodeSet s, TNodeSet x);

    TNodeSet NextBitset(const TNodeSet& current, const TNodeSet& final);

    std::shared_ptr<TJoinOptimizerNodeInternal>  PickBestJoin(
        std::shared_ptr<IBaseOptimizerNode> left,
        std::shared_ptr<IBaseOptimizerNode> right,
        EJoinKind joinKind,
        bool isCommutative,
        const std::set<std::pair<TJoinColumn, TJoinColumn>>& joinConditions,
        const std::set<std::pair<TJoinColumn, TJoinColumn>>& reversedJoinConditions,
        const TVector<TString>& leftJoinKeys,
        const TVector<TString>& rightJoinKeys,
        IProviderContext& ctx
    );

private:
    TJoinHypergraph<TNodeSet>& Graph_;
    size_t NNodes_;
    IProviderContext& Pctx_;
    THashMap<std::pair<TNodeSet, TNodeSet>, bool, pair_hash> CheckTable;

private:
    THashMap<TNodeSet, std::shared_ptr<IBaseOptimizerNode>, std::hash<TNodeSet>> DpTable_;
};

template<typename TNodeSet> TNodeSet TDPHypSolver<TNodeSet>::Neighs(TNodeSet s, TNodeSet x) {
    TNodeSet neighs{};

    auto& nodes = Graph_.GetNodes();

    TSetBitsIt<TNodeSet> setBitsIt(s);
    while (setBitsIt.HasNext()) {
        size_t nodeId = setBitsIt.Next();
        
        neighs |= nodes[nodeId].SimpleNeighborhood;

        for (const auto& edgeId: nodes[nodeId].ComplexEdgesId) {
            auto& edge = Graph_.GetEdges()[edgeId];
            if (
                IsSubset(edge.Left, s) &&
                !AreOverlaps(edge.Right, x) &&
                !AreOverlaps(edge.Right, s) && 
                !AreOverlaps(edge.Right, neighs)
            ) {
                neighs[GetLowestSetBit(edge.Right)] = 1;
            }
        }
    }

    neighs &= ~x;
    return neighs;
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

            if (auto* edge = Graph_.FindEdgeBetween(s1, s2); edge != nullptr) {
                EmitCsgCmp(s1, s2, *edge);
            }

            EnumerateCmpRec(s1, s2, x | MakeB(neighs, GetLowestSetBit(s2)));
        }
    }
}

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
            if (auto* edge = Graph_.FindEdgeBetween(s1, s2 | next); edge != nullptr) {
                EmitCsgCmp(s1, s2 | next, *edge);
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

/**
 * Iterate over all join algorithms and pick the best join that is applicable.
 * Also considers commuting joins
*/
template <typename TNodeSet> std::shared_ptr<TJoinOptimizerNodeInternal> TDPHypSolver<TNodeSet>::PickBestJoin(
    std::shared_ptr<IBaseOptimizerNode> left,
    std::shared_ptr<IBaseOptimizerNode> right,
    EJoinKind joinKind,
    bool isCommutative,
    const std::set<std::pair<TJoinColumn, TJoinColumn>>& joinConditions,
    const std::set<std::pair<TJoinColumn, TJoinColumn>>& reversedJoinConditions,
    const TVector<TString>& leftJoinKeys,
    const TVector<TString>& rightJoinKeys,
    IProviderContext& ctx
) {
    double bestCost = std::numeric_limits<double>::infinity();
    EJoinAlgoType bestAlgo{};
    bool bestJoinIsReversed = false;

    for (auto joinAlgo : AllJoinAlgos) {
        if (ctx.IsJoinApplicable(left, right, joinConditions, leftJoinKeys, rightJoinKeys, joinAlgo)){
            auto cost = ComputeJoinStats(*left->Stats, *right->Stats, leftJoinKeys, rightJoinKeys, joinAlgo, ctx).Cost;
            if (cost < bestCost){
                bestCost = cost;
                bestAlgo = joinAlgo;
                bestJoinIsReversed = false;
            }
        }

        if (isCommutative) {
            if (ctx.IsJoinApplicable(right, left, reversedJoinConditions, rightJoinKeys, leftJoinKeys, joinAlgo)){
                auto cost = ComputeJoinStats(*right->Stats, *left->Stats,  rightJoinKeys, leftJoinKeys, joinAlgo, ctx).Cost;
                if (cost < bestCost){
                    bestCost = cost;
                    bestAlgo = joinAlgo;
                    bestJoinIsReversed = true;
                }
            }
        }
    }

    Y_ENSURE(bestCost != std::numeric_limits<double>::infinity(), "No join was chosen!");

    if (bestJoinIsReversed) {
        return MakeJoinInternal(right, left, reversedJoinConditions, rightJoinKeys, leftJoinKeys, joinKind, bestAlgo, ctx);
    }
    
    return MakeJoinInternal(left, right, joinConditions, leftJoinKeys, rightJoinKeys, joinKind, bestAlgo, ctx);
}

/* 
 * Emit a single CSG + CMP pair
*/
template<typename TNodeSet> void TDPHypSolver<TNodeSet>::EmitCsgCmp(const TNodeSet& s1, const TNodeSet& s2, const TJoinHypergraph<TNodeSet>::TEdge& csgCmpEdge) {
    // Here we actually build the join and choose and compare the
    // new plan to what's in the dpTable, if it there

    Y_ENSURE(DpTable_.contains(s1), "DP Table does not contain S1");
    Y_ENSURE(DpTable_.contains(s2), "DP Table does not conaint S2");

    TNodeSet joined = s1 | s2;

    auto reversedEdge = Graph_.FindEdgeBetween(s2, s1);

    auto bestJoin = PickBestJoin(
        DpTable_[s1],
        DpTable_[s2],
        csgCmpEdge.JoinKind,
        csgCmpEdge.IsCommutative,
        csgCmpEdge.JoinConditions,
        reversedEdge->JoinConditions,
        csgCmpEdge.LeftJoinKeys,
        csgCmpEdge.RightJoinKeys,
        Pctx_
    );

    if (!DpTable_.contains(joined) || bestJoin->Stats->Cost < DpTable_[joined]->Stats->Cost) {
        DpTable_[joined] = bestJoin;
    }

    auto pair = std::make_pair(s1, s2);
    Y_ENSURE (!CheckTable.contains(pair), "Check table already contains pair S1|S2");

    CheckTable[ std::pair<TNodeSet,TNodeSet>(s1, s2) ] = true;
}

} // namespace NYql::NDq
