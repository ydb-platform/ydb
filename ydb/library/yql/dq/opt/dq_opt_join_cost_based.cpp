#include "dq_opt_join.h"
#include "dq_opt_phy.h"

#include <ydb/library/yql/core/yql_join.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/dq/type_ann/dq_type_ann.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/core/yql_type_helpers.h>
#include <ydb/library/yql/core/yql_statistics.h>
#include <ydb/library/yql/core/yql_cost_function.h>

#include <ydb/library/yql/core/cbo/cbo_optimizer_new.h> //new interface

#include <library/cpp/disjoint_sets/disjoint_sets.h>


#include <bitset>
#include <set>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <queue>
#include <memory>
#include <sstream>

namespace NYql::NDq {


using namespace NYql::NNodes;

/**
 * Edge structure records an edge in a Join graph.
 *  - from is the integer id of the source vertex of the graph
 *  - to is the integer id of the target vertex of the graph
 *  - joinConditions records the set of join conditions of this edge
*/
struct TEdge {
    int From;
    int To;
    mutable std::set<std::pair<TJoinColumn, TJoinColumn>> JoinConditions;
    mutable TVector<TString> LeftJoinKeys;
    mutable TVector<TString> RightJoinKeys;

    TEdge(): From(-1), To(-1) {}

    TEdge(int f, int t): From(f), To(t) {}

    TEdge(int f, int t, std::pair<TJoinColumn, TJoinColumn> cond): From(f), To(t) {
        JoinConditions.insert(cond);
        BuildCondVectors();
    }

    TEdge(int f, int t, std::set<std::pair<TJoinColumn, TJoinColumn>> conds): From(f), To(t),
        JoinConditions(conds) {
        BuildCondVectors();
        }

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

    bool operator==(const TEdge& other) const
    {
        return From==other.From && To==other.To;
    }

    struct HashFunction
    {
        size_t operator()(const TEdge& e) const
        {
            return e.From + e.To;
        }
    };

    static const struct TEdge ErrorEdge;
};

/**
 * Fetch join conditions from the equi-join tree
*/
void ComputeJoinConditions(const TCoEquiJoinTuple& joinTuple,
    std::set<std::pair<TJoinColumn, TJoinColumn>>& joinConditions) {
    if (joinTuple.LeftScope().Maybe<TCoEquiJoinTuple>()) {
        ComputeJoinConditions(joinTuple.LeftScope().Cast<TCoEquiJoinTuple>(), joinConditions);
    }

    if (joinTuple.RightScope().Maybe<TCoEquiJoinTuple>()) {
        ComputeJoinConditions(joinTuple.RightScope().Cast<TCoEquiJoinTuple>(), joinConditions);
    }

    size_t joinKeysCount = joinTuple.LeftKeys().Size() / 2;
    for (size_t i = 0; i < joinKeysCount; ++i) {
        size_t keyIndex = i * 2;

        auto leftScope = joinTuple.LeftKeys().Item(keyIndex).StringValue();
        auto leftColumn = joinTuple.LeftKeys().Item(keyIndex + 1).StringValue();
        auto rightScope = joinTuple.RightKeys().Item(keyIndex).StringValue();
        auto rightColumn = joinTuple.RightKeys().Item(keyIndex + 1).StringValue();

        joinConditions.insert( std::make_pair( TJoinColumn(leftScope, leftColumn),
            TJoinColumn(rightScope, rightColumn)));
    }
}

/**
 * Internal Join nodes are used inside the CBO. They don't own join condition data structures
 * and therefore avoid copying them during generation of candidate plans.
 *
 * These datastructures are owned by the query graph, so it is important to keep the graph around
 * while internal nodes are being used.
 *
 * After join enumeration, internal nodes need to be converted to regular nodes, that own the data
 * structures
*/
struct TJoinOptimizerNodeInternal : public IBaseOptimizerNode {
    std::shared_ptr<IBaseOptimizerNode> LeftArg;
    std::shared_ptr<IBaseOptimizerNode> RightArg;
    const std::set<std::pair<NDq::TJoinColumn, NDq::TJoinColumn>>& JoinConditions;
    const TVector<TString>& LeftJoinKeys;
    const TVector<TString>& RightJoinKeys;
    EJoinKind JoinType;
    EJoinAlgoType JoinAlgo;
    bool IsReorderable;

    TJoinOptimizerNodeInternal(const std::shared_ptr<IBaseOptimizerNode>& left,
        const std::shared_ptr<IBaseOptimizerNode>& right,
        const std::set<std::pair<NDq::TJoinColumn, NDq::TJoinColumn>>& joinConditions,
        const TVector<TString>& leftJoinKeys,
        const TVector<TString>& rightJoinKeys,
        const EJoinKind joinType,
        const EJoinAlgoType joinAlgo,
        bool nonReorderable=false);

    virtual ~TJoinOptimizerNodeInternal() {}
    virtual TVector<TString> Labels();
    virtual void Print(std::stringstream& stream, int ntabs=0);
};

TJoinOptimizerNodeInternal::TJoinOptimizerNodeInternal(const std::shared_ptr<IBaseOptimizerNode>& left, const std::shared_ptr<IBaseOptimizerNode>& right,
        const std::set<std::pair<TJoinColumn, TJoinColumn>>& joinConditions, const TVector<TString>& leftJoinKeys,
        const TVector<TString>& rightJoinKeys, const EJoinKind joinType, const EJoinAlgoType joinAlgo, bool nonReorderable) :
    IBaseOptimizerNode(JoinNodeType),
    LeftArg(left),
    RightArg(right),
    JoinConditions(joinConditions),
    LeftJoinKeys(leftJoinKeys),
    RightJoinKeys(rightJoinKeys),
    JoinType(joinType),
    JoinAlgo(joinAlgo) {
        IsReorderable = (JoinType==EJoinKind::InnerJoin) && (nonReorderable==false);
    }

TVector<TString> TJoinOptimizerNodeInternal::Labels() {
    auto res = LeftArg->Labels();
    auto rightLabels = RightArg->Labels();
    res.insert(res.begin(),rightLabels.begin(),rightLabels.end());
    return res;
}

/**
 * Convert a tree of internal optimizer nodes to external nodes that own the data structures.
 *
 * The internal node tree can have references to external nodes (since some subtrees are optimized
 * separately if the plan contains non-orderable joins). So we check the instances and if we encounter
 * an external node, we return the whole subtree unchanged.
*/
std::shared_ptr<TJoinOptimizerNode> ConvertFromInternal(const std::shared_ptr<IBaseOptimizerNode> internal) {
    Y_ENSURE(internal->Kind == EOptimizerNodeKind::JoinNodeType);

    if (dynamic_cast<TJoinOptimizerNode*>(internal.get()) != nullptr) {
        return  std::static_pointer_cast<TJoinOptimizerNode>(internal);
    }

    auto join = std::static_pointer_cast<TJoinOptimizerNodeInternal>(internal);

    auto left = join->LeftArg;
    auto right = join->RightArg;

    if (left->Kind == EOptimizerNodeKind::JoinNodeType) {
        left = ConvertFromInternal(left);
    }
    if (right->Kind == EOptimizerNodeKind::JoinNodeType) {
        right = ConvertFromInternal(right);
    }

    auto newJoin = std::make_shared<TJoinOptimizerNode>(left, right, join->JoinConditions, join->JoinType, join->JoinAlgo, !join->IsReorderable);
    newJoin->Stats = join->Stats;
    return newJoin;
}

void TJoinOptimizerNodeInternal::Print(std::stringstream& stream, int ntabs) {
    for (int i = 0; i < ntabs; i++){
        stream << "\t";
    }

    stream << "Join: (" << JoinType << ") ";
    for (auto c : JoinConditions){
        stream << c.first.RelName << "." << c.first.AttributeName
            << "=" << c.second.RelName << "."
            << c.second.AttributeName << ", ";
    }
    stream << "\n";

    for (int i = 0; i < ntabs; i++){
        stream << "\t";
    }

    if (Stats) {
        stream << *Stats << "\n";
    }

    LeftArg->Print(stream, ntabs+1);
    RightArg->Print(stream, ntabs+1);
}

/**
 * Create a new external join node and compute its statistics and cost
*/
std::shared_ptr<TJoinOptimizerNode> MakeJoin(std::shared_ptr<IBaseOptimizerNode> left,
    std::shared_ptr<IBaseOptimizerNode> right,
    const std::set<std::pair<TJoinColumn, TJoinColumn>>& joinConditions,
    const TVector<TString>& leftJoinKeys,
    const TVector<TString>& rightJoinKeys,
    EJoinKind joinKind,
    EJoinAlgoType joinAlgo,
    bool nonReorderable,
    IProviderContext& ctx) {

    auto res = std::make_shared<TJoinOptimizerNode>(left, right, joinConditions, joinKind, joinAlgo, nonReorderable);
    res->Stats = std::make_shared<TOptimizerStatistics>( ComputeJoinStats(*left->Stats, *right->Stats, leftJoinKeys, rightJoinKeys, joinAlgo, ctx));
    return res;
}

/**
 * Create a new internal join node and compute its statistics and cost
*/
std::shared_ptr<TJoinOptimizerNodeInternal> MakeJoinInternal(std::shared_ptr<IBaseOptimizerNode> left,
    std::shared_ptr<IBaseOptimizerNode> right,
    const std::set<std::pair<TJoinColumn, TJoinColumn>>& joinConditions,
    const TVector<TString>& leftJoinKeys,
    const TVector<TString>& rightJoinKeys,
    EJoinKind joinKind,
    EJoinAlgoType joinAlgo,
    IProviderContext& ctx) {

    auto res = std::make_shared<TJoinOptimizerNodeInternal>(left, right, joinConditions, leftJoinKeys, rightJoinKeys, joinKind, joinAlgo);
    res->Stats = std::make_shared<TOptimizerStatistics>( ComputeJoinStats(*left->Stats, *right->Stats, leftJoinKeys, rightJoinKeys, joinAlgo, ctx));
    return res;
}

/**
 * Iterate over all join algorithms and pick the best join that is applicable.
 * Also considers commuting joins
*/
std::shared_ptr<TJoinOptimizerNodeInternal> PickBestJoin(std::shared_ptr<IBaseOptimizerNode> left,
    std::shared_ptr<IBaseOptimizerNode> right,
    const std::set<std::pair<TJoinColumn, TJoinColumn>>& leftJoinConditions,
    const std::set<std::pair<TJoinColumn, TJoinColumn>>& rightJoinConditions,
    const TVector<TString>& leftJoinKeys,
    const TVector<TString>& rightJoinKeys,
    IProviderContext& ctx) {

    double bestCost;
    EJoinAlgoType bestAlgo;
    bool bestJoinLeftRight = true;
    bool bestJoinValid = false;

    for ( auto joinAlgo : AllJoinAlgos ) {
        if (ctx.IsJoinApplicable(left, right, leftJoinConditions, leftJoinKeys, rightJoinKeys, joinAlgo)){
            auto cost = ComputeJoinStats(*left->Stats, *right->Stats, leftJoinKeys, rightJoinKeys, joinAlgo, ctx).Cost;
            if (bestJoinValid){
                if (cost < bestCost) {
                    bestCost = cost;
                    bestAlgo = joinAlgo;
                    bestJoinLeftRight = true;
                }
            } else {
                bestCost = cost;
                bestAlgo = joinAlgo;
                bestJoinLeftRight = true;
                bestJoinValid = true;
            }
        }

        if (ctx.IsJoinApplicable(right, left, rightJoinConditions, rightJoinKeys, leftJoinKeys, joinAlgo)){
            auto cost = ComputeJoinStats(*right->Stats, *left->Stats,  rightJoinKeys, leftJoinKeys, joinAlgo, ctx).Cost;
            if (bestJoinValid){
                if (cost < bestCost) {
                    bestCost = cost;
                    bestAlgo = joinAlgo;
                    bestJoinLeftRight = false;
                }
            } else {
                bestCost = cost;
                bestAlgo = joinAlgo;
                bestJoinLeftRight = false;
                bestJoinValid = true;
            }
        }
    }

    Y_ENSURE(bestJoinValid,"No join was chosen!");

    if (bestJoinLeftRight) {
        return MakeJoinInternal(left, right, leftJoinConditions, leftJoinKeys, rightJoinKeys, EJoinKind::InnerJoin, bestAlgo, ctx);
    } else {
        return MakeJoinInternal(right, left, rightJoinConditions, rightJoinKeys, leftJoinKeys, EJoinKind::InnerJoin, bestAlgo, ctx);
    }
}

/**
 * Iterate over all join algorithms and pick the best join that is applicable
*/
std::shared_ptr<TJoinOptimizerNode> PickBestNonReorderabeJoin(const std::shared_ptr<TJoinOptimizerNode>& node,
    IProviderContext& ctx) {

    EJoinAlgoType bestJoinAlgo;
    bool bestJoinValid = false;
    double bestJoinCost;
    const auto& left = node->LeftArg;
    const auto& right = node->RightArg;
    const auto& joinConditions = node->JoinConditions;
    const auto& leftJoinKeys = node->LeftJoinKeys;
    const auto& rightJoinKeys = node->RightJoinKeys;

    for ( auto joinAlgo : AllJoinAlgos ) {
        if (ctx.IsJoinApplicable(left, right, joinConditions, leftJoinKeys, rightJoinKeys, joinAlgo)){
            auto cost = ComputeJoinStats(*right->Stats, *left->Stats,  rightJoinKeys, leftJoinKeys, joinAlgo, ctx).Cost;
            if (bestJoinValid) {
                if (cost < bestJoinCost) {
                    bestJoinAlgo = joinAlgo;
                    bestJoinCost = cost;
                }
            } else {
                bestJoinAlgo = joinAlgo;
                bestJoinCost = cost;
                bestJoinValid = true;
            }
        }
    }

    Y_ENSURE(bestJoinValid,"No join was chosen!");
    node->Stats = std::make_shared<TOptimizerStatistics>(ComputeJoinStats(*left->Stats, *right->Stats, leftJoinKeys, rightJoinKeys, bestJoinAlgo, ctx));
    node->JoinAlgo = bestJoinAlgo;
    return node;
}

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

/**
 * Graph is a data structure for the join graph
 * It is an undirected graph, with two edges per connection (from,to) and (to,from)
 * It needs to be constructed with addNode and addEdge methods, since its
 * keeping various indexes updated.
 * The graph also needs to be reordered with the breadth-first search method
*/
template <int N>
struct TGraph {
    // set of edges of the graph
    std::unordered_set<TEdge,TEdge::HashFunction> Edges;

    // neightborgh index
    TVector<std::bitset<N>> EdgeIdx;

    // number of nodes in a graph
    int NNodes;

    // mapping from rel label to node in the graph
    THashMap<TString,int> ScopeMapping;

    // mapping from node in the graph to rel label
    TVector<TString> RevScopeMapping;

    // Empty graph constructor intializes indexes to size N
    TGraph() : EdgeIdx(N), RevScopeMapping(N) {}

    // Add a node to a graph with a rel label
    void AddNode(int nodeId, TString scope){
        NNodes = nodeId + 1;
        ScopeMapping[scope] = nodeId;
        RevScopeMapping[nodeId] = scope;
    }

     // Add a node to a graph with a vector of rel label
    void AddNode(int nodeId, const TVector<TString>& scopes){
        NNodes = nodeId + 1;
        TString revScope;
        for (auto s: scopes ) {
            ScopeMapping[s] = nodeId;
            revScope += s + ",";
        }
        RevScopeMapping[nodeId] = revScope;
    }


    // Add an edge to the graph, if the edge is already in the graph
    // (we check both directions), no action is taken. Otherwise we
    // insert two edges, the forward edge with original joinConditions
    // and a reverse edge with swapped joinConditions
    void AddEdge(TEdge e){
        if (Edges.contains(e) || Edges.contains(TEdge(e.To, e.From))) {
            return;
        }

        Edges.insert(e);
        std::set<std::pair<TJoinColumn, TJoinColumn>> swappedSet;
        for (auto c : e.JoinConditions){
            swappedSet.insert(std::make_pair(c.second, c.first));
        }
        Edges.insert(TEdge(e.To,e.From,swappedSet));

        EdgeIdx[e.From].set(e.To);
        EdgeIdx[e.To].set(e.From);
    }

    // Find a node by the rel scope
    int FindNode(TString scope){
        return ScopeMapping[scope];
    }

    // Return a bitset of node's neighbors
    inline std::bitset<N> FindNeighbors(int fromVertex)
    {
        return EdgeIdx[fromVertex];
    }

    // Find an edge that connects two subsets of graph's nodes
    // We are guaranteed to find a match
    const TEdge& FindCrossingEdge(const std::bitset<N>& S1, const std::bitset<N>& S2) {
        for(int i = 0; i < NNodes; i++){
            if (!S1[i]) {
                continue;
            }
            for (int j = 0; j < NNodes; j++) {
                if (!S2[j]) {
                    continue;
                }
                if (EdgeIdx[i].test(j)) {
                    auto it = Edges.find(TEdge(i, j));
                    Y_DEBUG_ABORT_UNLESS(it != Edges.end());
                    return *it;
                }
            }
        }
        Y_ENSURE(false,"Connecting edge not found!");
        return TEdge::ErrorEdge;
    }

    /**
     * Create a union-set from the join conditions to record the equivalences.
     * Then use the equivalence set to compute transitive closure of the graph.
     * Transitive closure means that if we have an edge from (1,2) with join
     * condition R.A = S.A and we have an edge from (2,3) with join condition
     * S.A = T.A, we will find out that the join conditions form an equivalence set
     * and add an edge (1,3) with join condition R.A = T.A.
    */
    void ComputeTransitiveClosure(const std::set<std::pair<TJoinColumn, TJoinColumn>>& joinConditions) {
        std::set<TJoinColumn> columnSet;
        for (auto [ leftCondition, rightCondition ] : joinConditions) {
            columnSet.insert(leftCondition);
            columnSet.insert(rightCondition);
        }
        std::vector<TJoinColumn> columns;
        for (auto c : columnSet ) {
            columns.push_back(c);
        }

        THashMap<TJoinColumn, int, TJoinColumn::HashFunction> indexMapping;
        for (size_t i=0; i<columns.size(); i++) {
            indexMapping[columns[i]] = i;
        }

        TDisjointSets ds = TDisjointSets( columns.size() );
        for (auto [ leftCondition, rightCondition ] : joinConditions ) {
            int leftIndex = indexMapping[leftCondition];
            int rightIndex = indexMapping[rightCondition];
            ds.UnionSets(leftIndex,rightIndex);
        }

        for (size_t i = 0; i < columns.size(); i++) {
            for (size_t j = 0; j < i; j++) {
                if (ds.CanonicSetElement(i) == ds.CanonicSetElement(j)) {
                    TJoinColumn left = columns[i];
                    TJoinColumn right = columns[j];
                    int leftNodeId = ScopeMapping[left.RelName];
                    int rightNodeId = ScopeMapping[right.RelName];

                    auto maybeEdge1 = Edges.find({leftNodeId, rightNodeId});
                    auto maybeEdge2 = Edges.find({rightNodeId, leftNodeId});

                    if (maybeEdge1 == Edges.end() && maybeEdge2 == Edges.end()) {
                        AddEdge(TEdge(leftNodeId,rightNodeId,std::make_pair(left, right)));
                    } else {
                        Y_ABORT_UNLESS(maybeEdge1 != Edges.end() && maybeEdge2 != Edges.end());
                        auto edge1 = *maybeEdge1;
                        auto edge2 = *maybeEdge2;

                        edge1.JoinConditions.emplace(left, right);
                        edge2.JoinConditions.emplace(right, left);
                        edge1.BuildCondVectors();
                        edge2.BuildCondVectors();

                        Edges.erase(maybeEdge1);
                        Edges.erase(maybeEdge2);

                        Edges.emplace(edge1);
                        Edges.emplace(edge2);
                    }
                }
            }
        }
    }

    /**
     * Print the graph
    */
    void PrintGraph(std::stringstream& stream) {
        stream << "Join Graph:\n";
        stream << "nNodes: " << NNodes << ", nEdges: " << Edges.size() << "\n";

        for(int i = 0; i < NNodes; i++) {
            stream << "Node:" << i << "," << RevScopeMapping[i] << "\n";
        }
        for (const TEdge& e: Edges ) {
            stream << "Edge: " << e.From << " -> " << e.To << "\n";
            for (auto p : e.JoinConditions) {
                stream << p.first.RelName << "."
                    << p.first.AttributeName << "="
                    << p.second.RelName << "."
                    << p.second.AttributeName << "\n";
            }
            for (auto l : e.LeftJoinKeys) {
                stream << l << ",";
            }
            stream << "=";
            for (auto r : e.RightJoinKeys) {
                stream << r << ",";
            }
            stream << "\n";
        }
    }
};

/**
 * DPcpp (Dynamic Programming with connected complement pairs) is a graph-aware
 * join eumeration algorithm that only considers CSGs (Connected Sub-Graphs) of
 * the join graph and computes CMPs (Complement pairs) that are also connected
 * subgraphs of the join graph. It enumerates CSGs in the order, such that subsets
 * are enumerated first and no duplicates are ever enumerated. Then, for each emitted
 * CSG it computes the complements with the same conditions - they much already be
 * present in the dynamic programming table and no pair should be enumerated twice.
 *
 * The DPccp solver is templated by the largest number of joins we can process, this
 * is in turn used by bitsets that represent sets of relations.
*/
template <int N>
class TDPccpSolver {
public:

    // Construct the DPccp solver based on the join graph and data about input relations
    TDPccpSolver(TGraph<N>& g, TVector<std::shared_ptr<IBaseOptimizerNode>> rels, IProviderContext& ctx):
        Graph(g), Rels(rels), Pctx(ctx) {
        NNodes = g.NNodes;
    }

    // Run DPccp algorithm and produce the join tree in CBO's internal representation
    std::shared_ptr<TJoinOptimizerNodeInternal> Solve();

    // Calculate the size of a dynamic programming table with a budget
    ui32 CountCC(ui32 budget);

private:

    // Compute the next subset of relations, given by the final bitset
    std::bitset<N> NextBitset(const std::bitset<N>& current, const std::bitset<N>& final);

    // Print the set of relations in a bitset
    void PrintBitset(std::stringstream& stream, const std::bitset<N>& s, std::string name, int ntabs=0);

    // Dynamic programming table that records optimal join subtrees
    THashMap<std::bitset<N>, std::shared_ptr<IBaseOptimizerNode>, std::hash<std::bitset<N>>> DpTable;

    // REMOVE: Sanity check table that tracks that we don't consider the same pair twice
    THashMap<std::pair<std::bitset<N>, std::bitset<N>>, bool, pair_hash> CheckTable;

    // number of nodes in a graph
    int NNodes;

    // Join graph
    TGraph<N>& Graph;

    // List of input relations to DPccp
    TVector<std::shared_ptr<IBaseOptimizerNode>> Rels;

    // Provider specific contexts?
    // FIXME: This is a temporary structure that needs to be extended to multiple providers
    IProviderContext& Pctx;

    // Emit connected subgraph
    void EmitCsg(const std::bitset<N>&, int=0);

    // Enumerate subgraphs recursively
    void EnumerateCsgRec(const std::bitset<N>&, const std::bitset<N>&,int=0);

    // Emit the final pair of CSG and CMP - compute the join and record it in the
    // DP table
    void EmitCsgCmp(const std::bitset<N>&, const std::bitset<N>&,int=0);

    // Enumerate complement pairs recursively
    void EnumerateCmpRec(const std::bitset<N>&, const std::bitset<N>&, const std::bitset<N>&,int=0);

    // Compute the neighbors of a set of nodes, excluding the nodes in exclusion set
    std::bitset<N> Neighbors(const std::bitset<N>&, const std::bitset<N>&);

    // Create an exclusion set that contains all the nodes of the graph that are smaller or equal to
    // the smallest node in the provided bitset
    std::bitset<N> MakeBiMin(const std::bitset<N>&);

    // Create an exclusion set that contains all the nodes of the bitset that are smaller or equal to
    // the provided integer
    std::bitset<N> MakeB(const std::bitset<N>&,int);

    // Count the size of the dynamic programming table recursively
    ui32 CountCCRec(const std::bitset<N>&, const std::bitset<N>&, ui32, ui32);
};

// Print tabs
void PrintTabs(std::stringstream& stream, int ntabs) {

    for (int i = 0; i < ntabs; i++)
        stream << "\t";
}

// Print a set of nodes in the graph given by this bitset
template <int N> void TDPccpSolver<N>::PrintBitset(std::stringstream& stream,
    const std::bitset<N>& s, std::string name, int ntabs) {

    PrintTabs(stream, ntabs);

    stream << name << ": " << "{";
     for (int i = 0; i < NNodes; i++)
        if (s[i])
            stream << i << ",";

    stream <<"}\n";
}

// Compute neighbors of a set of nodes S, exclusing the exclusion set X
template<int N> std::bitset<N> TDPccpSolver<N>::Neighbors(const std::bitset<N>& S, const std::bitset<N>& X) {

    std::bitset<N> res;

    for (int i = 0; i < Graph.NNodes; i++) {
        if (S[i]) {
            std::bitset<N> n = Graph.FindNeighbors(i);
            res = res | n;
        }
    }

    res = res & ~ X;
    return res;
}

// Run the entire DPccp algorithm and compute the optimal join tree
template<int N> std::shared_ptr<TJoinOptimizerNodeInternal> TDPccpSolver<N>::Solve()
{
    // Process singleton sets
    for (int i = NNodes-1; i >= 0; i--) {
        std::bitset<N> s;
        s.set(i);
        DpTable[s] = Rels[i];
    }

    // Expand singleton sets
    for (int i = NNodes-1; i >= 0; i--) {
        std::bitset<N> s;
        s.set(i);
        EmitCsg(s);
        EnumerateCsgRec(s, MakeBiMin(s));
    }

    // Return the entry of the dpTable that corresponds to the full
    // set of nodes in the graph
    std::bitset<N> V;
    for (int i = 0; i < NNodes; i++) {
        V.set(i);
    }

    Y_ENSURE(DpTable.contains(V), "Final relset not in dptable");
    return std::static_pointer_cast<TJoinOptimizerNodeInternal>(DpTable[V]);
}

/**
 * EmitCsg emits Connected SubGraphs
 * First it iterates through neighbors of the initial set S and emits pairs
 * (S,S2), where S2 is the neighbor of S. Then it recursively emits complement pairs
*/
 template <int N> void TDPccpSolver<N>::EmitCsg(const std::bitset<N>& S, int ntabs) {
    std::bitset<N> X = S | MakeBiMin(S);
    std::bitset<N> Ns = Neighbors(S, X);

    if (Ns==std::bitset<N>()) {
        return;
    }

    for (int i = NNodes - 1; i >= 0; i--) {
        if (Ns[i]) {
            std::bitset<N> S2;
            S2.set(i);
            EmitCsgCmp(S, S2, ntabs+1);
            EnumerateCmpRec(S, S2, X | MakeB(Ns, i), ntabs+1);
        }
    }
 }

 /**
  * Enumerates connected subgraphs
  * First it emits CSGs that are created by adding neighbors of S to S
  * Then it recurses on the S fused with its neighbors.
 */
 template <int N> void TDPccpSolver<N>::EnumerateCsgRec(const std::bitset<N>& S, const std::bitset<N>& X, int ntabs) {

    std::bitset<N> Ns = Neighbors(S, X);

    if (Ns == std::bitset<N>()) {
        return;
    }

    std::bitset<N> prev;
    std::bitset<N> next;

    while(true) {
        next = NextBitset(prev, Ns);
        EmitCsg(S | next );
        if (next == Ns) {
            break;
        }
        prev = next;
    }

    prev.reset();
    while(true) {
        next = NextBitset(prev, Ns);
        EnumerateCsgRec(S | next, X | Ns , ntabs+1);
        if (next==Ns) {
            break;
        }
        prev = next;
    }
 }

/***
 * Enumerates complement pairs
 * First it emits the pairs (S1,S2+next) where S2+next is the set of relation sets
 * that are obtained by adding S2's neighbors to itself
 * Then it recusrses into pairs (S1,S2+next)
*/
 template <int N> void TDPccpSolver<N>::EnumerateCmpRec(const std::bitset<N>& S1,
    const std::bitset<N>& S2, const std::bitset<N>& X, int ntabs) {

    std::bitset<N> Ns = Neighbors(S2, X);

    if (Ns==std::bitset<N>()) {
        return;
    }

    std::bitset<N> prev;
    std::bitset<N> next;

    while(true) {
        next = NextBitset(prev, Ns);
        EmitCsgCmp(S1, S2 | next, ntabs+1);
        if (next==Ns) {
            break;
        }
        prev = next;
    }

    prev.reset();
    while(true) {
        next = NextBitset(prev, Ns);
        EnumerateCmpRec(S1, S2 | next, X | Ns, ntabs+1);
        if (next==Ns) {
            break;
        }
        prev = next;
    }
 }

/**
 * Emit a single CSG + CMP pair
*/
template <int N> void TDPccpSolver<N>::EmitCsgCmp(const std::bitset<N>& S1, const std::bitset<N>& S2, int ntabs) {

    Y_UNUSED(ntabs);
    // Here we actually build the join and choose and compare the
    // new plan to what's in the dpTable, if it there

    Y_ENSURE(DpTable.contains(S1),"DP Table does not contain S1");
    Y_ENSURE(DpTable.contains(S2),"DP Table does not conaint S2");

    std::bitset<N> joined = S1 | S2;

    const TEdge& e1 = Graph.FindCrossingEdge(S1, S2);
    const TEdge& e2 = Graph.FindCrossingEdge(S2, S1);
    auto bestJoin = PickBestJoin(DpTable[S1], DpTable[S2], e1.JoinConditions, e2.JoinConditions, e1.LeftJoinKeys, e1.RightJoinKeys, Pctx);

    if (! DpTable.contains(joined)) {
        DpTable[joined] = bestJoin;
    } else {
        if (bestJoin->Stats->Cost < DpTable[joined]->Stats->Cost) {
            DpTable[joined] = bestJoin;
        }
    }

    /*
    * This is a sanity check that slows down the optimizer
    *

    auto pair = std::make_pair(S1, S2);
    Y_ENSURE (!CheckTable.contains(pair), "Check table already contains pair S1|S2");

    CheckTable[ std::pair<std::bitset<N>,std::bitset<N>>(S1, S2) ] = true;
    */
}

/**
 * Create an exclusion set that contains all the nodes of the graph that are smaller or equal to
 * the smallest node in the provided bitset
*/
template <int N> std::bitset<N> TDPccpSolver<N>::MakeBiMin(const std::bitset<N>& S) {
    std::bitset<N> res;

    for (int i = 0; i < NNodes; i++) {
        if (S[i]) {
            for (int j = 0; j <= i; j++) {
                res.set(j);
            }
            break;
        }
    }
    return res;
}

/**
 * Create an exclusion set that contains all the nodes of the bitset that are smaller or equal to
 * the provided integer
*/
template <int N> std::bitset<N> TDPccpSolver<N>::MakeB(const std::bitset<N>& S, int x) {
    std::bitset<N> res;

    for (int i = 0; i < NNodes; i++) {
        if (S[i] && i <= x) {
            res.set(i);
        }
    }

    return res;
}

/**
 * Compute the next subset of relations, given by the final bitset
*/
template <int N> std::bitset<N> TDPccpSolver<N>::NextBitset(const std::bitset<N>& prev, const std::bitset<N>& final) {
    if (prev==final)
        return final;

    std::bitset<N> res = prev;

    bool carry = true;
    for (int i = 0; i < NNodes; i++)
    {
        if (!carry) {
            break;
        }

        if (!final[i]) {
            continue;
        }

        if (res[i]==1 && carry) {
            res.reset(i);
        } else if (res[i]==0 && carry)
        {
            res.set(i);
            carry = false;
        }
    }

    return res;

    // TODO: We can optimize this with a few long integer operations,
    // but it will only work for 64 bit bitsets
    // return std::bitset<N>((prev | ~final).to_ulong() + 1) & final;
}

/**
 * Count the number of items in the DP table of DPcpp
*/
template <int N> ui32 TDPccpSolver<N>::CountCC(ui32 budget) {
    std::bitset<N> allNodes;
    allNodes.set();
    ui32 cost = 0;

    for (int i = NNodes - 1; i >= 0; i--) {
        cost += 1;
        if (cost > budget) {
            return cost;
        }
        std::bitset<N> S;
        S.set(i);
        std::bitset<N> X = MakeB(allNodes,i);
        cost = CountCCRec(S,X,cost,budget);
    }

    return cost;
}

/**
 * Recursively count the nuber of items in the DP table of DPccp
*/
template <int N> ui32 TDPccpSolver<N>::CountCCRec(const std::bitset<N>& S, const std::bitset<N>& X, ui32 cost, ui32 budget) {
    std::bitset<N> Ns = Neighbors(S, X);

    if (Ns==std::bitset<N>()) {
        return cost;
    }

    std::bitset<N> prev;
    std::bitset<N> next;

    while(true) {
        next = NextBitset(prev, Ns);
        cost += 1;
        if (cost > budget) {
            return cost;
        }
        cost = CountCCRec(S | next, X | Ns, cost, budget);
        if (next==Ns) {
            break;
        }
        prev = next;
    }

    return cost;
}


/**
 * Build a join tree that will replace the original join tree in equiJoin
 * TODO: Add join implementations here
*/
TExprBase BuildTree(TExprContext& ctx, const TCoEquiJoin& equiJoin,
    std::shared_ptr<TJoinOptimizerNode>& reorderResult) {

    // Create dummy left and right arg that will be overwritten
    TExprBase leftArg(equiJoin);
    TExprBase rightArg(equiJoin);

    // Build left argument of the join
    if (reorderResult->LeftArg->Kind == RelNodeType) {
        std::shared_ptr<TRelOptimizerNode> rel =
            std::static_pointer_cast<TRelOptimizerNode>(reorderResult->LeftArg);
        leftArg = BuildAtom(rel->Label, equiJoin.Pos(), ctx);
    } else {
        std::shared_ptr<TJoinOptimizerNode> join =
            std::static_pointer_cast<TJoinOptimizerNode>(reorderResult->LeftArg);
        leftArg = BuildTree(ctx,equiJoin,join);
    }
    // Build right argument of the join
    if (reorderResult->RightArg->Kind == RelNodeType) {
        std::shared_ptr<TRelOptimizerNode> rel =
            std::static_pointer_cast<TRelOptimizerNode>(reorderResult->RightArg);
        rightArg = BuildAtom(rel->Label, equiJoin.Pos(), ctx);
    } else {
        std::shared_ptr<TJoinOptimizerNode> join =
            std::static_pointer_cast<TJoinOptimizerNode>(reorderResult->RightArg);
        rightArg = BuildTree(ctx,equiJoin,join);
    }

    TVector<TExprBase> leftJoinColumns;
    TVector<TExprBase> rightJoinColumns;

    // Build join conditions
    for( auto pair : reorderResult->JoinConditions) {
        leftJoinColumns.push_back(BuildAtom(pair.first.RelName, equiJoin.Pos(), ctx));
        leftJoinColumns.push_back(BuildAtom(pair.first.AttributeName, equiJoin.Pos(), ctx));
        rightJoinColumns.push_back(BuildAtom(pair.second.RelName, equiJoin.Pos(), ctx));
        rightJoinColumns.push_back(BuildAtom(pair.second.AttributeName, equiJoin.Pos(), ctx));
    }

    auto optionsList = ctx.Builder(equiJoin.Pos())
        .List()
            .List(0)
                .Atom(0, "join_algo")
                .Atom(1, std::to_string(reorderResult->JoinAlgo))
            .Seal()
        .Seal()
        .Build();

    // Build the final output
    return Build<TCoEquiJoinTuple>(ctx,equiJoin.Pos())
        .Type(BuildAtom(ConvertToJoinString(reorderResult->JoinType),equiJoin.Pos(),ctx))
        .LeftScope(leftArg)
        .RightScope(rightArg)
        .LeftKeys()
            .Add(leftJoinColumns)
            .Build()
        .RightKeys()
            .Add(rightJoinColumns)
            .Build()
        .Options(optionsList)
        .Done();
}

/**
 * Rebuild the equiJoinOperator with a new tree, that was obtained by optimizing join order
*/
TExprBase RearrangeEquiJoinTree(TExprContext& ctx, const TCoEquiJoin& equiJoin,
    std::shared_ptr<TJoinOptimizerNode> reorderResult) {
    TVector<TExprBase> joinArgs;
    for (size_t i = 0; i < equiJoin.ArgCount() - 2; i++){
        joinArgs.push_back(equiJoin.Arg(i));
    }

    joinArgs.push_back(BuildTree(ctx,equiJoin,reorderResult));

    joinArgs.push_back(equiJoin.Arg(equiJoin.ArgCount() - 1));

    return Build<TCoEquiJoin>(ctx, equiJoin.Pos())
        .Add(joinArgs)
        .Done();
}

/**
 * Collects EquiJoin inputs with statistics for cost based optimization
*/
bool DqCollectJoinRelationsWithStats(
    TVector<std::shared_ptr<TRelOptimizerNode>>& rels,
    TTypeAnnotationContext& typesCtx,
    const TCoEquiJoin& equiJoin,
    const std::function<void(TVector<std::shared_ptr<TRelOptimizerNode>>&, TStringBuf, const TExprNode::TPtr, const std::shared_ptr<TOptimizerStatistics>&)>& collector)
{
    if (equiJoin.ArgCount() < 3) {
        return false;
    }

    for (size_t i = 0; i < equiJoin.ArgCount() - 2; ++i) {
        auto input = equiJoin.Arg(i).Cast<TCoEquiJoinInput>();
        auto joinArg = input.List();

        auto maybeStat = typesCtx.StatisticsMap.find(joinArg.Raw());

        if (maybeStat == typesCtx.StatisticsMap.end()) {
            YQL_CLOG(TRACE, CoreDq) << "Didn't find statistics for scope " << input.Scope().Cast<TCoAtom>().StringValue() << "\n";
            return false;
        }

        auto scope = input.Scope();
        if (!scope.Maybe<TCoAtom>()){
            return false;
        }

        TStringBuf label = scope.Cast<TCoAtom>();
        auto stats = maybeStat->second;
        collector(rels, label, joinArg.Ptr(), stats);
    }
    return true;
}

/**
 * Convert JoinTuple from AST into an internal representation of a optimizer plan
 * This procedure also hooks up rels with statistics to the leaf nodes of the plan
 * Statistics for join nodes are not computed
*/
std::shared_ptr<TJoinOptimizerNode> ConvertToJoinTree(const TCoEquiJoinTuple& joinTuple,
    const TVector<std::shared_ptr<TRelOptimizerNode>>& rels) {

    std::shared_ptr<IBaseOptimizerNode> left;
    std::shared_ptr<IBaseOptimizerNode> right;


    if (joinTuple.LeftScope().Maybe<TCoEquiJoinTuple>()) {
        left = ConvertToJoinTree(joinTuple.LeftScope().Cast<TCoEquiJoinTuple>(), rels);
    }
    else {
        auto scope = joinTuple.LeftScope().Cast<TCoAtom>().StringValue();
        auto it = find_if(rels.begin(), rels.end(), [scope] (const std::shared_ptr<TRelOptimizerNode>& n) {
            return scope == n->Label;
        } );
        left = *it;
    }

    if (joinTuple.RightScope().Maybe<TCoEquiJoinTuple>()) {
        right = ConvertToJoinTree(joinTuple.RightScope().Cast<TCoEquiJoinTuple>(), rels);
    }
    else {
        auto scope = joinTuple.RightScope().Cast<TCoAtom>().StringValue();
        auto it = find_if(rels.begin(), rels.end(), [scope] (const std::shared_ptr<TRelOptimizerNode>& n) {
            return scope == n->Label;
        } );
        right =  *it;
    }

    std::set<std::pair<TJoinColumn, TJoinColumn>> joinConds;

    size_t joinKeysCount = joinTuple.LeftKeys().Size() / 2;
    for (size_t i = 0; i < joinKeysCount; ++i) {
        size_t keyIndex = i * 2;

        auto leftScope = joinTuple.LeftKeys().Item(keyIndex).StringValue();
        auto leftColumn = joinTuple.LeftKeys().Item(keyIndex + 1).StringValue();
        auto rightScope = joinTuple.RightKeys().Item(keyIndex).StringValue();
        auto rightColumn = joinTuple.RightKeys().Item(keyIndex + 1).StringValue();

        joinConds.insert( std::make_pair( TJoinColumn(leftScope, leftColumn),
            TJoinColumn(rightScope, rightColumn)));
    }

    return std::make_shared<TJoinOptimizerNode>(left, right, joinConds, ConvertToJoinKind(joinTuple.Type().StringValue()), EJoinAlgoType::DictJoin);
}

/**
 * Extract all non orderable joins from a plan is a post-order traversal order
*/
void ExtractNonOrderables(std::shared_ptr<TJoinOptimizerNode> joinTree,
    TVector<std::shared_ptr<TJoinOptimizerNode>>& result) {

        if (joinTree->LeftArg->Kind == EOptimizerNodeKind::JoinNodeType) {
            auto left = static_pointer_cast<TJoinOptimizerNode>(joinTree->LeftArg);
            ExtractNonOrderables(left, result);
        }
        if (joinTree->RightArg->Kind == EOptimizerNodeKind::JoinNodeType) {
            auto right = static_pointer_cast<TJoinOptimizerNode>(joinTree->RightArg);
            ExtractNonOrderables(right, result);
        }
        if (!joinTree->IsReorderable)
        {
            result.emplace_back(joinTree);
        }
}

/**
 * Extract relations and join conditions from an optimizer plan
 * If we hit a non-orderable join type in recursion, we don't recurse into it and
 * add it as a relation
*/
void ExtractRelsAndJoinConditions(const std::shared_ptr<TJoinOptimizerNode>& joinTree,
    TVector<std::shared_ptr<IBaseOptimizerNode>> & rels,
    std::set<std::pair<TJoinColumn, TJoinColumn>>& joinConditions) {
        if (!joinTree->IsReorderable){
            rels.emplace_back( joinTree );
            return;
        }

        for (auto c : joinTree->JoinConditions) {
            joinConditions.insert(c);
        }

        if (joinTree->LeftArg->Kind == EOptimizerNodeKind::JoinNodeType) {
            ExtractRelsAndJoinConditions(static_pointer_cast<TJoinOptimizerNode>(joinTree->LeftArg), rels, joinConditions);
        }
        else {
            rels.emplace_back(joinTree->LeftArg);
        }

        if (joinTree->RightArg->Kind == EOptimizerNodeKind::JoinNodeType) {
            ExtractRelsAndJoinConditions(static_pointer_cast<TJoinOptimizerNode>(joinTree->RightArg), rels, joinConditions);
        }
        else {
            rels.emplace_back(joinTree->RightArg);
        }
    }

/**
 * Recursively computes statistics for a join tree
*/
void ComputeStatistics(const std::shared_ptr<TJoinOptimizerNode>& join, IProviderContext& ctx) {
    if (join->LeftArg->Kind == EOptimizerNodeKind::JoinNodeType) {
        ComputeStatistics(static_pointer_cast<TJoinOptimizerNode>(join->LeftArg), ctx);
    }
    if (join->RightArg->Kind == EOptimizerNodeKind::JoinNodeType) {
        ComputeStatistics(static_pointer_cast<TJoinOptimizerNode>(join->RightArg), ctx);
    }
    join->Stats = std::make_shared<TOptimizerStatistics>(ComputeJoinStats(*join->LeftArg->Stats, *join->RightArg->Stats,
        join->LeftJoinKeys, join->RightJoinKeys, EJoinAlgoType::DictJoin, ctx));
}

/**
 * Optimize a subtree of a plan with DPccp
 * The root of the subtree that needs to be optimizer needs to be reorderable, otherwise we will
 * only update the statistics for it and return it unchanged
*/
std::shared_ptr<TJoinOptimizerNode> OptimizeSubtree(const std::shared_ptr<TJoinOptimizerNode>& joinTree, ui32 maxDPccpDPTableSize, IProviderContext& ctx) {
    if (!joinTree->IsReorderable) {
        return PickBestNonReorderabeJoin(joinTree, ctx);
    }

    TVector<std::shared_ptr<IBaseOptimizerNode>> rels;
    std::set<std::pair<TJoinColumn, TJoinColumn>> joinConditions;
    ExtractRelsAndJoinConditions(joinTree, rels, joinConditions);

    TGraph<128> joinGraph;

    for (size_t i = 0; i < rels.size(); i++) {
        joinGraph.AddNode(i, rels[i]->Labels());
    }

    // Check if we have more rels than DPccp can handle (128)
    // If that's the case - don't optimize the plan and just return it with
    // computed statistics
    if (rels.size() >= 128) {
        ComputeStatistics(joinTree, ctx);
        YQL_CLOG(TRACE, CoreDq) << "Too many rels";
        return joinTree;
    }

    for (auto cond : joinConditions ) {
        int fromNode = joinGraph.FindNode(cond.first.RelName);
        int toNode = joinGraph.FindNode(cond.second.RelName);
        joinGraph.AddEdge(TEdge(fromNode, toNode, cond));
    }

     if (NYql::NLog::YqlLogger().NeedToLog(NYql::NLog::EComponent::CoreDq, NYql::NLog::ELevel::TRACE)) {
        std::stringstream str;
        str << "Initial join graph:\n";
        joinGraph.PrintGraph(str);
        YQL_CLOG(TRACE, CoreDq) << str.str();
    }

    // make a transitive closure of the graph and reorder the graph via BFS
    joinGraph.ComputeTransitiveClosure(joinConditions);

    if (NYql::NLog::YqlLogger().NeedToLog(NYql::NLog::EComponent::CoreDq, NYql::NLog::ELevel::TRACE)) {
        std::stringstream str;
        str << "Join graph after transitive closure:\n";
        joinGraph.PrintGraph(str);
        YQL_CLOG(TRACE, CoreDq) << str.str();
    }

    TDPccpSolver<128> solver(joinGraph, rels, ctx);

    // Check that the dynamic table of DPccp is not too big
    // If it is, just compute the statistics for the join tree and return it
    if (solver.CountCC(maxDPccpDPTableSize) >= maxDPccpDPTableSize) {
        ComputeStatistics(joinTree, ctx);
        return joinTree;
    }

    // feed the graph to DPccp algorithm
    auto result = solver.Solve();

    if (NYql::NLog::YqlLogger().NeedToLog(NYql::NLog::EComponent::CoreDq, NYql::NLog::ELevel::TRACE)) {
        std::stringstream str;
        str << "Join tree after cost based optimization:\n";
        result->Print(str);
        YQL_CLOG(TRACE, CoreDq) << str.str();
    }

    return ConvertFromInternal(result);
}

class TOptimizerNativeNew: public IOptimizerNew {
public:
    TOptimizerNativeNew(IProviderContext& ctx, const ui32 maxDPccpDPTableSize)
        : IOptimizerNew(ctx), MaxDPccpDPTableSize(maxDPccpDPTableSize) { }

    std::shared_ptr<TJoinOptimizerNode>  JoinSearch(const std::shared_ptr<TJoinOptimizerNode>& joinTree) override {

        // Traverse the join tree and generate a list of non-orderable joins in a post-order
        TVector<std::shared_ptr<TJoinOptimizerNode>> nonOrderables;
        ExtractNonOrderables(joinTree, nonOrderables);

        // For all non-orderable joins, optimize the children
        for( auto join : nonOrderables ) {
            if (join->LeftArg->Kind == EOptimizerNodeKind::JoinNodeType) {
                join->LeftArg = OptimizeSubtree(static_pointer_cast<TJoinOptimizerNode>(join->LeftArg), MaxDPccpDPTableSize, Pctx);
            }
            if (join->RightArg->Kind == EOptimizerNodeKind::JoinNodeType) {
                join->RightArg = OptimizeSubtree(static_pointer_cast<TJoinOptimizerNode>(join->RightArg), MaxDPccpDPTableSize, Pctx);
            }
            join->Stats = std::make_shared<TOptimizerStatistics>(ComputeJoinStats(*join->LeftArg->Stats, *join->RightArg->Stats,
                join->LeftJoinKeys, join->RightJoinKeys, EJoinAlgoType::DictJoin, Pctx));
        }

        // Optimize the root
        return OptimizeSubtree(joinTree, MaxDPccpDPTableSize, Pctx);
    }

    const ui32 MaxDPccpDPTableSize;
};

/**
 * Main routine that checks:
 * 1. Do we have an equiJoin
 * 2. Is the cost already computed
 * 3. Are all the costs of equiJoin inputs computed?
 *
 * Then it optimizes the join tree by iterating over all non-orderable nodes and optimizing their children,
 * and finally optimizes the root of the tree
*/
TExprBase DqOptimizeEquiJoinWithCosts(const TExprBase& node, TExprContext& ctx, TTypeAnnotationContext& typesCtx,
    ui32 optLevel, IOptimizerNew& opt,
    const std::function<void(TVector<std::shared_ptr<TRelOptimizerNode>>&, TStringBuf, const TExprNode::TPtr, const std::shared_ptr<TOptimizerStatistics>&)>& providerCollect) {

    if (optLevel==0) {
        return node;
    }

    if (!node.Maybe<TCoEquiJoin>()) {
        return node;
    }
    auto equiJoin = node.Cast<TCoEquiJoin>();
    YQL_ENSURE(equiJoin.ArgCount() >= 4);

    if (typesCtx.StatisticsMap.contains(equiJoin.Raw())) {

        return node;
    }

    YQL_CLOG(TRACE, CoreDq) << "Optimizing join with costs";

    TVector<std::shared_ptr<TRelOptimizerNode>> rels;

    // Check that statistics for all inputs of equiJoin were computed
    // The arguments of the EquiJoin are 1..n-2, n-2 is the actual join tree
    // of the EquiJoin and n-1 argument are the parameters to EquiJoin

    if (!DqCollectJoinRelationsWithStats(rels, typesCtx, equiJoin, providerCollect)){
        return node;
    }

    YQL_CLOG(TRACE, CoreDq) << "All statistics for join in place";

    auto joinTuple = equiJoin.Arg(equiJoin.ArgCount() - 2).Cast<TCoEquiJoinTuple>();

    // Generate an initial tree
    auto joinTree = ConvertToJoinTree(joinTuple,rels);

    if (NYql::NLog::YqlLogger().NeedToLog(NYql::NLog::EComponent::ProviderKqp, NYql::NLog::ELevel::TRACE)) {
        std::stringstream str;
        str << "Converted join tree:\n";
        joinTree->Print(str);
        YQL_CLOG(TRACE, CoreDq) << str.str();
    }

    joinTree = opt.JoinSearch(joinTree);

    // rewrite the join tree and record the output statistics
    TExprBase res = RearrangeEquiJoinTree(ctx, equiJoin, joinTree);
    typesCtx.StatisticsMap[ res.Raw() ] = joinTree->Stats;
    return res;
}

IOptimizerNew* MakeNativeOptimizerNew(IProviderContext& ctx, const ui32 maxDPccpDPTableSize) {
    return new TOptimizerNativeNew(ctx, maxDPccpDPTableSize);
}

} // namespace NYql::NDq
