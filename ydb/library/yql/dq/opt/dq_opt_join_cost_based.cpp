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

#include <ydb/library/yql/core/cbo/cbo_optimizer.h> //interface

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
 * Join column is a struct that records the relation label and 
 * attribute name, used in join conditions
*/
struct TJoinColumn {
    TString RelName;
    TString AttributeName;

    TJoinColumn(TString relName, TString attributeName) : RelName(relName), 
        AttributeName(attributeName) {}

    bool operator == (const TJoinColumn& other) const {
        return RelName == other.RelName && AttributeName == other.AttributeName;
    }

    struct HashFunction
    {
        size_t operator()(const TJoinColumn& c) const
        {
            return THash<TString>{}(c.RelName) ^ THash<TString>{}(c.AttributeName);
        }
    };
};

bool operator < (const TJoinColumn& c1, const TJoinColumn& c2) {
    if (c1.RelName < c2.RelName){
        return true;
    } else if (c1.RelName == c2.RelName) {
        return c1.AttributeName < c2.AttributeName;
    }
    return false;
}

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

    TEdge(int f, int t): From(f), To(t) {}

    TEdge(int f, int t, std::pair<TJoinColumn, TJoinColumn> cond): From(f), To(t) {
        JoinConditions.insert(cond);
    }

    TEdge(int f, int t, std::set<std::pair<TJoinColumn, TJoinColumn>> conds): From(f), To(t), 
        JoinConditions(conds) {}

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
 * OptimizerNodes are the internal representations of operators inside the
 * Cost-based optimizer. Currently we only support RelOptimizerNode - a node that
 * is an input relation to the equi-join, and JoinOptimizerNode - an inner join 
 * that connects two sets of relations.
*/
enum EOptimizerNodeKind: ui32
{
    RelNodeType,
    JoinNodeType
};

/**
 * BaseOptimizerNode is a base class for the internal optimizer nodes
 * It records a pointer to statistics and records the current cost of the
 * operator tree, rooted at this node
*/
struct IBaseOptimizerNode {
    EOptimizerNodeKind Kind;
    std::shared_ptr<TOptimizerStatistics> Stats;

    IBaseOptimizerNode(EOptimizerNodeKind k) : Kind(k) {}
    IBaseOptimizerNode(EOptimizerNodeKind k, std::shared_ptr<TOptimizerStatistics> s) : 
        Kind(k), Stats(s) {}

    virtual TVector<TString> Labels()=0;
    virtual void Print(std::stringstream& stream, int ntabs=0)=0;
};

/**
 * RelOptimizerNode adds a label to base class
 * This is the label assinged to the input by equi-Join
*/
struct TRelOptimizerNode : public IBaseOptimizerNode {
    TString Label;

    TRelOptimizerNode(TString label, std::shared_ptr<TOptimizerStatistics> stats) : 
        IBaseOptimizerNode(RelNodeType, stats), Label(label) { }
    virtual ~TRelOptimizerNode() {}

    virtual TVector<TString> Labels()  {
        TVector<TString> res;
        res.emplace_back(Label);
        return res;
    }

    virtual void Print(std::stringstream& stream, int ntabs=0) {
        for (int i = 0; i < ntabs; i++){
            stream << "\t";
        }
        stream << "Rel: " << Label << "\n";

        for (int i = 0; i < ntabs; i++){
            stream << "\t";
        }
        stream << *Stats << "\n";
    }
};

/**
 * JoinOptimizerNode records the left and right arguments of the join
 * as well as the set of join conditions.
 * It also has methods to compute the statistics and cost of a join,
 * based on pre-computed costs and statistics of the children.
*/
struct TJoinOptimizerNode : public IBaseOptimizerNode {
    std::shared_ptr<IBaseOptimizerNode> LeftArg;
    std::shared_ptr<IBaseOptimizerNode> RightArg;
    std::set<std::pair<TJoinColumn, TJoinColumn>> JoinConditions;
    TString JoinType;

    TJoinOptimizerNode(const std::shared_ptr<IBaseOptimizerNode>& left, const std::shared_ptr<IBaseOptimizerNode>& right, 
        const std::set<std::pair<TJoinColumn, TJoinColumn>>& joinConditions, const TString& joinType) : 
        IBaseOptimizerNode(JoinNodeType), LeftArg(left), RightArg(right), JoinConditions(joinConditions), JoinType(joinType) {}
    virtual ~TJoinOptimizerNode() {}

    virtual TVector<TString> Labels() {
        auto res = LeftArg->Labels();
        auto rightLabels = RightArg->Labels();
        res.insert(res.begin(),rightLabels.begin(),rightLabels.end());
        return res;
    }

    bool Reorderable() {
        return JoinType == "Inner";
    }
    /**
     * Print out the join tree, rooted at this node
    */
    virtual void Print(std::stringstream& stream, int ntabs=0) {
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

        stream << *Stats << "\n";

        LeftArg->Print(stream, ntabs+1);
        RightArg->Print(stream, ntabs+1);
    }
};


/**
 * Create a new join and compute its statistics and cost
*/
std::shared_ptr<TJoinOptimizerNode> MakeJoin(std::shared_ptr<IBaseOptimizerNode> left, 
    std::shared_ptr<IBaseOptimizerNode> right, 
    const std::set<std::pair<TJoinColumn, TJoinColumn>>& joinConditions,
    EJoinImplType joinImpl) {

    auto res = std::make_shared<TJoinOptimizerNode>(left, right, joinConditions, "Inner");
    res->Stats = std::make_shared<TOptimizerStatistics>( ComputeJoinStats(*left->Stats, *right->Stats, joinImpl));
    return res;
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
    TEdge FindCrossingEdge(const std::bitset<N>& S1, const std::bitset<N>& S2) {
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
        return TEdge(-1,-1);
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
                        maybeEdge1->JoinConditions.emplace(left, right);
                        maybeEdge2->JoinConditions.emplace(right, left);
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
    TDPccpSolver(TGraph<N>& g, TVector<std::shared_ptr<IBaseOptimizerNode>> rels): 
        Graph(g), Rels(rels) {
        NNodes = g.NNodes;
    }

    // Run DPccp algorithm and produce the join tree in CBO's internal representation
    std::shared_ptr<TJoinOptimizerNode> Solve();

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
template<int N> std::shared_ptr<TJoinOptimizerNode> TDPccpSolver<N>::Solve() 
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
    return std::static_pointer_cast<TJoinOptimizerNode>(DpTable[V]); 
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

    if (! DpTable.contains(joined)) {
        TEdge e1 = Graph.FindCrossingEdge(S1, S2);
        DpTable[joined] = MakeJoin(DpTable[S1], DpTable[S2], e1.JoinConditions, GraceJoin);
        TEdge e2 = Graph.FindCrossingEdge(S2, S1);
        std::shared_ptr<TJoinOptimizerNode> newJoin = 
            MakeJoin(DpTable[S2], DpTable[S1], e2.JoinConditions, GraceJoin);
        if (newJoin->Stats->Cost < DpTable[joined]->Stats->Cost){
            DpTable[joined] = newJoin;
        }
    } else {
        TEdge e1 = Graph.FindCrossingEdge(S1, S2);
        std::shared_ptr<TJoinOptimizerNode> newJoin1 =
             MakeJoin(DpTable[S1], DpTable[S2], e1.JoinConditions, GraceJoin);
        TEdge e2 = Graph.FindCrossingEdge(S2, S1);
        std::shared_ptr<TJoinOptimizerNode> newJoin2 = 
            MakeJoin(DpTable[S2], DpTable[S1], e2.JoinConditions, GraceJoin);
        if (newJoin1->Stats->Cost < DpTable[joined]->Stats->Cost){
            DpTable[joined] = newJoin1;
        }
        if (newJoin2->Stats->Cost < DpTable[joined]->Stats->Cost){
            DpTable[joined] = newJoin2;
        }
    }

    auto pair = std::make_pair(S1, S2);
    Y_ENSURE (!CheckTable.contains(pair), "Check table already contains pair S1|S2");
    
    CheckTable[ std::pair<std::bitset<N>,std::bitset<N>>(S1, S2) ] = true;
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

    TVector<TExprBase> options;

    // Build the final output
    return Build<TCoEquiJoinTuple>(ctx,equiJoin.Pos())
        .Type(BuildAtom(reorderResult->JoinType,equiJoin.Pos(),ctx))
        .LeftScope(leftArg)
        .RightScope(rightArg)
        .LeftKeys()
            .Add(leftJoinColumns)
            .Build()
        .RightKeys()
            .Add(rightJoinColumns)
            .Build()
        .Options()
            .Add(options)
            .Build()
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

bool DqCollectJoinRelationsWithStats(
    TTypeAnnotationContext& typesCtx, 
    const TCoEquiJoin& equiJoin, 
    const std::function<void(TStringBuf, const std::shared_ptr<TOptimizerStatistics>&)>& collector) 
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
        collector(label, stats);
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

    return std::make_shared<TJoinOptimizerNode>(left,right,joinConds,joinTuple.Type().StringValue());
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
        if (!joinTree->Reorderable())
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
        if (!joinTree->Reorderable()){
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
void ComputeStatistics(const std::shared_ptr<TJoinOptimizerNode>& join) {
    if (join->LeftArg->Kind == EOptimizerNodeKind::JoinNodeType) {
        ComputeStatistics(static_pointer_cast<TJoinOptimizerNode>(join->LeftArg));
    }
    if (join->RightArg->Kind == EOptimizerNodeKind::JoinNodeType) {
        ComputeStatistics(static_pointer_cast<TJoinOptimizerNode>(join->RightArg));
    }
    join->Stats = std::make_shared<TOptimizerStatistics>(ComputeJoinStats(*join->LeftArg->Stats, *join->RightArg->Stats, EJoinImplType::DictJoin));
}

/**
 * Optimize a subtree of a plan with DPccp
 * The root of the subtree that needs to be optimizer needs to be reorderable, otherwise we will
 * only update the statistics for it and return it unchanged
*/
std::shared_ptr<TJoinOptimizerNode> OptimizeSubtree(const std::shared_ptr<TJoinOptimizerNode>& joinTree, ui32 maxDPccpDPTableSize) {
    if (!joinTree->Reorderable()) {
        joinTree->Stats = std::make_shared<TOptimizerStatistics>(ComputeJoinStats(*joinTree->LeftArg->Stats, *joinTree->RightArg->Stats, EJoinImplType::DictJoin));
        return joinTree;
    }

    TGraph<64> joinGraph;
    TVector<std::shared_ptr<IBaseOptimizerNode>> rels;
    std::set<std::pair<TJoinColumn, TJoinColumn>> joinConditions;

    ExtractRelsAndJoinConditions(joinTree, rels, joinConditions);

    for (size_t i = 0; i < rels.size(); i++) {
        joinGraph.AddNode(i, rels[i]->Labels());
    }

    // Check if we have more rels than DPccp can handle (64)
    // If that's the case - don't optimize the plan and just return it with
    // computed statistics
    if (rels.size() >= 64) {
        ComputeStatistics(joinTree);
        return joinTree;
    }

    for (auto cond : joinConditions ) {
        int fromNode = joinGraph.FindNode(cond.first.RelName);
        int toNode = joinGraph.FindNode(cond.second.RelName);
        joinGraph.AddEdge(TEdge(fromNode, toNode, cond));
    }

     if (NYql::NLog::YqlLogger().NeedToLog(NYql::NLog::EComponent::ProviderKqp, NYql::NLog::ELevel::TRACE)) {
        std::stringstream str;
        str << "Initial join graph:\n";
        joinGraph.PrintGraph(str);
        YQL_CLOG(TRACE, CoreDq) << str.str();
    }

    // make a transitive closure of the graph and reorder the graph via BFS
    joinGraph.ComputeTransitiveClosure(joinConditions);

    if (NYql::NLog::YqlLogger().NeedToLog(NYql::NLog::EComponent::ProviderKqp, NYql::NLog::ELevel::TRACE)) {
        std::stringstream str;
        str << "Join graph after transitive closure:\n";
        joinGraph.PrintGraph(str);
        YQL_CLOG(TRACE, CoreDq) << str.str();
    }

    TDPccpSolver<64> solver(joinGraph,rels);

    // Check that the dynamic table of DPccp is not too big
    // If it is, just compute the statistics for the join tree and return it
    if (solver.CountCC(maxDPccpDPTableSize) >= maxDPccpDPTableSize) {
        ComputeStatistics(joinTree);
        return joinTree;
    }

    // feed the graph to DPccp algorithm
    std::shared_ptr<TJoinOptimizerNode> result = solver.Solve();

    if (NYql::NLog::YqlLogger().NeedToLog(NYql::NLog::EComponent::ProviderKqp, NYql::NLog::ELevel::TRACE)) {
        std::stringstream str;
        str << "Join tree after cost based optimization:\n";
        result->Print(str);
        YQL_CLOG(TRACE, CoreDq) << str.str();
    }

    return result;
}

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
    bool ruleEnabled, ui32 maxDPccpDPTableSize) {

    if (!ruleEnabled) {
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
    if (!DqCollectJoinRelationsWithStats(typesCtx, equiJoin, [&](auto label, auto stat) {
        rels.emplace_back(std::make_shared<TRelOptimizerNode>(TString(label), stat));
    })) {
        return node;
    }

    YQL_CLOG(TRACE, CoreDq) << "All statistics for join in place";

    auto joinTuple = equiJoin.Arg(equiJoin.ArgCount() - 2).Cast<TCoEquiJoinTuple>();

    // Generate an initial tree
    auto joinTree = ConvertToJoinTree(joinTuple,rels);

    // Traverse the join tree and generate a list of non-orderable joins in a post-order
    TVector<std::shared_ptr<TJoinOptimizerNode>> nonOrderables;
    ExtractNonOrderables(joinTree, nonOrderables);

    // For all non-orderable joins, optimize the children
    for( auto join : nonOrderables ) {
        if (join->LeftArg->Kind == EOptimizerNodeKind::JoinNodeType) {
            join->LeftArg = OptimizeSubtree(static_pointer_cast<TJoinOptimizerNode>(join->LeftArg), maxDPccpDPTableSize);
        }
        if (join->RightArg->Kind == EOptimizerNodeKind::JoinNodeType) {
            join->RightArg = OptimizeSubtree(static_pointer_cast<TJoinOptimizerNode>(join->RightArg), maxDPccpDPTableSize);
        }
        join->Stats = std::make_shared<TOptimizerStatistics>(ComputeJoinStats(*join->LeftArg->Stats, *join->RightArg->Stats, EJoinImplType::DictJoin));
    }

    // Optimize the root
    joinTree = OptimizeSubtree(joinTree, maxDPccpDPTableSize);

    // rewrite the join tree and record the output statistics
    TExprBase res = RearrangeEquiJoinTree(ctx, equiJoin, joinTree);
    typesCtx.StatisticsMap[ res.Raw() ] = joinTree->Stats;
    return res;
}

class TOptimizerNative: public IOptimizer {
public:
    TOptimizerNative(const IOptimizer::TInput& input, const std::function<void(const TString&)>& log)
        : Input(input)
        , Log(log)
    {
        Prepare();
    }

    TOutput JoinSearch() override {
        TDPccpSolver<64> solver(JoinGraph, Rels);
        std::shared_ptr<TJoinOptimizerNode> result = solver.Solve();
        if (Log) {
            std::stringstream str;
            str << "Join tree after cost based optimization:\n";
            result->Print(str);
            Log(str.str());
        }

        TOutput output;
        output.Input = &Input;
        TVector<int> scope;
        BuildOutput(&output, result.get(), scope);
        output.Rows = result->Stats->Nrows;
        output.TotalCost = result->Stats->Cost;
        if (Log) {
            Log(output.ToString());
        }
        return output;
    }

private:
    int BuildOutput(TOutput* output, IBaseOptimizerNode* node, TVector<int>& scope) {
        int index = (int)output->Nodes.size();
        TJoinNode r = output->Nodes.emplace_back();
        switch (node->Kind) {
        case EOptimizerNodeKind::RelNodeType: {
            // leaf
            TRelOptimizerNode* n = static_cast<TRelOptimizerNode*>(node);
            int relId = FromString<int>(n->Label);
            r.Rels.emplace_back(relId);
            scope.emplace_back(relId);
            break;
        }
        case EOptimizerNodeKind::JoinNodeType: {
            // node
            r.Mode = IOptimizer::EJoinType::Inner;
            TJoinOptimizerNode* n = static_cast<TJoinOptimizerNode*>(node);
            int index = scope.size();
            r.Outer = BuildOutput(output, n->LeftArg.get(), scope);
            r.Inner = BuildOutput(output, n->RightArg.get(), scope);

            for (auto& [col1, col2] : n->JoinConditions) {
                int relId1 = FromString<int>(col1.RelName);
                int colId1 = FromString<int>(col1.AttributeName);
                int relId2 = FromString<int>(col2.RelName);
                int colId2 = FromString<int>(col2.AttributeName);

                r.LeftVars.emplace_back(std::make_tuple(relId1, colId1));
                r.RightVars.emplace_back(std::make_tuple(relId2, colId2));
            }

            r.Rels.reserve(scope.size());
            r.Rels.insert(r.Rels.end(), scope.begin() + index, scope.end());
            break;
        }
        default:
            Y_ABORT_UNLESS(false);
        };
        output->Nodes[index] = r;
        return index;
    }

    void Prepare() {
        int index = 1;
        for (const auto& r : Input.Rels) {
            auto label = ToString(index++);
            auto stats = std::make_shared<TOptimizerStatistics>(r.Rows, r.TargetVars.size(), r.TotalCost);
            Rels.push_back(std::make_shared<TRelOptimizerNode>(label, stats));
        }

        for (size_t i = 0; i < Rels.size(); i++) {
            JoinGraph.AddNode(i, Rels[i]->Labels());
        }

        for (const auto& clazz : Input.EqClasses) {
            for (size_t i = 0; i < clazz.Vars.size(); i++) {
                auto [lrelId, lvarId] = clazz.Vars[i];
                int leftNodeId = lrelId - 1;
                auto left = TJoinColumn{ToString(lrelId), ToString(lvarId)};
                for (size_t j = 0; j < i; j++) {
                    auto [rrelId, rvarId] = clazz.Vars[j];
                    int rightNodeId = rrelId - 1;
                    auto right = TJoinColumn{ToString(rrelId), ToString(rvarId)};

                    auto maybeEdge1 = JoinGraph.Edges.find({leftNodeId, rightNodeId});
                    auto maybeEdge2 = JoinGraph.Edges.find({rightNodeId, leftNodeId});
                    if (maybeEdge1 == JoinGraph.Edges.end() && maybeEdge2 == JoinGraph.Edges.end()) {
                        JoinGraph.AddEdge(TEdge(leftNodeId, rightNodeId, std::make_pair(left, right)));
                    } else {
                        Y_ABORT_UNLESS(maybeEdge1 != JoinGraph.Edges.end() && maybeEdge2 != JoinGraph.Edges.end());
                        maybeEdge1->JoinConditions.emplace(left, right);
                        maybeEdge2->JoinConditions.emplace(right, left);
                    }
                }
            }
        }

        if (Log) {
            std::stringstream str;
            str << "Join graph after transitive closure:\n";
            JoinGraph.PrintGraph(str);
            Log(str.str());
        }
    }

    TInput Input;
    const std::function<void(const TString&)> Log;

    TVector<std::shared_ptr<IBaseOptimizerNode>> Rels;
    TGraph<64> JoinGraph;
};

IOptimizer* MakeNativeOptimizer(const IOptimizer::TInput& input, const std::function<void(const TString&)>& log) {
    return new TOptimizerNative(input, log);
}

} // namespace NYql::NDq

