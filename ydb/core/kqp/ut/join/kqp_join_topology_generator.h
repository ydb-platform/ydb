#pragma once

#include <util/stream/output.h>
#include <util/string/builder.h>
#include <ydb/core/kqp/ut/common/kqp_serializable_rng.h>
#include <library/cpp/json/writer/json.h>
#include <library/cpp/json/writer/json_value.h>

#include <queue>
#include <random>
#include <set>
#include <vector>

namespace NKikimr::NKqp {

using TRNG = TSerializableMT19937;

class TLexicographicalNameGenerator {
public:
    static std::string getName(unsigned id, bool lowerCase = true) {
        if (id < Base_) {
            return std::string(1, fromDigit(id, lowerCase));
        }

        id -= Base_;

        unsigned count = 1;
        unsigned step = Base_;
        for (; id >= step;) {
            id -= step;
            step *= step;
            count *= 2;
        }

        std::string result(count, fromDigit(Base_ - 1, lowerCase));
        return result + fromNumber(id, result.size(), lowerCase);
    }

private:
    static std::string fromNumber(unsigned number, unsigned size, bool lowerCase) {
        std::string stringified = "";
        for (unsigned i = 0; i < size; ++ i) {
            stringified.push_back(fromDigit(number % Base_, lowerCase));
            number /= Base_;
        }

        return std::string(stringified.rbegin(), stringified.rend());
    }

    static char fromDigit(unsigned value, bool lowerCase) {
        Y_ASSERT(0 <= value && value < Base_);
        return (lowerCase ? 'a' : 'A') + value;
    }

    static constexpr unsigned Base_ = 'z' - 'a' + 1;
};


// Tables and columns are stored as numbers, this functions
// convert them to strings for usage in queries and humans:
std::string getTableName(unsigned tableID);
std::string getColumnName(unsigned tableID, unsigned columnID);
std::string getRelationName(unsigned tableID, unsigned columnID);
std::string getTablePath(unsigned tableID);


struct TPitmanYorConfig {
    double Alpha = 0.0;
    double Theta = 0.0;
    double Assortativity = 0.0;

    void DumpParamsHeader(IOutputStream& os) {
        os << "alpha,theta,assortativity";
    }

    void DumpParams(IOutputStream& os) {
        os << Alpha << "," << Theta << "," << Assortativity;
    }
};

class TTable {
public:
    TTable(unsigned numColumns = 0)
        : NumColumns_(numColumns)
    {
    }

    unsigned GetNumColumns() const {
        return NumColumns_;
    }

private:
    unsigned NumColumns_;
};

class TSchema {
public:
    TSchema(unsigned numNodes)
        : Tables_(numNodes)
    {
    }

    TSchema(std::vector<TTable> tables)
        : Tables_(std::move(tables))
    {
    }

    static TSchema MakeWithEnoughColumns(unsigned numNodes);

    std::string MakeCreateQuery() const;
    std::string MakeDropQuery() const;

    TTable& operator[](unsigned index) {
        return Tables_[index];
    }

    const TTable& operator[](unsigned index) const {
        return Tables_[index];
    }

    size_t GetSize() const {
        return Tables_.size();
    }

    void Rename(std::vector<int> oldToNew);

private:
    std::vector<TTable> Tables_;
};

struct TPitmanYorNodeState {
    std::vector<ui32> Counts;      // How many edges use Key[i]
    std::vector<ui32> FreeKeys;    // Recycled keys to keep vector small
    ui32 TotalEdges = 0;           // Total active edges ("customers")
    ui32 ActiveTables = 0;         // How many keys have Count > 0

    ui32 GenerateKey(TRNG& rng, TPitmanYorConfig config, double forceQuantile = -1.0);
    void ReleaseKey(ui32 key);
};


class TRelationGraph {
public:
    TRelationGraph(unsigned numNodes)
        : AdjacencyList_(numNodes)
        , Schema_(numNodes)
        , NodeStates_(numNodes)
    {
    }

    void Connect(TRNG& rng, unsigned u, unsigned v, TPitmanYorConfig config);
    void Disconnect(unsigned u, unsigned v);
    void Rewire(TRNG& rng, unsigned u, unsigned oldV, unsigned newV, TPitmanYorConfig config);

    bool HasEdge(unsigned u, unsigned v) const;

    std::string MakeQuery() const;

    ui32 GetNumEdges() const;
    unsigned GetN() const {
        return AdjacencyList_.size();
    }

    std::vector<int> FindComponents() const;
    int GetNumComponents() const;
    bool IsConnected() const {
        return GetNumComponents() == 1;
    }

    const TSchema& GetSchema() const {
        return Schema_;
    }

    std::vector<int> GetDegrees() const;

    // Reorder in connected order, meaning that first N vertices form
    // a connected subgraph if the whole graph is connected. This is used
    // to ensure that each JOIN clause only mentions tables that where
    // already joined (or FROM clause)
    void ReorderDFS();

    // Update vertex numbering accroding to oldToNew map, primarily
    // used to reorder graph in connected subgraphs-first order.
    void Rename(const std::vector<int>& oldToNew);

    // Dump graph in undirected graphviz dot format. Neato is recommended
    // for layouting such graphs.
    void DumpGraph(IOutputStream& os) const;

public:
    struct TEdge {
        unsigned Target;
        unsigned ColumnLHS, ColumnRHS;
    };

    using TAdjacencyList = std::vector<std::vector<TEdge>>;

public:
    const TAdjacencyList& GetAdjacencyList() const {
        return AdjacencyList_;
    }

private:
    TAdjacencyList AdjacencyList_;
    TSchema Schema_;

    // Stores live distribution state for every node
    std::vector<TPitmanYorNodeState> NodeStates_;

    void RemoveEdgeFromList(unsigned owner, unsigned target);
};

class TSchemaStats {
public:
    struct TTableStats {
        unsigned ByteSize;
        unsigned RowSize;
    };

public:
    TSchemaStats(std::vector<TTableStats> stats)
        : Stats_(std::move(stats))
    {
    }

    static TSchemaStats MakeRandom(TRNG& rng, const TSchema& schema, unsigned a, unsigned b);

    std::string ToJSON() const;

private:
    std::vector<TTableStats> Stats_;
};

class TRelationGraphSerializer {
public:
    static NJson::TJsonValue Serialize(const TRelationGraph& graph) {
        NJson::TJsonValue root(NJson::JSON_MAP);

        root.InsertValue("nodes", SerializeNodes(graph));
        root.InsertValue("edges", SerializeEdges(graph));
        return root;
    }

    static NJson::TJsonValue SerializeCompact(const TRelationGraph& graph) {
        NJson::TJsonValue root(NJson::JSON_MAP);

        root.InsertValue("nodes", NJson::TJsonValue(static_cast<int>(graph.GetN())));
        root.InsertValue("edges", SerializeEdgesCompact(graph));

        return root;
    }

private:
    static NJson::TJsonValue SerializeNodes(const TRelationGraph& graph) {
        NJson::TJsonValue nodesArray(NJson::JSON_ARRAY);

        for (size_t i = 0; i < graph.GetN(); ++i) {
            NJson::TJsonValue node(NJson::JSON_MAP);
            node.InsertValue("id", NJson::TJsonValue(static_cast<int>(i)));
            node.InsertValue("name", NJson::TJsonValue(getTableName(i)));
            node.InsertValue("columns", NJson::TJsonValue(static_cast<int>(graph.GetSchema()[i].GetNumColumns())));

            nodesArray.AppendValue(node);
        }

        return nodesArray;
    }

    static NJson::TJsonValue SerializeEdges(const TRelationGraph& graph) {
        NJson::TJsonValue edgesArray(NJson::JSON_ARRAY);
        const auto& adjacencyList = graph.GetAdjacencyList();

        // Only serialize each edge once (u < v to avoid duplicates)
        for (size_t u = 0; u < adjacencyList.size(); ++u) {
            for (const auto& edge : adjacencyList[u]) {
                if (u < edge.Target) {
                    NJson::TJsonValue edgeJson(NJson::JSON_MAP);

                    edgeJson.InsertValue("source", NJson::TJsonValue(static_cast<int>(u)));
                    edgeJson.InsertValue("target", NJson::TJsonValue(static_cast<int>(edge.Target)));
                    edgeJson.InsertValue("source_column", NJson::TJsonValue(static_cast<int>(edge.ColumnLHS)));
                    edgeJson.InsertValue("target_column", NJson::TJsonValue(static_cast<int>(edge.ColumnRHS)));

                    TString condition = TStringBuilder()
                        << getRelationName(u, edge.ColumnLHS)
                        << " = "
                        << getRelationName(edge.Target, edge.ColumnRHS);

                    edgeJson.InsertValue("condition", NJson::TJsonValue(condition));

                    edgesArray.AppendValue(edgeJson);
                }
            }
        }

        return edgesArray;
    }

    static NJson::TJsonValue SerializeEdgesCompact(const TRelationGraph& graph) {
        NJson::TJsonValue edgesArray(NJson::JSON_ARRAY);
        const auto& adjacencyList = graph.GetAdjacencyList();

        for (size_t u = 0; u < adjacencyList.size(); ++u) {
            for (const auto& edge : adjacencyList[u]) {
                if (u < edge.Target) {
                    NJson::TJsonValue edgeArray(NJson::JSON_ARRAY);
                    edgeArray.AppendValue(NJson::TJsonValue(static_cast<int>(u)));
                    edgeArray.AppendValue(NJson::TJsonValue(static_cast<int>(edge.Target)));
                    edgeArray.AppendValue(NJson::TJsonValue(static_cast<int>(edge.ColumnLHS)));
                    edgeArray.AppendValue(NJson::TJsonValue(static_cast<int>(edge.ColumnRHS)));

                    edgesArray.AppendValue(edgeArray);
                }
            }
        }

        return edgesArray;
    }
};

// Basic topologies, this all have fixed node layouts (not random)
// TODO: unsigned?
TRelationGraph GeneratePath(TRNG& rng, unsigned numNodes, TPitmanYorConfig config);
TRelationGraph GenerateStar(TRNG& rng, unsigned numNodes, TPitmanYorConfig config);
TRelationGraph GenerateClique(TRNG& rng, unsigned numNodes, TPitmanYorConfig config);

// Generate a tree from Prufer sequence (each labeled tree has a
// corresponding unique sequence)
TRelationGraph GenerateTreeFromPruferSequence(TRNG& rng, const std::vector<unsigned>& prufer, TPitmanYorConfig config);

// Uniformly random trees based on random Prufer sequence
TRelationGraph GenerateRandomTree(TRNG& rng, unsigned numNodes, TPitmanYorConfig config);

// Random graph using Chung Lu model that approximates graph with given degrees
TRelationGraph GenerateRandomChungLuGraph(TRNG& rng, const std::vector<int>& degrees, TPitmanYorConfig config);

std::vector<double> GenerateLogNormalProbabilities(TRNG& rng, double mu, double sigma);

// Sample a degree sequence from lognormal distribution
// TODO: is it necessarily a degree sequence?
std::vector<int> GenerateLogNormalDegrees(
    TRNG& rng, int numVertices,
    double mu = 1.0, double sigma = 0.5,
    int minDegree = 1, int maxDegree = -1);

// Adjust degree sequence to make it graphic (realizable by simple graph
// without self-loops and double edges) and check that it's likely possible
// to make a connected graph with that degree sequence
// (athough this property is not guaranteed)
std::vector<int> MakeGraphicConnected(std::vector<int> degrees);

// Deterministically constructs graph for a given degree sequence
TRelationGraph ConstructGraphHavelHakimi(TRNG &rng, std::vector<int> degrees, TPitmanYorConfig config);

// Sometimes, even though we tried, we couldn't get graph to be connected.
// This connects components randomly until it becomes connected as a last resort,
// if anything else fails (like simulated annealing in MCMC)
void ForceReconnection(TRNG& rng, TRelationGraph& graph, TPitmanYorConfig config);

// =================== Markov chain Monte Carlo ===================

// Randomize graph using ~E*log(E) l-switches (preserve degrees of all
// verticies) Uses Metropolis-Hastings based acceptance with annealing (to make
// switching edges ergodic and still produce connected graphs)
void MCMCRandomize(TRNG& rng, TRelationGraph& graph, TPitmanYorConfig config);

} // namespace NKikimr::NKqp
