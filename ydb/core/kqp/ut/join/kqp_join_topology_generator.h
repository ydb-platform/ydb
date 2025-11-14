#pragma once

#include <util/stream/output.h>
#include <ydb/core/kqp/ut/common/kqp_serializable_rng.h>

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

struct TPitmanYorConfig {
    double Alpha;
    double Theta;

    void DumpParamsHeader(IOutputStream& os) {
        os << "alpha,theta";
    }

    void DumpParams(IOutputStream& os) {
        os << Alpha << "," << Theta;
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

    size_t GetSize() const {
        return Tables_.size();
    }

    void Rename(std::vector<int> oldToNew);

private:
    std::vector<TTable> Tables_;
};

class TRelationGraph {
public:
    TRelationGraph(unsigned numNodes)
        : AdjacencyList_(numNodes)
        , Schema_(numNodes)
    {
    }

    void Connect(unsigned lhs, unsigned rhs);
    void Disconnect(unsigned u, unsigned v);
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

    // Update keys for the whole graph accroding to Pitman-Yor distribution, 
    // where degree is distributed into clusters and each cluster means
    // that that number of edges joins this particular node with the same key
    void SetupKeysPitmanYor(TRNG& rng, TPitmanYorConfig config);

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
    TAdjacencyList& GetAdjacencyList() {
        return AdjacencyList_;
    }

private:
    TAdjacencyList AdjacencyList_;
    TSchema Schema_;
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

// Basic topologies, this all have fixed node layouts (not random)
TRelationGraph GeneratePath(unsigned numNodes);
TRelationGraph GenerateStar(unsigned numNodes);
TRelationGraph GenerateClique(unsigned numNodes);

// Generate a tree from Prufer sequence (each labeled tree has a
// corresponding unique sequence)
TRelationGraph GenerateTreeFromPruferSequence(const std::vector<unsigned>& prufer);

// Uniformly random trees based on random Prufer sequence
TRelationGraph GenerateRandomTree(TRNG& rng, unsigned numNodes);

// Random graph using Chung Lu model that approximates graph with given degrees
TRelationGraph GenerateRandomChungLuGraph(TRNG& rng, const std::vector<int>& degrees);

// Sample a degree sequence from a given probability distribution
std::vector<int> SampleFromPMF(
    TRNG& rng,
    const std::vector<double>& probabilities,
    int numVertices, int minDegree);

// Sample a degree sequence from lognormal distribution
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
TRelationGraph ConstructGraphHavelHakimi(std::vector<int> degrees);

// Randomize graph using ~E*log(E) l-switches (preserve degrees of all
// verticies) Uses Metropolis-Hastings based acceptance with annealing (to make
// switching edges ergodic and still produce connected graphs)
void MCMCRandomize(TRNG& rng, TRelationGraph& graph);

} // namespace NKikimr::NKqp
