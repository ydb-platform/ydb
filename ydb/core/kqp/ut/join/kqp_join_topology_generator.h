#pragma once

#include <util/stream/output.h>
#include <ydb/core/kqp/ut/common/kqp_serializable_rng.h>

#include <vector>
#include <cassert>
#include <random>
#include <set>
#include <queue>


namespace NKikimr::NKqp {


using TRNG = TSerializableMT19937;


class TLexicographicalNameGenerator {
public:
    static std::string getName(unsigned ID, bool lowerCase = true) {
        if (ID < Base_)
            return std::string(1, fromDigit(ID, lowerCase));

        ID -= Base_;

        unsigned count = 1;
        unsigned step = Base_;
        for (; ID >= step;) {
            ID -= step;
            step *= step;
            count *= 2;
        }

        std::string result(count, fromDigit(Base_ - 1, lowerCase));
        return result + fromNumber(ID, result.size(), lowerCase);
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
        assert(0 <= value && value < Base_);
        return (lowerCase ? 'a' : 'A') + value;
    }

    static constexpr unsigned Base_ = 'z' - 'a' + 1;
};




struct TPitmanYorConfig {
    double Alpha;
    double Theta;

    void DumpParamsHeader(IOutputStream &OS) {
        OS << "alpha,theta";
    }

    void DumpParams(IOutputStream &OS) {
        OS << Alpha << "," << Theta;
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

    static TRelationGraph FromPrufer(const std::vector<unsigned>& prufer);

    void Connect(unsigned lhs, unsigned rhs);

    void Disconnect(unsigned u, unsigned v) {
        auto& adjacencyU = AdjacencyList_[u];
        auto& adjacencyV = AdjacencyList_[v];
        adjacencyU.erase(std::remove_if(adjacencyU.begin(), adjacencyU.end(), [v](TEdge edge) { return edge.Target == v; }), adjacencyU.end());
        adjacencyV.erase(std::remove_if(adjacencyV.begin(), adjacencyV.end(), [u](TEdge edge) { return edge.Target == u; }), adjacencyV.end());

    }

    bool HasEdge(unsigned u, unsigned v) const {
        for (ui32 i = 0; i < AdjacencyList_[u].size(); ++i) {
            if (AdjacencyList_[u][i].Target == v) {
                return true;
            }
        }

        return false;
    }

    std::vector<int> FindComponents() const {
        std::vector<int> component(GetN(), -1);
        int numComponents = 0;

        for (unsigned start = 0; start < GetN(); ++ start) {
            if (component[start] != -1) {
                continue;
            }

            std::queue<int> queue;
            queue.push(start);
            component[start] = numComponents;

            while (!queue.empty()) {
                unsigned u = queue.front();
                queue.pop();
                for (TEdge edge : AdjacencyList_[u]) {
                    unsigned v = edge.Target;
                    if (component[v] == -1) {
                        component[v] = numComponents;
                        queue.push(v);
                    }
                }
            }

            ++ numComponents;
        }

        return component;
    }

    int NumComponents() const {
        auto comp = FindComponents();
        return comp.empty() ? 0 : *std::max_element(comp.begin(), comp.end()) + 1;
    }

    bool IsConnected() const {
        return NumComponents() == 1;
    }

    int NumEdges() const {
        int count = 0;
        for (const auto& adjacency : AdjacencyList_) {
            count += adjacency.size();
        }
        return count / 2;
    }


    std::string MakeQuery() const;

    void DumpGraph(IOutputStream &OS) const;

    const TSchema& GetSchema() const {
        return Schema_;
    }

    std::vector<int> GetDegrees() const;

    unsigned GetN() const {
        return AdjacencyList_.size();
    }

    void ReorderDFS();

    void Rename(const std::vector<int> &oldToNew);

    void SetupKeysPitmanYor(TRNG &mt, TPitmanYorConfig config);

    ui32 GetEdges() {
        ui32 numEdges = 0;
        for (auto edges : AdjacencyList_) {
            numEdges += edges.size();
        }

        return numEdges;
    }

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

    static TSchemaStats MakeRandom(TRNG &mt, const TSchema &schema, unsigned a, unsigned b);

    std::string ToJSON() const;

private:
    std::vector<TTableStats> Stats_;
};


void NormalizeProbabilities(std::vector<double>& probabilities);

std::vector<int> SampleFromPMF(
    const std::vector<double>& probabilities,
    int numVertices, int minDegree);

std::vector<int> GenerateLogNormalDegrees(
    int numVertices, double logMean = 1.0, double logStdDev = 0.5,
    int minDegree = 1, int maxDegree = -1
);

TRelationGraph ConstructGraphHavelHakimi(std::vector<int> degrees);

std::vector<int> MakeGraphicConnected(std::vector<int> degrees);

void MCMCRandomize(TRNG &mt, TRelationGraph& graph);

TRelationGraph GenerateLine(TRNG &rng, unsigned numNodes);
TRelationGraph GenerateStar(TRNG &rng, unsigned numNodes);
TRelationGraph GenerateFullyConnected(TRNG &rng, unsigned numNodes);
TRelationGraph GenerateRandomTree(TRNG &mt, unsigned numNodes);

TRelationGraph GenerateRandomChungLuGraph(TRNG &mt, const std::vector<int>& degrees);

} // namespace NKikimr::NKqp
