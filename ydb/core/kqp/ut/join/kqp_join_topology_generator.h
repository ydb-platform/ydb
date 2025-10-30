#pragma once

#include <util/stream/output.h>
#include <ydb/core/kqp/ut/common/kqp_serializable_rng.h>

#include <vector>
#include <cassert>
#include <random>
#include <set>


namespace NKikimr::NKqp {


using TRNG = TSerializableMT19937;


struct TPitmanYorConfig {
    double Alpha;
    double Theta;
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


private:
    struct TEdge {
        unsigned Target;
        unsigned ColumnLHS, ColumnRHS;
    };

    using TAdjacencyList = std::vector<std::vector<TEdge>>;

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

std::vector<int> SampleFromPMF(const std::vector<double>& probabilities,
                               int numVertices, int minDegree);

std::vector<int> GenerateLogNormalDegrees(int numVertices, double logMean = 1.0,
                                          double logStdDev = 0.5, int minDegree = 1,
                                          int maxDegree = -1);
TRelationGraph GenerateLine(unsigned numNodes);
TRelationGraph GenerateStar(unsigned numNodes);
TRelationGraph GenerateFullyConnected(unsigned numNodes);
TRelationGraph GenerateRandomTree(TRNG &mt, unsigned numNodes);

TRelationGraph GenerateRandomChungLuGraph(TRNG &mt, const std::vector<int>& degrees);

} // namespace NKikimr::NKqp
