#pragma once

#include <util/stream/output.h>
#include <ydb/core/kqp/ut/common/kqp_serializable_rng.h>

#include <vector>
#include <cassert>
#include <random>
#include <set>


namespace NKikimr::NKqp {


using TRNG = TSerializableMT19937;

class TTable {
public:
    TTable(unsigned numColumns = 0)
        : NumColumns(numColumns)
    {
    }

    unsigned GetRandomOrNewColumn(TRNG &mt, double newColumnProbability);

    unsigned GetNumColumns() const {
        return NumColumns;
    }

private:
    unsigned GetRandomColumn(TRNG &mt) const;

private:
    // table has columns from 0..NumColumns
    unsigned NumColumns;
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

    static TRelationGraph FromPrufer(TRNG &mt, const std::vector<unsigned>& prufer, double newColumnProbability);

    void Connect(TRNG &mt, unsigned lhs, unsigned rhs, double newColumnProbability);

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


TRelationGraph GenerateLine(TRNG &mt, unsigned numNodes, double newColumnProbability);
TRelationGraph GenerateStar(TRNG &mt, unsigned numNodes, double newColumnProbability);
TRelationGraph GenerateFullyConnected(TRNG &mt, unsigned numNodes, double newColumnProbability);
TRelationGraph GenerateRandomTree(TRNG &mt, unsigned numNodes, double newColumnProbability);

} // namespace NKikimr::NKqp

