#include "kqp_join_topology_generator.h"

#include <vector>
#include <cassert>
#include <sstream>
#include <random>


namespace NKikimr::NKqp {

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


static std::string getTableName(unsigned tableID) {
    return TLexicographicalNameGenerator::getName(tableID, /*lowerCase=*/false);
}

static std::string getColumnName(unsigned tableID, unsigned columnID) {
    return TLexicographicalNameGenerator::getName(tableID, /*lowerCase=*/true) + "_" +
           TLexicographicalNameGenerator::getName(columnID, /*lowerCase=*/true);
}

static std::string getRelationName(unsigned tableID, unsigned columnID) {
    return getTableName(tableID) + "." + getColumnName(tableID, columnID);
}

static std::string getTablePath(unsigned tableID) {
    return "/Root/" + getTableName(tableID);
}


static unsigned getRandom(std::mt19937 &mt, unsigned a, unsigned b) {
    std::uniform_int_distribution<> distribution(a, b);
    return distribution(mt);
}

static unsigned getRandomBool(std::mt19937 &mt, double trueProbability) {
    std::uniform_real_distribution<> distribution(0.0, 1.0);
    return distribution(mt) < trueProbability;
}


unsigned TTable::GetRandomOrNewColumn(std::mt19937 &mt, double newColumnProbability) {
    bool generateNewColumn = getRandomBool(mt, newColumnProbability);
    if (generateNewColumn || NumColumns == 0) {
        return NumColumns ++;
    }

    return GetRandomColumn(mt);
}

unsigned TTable::GetRandomColumn(std::mt19937 &mt) const {
    assert(NumColumns != 0);
    return getRandom(mt, 0, NumColumns - 1);
}


TSchema TSchema::MakeWithEnoughColumns(unsigned numNodes) {
    return TSchema{std::vector(numNodes, TTable{numNodes})};
}

std::string TSchema::MakeCreateQuery() const {
    std::string prerequisites;
    for (unsigned i = 0; i < Tables_.size(); ++ i) {
        prerequisites += "CREATE TABLE `" + getTablePath(i) + "` (\n";

        for (unsigned j = 0; j < Tables_[i].GetNumColumns(); ++ j) {
            prerequisites += "    " + getColumnName(i, j) + " Int32 NOT NULL,\n";
        }
        prerequisites += "    PRIMARY KEY (" + getColumnName(i, 0) + ")\n";

        prerequisites += ") WITH (STORE = COLUMN);\n";
    }

    return prerequisites;
}

std::string TSchema::MakeDropQuery() const {
    std::string query;
    for (unsigned i = 0; i < Tables_.size(); ++ i) {
        query += "DROP TABLE `" + getTablePath(i) + "`;\n";
    }

    return query;
}

void TSchema::Rename(std::vector<int> oldToNew) {
    std::vector<TTable> newTables(Tables_.size());

    for (unsigned i = 0; i < oldToNew.size(); ++i) {
        newTables[oldToNew[i]] = Tables_[i];
    }

    Tables_ = newTables;
}


TRelationGraph TRelationGraph::FromPrufer(std::mt19937 &mt, const std::vector<unsigned>& prufer, double newColumnProbability) {
    unsigned n = prufer.size() + 2;

    std::vector<int> degree(n, 1);
    for (unsigned i : prufer) {
        ++ degree[i];
    }

    TRelationGraph graph(n);

    for (unsigned u : prufer) {
        for (unsigned v = 0; v < n; ++ v) {
            if (degree[v] == 1) {
                graph.Connect(mt, u, v, newColumnProbability);

                -- degree[v];
                -- degree[u];
                break;
            }
        }
    }

    int u = -1;
    unsigned v = 0;
    for (; v < n; ++ v) {
        if (degree[v] == 1) {
            if (u != -1) {
                graph.Connect(mt, u, v, newColumnProbability);
                break;
            }

            u = v;
        }
    }

    return graph;
}

void TRelationGraph::Connect(std::mt19937 &mt, unsigned lhs, unsigned rhs, double newColumnProbability) {
    unsigned ColumnLHS = Schema_[lhs].GetRandomOrNewColumn(mt, newColumnProbability);
    unsigned ColumnRHS = Schema_[rhs].GetRandomOrNewColumn(mt, newColumnProbability);

    AdjacencyList_[lhs].push_back({rhs, ColumnLHS, ColumnRHS});
    AdjacencyList_[rhs].push_back({lhs, ColumnRHS, ColumnLHS});
}

std::string TRelationGraph::MakeQuery() const {
    std::string fromClause;
    std::string joinClause;
    for (unsigned i = 0; i < AdjacencyList_.size(); ++ i) {
        std::string currentJoin =
            "JOIN " + getTableName(i) + " ON";

        bool hasJoin = false;
        auto addJoinCodition = [&](int j) {
            if (hasJoin) {
                currentJoin += " AND";
            }

            hasJoin = true;

            currentJoin += " " +
                           getRelationName(i, AdjacencyList_[i][j].ColumnLHS) + " = " +
                           getRelationName(AdjacencyList_[i][j].Target, AdjacencyList_[i][j].ColumnRHS);

        };

        for (unsigned j = 0; j < AdjacencyList_[i].size(); ++ j) {
            if (i < AdjacencyList_[i][j].Target) {
                continue;
            }

            addJoinCodition(j);
        }
        currentJoin += "\n";

        if (hasJoin) {
            joinClause += currentJoin;
        } else if (fromClause.empty()) {
            fromClause = "SELECT *\nFROM " + getTableName(i) + "\n";
        } else {
            joinClause += "CROSS JOIN " + getTableName(i) + "\n";
        }
    }

    // remove extra '\n' from last join
    if (!joinClause.empty()) {
        assert(joinClause.back() == '\n');
        joinClause.pop_back();
    }

    return std::move(fromClause) + std::move(joinClause) + ";\n";
}

void TRelationGraph::DumpGraph(IOutputStream &OS) const {
    OS << "graph {\n";
    for (unsigned i = 0; i < AdjacencyList_.size(); ++ i) {
        for (unsigned j = 0; j < AdjacencyList_[i].size(); ++ j) {
            const TEdge &edge = AdjacencyList_[i][j];
            if (i <= edge.Target) {
                OS << "    " << getTableName(i) << " -- " << getTableName(edge.Target)
                << " [label = \""
                << getRelationName(i, edge.ColumnLHS) << " = "
                << getRelationName(edge.Target, edge.ColumnRHS)
                << "\"];\n";
            }
        }
    }

    OS << "}\n";
}

std::vector<int> TRelationGraph::GetDegrees() const {
    std::vector<int> degrees(AdjacencyList_.size());
    for (unsigned i = 0; i < AdjacencyList_.size(); ++ i) {
        degrees[i] = AdjacencyList_[i].size();
    }

    std::sort(degrees.begin(), degrees.end());
    return degrees;
}

void TRelationGraph::ReorderDFS() {
    std::vector<bool> visited(GetN(), false);
    std::vector<int> newOrder;
    newOrder.reserve(GetN());

    auto searchDepthFirst = [&](auto &&self, unsigned node) {
        if (visited[node]) {
            return;
        }

        visited[node] = true;
        newOrder.push_back(node);

        for (TEdge edge : AdjacencyList_[node]) {
            self(self, edge.Target);
        }
    };

    for (unsigned i = 0; i < GetN(); i++) {
        searchDepthFirst(searchDepthFirst, i);
    }

    std::vector<int> oldToNew(GetN());
    for (unsigned i = 0; i < GetN(); i++) {
        oldToNew[newOrder[i]] = i;
    }

    Rename(oldToNew);
    Schema_.Rename(oldToNew);
}

void TRelationGraph::Rename(const std::vector<int> &oldToNew) {
    TAdjacencyList newGraph(GetN());
    for (unsigned u = 0; u < GetN(); ++ u) {
        for (TEdge edge : AdjacencyList_[u]) {
            unsigned v = edge.Target;

            edge.Target = oldToNew[v];
            newGraph[oldToNew[u]].push_back(edge);
        }
    }

    AdjacencyList_ = newGraph;
}


TSchemaStats TSchemaStats::MakeRandom(std::mt19937 &mt, const TSchema &schema, unsigned a, unsigned b) {
    std::uniform_int_distribution<> distribution(a, b);
    std::vector<TTableStats> stats(schema.GetSize());

    for (unsigned i = 0; i < schema.GetSize(); ++ i) {
        unsigned RowSize = std::pow(10, distribution(mt));
        unsigned ByteSize = RowSize * 64;
        stats[i] = {ByteSize, RowSize};
    }

    return TSchemaStats{stats};
}

std::string TSchemaStats::ToJSON() const {
    std::stringstream ss;

    ss << "{";
    for (unsigned i = 0; i < Stats_.size(); ++ i) {
        if (i != 0)
            ss << ",";

        ss << "\"" << getTablePath(i) << "\": ";
        ss << "{";
        ss << "\"" << "n_rows" << "\": " << Stats_[i].RowSize << ", ";
        ss << "\"" << "byte_size" << "\": " << Stats_[i].ByteSize;
        ss << "}";
    }
    ss << "}";

    return ss.str();
}

TRelationGraph GenerateLine(std::mt19937 &mt, unsigned numNodes, double newColumnProbability) {
    TRelationGraph graph(numNodes);

    unsigned lastVertex = 0;
    bool first = true;
    for (unsigned i = 0; i < numNodes; ++ i) {
        if (!first) {
            graph.Connect(mt, lastVertex, i, newColumnProbability);
        }

        first = false;
        lastVertex = i;
    }

    return graph;
}

TRelationGraph GenerateStar(std::mt19937 &mt, unsigned numNodes, double newColumnProbability) {
    TRelationGraph graph(numNodes);

    unsigned root = 0;
    for (unsigned i = 1; i < numNodes; ++ i) {
        graph.Connect(mt, root, i, newColumnProbability);
    }

    return graph;
}

TRelationGraph GenerateFullyConnected(std::mt19937 &mt, unsigned numNodes,
                                      double newColumnProbability) {

    TRelationGraph graph(numNodes);

    for (unsigned i = 0; i < numNodes; ++ i) {
        for (unsigned j = i + 1; j < numNodes; ++ j) {
            graph.Connect(mt, i, j, newColumnProbability);
        }
    }

    return graph;
}

static std::vector<unsigned> GenerateRandomPruferSequence(std::mt19937 &mt, unsigned numNodes) {
    assert(numNodes >= 2);
    std::uniform_int_distribution<> distribution(0, numNodes - 1);

    std::vector<unsigned> prufer(numNodes - 2);
    for (unsigned i = 0; i < numNodes - 2; ++i) {
        prufer[i] = distribution(mt);
    }

    return prufer;
}

TRelationGraph GenerateRandomTree(std::mt19937 &mt, unsigned numNodes, double newColumnProbability) {
    auto prufer = GenerateRandomPruferSequence(mt, numNodes);
    return TRelationGraph::FromPrufer(mt, prufer, newColumnProbability);
}

}
