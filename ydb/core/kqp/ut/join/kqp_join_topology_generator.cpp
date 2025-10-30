#include "kqp_join_topology_generator.h"

#include <cstdint>
#include <vector>
#include <cassert>
#include <sstream>
#include <random>
#include <set>


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

static double getRandomNormalizedDouble(TRNG &mt) {
    std::uniform_real_distribution<> distribution(0.0, 1.0);
    return distribution(mt);
}

static std::vector<ui32> GetPitmanYor(TRNG &mt, ui32 sum, TPitmanYorConfig config) {
    std::vector<ui32> keyCounts{/*initial key=*/1};

    for (ui32 column = 1; column < sum; ++ column) {
        double random = getRandomNormalizedDouble(mt);

        double cumulative = 0.0;
        int chosenTableIndex = -1;

        for (ui32 i = 0; i < keyCounts.size(); ++ i) {
            cumulative += (keyCounts[i] - config.Alpha) / (column + config.Theta);
            if (random < cumulative) {
                chosenTableIndex = i;
                break;
            }
        }

        if (chosenTableIndex == -1) {
            keyCounts.push_back(1);
        } else {
            ++ keyCounts[chosenTableIndex];
        }
    }

    std::sort(keyCounts.begin(), keyCounts.end(), std::greater<int>{});
    return keyCounts;
}

void TRelationGraph::SetupKeysPitmanYor(TRNG &mt, TPitmanYorConfig config) {
    std::vector<std::pair<std::vector<ui32>, /*last index*/ui32>> distributions(GetN());
    for (ui32 i = 0; i < GetN(); ++ i) {
        auto distribution = GetPitmanYor(mt, AdjacencyList_[i].size(), config);
        Schema_[i] = TTable{static_cast<ui32>(distribution.size())};
        distributions[i] = {std::move(distribution), /*initial index=*/0};
    }

    auto GetKey = [&](ui32 node) {
        auto &[nodeDistribution, lastIndex] = distributions[node];
        ui32 key = lastIndex;

        assert(lastIndex < nodeDistribution.size());
        assert(nodeDistribution[lastIndex] != 0);
        if (-- nodeDistribution[lastIndex] == 0) {
            ++ lastIndex;
        }

        return key;
    };

    std::set<std::pair<uint32_t, uint32_t>> visited;

    for (ui32 u = 0; u < GetN(); ++ u) {
        for (ui32 i = 0; i < AdjacencyList_[u].size(); ++ i) {
            TEdge* forwardEdge = &AdjacencyList_[u][i];
            ui32 v = forwardEdge->Target;

            if (visited.contains({v, u}) || visited.contains({u, v})) {
                continue;
            }

            visited.insert({u, v});

            // find backward edge
            TEdge* backwardEdge = nullptr;
            std::vector<TEdge>& targetAdjacency = AdjacencyList_[forwardEdge->Target];
            for (ui32 k = 0; k < targetAdjacency.size(); ++ k) {
                if (targetAdjacency[k].Target == u) {
                    backwardEdge = &targetAdjacency[k];
                }
            }

            ui32 ColumnLHS = GetKey(u);
            ui32 ColumnRHS = GetKey(v);

            forwardEdge->ColumnLHS = ColumnLHS;
            forwardEdge->ColumnRHS = ColumnRHS;
            backwardEdge->ColumnLHS = ColumnRHS;
            backwardEdge->ColumnRHS = ColumnLHS;
        }
    }

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


TRelationGraph TRelationGraph::FromPrufer(const std::vector<unsigned>& prufer) {
    unsigned n = prufer.size() + 2;

    std::vector<int> degree(n, 1);
    for (unsigned i : prufer) {
        ++ degree[i];
    }

    TRelationGraph graph(n);

    for (unsigned u : prufer) {
        for (unsigned v = 0; v < n; ++ v) {
            if (degree[v] == 1) {
                graph.Connect(u, v);

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
                graph.Connect(u, v);
                break;
            }

            u = v;
        }
    }

    return graph;
}

void TRelationGraph::Connect(unsigned lhs, unsigned rhs) {
    AdjacencyList_[lhs].push_back({/*Target=*/rhs, 0, 0});
    AdjacencyList_[rhs].push_back({/*Target=*/lhs, 0, 0});
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


TSchemaStats TSchemaStats::MakeRandom(TRNG &mt, const TSchema &schema, unsigned a, unsigned b) {
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

TRelationGraph GenerateLine(unsigned numNodes) {
    TRelationGraph graph(numNodes);

    unsigned lastVertex = 0;
    bool first = true;
    for (unsigned i = 0; i < numNodes; ++ i) {
        if (!first) {
            graph.Connect(lastVertex, i);
        }

        first = false;
        lastVertex = i;
    }

    return graph;
}

TRelationGraph GenerateStar(unsigned numNodes) {
    TRelationGraph graph(numNodes);

    unsigned root = 0;
    for (unsigned i = 1; i < numNodes; ++ i) {
        graph.Connect(root, i);
    }

    return graph;
}

TRelationGraph GenerateFullyConnected(unsigned numNodes) {
    TRelationGraph graph(numNodes);

    for (unsigned i = 0; i < numNodes; ++ i) {
        for (unsigned j = i + 1; j < numNodes; ++ j) {
            graph.Connect(i, j);
        }
    }

    return graph;
}

static std::vector<unsigned> GenerateRandomPruferSequence(TRNG &mt, unsigned numNodes) {
    assert(numNodes >= 2);
    std::uniform_int_distribution<> distribution(0, numNodes - 1);

    std::vector<unsigned> prufer(numNodes - 2);
    for (unsigned i = 0; i < numNodes - 2; ++i) {
        prufer[i] = distribution(mt);
    }

    return prufer;
}

TRelationGraph GenerateRandomTree(TRNG &mt, unsigned numNodes) {
    auto prufer = GenerateRandomPruferSequence(mt, numNodes);
    return TRelationGraph::FromPrufer(prufer);
}

void NormalizeProbabilities(std::vector<double>& probabilities) {
    double sum = std::accumulate(probabilities.begin(), probabilities.end(), 0.0);
    if (sum > 0.0) {
        for (double& probability : probabilities) {
            probability /= sum;
        }
    }
}

std::vector<int> SampleFromPMF(const std::vector<double>& probabilities,
                               int numVertices, int minDegree) {
    std::random_device randomDevice;
    std::mt19937 generator(randomDevice());
    std::discrete_distribution<int> distribution(probabilities.begin(),
                                                 probabilities.end());

    std::vector<int> degrees(numVertices);
    for (int i = 0; i < numVertices; i++) {
        degrees[i] = distribution(generator) + minDegree;
    }
    return degrees;
}

std::vector<int> GenerateLogNormalDegrees(int numVertices, double logMean,
                                          double logStdDev, int minDegree,
                                          int maxDegree) {
    if (maxDegree == -1) maxDegree = numVertices - 1;

    std::vector<double> probabilities(maxDegree - minDegree + 1);
    for (int k = minDegree; k <= maxDegree; k++) {
        if (k <= 0) {
            probabilities[k - minDegree] = 0.0;
            continue;
        }

        double x = (double)k;
        double logX = std::log(x);
        double z = (logX - logMean) / logStdDev;

        // PDF: (1/(x*σ*√(2π))) * exp(-(ln(x)-μ)²/(2σ²))
        probabilities[k - minDegree] = (1.0 / (x * logStdDev * std::sqrt(2.0 * M_PI))) *
                                       std::exp(-0.5 * z * z);
    }

    NormalizeProbabilities(probabilities);
    return SampleFromPMF(probabilities, numVertices, minDegree);
}

TRelationGraph GenerateRandomChungLuGraph(TRNG &mt, const std::vector<int>& degrees) {
    TRelationGraph graph(degrees.size());

    double sum = std::accumulate(degrees.begin(), degrees.end(), 0.0);
    if (sum == 0) {
        return graph;
    }

    std::uniform_real_distribution<> distribution(0, 1);
    for (ui32 i = 0; i < degrees.size(); ++ i) {
        for (ui32 j = i + 1; j < degrees.size(); ++ j) {
            if (distribution(mt) < std::min(1.0, degrees[i] * degrees[j] / sum)) {
                graph.Connect(i, j);
            }
        }
    }

    return graph;
}

}
