#include "kqp_join_topology_generator.h"

#include <cstdint>
#include <vector>
#include <sstream>
#include <random>
#include <set>

namespace NKikimr::NKqp {

std::string getTableName(unsigned tableID) {
    return TLexicographicalNameGenerator::getName(tableID, /*lowerCase=*/false);
}

std::string getColumnName(unsigned tableID, unsigned columnID) {
    return TLexicographicalNameGenerator::getName(tableID, /*lowerCase=*/true) + "_" +
           TLexicographicalNameGenerator::getName(columnID, /*lowerCase=*/true);
}

std::string getRelationName(unsigned tableID, unsigned columnID) {
    return getTableName(tableID) + "." + getColumnName(tableID, columnID);
}

std::string getTablePath(unsigned tableID) {
    return "/Root/" + getTableName(tableID);
}

static double getRandomNormalizedDouble(TRNG& rng) {
    std::uniform_real_distribution<> distribution(0.0, 1.0);
    return distribution(rng);
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

void TRelationGraph::Connect(TRNG& rng, unsigned u, unsigned v, TPitmanYorConfig config) {
    // 1. Generate L.H.S (Source) Key
    // LHS is the "Leader" - it picks purely based on its internal PY weights
    double quantileU = getRandomNormalizedDouble(rng);
    ui32 keyU = NodeStates_[u].GenerateKey(rng, config, quantileU);

    // 2. Calculate coupling logic for R.H.S (Target)
    // Default: Random (Independent)
    double quantileV = -1.0;

    // Check if we should enforce correlation
    bool enforceCorrelation = false;
    if (std::abs(config.Assortativity) > 1e-6) {
        // Probability of coupling equals the magnitude of assortativity
        if (getRandomNormalizedDouble(rng) < std::abs(config.Assortativity)) {
            enforceCorrelation = true;
        }
    }

    if (enforceCorrelation) {
        if (config.Assortativity > 0) {
            // Positive: Rich connects to Rich (match quantile)
            quantileV = quantileU;
        } else {
            // Negative: Rich connects to Poor (invert quantile)
            quantileV = 1.0 - quantileU;
        }
    }

    // 3. Generate R.H.S (Target) Key
    // RHS is the "Follower" - it might successfully find a key at the requested
    // quantile, or create a new one if it has to.
    ui32 keyV = NodeStates_[v].GenerateKey(rng, config, quantileV);

    // 4. Update Topology
    AdjacencyList_[u].push_back({v, keyU, keyV});
    AdjacencyList_[v].push_back({u, keyV, keyU});
}

void TRelationGraph::Rewire(TRNG& rng, unsigned u, unsigned oldV, unsigned newV, TPitmanYorConfig config) {
    Disconnect(u, oldV);
    Connect(rng, u, newV, config);
}

void TRelationGraph::Disconnect(unsigned u, unsigned v) {
    RemoveEdgeFromList(u, v);
    RemoveEdgeFromList(v, u);
}

bool TRelationGraph::HasEdge(unsigned u, unsigned v) const {
    for (ui32 i = 0; i < AdjacencyList_[u].size(); ++i) {
        if (AdjacencyList_[u][i].Target == v) {
            return true;
        }
    }

    return false;
}

bool TRelationGraph::SelectRandomEdge(TRNG& rng, unsigned& outU, unsigned& outV) const {
    std::vector<unsigned> nodesWithEdges;
    for (unsigned i = 0; i < AdjacencyList_.size(); ++i) {
        if (!AdjacencyList_[i].empty()) {
            nodesWithEdges.push_back(i);
        }
    }

    if (nodesWithEdges.empty()) {
        return false;
    }

    std::uniform_int_distribution<unsigned> nodeDist(0, nodesWithEdges.size() - 1);
    outU = nodesWithEdges[nodeDist(rng)];

    std::uniform_int_distribution<unsigned> edgeDist(0, AdjacencyList_[outU].size() - 1);
    outV = AdjacencyList_[outU][edgeDist(rng)].Target;

    return true;
}

unsigned TRelationGraph::SelectRandomNode(TRNG& rng) const {
    std::uniform_int_distribution<unsigned> dist(0, GetN() - 1);
    return dist(rng);
}

std::optional<TRelationGraph::TEdge> TRelationGraph::GetEdge(unsigned u, unsigned v) const {
    for (const auto& edge : AdjacencyList_[u]) {
        if (edge.Target == v) {
            return TEdge{edge.Target, edge.ColumnLHS, edge.ColumnRHS};
        }
    }
    return std::nullopt;
}

void TRelationGraph::ConnectWithKeys(unsigned u, unsigned v, unsigned keyU, unsigned keyV) {
    AdjacencyList_[u].push_back({v, keyU, keyV});
    AdjacencyList_[v].push_back({u, keyV, keyU});

    NodeStates_[u].Counts[keyU]++;
    NodeStates_[u].TotalEdges++;
    NodeStates_[v].Counts[keyV]++;
    NodeStates_[v].TotalEdges++;
}

void TRelationGraph::RemoveEdgeFromList(unsigned owner, unsigned target) {
    auto& list = AdjacencyList_[owner];
    for (size_t i = 0; i < list.size(); ++i) {
        if (list[i].Target == target) {
            // Return the used key to the state manager
            // If I am LHS (u), my key is stored in ColumnLHS.
            // If I am RHS (v), looking at 'u' edge, my key is ColumnLHS (relative to list owner)
            ui32 keyUsed = list[i].ColumnLHS;
            NodeStates_[owner].ReleaseKey(keyUsed);

            // Fast remove (Swap with last and pop)
            // Note: changes edge order, usually acceptable in graph algos
            if (i != list.size() - 1) {
                list[i] = list.back();
            }
            list.pop_back();
            return;
        }
    }
}

std::vector<int> TRelationGraph::FindComponents() const {
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
        Y_ASSERT(joinClause.back() == '\n');
        joinClause.pop_back();
    }

    return std::move(fromClause) + std::move(joinClause) + ";\n";
}

void TRelationGraph::DumpGraph(IOutputStream& os) const {
    os << "graph {\n";
    for (unsigned i = 0; i < AdjacencyList_.size(); ++ i) {
        for (unsigned j = 0; j < AdjacencyList_[i].size(); ++ j) {
            const TEdge& edge = AdjacencyList_[i][j];
            if (i <= edge.Target) {
                os << "    " << getTableName(i) << " -- " << getTableName(edge.Target)
                   << " [label = \""
                   << getRelationName(i, edge.ColumnLHS) << " = "
                   << getRelationName(edge.Target, edge.ColumnRHS)
                   << "\"];\n";
            }
        }
    }

    os << "}\n";
}

std::vector<int> TRelationGraph::GetDegrees() const {
    std::vector<int> degrees(AdjacencyList_.size());
    for (unsigned i = 0; i < AdjacencyList_.size(); ++i) {
        degrees[i] = AdjacencyList_[i].size();
    }

    std::sort(degrees.begin(), degrees.end());
    return degrees;
}

void TRelationGraph::ReorderDFS() {
    std::vector<bool> visited(GetN(), false);
    std::vector<int> newOrder;
    newOrder.reserve(GetN());

    auto searchDepthFirst = [&](auto&& self, unsigned node) {
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

void TRelationGraph::Rename(const std::vector<int>& oldToNew) {
    TAdjacencyList newGraph(GetN());
    for (unsigned u = 0; u < GetN(); ++u) {
        for (TEdge edge : AdjacencyList_[u]) {
            unsigned v = edge.Target;

            edge.Target = oldToNew[v];
            newGraph[oldToNew[u]].push_back(edge);
        }
    }

    AdjacencyList_ = newGraph;
}

ui32 TRelationGraph::GetNumEdges() const {
    ui32 numEdges = 0;
    for (auto edges : AdjacencyList_) {
        numEdges += edges.size();
    }

    return numEdges;
}

int TRelationGraph::GetNumComponents() const {
    auto comp = FindComponents();
    return comp.empty() ? 0 : *std::max_element(comp.begin(), comp.end()) + 1;
}

TSchemaStats TSchemaStats::MakeRandom(TRNG& rng, const TSchema& schema, unsigned a, unsigned b) {
    std::uniform_int_distribution<> distribution(a, b);
    std::vector<TTableStats> stats(schema.GetSize());

    for (unsigned i = 0; i < schema.GetSize(); ++i) {
        unsigned RowSize = std::pow(10, distribution(rng));
        unsigned ByteSize = RowSize * 64;
        stats[i] = {ByteSize, RowSize};
    }

    return TSchemaStats{stats};
}

std::string TSchemaStats::ToJSON() const {
    std::stringstream ss;

    ss << "{";
    for (unsigned i = 0; i < Stats_.size(); ++i) {
        if (i != 0) {
            ss << ",";
        }

        ss << "\"" << getTablePath(i) << "\": ";
        ss << "{";
        ss << "\"" << "n_rows" << "\": " << Stats_[i].RowSize << ", ";
        ss << "\"" << "byte_size" << "\": " << Stats_[i].ByteSize;
        ss << "}";
    }
    ss << "}";

    return ss.str();
}

TRelationGraph GeneratePath(TRNG &rng, unsigned numNodes, TPitmanYorConfig config) {
    TRelationGraph graph(numNodes);

    unsigned lastVertex = 0;
    bool first = true;
    for (unsigned i = 0; i < numNodes; ++i) {
        if (!first) {
            graph.Connect(rng, lastVertex, i, config);
        }

        first = false;
        lastVertex = i;
    }

    return graph;
}

TRelationGraph GenerateStar(TRNG &rng, unsigned numNodes, TPitmanYorConfig config) {
    TRelationGraph graph(numNodes);

    unsigned root = 0;
    for (unsigned i = 1; i < numNodes; ++i) {
        graph.Connect(rng, root, i, config);
    }

    return graph;
}

TRelationGraph GenerateClique(TRNG &rng, unsigned numNodes, TPitmanYorConfig config) {
    TRelationGraph graph(numNodes);

    for (unsigned i = 0; i < numNodes; ++i) {
        for (unsigned j = i + 1; j < numNodes; ++j) {
            graph.Connect(rng, i, j, config);
        }
    }

    return graph;
}

TRelationGraph GenerateTreeFromPruferSequence(TRNG &rng, const std::vector<unsigned>& prufer, TPitmanYorConfig config) {
    unsigned n = prufer.size() + 2;

    std::vector<int> degree(n, 1);
    for (unsigned i : prufer) {
        ++ degree[i];
    }

    TRelationGraph graph(n);

    for (unsigned u : prufer) {
        for (unsigned v = 0; v < n; ++ v) {
            if (degree[v] == 1) {
                graph.Connect(rng, u, v, config);

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
                graph.Connect(rng, u, v, config);
                break;
            }

            u = v;
        }
    }

    return graph;
}


static std::vector<unsigned> GenerateRandomPruferSequence(TRNG& rng, unsigned numNodes) {
    Y_ASSERT(numNodes >= 2);
    std::uniform_int_distribution<> distribution(0, numNodes - 1);

    std::vector<unsigned> prufer(numNodes - 2);
    for (unsigned i = 0; i < numNodes - 2; ++i) {
        prufer[i] = distribution(rng);
    }

    return prufer;
}

TRelationGraph GenerateRandomTree(TRNG& rng, unsigned numNodes, TPitmanYorConfig config) {
    auto prufer = GenerateRandomPruferSequence(rng, numNodes);
    return GenerateTreeFromPruferSequence(rng, prufer, config);
}

std::vector<int> GenerateLogNormalDegrees(TRNG& rng, int numVertices, double logMedian, double sigma, int minDegree, int maxDegree) {
    if (maxDegree == -1) {
        maxDegree = numVertices - 1;
    }

    // --- 1. ADJUSTMENT CALCULATION ---
    // We want the final result (X + minDegree) to have a median of targetMedian.
    // Therefore, the underlying LogNormal X must have a median of (targetMedian - minDegree).

    double adjustedMedian = exp(logMedian) - minDegree;

    // EDGE CASE SAFTY:
    // LogNormal values must be > 0. Therefore, Target Median MUST be > Min Degree.
    // If you ask for Median 2 and Min 2, that implies the "LogNormal" part averages to 0,
    // which is mathematically impossible (ln(0) = -inf).
    if (adjustedMedian <= 0.001) {
        // TODO: remove this
        Cerr << "Warning: exp(mu) (" << exp(logMedian) << ") must be strictly greater than minDegree ("
             << minDegree << ") for a Shifted LogNormal.\n"
             << "Clamping adjusted median to small epsilon.\n";
        adjustedMedian = 0.1; // Fallback: creates a distribution heavily skewed toward minDegree
    }

    // Calculate mu based on the shifted expectation
    double mu = std::log(adjustedMedian);

    std::lognormal_distribution<double> distribution(mu, sigma);

    std::vector<int> degrees;
    degrees.reserve(numVertices);

    int count = 0;

    // Safety break for MaxDegree enforcement
    int attempts = 0;
    const int MAX_ATTEMPTS = numVertices * 100;

    while (count < numVertices) {
        attempts++;

        // 2. Sample
        double val = distribution(rng);

        // 3. Shift (The "+ minDegree" logic)
        // We calculate the integer part first, then add the minimum.
        int rawShift = std::round(val);
        int degree = rawShift + minDegree;

        // 4. Max Bound Check
        // We theoretically don't need to check minDegree anymore (it's guaranteed),
        // but we might hit the MaxDegree roof.
        if (degree <= maxDegree) {
            degrees.push_back(degree);
            count++;
        }

        // Infinite loop guard (extremely rare nicely configured params)
        if (attempts > MAX_ATTEMPTS) {
            // Fill rest to exit safely
            while(count < numVertices) { degrees.push_back(minDegree); count++; }
        }
    }

    return degrees;
}

ui32 TPitmanYorNodeState::GenerateKey(TRNG& rng, TPitmanYorConfig config, double forceQuantile) {
    double denom = TotalEdges + config.Theta;
    double r = (forceQuantile >= 0.0) ? forceQuantile : getRandomNormalizedDouble(rng);

    // 1. Try joining existing tables
    double cumulative = 0.0;
    for (ui32 k = 0; k < Counts.size(); ++k) {
        if (Counts[k] == 0) continue;

        // Probability of joining table k = (Count - Alpha) / (Total + Theta)
        double prob = (Counts[k] - config.Alpha) / denom;
        cumulative += prob;

        if (r < cumulative) {
            Counts[k]++;
            TotalEdges++;
            return k;
        }
    }

    // 2. Create new table
    // If we get here, 'r' fell into the "New Table" probability space
    ui32 newKey;
    if (!FreeKeys.empty()) {
        newKey = FreeKeys.back();
        FreeKeys.pop_back();
    } else {
        newKey = static_cast<ui32>(Counts.size());
        Counts.push_back(0);
    }

    Counts[newKey] = 1;
    TotalEdges++;
    ActiveTables++;
    return newKey;
}

void TPitmanYorNodeState::ReleaseKey(ui32 key) {
    Y_ASSERT(key < Counts.size());
    Y_ASSERT(Counts[key] > 0);

    Counts[key]--;
    TotalEdges--;

    if (Counts[key] == 0) {
        ActiveTables--;
        FreeKeys.push_back(key);
    }
}

TRelationGraph GenerateRandomChungLuGraph(TRNG& rng, const std::vector<int>& degrees, TPitmanYorConfig config) {
    TRelationGraph graph(degrees.size());

    double sum = std::accumulate(degrees.begin(), degrees.end(), 0.0);
    if (sum == 0) {
        return graph;
    }

    std::uniform_real_distribution<> distribution(0, 1);
    for (ui32 i = 0; i < degrees.size(); ++i) {
        for (ui32 j = i + 1; j < degrees.size(); ++j) {
            if (distribution(rng) < std::min(1.0, degrees[i] * degrees[j] / sum)) {
                graph.Connect(rng, i, j, config);
            }
        }
    }

    return graph;
}

static void MakeEvenSum(std::vector<int>& degrees) {
    int sum = std::accumulate(degrees.begin(), degrees.end(), 0);
    if (sum % 2 == 1) {
        auto minIt = std::min_element(degrees.begin(), degrees.end());
        ++*minIt;
    }
}

static bool CheckSatisfiesErdosGallai(std::vector<int> degrees) {
    int sum = std::accumulate(degrees.begin(), degrees.end(), 0);

    if (sum % 2 != 0) {
        return false;
    }

    for (int degree : degrees) {
        if (degree < 0 || degree >= static_cast<int>(degrees.size())) {
            return false;
        }
    }

    std::sort(degrees.rbegin(), degrees.rend());

    uint64_t sumLeft = 0;
    for (uint64_t k = 0; k < degrees.size(); ++k) {
        sumLeft += degrees[k];

        uint64_t sumRight = k * (k + 1);
        for (uint64_t i = k + 1; i < degrees.size(); ++i) {
            sumRight += std::min<uint64_t>(k + 1, degrees[i]);
        }

        if (sumLeft > sumRight) {
            return false;
        }
    }

    return true;
}

bool CanBeConnected(const std::vector<int>& degrees) {
    int n = degrees.size();
    if (n <= 1) {
        return true;
    }

    int sum = std::accumulate(degrees.begin(), degrees.end(), 0);
    if (sum < 2 * (n - 1)) {
        return false;
    }

    for (int degree : degrees) {
        if (degree == 0) {
            return false;
        }
    }

    return true;
}

std::vector<int> MakeGraphicConnected(std::vector<int> degrees) {
    const i32 MAX_ITERATIONS = 1000;

    for (int& degree : degrees) {
        if (degree == 0) {
            degree = 1;
        }
    }

    // Cap degrees at n-1
    for (int& degree : degrees) {
        degree = std::min<int>(degree, degrees.size() - 1);
    }

    MakeEvenSum(degrees);

    int iterations = 0;
    while ((!CheckSatisfiesErdosGallai(degrees) || !CanBeConnected(degrees)) && iterations++ < MAX_ITERATIONS) {
        std::sort(degrees.begin(), degrees.end(), std::greater<int>());

        if (!CheckSatisfiesErdosGallai(degrees)) {
            // Reduce max, increase min (redistribute)
            if (degrees[0] > degrees[degrees.size() - 1] + 1) {
                --degrees[0];
                ++degrees[degrees.size() - 1];
            } else {
                --degrees[0];
            }
        } else if (!CanBeConnected(degrees)) {
            // Increase minimum degrees to help connectivity
            for (int& degree : degrees) {
                ui32 edges = std::accumulate(degrees.begin(), degrees.end(), 0) / 2;
                if (degree < 2 && edges + 2 <= degrees.size() * (degrees.size() - 1) / 2) {
                    ++degree;
                }
            }
        }

        MakeEvenSum(degrees);
    }

    return degrees;
}

TRelationGraph ConstructGraphHavelHakimi(TRNG &rng, std::vector<int> degrees, TPitmanYorConfig config) {
    TRelationGraph graph(degrees.size());

    std::vector<std::pair</*degree*/ int, /*node*/ int>> nodes;
    for (uint32_t i = 0; i < degrees.size(); ++i) {
        nodes.push_back({degrees[i], i});
    }

    while (true) {
        std::sort(nodes.begin(), nodes.end(), std::greater<std::pair<int, int>>{});

        while (!nodes.empty() && nodes.back().first == 0) {
            nodes.pop_back();
        }

        if (nodes.empty()) {
            break;
        }

        auto [degree, u] = nodes[0];
        nodes.erase(nodes.begin());

        if (degree > static_cast<int>(nodes.size())) {
            break;
        }

        for (uint32_t i = 0; i < static_cast<uint32_t>(degree); ++i) {
            uint32_t v = nodes[i].second;
            graph.Connect(rng, u, v, config);
            --nodes[i].first;
        }
    }

    return graph;
}

struct TMCMCIterationParams {
    ui32 NumIterations;
    ui32 MaxAttempts;

    static TMCMCIterationParams Calculate(ui32 numEdges, const TMCMCConfig& config) {
        numEdges = std::max(numEdges, 1u);
        ui32 base = static_cast<ui32>(numEdges * std::log(numEdges + 1));
        base = std::max(base, 10u);

        TMCMCIterationParams params;
        params.NumIterations = config.NumIterations > 0
            ? config.NumIterations
            : static_cast<ui32>(base * config.IterationMultiplier);

        params.MaxAttempts = config.MaxAttempts > 0
            ? config.MaxAttempts
            : static_cast<ui32>(params.NumIterations * config.MaxAttemptsMultiplier);

        return params;
    }
};

double CalculateTemperature(const TMCMCConfig& config, ui32 attempt, ui32 maxAttempts) {
    double progress = std::min(1.0, static_cast<double>(attempt) / maxAttempts);
    return config.InitialTemperature *
           std::pow(config.FinalTemperature / config.InitialTemperature, progress);
}

TMCMCResult MCMCRandomizeDegreePreserving(TRNG& rng, TRelationGraph& graph, TPitmanYorConfig pyConfig, TMCMCConfig mcmcConfig) {
    TMCMCResult result;

    if (graph.GetN() < 4) {
        result.FinalComponents = graph.GetNumComponents();
        return result;
    }

    auto params = TMCMCIterationParams::Calculate(graph.GetNumEdges() / 2, mcmcConfig);
    std::uniform_real_distribution<double> uniform01(0.0, 1.0);

    auto shouldContinue = [&]() {
        if (result.TotalAttempts >= params.MaxAttempts) return false;
        if (result.SuccessfulSwaps < params.NumIterations) return true;
        return mcmcConfig.EnsureConnectivity && !graph.IsConnected();
    };

    while (shouldContinue()) {
        ++result.TotalAttempts;

        // Select two random edges (a-b) and (c-d)
        unsigned a, b, c, d;
        if (!graph.SelectRandomEdge(rng, a, b) || !graph.SelectRandomEdge(rng, c, d)) {
            continue;
        }

        // Validate geometry: all four nodes must be distinct
        if (a == c || a == d || b == c || b == d) {
            ++result.RejectedByGeometry;
            continue;
        }

        // Check new edges won't duplicate existing
        if (graph.HasEdge(a, c) || graph.HasEdge(b, d)) {
            ++result.RejectedByGeometry;
            continue;
        }

        // Save original edges for potential undo
        auto edgeAB = graph.GetEdge(a, b);
        auto edgeCD = graph.GetEdge(c, d);

        // Selected edges randomly, so they have to exist
        Y_ASSERT(edgeAB);
        Y_ASSERT(edgeCD);

        int oldComponents = graph.GetNumComponents();

        // Perform swap: (a-b, c-d) -> (a-c, b-d)
        graph.Disconnect(a, b);
        graph.Disconnect(c, d);
        graph.Connect(rng, a, c, pyConfig);
        graph.Connect(rng, b, d, pyConfig);

        int newComponents = graph.GetNumComponents();

        // Metropolis-Hastings acceptance
        double deltaEnergy = mcmcConfig.ConnectivityPenalty * (newComponents - oldComponents);
        double temperature = CalculateTemperature(mcmcConfig, result.TotalAttempts, params.MaxAttempts);
        bool accept = (deltaEnergy <= 0) || (uniform01(rng) < std::exp(-deltaEnergy / temperature));

        if (accept) {
            ++result.SuccessfulSwaps;
        } else {
            // Undo: restore original edges with original keys
            graph.Disconnect(a, c);
            graph.Disconnect(b, d);
            graph.ConnectWithKeys(a, b, edgeAB->ColumnLHS, edgeAB->ColumnRHS);
            graph.ConnectWithKeys(c, d, edgeCD->ColumnLHS, edgeCD->ColumnRHS);
            ++result.RejectedByMetropolis;
        }
    }

    if (mcmcConfig.EnsureConnectivity && !graph.IsConnected()) {
        ForceReconnection(rng, graph, pyConfig);
        result.ForcedReconnection = true;
    }

    result.FinalComponents = graph.GetNumComponents();
    return result;
}

// Edge preserving MCMC is simpler than degree preserving one, because in space
// of connected graphs edge preserving MCMC is ergodic without annealing,
// which degree preserving MCMC is not (e.g. a tree)
TMCMCResult MCMCRandomizeEdgePreserving(TRNG& rng, TRelationGraph& graph, TPitmanYorConfig pyConfig, TMCMCConfig mcmcConfig) {
    TMCMCResult result;

    if (graph.GetN() < 2) {
        result.FinalComponents = graph.GetNumComponents();
        return result;
    }

    auto params = TMCMCIterationParams::Calculate(graph.GetNumEdges() / 2, mcmcConfig);

    while (result.TotalAttempts < params.MaxAttempts &&
           result.SuccessfulSwaps < params.NumIterations)
    {
        ++result.TotalAttempts;

        // Select random edge to relocate
        unsigned oldU, oldV;
        if (!graph.SelectRandomEdge(rng, oldU, oldV)) {
            continue;
        }

        // Select random new endpoints
        unsigned newU = graph.SelectRandomNode(rng);
        unsigned newV = graph.SelectRandomNode(rng);

        // Validate
        if (newU == newV) {
            ++result.RejectedByGeometry;
            continue;
        }

        if ((newU == oldU && newV == oldV) || (newU == oldV && newV == oldU)) {
            ++result.RejectedByGeometry;
            continue;
        }

        if (graph.HasEdge(newU, newV)) {
            ++result.RejectedByGeometry;
            continue;
        }

        // Relocate edge
        graph.Disconnect(oldU, oldV);
        graph.Connect(rng, newU, newV, pyConfig);
        ++result.SuccessfulSwaps;
    }

    if (mcmcConfig.EnsureConnectivity && !graph.IsConnected()) {
        ForceReconnection(rng, graph, pyConfig);
        result.ForcedReconnection = true;
    }

    result.FinalComponents = graph.GetNumComponents();
    return result;
}

void ForceReconnection(TRNG& rng, TRelationGraph& graph, TPitmanYorConfig config) {
    while (!graph.IsConnected()) {
        std::vector<int> components = graph.FindComponents();
        int numComponents = *std::max_element(components.begin(), components.end()) + 1;

        if (numComponents <= 1) {
            break;
        }

        // Group nodes by component
        std::vector<std::vector<ui32>> componentNodes(numComponents);
        for (ui32 i = 0; i < components.size(); ++i) {
            componentNodes[components[i]].push_back(i);
        }

        // Pick two different components randomly
        std::uniform_int_distribution<> componentDist(0, numComponents - 1);
        int comp1 = componentDist(rng);
        int comp2 = componentDist(rng);

        // Make sure they're different
        while (comp2 == comp1) {
            comp2 = componentDist(rng);
        }

        // Pick random nodes from each component
        std::uniform_int_distribution<> nodeDist1(0, componentNodes[comp1].size() - 1);
        std::uniform_int_distribution<> nodeDist2(0, componentNodes[comp2].size() - 1);

        ui32 node1 = componentNodes[comp1][nodeDist1(rng)];
        ui32 node2 = componentNodes[comp2][nodeDist2(rng)];

        // Connect them if not already connected
        if (!graph.HasEdge(node1, node2)) {
            graph.Connect(rng, node1, node2, config);
        }
    }
}

} // namespace NKikimr::NKqp
