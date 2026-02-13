#pragma once

#include "dq_opt_join_hypergraph.h"
#include <cmath>
#include <optional>
#include <vector>
#include <set>

namespace NYql::NDq {

    template <typename TNodeSet>
    struct THypergraphFeatures {
        size_t NumHyperedges = 0;
        double InnerJoinRatio = 0.0;
        double HyperedgeVolumeStd = 0.0;
        double DegreesStd = 0.0;
        double DegreesMean = 0.0;
        double PrimalAssortativity = 0.0;
        size_t PrimalDegeneracy = 0;
        double PrimalDensity = 0.0;
        double PrimalHeterogeneity = 0.0;
        double BipartiteAssortativity = 0.0;
        size_t BipartiteLeafNodes = 0;
    };

    // ==================== Helper Functions ====================

    double ComputeStdDev(const std::vector<double>& values);

    std::pair<double, double> ComputeMeanAndStdDev(const std::vector<double>& values);

    std::optional<double> ComputeAssortativity(
        const std::vector<double>& sourceDegrees,
        const std::vector<double>& targetDegrees
    );

    // ==================== Primal Graph Builder ====================

    template <typename TNodeSet>
    std::vector<std::set<size_t>> BuildPrimalAdjacency(TJoinHypergraph<TNodeSet>& graph) {
        std::vector<std::set<size_t>> adjacency(graph.GetNodes().size());

        for (const auto& edge : graph.GetEdges()) {
            if (edge.IsReversed) {
                continue;
            }
            for (auto leftIt = TSetBitsIt(edge.Left); leftIt.HasNext(); ) {
                size_t leftNode = leftIt.Next();
                for (auto rightIt = TSetBitsIt(edge.Right); rightIt.HasNext(); ) {
                    size_t rightNode = rightIt.Next();
                    adjacency[leftNode].insert(rightNode);
                    adjacency[rightNode].insert(leftNode);
                }
            }
        }

        return adjacency;
    }

    template <typename TNodeSet>
    std::vector<double> GetPrimalDegrees(const std::vector<std::set<size_t>>& adjacency) {
        std::vector<double> degrees;
        degrees.reserve(adjacency.size());
        for (const auto& neighbors : adjacency) {
            degrees.push_back(static_cast<double>(neighbors.size()));
        }
        return degrees;
    }

    // ==================== Feature Functions ====================

    template <typename TNodeSet>
    size_t ComputeNumHyperedges(TJoinHypergraph<TNodeSet>& graph) {
        size_t count = 0;
        for (const auto& edge : graph.GetEdges()) {
            if (!edge.IsReversed) {
                count++;
            }
        }
        return count;
    }

    template <typename TNodeSet>
    double ComputeInnerJoinRatio(TJoinHypergraph<TNodeSet>& graph) {
        size_t total = 0;
        size_t inner = 0;
        for (const auto& edge : graph.GetEdges()) {
            if (edge.IsReversed) {
                continue;
            }
            total++;
            if (edge.JoinKind == EJoinKind::InnerJoin) {
                inner++;
            }
        }
        return total > 0 ? static_cast<double>(inner) / total : 0.0;
    }

    template <typename TNodeSet>
    size_t PopCount(const TNodeSet& nodeSet) {
        size_t count = 0;
        for (auto it = TSetBitsIt(nodeSet); it.HasNext(); ) {
            it.Next();
            count++;
        }
        return count;
    }

    template <typename TNodeSet>
    double ComputeHyperedgeVolumeStd(TJoinHypergraph<TNodeSet>& graph) {
        std::vector<double> volumes;
        for (const auto& edge : graph.GetEdges()) {
            if (edge.IsReversed) {
                continue;
            }
            size_t volume = PopCount(edge.Left | edge.Right);
            volumes.push_back(static_cast<double>(volume));
        }
        return ComputeStdDev(volumes);
    }

    template <typename TNodeSet>
    std::pair<double, double> ComputeNodeDegreeStats(TJoinHypergraph<TNodeSet>& graph) {
        std::vector<size_t> degreeCounts(graph.GetNodes().size(), 0);

        for (const auto& edge : graph.GetEdges()) {
            if (edge.IsReversed) {
                continue;
            }
            TNodeSet combined = edge.Left | edge.Right;
            for (auto it = TSetBitsIt(combined); it.HasNext(); ) {
                degreeCounts[it.Next()]++;
            }
        }

        std::vector<double> degrees;
        degrees.reserve(degreeCounts.size());
        for (size_t deg : degreeCounts) {
            degrees.push_back(static_cast<double>(deg));
        }

        return ComputeMeanAndStdDev(degrees);
    }

    template <typename TNodeSet>
    std::optional<double> ComputePrimalAssortativity(TJoinHypergraph<TNodeSet>& graph) {
        auto adjacency = BuildPrimalAdjacency(graph);
        auto degrees = GetPrimalDegrees<TNodeSet>(adjacency);

        std::vector<double> sourceDegrees;
        std::vector<double> targetDegrees;

        for (size_t node = 0; node < adjacency.size(); ++node) {
            for (size_t neighbor : adjacency[node]) {
                if (node < neighbor) {
                    sourceDegrees.push_back(degrees[node]);
                    targetDegrees.push_back(degrees[neighbor]);
                }
            }
        }

        return ComputeAssortativity(sourceDegrees, targetDegrees);
    }

    template <typename TNodeSet>
    size_t ComputePrimalDegeneracy(TJoinHypergraph<TNodeSet>& graph) {
        auto adjacency = BuildPrimalAdjacency(graph);
        size_t numNodes = adjacency.size();

        std::vector<size_t> degree(numNodes);
        for (size_t idx = 0; idx < numNodes; ++idx) {
            degree[idx] = adjacency[idx].size();
        }

        std::vector<size_t> coreNumber(numNodes, 0);
        std::vector<bool> removed(numNodes, false);
        size_t remaining = numNodes;

        for (size_t currentCore = 0; remaining > 0; ) {
            bool foundNode = false;
            for (size_t idx = 0; idx < numNodes; ++idx) {
                if (!removed[idx] && degree[idx] <= currentCore) {
                    removed[idx] = true;
                    coreNumber[idx] = currentCore;
                    remaining--;
                    foundNode = true;
                    for (size_t neighbor : adjacency[idx]) {
                        if (!removed[neighbor]) {
                            degree[neighbor]--;
                        }
                    }
                }
            }
            if (!foundNode) {
                currentCore++;
            }
        }

        size_t maxCore = 0;
        for (size_t core : coreNumber) {
            maxCore = std::max(maxCore, core);
        }
        return maxCore;
    }

    template <typename TNodeSet>
    double ComputePrimalDensity(TJoinHypergraph<TNodeSet>& graph) {
        size_t numNodes = graph.GetNodes().size();
        if (numNodes < 2) {
            return 0.0;
        }

        auto adjacency = BuildPrimalAdjacency(graph);

        size_t numEdges = 0;
        for (size_t idx = 0; idx < adjacency.size(); ++idx) {
            for (size_t neighbor : adjacency[idx]) {
                if (idx < neighbor) {
                    numEdges++;
                }
            }
        }

        size_t maxEdges = numNodes * (numNodes - 1) / 2;
        return static_cast<double>(numEdges) / maxEdges;
    }

    template <typename TNodeSet>
    double ComputePrimalHeterogeneity(TJoinHypergraph<TNodeSet>& graph) {
        auto adjacency = BuildPrimalAdjacency(graph);
        auto degrees = GetPrimalDegrees<TNodeSet>(adjacency);

        if (degrees.empty()) {
            return 0.0;
        }

        double sumDegree = 0.0;
        double sumDegreeSq = 0.0;
        for (double deg : degrees) {
            sumDegree += deg;
            sumDegreeSq += deg * deg;
        }

        if (sumDegreeSq < 1e-12) {
            return 0.0;
        }

        double numNodes = static_cast<double>(degrees.size());
        return (sumDegree / numNodes) / (sumDegreeSq / numNodes);
    }

    template <typename TNodeSet>
    std::optional<double> ComputeBipartiteAssortativity(TJoinHypergraph<TNodeSet>& graph) {
        std::vector<size_t> nodeDegree(graph.GetNodes().size(), 0);
        std::vector<std::pair<size_t, std::vector<size_t>>> hyperedges;

        for (const auto& edge : graph.GetEdges()) {
            if (edge.IsReversed) {
                continue;
            }
            std::vector<size_t> edgeNodes;
            TNodeSet combined = edge.Left | edge.Right;
            for (auto it = TSetBitsIt(combined); it.HasNext(); ) {
                size_t nodeIdx = it.Next();
                edgeNodes.push_back(nodeIdx);
                nodeDegree[nodeIdx]++;
            }
            hyperedges.emplace_back(edgeNodes.size(), std::move(edgeNodes));
        }

        if (hyperedges.empty()) {
            return std::nullopt;
        }

        std::vector<double> sourceDegrees;
        std::vector<double> targetDegrees;

        for (const auto& [hyperedgeSize, nodes] : hyperedges) {
            double edgeDegree = static_cast<double>(hyperedgeSize);
            for (size_t nodeIdx : nodes) {
                sourceDegrees.push_back(static_cast<double>(nodeDegree[nodeIdx]));
                targetDegrees.push_back(edgeDegree);
            }
        }

        return ComputeAssortativity(sourceDegrees, targetDegrees);
    }

    template <typename TNodeSet>
    size_t ComputeBipartiteLeafNodes(TJoinHypergraph<TNodeSet>& graph) {
        std::vector<size_t> nodeDegree(graph.GetNodes().size(), 0);
        size_t hyperedgeLeaves = 0;

        for (const auto& edge : graph.GetEdges()) {
            if (edge.IsReversed) {
                continue;
            }
            TNodeSet combined = edge.Left | edge.Right;
            size_t edgeSize = PopCount(combined);
            if (edgeSize == 1) {
                hyperedgeLeaves++;
            }
            for (auto it = TSetBitsIt(combined); it.HasNext(); ) {
                nodeDegree[it.Next()]++;
            }
        }

        size_t nodeLeaves = 0;
        for (size_t deg : nodeDegree) {
            if (deg == 1) {
                nodeLeaves++;
            }
        }

        return nodeLeaves + hyperedgeLeaves;
    }

    template <typename TNodeSet>
    THypergraphFeatures<TNodeSet> ComputeAllFeatures(TJoinHypergraph<TNodeSet>& graph) {
        THypergraphFeatures<TNodeSet> features;

        features.NumHyperedges = ComputeNumHyperedges(graph);
        features.InnerJoinRatio = ComputeInnerJoinRatio(graph);
        features.HyperedgeVolumeStd = ComputeHyperedgeVolumeStd(graph);

        auto [degreesMean, degreesStd] = ComputeNodeDegreeStats(graph);
        features.DegreesMean = degreesMean;
        features.DegreesStd = degreesStd;

        features.PrimalAssortativity = ComputePrimalAssortativity(graph).value_or(0.0);
        features.PrimalDegeneracy = ComputePrimalDegeneracy(graph);
        features.PrimalDensity = ComputePrimalDensity(graph);
        features.PrimalHeterogeneity = ComputePrimalHeterogeneity(graph);

        features.BipartiteAssortativity = ComputeBipartiteAssortativity(graph).value_or(0.0);
        features.BipartiteLeafNodes = ComputeBipartiteLeafNodes(graph);

        return features;
    }

    // ==================== Safe Operators ====================

    double SafeDiv(double a, double b);
    double SafeSqrt(double a);
    double SafeLog(double a);
    double SafePow(double base, double exponent);
    double SafeExpm1(double a);

    // ==================== The predictor ====================

    // This prediction function is created by running symbolic
    // regression on a dataset of randomized join graphs
    // (collected by kqp_join_topology_ut.cpp)

    // It is a subject to change if CBO's enumeration algorithm
    // changes. E.g. a different approach, pruning states,
    // adding features like auto-selection of indexes, etc.

    // The set of features is designed to be mostly reusable
    // even if CBO optimization time changes and it needs to be
    // retrained and updated.

    // This function is specifically trained to predict
    // time of CBO with Shuffle Elimination enabled.
    template <typename TNodeSet>
    ui64 PredictCBOTime(TJoinHypergraph<TNodeSet>& hypergraph) {
        auto f = ComputeAllFeatures(hypergraph);
        double log1pTime =
            - f.DegreesStd * std::tanh(f.PrimalAssortativity + 0.887)
            - 1.79 * f.BipartiteAssortativity
            + SafeDiv(f.BipartiteAssortativity, f.DegreesMean + f.PrimalAssortativity)
            + SafeSqrt(
                f.NumHyperedges
                * SafeDiv(
                    (SafeDiv(
                       SafePow(f.PrimalDensity, f.DegreesMean - 0.664),
                       (f.DegreesStd - f.BipartiteAssortativity + 0.646))
                     + 0.328 * f.DegreesStd),
                    f.PrimalDensity
                  )
              )
            + 9.26;
        return SafeExpm1(log1pTime);
    }


    // ==================== Displaying predictions ====================

    std::string FormatTime(ui64 valueNs);

} // namespace NYql::NDq
