#pragma once

#include "hive.h"
#include "hive_log.h"
#include "node_info.h"
#include "tablet_info.h"

#include <set>
#include <unordered_map>
#include <iostream>

namespace NKikimr {
namespace NHive {

struct TObjectDistribution {
    struct CountOnNode {
        i64 Count;
        TNodeId Node;

        CountOnNode(i64 count, TNodeId node) : Count(count), Node(node) {}

        bool operator<(const CountOnNode& other) const {
            return std::tie(Count, Node) < std::tie(other.Count, other.Node);
        }
    };

    std::set<CountOnNode> SortedDistribution;
    std::unordered_map<TNodeId, i64> Distribution;
    const TFullObjectId Id;
    double Mean = 0;
    double VarianceNumerator = 0;
    TNodeFilter NodeFilter; // We assume all tablets of one object have the same filter

    TObjectDistribution(const TLeaderTabletInfo& tablet) : Id(tablet.ObjectId)
                                                         , NodeFilter(tablet.NodeFilter)
    {
    }

    double GetImbalance() const {
        if (SortedDistribution.empty()) {
            return 0;
        }
        i64 minVal = SortedDistribution.begin()->Count;
        i64 maxVal = SortedDistribution.rbegin()->Count;
        if (maxVal == 0) {
            return 0;
        }
        return (std::max<double>(maxVal - minVal, 1) - 1) / maxVal;
    }

    double GetVariance() const {
        if (Distribution.empty()) {
            return 0;
        }
        return VarianceNumerator / Distribution.size();
    }

    void RemoveFromSortedDistribution(CountOnNode value) {
        auto cnt = value.Count;
        i64 numNodes = Distribution.size();
        auto it = SortedDistribution.find(value);
        if (it == SortedDistribution.end()) {
            return;
        }
        SortedDistribution.erase(it);
        double meanWithoutNode = 0;
        if (numNodes > 1) {
            meanWithoutNode = (Mean * numNodes - cnt) / (numNodes - 1);
        }
        VarianceNumerator -= (Mean - cnt) * (meanWithoutNode - cnt);
        Mean = meanWithoutNode;
    }

    void UpdateCount(const TNodeInfo& node, i64 diff) {
        if (!node.IsAllowedToRunTablet()) {
            // We should not use this node for computing imbalance, hence we ignore it in SortedDistribution
            // But we still account for it in Distribution, because it might become relevant later
            Distribution[node.Id] += diff;
            return;
        }
        auto [it, newNode] = Distribution.insert({node.Id, 0});
        i64& value = it->second;
        i64 numNodes = Distribution.size();
        if (!newNode) {
            RemoveFromSortedDistribution({value, node.Id});
        }
        if (diff + value < 0) {
            BLOG_ERROR("UpdateObjectCount: new value " << diff + value << " is negative");
        }
        Y_DEBUG_ABORT_UNLESS(diff + value >= 0);
        value += diff;
        SortedDistribution.emplace(value, node.Id);
        double newMean = (Mean * (numNodes - 1) + value) / numNodes;
        VarianceNumerator += (Mean - value) * (newMean - value);
        Mean = newMean;
    }

    void RemoveNode(TNodeId node) {
        auto it = Distribution.find(node);
        if (it == Distribution.end()) {
            return;
        }
        RemoveFromSortedDistribution({it->second, node});
    }

    bool operator<(const TObjectDistribution& other) const {
        return GetImbalance() < other.GetImbalance();
    }
};

struct TObjectDistributions {
    std::multiset<TObjectDistribution> SortedDistributions;
    std::unordered_map<TFullObjectId, std::multiset<TObjectDistribution>::iterator> Distributions;
    ui64 ImbalancedObjects = 0;
    const std::unordered_map<TNodeId, TNodeInfo>& Nodes;
    bool Enabled = true;

    TObjectDistributions(const std::unordered_map<TNodeId, TNodeInfo>& nodes) : Nodes(nodes) {}

    double GetMaxImbalance() {
        if (SortedDistributions.empty()) {
            return 0;
        }
        return SortedDistributions.rbegin()->GetImbalance();
    }

    struct TObjectToBalance {
        TFullObjectId ObjectId;
        std::vector<TNodeId> Nodes;
        TSubDomainKey SubDomain;

        TObjectToBalance(const TFullObjectId& object, const TSubDomainKey& subDomain) : ObjectId(object), SubDomain(subDomain) {}
    };

    TObjectToBalance GetObjectToBalance() {
        Y_DEBUG_ABORT_UNLESS(!SortedDistributions.empty());
        if (SortedDistributions.empty()) {
            return TObjectToBalance({}, {});
        }
        const auto& dist = *SortedDistributions.rbegin();
        i64 maxCnt = dist.SortedDistribution.rbegin()->Count;
        TObjectToBalance result(dist.Id, dist.NodeFilter.ObjectDomain);
        for (const auto& [node, cnt] : dist.Distribution) {
            if (cnt == maxCnt) {
                result.Nodes.push_back(node);
            }
        }
        return result;
    }

    ui64 GetImbalancedObjectsCount() {
        return ImbalancedObjects;
    }

    double GetWorstObjectVariance() {
        if (SortedDistributions.empty()) {
            return 0;
        }
        return SortedDistributions.rbegin()->GetVariance();
    }

    template <typename F>
    bool UpdateDistribution(TFullObjectId object, F updateFunc) {
        auto distIt = Distributions.find(object);
        if (distIt == Distributions.end()) {
            return false;
        }
        auto handle = SortedDistributions.extract(distIt->second);
        if (!handle) {
            return false;
        }
        auto& dist = handle.value();
        double imbalanceBefore = dist.GetImbalance();
        updateFunc(dist);
        double imbalanceAfter = dist.GetImbalance();
        if (imbalanceBefore <= 1e-7 && imbalanceAfter > 1e-7) {
            ++ImbalancedObjects;
        } else if (imbalanceBefore > 1e-7 && imbalanceAfter <= 1e-7) {
            --ImbalancedObjects;
        }
        auto sortedIt = SortedDistributions.insert(std::move(handle));
        distIt->second = sortedIt;
        return true;
    }


    bool UpdateCount(TFullObjectId object, const TNodeInfo& node, i64 diff) {
        auto updateFunc = [&](TObjectDistribution& dist) {
            dist.UpdateCount(node, diff);
        };
        return UpdateDistribution(object, updateFunc);
    }

    void UpdateCountForTablet(const TLeaderTabletInfo& tablet, const TNodeInfo& node, i64 diff) {
        if (!Enabled) {
            return;
        }
        auto object = tablet.ObjectId;
        if (!UpdateCount(object, node, diff)) {
            if (diff <= 0) {
                return;
            }
            TObjectDistribution dist(tablet);
            for (const auto& [nodeId, node] : Nodes) {
                if (node.IsAllowedToRunTablet(tablet)) {
                    dist.UpdateCount(node, 0);
                }
            }
            dist.UpdateCount(node, diff);
            auto sortedDistIt = SortedDistributions.insert(std::move(dist));
            Distributions.emplace(object, sortedDistIt);
        }
    }

    void AddNode(const TNodeInfo& node) {
        if (!Enabled) {
            return;
        }
        for (const auto& [obj, it] : Distributions) {
            if (node.MatchesFilter(it->NodeFilter)) {
                UpdateCount(obj, node, 0);
            }
        }
    }

    void RemoveNode(const TNodeInfo& node) {
        if (!Enabled) {
            return;
        }
        TNodeId nodeId = node.Id;
        auto updateFunc = [=](TObjectDistribution& dist) {
            dist.RemoveNode(nodeId);
        };
        for (auto it = Distributions.begin(); it != Distributions.end();) {
            UpdateDistribution((it++)->first, updateFunc);
        }
    }

    void Disable() {
        Enabled = false;
        SortedDistributions.clear();
        Distributions.clear();
    }
};

} // NHive
} // NKikimr
