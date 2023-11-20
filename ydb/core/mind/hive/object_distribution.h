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
    std::multiset<i64> SortedDistribution;
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
        i64 minVal = *SortedDistribution.begin();
        i64 maxVal = *SortedDistribution.rbegin();
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

    void RemoveFromSortedDistribution(i64 value) {
        i64 numNodes = Distribution.size();
        auto it = SortedDistribution.find(value);
        SortedDistribution.erase(it);
        double meanWithoutNode = 0;
        if (numNodes > 1) {
            meanWithoutNode = (Mean * numNodes - value) / (numNodes - 1);
        }
        VarianceNumerator -= (Mean - value) * (meanWithoutNode - value);
        Mean = meanWithoutNode;
    }

    void UpdateCount(const TNodeInfo& node, i64 diff) {
        if (!node.MatchesFilter(NodeFilter) || !node.IsAllowedToRunTablet()) {
            return;
        }
        auto [it, newNode] = Distribution.insert({node.Id, 0});
        i64& value = it->second;
        i64 numNodes = Distribution.size();
        if (!newNode) {
            RemoveFromSortedDistribution(value);
        }
        if (diff + value < 0) {
            BLOG_ERROR("UpdateObjectCount: new value " << diff + value << " is negative");
        }
        Y_DEBUG_ABORT_UNLESS(diff + value >= 0);
        value += diff;
        SortedDistribution.insert(value);
        double newMean = (Mean * (numNodes - 1) + value) / numNodes;
        VarianceNumerator += (Mean - value) * (newMean - value);
        Mean = newMean;
    }

    void SetCount(const TNodeInfo& node, i64 value) {
        auto it = Distribution.find(node.Id);
        i64 oldValue = (it == Distribution.end()) ? 0 : it->second;
        UpdateCount(node, value - oldValue);
    }

    void RemoveNode(TNodeId node) {
        auto it = Distribution.find(node);
        if (it == Distribution.end()) {
            return;
        }
        RemoveFromSortedDistribution(it->second);
        Distribution.erase(node);
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

        TObjectToBalance(TFullObjectId objectId) : ObjectId(objectId) {}
    };

    TObjectToBalance GetObjectToBalance() {
        Y_DEBUG_ABORT_UNLESS(!SortedDistributions.empty());
        if (SortedDistributions.empty()) {
            return TObjectToBalance(TFullObjectId());
        }
        const auto& dist = *SortedDistributions.rbegin();
        i64 maxCnt = *dist.SortedDistribution.rbegin();
        TObjectToBalance result(dist.Id);
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
            UpdateCount(obj, node, 0);
        }
        for (const auto& [obj, tablets] : node.TabletsOfObject) {
            ui64 cnt = tablets.size();
            auto updateFunc = [&](TObjectDistribution& dist) {
                dist.SetCount(node, cnt);
            };
            UpdateDistribution(obj, updateFunc);
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
