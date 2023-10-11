#pragma once

#include "hive.h"
#include "hive_log.h"

#include <set>
#include <unordered_map>
#include <iostream>

namespace NKikimr {
namespace NHive {

struct TObjectDistribution {
    std::multiset<i64> SortedDistribution;
    std::unordered_map<TNodeId, i64> Distribution;
    const TObjectId Id;
    double Mean = 0;
    double VarianceNumerator = 0;

    TObjectDistribution(TObjectId id) : Id(id) {}

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

    void UpdateCount(TNodeId node, i64 diff) {
        auto [it, newNode] = Distribution.insert({node, 0});
        i64& value = it->second;
        i64 numNodes = Distribution.size();
        if (!newNode) {
            RemoveFromSortedDistribution(value);
        }
        if (diff + value < 0) {
            BLOG_ERROR("UpdateObjectCount: new value " << diff + value << " is negative");
        }
        Y_VERIFY_DEBUG(diff + value >= 0);
        value += diff;
        SortedDistribution.insert(value);
        double newMean = (Mean * (numNodes - 1) + value) / numNodes;
        VarianceNumerator += (Mean - value) * (newMean - value);
        Mean = newMean;
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
    std::unordered_map<TObjectId, std::multiset<TObjectDistribution>::iterator> Distributions;
    ui64 ImbalancedObjects = 0;
    std::unordered_set<TNodeId> Nodes;
    bool Enabled = true;

    double GetMaxImbalance() {
        if (SortedDistributions.empty()) {
            return 0;
        }
        return SortedDistributions.rbegin()->GetImbalance();
    }

    struct TObjectToBalance {
        TObjectId ObjectId;
        std::vector<TNodeId> Nodes;

        TObjectToBalance(TObjectId objectId) : ObjectId(objectId) {}
    };

    TObjectToBalance GetObjectToBalance() {
        Y_DEBUG_ABORT_UNLESS(!SortedDistributions.empty());
        if (SortedDistributions.empty()) {
            return TObjectToBalance(0);
        }
        const auto& dist = *SortedDistributions.rbegin();
        i64 maxCnt = *dist.SortedDistribution.rbegin();
        TObjectToBalance result(dist.Id);
        for (const auto& [node, cnt] : dist.Distribution) {
            ui64 n = node;
            i64 c = cnt;
            BLOG_TRACE("Node " << n << "has " << c << ", maximum: " << maxCnt);
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
    bool UpdateDistribution(TObjectId object, F updateFunc) {
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
        if (!dist.Distribution.empty()) {
            auto sortedIt = SortedDistributions.insert(std::move(handle));
            distIt->second = sortedIt;
        } else {
            Distributions.erase(distIt);
        }
        return true;
    }


    void UpdateCount(TObjectId object, TNodeId node, i64 diff) {
        if (!Enabled) {
            return;
        }
        auto updateFunc = [=](TObjectDistribution& dist) {
            dist.UpdateCount(node, diff);
        };
        if (!UpdateDistribution(object, updateFunc)) {
            TObjectDistribution dist(object);
            for (auto node : Nodes) {
                dist.UpdateCount(node, 0);
            }
            dist.UpdateCount(node, diff);
            auto sortedDistIt = SortedDistributions.insert(std::move(dist));
            Distributions.emplace(object, sortedDistIt);
            return;
        }
        // std::cerr << object << ": " << diff << " ~>" << GetTotalImbalance() << std::endl;
    }

    void AddNode(TNodeId node) {
        if (!Enabled) {
            return;
        }
        Nodes.insert(node);
        for (const auto& [obj, it] : Distributions) {
            UpdateCount(obj, node, 0);
        }
    }

    void RemoveNode(TNodeId node) {
        if (!Enabled) {
            return;
        }
        Nodes.erase(node);
        auto updateFunc = [=](TObjectDistribution& dist) {
            dist.RemoveNode(node);
        };
        for (auto it = Distributions.begin(); it != Distributions.end();) {
            UpdateDistribution((it++)->first, updateFunc);
        }
    }

    void Disable() {
        Enabled = false;
        Nodes.clear();
        SortedDistributions.clear();
        Distributions.clear();
    }
};

} // NHive
} // NKikimr
