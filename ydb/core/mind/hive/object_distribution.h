#pragma once

#include "hive.h"
#include "hive_log.h"

#include <set>
#include <unordered_map>
#include <iostream>

namespace NKikimr {
namespace NHive {

struct TObjectDistribution {
    std::multiset<ui64> SortedDistribution;
    std::unordered_map<TNodeId, ui64> Distribution;
    const TObjectId Id;
    double Mean = 0;
    double VarianceNumerator = 0;

    TObjectDistribution(TObjectId id) : Id(id) {}

    ui64 GetImbalance() const {
        if (SortedDistribution.empty()) {
            return 0;
        }
        ui64 minVal = *SortedDistribution.begin();
        ui64 maxVal = *SortedDistribution.rbegin();
        return std::max<ui64>(maxVal - minVal, 1) - 1;
    }

    double GetVariance() const {
        if (Distribution.empty()) {
            return 0;
        }
        return VarianceNumerator / Distribution.size();
    }

    void UpdateCount(TNodeId node, i64 diff) {
        ui64& value = Distribution[node];
        auto it = SortedDistribution.find(value);
        i64 numNodes = Distribution.size();
        if (it != SortedDistribution.end()) {
            SortedDistribution.erase(it);
            double meanWithoutNode = 0;
            if (numNodes > 1) {
                meanWithoutNode = (Mean * numNodes - value) / (numNodes - 1);
            }
            VarianceNumerator -= (Mean - value) * (meanWithoutNode - value);
            Mean = meanWithoutNode;
        }
        Y_VERIFY(diff + value >= 0);
        value += diff;
        if (value > 0) {
            SortedDistribution.insert(value);
            double newMean = (Mean * (numNodes - 1) + value) / numNodes;
            VarianceNumerator += (Mean - value) * (newMean - value);
            Mean = newMean;
        } else {
            Distribution.erase(node);
        }
    }

    bool operator<(const TObjectDistribution& other) const {
        return GetImbalance() < other.GetImbalance();
    }
};

struct TObjectDistributions {
    std::multiset<TObjectDistribution> SortedDistributions;
    std::unordered_map<TObjectId, std::multiset<TObjectDistribution>::iterator> Distributions;
    ui64 TotalImbalance = 0;
    ui64 ImbalancedObjects = 0;

    ui64 GetTotalImbalance() {
        return TotalImbalance;
    }

    struct TObjectToBalance {
        TObjectId ObjectId;
        std::vector<TNodeId> Nodes;

        TObjectToBalance(TObjectId objectId) : ObjectId(objectId) {}
    };

    TObjectToBalance GetObjectToBalance() {
        Y_VERIFY(!SortedDistributions.empty());
        const auto& dist = *SortedDistributions.rbegin();
        ui64 maxCnt = *dist.SortedDistribution.rbegin();
        TObjectToBalance result(dist.Id);
        for (const auto& [node, cnt] : dist.Distribution) {
            ui64 n = node;
            ui64 c = cnt;
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

    void UpdateCount(TObjectId object, TNodeId node, i64 diff) {
        auto distIt = Distributions.find(object);
        if (distIt == Distributions.end()) {
            TObjectDistribution dist(object);
            dist.UpdateCount(node, diff);
            TotalImbalance += dist.GetImbalance();
            auto sortedDistIt = SortedDistributions.insert(std::move(dist));
            Distributions.emplace(object, sortedDistIt);
            return;
        }
        auto handle = SortedDistributions.extract(distIt->second);
        Y_VERIFY(handle);
        auto& dist = handle.value();
        ui64 imbalanceBefore = dist.GetImbalance();
        dist.UpdateCount(node, diff);
        ui64 imbalanceAfter = dist.GetImbalance();
        TotalImbalance += imbalanceAfter - imbalanceBefore;
        if (imbalanceBefore == 0 && imbalanceAfter > 0) {
            ++ImbalancedObjects;
        } else if (imbalanceBefore > 0 && imbalanceAfter == 0) {
            --ImbalancedObjects;
        }
        auto sortedIt = SortedDistributions.insert(std::move(handle));
        distIt->second = sortedIt;
    }
};

} // NHive
} // NKikimr
