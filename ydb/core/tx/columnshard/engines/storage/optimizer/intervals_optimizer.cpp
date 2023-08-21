#include "intervals_optimizer.h"
#include <ydb/core/tx/columnshard/engines/changes/general_compaction.h>

namespace NKikimr::NOlap::NStorageOptimizer {

std::shared_ptr<TColumnEngineChanges> TIntervalsOptimizerPlanner::GetSmallPortionsMergeTask(const TCompactionLimits& limits, std::shared_ptr<TGranuleMeta> granule) const {
    if (SumSmall > 8 * 1024 * 1024) {
        ui64 currentSum = 0;
        std::map<ui64, std::shared_ptr<TPortionInfo>> portions;
        for (auto&& i : SmallBlobs) {
            for (auto&& c : i.second) {
                currentSum += c.second->BlobsBytes();
                portions.emplace(c.first, c.second);
                if (currentSum > 8 * 1024 * 1024) {
                    break;
                }
            }
            if (currentSum > 8 * 1024 * 1024) {
                break;
            }
        }
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "take_granule_with_small")("portions", portions.size())("current_sum", currentSum)("remained", SmallBlobs.size())("remained_size", SumSmall);
        return std::make_shared<TGeneralCompactColumnEngineChanges>(limits, granule, portions);
    }
    return nullptr;
}

std::shared_ptr<TColumnEngineChanges> TIntervalsOptimizerPlanner::DoGetOptimizationTask(const TCompactionLimits& limits, std::shared_ptr<TGranuleMeta> granule, const THashSet<TPortionAddress>& busyPortions) const {
    if (auto result = GetSmallPortionsMergeTask(limits, granule)) {
        return result;
    }
    if (RangedSegments.empty()) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "no_ranged_segments");
        return nullptr;
    }
    auto& topSegment = **RangedSegments.begin()->second.begin();
    auto& topFeaturesTask = topSegment.GetFeatures();
    if (!topFeaturesTask.IsCritical()) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "not_critical_task_top");
        return nullptr;
    }
    TIntervalFeatures features;
    if (topFeaturesTask.GetPortionsRawWeight() > 512 * 1024 * 1024) {
        for (auto&& p : topFeaturesTask.GetSummaryPortions()) {
            features.Add(p.second);
            if (features.GetPortionsCount() > 1 && features.GetPortionsRawWeight() > 512 * 1024 * 1024) {
                break;
            }
        }
    } else {
        auto itFwd = Positions.find(topSegment.GetPosition());
        Y_VERIFY(itFwd != Positions.end());
        features = itFwd->second.GetFeatures();
        // this method made iterator next for itFwdPosition by reverse direction (left for topSegment.GetPosition())
        auto itReverse = std::make_reverse_iterator(itFwd);
        ++itFwd;
        while (!features.IsEnoughWeight()) {
            if (itFwd == Positions.end() && itReverse == Positions.rend()) {
                break;
            }
            if (itFwd == Positions.end()) {
                if (!features.Merge(itReverse->second.GetFeatures(), 512 * 1024 * 1024)) {
                    break;
                }
                ++itReverse;
            } else if (itReverse == Positions.rend()) {
                if (!features.Merge(itFwd->second.GetFeatures(), 512 * 1024 * 1024)) {
                    break;
                }
                ++itFwd;
            } else if (itFwd->second.GetFeatures().GetUsefulKff() < itReverse->second.GetFeatures().GetUsefulKff()) {
                if (!features.Merge(itReverse->second.GetFeatures(), 512 * 1024 * 1024)) {
                    break;
                }
                ++itReverse;
            } else {
                if (!features.Merge(itFwd->second.GetFeatures(), 512 * 1024 * 1024)) {
                    break;
                }
                ++itFwd;
            }
        }
    }
    Y_VERIFY(features.GetPortionsCount() > 1);

    for (auto&& i : features.GetSummaryPortions()) {
        if (busyPortions.contains(i.second->GetAddress())) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "take_granule_skip")("features", features.DebugJson().GetStringRobust())
                ("count", features.GetSummaryPortions().size())("reason", "busy_portion")("portion_address", i.second->GetAddress().DebugString());
            return nullptr;
        }
    }

    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "take_granule")("features", features.DebugJson().GetStringRobust())("count", features.GetSummaryPortions().size());
    return std::make_shared<TGeneralCompactColumnEngineChanges>(limits, granule, features.GetSummaryPortions());
}

} // namespace NKikimr::NOlap
