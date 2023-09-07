#include "intervals_optimizer.h"
#include <ydb/core/tx/columnshard/engines/changes/general_compaction.h>
#include <ydb/core/tx/columnshard/counters/common/owner.h>
#include <ydb/core/tx/columnshard/counters/engine_logs.h>

namespace NKikimr::NOlap::NStorageOptimizer {

ui64 TIntervalsOptimizerPlanner::LimitSmallBlobsMerge = 2 * 1024 * 1024;
ui64 TIntervalsOptimizerPlanner::LimitSmallBlobDetect = 512 * 1024;

class TGlobalCounters: public NColumnShard::TCommonCountersOwner {
private:
    using TBase = NColumnShard::TCommonCountersOwner;
    NMonitoring::TDynamicCounters::TCounterPtr SmallPortionsCount;
    std::shared_ptr<NColumnShard::TIncrementalHistogram> HistogramOverlappedIntervalsCount;
    std::shared_ptr<NColumnShard::TIncrementalHistogram> HistogramOverlappedIntervalsPackedSizeCount;
    std::shared_ptr<NColumnShard::TIncrementalHistogram> HistogramOverlappedIntervalsRawSizeCount;
public:
    TGlobalCounters()
        : TBase("IntervalsStorageOptimizer") {
        SmallPortionsCount = TBase::GetValue("SmallPortions/Count");

        const std::set<i64> borders = {0, 1, 2, 4, 8, 16, 32, 64};
        HistogramOverlappedIntervalsCount = std::make_shared<NColumnShard::TIncrementalHistogram>("IntervalsStorageOptimizer", "OverlappedIntervals/Count", "", borders);
        HistogramOverlappedIntervalsPackedSizeCount = std::make_shared<NColumnShard::TIncrementalHistogram>("IntervalsStorageOptimizer", "OverlappedIntervals/Size/Packed", "", borders);
        HistogramOverlappedIntervalsRawSizeCount = std::make_shared<NColumnShard::TIncrementalHistogram>("IntervalsStorageOptimizer", "OverlappedIntervals/Size/Raw", "", borders);
    }

    static std::shared_ptr<NColumnShard::TIncrementalHistogram::TGuard> BuildGuardIntervalsOverlapping() {
        return Singleton<TGlobalCounters>()->HistogramOverlappedIntervalsCount->BuildGuard();
    }

    static std::shared_ptr<NColumnShard::TIncrementalHistogram::TGuard> BuildGuardIntervalsPackedSizeOverlapping() {
        return Singleton<TGlobalCounters>()->HistogramOverlappedIntervalsPackedSizeCount->BuildGuard();
    }

    static std::shared_ptr<NColumnShard::TIncrementalHistogram::TGuard> BuildGuardIntervalsRawSizeOverlapping() {
        return Singleton<TGlobalCounters>()->HistogramOverlappedIntervalsRawSizeCount->BuildGuard();
    }

    static std::shared_ptr<NColumnShard::TValueGuard> BuildSmallPortionsGuard() {
        return std::make_shared<NColumnShard::TValueGuard>(Singleton<TGlobalCounters>()->SmallPortionsCount);
    }

};

class TCounters {
private:
    std::shared_ptr<NColumnShard::TIncrementalHistogram::TGuard> IntervalsGuard;
    std::shared_ptr<NColumnShard::TIncrementalHistogram::TGuard> IntervalsPackedSizeGuard;
    std::shared_ptr<NColumnShard::TIncrementalHistogram::TGuard> IntervalsRawSizeGuard;
    std::shared_ptr<NColumnShard::TValueGuard> SmallPortionsCount;
public:
    TCounters() {
        IntervalsGuard = TGlobalCounters::BuildGuardIntervalsOverlapping();
        IntervalsPackedSizeGuard = TGlobalCounters::BuildGuardIntervalsPackedSizeOverlapping();
        IntervalsRawSizeGuard = TGlobalCounters::BuildGuardIntervalsRawSizeOverlapping();
        SmallPortionsCount = TGlobalCounters::BuildSmallPortionsGuard();
    }

    void OnRemoveIntervalsCount(const ui32 count, const ui64 rawSize, const ui64 packedSize) {
        IntervalsGuard->Sub(count, 1);
        IntervalsPackedSizeGuard->Sub(count, packedSize);
        IntervalsRawSizeGuard->Sub(count, rawSize);
    }

    void OnAddIntervalsCount(const ui32 count, const ui64 rawSize, const ui64 packedSize) {
        IntervalsGuard->Add(count, 1);
        IntervalsPackedSizeGuard->Add(count, packedSize);
        IntervalsRawSizeGuard->Add(count, rawSize);
    }

    void OnAddSmallPortion() {
        SmallPortionsCount->Add(1);
    }

    void OnRemoveSmallPortion() {
        SmallPortionsCount->Sub(1);
    }

};

std::shared_ptr<TColumnEngineChanges> TIntervalsOptimizerPlanner::GetSmallPortionsMergeTask(const TCompactionLimits& limits, std::shared_ptr<TGranuleMeta> granule) const {
    if (SumSmall > (i64)LimitSmallBlobsMerge) {
        ui64 currentSum = 0;
        std::map<ui64, std::shared_ptr<TPortionInfo>> portions;
        for (auto&& i : SmallBlobs) {
            for (auto&& c : i.second) {
                currentSum += c.second->RawBytesSum();
                portions.emplace(c.first, c.second);
            }
        }
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "take_granule_with_small")("portions", portions.size())("current_sum", currentSum)("remained", SmallBlobs.size())("remained_size", SumSmall);
        return std::make_shared<NCompaction::TGeneralCompactColumnEngineChanges>(limits, granule, portions);
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
    auto& topSegment = **RangedSegments.rbegin()->second.begin();
    auto& topFeaturesTask = topSegment.GetFeatures();
    TIntervalFeatures features;
    if (topFeaturesTask.IsEnoughWeight()) {
        if (topFeaturesTask.GetPortionsCount() == 1) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "not_critical_task_top");
            return nullptr;
        }
        std::map<ui64, std::vector<std::shared_ptr<TPortionInfo>>> sortedPortions;
        for (auto&& p : topFeaturesTask.GetSummaryPortions()) {
            sortedPortions[p.second->BlobsBytes()].emplace_back(p.second);
        }

        for (auto&& s : sortedPortions) {
            for (auto&& p : s.second) {
                if (features.GetPortionsCount() > 1 && s.first > 128 * 1024) {
                    if (features.GetPortionsWeight() + p->BlobsBytes() > 512 * 1024 * 1024 && features.GetPortionsCount() > 1) {
                        break;
                    }
                }
                features.Add(p);
            }
        }
    } else {
        auto itFwd = Positions.find(topSegment.GetPosition());
        Y_VERIFY(itFwd != Positions.end());
        features = itFwd->second.GetFeatures();
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
    if (features.GetPortionsCount() <= 1) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "take_granule_skip")("features", features.DebugJson().GetStringRobust())("reason", "one_portion");
        return nullptr;
    }

    for (auto&& i : features.GetSummaryPortions()) {
        if (busyPortions.contains(i.second->GetAddress())) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "take_granule_skip")("features", features.DebugJson().GetStringRobust())
                ("count", features.GetSummaryPortions().size())("reason", "busy_portion")("portion_address", i.second->GetAddress().DebugString());
            return nullptr;
        }
    }
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "take_granule")("features", features.DebugJson().GetStringRobust())("count", features.GetSummaryPortions().size());
    return std::make_shared<NCompaction::TGeneralCompactColumnEngineChanges>(limits, granule, features.GetSummaryPortions());
}

bool TIntervalsOptimizerPlanner::RemoveSmallPortion(const std::shared_ptr<TPortionInfo>& info) {
    if (info->BlobsBytes() < LimitSmallBlobDetect) {
        Counters->OnRemoveSmallPortion();
        auto it = SmallBlobs.find(info->IndexKeyStart());
        Y_VERIFY(it->second.erase(info->GetPortion()));
        if (it->second.empty()) {
            SmallBlobs.erase(it);
        }
        SumSmall -= info->BlobsBytes();
        Y_VERIFY(SumSmall >= 0);
        return true;
    }
    return false;
}

bool TIntervalsOptimizerPlanner::AddSmallPortion(const std::shared_ptr<TPortionInfo>& info) {
    if (info->BlobsBytes() < LimitSmallBlobDetect) {
        Counters->OnAddSmallPortion();
        Y_VERIFY(SmallBlobs[info->IndexKeyStart()].emplace(info->GetPortion(), info).second);
        SumSmall += info->BlobsBytes();
        return true;
    }
    return false;
}

void TIntervalsOptimizerPlanner::DoRemovePortion(const std::shared_ptr<TPortionInfo>& info) {
    RemoveSmallPortion(info);
    AFL_TRACE(NKikimrServices::TX_COLUMNSHARD)("event", "remove_portion")("portion_id", info->GetPortion());
    auto itStart = Positions.find(info->IndexKeyStart());
    auto itFinish = Positions.find(info->IndexKeyEnd());
    Y_VERIFY(itStart != Positions.end());
    Y_VERIFY(itFinish != Positions.end());
    for (auto it = itStart; it != itFinish; ++it) {
        RemoveRanged(it->second);
        it->second.RemoveSummary(info);
        AddRanged(it->second);
    }
    if (itStart->second.RemoveStart(info)) {
        RemoveRanged(itStart->second);
        Positions.erase(itStart);
    }
    if (itFinish->second.RemoveFinish(info)) {
        RemoveRanged(itFinish->second);
        Positions.erase(itFinish);
    }
}

void TIntervalsOptimizerPlanner::DoAddPortion(const std::shared_ptr<TPortionInfo>& info) {
    AddSmallPortion(info);
    AFL_TRACE(NKikimrServices::TX_COLUMNSHARD)("event", "add_portion")("portion_id", info->GetPortion());
    auto itStart = Positions.find(info->IndexKeyStart());
    if (itStart == Positions.end()) {
        itStart = Positions.emplace(info->IndexKeyStart(), TBorderPositions(info->IndexKeyStart())).first;
        if (itStart != Positions.begin()) {
            auto itStartCopy = itStart;
            --itStartCopy;
            itStart->second.CopyFrom(itStartCopy->second);
            AddRanged(itStart->second);
        }
    }
    auto itEnd = Positions.find(info->IndexKeyEnd());
    if (itEnd == Positions.end()) {
        itEnd = Positions.emplace(info->IndexKeyEnd(), TBorderPositions(info->IndexKeyEnd())).first;
        Y_VERIFY(itEnd != Positions.begin());
        auto itEndCopy = itEnd;
        --itEndCopy;
        itEnd->second.CopyFrom(itEndCopy->second);
        AddRanged(itEnd->second);
        itStart = Positions.find(info->IndexKeyStart());
    }
    Y_VERIFY(itStart != Positions.end());
    Y_VERIFY(itEnd != Positions.end());
    itStart->second.AddStart(info);
    itEnd->second.AddFinish(info);
    for (auto it = itStart; it != itEnd; ++it) {
        RemoveRanged(it->second);
        it->second.AddSummary(info);
        AddRanged(it->second);
    }
}

void TIntervalsOptimizerPlanner::RemoveRanged(const TBorderPositions& data) {
    if (!!data.GetFeatures()) {
        Counters->OnRemoveIntervalsCount(data.GetFeatures().GetSummaryPortions().size(), data.GetFeatures().GetPortionsRawWeight(), data.GetFeatures().GetPortionsWeight());
        auto itFeatures = RangedSegments.find(data.GetFeatures());
        Y_VERIFY(itFeatures->second.erase(&data));
        if (itFeatures->second.empty()) {
            RangedSegments.erase(itFeatures);
        }
    }
}

void TIntervalsOptimizerPlanner::AddRanged(const TBorderPositions& data) {
    if (!!data.GetFeatures()) {
        Counters->OnAddIntervalsCount(data.GetFeatures().GetSummaryPortions().size(), data.GetFeatures().GetPortionsRawWeight(), data.GetFeatures().GetPortionsWeight());
        Y_VERIFY(RangedSegments[data.GetFeatures()].emplace(&data).second);
    }
}

TIntervalsOptimizerPlanner::TIntervalsOptimizerPlanner(const ui64 granuleId)
    : TBase(granuleId) {
    Counters = std::make_shared<TCounters>();
}

std::vector<std::shared_ptr<TPortionInfo>> TIntervalsOptimizerPlanner::DoGetPortionsOrderedByPK(const TSnapshot& snapshot) const {
    std::vector<std::shared_ptr<TPortionInfo>> result;
    for (auto&& i : Positions) {
        for (auto&& p : i.second.GetPositions()) {
            if (!p.first.GetIsStart()) {
                continue;
            }
            if (!p.second.GetPortion().IsVisible(snapshot)) {
                continue;
            }
            result.emplace_back(p.second.GetPortionPtr());
        }
    }
    return result;
}

i64 TIntervalsOptimizerPlanner::DoGetUsefulMetric() const {
    if (RangedSegments.empty()) {
        return 0;
    }
    auto& topSegment = **RangedSegments.rbegin()->second.begin();
    auto& topFeaturesTask = topSegment.GetFeatures();
    return std::max<i64>(topFeaturesTask.GetUsefulMetric(), std::max<ui64>(SumSmall ? 1 : 0, SumSmall / LimitSmallBlobsMerge));
}

TString TIntervalsOptimizerPlanner::DoDebugString() const {
    NJson::TJsonValue result = NJson::JSON_MAP;
    auto& positions = result.InsertValue("positions", NJson::JSON_ARRAY);
    for (auto&& i : Positions) {
        positions.AppendValue(i.second.DebugJson());
    }
    return result.GetStringRobust();
}

void TIntervalsOptimizerPlanner::TBorderPositions::AddSummary(const std::shared_ptr<TPortionInfo>& info) {
    Features.Add(info);
}

void TIntervalsOptimizerPlanner::TBorderPositions::RemoveSummary(const std::shared_ptr<TPortionInfo>& info) {
    Features.Remove(info);
}

} // namespace NKikimr::NOlap
