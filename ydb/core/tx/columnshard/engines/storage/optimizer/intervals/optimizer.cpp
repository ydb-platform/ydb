#include "optimizer.h"
#include <ydb/core/tx/columnshard/blobs_action/abstract/storages_manager.h>
#include <ydb/core/tx/columnshard/counters/common/owner.h>
#include <ydb/core/tx/columnshard/counters/engine_logs.h>
#include <ydb/core/tx/columnshard/data_locks/manager/manager.h>
#include <ydb/core/tx/columnshard/engines/changes/general_compaction.h>

namespace NKikimr::NOlap::NStorageOptimizer {

std::vector<std::shared_ptr<TPortionInfo>> TIntervalsOptimizerPlanner::GetPortionsForIntervalStartedIn(const NArrow::TReplaceKey& keyStart, const ui32 countExpectation) const {
    std::vector<std::shared_ptr<TPortionInfo>> result;
    auto it = Positions.find(keyStart);
    AFL_VERIFY(it != Positions.end());
    THashSet<ui64> portionsCurrentlyClosed;
    auto itReverse = make_reverse_iterator(it);
    AFL_VERIFY(itReverse != Positions.rbegin());
    --itReverse;
    for (; itReverse != Positions.rend(); ++itReverse) {
        for (auto&& i : itReverse->second.GetPositions()) {
            if (i.first.GetIsStart()) {
                if (!portionsCurrentlyClosed.erase(i.first.GetPortionId())) {
                    result.emplace_back(i.second.GetPortionPtr());
                }
            } else {
                AFL_VERIFY(portionsCurrentlyClosed.emplace(i.first.GetPortionId()).second);
            }
        }
        if (result.size() == countExpectation) {
            return result;
        }
    }
    AFL_VERIFY(false)("result.size()", result.size())("expectation", countExpectation);
    return result;
}

std::shared_ptr<TColumnEngineChanges> TIntervalsOptimizerPlanner::DoGetOptimizationTask(const TCompactionLimits& limits, std::shared_ptr<TGranuleMeta> granule, const std::shared_ptr<NDataLocks::TManager>& locksManager) const {
    if (auto result = SizeProblemBlobs.BuildMergeTask(limits, granule, locksManager)) {
        return result;
    }
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("skip", "no_small_portion_tasks");
    return nullptr;
    if (RangedSegments.empty()) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "no_ranged_segments");
        return nullptr;
    }
    auto& topSegment = **RangedSegments.rbegin()->second.begin();
    auto& features = topSegment.GetFeatures();
    std::vector<std::shared_ptr<TPortionInfo>> portions = GetPortionsForIntervalStartedIn(topSegment.GetPosition(), features.GetPortionsCount());

    if (portions.size() <= 1) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "take_granule_skip")("features", features.DebugJson().GetStringRobust())("reason", "one_portion");
        return nullptr;
    }

    std::optional<TString> tierName;
    for (auto&& i : portions) {
        if (i->GetMeta().GetTierName() && (!tierName || *tierName < i->GetMeta().GetTierName())) {
            tierName = i->GetMeta().GetTierName();
        }
        if (locksManager->IsLocked(*i)) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "take_granule_skip")("features", features.DebugJson().GetStringRobust())
                ("count", features.GetPortionsCount())("reason", "busy_portion")("portion_address", i->GetAddress().DebugString());
            return nullptr;
        }
    }
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "take_granule")("features", features.DebugJson().GetStringRobust())("count", features.GetPortionsCount());

    TSaverContext saverContext(StoragesManager->GetOperator(tierName.value_or(IStoragesManager::DefaultStorageId)), StoragesManager);
    return std::make_shared<NCompaction::TGeneralCompactColumnEngineChanges>(limits.GetSplitSettings(), granule, portions, saverContext);
}

void TIntervalsOptimizerPlanner::RemovePortion(const std::shared_ptr<TPortionInfo>& info) {
    AFL_TRACE(NKikimrServices::TX_COLUMNSHARD)("event", "remove_portion")("portion_id", info->GetPortion());
    auto itStart = Positions.find(info->IndexKeyStart());
    auto itFinish = Positions.find(info->IndexKeyEnd());
    Y_ABORT_UNLESS(itStart != Positions.end());
    Y_ABORT_UNLESS(itFinish != Positions.end());
    if (itStart == itFinish) {
        RemoveRanged(itStart->second);
        itStart->second.RemoveSummary(info);
        AddRanged(itStart->second);
        if (itStart->second.RemoveStart(info) || itStart->second.RemoveFinish(info)) {
            RemoveRanged(itStart->second);
            Positions.erase(itStart);
        }
    } else {
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
    AFL_VERIFY(RangedSegments.empty() == Positions.empty())("rs_size", RangedSegments.size())("p_size", Positions.size());
}

void TIntervalsOptimizerPlanner::AddPortion(const std::shared_ptr<TPortionInfo>& info) {
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
        Y_ABORT_UNLESS(itEnd != Positions.begin());
        auto itEndCopy = itEnd;
        --itEndCopy;
        itEnd->second.CopyFrom(itEndCopy->second);
        AddRanged(itEnd->second);
        itStart = Positions.find(info->IndexKeyStart());
    }
    Y_ABORT_UNLESS(itStart != Positions.end());
    Y_ABORT_UNLESS(itEnd != Positions.end());
    itStart->second.AddStart(info);
    itEnd->second.AddFinish(info);
    if (itStart != itEnd) {
        for (auto it = itStart; it != itEnd; ++it) {
            RemoveRanged(it->second);
            it->second.AddSummary(info);
            AFL_VERIFY(!!it->second.GetFeatures());
            AddRanged(it->second);
        }
    } else {
        RemoveRanged(itStart->second);
        itStart->second.AddSummary(info);
        AddRanged(itStart->second);
    }
    AFL_VERIFY(RangedSegments.empty() == Positions.empty())("rs_size", RangedSegments.size())("p_size", Positions.size());
}

void TIntervalsOptimizerPlanner::DoModifyPortions(const THashMap<ui64, std::shared_ptr<TPortionInfo>>& add, const THashMap<ui64, std::shared_ptr<TPortionInfo>>& remove) {
    for (auto&& [_, i] : remove) {
        SizeProblemBlobs.RemovePortion(i);
        RemovePortion(i);
    }
    for (auto&& [_, i] : add) {
        SizeProblemBlobs.AddPortion(i);
        AddPortion(i);
    }
}

void TIntervalsOptimizerPlanner::RemoveRanged(const TBorderPositions& data) {
    if (!!data.GetFeatures()) {
        Counters->OnRemoveIntervalsCount(data.GetFeatures().GetPortionsCount(), data.GetFeatures().GetPortionsRawWeight(), data.GetFeatures().GetPortionsWeight());
        auto itFeatures = RangedSegments.find(data.GetFeatures());
        Y_ABORT_UNLESS(itFeatures->second.erase(&data));
        if (itFeatures->second.empty()) {
            RangedSegments.erase(itFeatures);
        }
    }
}

void TIntervalsOptimizerPlanner::AddRanged(const TBorderPositions& data) {
    if (!!data.GetFeatures()) {
        Counters->OnAddIntervalsCount(data.GetFeatures().GetPortionsCount(), data.GetFeatures().GetPortionsRawWeight(), data.GetFeatures().GetPortionsWeight());
        Y_ABORT_UNLESS(RangedSegments[data.GetFeatures()].emplace(&data).second);
    }
}

TIntervalsOptimizerPlanner::TIntervalsOptimizerPlanner(const ui64 pathId, const std::shared_ptr<IStoragesManager>& storagesManager)
    : TBase(pathId)
    , StoragesManager(storagesManager)
    , Counters(std::make_shared<TCounters>())
    , SizeProblemBlobs(Counters, storagesManager)
{
}

TOptimizationPriority TIntervalsOptimizerPlanner::DoGetUsefulMetric() const {
    auto res = SizeProblemBlobs.GetWeight();
    if (!!res) {
        AFL_VERIFY(RangedSegments.size())("positions", Positions.size())("sizes", SizeProblemBlobs.DebugString());
        return *res;
    }
    if (RangedSegments.empty()) {
        return TOptimizationPriority::Zero();
    }
    auto& topSegment = **RangedSegments.rbegin()->second.begin();
    auto& topFeaturesTask = topSegment.GetFeatures();
    return TOptimizationPriority::Optimization(topFeaturesTask.GetUsefulMetric());
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
