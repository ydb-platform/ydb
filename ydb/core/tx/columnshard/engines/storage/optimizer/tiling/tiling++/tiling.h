#pragma once

#include <algorithm>

#include <ydb/core/tx/columnshard/engines/storage/optimizer/lbuckets/planner/optimizer.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/tiling/counters.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/tiling/tiling++/levels.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/tiling/tiling++/settings.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/abstract/optimizer.h>

namespace NKikimr::NOlap::NStorageOptimizer::NTiling {

template <std::totally_ordered TKey, typename TPortion>
    requires CPortionInfoSlice<TKey, TPortion>
struct Tiling : ICompactionUnit<TKey, TPortion> {
    using TBase = ICompactionUnit<TKey, TPortion>;
    using TLevelCounters = typename TBase::TLevelCounters;

    using TAccumulatorSettings = NTiling::TAccumulatorSettings;
    using TLastLevelSettings = NTiling::TLastLevelSettings;
    using TMiddleLevelSettings = NTiling::TMiddleLevelSettings;
    using TilingSettings = TTilingSettings;

    Tiling(TilingSettings settings, const TCounters& counters)
        : TBase(counters.GetTilingCounters())
        , Settings(std::move(settings))
        , Accumulator(Settings.AccumulatorSettings, counters)
        , LastLevel(Settings.LastLevelSettings, counters) {
        for (ui64 i = 2; i < Settings.MiddleLevelCount; ++i) {
            MiddleLevels.emplace(i, MiddleLevel<TKey, TPortion>(Settings.MiddleLevelSettings, i, counters));
        }
    }

    TilingSettings Settings;
    Accumulator<TKey, TPortion> Accumulator;
    LastLevel<TKey, TPortion> LastLevel;
    THashMap<ui64, MiddleLevel<TKey, TPortion>> MiddleLevels;

    TIntersectionTree<TKey, ui64> Intersections;
    THashMap<ui64, typename TPortion::TPtr> PortionById;
    /// DEBUG routing snapshot: must match physical home (Accumulator / LastLevel / MiddleLevel) for counter Add/Sub.
    struct TPortionPlacementForDebug {
        ui8 Level = 0;
        ui64 Width = 0;
    };
    THashMap<ui64, TPortionPlacementForDebug> InternalLevelForDebug;
    THashMap<ui64, typename TPortion::TConstPtr> PortionRegistry;
    THashMap<ui64, TInstant> InsertTimeByPortionId;
    TSet<std::pair<TInstant, ui64>> PortionsByTime;
    mutable bool LastTaskWasCritical = false;

    void DoActualize() override {
        Accumulator.DoActualize();
        LastLevel.DoActualize();
        for (auto& middleLevel : MiddleLevels) {
            middleLevel.second.DoActualize();
        }
    }

    void DoAddPortion(typename TPortion::TConstPtr p) override {
        switch (p->GetProduced()) {
            case NPortion::EVICTED:
                // Evicted portions (e.g. tier deletion) are not tracked by the optimizer.
                return;
            case NPortion::INACTIVE:
                AFL_VERIFY(false)("reason", "Unexpected portion type")("type", "INACTIVE");
                break;
            case NPortion::UNSPECIFIED:
            case NPortion::COMPACTED:
                AFL_VERIFY(false)("reason", "Legacy portions");
                break;
            default:
                break;
        }

        const ui64 portionId = p->GetPortionId();
        const auto existingIt = InternalLevelForDebug.find(portionId);
        if (existingIt != InternalLevelForDebug.end()) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)(
                "event", "tiling++_portion_already_exists")(
                "portion_id", portionId)(
                "existing_level", (ui32)existingIt->second.Level)(
                "existing_width", existingIt->second.Width)(
                "blob_bytes", p->GetTotalBlobBytes())(
                "action", "removing_before_readd");
            DoRemovePortion(p);
        }

        Place(p);
    }

    void Place(typename TPortion::TConstPtr p, bool accumulatorAllowed = true, std::optional<ui8> forcedLevel = std::nullopt) {
        const ui64 portionId = p->GetPortionId();
        PortionRegistry[portionId] = p;
        ui8 level = 0;
        ui64 measure = 0;
        if (accumulatorAllowed && p->GetTotalBlobBytes() < Settings.AccumulatorPortionSizeLimit) {
            level = 0;
        } else if (forcedLevel.has_value()) {
            level = *forcedLevel;
            if (level >= 2) {
                measure = LastLevel.Measure(p);
            }
        } else {
            measure = LastLevel.Measure(p);
            ui8 measuredLevel = 1;
            if (measure > 0) {
                ui64 threshold = 1;
                while (threshold * Settings.K <= measure) {
                    threshold *= Settings.K;
                    ++measuredLevel;
                }
            }
            if (measuredLevel <= 1) {
                level = 1;
            } else {
                level = std::min(measuredLevel, static_cast<ui8>(Settings.MiddleLevelCount - 1));
            }
        }

        if (level == 0) {
            Accumulator.AddPortion(p);
            InternalLevelForDebug[portionId] = {.Level = 0, .Width = 0};
        } else if (level == 1) {
            LastLevel.AddPortion(p);
            InternalLevelForDebug[portionId] = {.Level = 1, .Width = measure};
        } else {
            MiddleLevels.at(level).RegisterRoutingWidth(portionId, measure);
            MiddleLevels.at(level).AddPortion(p);
            InternalLevelForDebug[portionId] = {.Level = level, .Width = measure};
        }

        if (level != 1 && Settings.AgingSettings.Enabled) {
            const TInstant now = TInstant::Now();
            InsertTimeByPortionId[portionId] = now;
            PortionsByTime.insert({now, portionId});
        }
    }

    void DoRemovePortion(typename TPortion::TConstPtr p) override {
        switch (p->GetProduced()) {
            case NPortion::EVICTED:
                // Evicted portions (e.g. tier deletion) are not tracked by the optimizer.
                return;
            case NPortion::INACTIVE:
            case NPortion::UNSPECIFIED:
            case NPortion::COMPACTED:
                AFL_VERIFY(false)("reason", "Remove not tracked portions");
                break;
            default:
                break;
        }

        const ui64 portionId = p->GetPortionId();
        auto lit = InternalLevelForDebug.find(portionId);
        if (lit == InternalLevelForDebug.end()) {
            AFL_VERIFY(false)("reason", "Remove unknown portion");
            return;
        }
        if (lit->second.Level == 0) {
            Accumulator.RemovePortion(p);
        } else if (lit->second.Level == 1) {
            LastLevel.RemovePortion(p);
        } else {
            auto mit = MiddleLevels.find(lit->second.Level);
            if (mit != MiddleLevels.end()) {
                mit->second.UnregisterRoutingWidth(portionId);
                mit->second.RemovePortion(p);
            } else {
                AFL_VERIFY(false)("reason", "Bad level info");
            }
        }
        InternalLevelForDebug.erase(lit);

        auto tit = InsertTimeByPortionId.find(portionId);
        if (tit != InsertTimeByPortionId.end()) {
            PortionsByTime.erase({tit->second, portionId});
            InsertTimeByPortionId.erase(tit);
        }
        PortionRegistry.erase(portionId);
    }

    void PromoteExpiredPortions(const TInstant currentInstant) {
        if (!Settings.AgingSettings.Enabled) {
            return;
        }
        // NOTE: do not bail out on LastTaskWasCritical — aging must continue
        // to make progress even while compaction is busy on overloaded layers,
        // otherwise a single critical task permanently disables aging.
        const TDuration wait = Settings.AgingSettings.PromoteTime;
        const ui64 maxCount = Settings.AgingSettings.MaxPortionPromotion;

        std::vector<typename TPortion::TConstPtr> expired;
        expired.reserve(std::min<size_t>(maxCount, PortionsByTime.size()));
        for (auto it = PortionsByTime.begin();
             expired.size() < maxCount && it != PortionsByTime.end() && it->first + wait <= currentInstant;
             ++it) {
            auto pit = PortionRegistry.find(it->second);
            if (pit != PortionRegistry.end()) {
                expired.push_back(pit->second);
            }
        }
        if (expired.empty()) {
            return;
        }
        for (const auto& p : expired) {
            const ui64 portionId = p->GetPortionId();
            auto lit = InternalLevelForDebug.find(portionId);
            if (lit == InternalLevelForDebug.end()) {
                continue;
            }
            const ui8 currentLevel = lit->second.Level;
            std::optional<ui8> nextLevel;
            if (currentLevel == 0) {
                // Promote out of accumulator: acc no longer allowed, use natural routing.
                nextLevel = std::nullopt;
            } else if (currentLevel >= 2) {
                // Middle level promotion: force one level lower; acc impossible.
                nextLevel = static_cast<ui8>(currentLevel - 1);
            } else {
                AFL_VERIFY(false)("reason", "last_level_portion_should_not_have_timer")("portion_id", portionId);
                continue;
            }
            DoRemovePortion(p);
            Place(p, /*accumulatorAllowed=*/false, nextLevel);
        }
    }

    std::vector<CompactionTask<TKey, TPortion>> DoGetOptimizationTasks(
        TFunctionRef<bool(typename TPortion::TConstPtr)> isLocked) const override {
        const auto accumulatorPriority = Accumulator.DoGetUsefulMetric();
        const auto lastLevelPriority = LastLevel.DoGetUsefulMetric();
        const auto [middleLevelsPriority, maxMiddleLevel] = GetMiddleUsefulMetric();

        TOptimizationPriority chosenPriority = TOptimizationPriority::Zero();
        std::vector<CompactionTask<TKey, TPortion>> tasks;
        if (lastLevelPriority < accumulatorPriority && middleLevelsPriority < accumulatorPriority) {
            chosenPriority = accumulatorPriority;
            tasks = Accumulator.GetOptimizationTasks(isLocked);
        } else if (lastLevelPriority < middleLevelsPriority) {
            chosenPriority = middleLevelsPriority;
            tasks = MiddleLevels.at(maxMiddleLevel).GetOptimizationTasks(isLocked);
        } else {
            chosenPriority = lastLevelPriority;
            tasks = LastLevel.GetOptimizationTasks(isLocked);
        }
        LastTaskWasCritical = !tasks.empty() && chosenPriority.IsCritical();
        return tasks;
    }

    TOptimizationPriority DoGetUsefulMetric() const override {
        return std::max(Accumulator.DoGetUsefulMetric(), std::max(LastLevel.DoGetUsefulMetric(), GetMiddleUsefulMetric().first));
    }

    std::pair<TOptimizationPriority, ui64> GetMiddleUsefulMetric() const {
        auto middleLevelsPriority = TOptimizationPriority::Zero();
        ui64 maxMiddleLevelKey = 0;
        for (const auto& [level, middleLevel] : MiddleLevels) {
            auto priority = middleLevel.DoGetUsefulMetric();
            if (middleLevelsPriority < priority) {
                middleLevelsPriority = priority;
                maxMiddleLevelKey = level;
            }
        }
        return {middleLevelsPriority, maxMiddleLevelKey};
    }
};

} // namespace NKikimr::NOlap::NStorageOptimizer::NTiling
