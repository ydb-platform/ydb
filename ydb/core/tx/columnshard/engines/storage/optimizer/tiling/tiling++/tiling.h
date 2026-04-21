#pragma once

#include <algorithm>

#include <ydb/core/tx/columnshard/engines/storage/optimizer/lbuckets/planner/optimizer.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/tiling/counters.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/tiling/tiling++/levels.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/abstract/optimizer.h>

namespace NKikimr::NOlap::NStorageOptimizer::NTiling {

/// Owns a single TCounters instance for one tiling compaction graph. Declared as the first base of Tiling so
/// ICompactionUnit can bind to tiling-level counters while sub-levels share the same TCounters via constructor args.
struct TTilingCompactionCountersHolder {
    TCounters Counters;
};

template <std::totally_ordered TKey, typename TPortion>
    requires CPortionInfoSlice<TKey, TPortion>
struct Tiling : private TTilingCompactionCountersHolder, ICompactionUnit<TKey, TPortion> {
    using TBase = ICompactionUnit<TKey, TPortion>;
    using TLevelCounters = typename TBase::TLevelCounters;

    using TAccumulatorSettings = typename Accumulator<TKey, TPortion>::AccumulatorSettings;
    using TLastLevelSettings = typename LastLevel<TKey, TPortion>::LastLevelSettings;
    using TMiddleLevelSettings = typename MiddleLevel<TKey, TPortion>::MiddleLevelSettings;

    struct TilingSettings {
        TAccumulatorSettings AccumulatorSettings;
        TLastLevelSettings LastLevelSettings;
        TMiddleLevelSettings MiddleLevelSettings;
        ui64 AccumulatorPortionSizeLimit = 512ULL * 1024;
        ui8 K = 10;
        /// Exclusive upper bound on middle-level index (allowed middle indices: 2 .. MiddleLevelCount - 1).
        ui64 MiddleLevelCount = TILING_LAYERS_COUNT;
    };

    Tiling(TilingSettings settings)
        : TTilingCompactionCountersHolder{}
        , TBase(Counters.GetTilingCounters())
        , Settings(std::move(settings))
        , Accumulator(Settings.AccumulatorSettings, Counters)
        , LastLevel(Settings.LastLevelSettings, Counters) {
        for (ui64 i = 2; i < Settings.MiddleLevelCount; ++i) {
            MiddleLevels.emplace(i, MiddleLevel<TKey, TPortion>(Settings.MiddleLevelSettings, i, Counters));
        }
    }

    /// Sub-levels update their own counters; skip ICompactionUnit's global counter to avoid double counting.
    void AddPortion(typename TPortion::TConstPtr p) override {
        DoAddPortion(p);
    }
    void RemovePortion(typename TPortion::TConstPtr p) override {
        DoRemovePortion(p);
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
    // NLBuckets::TSimplePortionsGroupInfo PortionsInfo;

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
            case NPortion::INACTIVE:
                AFL_VERIFY(false)("reason", "Unexpected portion type");
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

        if (p->GetTotalBlobBytes() < Settings.AccumulatorPortionSizeLimit) {
            Accumulator.AddPortion(p);
            InternalLevelForDebug[portionId] = {.Level = 0, .Width = 0};
        } else {
            const ui64 measure = LastLevel.Measure(p);
            ui8 level = 1;
            if (measure > 0) {
                ui64 threshold = 1;
                while (threshold * Settings.K <= measure) {
                    threshold *= Settings.K;
                    ++level;
                }
            }
            if (level <= 1) {
                LastLevel.AddPortion(p);
                InternalLevelForDebug[portionId] = {.Level = 1, .Width = measure};
            } else {
                level = std::min(level, static_cast<ui8>(Settings.MiddleLevelCount - 1));
                MiddleLevels.at(level).RegisterRoutingWidth(portionId, measure);
                MiddleLevels.at(level).AddPortion(p);
                InternalLevelForDebug[portionId] = {.Level = level, .Width = measure};
            }
        }
    }

    void DoRemovePortion(typename TPortion::TConstPtr p) override {
        switch (p->GetProduced()) {
            case NPortion::EVICTED:
            case NPortion::INACTIVE:
            case NPortion::UNSPECIFIED:
            case NPortion::COMPACTED:
                AFL_VERIFY(false)("reason", "Remove not tracked portions");
                break;
            default:
                break;
        }

        auto lit = InternalLevelForDebug.find(p->GetPortionId());
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
                mit->second.UnregisterRoutingWidth(p->GetPortionId());
                mit->second.RemovePortion(p);
            } else {
                AFL_VERIFY(false)("reason", "Bad level info");
            }
        }
        InternalLevelForDebug.erase(lit);
    }

    std::vector<CompactionTask<TKey, TPortion>> DoGetOptimizationTasks(
        TFunctionRef<bool(typename TPortion::TConstPtr)> isLocked) const override {
        const auto accumulatorPriority = Accumulator.DoGetUsefulMetric();
        const auto lastLevelPriority = LastLevel.DoGetUsefulMetric();
        const auto [middleLevelsPriority, maxMiddleLevel] = GetMiddleUsefulMetric();

        if (lastLevelPriority < accumulatorPriority && middleLevelsPriority < accumulatorPriority) {
            return Accumulator.GetOptimizationTasks(isLocked);
        } else if (lastLevelPriority < middleLevelsPriority) {
            return MiddleLevels.at(maxMiddleLevel).GetOptimizationTasks(isLocked);
        } else {
            return LastLevel.GetOptimizationTasks(isLocked);
        }
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
