#pragma once

#include <ydb/core/tx/columnshard/engines/storage/optimizer/abstract/optimizer.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/lbuckets/planner/optimizer.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/tiling/counters.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/tiling/tiling_pp/levels.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/tiling/tiling_pp/settings.h>

#include <algorithm>

namespace NKikimr::NOlap::NStorageOptimizer::NTiling {

template <std::totally_ordered TKey, typename TPortion>
struct Tiling: ICompactionUnit<TKey, TPortion> {
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
        , LastLevel(Settings.LastLevelSettings, counters)
    {
        for (ui64 i = 2; i < Settings.MiddleLevelCount; ++i) {
            MiddleLevels.emplace(i, MiddleLevel<TKey, TPortion>(Settings.MiddleLevelSettings, i, counters));
        }
    }

    TilingSettings Settings;
    Accumulator<TKey, TPortion> Accumulator;
    LastLevel<TKey, TPortion> LastLevel;
    THashMap<ui64, MiddleLevel<TKey, TPortion>> MiddleLevels;

    struct TPortionPlacement {
        ui8 Level = 0;
        ui64 Width = 0;
    };

    THashMap<ui64, TPortionPlacement> InternalLevel;
    THashMap<ui64, typename TPortion::TPtr> PortionRegistry;
    THashMap<ui64, TInstant> InsertTimeByPortionId;
    TSet<std::pair<TInstant, ui64>> PortionsByTime;
    bool FirstLoad = true;

    enum class EState {
        REGULAR,
        COMPATIBILITY,
        BORED,
    };

    EState State = EState::REGULAR;
    TOptimizationPriority OverloadPriority = TOptimizationPriority::Critical(0);

    void InitialAddPortions(const std::vector<typename TPortion::TPtr>& add) {
        auto comparator = TPortionByIndexKeyEndComparator<TKey, TPortion>();

        auto sortedPortions = add;
        Sort(sortedPortions, comparator);

        std::vector<typename TPortion::TPtr> toLastLevel;
        std::vector<typename TPortion::TPtr> toAccumulator;
        std::vector<typename TPortion::TPtr> toMiddleLevels;
        std::optional<TKey> lastKey;

        State = EState::COMPATIBILITY;

        for (auto portion : sortedPortions) {
            if (portion->GetTotalBlobBytes() < Settings.AccumulatorPortionSizeLimit) {
                toAccumulator.push_back(portion);
                continue;
            }
            if (lastKey && *lastKey >= portion->IndexKeyStart()) {
                toMiddleLevels.push_back(portion);
                continue;
            }
            toLastLevel.push_back(portion);
            lastKey = portion->IndexKeyEnd();
        }

        for (auto& portion : toLastLevel) {
            this->AddPortion(portion);
        }

        for (auto& portion : toMiddleLevels) {
            this->AddPortion(portion);
        }

        for (auto& portion : toAccumulator) {
            this->AddPortion(portion);
        }

        const auto usefulMetric = DoGetUsefulMetric();
        if (usefulMetric.IsCritical() && Settings.EnableCompatibilityMode) {
            State = EState::COMPATIBILITY;
            OverloadPriority = usefulMetric.IncPercent(10);
        } else {
            State = EState::REGULAR;
        }
    }

    void ModifyPortions(const std::vector<typename TPortion::TPtr>& add, const std::vector<typename TPortion::TConstPtr>& remove) {
        for (const auto& p : remove) {
            this->RemovePortion(p);
        }

        if (FirstLoad) {
            FirstLoad = false;
            InitialAddPortions(add);
        } else {
            for (const auto& p : add) {
                this->AddPortion(p);
            }
        }
    }

    void DoActualize(const TInstant currentInstant) override {
        Accumulator.DoActualize(currentInstant);
        LastLevel.DoActualize(currentInstant);
        for (auto& middleLevel : MiddleLevels) {
            middleLevel.second.DoActualize(currentInstant);
        }

        if (State == EState::COMPATIBILITY) {
            const auto useful = DoGetUsefulMetric();
            const auto desiredCeiling = useful.IncPercent(10);
            if (desiredCeiling < OverloadPriority) {
                OverloadPriority = desiredCeiling;
            }
            const bool exiting = !OverloadPriority.IsCritical();
            if (exiting) {
                State = EState::REGULAR;
                OverloadPriority = TOptimizationPriority::Critical(0);
            }
        }
        PromoteExpiredPortions(currentInstant);
    }

    void DoAddPortion(typename TPortion::TPtr p) override {
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
        AFL_VERIFY(!InternalLevel.contains(portionId))("portion_id", portionId)("existing_level", (ui32)InternalLevel[portionId].Level)(
            "existing_width", InternalLevel[portionId].Width)("blob_bytes", p->GetTotalBlobBytes())("reason", "tiling++_portion_already_exists");

        Place(p, TInstant::Now());
    }

    void Place(typename TPortion::TPtr p, TInstant now, bool accumulatorAllowed = true, std::optional<ui8> forcedLevel = std::nullopt) {
        const ui64 portionId = p->GetPortionId();
        PortionRegistry[portionId] = p;
        ui8 level = 0;
        ui64 measure = 0;

        if (accumulatorAllowed && p->GetTotalBlobBytes() < Settings.AccumulatorPortionSizeLimit) {
            level = 0;
        } else {
            measure = LastLevel.Measure(p);
            if (p->GetCompactionLevel() == 1 && State != EState::COMPATIBILITY) {
                level = 1;
            } else if (forcedLevel.has_value()) {
                level = *forcedLevel;
            } else {
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
        }

        if (level == 0) {
            Accumulator.AddPortion(p);
            InternalLevel[portionId] = { .Level = 0, .Width = 0 };
        } else if (level == 1) {
            LastLevel.AddPortion(p);
            InternalLevel[portionId] = { .Level = 1, .Width = measure };
        } else {
            MiddleLevels.at(level).RegisterRoutingWidth(portionId, measure);
            MiddleLevels.at(level).AddPortion(p);
            InternalLevel[portionId] = { .Level = level, .Width = measure };
        }

        if (level != 1 && Settings.AgingSettings.Enabled) {
            InsertTimeByPortionId[portionId] = now;
            PortionsByTime.insert({ now, portionId });
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
        auto lit = InternalLevel.find(portionId);
        if (lit == InternalLevel.end()) {
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
        InternalLevel.erase(lit);

        auto tit = InsertTimeByPortionId.find(portionId);
        if (tit != InsertTimeByPortionId.end()) {
            PortionsByTime.erase({ tit->second, portionId });
            InsertTimeByPortionId.erase(tit);
        }
        PortionRegistry.erase(portionId);
    }

    void PromoteExpiredPortions(const TInstant currentInstant) {
        if (!Settings.AgingSettings.Enabled || State == EState::COMPATIBILITY) {
            return;
        }

        const TDuration wait = Settings.AgingSettings.PromoteTime;
        const ui64 maxCount = Settings.AgingSettings.MaxPortionPromotion;

        std::vector<typename TPortion::TPtr> expired;
        expired.reserve(std::min<size_t>(maxCount, PortionsByTime.size()));
        for (auto it = PortionsByTime.begin(); expired.size() < maxCount && it != PortionsByTime.end(); ++it) {
            auto pit = PortionRegistry.find(it->second);
            if (pit != PortionRegistry.end()) {
                auto lit = InternalLevel.find(it->second);
                if (lit != InternalLevel.end()) {
                    if ((State == EState::BORED && lit->second.Level != 0) || it->first + wait <= currentInstant) {
                        expired.push_back(pit->second);
                    }
                }
            }
            if (State != EState::BORED && it->first + wait > currentInstant) {
                break;
            }
        }

        for (const auto& p : expired) {
            const ui64 portionId = p->GetPortionId();
            auto lit = InternalLevel.find(portionId);
            if (lit == InternalLevel.end()) {
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
            Place(p, currentInstant, /*accumulatorAllowed=*/false, nextLevel);
        }

        ConsiderState();
    }

    std::optional<CompactionTask<TKey, TPortion>> DoGetNextOptimizationTask(
        TFunctionRef<bool(typename TPortion::TConstPtr)> isLocked) const override {
        auto result = Accumulator.DoGetNextOptimizationTask(isLocked);
        const auto consider = [&result](std::optional<CompactionTask<TKey, TPortion>>&& candidate) {
            if (candidate && (!result || result->Priority < candidate->Priority)) {
                result = std::move(candidate);
            }
        };
        consider(LastLevel.DoGetNextOptimizationTask(isLocked));
        for (const auto& [_, middleLevel] : MiddleLevels) {
            consider(middleLevel.DoGetNextOptimizationTask(isLocked));
        }

        return result;
    }

    void ConsiderState() {
        auto priority = DoGetUsefulMetric();
        if (priority.IsZeroLevel() && State == EState::REGULAR) {
            State = EState::BORED;
        } else if (!priority.IsZeroLevel() && State == EState::BORED) {
            State = EState::REGULAR;
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
        return { middleLevelsPriority, maxMiddleLevelKey };
    }

    bool IsOverloaded() const {
        return OverloadPriority < DoGetUsefulMetric();
    }

    // True when every portion has settled into the regular last level (LastLevel.Portions): nothing
    // waiting in the accumulator, no intersecting candidates in the last level, and no middle levels.
    bool HasNoIntersections() const {
        if (!Accumulator.Portions.empty()) {
            return false;
        }
        if (!LastLevel.CandidateIds.empty()) {
            return false;
        }
        for (const auto& [_, middleLevel] : MiddleLevels) {
            if (!middleLevel.PortionById.empty()) {
                return false;
            }
        }
        return true;
    }
};

}   // namespace NKikimr::NOlap::NStorageOptimizer::NTiling
