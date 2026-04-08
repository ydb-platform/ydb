#include <ydb/core/tx/columnshard/data_locks/manager/manager.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/abstract/optimizer.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/tiling/counters.h>
#include <ydb/library/intersection_tree/intersection_tree.h>
namespace NKikimr::NOlap::NStorageOptimizer::NTiling {

struct LastLevel {
    struct LastLevelSettings {
        struct Limit {
            ui64 Portions;
            ui64 Bytes;
        };

        Limit Compaction {1'000, 64 * 1024 * 1024};
        ui64 CandidatePortionsOverload = 10;
    };

    LastLevelSettings Settings;
    const TLevelCounters& Counters;
    LastLevel(LastLevelSettings settings, const TLevelCounters& counters)
        : Settings(settings)
        , Counters(counters) {}

    struct TPortionByIndexKeyEndComparator {
        using is_transparent = void;  // Enable heterogeneous lookup

        bool operator()(const TPortionInfo::TPtr& left, const TPortionInfo::TPtr& right) const {
            return left->IndexKeyEnd() < right->IndexKeyEnd();
        }

        bool operator()(const TPortionInfo::TPtr& left, const NArrow::TSimpleRow& right) const {
            return left->IndexKeyEnd() < right;
        }

        bool operator()(const NArrow::TSimpleRow& left, const TPortionInfo::TPtr& right) const {
            return left < right->IndexKeyEnd();
        }
    };

    using PrtionsEndSorted = TSet<TPortionInfo::TPtr, TPortionByIndexKeyEndComparator>;
    PrtionsEndSorted Portions;
    PrtionsEndSorted Candidates;

    TOptimizationPriority DoGetUsefulMetric() const {
        auto priority = TOptimizationPriority::Normalize(1, Settings.CandidatePortionsOverload, Candidates.size());
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)(
            "event", "last_level_get_useful_metric")(
            "overloaded", priority.IsCritical())(
            "candidates_count", Candidates.size())(
            "candidates_overload_limit", Settings.CandidatePortionsOverload)(
            "portions_count", Portions.size())(
            "priority_level", priority.GetLevel())(
            "priority_weight", priority.GetInternalLevelWeight());
        return priority;
    }

    void Add(TPortionInfo::TPtr p) {
        Counters.Portions->AddPortion(p);
        const ui64 measure = Measure(p);
        if (measure == 0) {
            Portions.insert(p);
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)(
                "event", "last_level_add_to_portions")(
                "portion_id", p->GetPortionId())(
                "portions_count", Portions.size())(
                "candidates_count", Candidates.size());
        }
        else {
            Candidates.insert(p);
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)(
                "event", "last_level_add_to_candidates")(
                "portion_id", p->GetPortionId())(
                "measure", measure)(
                "portions_count", Portions.size())(
                "candidates_count", Candidates.size())(
                "overloaded", Candidates.size() >= Settings.CandidatePortionsOverload);
        }
        Counters.Portions->SetHeight(Candidates.size());
    }

    void Remove(TPortionInfo::TPtr p) {
        Counters.Portions->RemovePortion(p);
        if (Portions.contains(p)) {
            Portions.erase(p);
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)(
                "event", "last_level_remove_from_portions")(
                "portion_id", p->GetPortionId())(
                "portions_count", Portions.size())(
                "candidates_count", Candidates.size());
        }
        else if (Candidates.contains(p)) {
            Candidates.erase(p);
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)(
                "event", "last_level_remove_from_candidates")(
                "portion_id", p->GetPortionId())(
                "portions_count", Portions.size())(
                "candidates_count", Candidates.size())(
                "overloaded", Candidates.size() >= Settings.CandidatePortionsOverload);
        }
        else {
            AFL_VERIFY(false);
        }
        Counters.Portions->SetHeight(Candidates.size());
    }

    std::pair<PrtionsEndSorted::iterator, PrtionsEndSorted::iterator> Borders(TPortionInfo::TPtr p) const {
        auto begin = Portions.lower_bound(p->IndexKeyStart());
        auto end = Portions.upper_bound(p->IndexKeyEnd());
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "last_level_Borders")("portion_id", p->GetPortionId())
        ("begin", begin == Portions.end() ? -1 : (i64)begin->get()->GetPortionId())
        ("end", end == Portions.end() ? -1 : (i64)end->get()->GetPortionId());
        if (end != Portions.end() && (*end)->IndexKeyStart() <= p->IndexKeyEnd()) {
            ++end;
        }
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "last_level_Borders")("portion_id", p->GetPortionId())
        ("begin", begin == Portions.end() ? -1 : (i64)begin->get()->GetPortionId())
        ("end", end == Portions.end() ? -1 : (i64)end->get()->GetPortionId());
        return std::make_pair(begin, end);
    }

    ui64 Measure(TPortionInfo::TPtr p) const {
        auto [start, end] = Borders(p);
        auto dist = std::distance(start, end);
                    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "Measure");
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "last_level_measure")("portion_id", p->GetPortionId())("size", dist);
        return dist;
    }

    std::vector<TPortionInfo::TPtr> GetOptimizationTasks(const std::shared_ptr<NDataLocks::TManager>& locksManager) const {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "last_level_GetOptimizationTasks");
        for (auto candidate : Candidates) {
            std::vector<TPortionInfo::TPtr> result;
            result.push_back(candidate);
            ui64 currentBlobBytes = candidate->GetTotalBlobBytes();
            auto [begin, end] = Borders(candidate);

            bool success = true;
            for (auto it = begin; it != end; ++it) {
                if (locksManager->IsLocked(*it, NDataLocks::ELockCategory::Compaction)) {
                    success = false;
                    break;
                }
                result.push_back(*it);
                currentBlobBytes += (*it)->GetTotalBlobBytes();
                if (currentBlobBytes > Settings.Compaction.Bytes || result.size() > Settings.Compaction.Portions) {
                    break;
                }
            }
            if (success) {
                if (result.size() == 1) {
                    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "last_level_one_result");
                    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "last_level_one_result")("portion_id", candidate->GetPortionId());
                    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "last_level_one_result")("begin", begin == Portions.end());
                    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "last_level_one_result")("end", end == Portions.end());
                    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "last_level_one_result")("eq", begin == end);
                    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "last_level_one_result")("portion_id", candidate->GetPortionId())
                    ("begin", begin == Portions.end() ? -1 : (i64)begin->get()->GetPortionId())
                    ("end", end == Portions.end() ? -1 : (i64)end->get()->GetPortionId());
                    // continue;
                }
                return result;
            }
            else {
                AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "last_level_skip");
            }
        }
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "last_level_no_result");
        AFL_VERIFY(!DoGetUsefulMetric().IsCritical());
        return {};
    }

};

struct Accumulator {
    struct AccumulatorSettings {
        struct Limit {
            ui64 Portions;
            ui64 Bytes;
        };

        Limit Compaction {1'000, 64 * 1024 * 1024};
        Limit Trigger {1'000, 2 * 1024 * 1024};
        Limit Overload {10'000, 256 * 1024 * 1024};
    };

    AccumulatorSettings Settings;
    const TLevelCounters& Counters;
    Accumulator(AccumulatorSettings settings, const TLevelCounters& counters)
        : Settings(settings)
        , Counters(counters) {}

    TSet<TPortionInfo::TPtr> Portions;
    ui64 TotalBlobBytes = 0;

    TOptimizationPriority DoGetUsefulMetric() const {
        auto portionPriority = TOptimizationPriority::Normalize(Settings.Trigger.Portions, Settings.Overload.Portions, Portions.size());
        auto bytestPriority = TOptimizationPriority::Normalize(Settings.Trigger.Bytes, Settings.Overload.Bytes, TotalBlobBytes);
        auto priority = std::max(portionPriority, bytestPriority);
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)(
            "event", "accumulator_get_useful_metric")(
            "overloaded", priority.IsCritical())(
            "portions_count", Portions.size())(
            "trigger_portions", Settings.Trigger.Portions)(
            "overload_portions", Settings.Overload.Portions)(
            "total_blob_bytes", TotalBlobBytes)(
            "trigger_bytes", Settings.Trigger.Bytes)(
            "overload_bytes", Settings.Overload.Bytes)(
            "priority_level", priority.GetLevel())(
            "priority_weight", priority.GetInternalLevelWeight());
        return priority;
    }

    void Add(TPortionInfo::TPtr p) {
        Counters.Portions->AddPortion(p);
        Portions.insert(p);
        TotalBlobBytes += p->GetTotalBlobBytes();
        Counters.Portions->SetHeight(Portions.size());
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)(
            "event", "accumulator_add_portion")(
            "portion_id", p->GetPortionId())(
            "portion_blob_bytes", p->GetTotalBlobBytes())(
            "total_blob_bytes", TotalBlobBytes)(
            "portions_count", Portions.size())(
            "overloaded", DoGetUsefulMetric().IsCritical());
    }

    void Remove(TPortionInfo::TPtr p) {
        Counters.Portions->RemovePortion(p);
        Portions.erase(p);
        TotalBlobBytes -= p->GetTotalBlobBytes();
        Counters.Portions->SetHeight(Portions.size());
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)(
            "event", "accumulator_remove_portion")(
            "portion_id", p->GetPortionId())(
            "portion_blob_bytes", p->GetTotalBlobBytes())(
            "total_blob_bytes", TotalBlobBytes)(
            "portions_count", Portions.size())(
            "overloaded", DoGetUsefulMetric().IsCritical());
    }

    std::vector<TPortionInfo::TPtr> GetOptimizationTasks(const std::shared_ptr<NDataLocks::TManager>& locksManager) const {
        if (TotalBlobBytes < Settings.Trigger.Bytes && Portions.size() < Settings.Trigger.Portions) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)(
                "event", "accumulator_get_tasks_skipped_below_trigger")(
                "total_blob_bytes", TotalBlobBytes)(
                "trigger_bytes", Settings.Trigger.Bytes)(
                "portions_count", Portions.size())(
                "trigger_portions", Settings.Trigger.Portions);
            return {};
        }
        std::vector<TPortionInfo::TPtr> result;
        ui64 currentBlobBytes = 0;
        ui64 lockedCount = 0;
        for (auto it : Portions) {
            if (locksManager->IsLocked(*it, NDataLocks::ELockCategory::Compaction)) {
                ++lockedCount;
                AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)(
                    "event", "accumulator_portion_locked")(
                    "portion_id", it->GetPortionId())(
                    "locked_so_far", lockedCount)(
                    "total_portions", Portions.size());
                continue;
            }
            result.push_back(it);
            currentBlobBytes += it->GetTotalBlobBytes();
            if (currentBlobBytes > Settings.Compaction.Bytes || result.size() > Settings.Compaction.Portions) {
                break;
            }
        }
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)(
            "event", "accumulator_get_tasks_result")(
            "result_size", result.size())(
            "locked_count", lockedCount)(
            "total_portions", Portions.size())(
            "total_blob_bytes", TotalBlobBytes)(
            "trigger_bytes", Settings.Trigger.Bytes);
        // If the accumulator is overloaded (above trigger) but ALL portions are locked,
        // it means a compaction is already in progress — that's expected, not a bug.
        // But if there are unlocked portions and we still return < 2, that's a problem.
        AFL_VERIFY(result.size() >= 2 || lockedCount > 0 || Portions.size() < 2)(
            "event", "accumulator_overloaded_but_no_task")(
            "result_size", result.size())(
            "locked_count", lockedCount)(
            "total_portions", Portions.size())(
            "total_blob_bytes", TotalBlobBytes);
        return result;
    }

};

struct MiddleLevels {

    struct MiddleLevel {
        struct MiddleLevelSettings {
            ui64 TriggerHight = 10;
            ui64 OverloadHight = 15;
        };

        MiddleLevelSettings Settings;
        const TLevelCounters& Counters;

        MiddleLevel(MiddleLevelSettings settings, const TLevelCounters& counters)
            : Settings(settings)
            , Counters(counters) {};

        // TIntersectionTree uses THashMap<TValue, TValueNode> internally, so TValue must be
        // hashable via THash<>. std::shared_ptr<TPortionInfo> is not hashable with the Arcadia
        // THash, so we use ui64 (portion ID) as the value type and keep a separate id→ptr map.
        TIntersectionTree<NArrow::TSimpleRow, ui64> Intersections;
        THashMap<ui64, TPortionInfo::TPtr> PortionById;

        TOptimizationPriority DoGetUsefulMetric() const {
            const ui64 maxCount = Intersections.GetMaxCount();
            auto priority = TOptimizationPriority::Normalize(Settings.TriggerHight, Settings.OverloadHight, maxCount);
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)(
                "event", "middle_level_get_useful_metric")(
                "overloaded", priority.IsCritical())(
                "intersection_max_count", maxCount)(
                "trigger_height", Settings.TriggerHight)(
                "overload_height", Settings.OverloadHight)(
                "portions_count", PortionById.size())(
                "priority_level", priority.GetLevel())(
                "priority_weight", priority.GetInternalLevelWeight());
            return priority;
        }

        void Add(const TPortionInfo::TPtr& p) {
            Counters.Portions->AddPortion(p);
            const ui64 id = p->GetPortionId();
            PortionById.emplace(id, p);
            Intersections.Add(id, p->IndexKeyStart(), p->IndexKeyEnd());
            const ui64 maxCount = Intersections.GetMaxCount();
            Counters.Portions->SetHeight(maxCount);
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)(
                "event", "middle_level_add_portion")(
                "portion_id", id)(
                "portions_count", PortionById.size())(
                "intersection_max_count", maxCount)(
                "overloaded", maxCount >= Settings.OverloadHight);
        }

        void Remove(const TPortionInfo::TPtr& p) {
            Counters.Portions->RemovePortion(p);
            const ui64 id = p->GetPortionId();
            Intersections.Remove(id);
            PortionById.erase(id);
            const ui64 maxCount = Intersections.GetMaxCount();
            Counters.Portions->SetHeight(maxCount);
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)(
                "event", "middle_level_remove_portion")(
                "portion_id", id)(
                "portions_count", PortionById.size())(
                "intersection_max_count", maxCount)(
                "overloaded", maxCount >= Settings.OverloadHight);
        }

        std::vector<TPortionInfo::TPtr> GetOptimizationTasks(const std::shared_ptr<NDataLocks::TManager>& locksManager) const {
            std::vector<TPortionInfo::TPtr> portions;
            auto range = Intersections.GetMaxRange();
            range.ForEachValue([&](const ui64 id) {
                auto it = PortionById.find(id);
                if (it == PortionById.end()) {
                    return true;
                }
                const TPortionInfo::TPtr& p = it->second;
                if (!locksManager->IsLocked(*p, NDataLocks::ELockCategory::Compaction)) {
                    portions.push_back(p);
                }
                return true;
            });
            return portions;
        }

    };

    std::vector<MiddleLevel> Levels;
    THashMap<ui64, ui8> InternalLevel;

    // Each middle level i (i >= 2) maps directly to level counter index i.
    // Indices 0 and 1 are reserved for accumulator and last-level respectively.
    MiddleLevels(MiddleLevel::MiddleLevelSettings settings, const TCounters& counters) {
        Levels.reserve(10);
        for (int i = 0; i < 10; ++i) {
            Levels.emplace_back(settings, counters.GetLevelCounters(i));
        }
    }


    void Add(TPortionInfo::TPtr p, ui8 level) {
        Levels.at(level).Add(p);
    }

    void Remove(TPortionInfo::TPtr p, ui8 level) {
        Levels.at(level).Remove(p);
    }

    ui8 GetMaxLevel() const {
        auto maxPriority = TOptimizationPriority::Zero();
        ui8 maxPriorityLevelIdx = 0;
        for (ui8 i = 0; i < (ui8)Levels.size(); ++i) {
            auto levelPriority = Levels[i].DoGetUsefulMetric();
            if (maxPriority < levelPriority) {
                maxPriority = levelPriority;
                maxPriorityLevelIdx = i;
            }
        }
        return maxPriorityLevelIdx;
    }

    TOptimizationPriority DoGetUsefulMetric() const {
        ui8 maxLevel = GetMaxLevel();
        auto maxPriority = Levels.at(maxLevel).DoGetUsefulMetric();
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)(
            "event", "middle_levels_get_useful_metric")(
            "any_overloaded", maxPriority.IsCritical())(
            "max_priority_level_idx", (ui32)maxLevel)(
            "max_priority_level", maxPriority.GetLevel())(
            "max_priority_weight", maxPriority.GetInternalLevelWeight())(
            "levels_count", Levels.size());
        return maxPriority;
    }

    std::vector<TPortionInfo::TPtr> GetOptimizationTasks(const std::shared_ptr<NDataLocks::TManager>& locksManager) const {
        ui8 maxLevel = GetMaxLevel();
        return Levels.at(maxLevel).GetOptimizationTasks(locksManager);
    }
};



} // NKikimr::NOlap::NStorageOptimizer::NTiling