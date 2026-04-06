#include <ydb/core/tx/columnshard/data_locks/manager/manager.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/abstract/optimizer.h>
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
    LastLevel(LastLevelSettings settings)
        : Settings(settings) {}

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
        return TOptimizationPriority::Normalize(1, Settings.CandidatePortionsOverload, Candidates.size());
    }

    void Add(TPortionInfo::TPtr p) {
        if (Measure(p) == 0) {
            Portions.insert(p);
        }
        else {
            Candidates.insert(p);
        }
    }

    void Remove(TPortionInfo::TPtr p) {
        if (Portions.contains(p)) {
            Portions.erase(p);
        }
        else if (Candidates.contains(p)) {
            Candidates.erase(p);
        }
        else {
            AFL_VERIFY(false);
        }
    }

    std::pair<PrtionsEndSorted::iterator, PrtionsEndSorted::iterator> Borders(TPortionInfo::TPtr p) const {
        auto start = Portions.lower_bound(p->IndexKeyStart());
        auto end = Portions.upper_bound(p->IndexKeyEnd());
        if (end != Portions.end() && (*end)->IndexKeyStart() <= p->IndexKeyEnd()) {
            ++end;
        }
        return std::make_pair(start, end);
    }

    ui64 Measure(TPortionInfo::TPtr p) const {
        auto [start, end] = Borders(p);
        return std::distance(start, end);
    }

    std::vector<TPortionInfo::TPtr> GetOptimizationTasks(const std::shared_ptr<NDataLocks::TManager>& locksManager) const{
        for (auto& candidate : Candidates) {
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
                if (currentBlobBytes > Settings.Compaction.Bytes || result.size() > Settings.Compaction.Bytes) {
                    break;
                }
            }
            if (success) {
                return result;
            }
        }
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
    Accumulator(AccumulatorSettings settings)
        : Settings(settings) {}

    TSet<TPortionInfo::TPtr> Portions;
    ui64 TotalBlobBytes = 0;

    TOptimizationPriority DoGetUsefulMetric() const {
        auto portionPriority = TOptimizationPriority::Normalize(Settings.Trigger.Portions, Settings.Overload.Portions, Portions.size());
        auto bytestPriority = TOptimizationPriority::Normalize(Settings.Trigger.Bytes, Settings.Overload.Bytes, TotalBlobBytes);
        return std::max(portionPriority, bytestPriority);
    }

    void Add(TPortionInfo::TPtr p) {
        Portions.insert(p);
        TotalBlobBytes += p->GetTotalBlobBytes();
    }

    void Remove(TPortionInfo::TPtr p) {
        Portions.erase(p);
        TotalBlobBytes -= p->GetTotalBlobBytes();
    }

    std::vector<TPortionInfo::TPtr> GetOptimizationTasks(const std::shared_ptr<NDataLocks::TManager>& locksManager) const {
        std::vector<TPortionInfo::TPtr> result;
        ui64 currentBlobBytes = 0;
        for (auto it : Portions) {
            if (locksManager->IsLocked(*it, NDataLocks::ELockCategory::Compaction)) {
                continue;
            }
            result.push_back(it);
            currentBlobBytes += it->GetTotalBlobBytes();
            if (currentBlobBytes > Settings.Compaction.Bytes || result.size() > Settings.Compaction.Portions) {
                break;
            }
        }
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

        MiddleLevel(MiddleLevelSettings settings)
            : Settings(settings) {};

        TIntersectionTree<NArrow::TSimpleRow, TPortionInfo::TPtr> Intersections;

        TOptimizationPriority DoGetUsefulMetric() const {
            return TOptimizationPriority::Normalize(Settings.TriggerHight, Settings.OverloadHight, Intersections.GetMaxCount());
        }

        void Add(const TPortionInfo::TPtr& p) {
            Intersections.Add(p, p->IndexKeyStart(), p->IndexKeyEnd());
            // Counters.Portions->AddPortion(p);
            // Counters.Portions->SetHeight(Intersections.GetMaxCount());
        }

        void Remove(const TPortionInfo::TPtr& p) {
            Intersections.Remove(p);
            // Counters.Portions->RemovePortion(p);
            // Counters.Portions->SetHeight(Intersections.GetMaxCount());
        }

        std::vector<TPortionInfo::TPtr> GetOptimizationTasks(const std::shared_ptr<NDataLocks::TManager>& locksManager) const {
            std::vector<TPortionInfo::TPtr> portions;
            auto range = Intersections.GetMaxRange();
            range.ForEachValue([&](const TPortionInfo::TPtr& p) {
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

    MiddleLevels(MiddleLevel::MiddleLevelSettings settings)
        : Levels(10, settings) {}


    void Add(TPortionInfo::TPtr p, ui8 level) {
        Levels.at(level).Add(p);
    }

    void Remove(TPortionInfo::TPtr p, ui8 level) {
        Levels.at(level).Remove(p);
    }

    TOptimizationPriority DoGetUsefulMetric() const {
        auto maxPriority = TOptimizationPriority::Zero();
        for (auto& level : Levels) {
            maxPriority = std::max(maxPriority, level.DoGetUsefulMetric());
        }
        return maxPriority;
    }

    std::vector<TPortionInfo::TPtr> GetOptimizationTasks(const std::shared_ptr<NDataLocks::TManager>& locksManager) const {

        for (auto& level : Levels) {
            if (level.DoGetUsefulMetric().IsCritical()) {
                auto portions = level.GetOptimizationTasks(locksManager);
                if (portions.size() > 1) {
                    return portions;
                }
            }
        }

        return {};
    }
};



} // NKikimr::NOlap::NStorageOptimizer::NTiling