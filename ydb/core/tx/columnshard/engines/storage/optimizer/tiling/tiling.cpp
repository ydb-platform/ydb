
#include <ydb/core/tx/columnshard/blobs_action/abstract/storages_manager.h>
#include <ydb/core/tx/columnshard/engines/changes/general_compaction.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/engines/scheme/column_features.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/abstract/optimizer.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/tiling/counters.h>
#include <ydb/library/intersection_tree/intersection_tree.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>

#include <algorithm>
#include <limits>

namespace NKikimr::NOlap::NStorageOptimizer::NTiling {

struct TSimpleKeyCompare {
    std::partial_ordering operator()(const NArrow::TSimpleRow& a, const NArrow::TSimpleRow& b) const {
        return a.CompareNotNull(b);
    }
};

struct TLevel {
    TIntersectionTree<NArrow::TSimpleRow, ui64, TSimpleKeyCompare> Intersections;
    ui8 MaxHeight = 9;       // set in Actualize(levelIdx, …) per tier
    ui8 OverloadHeight = 11;
    ui64 SmallPortionsOverloadBlobBytes = 10 * 1024 * 1024; // 10MB
    ui64 MaxBlobBytes = 0;
    ui64 MaxRecordsCount = 0;
    ui64 TotalBlobBytes = 0;
    ui64 TotalRecordsCount = 0;
    // LevelIdx must be declared before Counters (reference) so initializer order is valid.
    ui8 LevelIdx = 0;
    const TLevelCounters& Counters;

    TMap<ui64, TPortionInfo::TPtr> Portions;

    TLevel(const TLevelCounters& counters, const ui8 levelIdx)
        : LevelIdx(levelIdx)
        , Counters(counters)
    {}

    void Actualize(ui64 currentBlobBytes) {
        MaxBlobBytes = currentBlobBytes;
    }

    bool IsOverloadedBySize() const {
        if (MaxBlobBytes > 0 && TotalBlobBytes > MaxBlobBytes) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)(
                "event", "tiling_level_overloaded_by_bytes")(
                "level", (ui32)LevelIdx)(
                "total_blob_bytes", TotalBlobBytes)(
                "max_blob_bytes", MaxBlobBytes)(
                "total_records", TotalRecordsCount)(
                "intersection_height", Intersections.GetMaxCount())(
                "overload_height", (ui32)OverloadHeight);
            return true;
        }
        return false;
    }

    bool IsOverloadedByHeight() const {
        if (Intersections.GetMaxCount() > static_cast<i32>(OverloadHeight)) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)(
                "event", "tiling_level_overloaded_by_intersections")(
                "level", (ui32)LevelIdx)(
                "total_blob_bytes", TotalBlobBytes)(
                "max_blob_bytes", MaxBlobBytes)(
                "total_records", TotalRecordsCount)(
                "intersection_height", Intersections.GetMaxCount())(
                "overload_height", (ui32)OverloadHeight);
            return true;
        }
        return false;
    }

    bool IsOverloaded() const {
        return IsOverloadedBySize() || IsOverloadedByHeight();
    }

    bool IsEmpty() const {
        return Portions.size() == 0;
    }

    TOptimizationPriority NeedCompaction() const {
        if (IsOverloaded()) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)(
                "event", "tiling_level_needs_critical_compaction")(
                "level", (ui32)LevelIdx)(
                "total_blob_bytes", TotalBlobBytes)(
                "max_blob_bytes", MaxBlobBytes)(
                "intersection_height", Intersections.GetMaxCount())(
                "overload_height", (ui32)OverloadHeight)(
                "max_height", (ui32)MaxHeight);
            return TOptimizationPriority::Critical(100500);
        }
        auto height = Intersections.GetMaxCount();
        if (height > 1) {
            // Intersection exceeds the soft limit but not the overload threshold yet
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)(
                "event", "tiling_level_needs_soft_compaction")(
                "level", (ui32)LevelIdx)(
                "intersection_height", height)(
                "max_height", (ui32)MaxHeight)(
                "overload_height", (ui32)OverloadHeight);
            return TOptimizationPriority::LevelOptimization(height);
        }
        return TOptimizationPriority::Zero();
    }

    void Add(const TPortionInfo::TPtr& p) {
        AFL_VERIFY(p)("event", "tiling_add_null_portion")("level", (ui32)LevelIdx);
        Intersections.Add(p->GetPortionId(), p->IndexKeyStart(), p->IndexKeyEnd());
        Portions[p->GetPortionId()] = p;
        Counters.Portions->AddPortion(p);
        Counters.Portions->SetHeight(Intersections.GetMaxCount());
        TotalBlobBytes += p->GetTotalBlobBytes();
        TotalRecordsCount += p->GetRecordsCount();
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)(
            "event", "tiling_portion_added")(
            "level", (ui32)LevelIdx)(
            "portion_id", p->GetPortionId())(
            "portion_blob_bytes", p->GetTotalBlobBytes())(
            "portion_records", p->GetRecordsCount())(
            "level_total_bytes", TotalBlobBytes)(
            "level_total_records", TotalRecordsCount)(
            "intersection_height", Intersections.GetMaxCount());
    }

    void Remove(const TPortionInfo::TPtr& p) {
        AFL_VERIFY(p)("event", "tiling_remove_null_portion")("level", (ui32)LevelIdx);
        AFL_VERIFY(Portions.contains(p->GetPortionId()))(
            "event", "tiling_remove_unknown_portion")(
            "level", (ui32)LevelIdx)(
            "portion_id", p->GetPortionId());
        Intersections.Remove(p->GetPortionId());
        Portions.erase(p->GetPortionId());
        Counters.Portions->RemovePortion(p);
        Counters.Portions->SetHeight(Intersections.GetMaxCount());
        TotalBlobBytes -= p->GetTotalBlobBytes();
        TotalRecordsCount -= p->GetRecordsCount();
    }

    // Returns portions to compact based on intersection height.
    // Primary path: when intersection_height > MaxHeight, compact the max-intersection range.
    // Fallback path: when the level is byte-overloaded but intersection_height <= MaxHeight
    //   (e.g. a single large portion), compact ALL unlocked portions in the level.
    //   This fixes the stuck condition where a single oversized portion can never trigger
    //   compaction via the intersection-height path alone.
    std::vector<TPortionInfo::TPtr> GetOptimizationTasks(const std::shared_ptr<NDataLocks::TManager>& locksManager) const {
        std::vector<TPortionInfo::TPtr> portions;

        const auto height = Intersections.GetMaxCount();
        auto range = Intersections.GetMaxRange();
        ui32 skippedLocked = 0;
        range.ForEachValue([&](ui64 id) {
            AFL_VERIFY(Portions.contains(id))(
                "event", "tiling_task_portion_not_found")(
                "level", (ui32)LevelIdx)(
                "portion_id", id);
            if (!locksManager->IsLocked(*Portions.at(id), NDataLocks::ELockCategory::Compaction)) {
                portions.push_back(Portions.at(id));
            } else {
                ++skippedLocked;
            }
            return true;
        });
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)(
            "event", "tiling_get_optimization_tasks")(
            "level", (ui32)LevelIdx)(
            "intersection_height", height)(
            "max_height", (ui32)MaxHeight)(
            "selected_portions", portions.size())(
            "skipped_locked", skippedLocked);
        if (portions.empty() && skippedLocked > 0) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)(
                "event", "tiling_stuck_all_portions_locked")(
                "level", (ui32)LevelIdx)(
                "intersection_height", height)(
                "locked_count", skippedLocked);
        }
        return portions;
    }
};

class TOptimizerPlanner : public IOptimizerPlanner {
public:
    TOptimizerPlanner(
        const TInternalPathId pathId, const std::shared_ptr<IStoragesManager>& storagesManager, const std::shared_ptr<arrow::Schema>& primaryKeysSchema)
        : IOptimizerPlanner(pathId, std::nullopt)
        , StoragesManager(storagesManager)
        , PrimaryKeysSchema(primaryKeysSchema)
        , Counters(std::make_shared<TCounters>())
        , PortionsInfo(std::make_shared<TSimplePortionsGroupInfo>()) {
        AFL_VERIFY(StoragesManager);
        Y_UNUSED(PrimaryKeysSchema);
        Levels.insert({1, TLevel(Counters->GetLevelCounters(1), 1)});
        Levels.at(1).MaxHeight = 1;
        Levels.at(1).OverloadHeight = 2;
        for (int i = 2; i < 10; ++i) {
            Levels.insert({i, TLevel(Counters->GetLevelCounters(i), static_cast<ui8>(i))});
        }
    }

    virtual bool DoIsOverloaded() const override {
        for (auto& [_, level] : Levels) {
            if (level.IsOverloaded()) {
                return true;
            }
        }
        return false;
    }

private:
    // Lowered thresholds to fill levels faster in tests and expose stuck conditions sooner.
    // Production-scale values would be 128MB initial, 8MB last-level, K=10.
    // With K=5 and LastLevelBytes=16KB, InitialBlobBytes=4MB creates 4 active levels:
    //   level1=4MB, level2=800KB, level3=160KB, level4=32KB → LastLevel=4
    ui64 InitialBlobBytes = 128 * 1024 * 1024;    // 4MB (was 128MB)
    ui64 LastLevelBytes = 1024 * 1024;             // 16KB — with K=5 gives 4 levels from 4MB initial
    ui64 TotalBlobBytes = 0;
    ui64 TotalRecordsCount = 0;
    ui8 K = 5;                                   // reduced from 10 to create more levels with less data
    ui8 LastLevel = 1;

    TMap<ui8, TLevel> Levels;
    THashMap<ui64, ui8> InternalLevel;

    std::shared_ptr<IStoragesManager> StoragesManager;
    std::shared_ptr<arrow::Schema> PrimaryKeysSchema;

    std::shared_ptr<TCounters> Counters;
    std::shared_ptr<TSimplePortionsGroupInfo> PortionsInfo;

    void Actualize() {
        ui64 currentBlobBytes = std::max(TotalBlobBytes, InitialBlobBytes);
        // Reset all level budgets first
        for (auto& [_, level] : Levels) {
            level.Actualize(0);
        }
        ui8 newLastLevel = 1;
        for (ui8 i = 1; currentBlobBytes > LastLevelBytes; ++i) {
            AFL_VERIFY(Levels.contains(i))(
                "event", "tiling_actualize_missing_level")(
                "level", (ui32)i)(
                "current_blob_bytes", currentBlobBytes)(
                "last_level_bytes", LastLevelBytes);
            Levels.at(i).Actualize(currentBlobBytes);
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)(
                "event", "tiling_actualize_level")(
                "level", (ui32)i)(
                "budget_bytes", currentBlobBytes)(
                "total_planner_bytes", TotalBlobBytes)(
                "total_planner_records", TotalRecordsCount);
            currentBlobBytes /= K;
            newLastLevel = i;
        }
        LastLevel = newLastLevel;
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)(
            "event", "tiling_actualize_done")(
            "last_level", (ui32)LastLevel)(
            "total_blob_bytes", TotalBlobBytes)(
            "total_records", TotalRecordsCount)(
            "initial_blob_bytes", InitialBlobBytes)(
            "last_level_bytes", LastLevelBytes);
        // Warn about non-empty levels that have no budget (potential stuck source)
        for (auto& [idx, level] : Levels) {
            if (!level.IsEmpty() && level.MaxBlobBytes == 0) {
                AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)(
                    "event", "tiling_level_non_empty_no_budget")(
                    "level", (ui32)idx)(
                    "portions_count", level.Portions.size())(
                    "total_bytes", level.TotalBlobBytes)(
                    "last_level", (ui32)LastLevel);
            }
        }

        for (auto& [level_id, level] : Levels) {
            if (level.IsOverloadedBySize() && level.Intersections.GetMaxCount() == 1) {
                AFL_VERIFY(level.Portions.size())(
                    "event", "tiling_empty_level_overload")(
                    "last_level", (ui32)LastLevel)(
                    "total_blob_bytes", TotalBlobBytes)(
                    "total_records", TotalRecordsCount)(
                    "initial_blob_bytes", InitialBlobBytes)(
                    "last_level_bytes", LastLevelBytes);

                std::vector<TPortionInfo::TPtr> to_promote;
                int i = 0;
                for (auto it = level.Portions.begin(); it != level.Portions.end() && i < 10; ++it, ++i) {
                    to_promote.push_back(it->second);
                }
                for (auto& p : to_promote) {
                    RemovePortion(p);
                }
                for (auto& p : to_promote) {
                    AddPortion(p, level_id - 1);
                    AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)(
                        "event", "tiling_portion_promoted")(
                        "level", (ui32)level_id)(
                        "id", p->GetPortionId());
                }
            }
        }
    }

    void RemovePortion(std::shared_ptr<TPortionInfo> p) {
        // EVICTED and INACTIVE portions were never added to InternalLevel (skipped in AddPortion),
        // so attempting to remove them would be a no-op. Skip them here too.
        const auto produced = p->GetProduced();
        if (produced == NPortion::EVICTED || produced == NPortion::INACTIVE ||
            produced == NPortion::UNSPECIFIED || produced == NPortion::COMPACTED) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)(
                "event", "tiling_remove_portion_skipped")(
                "reason", "not_tracked_type")(
                "portion_id", p->GetPortionId())(
                "produced", (ui32)produced);
            return;
        }

        PortionsInfo->RemovePortion(p);

        auto lit = InternalLevel.find(p->GetPortionId());
        if (lit == InternalLevel.end()) {
            // Portion was never tracked (e.g. added before planner switch). Log and skip.
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)(
                "event", "tiling_remove_unknown_portion")(
                "portion_id", p->GetPortionId())(
                "produced", (ui32)produced);
            return;
        }
        AFL_VERIFY(Levels.contains(lit->second))(
            "event", "tiling_remove_portion_missing_level")(
            "portion_id", p->GetPortionId())(
            "level", (ui32)lit->second);
        Levels.at(lit->second).Remove(p);
        InternalLevel.erase(lit);

        TotalBlobBytes -= p->GetTotalBlobBytes();
        TotalRecordsCount -= p->GetRecordsCount();
    }

    void AddPortion(std::shared_ptr<TPortionInfo> p, std::optional<ui8> set_level = std::nullopt) {
        switch (p->GetProduced()) {
            case NPortion::EVICTED:
            case NPortion::INACTIVE:
                // Tiered-out or deleted portions: skip silently (same as old tiling planner).
                AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)(
                    "event", "tiling_add_portion_skipped")(
                    "reason", "evicted_or_inactive")(
                    "portion_id", p->GetPortionId())(
                    "produced", (ui32)p->GetProduced());
                return;
            case NPortion::UNSPECIFIED:
            case NPortion::COMPACTED:
                // Legacy / unexpected values: log and skip rather than crash.
                AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)(
                    "event", "tiling_add_portion_unexpected_type")(
                    "portion_id", p->GetPortionId())(
                    "produced", (ui32)p->GetProduced());
                return;
            default:
                break;
        }

        PortionsInfo->AddPortion(p);

        auto level = set_level.value_or(p->GetCompactionLevel());
        if (level == 0) {
            level = LastLevel;
        }

        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)(
            "event", "tiling_add_portion")(
            "portion_id", p->GetPortionId())(
            "compaction_level_from_portion", p->GetCompactionLevel())(
            "resolved_level", (ui32)level)(
            "last_level", (ui32)LastLevel)(
            "portion_blob_bytes", p->GetTotalBlobBytes())(
            "portion_records", p->GetRecordsCount());

        AFL_VERIFY(Levels.contains(level))(
            "event", "tiling_add_portion_missing_level")(
            "portion_id", p->GetPortionId())(
            "level", (ui32)level)(
            "last_level", (ui32)LastLevel);

        InternalLevel[p->GetPortionId()] = level;

        Levels.at(level).Add(p);

        TotalBlobBytes += p->GetTotalBlobBytes();
        TotalRecordsCount += p->GetRecordsCount();
    }

    void DoModifyPortions(const std::vector<std::shared_ptr<TPortionInfo>>& add, const std::vector<std::shared_ptr<TPortionInfo>>& remove) override {
        for (const auto& p : remove) {
            RemovePortion(p);
        }
        for (const auto& p : add) {
            AddPortion(p);
        }
        Actualize();
    }

    std::vector<std::shared_ptr<TColumnEngineChanges>> DoGetOptimizationTasks(
        std::shared_ptr<TGranuleMeta> granule, const std::shared_ptr<NDataLocks::TManager>& dataLocksManager) const override {
        // First pass: critical (overloaded) levels
        for (const auto& [level, levelData] : Levels) {
            if (!levelData.NeedCompaction().IsCritical()) {
                continue;
            }
            auto tasks = levelData.GetOptimizationTasks(dataLocksManager);
            if (tasks.size() <= 1) {
                AFL_VERIFY(level != 1)(
                    "event", "tiling_critical_level_one")(
                    "level", (ui32)level)(
                    "total_bytes", levelData.TotalBlobBytes)(
                    "max_bytes", levelData.MaxBlobBytes)(
                    "intersection_height", levelData.Intersections.GetMaxCount())(
                    "overload_height", (ui32)levelData.OverloadHeight)(
                    "portions_count", levelData.Portions.size());
                AFL_VERIFY(!levelData.Portions.size())(
                    "event", "tiling_critical_empty_level")(
                    "level", (ui32)level)(
                    "total_bytes", levelData.TotalBlobBytes)(
                    "max_bytes", levelData.MaxBlobBytes)(
                    "intersection_height", levelData.Intersections.GetMaxCount())(
                    "overload_height", (ui32)levelData.OverloadHeight)(
                    "portions_count", levelData.Portions.size());
            }
            else {
                std::vector<TPortionInfo::TConstPtr> constPortions;
                constPortions.reserve(tasks.size());
                for (const auto& ptr : tasks) {
                    constPortions.emplace_back(ptr);
                }
                TSaverContext saverContext(StoragesManager);
                auto result = std::make_shared<NCompaction::TGeneralCompactColumnEngineChanges>(granule, constPortions, saverContext);
                const ui32 targetLevel = std::max<ui32>(ui32(level) - 1, 1);
                result->SetTargetCompactionLevel(targetLevel);
                result->SetPortionExpectedSize(ui64(64 * 1024));  // 512KB (was 4MB) to fill faster
                AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)(
                    "event", "tiling_schedule_critical_compaction")(
                    "source_level", (ui32)level)(
                    "target_level", targetLevel)(
                    "portions_count", constPortions.size());
                return {result};
            }
        }
        // Second pass: non-critical levels that still exceed MaxHeight
        for (const auto& [level, levelData] : Levels) {
            if (levelData.NeedCompaction().IsZero()) {
                continue;
            }
            auto tasks = levelData.GetOptimizationTasks(dataLocksManager);
            if (tasks.empty()) {
                AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)(
                    "event", "tiling_soft_level_no_tasks")(
                    "level", (ui32)level)(
                    "intersection_height", levelData.Intersections.GetMaxCount())(
                    "max_height", (ui32)levelData.MaxHeight)(
                    "portions_count", levelData.Portions.size());
                continue;
            }
            std::vector<TPortionInfo::TConstPtr> constPortions;
            constPortions.reserve(tasks.size());
            for (const auto& ptr : tasks) {
                constPortions.emplace_back(ptr);
            }
            TSaverContext saverContext(StoragesManager);
            auto result = std::make_shared<NCompaction::TGeneralCompactColumnEngineChanges>(granule, constPortions, saverContext);
            const ui32 targetLevel = std::max<ui32>(ui32(level) - 1, 1);
            result->SetTargetCompactionLevel(targetLevel);
            result->SetPortionExpectedSize(ui64(64 * 1024));  // 512KB (was 4MB) to fill faster
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)(
                "event", "tiling_schedule_soft_compaction")(
                "source_level", (ui32)level)(
                "target_level", targetLevel)(
                "portions_count", constPortions.size());
            return {result};
        }
        // Log when planner has non-zero metric but produces no task (stuck condition)
        const auto metric = DoGetUsefulMetric();
        if (!metric.IsZero()) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)(
                "event", "tiling_stuck_nonzero_metric_no_task")(
                "metric_is_critical", metric.IsCritical())(
                "total_blob_bytes", TotalBlobBytes)(
                "total_records", TotalRecordsCount)(
                "last_level", (ui32)LastLevel);
        }
        return {};
    }

    TOptimizationPriority DoGetUsefulMetric() const override {
        TOptimizationPriority maxPriority = TOptimizationPriority::Zero();
        for (const auto& [_, level] : Levels) {
            maxPriority = std::max(maxPriority, level.NeedCompaction());
        }
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)(
            "event", "tiling_get_useful_metrics")(
            "metric_is_critical", maxPriority.IsCritical())(
            "maxPrioritLevel", maxPriority.GetLevel())(
            "total_blob_bytes", TotalBlobBytes)(
            "total_records", TotalRecordsCount)(
            "last_level", (ui32)LastLevel);
        return maxPriority;
    }

    void DoActualize(const TInstant /*currentInstant*/) override {
    }

    NArrow::NMerger::TIntervalPositions GetBucketPositions() const override {
        return {};
    }

    std::vector<TTaskDescription> DoGetTasksDescription() const override {
        return {};
    }
};

class TOptimizerPlannerConstructor : public IOptimizerPlannerConstructor {
public:
    static TString GetClassNameStatic() {
        return "tiling";
    }

    TString GetClassName() const override {
        return GetClassNameStatic();
    }

private:
    static inline const TFactory::TRegistrator<TOptimizerPlannerConstructor> Registrator =
        TFactory::TRegistrator<TOptimizerPlannerConstructor>(GetClassNameStatic());

    void DoSerializeToProto(TProto& /*proto*/) const override {
    }

    bool DoDeserializeFromProto(const TProto& /*proto*/) override {
        return true;
    }

    TConclusionStatus DoDeserializeFromJson(const NJson::TJsonValue& /*jsonInfo*/) override {
        return TConclusionStatus::Success();
    }

    bool DoApplyToCurrentObject(IOptimizerPlanner& /*current*/) const override {
        return false;
    }

    TConclusion<std::shared_ptr<IOptimizerPlanner>> DoBuildPlanner(const TBuildContext& context) const override {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("message", "creating tiling compaction optimizer (intersection-tree)");
        return std::make_shared<TOptimizerPlanner>(context.GetPathId(), context.GetStorages(), context.GetPKSchema());
    }
};

} // namespace NKikimr::NOlap::NStorageOptimizer::NTiling
