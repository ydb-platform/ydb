
#include <ydb/core/tx/columnshard/blobs_action/abstract/storages_manager.h>
#include <ydb/core/tx/columnshard/engines/changes/general_compaction.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/engines/scheme/column_features.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/abstract/optimizer.h>
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
    ui64 SmallPortionLimitBytes = 512 * 1024; // 512KiB

    TMap<ui64, TPortionInfo::TPtr> Portions;
    TSet<ui64> SmallPortions;
    ui64 SmallPortionsTotalBlobBytes = 0;

    void Actualize(const ui8 levelIdx, ui64 currentBlobBytes, ui64 currentRecordsCount) {
        MaxBlobBytes = currentBlobBytes;
        MaxRecordsCount = currentRecordsCount;
        if (levelIdx == 1) {
            MaxHeight = 1;
            OverloadHeight = 3;
        } else {
            MaxHeight = 9;
            OverloadHeight = 11;
        }
    }

    bool IsOverloaded() const {
        return TotalBlobBytes > MaxBlobBytes || TotalRecordsCount > MaxRecordsCount ||
            Intersections.GetMaxCount() > static_cast<i32>(OverloadHeight) || SmallPortionsTotalBlobBytes > SmallPortionsOverloadBlobBytes;
    }

    TOptimizationPriority NeedCompaction() const {
        if (IsOverloaded()) {
            return TOptimizationPriority::Critical(100500);
        }
        return TOptimizationPriority::Zero();
    }

    void Add(const TPortionInfo::TPtr& p) {
        Intersections.Add(p->GetPortionId(), p->IndexKeyStart(), p->IndexKeyEnd());
        Portions[p->GetPortionId()] = p;
        if (p->GetTotalBlobBytes() < SmallPortionLimitBytes) {
            SmallPortions.insert(p->GetPortionId());
            SmallPortionsTotalBlobBytes += p->GetTotalBlobBytes();
        }
        TotalBlobBytes += p->GetTotalBlobBytes();
        TotalRecordsCount += p->GetRecordsCount();
    }

    void Remove(const TPortionInfo::TPtr& p) {
        Intersections.Remove(p->GetPortionId());
        Portions.erase(p->GetPortionId());
        if (SmallPortions.contains(p->GetPortionId())) {
            SmallPortions.erase(p->GetPortionId());
            SmallPortionsTotalBlobBytes -= p->GetTotalBlobBytes();
        }
        TotalBlobBytes -= p->GetTotalBlobBytes();
        TotalRecordsCount -= p->GetRecordsCount();
    }

    std::vector<TPortionInfo::TPtr> GetOptimizationTasks(const std::shared_ptr<NDataLocks::TManager>& locksManager) const {
        std::vector<TPortionInfo::TPtr> portions;

        if (Intersections.GetMaxCount() > static_cast<i32>(MaxHeight)) {
            auto range = Intersections.GetMaxRange();
            range.ForEachValue([&](ui64 id) {
                if (!locksManager->IsLocked(*Portions.at(id), NDataLocks::ELockCategory::Compaction)) {
                    portions.push_back(Portions.at(id));
                }
                return true;
            });
        } else if (SmallPortionsTotalBlobBytes > SmallPortionLimitBytes) {
            ui64 currentBlobBytes = 0;
            auto it = SmallPortions.begin();
            while (it != SmallPortions.end()) {
                if (locksManager->IsLocked(*Portions.at(*it), NDataLocks::ELockCategory::Compaction)) {
                    ++it;
                    continue;
                }
                currentBlobBytes += Portions.at(*it)->GetTotalBlobBytes();
                if (currentBlobBytes > SmallPortionLimitBytes) {
                    break;
                }
                portions.push_back(Portions.at(*it));
                ++it;
            }
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
        , PrimaryKeysSchema(primaryKeysSchema) {
        AFL_VERIFY(StoragesManager);
        Y_UNUSED(PrimaryKeysSchema);
    }

private:
    ui64 InitialBlobBytes = 1024 * 1024 * 1024;           // 1GB
    ui64 InitialRecordsCount = 1'000'000;                 // 1M
    ui64 LastLevelBytes = 128 * 1024 * 1024;            // 128MB
    ui64 LastLevelRecordsCount = 100'000;               // 100K
    ui64 TotalBlobBytes = 0;
    ui64 TotalRecordsCount = 0;
    ui8 K = 10;
    ui8 LastLevel = 1;

    TMap<ui8, TLevel> Levels;
    THashMap<ui64, ui8> InternalLevel;

    std::shared_ptr<IStoragesManager> StoragesManager;
    std::shared_ptr<arrow::Schema> PrimaryKeysSchema;

    void Actualize() {
        ui64 currentBlobBytes = std::max(TotalBlobBytes, InitialBlobBytes);
        ui64 currentRowCount = std::max(TotalRecordsCount, InitialRecordsCount);
        for (ui32 i = 1; i < 3 || currentBlobBytes > LastLevelBytes || currentRowCount > LastLevelRecordsCount; ++i) {
            Y_ABORT_UNLESS(i <= std::numeric_limits<ui8>::max());
            const ui8 levelIdx = static_cast<ui8>(i);
            if (!Levels.contains(levelIdx)) {
                Levels[levelIdx] = TLevel();
            }
            Levels[levelIdx].Actualize(levelIdx, currentBlobBytes, currentRowCount);
            currentBlobBytes /= K;
            currentRowCount /= K;
            LastLevel = levelIdx;
        }
    }

    void DoModifyPortions(const std::vector<std::shared_ptr<TPortionInfo>>& add, const std::vector<std::shared_ptr<TPortionInfo>>& remove) override {
        for (const auto& p : remove) {
            auto lit = InternalLevel.find(p->GetPortionId());
            AFL_VERIFY(lit != InternalLevel.end())("event", "remove_unknown_portion")("portion_id", p->GetPortionId());
            Levels[lit->second].Remove(p);
            InternalLevel.erase(lit);
            TotalBlobBytes -= p->GetTotalBlobBytes();
            TotalRecordsCount -= p->GetRecordsCount();
        }
        for (const auto& p : add) {
            const ui64 id = p->GetPortionId();
            if (auto lit = InternalLevel.find(id); lit != InternalLevel.end()) {
                Levels[lit->second].Remove(p);
                InternalLevel.erase(lit);
            } else {
                TotalBlobBytes += p->GetTotalBlobBytes();
                TotalRecordsCount += p->GetRecordsCount();
            }
            if (p->GetCompactionLevel() == 0) {
                InternalLevel[id] = LastLevel;
                Levels[LastLevel].Add(p);
            } else {
                const ui8 lvl = static_cast<ui8>(std::min<ui32>(p->GetCompactionLevel(), std::numeric_limits<ui8>::max()));
                InternalLevel[id] = lvl;
                Levels[lvl].Add(p);
            }
        }
        Actualize();
    }

    std::vector<std::shared_ptr<TColumnEngineChanges>> DoGetOptimizationTasks(
        std::shared_ptr<TGranuleMeta> granule, const std::shared_ptr<NDataLocks::TManager>& dataLocksManager) const override {
        for (const auto& [level, levelData] : Levels) {
            if (!levelData.NeedCompaction().IsCritical()) {
                continue;
            }
            auto tasks = levelData.GetOptimizationTasks(dataLocksManager);
            if (tasks.empty()) {
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
            result->SetPortionExpectedSize(std::max(levelData.MaxBlobBytes / 4, ui64(4 * 1024 * 1024)));
            return {result};
        }
        return {};
    }

    TOptimizationPriority DoGetUsefulMetric() const override {
        TOptimizationPriority maxPriority = TOptimizationPriority::Zero();
        for (const auto& [_, levelData] : Levels) {
            maxPriority = std::max(maxPriority, levelData.NeedCompaction());
        }
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
