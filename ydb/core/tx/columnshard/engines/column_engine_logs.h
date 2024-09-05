#pragma once

#include "defs.h"
#include "column_engine.h"
#include <ydb/core/tx/columnshard/common/scalars.h>
#include <ydb/core/tx/columnshard/common/limits.h>
#include <ydb/core/tx/columnshard/counters/engine_logs.h>
#include <ydb/core/tx/columnshard/columnshard_ttl.h>

#include "changes/actualization/controller/controller.h"

#include "scheme/tier_info.h"
#include "storage/granule.h"
#include "storage/storage.h"

namespace NKikimr::NArrow {
struct TSortDescription;
}

namespace NKikimr::NOlap {

class TCompactColumnEngineChanges;
class TTTLColumnEngineChanges;
class TChangesWithAppend;
class TCompactColumnEngineChanges;
class TCleanupPortionsColumnEngineChanges;
class TCleanupTablesColumnEngineChanges;

namespace NDataSharing {
class TDestinationSession;
}

struct TReadMetadata;

/// Engine with 2 tables:
/// - Granules: PK -> granules (use part of PK)
/// - Columns: granule -> blobs
///
/// @note One instance per tablet.
class TColumnEngineForLogs : public IColumnEngine {
    friend class TCompactColumnEngineChanges;
    friend class TTTLColumnEngineChanges;
    friend class TChangesWithAppend;
    friend class TCompactColumnEngineChanges;
    friend class TCleanupPortionsColumnEngineChanges;
    friend class TCleanupTablesColumnEngineChanges;
    friend class NDataSharing::TDestinationSession;

private:
    bool ActualizationStarted = false;
    const NColumnShard::TEngineLogsCounters SignalCounters;
    std::shared_ptr<TGranulesStorage> GranulesStorage;
    std::shared_ptr<IStoragesManager> StoragesManager;

    std::shared_ptr<NActualizer::TController> ActualizationController;

public:
    const std::shared_ptr<NActualizer::TController>& GetActualizationController() const {
        return ActualizationController;
    }

    ui64* GetLastPortionPointer() {
        return &LastPortion;
    }

    enum ETableIdx {
        GRANULES = 0,
    };

    enum ECounterIds {
        LAST_PORTION = 1,
        LAST_GRANULE,
        LAST_PLAN_STEP,
        LAST_TX_ID,
    };

    enum class EStatsUpdateType {
        DEFAULT = 0,
        ERASE,
        ADD,
    };

    TColumnEngineForLogs(ui64 tabletId, const std::shared_ptr<IStoragesManager>& storagesManager, const TSnapshot& snapshot, const NKikimrSchemeOp::TColumnTableSchema& schema);
    TColumnEngineForLogs(ui64 tabletId, const std::shared_ptr<IStoragesManager>& storagesManager, const TSnapshot& snapshot, TIndexInfo&& schema);

    virtual void OnTieringModified(const std::shared_ptr<NColumnShard::TTiersManager>& manager, const NColumnShard::TTtl& ttl, const std::optional<ui64> pathId) override;

    virtual std::shared_ptr<TVersionedIndex> CopyVersionedIndexPtr() const override {
        return std::make_shared<TVersionedIndex>(VersionedIndex);
    }

    const TVersionedIndex& GetVersionedIndex() const override {
        return VersionedIndex;
    }

    const TMap<ui64, std::shared_ptr<TColumnEngineStats>>& GetStats() const override;
    const TColumnEngineStats& GetTotalStats() override;
    TSnapshot LastUpdate() const override {
        return LastSnapshot;
    }

    virtual void DoRegisterTable(const ui64 pathId) override;

public:
    bool Load(IDbWrapper& db) override;

    virtual bool IsOverloadedByMetadata(const ui64 limit) const override {
        return limit < TGranulesStat::GetSumMetadataMemoryPortionsSize();
    }

    std::shared_ptr<TInsertColumnEngineChanges> StartInsert(std::vector<TInsertedData>&& dataToIndex) noexcept override;
    std::shared_ptr<TColumnEngineChanges> StartCompaction(const std::shared_ptr<NDataLocks::TManager>& dataLocksManager) noexcept override;
    std::shared_ptr<TCleanupPortionsColumnEngineChanges> StartCleanupPortions(const TSnapshot& snapshot, const THashSet<ui64>& pathsToDrop, const std::shared_ptr<NDataLocks::TManager>& dataLocksManager) noexcept override;
    std::shared_ptr<TCleanupTablesColumnEngineChanges> StartCleanupTables(const THashSet<ui64>& pathsToDrop) noexcept override;
    std::vector<std::shared_ptr<TTTLColumnEngineChanges>> StartTtl(const THashMap<ui64, TTiering>& pathEviction, const std::shared_ptr<NDataLocks::TManager>& locksManager, const ui64 memoryUsageLimit) noexcept override;

    void ReturnToIndexes(const THashMap<ui64, THashSet<ui64>>& portions) const {
        return GranulesStorage->ReturnToIndexes(portions);
    }
    virtual bool ApplyChangesOnTxCreate(std::shared_ptr<TColumnEngineChanges> indexChanges, const TSnapshot& snapshot) noexcept override;
    virtual bool ApplyChangesOnExecute(IDbWrapper& db, std::shared_ptr<TColumnEngineChanges> indexChanges, const TSnapshot& snapshot) noexcept override;

    void RegisterSchemaVersion(const TSnapshot& snapshot, TIndexInfo&& info) override;
    void RegisterSchemaVersion(const TSnapshot& snapshot, const NKikimrSchemeOp::TColumnTableSchema& schema) override;

    std::shared_ptr<TSelectInfo> Select(ui64 pathId, TSnapshot snapshot, const TPKRangesFilter& pkRangesFilter) const override;

    bool IsPortionExists(const ui64 pathId, const ui64 portionId) const {
        return !!GranulesStorage->GetPortionOptional(pathId, portionId);
    }

    virtual bool ErasePathId(const ui64 pathId) override {
        if (HasDataInPathId(pathId)) {
            return false;
        }
        return GranulesStorage->EraseTable(pathId);
    }


    virtual bool HasDataInPathId(const ui64 pathId) const override {
        auto g = GetGranuleOptional(pathId);
        return g && g->GetPortions().size();
    }

    bool IsGranuleExists(const ui64 pathId) const {
        return !!GetGranuleOptional(pathId);
    }

    const TGranuleMeta& GetGranuleVerified(const ui64 pathId) const {
        return *GetGranulePtrVerified(pathId);
    }

    std::shared_ptr<TGranuleMeta> GetGranulePtrVerified(const ui64 pathId) const {
        auto result = GetGranuleOptional(pathId);
        AFL_VERIFY(result)("path_id", pathId);
        return result;
    }

    std::shared_ptr<TGranuleMeta> GetGranuleOptional(const ui64 pathId) const {
        return GranulesStorage->GetGranuleOptional(pathId);
    }

    std::vector<std::shared_ptr<TGranuleMeta>> GetTables(const std::optional<ui64> pathIdFrom, const std::optional<ui64> pathIdTo) const {
        return GranulesStorage->GetTables(pathIdFrom, pathIdTo);
    }

    ui64 GetTabletId() const {
        return TabletId;
    }

    void AddCleanupPortion(const TPortionInfo& info) {
        CleanupPortions[info.GetRemoveSnapshotVerified().GetPlanInstant()].emplace_back(info);
    }
    void AddShardingInfo(const TGranuleShardingInfo& shardingInfo) {
        VersionedIndex.AddShardingInfo(shardingInfo);
    }

private:
    TVersionedIndex VersionedIndex;
    ui64 TabletId;
    TMap<ui64, std::shared_ptr<TColumnEngineStats>> PathStats;   // per path_id stats sorted by path_id
    std::map<TInstant, std::vector<TPortionInfo>> CleanupPortions;
    TColumnEngineStats Counters;
    ui64 LastPortion;
    ui64 LastGranule;
    TSnapshot LastSnapshot = TSnapshot::Zero();
    bool Loaded = false;

private:
    bool LoadColumns(IDbWrapper& db);
    bool LoadShardingInfo(IDbWrapper& db);
    bool LoadCounters(IDbWrapper& db);

    void UpsertPortion(const TPortionInfo& portionInfo, const TPortionInfo* exInfo = nullptr);
    bool ErasePortion(const TPortionInfo& portionInfo, bool updateStats = true);
    void UpdatePortionStats(const TPortionInfo& portionInfo, EStatsUpdateType updateType = EStatsUpdateType::DEFAULT, const TPortionInfo* exPortionInfo = nullptr);
    void UpdatePortionStats(TColumnEngineStats& engineStats, const TPortionInfo& portionInfo, EStatsUpdateType updateType, const TPortionInfo* exPortionInfo = nullptr) const;
};

}   // namespace NKikimr::NOlap
