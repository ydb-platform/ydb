#pragma once

#include "defs.h"
#include "column_engine.h"
#include <ydb/core/tx/columnshard/common/scalars.h>
#include <ydb/core/tx/columnshard/common/limits.h>
#include <ydb/core/tx/columnshard/counters/engine_logs.h>
#include <ydb/core/tx/columnshard/columnshard_ttl.h>
#include "scheme/tier_info.h"
#include "storage/granule.h"
#include "storage/storage.h"
#include "changes/indexation.h"
#include "changes/ttl.h"
#include "changes/with_appended.h"

namespace NKikimr::NArrow {
struct TSortDescription;
}

namespace NKikimr::NOlap {

namespace NDataSharing {
class TDestinationSession;
}

struct TReadMetadata;
class TGranulesTable;
class TColumnsTable;
class TCountersTable;

class TEvictionsController {
public:
    class TTieringWithPathId {
    private:
        const ui64 PathId;
        TTiering TieringInfo;
    public:
        TTieringWithPathId(const ui64 pathId, TTiering&& tieringInfo)
            : PathId(pathId)
            , TieringInfo(std::move(tieringInfo))
        {

        }

        ui64 GetPathId() const {
            return PathId;
        }

        const TTiering& GetTieringInfo() const {
            return TieringInfo;
        }
    };
private:
    THashMap<ui64, TTiering> OriginalTierings;
    std::map<TMonotonic, std::vector<TTieringWithPathId>> NextCheckInstantForTierings;

    std::map<TMonotonic, std::vector<TTieringWithPathId>> BuildNextInstantCheckers(THashMap<ui64, TTiering>&& info) {
        std::map<TMonotonic, std::vector<TTieringWithPathId>> result;
        std::vector<TTieringWithPathId> newTasks;
        for (auto&& i : info) {
            newTasks.emplace_back(i.first, std::move(i.second));
        }
        result.emplace(TMonotonic::Zero(), std::move(newTasks));
        return result;
    }
public:
    std::map<TMonotonic, std::vector<TTieringWithPathId>>& MutableNextCheckInstantForTierings() {
        return NextCheckInstantForTierings;
    }

    void RefreshTierings(std::optional<THashMap<ui64, TTiering>>&& tierings, const NColumnShard::TTtl& ttl);
};


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
    friend class TCleanupColumnEngineChanges;
    friend class NDataSharing::TDestinationSession;
private:
    const NColumnShard::TEngineLogsCounters SignalCounters;
    std::shared_ptr<TGranulesStorage> GranulesStorage;
    std::shared_ptr<IStoragesManager> StoragesManager;
    TEvictionsController EvictionsController;
    class TTieringProcessContext {
    private:
        const ui64 MemoryUsageLimit;
        ui64 MemoryUsage = 0;
        ui64 TxWriteVolume = 0;
        std::shared_ptr<TColumnEngineChanges::IMemoryPredictor> MemoryPredictor;
    public:
        const TInstant Now;
        std::shared_ptr<TTTLColumnEngineChanges> Changes;
        std::map<ui64, TDuration> DurationsForced;
        const std::shared_ptr<NDataLocks::TManager> DataLocksManager;

        void AppPortionForEvictionChecker(const TPortionInfo& info) {
            MemoryUsage = MemoryPredictor->AddPortion(info);
            TxWriteVolume += info.GetTxVolume();
        }

        void AppPortionForTtlChecker(const TPortionInfo& info) {
            TxWriteVolume += info.GetTxVolume();
        }

        bool HasLimitsForEviction() const {
            return MemoryUsage < MemoryUsageLimit && TxWriteVolume < TGlobalLimits::TxWriteLimitBytes;
        }

        bool HasLimitsForTtl() const {
            return TxWriteVolume < TGlobalLimits::TxWriteLimitBytes;
        }

        TTieringProcessContext(const ui64 memoryUsageLimit, std::shared_ptr<TTTLColumnEngineChanges> changes,
            const std::shared_ptr<NDataLocks::TManager>& dataLocksManager, const std::shared_ptr<TColumnEngineChanges::IMemoryPredictor>& memoryPredictor);
    };

    TDuration ProcessTiering(const ui64 pathId, const TTiering& tiering, TTieringProcessContext& context) const;
    bool DrainEvictionQueue(std::map<TMonotonic, std::vector<TEvictionsController::TTieringWithPathId>>& evictionsQueue, TTieringProcessContext& context) const;
public:
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

    TColumnEngineForLogs(ui64 tabletId, const TCompactionLimits& limits, const std::shared_ptr<IStoragesManager>& storagesManager);

    virtual void OnTieringModified(const std::shared_ptr<NColumnShard::TTiersManager>& manager, const NColumnShard::TTtl& ttl, const std::optional<ui64> pathId) override;

    virtual std::shared_ptr<TVersionedIndex> CopyVersionedIndexPtr() const override {
        return std::make_shared<TVersionedIndex>(VersionedIndex);
    }

    const TVersionedIndex& GetVersionedIndex() const override {
        return VersionedIndex;
    }

    const TMap<ui64, std::shared_ptr<TColumnEngineStats>>& GetStats() const override;
    const TColumnEngineStats& GetTotalStats() override;
    ui64 MemoryUsage() const override;
    TSnapshot LastUpdate() const override { return LastSnapshot; }

    virtual void DoRegisterTable(const ui64 pathId) override;
public:
    bool Load(IDbWrapper& db) override;

    std::shared_ptr<TInsertColumnEngineChanges> StartInsert(std::vector<TInsertedData>&& dataToIndex) noexcept override;
    std::shared_ptr<TColumnEngineChanges> StartCompaction(const TCompactionLimits& limits, const std::shared_ptr<NDataLocks::TManager>& dataLocksManager) noexcept override;
    std::shared_ptr<TCleanupColumnEngineChanges> StartCleanup(const TSnapshot& snapshot, THashSet<ui64>& pathsToDrop, const std::shared_ptr<NDataLocks::TManager>& dataLocksManager) noexcept override;
    std::shared_ptr<TTTLColumnEngineChanges> StartTtl(const THashMap<ui64, TTiering>& pathEviction,
        const std::shared_ptr<NDataLocks::TManager>& locksManager, const ui64 memoryUsageLimit) noexcept override;

    bool ApplyChanges(IDbWrapper& db, std::shared_ptr<TColumnEngineChanges> indexChanges,
                      const TSnapshot& snapshot) noexcept override;

    void RegisterSchemaVersion(const TSnapshot& snapshot, TIndexInfo&& info) override;

    std::shared_ptr<TSelectInfo> Select(ui64 pathId, TSnapshot snapshot,
                                        const TPKRangesFilter& pkRangesFilter) const override;

    bool IsPortionExists(const ui64 pathId, const ui64 portionId) const {
        auto it = Tables.find(pathId);
        if (it == Tables.end()) {
            return false;
        }
        return !!it->second->GetPortionPtr(portionId);
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
        auto it = Tables.find(pathId);
        if (it == Tables.end()) {
            return nullptr;
        }
        return it->second;
    }

    std::vector<std::shared_ptr<TGranuleMeta>> GetTables(const std::optional<ui64> pathIdFrom, const std::optional<ui64> pathIdTo) const {
        std::vector<std::shared_ptr<TGranuleMeta>> result;
        for (auto&& i : Tables) {
            if (pathIdFrom && i.first < *pathIdFrom) {
                continue;
            }
            if (pathIdTo && i.first > *pathIdTo) {
                continue;
            }
            result.emplace_back(i.second);
        }
        return result;
    }

    ui64 GetTabletId() const {
        return TabletId;
    }

private:
    TVersionedIndex VersionedIndex;
    ui64 TabletId;
    std::shared_ptr<TColumnsTable> ColumnsTable;
    std::shared_ptr<TCountersTable> CountersTable;
    THashMap<ui64, std::shared_ptr<TGranuleMeta>> Tables; // pathId into Granule that equal to Table
    TMap<ui64, std::shared_ptr<TColumnEngineStats>> PathStats; // per path_id stats sorted by path_id
    std::map<TSnapshot, std::vector<TPortionInfo>> CleanupPortions;
    TColumnEngineStats Counters;
    ui64 LastPortion;
    ui64 LastGranule;
    TSnapshot LastSnapshot = TSnapshot::Zero();
    bool Loaded = false;
private:
    bool LoadColumns(IDbWrapper& db);
    bool LoadCounters(IDbWrapper& db);

    void EraseTable(const ui64 pathId);

    void UpsertPortion(const TPortionInfo& portionInfo, const TPortionInfo* exInfo = nullptr);
    bool ErasePortion(const TPortionInfo& portionInfo, bool updateStats = true);
    void UpdatePortionStats(const TPortionInfo& portionInfo, EStatsUpdateType updateType = EStatsUpdateType::DEFAULT,
                            const TPortionInfo* exPortionInfo = nullptr);
    void UpdatePortionStats(TColumnEngineStats& engineStats, const TPortionInfo& portionInfo,
                            EStatsUpdateType updateType,
                            const TPortionInfo* exPortionInfo = nullptr) const;
};

} // namespace NKikimr::NOlap
