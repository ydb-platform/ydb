#pragma once

#include "column_engine.h"
#include "defs.h"

#include "changes/actualization/controller/controller.h"
#include "scheme/tier_info.h"
#include "storage/granule/granule.h"
#include "storage/granule/storage.h"

#include <ydb/core/tx/columnshard/common/limits.h>
#include <ydb/core/tx/columnshard/common/scalars.h>
#include <ydb/core/tx/columnshard/counters/common_data.h>
#include <ydb/core/tx/columnshard/counters/engine_logs.h>
#include <ydb/core/tx/columnshard/data_accessor/manager.h>

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
namespace NEngineLoading {
class TEngineShardingInfoReader;
class TEngineCountersReader;
}

struct TReadMetadata;

/// Engine with 2 tables:
/// - Granules: PK -> granules (use part of PK)
/// - Columns: granule -> blobs
///
/// @note One instance per tablet.
class TColumnEngineForLogs: public IColumnEngine {
    friend class TCompactColumnEngineChanges;
    friend class TTTLColumnEngineChanges;
    friend class TChangesWithAppend;
    friend class TCompactColumnEngineChanges;
    friend class TCleanupPortionsColumnEngineChanges;
    friend class TCleanupTablesColumnEngineChanges;
    friend class NDataSharing::TDestinationSession;
    friend class NEngineLoading::TEngineShardingInfoReader;
    friend class NEngineLoading::TEngineCountersReader;

private:
    bool ActualizationStarted = false;
    const NColumnShard::TEngineLogsCounters SignalCounters;
    std::shared_ptr<TGranulesStorage> GranulesStorage;
    std::shared_ptr<NDataAccessorControl::IDataAccessorsManager> DataAccessorsManager;
    std::shared_ptr<IStoragesManager> StoragesManager;

    std::shared_ptr<NActualizer::TController> ActualizationController;
    std::shared_ptr<TSchemaObjectsCache> SchemaObjectsCache = std::make_shared<TSchemaObjectsCache>();
    TVersionedIndex VersionedIndex;
    std::shared_ptr<TVersionedIndex> VersionedIndexCopy;

public:
    virtual const std::shared_ptr<TVersionedIndex>& GetVersionedIndexReadonlyCopy() override {
        if (!VersionedIndexCopy || !VersionedIndexCopy->IsEqualTo(VersionedIndex)) {
            VersionedIndexCopy = std::make_shared<TVersionedIndex>(VersionedIndex);
        }
        return VersionedIndexCopy;
    }

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

    TColumnEngineForLogs(const ui64 tabletId, const std::shared_ptr<NDataAccessorControl::IDataAccessorsManager>& dataAccessorsManager,
        const std::shared_ptr<IStoragesManager>& storagesManager, const TSnapshot& snapshot, const TSchemaInitializationData& schema);
    TColumnEngineForLogs(const ui64 tabletId, const std::shared_ptr<NDataAccessorControl::IDataAccessorsManager>& dataAccessorsManager,
        const std::shared_ptr<IStoragesManager>& storagesManager, const TSnapshot& snapshot, TIndexInfo&& schema);

    void OnTieringModified(const std::optional<NOlap::TTiering>& ttl, const ui64 pathId) override;
    void OnTieringModified(const THashMap<ui64, NOlap::TTiering>& ttl) override;

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
    void DoFetchDataAccessors(const std::shared_ptr<TDataAccessorsRequest>& request) const override {
        GranulesStorage->FetchDataAccessors(request);
    }

    bool TestingLoadColumns(IDbWrapper& db);
    bool LoadCounters(IDbWrapper& db);

public:
    virtual std::shared_ptr<ITxReader> BuildLoader(const std::shared_ptr<IBlobGroupSelector>& dsGroupSelector) override;
    bool FinishLoading();
    bool StartActualization(const THashMap<ui64, TTiering>& specialPathEviction);

    virtual bool IsOverloadedByMetadata(const ui64 limit) const override {
        return limit < TGranulesStat::GetSumMetadataMemoryPortionsSize();
    }

    virtual std::vector<TCSMetadataRequest> CollectMetadataRequests() const override {
        return GranulesStorage->CollectMetadataRequests();
    }
    std::shared_ptr<TInsertColumnEngineChanges> StartInsert(std::vector<TCommittedData>&& dataToIndex) noexcept override;
    ui64 GetCompactionPriority(const std::shared_ptr<NDataLocks::TManager>& dataLocksManager, const std::set<ui64>& pathIds,
        const std::optional<ui64> waitingPriority) noexcept override;
    std::shared_ptr<TColumnEngineChanges> StartCompaction(const std::shared_ptr<NDataLocks::TManager>& dataLocksManager) noexcept override;
    std::shared_ptr<TCleanupPortionsColumnEngineChanges> StartCleanupPortions(const TSnapshot& snapshot, const THashSet<ui64>& pathsToDrop,
        const std::shared_ptr<NDataLocks::TManager>& dataLocksManager) noexcept override;
    std::shared_ptr<TCleanupTablesColumnEngineChanges> StartCleanupTables(const THashSet<ui64>& pathsToDrop) noexcept override;
    std::vector<std::shared_ptr<TTTLColumnEngineChanges>> StartTtl(const THashMap<ui64, TTiering>& pathEviction,
        const std::shared_ptr<NDataLocks::TManager>& locksManager, const ui64 memoryUsageLimit) noexcept override;

    void ReturnToIndexes(const THashMap<ui64, THashSet<ui64>>& portions) const {
        return GranulesStorage->ReturnToIndexes(portions);
    }
    virtual bool ApplyChangesOnTxCreate(std::shared_ptr<TColumnEngineChanges> indexChanges, const TSnapshot& snapshot) noexcept override;
    virtual bool ApplyChangesOnExecute(
        IDbWrapper& db, std::shared_ptr<TColumnEngineChanges> indexChanges, const TSnapshot& snapshot) noexcept override;

    void RegisterSchemaVersion(const TSnapshot& snapshot, TIndexInfo&& info) override;
    void RegisterSchemaVersion(const TSnapshot& snapshot, const TSchemaInitializationData& schema) override;
    void RegisterOldSchemaVersion(const TSnapshot& snapshot, const TSchemaInitializationData& schema) override;

    std::shared_ptr<TSelectInfo> Select(
        ui64 pathId, TSnapshot snapshot, const TPKRangesFilter& pkRangesFilter, const bool withUncommitted) const override;

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

    TGranuleMeta& MutableGranuleVerified(const ui64 pathId) const {
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

    void AddCleanupPortion(const TPortionInfo::TConstPtr& info) {
        AFL_VERIFY(info->HasRemoveSnapshot());
        CleanupPortions[info->GetRemoveSnapshotVerified().GetPlanInstant()].emplace_back(info);
    }
    void AddShardingInfo(const TGranuleShardingInfo& shardingInfo) {
        VersionedIndex.AddShardingInfo(shardingInfo);
    }

    bool TestingLoad(IDbWrapper& db);

    template <class TModifier>
    void ModifyPortionOnComplete(const TPortionInfo::TConstPtr& portion, const TModifier& modifier) {
        auto exPortion = portion->MakeCopy();
        AFL_VERIFY(portion);
        auto granule = GetGranulePtrVerified(portion->GetPathId());
        granule->ModifyPortionOnComplete(portion, modifier);
        UpdatePortionStats(*portion, EStatsUpdateType::DEFAULT, &exPortion);
    }

    void AppendPortion(const TPortionDataAccessor& portionInfo, const bool addAsAccessor = true);

private:
    ui64 TabletId;
    TMap<ui64, std::shared_ptr<TColumnEngineStats>> PathStats;   // per path_id stats sorted by path_id
    std::map<TInstant, std::vector<TPortionInfo::TConstPtr>> CleanupPortions;
    TColumnEngineStats Counters;
    ui64 LastPortion;
    ui64 LastGranule;
    TSnapshot LastSnapshot = TSnapshot::Zero();
    bool Loaded = false;

private:
    bool ErasePortion(const TPortionInfo& portionInfo, bool updateStats = true);
    void UpdatePortionStats(
        const TPortionInfo& portionInfo, EStatsUpdateType updateType = EStatsUpdateType::DEFAULT, const TPortionInfo* exPortionInfo = nullptr);
    void UpdatePortionStats(TColumnEngineStats& engineStats, const TPortionInfo& portionInfo, EStatsUpdateType updateType,
        const TPortionInfo* exPortionInfo = nullptr) const;
};

}   // namespace NKikimr::NOlap
