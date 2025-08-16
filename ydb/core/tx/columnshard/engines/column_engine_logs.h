#pragma once

#include "column_engine.h"
#include "defs.h"

#include "changes/actualization/controller/controller.h"
#include "scheme/tier_info.h"
#include "scheme/versions/preset_schemas.h"
#include "storage/granule/granule.h"
#include "storage/granule/storage.h"

#include <ydb/core/tx/columnshard/common/limits.h>
#include <ydb/core/tx/columnshard/common/path_id.h>
#include <ydb/core/tx/columnshard/common/scalars.h>
#include <ydb/core/tx/columnshard/counters/common_data.h>
#include <ydb/core/tx/columnshard/counters/engine_logs.h>
#include <ydb/core/tx/columnshard/counters/portion_index.h>
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
class TRemovePortionsChange;

namespace NDataSharing {
class TDestinationSession;
}
namespace NEngineLoading {
class TEngineShardingInfoReader;
class TEngineCountersReader;
}   // namespace NEngineLoading

struct TReadMetadata;

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
    friend class TRemovePortionsChange;

private:
    bool ActualizationStarted = false;
    const NColumnShard::TEngineLogsCounters SignalCounters;
    std::shared_ptr<TGranulesStorage> GranulesStorage;
    std::shared_ptr<NDataAccessorControl::IDataAccessorsManager> DataAccessorsManager;
    std::shared_ptr<IStoragesManager> StoragesManager;

    std::shared_ptr<NActualizer::TController> ActualizationController;
    std::shared_ptr<TSchemaObjectsCache> SchemaObjectsCache;
    TVersionedPresetSchemas VersionedSchemas;

public:

    const TVersionedPresetSchemas& GetVersionedSchemas() const {
        return VersionedSchemas;
    }

    TVersionedPresetSchemas& MutableVersionedSchemas() {
        return VersionedSchemas;
    }

    virtual const std::shared_ptr<const TVersionedIndex>& GetVersionedIndexReadonlyCopy() override {
        return VersionedSchemas.GetDefaultVersionedIndexCopy();
    }

    TVersionedIndex& MutableVersionedIndex() {
        return VersionedSchemas.MutableDefaultVersionedIndex();
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
        SUB = 0,
        ADD,
    };

    TColumnEngineForLogs(const ui64 tabletId, const std::shared_ptr<TSchemaObjectsCache>& schemaCache,
        const std::shared_ptr<NDataAccessorControl::IDataAccessorsManager>& dataAccessorsManager,
        const std::shared_ptr<IStoragesManager>& storagesManager, const TSnapshot& snapshot, const ui64 presetId,
        const TSchemaInitializationData& schema, const std::shared_ptr<NColumnShard::TPortionIndexStats>& counters);
    TColumnEngineForLogs(const ui64 tabletId, const std::shared_ptr<TSchemaObjectsCache>& schemaCache,
        const std::shared_ptr<NDataAccessorControl::IDataAccessorsManager>& dataAccessorsManager,
        const std::shared_ptr<IStoragesManager>& storagesManager, const TSnapshot& snapshot, TIndexInfo&& schema,
        const std::shared_ptr<NColumnShard::TPortionIndexStats>& counters);

    void OnTieringModified(const std::optional<NOlap::TTiering>& ttl, const TInternalPathId pathId) override;
    void OnTieringModified(const THashMap<TInternalPathId, NOlap::TTiering>& ttl) override;

    virtual const TVersionedIndex& GetVersionedIndex() const override {
        return VersionedSchemas.GetDefaultVersionedIndex();
    }

    TSnapshot LastUpdate() const override {
        return LastSnapshot;
    }

    virtual void DoRegisterTable(const TInternalPathId pathId) override;
    void DoFetchDataAccessors(const std::shared_ptr<TDataAccessorsRequest>& request) const override {
        GranulesStorage->FetchDataAccessors(request);
    }

    bool TestingLoadColumns(IDbWrapper& db);
    bool LoadCounters(IDbWrapper& db);

public:
    virtual std::shared_ptr<ITxReader> BuildLoader(const std::shared_ptr<IBlobGroupSelector>& dsGroupSelector) override;
    bool FinishLoading();
    bool StartActualization(const THashMap<TInternalPathId, TTiering>& specialPathEviction);

    virtual bool IsOverloadedByMetadata(const ui64 limit) const override {
        return limit < TGranulesStat::GetSumMetadataMemoryPortionsSize();
    }

    virtual std::vector<TCSMetadataRequest> CollectMetadataRequests() const override {
        return GranulesStorage->CollectMetadataRequests();
    }
    ui64 GetCompactionPriority(const std::shared_ptr<NDataLocks::TManager>& dataLocksManager, const std::set<TInternalPathId>& pathIds,
        const std::optional<ui64> waitingPriority) const noexcept override;
    std::shared_ptr<TColumnEngineChanges> StartCompaction(const std::shared_ptr<NDataLocks::TManager>& dataLocksManager) noexcept override;
    std::shared_ptr<TCleanupPortionsColumnEngineChanges> StartCleanupPortions(const TSnapshot& snapshot,
        const THashSet<TInternalPathId>& pathsToDrop, const std::shared_ptr<NDataLocks::TManager>& dataLocksManager) noexcept override;
    std::shared_ptr<TCleanupTablesColumnEngineChanges> StartCleanupTables(const THashSet<TInternalPathId>& pathsToDrop) noexcept override;
    std::vector<std::shared_ptr<TTTLColumnEngineChanges>> StartTtl(const THashMap<TInternalPathId, TTiering>& pathEviction,
        const std::shared_ptr<NDataLocks::TManager>& locksManager, const ui64 memoryUsageLimit) noexcept override;

    void ReturnToIndexes(const THashMap<TInternalPathId, THashSet<ui64>>& portions) const {
        return GranulesStorage->ReturnToIndexes(portions);
    }
    virtual bool ApplyChangesOnTxCreate(std::shared_ptr<TColumnEngineChanges> indexChanges, const TSnapshot& snapshot) noexcept override;
    virtual bool ApplyChangesOnExecute(
        IDbWrapper& db, std::shared_ptr<TColumnEngineChanges> indexChanges, const TSnapshot& snapshot) noexcept override;

    void RegisterSchemaVersion(const TSnapshot& snapshot, TIndexInfo&& info) override;
    void RegisterSchemaVersion(const TSnapshot& snapshot, const ui64 presetId, const TSchemaInitializationData& schema) override;
    void RegisterOldSchemaVersion(const TSnapshot& snapshot, const ui64 presetId, const TSchemaInitializationData& schema) override;

    std::vector<std::shared_ptr<TPortionInfo>> Select(
        TInternalPathId pathId, TSnapshot snapshot, const TPKRangesFilter& pkRangesFilter, const bool withUncommitted) const override;

    bool IsPortionExists(const TInternalPathId pathId, const ui64 portionId) const {
        return !!GranulesStorage->GetPortionOptional(pathId, portionId);
    }

    virtual bool ErasePathId(const TInternalPathId pathId) override {
        if (HasDataInPathId(pathId)) {
            return false;
        }
        return GranulesStorage->EraseTable(pathId);
    }

    virtual bool HasDataInPathId(const TInternalPathId pathId) const override {
        auto g = GetGranuleOptional(pathId);
        return g && g->GetPortions().size();
    }

    bool IsGranuleExists(const TInternalPathId pathId) const {
        return !!GetGranuleOptional(pathId);
    }

    const TGranuleMeta& GetGranuleVerified(const TInternalPathId pathId) const {
        return *GetGranulePtrVerified(pathId);
    }

    TGranuleMeta& MutableGranuleVerified(const TInternalPathId pathId) const {
        return *GetGranulePtrVerified(pathId);
    }

    bool HasDataWithSchemaVersion(const ui64 fromVersion, const ui64 version) const {
        return GranulesStorage->GetStats()->HasSchemaVersion(fromVersion, version);
    }

    std::shared_ptr<TGranuleMeta> GetGranulePtrVerified(const TInternalPathId pathId) const {
        auto result = GetGranuleOptional(pathId);
        AFL_VERIFY(result)("path_id", pathId);
        return result;
    }

    std::shared_ptr<TGranuleMeta> GetGranuleOptional(const TInternalPathId pathId) const {
        return GranulesStorage->GetGranuleOptional(pathId);
    }

    const THashMap<NColumnShard::TInternalPathId, std::shared_ptr<TGranuleMeta>>& GetTables() const {
        return GranulesStorage->GetTables();
    }

    ui64 GetTabletId() const {
        return TabletId;
    }

    void AddCleanupPortion(const TPortionInfo::TConstPtr& info) {
        AFL_VERIFY(info->HasRemoveSnapshot());
        CleanupPortions[info->GetRemoveSnapshotVerified().GetPlanInstant()].emplace_back(info);
    }
    void AddShardingInfo(const TGranuleShardingInfo& shardingInfo) {
        VersionedSchemas.MutableDefaultVersionedIndex().AddShardingInfo(shardingInfo);
    }

    bool TestingLoad(IDbWrapper& db);

    template <class TModifier>
    void ModifyPortionOnComplete(const TPortionInfo::TConstPtr& portion, const TModifier& modifier) {
        auto exPortion = portion->MakeCopy();
        AFL_VERIFY(portion);
        auto granule = GetGranulePtrVerified(portion->GetPathId());
        granule->ModifyPortionOnComplete(portion, modifier);
        Counters->AddPortion(*portion);
        Counters->RemovePortion(*exPortion);
    }

    void AppendPortion(const std::shared_ptr<TPortionDataAccessor>& portionInfo);
    void AppendPortion(const std::shared_ptr<TPortionInfo>& portionInfo);

private:
    ui64 TabletId;
    std::map<TInstant, std::vector<TPortionInfo::TConstPtr>> CleanupPortions;
    std::shared_ptr<NColumnShard::TPortionIndexStats> Counters;
    ui64 LastPortion;
    ui64 LastGranule;
    TSnapshot LastSnapshot = TSnapshot::Zero();
    bool Loaded = false;

private:
    bool ErasePortion(const TPortionInfo& portionInfo, bool updateStats = true);
};

}   // namespace NKikimr::NOlap
