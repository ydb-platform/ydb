#include <ydb/core/tx/columnshard/common/path_id.h>
#include "column_engine_logs.h"
#include "filter.h"

#include "changes/actualization/construction/context.h"
#include "changes/cleanup_portions.h"
#include "changes/cleanup_tables.h"
#include "changes/general_compaction.h"
#include "changes/indexation.h"
#include "changes/ttl.h"
#include "loading/stages.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/columnshard/common/limits.h>
#include <ydb/core/tx/columnshard/data_locks/manager/manager.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>
#include <ydb/core/tx/columnshard/tx_reader/composite.h>
#include <ydb/core/tx/tiering/manager.h>

#include <ydb/library/actors/core/monotonic_provider.h>
#include <ydb/library/conclusion/status.h>

#include <library/cpp/time_provider/time_provider.h>

#include <concepts>

namespace NKikimr::NOlap {

TColumnEngineForLogs::TColumnEngineForLogs(const ui64 tabletId, const std::shared_ptr<TSchemaObjectsCache>& schemaCache,
    const std::shared_ptr<NDataAccessorControl::IDataAccessorsManager>& dataAccessorsManager,
    const std::shared_ptr<IStoragesManager>& storagesManager, const TSnapshot& snapshot, const ui64 presetId, const TSchemaInitializationData& schema, const std::shared_ptr<NColumnShard::TPortionIndexStats>& counters)
    : GranulesStorage(std::make_shared<TGranulesStorage>(SignalCounters, dataAccessorsManager, storagesManager))
    , DataAccessorsManager(dataAccessorsManager)
    , StoragesManager(storagesManager)
    , SchemaObjectsCache(schemaCache)
    , TabletId(tabletId)
    , Counters(counters)
    , LastPortion(0)
    , LastGranule(0) {
    AFL_VERIFY(SchemaObjectsCache);
    ActualizationController = std::make_shared<NActualizer::TController>();
    RegisterSchemaVersion(snapshot, presetId, schema);
}

TColumnEngineForLogs::TColumnEngineForLogs(const ui64 tabletId, const std::shared_ptr<TSchemaObjectsCache>& schemaCache,
    const std::shared_ptr<NDataAccessorControl::IDataAccessorsManager>& dataAccessorsManager,
    const std::shared_ptr<IStoragesManager>& storagesManager, const TSnapshot& snapshot, const ui64 presetId, TIndexInfo&& schema, const std::shared_ptr<NColumnShard::TPortionIndexStats>& counters)
    : GranulesStorage(std::make_shared<TGranulesStorage>(SignalCounters, dataAccessorsManager, storagesManager))
    , DataAccessorsManager(dataAccessorsManager)
    , StoragesManager(storagesManager)
    , SchemaObjectsCache(schemaCache)
    , TabletId(tabletId)
    , Counters(counters)
    , LastPortion(0)
    , LastGranule(0) {
    AFL_VERIFY(SchemaObjectsCache);
    ActualizationController = std::make_shared<NActualizer::TController>();
    RegisterSchemaVersion(snapshot, presetId, std::move(schema));
}

void TColumnEngineForLogs::RegisterSchemaVersion(const TSnapshot& snapshot, const ui64 presetId, TIndexInfo&& indexInfo) {
    AFL_VERIFY(DataAccessorsManager);
    bool switchOptimizer = false;
    bool switchAccessorsManager = false;
    if (!VersionedIndex.IsEmpty()) {
        const NOlap::TIndexInfo& lastIndexInfo = VersionedIndex.GetLastSchema()->GetIndexInfo();
        lastIndexInfo.CheckCompatible(indexInfo).Validate();
        switchOptimizer = !indexInfo.GetCompactionPlannerConstructor()->IsEqualTo(lastIndexInfo.GetCompactionPlannerConstructor());
        switchAccessorsManager = !indexInfo.GetMetadataManagerConstructor()->IsEqualTo(*lastIndexInfo.GetMetadataManagerConstructor());
    }

    const bool isCriticalScheme = indexInfo.GetSchemeNeedActualization();
    auto* indexInfoActual = VersionedIndex.AddIndex(snapshot, SchemaObjectsCache->UpsertIndexInfo(presetId, std::move(indexInfo)));
    if (isCriticalScheme) {
        StartActualization({});
        for (auto&& i : GranulesStorage->GetTables()) {
            i.second->RefreshScheme();
        }
    }
    if (switchAccessorsManager) {
        NDataAccessorControl::TManagerConstructionContext context(DataAccessorsManager->GetTabletActorId(), true);
        for (auto&& i : GranulesStorage->GetTables()) {
            i.second->ResetAccessorsManager(indexInfoActual->GetMetadataManagerConstructor(), context);
        }
    }
    if (switchOptimizer) {
        for (auto&& i : GranulesStorage->GetTables()) {
            i.second->ResetOptimizer(indexInfoActual->GetCompactionPlannerConstructor(), StoragesManager, indexInfoActual->GetPrimaryKey());
        }
    }
}

void TColumnEngineForLogs::RegisterSchemaVersion(const TSnapshot& snapshot, const ui64 presetId, const TSchemaInitializationData& schema) {
    AFL_VERIFY(VersionedIndex.IsEmpty() || schema.GetVersion() >= VersionedIndex.GetLastSchema()->GetVersion())("empty", VersionedIndex.IsEmpty())("current", schema.GetVersion())(
                                            "last", VersionedIndex.GetLastSchema()->GetVersion());

    std::optional<NOlap::TIndexInfo> indexInfoOptional;
    if (schema.GetDiff()) {
        AFL_VERIFY(!VersionedIndex.IsEmpty());

        indexInfoOptional = NOlap::TIndexInfo::BuildFromProto(
            *schema.GetDiff(), VersionedIndex.GetLastSchema()->GetIndexInfo(), StoragesManager, SchemaObjectsCache);
    } else {
        indexInfoOptional = NOlap::TIndexInfo::BuildFromProto(schema.GetSchemaVerified(), StoragesManager, SchemaObjectsCache);
    }
    AFL_VERIFY(indexInfoOptional);
    RegisterSchemaVersion(snapshot, presetId, std::move(*indexInfoOptional));
}

void TColumnEngineForLogs::RegisterOldSchemaVersion(const TSnapshot& snapshot, const ui64 presetId, const TSchemaInitializationData& schema) {
    AFL_VERIFY(!VersionedIndex.IsEmpty());

    ui64 version = schema.GetVersion();

    ISnapshotSchema::TPtr prevSchema = VersionedIndex.GetLastSchemaBeforeOrEqualSnapshotOptional(version);

    if (prevSchema && version == prevSchema->GetVersion()) {
        // skip already registered version
        return;
    }

    ISnapshotSchema::TPtr secondLast =
        VersionedIndex.GetLastSchemaBeforeOrEqualSnapshotOptional(VersionedIndex.GetLastSchema()->GetVersion() - 1);

    AFL_VERIFY(!secondLast || secondLast->GetVersion() <= version)("reason", "incorrect schema registration order");

    std::optional<NOlap::TIndexInfo> indexInfoOptional;
    if (schema.GetDiff()) {
        AFL_VERIFY(prevSchema)("reason", "no base schema to apply diff for");

        indexInfoOptional =
            NOlap::TIndexInfo::BuildFromProto(*schema.GetDiff(), prevSchema->GetIndexInfo(), StoragesManager, SchemaObjectsCache);
    } else {
        indexInfoOptional = NOlap::TIndexInfo::BuildFromProto(schema.GetSchemaVerified(), StoragesManager, SchemaObjectsCache);
    }

    AFL_VERIFY(indexInfoOptional);
    VersionedIndex.AddIndex(snapshot, SchemaObjectsCache->UpsertIndexInfo(presetId, std::move(*indexInfoOptional)));
}

std::shared_ptr<ITxReader> TColumnEngineForLogs::BuildLoader(const std::shared_ptr<IBlobGroupSelector>& dsGroupSelector) {
    auto result = std::make_shared<TTxCompositeReader>("column_engines");
    result->AddChildren(std::make_shared<NEngineLoading::TEngineCountersReader>("counters", this, dsGroupSelector));
    result->AddChildren(std::make_shared<NEngineLoading::TEngineShardingInfoReader>("sharding_info", this, dsGroupSelector));
    if (GranulesStorage->GetTables().size()) {
        auto granules = std::make_shared<TTxCompositeReader>("granules");
        for (auto&& i : GranulesStorage->GetTables()) {
            granules->AddChildren(i.second->BuildLoader(dsGroupSelector, VersionedIndex));
        }
        result->AddChildren(granules);
    }
    result->AddChildren(std::make_shared<NEngineLoading::TEngineLoadingFinish>("finish", this));
    return result;
}

bool TColumnEngineForLogs::FinishLoading() {
    for (const auto& [pathId, spg] : GranulesStorage->GetTables()) {
        for (const auto& [_, portionInfo] : spg->GetPortions()) {
            Counters->AddPortion(*portionInfo);
            if (portionInfo->CheckForCleanup()) {
                AddCleanupPortion(portionInfo);
            }
        }
    }

    Y_ABORT_UNLESS(!(LastPortion >> 63), "near to int overflow");
    Y_ABORT_UNLESS(!(LastGranule >> 63), "near to int overflow");
    return true;
}

std::shared_ptr<TInsertColumnEngineChanges> TColumnEngineForLogs::StartInsert(std::vector<TCommittedData>&& dataToIndex) noexcept {
    Y_ABORT_UNLESS(dataToIndex.size());

    TSaverContext saverContext(StoragesManager);
    auto changes = std::make_shared<TInsertColumnEngineChanges>(std::move(dataToIndex), saverContext);
    auto pkSchema = VersionedIndex.GetLastSchema()->GetIndexInfo().GetReplaceKey();

    for (const auto& data : changes->GetDataToIndex()) {
        const TInternalPathId pathId = data.GetPathId();

        if (changes->PathToGranule.contains(pathId)) {
            continue;
        }
        if (!data.GetRemove()) {
            AFL_VERIFY(changes->PathToGranule.emplace(pathId, GetGranulePtrVerified(pathId)->GetBucketPositions()).second);
        }
    }

    return changes;
}

ui64 TColumnEngineForLogs::GetCompactionPriority(const std::shared_ptr<NDataLocks::TManager>& dataLocksManager, const std::set<TInternalPathId>& pathIds,
    const std::optional<ui64> waitingPriority) const noexcept {
    auto priority = GranulesStorage->GetCompactionPriority(dataLocksManager, pathIds, waitingPriority);
    if (!priority) {
        return 0;
    } else {
        return priority->GetGeneralPriority();
    }
}

std::shared_ptr<TColumnEngineChanges> TColumnEngineForLogs::StartCompaction(
    const std::shared_ptr<NDataLocks::TManager>& dataLocksManager) noexcept {
    AFL_VERIFY(dataLocksManager);
    auto granule = GranulesStorage->GetGranuleForCompaction(dataLocksManager);
    if (!granule) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "no granules for start compaction");
        return nullptr;
    }
    granule->OnStartCompaction();
    auto changes = granule->GetOptimizationTask(granule, dataLocksManager);
    if (!changes) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "cannot build optimization task for granule that need compaction")(
            "weight", granule->GetCompactionPriority().DebugString());
    }
    return changes;
}

std::shared_ptr<TCleanupTablesColumnEngineChanges> TColumnEngineForLogs::StartCleanupTables(const THashSet<TInternalPathId>& pathsToDrop) noexcept {
    if (pathsToDrop.empty()) {
        return nullptr;
    }
    auto changes = std::make_shared<TCleanupTablesColumnEngineChanges>(StoragesManager);

    ui64 txSize = 0;
    const ui64 txSizeLimit = TGlobalLimits::TxWriteLimitBytes / 4;
    for (TInternalPathId pathId : pathsToDrop) {
        if (!HasDataInPathId(pathId)) {
            changes->TablesToDrop.emplace(pathId);
        }
        txSize += 256;
        if (txSize > txSizeLimit) {
            break;
        }
    }
    if (changes->TablesToDrop.empty()) {
        return nullptr;
    }
    return changes;
}

std::shared_ptr<TCleanupPortionsColumnEngineChanges> TColumnEngineForLogs::StartCleanupPortions(
    const TSnapshot& snapshot, const THashSet<TInternalPathId>& pathsToDrop, const std::shared_ptr<NDataLocks::TManager>& dataLocksManager) noexcept {
    AFL_VERIFY(dataLocksManager);
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "StartCleanup")("portions_count", CleanupPortions.size());
    std::shared_ptr<TCleanupPortionsColumnEngineChanges> changes = std::make_shared<TCleanupPortionsColumnEngineChanges>(StoragesManager);
    // Add all portions from dropped paths
    ui64 portionsCount = 0;
    ui64 chunksCount = 0;
    ui32 skipLocked = 0;
    ui32 portionsFromDrop = 0;
    bool limitExceeded = false;
    const ui32 maxChunksCount = 500000;
    const ui32 maxPortionsCount = 1000;
    for (TInternalPathId pathId : pathsToDrop) {
        auto g = GranulesStorage->GetGranuleOptional(pathId);
        if (!g) {
            continue;
        }
        if (dataLocksManager->IsLocked(*g, NDataLocks::ELockCategory::Tables)) {
            continue;
        }
        for (auto& [portion, info] : g->GetPortions()) {
            if (info->CheckForCleanup()) {
                continue;
            }
            if (dataLocksManager->IsLocked(*info, NDataLocks::ELockCategory::Cleanup)) {
                ++skipLocked;
                continue;
            }
            ++portionsCount;
            chunksCount += info->GetApproxChunksCount(info->GetSchema(VersionedIndex)->GetColumnsCount());
            if ((portionsCount < maxPortionsCount && chunksCount < maxChunksCount) || changes->GetPortionsToDrop().empty()) {
            } else {
                limitExceeded = true;
                break;
            }
            changes->AddPortionToRemove(info);
            ++portionsFromDrop;
        }
        changes->AddTableToDrop(pathId);
    }

    const TInstant snapshotInstant = snapshot.GetPlanInstant();
    for (auto it = CleanupPortions.begin(); !limitExceeded && it != CleanupPortions.end();) {
        if (it->first > snapshotInstant) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "StartCleanupStop")("snapshot", snapshot.DebugString())(
                "current_snapshot_ts", it->first.MilliSeconds());
            break;
        }
        for (ui32 i = 0; i < it->second.size();) {
            if (dataLocksManager->IsLocked(it->second[i], NDataLocks::ELockCategory::Cleanup)) {
                ++skipLocked;
                ++i;
                continue;
            }
            AFL_VERIFY(it->second[i]->CheckForCleanup(snapshot))("p_snapshot", it->second[i]->GetRemoveSnapshotOptional())("snapshot", snapshot);
            ++portionsCount;
            chunksCount += it->second[i]->GetApproxChunksCount(it->second[i]->GetSchema(VersionedIndex)->GetColumnsCount());
            if ((portionsCount < maxPortionsCount && chunksCount < maxChunksCount) || changes->GetPortionsToDrop().empty()) {
            } else {
                limitExceeded = true;
                break;
            }
            changes->AddPortionToDrop(it->second[i]);
            if (i + 1 < it->second.size()) {
                it->second[i] = std::move(it->second.back());
            }
            it->second.pop_back();
        }
        if (limitExceeded) {
            break;
        }
        if (it->second.empty()) {
            it = CleanupPortions.erase(it);
        } else {
            ++it;
        }
    }
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "StartCleanup")("portions_count", CleanupPortions.size())(
        "portions_prepared", changes->GetPortionsToAccess().size())("drop", portionsFromDrop)("skip", skipLocked)("portions_counter", portionsCount)(
        "chunks", chunksCount)("limit", limitExceeded)("max_portions", maxPortionsCount)("max_chunks", maxChunksCount);

    if (changes->GetPortionsToAccess().empty()) {
        return nullptr;
    }

    return changes;
}

std::vector<std::shared_ptr<TTTLColumnEngineChanges>> TColumnEngineForLogs::StartTtl(const THashMap<TInternalPathId, TTiering>& pathEviction,
    const std::shared_ptr<NDataLocks::TManager>& dataLocksManager, const ui64 memoryUsageLimit) noexcept {
    AFL_VERIFY(dataLocksManager);
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_ACTUALIZATION)("event", "StartTtl")("external", pathEviction.size());

    TSaverContext saverContext(StoragesManager);
    NActualizer::TTieringProcessContext context(memoryUsageLimit, saverContext, dataLocksManager, VersionedIndex, SignalCounters, ActualizationController);
    const TDuration actualizationLag = NYDBTest::TControllers::GetColumnShardController()->GetActualizationTasksLag();
    for (auto&& i : pathEviction) {
        auto g = GetGranuleOptional(i.first);
        if (g) {
            if (!ActualizationStarted) {
                g->StartActualizationIndex();
            }
            g->RefreshTiering(i.second);
            context.ResetActualInstantForTest();
            g->BuildActualizationTasks(context, actualizationLag);
        }
    }

    if (ActualizationStarted) {
        TLogContextGuard lGuard(TLogContextBuilder::Build()("queue", "ttl")("external_count", pathEviction.size()));
        for (auto&& i : GranulesStorage->GetTables()) {
            if (pathEviction.contains(i.first)) {
                continue;
            }
            i.second->BuildActualizationTasks(context, actualizationLag);
        }
    } else {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD_ACTUALIZATION)("event", "StartTtl")("skip", "not_ready_tiers");
    }
    std::vector<std::shared_ptr<TTTLColumnEngineChanges>> result;
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_ACTUALIZATION)("event", "StartTtl")("rw_tasks_count", context.GetTasks().size());
    for (auto&& i : context.GetTasks()) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_ACTUALIZATION)("event", "StartTtl")("rw", i.first.DebugString())("count", i.second.size());
        for (auto&& t : i.second) {
            SignalCounters.OnActualizationTask(t.GetTask()->GetPortionsToEvictCount(), t.GetTask()->GetPortionsToRemove().GetSize());
            result.emplace_back(t.GetTask());
        }
    }
    return result;
}

bool TColumnEngineForLogs::ApplyChangesOnTxCreate(std::shared_ptr<TColumnEngineChanges> indexChanges, const TSnapshot& snapshot) noexcept {
    TFinalizationContext context(LastGranule, LastPortion, snapshot);
    indexChanges->Compile(context);
    return true;
}

bool TColumnEngineForLogs::ApplyChangesOnExecute(
    IDbWrapper& db, std::shared_ptr<TColumnEngineChanges> /*indexChanges*/, const TSnapshot& snapshot) noexcept {
    db.WriteCounter(LAST_PORTION, LastPortion);
    db.WriteCounter(LAST_GRANULE, LastGranule);

    if (LastSnapshot < snapshot) {
        LastSnapshot = snapshot;
        db.WriteCounter(LAST_PLAN_STEP, LastSnapshot.GetPlanStep());
        db.WriteCounter(LAST_TX_ID, LastSnapshot.GetTxId());
    }
    return true;
}

void TColumnEngineForLogs::AppendPortion(const std::shared_ptr<TPortionInfo>& portionInfo) {
    AFL_VERIFY(portionInfo);
    auto granule = GetGranulePtrVerified(portionInfo->GetPathId());
    AFL_VERIFY(!granule->GetPortionOptional(portionInfo->GetPortionId()));
    Counters->AddPortion(*portionInfo);
    granule->AppendPortion(portionInfo);
    if (portionInfo->HasRemoveSnapshot()) {
        AddCleanupPortion(portionInfo);
    }
}

void TColumnEngineForLogs::AppendPortion(const TPortionDataAccessor& portionInfo) {
    auto granule = GetGranulePtrVerified(portionInfo.GetPortionInfo().GetPathId());
    AFL_VERIFY(!granule->GetPortionOptional(portionInfo.GetPortionInfo().GetPortionId()));
    Counters->AddPortion(portionInfo.GetPortionInfo());
    granule->AppendPortion(portionInfo);
    if (portionInfo.GetPortionInfo().HasRemoveSnapshot()) {
        AddCleanupPortion(portionInfo.GetPortionInfoPtr());
    }
}

bool TColumnEngineForLogs::ErasePortion(const TPortionInfo& portionInfo, bool updateStats) {
    const ui64 portion = portionInfo.GetPortionId();
    auto& spg = MutableGranuleVerified(portionInfo.GetPathId());
    auto p = spg.GetPortionOptional(portion);

    if (!p) {
        LOG_S_WARN("Portion erased already " << portionInfo << " at tablet " << TabletId);
        return false;
    } else {
        if (updateStats) {
            Counters->RemovePortion(*p);
        }
        Y_ABORT_UNLESS(spg.ErasePortion(portion));
        return true;
    }
}

std::shared_ptr<TSelectInfo> TColumnEngineForLogs::Select(
    TInternalPathId pathId, TSnapshot snapshot, const TPKRangesFilter& pkRangesFilter, const bool withUncommitted) const {
    auto out = std::make_shared<TSelectInfo>();
    auto spg = GranulesStorage->GetGranuleOptional(pathId);
    if (!spg) {
        return out;
    }

    for (const auto& [_, portionInfo] : spg->GetInsertedPortions()) {
        if (!portionInfo->IsVisible(snapshot, !withUncommitted)) {
            continue;
        }
        const bool skipPortion = !pkRangesFilter.IsUsed(*portionInfo);
        AFL_TRACE(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", skipPortion ? "portion_skipped" : "portion_selected")("pathId", pathId)(
            "portion", portionInfo->DebugString());
        if (skipPortion) {
            continue;
        }
        out->Portions.emplace_back(portionInfo);
    }
    for (const auto& [_, portionInfo] : spg->GetPortions()) {
        if (!portionInfo->IsVisible(snapshot, !withUncommitted)) {
            continue;
        }
        const bool skipPortion = !pkRangesFilter.IsUsed(*portionInfo);
        AFL_TRACE(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", skipPortion ? "portion_skipped" : "portion_selected")("pathId", pathId)(
            "portion", portionInfo->DebugString());
        if (skipPortion) {
            continue;
        }
        out->Portions.emplace_back(portionInfo);
    }

    return out;
}

bool TColumnEngineForLogs::StartActualization(const THashMap<TInternalPathId, TTiering>& specialPathEviction) {
    if (ActualizationStarted) {
        return false;
    }
    for (auto&& i : GranulesStorage->GetTables()) {
        i.second->StartActualizationIndex();
    }
    for (auto&& i : specialPathEviction) {
        auto g = GetGranuleOptional(i.first);
        if (g) {
            g->RefreshTiering(i.second);
        }
    }
    ActualizationStarted = true;
    return true;
}
void TColumnEngineForLogs::OnTieringModified(const std::optional<NOlap::TTiering>& ttl, const TInternalPathId pathId) {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "OnTieringModified")("path_id", pathId);
    StartActualization({});

    auto g = GetGranulePtrVerified(pathId);
    g->RefreshTiering(ttl);
}

void TColumnEngineForLogs::OnTieringModified(const THashMap<TInternalPathId, NOlap::TTiering>& ttl) {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "OnTieringModified")("new_count_tierings", ttl.size());
    StartActualization({});

    for (auto&& [gPathId, g] : GranulesStorage->GetTables()) {
        auto it = ttl.find(gPathId);
        if (it == ttl.end()) {
            g->RefreshTiering({});
        } else {
            g->RefreshTiering(it->second);
        }
    }
}

void TColumnEngineForLogs::DoRegisterTable(const TInternalPathId pathId) {
    std::shared_ptr<TGranuleMeta> g = GranulesStorage->RegisterTable(pathId, SignalCounters.RegisterGranuleDataCounters(), VersionedIndex);
    if (ActualizationStarted) {
        g->StartActualizationIndex();
        g->RefreshScheme();
    }
}

bool TColumnEngineForLogs::TestingLoad(IDbWrapper& db) {
    {
        TMemoryProfileGuard g("TTxInit/LoadShardingInfo");
        if (!VersionedIndex.LoadShardingInfo(db)) {
            return false;
        }
    }

    {
        auto guard = GranulesStorage->GetStats()->StartPackModification();
        for (auto&& [_, i] : GranulesStorage->GetTables()) {
            i->TestingLoad(db, VersionedIndex);
        }
        if (!LoadCounters(db)) {
            return false;
        }
        FinishLoading();
    }
    return true;
}

bool TColumnEngineForLogs::LoadCounters(IDbWrapper& db) {
    auto callback = [&](ui32 id, ui64 value) {
        switch (id) {
            case LAST_PORTION:
                LastPortion = value;
                break;
            case LAST_GRANULE:
                LastGranule = value;
                break;
            case LAST_PLAN_STEP:
                LastSnapshot = TSnapshot(value, LastSnapshot.GetTxId());
                break;
            case LAST_TX_ID:
                LastSnapshot = TSnapshot(LastSnapshot.GetPlanStep(), value);
                break;
        }
    };

    return db.LoadCounters(callback);
}

}   // namespace NKikimr::NOlap
