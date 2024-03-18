#include "column_engine_logs.h"
#include "filter.h"

#include "changes/actualization/construction/context.h"
#include "changes/indexation.h"
#include "changes/general_compaction.h"
#include "changes/cleanup_portions.h"
#include "changes/cleanup_tables.h"
#include "changes/ttl.h"

#include <ydb/core/tx/columnshard/common/limits.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>
#include <ydb/core/tx/columnshard/columnshard_ttl.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/columnshard/data_locks/manager/manager.h>
#include <ydb/core/tx/tiering/manager.h>

#include <ydb/core/formats/arrow/one_batch_input_stream.h>
#include <ydb/core/formats/arrow/merging_sorted_input_stream.h>

#include <ydb/library/conclusion/status.h>

#include <library/cpp/time_provider/time_provider.h>
#include <ydb/library/actors/core/monotonic_provider.h>

#include <concepts>

namespace NKikimr::NOlap {

TColumnEngineForLogs::TColumnEngineForLogs(ui64 tabletId, const TCompactionLimits& limits, const std::shared_ptr<IStoragesManager>& storagesManager,
    const TSnapshot& snapshot, const NKikimrSchemeOp::TColumnTableSchema& schema)
    : GranulesStorage(std::make_shared<TGranulesStorage>(SignalCounters, limits, storagesManager))
    , StoragesManager(storagesManager)
    , TabletId(tabletId)
    , LastPortion(0)
    , LastGranule(0)
{
    ActualizationController = std::make_shared<NActualizer::TController>();
    RegisterSchemaVersion(snapshot, schema);
}

TColumnEngineForLogs::TColumnEngineForLogs(ui64 tabletId, const TCompactionLimits& limits, const std::shared_ptr<IStoragesManager>& storagesManager,
    const TSnapshot& snapshot, TIndexInfo&& schema)
    : GranulesStorage(std::make_shared<TGranulesStorage>(SignalCounters, limits, storagesManager))
    , StoragesManager(storagesManager)
    , TabletId(tabletId)
    , LastPortion(0)
    , LastGranule(0) {
    ActualizationController = std::make_shared<NActualizer::TController>();
    RegisterSchemaVersion(snapshot, std::move(schema));
}

ui64 TColumnEngineForLogs::MemoryUsage() const {
    auto numPortions = Counters.GetPortionsCount();

    return Counters.Tables * (sizeof(TGranuleMeta) + sizeof(ui64)) +
        numPortions * (sizeof(TPortionInfo) + sizeof(ui64)) +
        Counters.ColumnRecords * sizeof(TColumnRecord) +
        Counters.ColumnMetadataBytes;
}

const TMap<ui64, std::shared_ptr<TColumnEngineStats>>& TColumnEngineForLogs::GetStats() const {
    return PathStats;
}

const TColumnEngineStats& TColumnEngineForLogs::GetTotalStats() {
    Counters.Tables = Tables.size();

    return Counters;
}

void TColumnEngineForLogs::UpdatePortionStats(const TPortionInfo& portionInfo, EStatsUpdateType updateType,
                                            const TPortionInfo* exPortionInfo) {
    UpdatePortionStats(Counters, portionInfo, updateType, exPortionInfo);

    const ui64 pathId = portionInfo.GetPathId();
    Y_ABORT_UNLESS(pathId);
    if (!PathStats.contains(pathId)) {
        auto& stats = PathStats[pathId];
        stats = std::make_shared<TColumnEngineStats>();
        stats->Tables = 1;
    }
    UpdatePortionStats(*PathStats[pathId], portionInfo, updateType, exPortionInfo);
}

TColumnEngineStats::TPortionsStats DeltaStats(const TPortionInfo& portionInfo, ui64& metadataBytes) {
    TColumnEngineStats::TPortionsStats deltaStats;
    deltaStats.Bytes = 0;
    for (auto& rec : portionInfo.Records) {
        metadataBytes += rec.GetMeta().GetMetadataSize();
        deltaStats.Bytes += rec.GetBlobRange().GetSize();
        deltaStats.BytesByColumn[rec.ColumnId] += rec.BlobRange.Size;
        deltaStats.RawBytesByColumn[rec.ColumnId] += rec.GetMeta().GetRawBytes().value_or(0);
    }
    deltaStats.Rows = portionInfo.NumRows();
    deltaStats.RawBytes = portionInfo.RawBytesSum();
    deltaStats.Blobs = portionInfo.GetBlobIdsCount();
    deltaStats.Portions = 1;
    return deltaStats;
}

void TColumnEngineForLogs::UpdatePortionStats(TColumnEngineStats& engineStats, const TPortionInfo& portionInfo,
                                              EStatsUpdateType updateType,
                                              const TPortionInfo* exPortionInfo) const {
    ui64 columnRecords = portionInfo.Records.size();
    ui64 metadataBytes = 0;
    TColumnEngineStats::TPortionsStats deltaStats = DeltaStats(portionInfo, metadataBytes);

    Y_ABORT_UNLESS(!exPortionInfo || exPortionInfo->GetMeta().Produced != TPortionMeta::EProduced::UNSPECIFIED);
    Y_ABORT_UNLESS(portionInfo.GetMeta().Produced != TPortionMeta::EProduced::UNSPECIFIED);

    TColumnEngineStats::TPortionsStats& srcStats = exPortionInfo
        ? (exPortionInfo->HasRemoveSnapshot()
            ? engineStats.StatsByType[TPortionMeta::EProduced::INACTIVE]
            : engineStats.StatsByType[exPortionInfo->GetMeta().Produced])
        : engineStats.StatsByType[portionInfo.GetMeta().Produced];
    TColumnEngineStats::TPortionsStats& stats = portionInfo.HasRemoveSnapshot()
        ? engineStats.StatsByType[TPortionMeta::EProduced::INACTIVE]
        : engineStats.StatsByType[portionInfo.GetMeta().Produced];

    const bool isErase = updateType == EStatsUpdateType::ERASE;
    const bool isAdd = updateType == EStatsUpdateType::ADD;

    if (isErase) { // PortionsToDrop
        engineStats.ColumnRecords -= columnRecords;
        engineStats.ColumnMetadataBytes -= metadataBytes;

        stats -= deltaStats;
    } else if (isAdd) { // Load || AppendedPortions
        engineStats.ColumnRecords += columnRecords;
        engineStats.ColumnMetadataBytes += metadataBytes;

        stats += deltaStats;
    } else if (&srcStats != &stats || exPortionInfo) { // SwitchedPortions || PortionsToEvict
        stats += deltaStats;

        if (exPortionInfo) {
            ui64 rmMetadataBytes = 0;
            srcStats -= DeltaStats(*exPortionInfo, rmMetadataBytes);

            engineStats.ColumnRecords += columnRecords - exPortionInfo->Records.size();
            engineStats.ColumnMetadataBytes += metadataBytes - rmMetadataBytes;
        } else {
            srcStats -= deltaStats;
        }
    }
}

void TColumnEngineForLogs::RegisterSchemaVersion(const TSnapshot& snapshot, TIndexInfo&& indexInfo) {
    if (!VersionedIndex.IsEmpty()) {
        const NOlap::TIndexInfo& lastIndexInfo = VersionedIndex.GetLastSchema()->GetIndexInfo();
        Y_ABORT_UNLESS(lastIndexInfo.CheckCompatible(indexInfo));
    }
    const bool isCriticalScheme = indexInfo.GetSchemeNeedActualization();
    VersionedIndex.AddIndex(snapshot, std::move(indexInfo));
    if (isCriticalScheme) {
        if (!ActualizationStarted) {
            ActualizationStarted = true;
            for (auto&& i : Tables) {
                i.second->StartActualizationIndex();
            }
        }
        for (auto&& i : Tables) {
            i.second->RefreshScheme();
        }
    }
}

void TColumnEngineForLogs::RegisterSchemaVersion(const TSnapshot& snapshot, const NKikimrSchemeOp::TColumnTableSchema& schema) {
    std::optional<NOlap::TIndexInfo> indexInfoOptional = NOlap::TIndexInfo::BuildFromProto(schema, StoragesManager);
    AFL_VERIFY(indexInfoOptional);
    NOlap::TIndexInfo indexInfo = std::move(*indexInfoOptional);
    indexInfo.SetAllKeys(StoragesManager);
    RegisterSchemaVersion(snapshot, std::move(indexInfo));
}

bool TColumnEngineForLogs::Load(IDbWrapper& db) {
    Y_ABORT_UNLESS(!Loaded);
    Loaded = true;
    THashMap<ui64, ui64> granuleToPathIdDecoder;
    {
        TMemoryProfileGuard g("TTxInit/LoadColumns");
        auto guard = GranulesStorage->StartPackModification();
        if (!LoadColumns(db)) {
            return false;
        }
        if (!LoadCounters(db)) {
            return false;
        }
    }

    for (const auto& [pathId, spg] : Tables) {
        for (const auto& [_, portionInfo] : spg->GetPortions()) {
            UpdatePortionStats(*portionInfo, EStatsUpdateType::ADD);
            if (portionInfo->CheckForCleanup()) {
                CleanupPortions[portionInfo->GetRemoveSnapshotVerified()].emplace_back(*portionInfo);
            }
        }
    }

    Y_ABORT_UNLESS(!(LastPortion >> 63), "near to int overflow");
    Y_ABORT_UNLESS(!(LastGranule >> 63), "near to int overflow");
    return true;
}

bool TColumnEngineForLogs::LoadColumns(IDbWrapper& db) {
    TSnapshot lastSnapshot(0, 0);
    const TIndexInfo* currentIndexInfo = nullptr;
    if (!db.LoadColumns([&](const TPortionInfo& portion, const TColumnChunkLoadContext& loadContext) {
        if (!currentIndexInfo || lastSnapshot != portion.GetMinSnapshot()) {
            currentIndexInfo = &VersionedIndex.GetSchema(portion.GetMinSnapshot())->GetIndexInfo();
            lastSnapshot = portion.GetMinSnapshot();
        }
        AFL_VERIFY(portion.ValidSnapshotInfo())("details", portion.DebugString());
        // Locate granule and append the record.
        GetGranulePtrVerified(portion.GetPathId())->AddColumnRecordOnLoad(*currentIndexInfo, portion, loadContext, loadContext.GetPortionMeta());
    })) {
        return false;
    }

    if (!db.LoadIndexes([&](const ui64 pathId, const ui64 portionId, const TIndexChunkLoadContext& loadContext) {
        auto portion = GetGranulePtrVerified(pathId)->GetPortionPtr(portionId);
        AFL_VERIFY(portion);
        const auto linkBlobId = portion->RegisterBlobId(loadContext.GetBlobRange().GetBlobId());
        portion->AddIndex(loadContext.BuildIndexChunk(linkBlobId));
    })) {
        return false;
    };

    for (auto&& i : Tables) {
        i.second->OnAfterPortionsLoad();
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

std::shared_ptr<TInsertColumnEngineChanges> TColumnEngineForLogs::StartInsert(std::vector<TInsertedData>&& dataToIndex) noexcept {
    Y_ABORT_UNLESS(dataToIndex.size());

    TSaverContext saverContext(StoragesManager);
    auto changes = std::make_shared<TInsertColumnEngineChanges>(std::move(dataToIndex), TSplitSettings(), saverContext);
    auto pkSchema = VersionedIndex.GetLastSchema()->GetIndexInfo().GetReplaceKey();

    for (const auto& data : changes->GetDataToIndex()) {
        const ui64 pathId = data.PathId;

        if (changes->PathToGranule.contains(pathId)) {
            continue;
        }
        changes->PathToGranule[pathId] = GetGranulePtrVerified(pathId)->GetBucketPositions();
    }

    return changes;
}

std::shared_ptr<TColumnEngineChanges> TColumnEngineForLogs::StartCompaction(const TCompactionLimits& limits, const std::shared_ptr<NDataLocks::TManager>& dataLocksManager) noexcept {
    AFL_VERIFY(dataLocksManager);
    auto granule = GranulesStorage->GetGranuleForCompaction(Tables, dataLocksManager);
    if (!granule) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "no granules for start compaction");
        return nullptr;
    }
    granule->OnStartCompaction();
    auto changes = granule->GetOptimizationTask(limits, granule, dataLocksManager);
    NYDBTest::TControllers::GetColumnShardController()->OnStartCompaction(changes);
    if (!changes) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "cannot build optimization task for granule that need compaction")("weight", granule->GetCompactionPriority().DebugString());
    }
    return changes;
}

std::shared_ptr<TCleanupTablesColumnEngineChanges> TColumnEngineForLogs::StartCleanupTables(THashSet<ui64>& pathsToDrop) noexcept {
    if (pathsToDrop.empty()) {
        return nullptr;
    }
    auto changes = std::make_shared<TCleanupTablesColumnEngineChanges>(StoragesManager);

    ui64 txSize = 0;
    const ui64 txSizeLimit = TGlobalLimits::TxWriteLimitBytes / 4;
    THashSet<ui64> pathsToRemove;
    for (ui64 pathId : pathsToDrop) {
        if (!HasDataInPathId(pathId)) {
            changes->TablesToDrop.emplace(pathId);
        }
        txSize += 256;
        if (txSize > txSizeLimit) {
            break;
        }
    }
    for (auto&& i : pathsToRemove) {
        pathsToDrop.erase(i);
    }
    if (changes->TablesToDrop.empty()) {
        return nullptr;
    }
    return changes;
}

std::shared_ptr<TCleanupPortionsColumnEngineChanges> TColumnEngineForLogs::StartCleanupPortions(const TSnapshot& snapshot,
                                                                         const THashSet<ui64>& pathsToDrop, const std::shared_ptr<NDataLocks::TManager>& dataLocksManager) noexcept {
    AFL_VERIFY(dataLocksManager);
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "StartCleanup")("portions_count", CleanupPortions.size());
    auto changes = std::make_shared<TCleanupPortionsColumnEngineChanges>(StoragesManager);

    // Add all portions from dropped paths
    ui64 txSize = 0;
    const ui64 txSizeLimit = TGlobalLimits::TxWriteLimitBytes / 4;
    ui32 skipLocked = 0;
    ui32 portionsFromDrop = 0;
    bool limitExceeded = false;
    for (ui64 pathId : pathsToDrop) {
        auto itTable = Tables.find(pathId);
        if (itTable == Tables.end()) {
            continue;
        }

        for (auto& [portion, info] : itTable->second->GetPortions()) {
            if (dataLocksManager->IsLocked(*info)) {
                ++skipLocked;
                continue;
            }
            if (txSize + info->GetTxVolume() < txSizeLimit || changes->PortionsToDrop.empty()) {
                txSize += info->GetTxVolume();
            } else {
                limitExceeded = true;
                break;
            }
            changes->PortionsToDrop.push_back(*info);
            ++portionsFromDrop;
        }
    }

    for (auto it = CleanupPortions.begin(); !limitExceeded && it != CleanupPortions.end();) {
        if (it->first >= snapshot) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "StartCleanupStop")("snapshot", snapshot.DebugString())("current_snapshot", it->first.DebugString());
            break;
        }
        for (ui32 i = 0; i < it->second.size();) {
            if (dataLocksManager->IsLocked(it->second[i])) {
                ++skipLocked;
                ++i;
                continue;
            }
            Y_ABORT_UNLESS(it->second[i].CheckForCleanup(snapshot));
            if (txSize + it->second[i].GetTxVolume() < txSizeLimit || changes->PortionsToDrop.empty()) {
                txSize += it->second[i].GetTxVolume();
            } else {
                limitExceeded = true;
                break;
            }
            changes->PortionsToDrop.push_back(std::move(it->second[i]));
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
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "StartCleanup")
        ("portions_count", CleanupPortions.size())("portions_prepared", changes->PortionsToDrop.size())("drop", portionsFromDrop)("skip", skipLocked);

    if (changes->PortionsToDrop.empty()) {
        return nullptr;
    }

    return changes;
}

std::vector<std::shared_ptr<TTTLColumnEngineChanges>> TColumnEngineForLogs::StartTtl(const THashMap<ui64, TTiering>& pathEviction, const std::shared_ptr<NDataLocks::TManager>& dataLocksManager, 
    const ui64 memoryUsageLimit) noexcept {
    AFL_VERIFY(dataLocksManager);
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "StartTtl")("external", pathEviction.size());

    TSaverContext saverContext(StoragesManager);
    NActualizer::TTieringProcessContext context(memoryUsageLimit, saverContext, dataLocksManager, SignalCounters, ActualizationController);
    for (auto&& i : pathEviction) {
        auto g = GetGranuleOptional(i.first);
        if (g) {
            if (!ActualizationStarted) {
                g->StartActualizationIndex();
            }
            g->RefreshTiering(i.second);
            g->BuildActualizationTasks(context);
        }
    }

    if (ActualizationStarted) {
        TLogContextGuard lGuard(TLogContextBuilder::Build()("queue", "ttl")("external_count", pathEviction.size()));
        for (auto&& i : Tables) {
            if (pathEviction.contains(i.first)) {
                continue;
            }
            i.second->BuildActualizationTasks(context);
        }
    } else {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "StartTtl")("skip", "not_ready_tiers");
    }
    std::vector<std::shared_ptr<TTTLColumnEngineChanges>> result;
    for (auto&& i : context.GetTasks()) {
        for (auto&& t : i.second) {
            SignalCounters.OnActualizationTask(t.GetTask()->GetPortionsToEvictCount(), t.GetTask()->PortionsToRemove.size());
            result.emplace_back(t.GetTask());
        }
    }
    return result;
}

bool TColumnEngineForLogs::ApplyChanges(IDbWrapper& db, std::shared_ptr<TColumnEngineChanges> indexChanges, const TSnapshot& snapshot) noexcept {
    {
        TFinalizationContext context(LastGranule, LastPortion, snapshot);
        indexChanges->Compile(context);
    }
    db.WriteCounter(LAST_PORTION, LastPortion);
    db.WriteCounter(LAST_GRANULE, LastGranule);

    if (LastSnapshot < snapshot) {
        LastSnapshot = snapshot;
        db.WriteCounter(LAST_PLAN_STEP, LastSnapshot.GetPlanStep());
        db.WriteCounter(LAST_TX_ID, LastSnapshot.GetTxId());
    }
    return true;
}

void TColumnEngineForLogs::EraseTable(const ui64 pathId) {
    auto it = Tables.find(pathId);
    Y_ABORT_UNLESS(it != Tables.end());
    Y_ABORT_UNLESS(it->second->IsErasable());
    Tables.erase(it);
}

void TColumnEngineForLogs::UpsertPortion(const TPortionInfo& portionInfo, const TPortionInfo* exInfo) {
    if (exInfo) {
        UpdatePortionStats(portionInfo, EStatsUpdateType::DEFAULT, exInfo);
    } else {
        UpdatePortionStats(portionInfo, EStatsUpdateType::ADD);
    }

    GetGranulePtrVerified(portionInfo.GetPathId())->UpsertPortion(portionInfo);
}

bool TColumnEngineForLogs::ErasePortion(const TPortionInfo& portionInfo, bool updateStats) {
    Y_ABORT_UNLESS(!portionInfo.Empty());
    const ui64 portion = portionInfo.GetPortion();
    auto spg = GetGranulePtrVerified(portionInfo.GetPathId());
    Y_ABORT_UNLESS(spg);
    auto p = spg->GetPortionPtr(portion);

    if (!p) {
        LOG_S_WARN("Portion erased already " << portionInfo << " at tablet " << TabletId);
        return false;
    } else {
        if (updateStats) {
            UpdatePortionStats(*p, EStatsUpdateType::ERASE);
        }
        Y_ABORT_UNLESS(spg->ErasePortion(portion));
        return true;
    }
}

std::shared_ptr<TSelectInfo> TColumnEngineForLogs::Select(ui64 pathId, TSnapshot snapshot,
    const TPKRangesFilter& pkRangesFilter) const {
    auto out = std::make_shared<TSelectInfo>();
    auto itTable = Tables.find(pathId);
    if (itTable == Tables.end()) {
        return out;
    }

    auto spg = itTable->second;
    for (const auto& [indexKey, keyPortions] : spg->GroupOrderedPortionsByPK()) {
        for (auto&& [_, portionInfo] : keyPortions) {
            if (!portionInfo->IsVisible(snapshot)) {
                continue;
            }
            Y_ABORT_UNLESS(portionInfo->Produced());
            const bool skipPortion = !pkRangesFilter.IsPortionInUsage(*portionInfo, VersionedIndex.GetLastSchema()->GetIndexInfo());
            AFL_TRACE(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", skipPortion ? "portion_skipped" : "portion_selected")
                ("pathId", pathId)("portion", portionInfo->DebugString());
            if (skipPortion) {
                continue;
            }
            out->PortionsOrderedPK.emplace_back(portionInfo);
        }
    }

    return out;
}

void TColumnEngineForLogs::OnTieringModified(const std::shared_ptr<NColumnShard::TTiersManager>& manager, const NColumnShard::TTtl& ttl, const std::optional<ui64> pathId) {
    if (!ActualizationStarted) {
        for (auto&& i : Tables) {
            i.second->StartActualizationIndex();
        }
    }

    ActualizationStarted = true;
    AFL_VERIFY(manager);
    THashMap<ui64, TTiering> tierings = manager->GetTiering();
    ttl.AddTtls(tierings);

    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "OnTieringModified")
        ("new_count_tierings", tierings.size())
        ("new_count_ttls", ttl.PathsCount());
    // some string


    if (pathId) {
        auto itGranule = Tables.find(*pathId);
        AFL_VERIFY(itGranule != Tables.end());
        auto it = tierings.find(*pathId);
        if (it == tierings.end()) {
            itGranule->second->RefreshTiering({});
        } else {
            itGranule->second->RefreshTiering(it->second);
        }
    } else {
        for (auto&& g : Tables) {
            auto it = tierings.find(g.first);
            if (it == tierings.end()) {
                g.second->RefreshTiering({});
            } else {
                g.second->RefreshTiering(it->second);
            }
        }
    }
}

void TColumnEngineForLogs::DoRegisterTable(const ui64 pathId) {
    auto infoEmplace = Tables.emplace(pathId, std::make_shared<TGranuleMeta>(pathId, GranulesStorage, SignalCounters.RegisterGranuleDataCounters(), VersionedIndex));
    AFL_VERIFY(infoEmplace.second);
    if (ActualizationStarted) {
        infoEmplace.first->second->StartActualizationIndex();
        infoEmplace.first->second->RefreshScheme();
    }
}

} // namespace NKikimr::NOlap
