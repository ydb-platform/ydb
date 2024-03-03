#include "column_engine_logs.h"
#include "filter.h"

#include "changes/indexation.h"
#include "changes/general_compaction.h"
#include "changes/cleanup.h"
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

TColumnEngineForLogs::TColumnEngineForLogs(ui64 tabletId, const TCompactionLimits& limits, const std::shared_ptr<IStoragesManager>& storagesManager)
    : GranulesStorage(std::make_shared<TGranulesStorage>(SignalCounters, limits, storagesManager))
    , StoragesManager(storagesManager)
    , TabletId(tabletId)
    , LastPortion(0)
    , LastGranule(0)
{
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
    VersionedIndex.AddIndex(snapshot, std::move(indexInfo));
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
                CleanupPortions[portionInfo->GetRemoveSnapshot()].emplace_back(*portionInfo);
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

std::shared_ptr<TCleanupColumnEngineChanges> TColumnEngineForLogs::StartCleanup(const TSnapshot& snapshot,
                                                                         THashSet<ui64>& pathsToDrop, const std::shared_ptr<NDataLocks::TManager>& dataLocksManager) noexcept {
    AFL_VERIFY(dataLocksManager);
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "StartCleanup")("portions_count", CleanupPortions.size());
    auto changes = std::make_shared<TCleanupColumnEngineChanges>(StoragesManager);

    // Add all portions from dropped paths
    THashSet<ui64> dropPortions;
    THashSet<ui64> emptyPaths;
    ui64 txSize = 0;
    const ui64 txSizeLimit = TGlobalLimits::TxWriteLimitBytes / 4;
    changes->NeedRepeat = false;
    for (ui64 pathId : pathsToDrop) {
        auto itTable = Tables.find(pathId);
        if (itTable == Tables.end()) {
            emptyPaths.insert(pathId);
            continue;
        }

        for (auto& [portion, info] : itTable->second->GetPortions()) {
            if (dataLocksManager->IsLocked(*info)) {
                continue;
            }
            if (txSize + info->GetTxVolume() < txSizeLimit || changes->PortionsToDrop.empty()) {
                txSize += info->GetTxVolume();
            } else {
                changes->NeedRepeat = true;
                break;
            }
            changes->PortionsToDrop.push_back(*info);
            dropPortions.insert(portion);
        }
    }
    for (ui64 pathId : emptyPaths) {
        pathsToDrop.erase(pathId);
    }

    while (CleanupPortions.size() && !changes->NeedRepeat) {
        auto it = CleanupPortions.begin();
        if (it->first >= snapshot) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "StartCleanupStop")("snapshot", snapshot.DebugString())("current_snapshot", it->first.DebugString());
            break;
        }
        for (auto&& i : it->second) {
            if (dataLocksManager->IsLocked(i)) {
                continue;
            }
            Y_ABORT_UNLESS(i.CheckForCleanup(snapshot));
            if (txSize + i.GetTxVolume() < txSizeLimit || changes->PortionsToDrop.empty()) {
                txSize += i.GetTxVolume();
            } else {
                changes->NeedRepeat = true;
                break;
            }
            changes->PortionsToDrop.push_back(i);
        }
        CleanupPortions.erase(it);
    }
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "StartCleanup")("portions_count", CleanupPortions.size())("portions_prepared", changes->PortionsToDrop.size());

    if (changes->PortionsToDrop.empty()) {
        return nullptr;
    }

    return changes;
}

TDuration TColumnEngineForLogs::ProcessTiering(const ui64 pathId, const TTiering& ttl, TTieringProcessContext& context) const {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "ProcessTiering")("path_id", pathId)("ttl", ttl.GetDebugString());
    auto& indexInfo = VersionedIndex.GetLastSchema()->GetIndexInfo();
    Y_ABORT_UNLESS(context.Changes->Tiering.emplace(pathId, ttl).second);

    TDuration dWaiting = NYDBTest::TControllers::GetColumnShardController()->GetTTLDefaultWaitingDuration(TDuration::Minutes(1));
    auto itTable = Tables.find(pathId);
    if (itTable == Tables.end()) {
        return dWaiting;
    }

    std::optional<TInstant> expireTimestampOpt;
    if (ttl.Ttl) {
        expireTimestampOpt = ttl.Ttl->GetEvictInstant(context.Now);
    }

    auto ttlColumnNames = ttl.GetTtlColumns();
    Y_ABORT_UNLESS(ttlColumnNames.size() == 1); // TODO: support different ttl columns
    ui32 ttlColumnId = indexInfo.GetColumnId(*ttlColumnNames.begin());
    const TInstant now = TInstant::Now();
    for (auto& [portion, info] : itTable->second->GetPortions()) {
        if (info->HasRemoveSnapshot()) {
            continue;
        }
        if (context.DataLocksManager->IsLocked(*info)) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "skip ttl through busy portion")("portion_id", info->GetAddress().DebugString());
            continue;
        }

        const bool tryEvictPortion = ttl.HasTiers() && context.HasLimitsForEviction();

        if (auto max = info->MaxValue(ttlColumnId)) {
            bool keep = !expireTimestampOpt;
            if (expireTimestampOpt) {
                auto mpiOpt = ttl.Ttl->ScalarToInstant(max);
                Y_ABORT_UNLESS(mpiOpt);
                const TInstant maxTtlPortionInstant = *mpiOpt;
                const TDuration d = maxTtlPortionInstant - *expireTimestampOpt;
                keep = !!d;
                AFL_TRACE(NKikimrServices::TX_COLUMNSHARD)("event", "keep_detect")("max", maxTtlPortionInstant.Seconds())("expire", expireTimestampOpt->Seconds());
                if (d && dWaiting > d) {
                    dWaiting = d;
                }
            }

            AFL_TRACE(NKikimrServices::TX_COLUMNSHARD)("event", "scalar_less_result")("keep", keep)("tryEvictPortion", tryEvictPortion)("allowDrop", context.HasLimitsForTtl());
            if (keep && tryEvictPortion) {
                const TString currentTierName = info->GetMeta().GetTierName() ? info->GetMeta().GetTierName() : IStoragesManager::DefaultStorageId;
                TString tierName = "";
                const TInstant maxChangePortionInstant = info->RecordSnapshotMax().GetPlanInstant();
                if (now - maxChangePortionInstant < TDuration::Minutes(60)) {
                    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "skip_portion_to_evict")("reason", "too_fresh")("delta", now - maxChangePortionInstant);
                    continue;
                }
                for (auto& tierRef : ttl.GetOrderedTiers()) {
                    auto& tierInfo = tierRef.Get();
                    if (!indexInfo.AllowTtlOverColumn(tierInfo.GetEvictColumnName())) {
                        SignalCounters.OnPortionNoTtlColumn(info->BlobsBytes());
                        continue;
                    }
                    auto mpiOpt = tierInfo.ScalarToInstant(max);
                    Y_ABORT_UNLESS(mpiOpt);
                    const TInstant maxTieringPortionInstant = *mpiOpt;

                    const TDuration d = tierInfo.GetEvictInstant(context.Now) - maxTieringPortionInstant;
                    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "tiering_choosing")("max", maxTieringPortionInstant.Seconds())
                        ("evict", tierInfo.GetEvictInstant(context.Now).Seconds())("tier_name", tierInfo.GetName())("d", d);
                    if (d) {
                        tierName = tierInfo.GetName();
                        break;
                    } else {
                        auto dWaitLocal = maxTieringPortionInstant - tierInfo.GetEvictInstant(context.Now);
                        if (dWaiting > dWaitLocal) {
                            dWaiting = dWaitLocal;
                        }
                    }
                }
                if (!tierName) {
                    tierName = IStoragesManager::DefaultStorageId;
                }
                if (currentTierName != tierName) {
                    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "tiering switch detected")("from", currentTierName)("to", tierName);
                    context.Changes->AddPortionToEvict(*info, TPortionEvictionFeatures(tierName, pathId));
                    context.AppPortionForEvictionChecker(*info);
                    SignalCounters.OnPortionToEvict(info->BlobsBytes());
                }
            }
            if (!keep && context.HasLimitsForTtl()) {
                AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "portion_remove")("portion", info->DebugString());
                AFL_VERIFY(context.Changes->PortionsToRemove.emplace(info->GetAddress(), *info).second);
                SignalCounters.OnPortionToDrop(info->BlobsBytes());
                context.AppPortionForTtlChecker(*info);
            }
        } else {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "scalar_less_not_max");
            SignalCounters.OnPortionNoBorder(info->BlobsBytes());
        }
    }
    if (dWaiting > TDuration::MilliSeconds(500) && (!context.HasLimitsForEviction() || !context.HasLimitsForTtl())) {
        dWaiting = TDuration::MilliSeconds(500);
    }
    Y_ABORT_UNLESS(!!dWaiting);
    return dWaiting;
}

bool TColumnEngineForLogs::DrainEvictionQueue(std::map<TMonotonic, std::vector<TEvictionsController::TTieringWithPathId>>& evictionsQueue, TTieringProcessContext& context) const {
    const TMonotonic nowMonotonic = TlsActivationContext ? AppData()->MonotonicTimeProvider->Now() : TMonotonic::Now();
    bool hasChanges = false;
    while (evictionsQueue.size() && evictionsQueue.begin()->first < nowMonotonic) {
        hasChanges = true;
        auto tierings = std::move(evictionsQueue.begin()->second);
        evictionsQueue.erase(evictionsQueue.begin());
        for (auto&& i : tierings) {
            auto itDuration = context.DurationsForced.find(i.GetPathId());
            if (itDuration == context.DurationsForced.end()) {
                const TDuration dWaiting = ProcessTiering(i.GetPathId(), i.GetTieringInfo(), context);
                evictionsQueue[nowMonotonic + dWaiting].emplace_back(std::move(i));
            } else {
                evictionsQueue[nowMonotonic + itDuration->second].emplace_back(std::move(i));
            }
        }
    }

    if (evictionsQueue.size()) {
        if (evictionsQueue.begin()->first < nowMonotonic) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "stop scan")("reason", "too many data")("first", evictionsQueue.begin()->first)("now", nowMonotonic);
        } else if (!hasChanges) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "stop scan")("reason", "too early")("first", evictionsQueue.begin()->first)("now", nowMonotonic);
        } else {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "stop scan")("reason", "task_ready")("first", evictionsQueue.begin()->first)("now", nowMonotonic)
                ("internal", hasChanges)("evict_portions", context.Changes->GetPortionsToEvictCount())
                ("drop_portions", context.Changes->PortionsToRemove.size());
        }
    } else {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "stop scan")("reason", "no data in queue");
    }
    return hasChanges;
}

std::shared_ptr<TTTLColumnEngineChanges> TColumnEngineForLogs::StartTtl(const THashMap<ui64, TTiering>& pathEviction, const std::shared_ptr<NDataLocks::TManager>& dataLocksManager, const ui64 memoryUsageLimit) noexcept {
    AFL_VERIFY(dataLocksManager);
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "StartTtl")("external", pathEviction.size())
        ("internal", EvictionsController.MutableNextCheckInstantForTierings().size())
        ;

    TSaverContext saverContext(StoragesManager);

    auto changes = std::make_shared<TTTLColumnEngineChanges>(TSplitSettings(), saverContext);

    TTieringProcessContext context(memoryUsageLimit, changes, dataLocksManager, TTTLColumnEngineChanges::BuildMemoryPredictor());
    bool hasExternalChanges = false;
    for (auto&& i : pathEviction) {
        context.DurationsForced[i.first] = ProcessTiering(i.first, i.second, context);
        hasExternalChanges = true;
    }

    {
        TLogContextGuard lGuard(TLogContextBuilder::Build()("queue", "ttl")("has_external", hasExternalChanges));
        DrainEvictionQueue(EvictionsController.MutableNextCheckInstantForTierings(), context);
    }

    if (changes->PortionsToRemove.empty() && !changes->GetPortionsToEvictCount()) {
        return nullptr;
    }
    return changes;
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

void TColumnEngineForLogs::OnTieringModified(std::shared_ptr<NColumnShard::TTiersManager> manager, const NColumnShard::TTtl& ttl) {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "OnTieringModified");
    std::optional<THashMap<ui64, TTiering>> tierings;
    if (manager) {
        tierings = manager->GetTiering();
    }
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "OnTieringModified")
        ("new_count_tierings", tierings ? ::ToString(tierings->size()) : TString("undefined"))
        ("new_count_ttls", ttl.PathsCount());
    EvictionsController.RefreshTierings(std::move(tierings), ttl);

}

void TColumnEngineForLogs::DoRegisterTable(const ui64 pathId) {
    AFL_VERIFY(Tables.emplace(pathId, std::make_shared<TGranuleMeta>(pathId, GranulesStorage, SignalCounters.RegisterGranuleDataCounters(), VersionedIndex)).second);
}

TColumnEngineForLogs::TTieringProcessContext::TTieringProcessContext(const ui64 memoryUsageLimit,
    std::shared_ptr<TTTLColumnEngineChanges> changes, const std::shared_ptr<NDataLocks::TManager>& dataLocksManager, const std::shared_ptr<TColumnEngineChanges::IMemoryPredictor>& memoryPredictor)
    : MemoryUsageLimit(memoryUsageLimit)
    , MemoryPredictor(memoryPredictor)
    , Now(TlsActivationContext ? AppData()->TimeProvider->Now() : TInstant::Now())
    , Changes(changes)
    , DataLocksManager(dataLocksManager)
{

}

void TEvictionsController::RefreshTierings(std::optional<THashMap<ui64, TTiering>>&& tierings, const NColumnShard::TTtl& ttl) {
    if (tierings) {
        OriginalTierings = std::move(*tierings);
    }
    auto copy = OriginalTierings;
    ttl.AddTtls(copy);
    NextCheckInstantForTierings = BuildNextInstantCheckers(std::move(copy));
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "RefreshTierings")("count", NextCheckInstantForTierings.size());
}

} // namespace NKikimr::NOlap
