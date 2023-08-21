#include "column_engine_logs.h"
#include "filter.h"
#include "indexed_read_data.h"

#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>
#include <ydb/core/formats/arrow/one_batch_input_stream.h>
#include <ydb/core/formats/arrow/merging_sorted_input_stream.h>
#include <ydb/core/tx/tiering/manager.h>
#include <ydb/core/tx/columnshard/columnshard_ttl.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/library/conclusion/status.h>
#include "changes/indexation.h"
#include "changes/in_granule_compaction.h"
#include "changes/split_compaction.h"
#include "changes/cleanup.h"
#include "changes/ttl.h"

#include <concepts>

namespace NKikimr::NOlap {

std::shared_ptr<NKikimr::NOlap::TCompactColumnEngineChanges> TColumnEngineForLogs::TChangesConstructor::BuildCompactionChanges(std::unique_ptr<TCompactionInfo>&& info,
    const TCompactionLimits& limits, const TSnapshot& initSnapshot, const TCompactionSrcGranule& srcGranule) {
    std::shared_ptr<TCompactColumnEngineChanges> result;
    if (info->InGranule()) {
        result = std::make_shared<TInGranuleCompactColumnEngineChanges>(limits, info->GetGranule(), srcGranule);
    } else {
        result = std::make_shared<TSplitCompactColumnEngineChanges>(limits, info->GetGranule(), srcGranule);
    }
    result->InitSnapshot = initSnapshot;
    return result;
}

std::shared_ptr<NKikimr::NOlap::TCleanupColumnEngineChanges> TColumnEngineForLogs::TChangesConstructor::BuildCleanupChanges(const TSnapshot& initSnapshot) {
    auto changes = std::make_shared<TCleanupColumnEngineChanges>();
    changes->InitSnapshot = initSnapshot;
    return changes;
}

std::shared_ptr<NKikimr::NOlap::TTTLColumnEngineChanges> TColumnEngineForLogs::TChangesConstructor::BuildTtlChanges() {
    return std::make_shared<TTTLColumnEngineChanges>();
}

TColumnEngineForLogs::TColumnEngineForLogs(ui64 tabletId, const TCompactionLimits& limits)
    : GranulesStorage(std::make_shared<TGranulesStorage>(SignalCounters, limits))
    , TabletId(tabletId)
    , LastPortion(0)
    , LastGranule(0)
{
}

ui64 TColumnEngineForLogs::MemoryUsage() const {
    auto numPortions = Counters.GetPortionsCount();

    return Counters.Granules * (sizeof(TGranuleMeta) + sizeof(ui64)) +
        numPortions * (sizeof(TPortionInfo) + sizeof(ui64)) +
        Counters.ColumnRecords * sizeof(TColumnRecord) +
        Counters.ColumnMetadataBytes;
}

const TMap<ui64, std::shared_ptr<TColumnEngineStats>>& TColumnEngineForLogs::GetStats() const {
    return PathStats;
}

const TColumnEngineStats& TColumnEngineForLogs::GetTotalStats() {
    Counters.Tables = PathGranules.size();
    Counters.Granules = Granules.size();
    Counters.EmptyGranules = EmptyGranules.size();
    Counters.OverloadedGranules = GranulesStorage->GetOverloadedGranulesCount();

    return Counters;
}

void TColumnEngineForLogs::UpdatePortionStats(const TPortionInfo& portionInfo, EStatsUpdateType updateType,
                                            const TPortionInfo* exPortionInfo) {
    UpdatePortionStats(Counters, portionInfo, updateType, exPortionInfo);

    const ui64 granule = portionInfo.GetGranule();
    Y_VERIFY(granule);
    Y_VERIFY(Granules.contains(granule));
    ui64 pathId = Granules[granule]->PathId();
    Y_VERIFY(pathId);
    if (!PathStats.contains(pathId)) {
        auto& stats = PathStats[pathId];
        stats = std::make_shared<TColumnEngineStats>();
        stats->Tables = 1;
    }
    UpdatePortionStats(*PathStats[pathId], portionInfo, updateType, exPortionInfo);
}

TColumnEngineStats::TPortionsStats DeltaStats(const TPortionInfo& portionInfo, ui64& metadataBytes) {
    TColumnEngineStats::TPortionsStats deltaStats;
    THashSet<TUnifiedBlobId> blobs;
    for (auto& rec : portionInfo.Records) {
        metadataBytes += rec.GetMeta().GetMetadataSize();
        blobs.insert(rec.BlobRange.BlobId);
        deltaStats.BytesByColumn[rec.ColumnId] += rec.BlobRange.Size;
        deltaStats.RawBytesByColumn[rec.ColumnId] += rec.GetMeta().GetRawBytes().value_or(0);
    }
    deltaStats.Rows = portionInfo.NumRows();
    deltaStats.RawBytes = portionInfo.RawBytesSum();
    deltaStats.Bytes = 0;
    for (auto& blobId : blobs) {
        deltaStats.Bytes += blobId.BlobSize();
    }
    deltaStats.Blobs = blobs.size();
    deltaStats.Portions = 1;
    return deltaStats;
}

void TColumnEngineForLogs::UpdatePortionStats(TColumnEngineStats& engineStats, const TPortionInfo& portionInfo,
                                              EStatsUpdateType updateType,
                                              const TPortionInfo* exPortionInfo) const {
    ui64 columnRecords = portionInfo.Records.size();
    ui64 metadataBytes = 0;
    TColumnEngineStats::TPortionsStats deltaStats = DeltaStats(portionInfo, metadataBytes);

    Y_VERIFY(!exPortionInfo || exPortionInfo->GetMeta().Produced != TPortionMeta::EProduced::UNSPECIFIED);
    Y_VERIFY(portionInfo.GetMeta().Produced != TPortionMeta::EProduced::UNSPECIFIED);

    TColumnEngineStats::TPortionsStats& srcStats = exPortionInfo
        ? (exPortionInfo->IsActive()
            ? engineStats.StatsByType[exPortionInfo->GetMeta().Produced]
            : engineStats.StatsByType[TPortionMeta::EProduced::INACTIVE])
        : engineStats.StatsByType[portionInfo.GetMeta().Produced];
    TColumnEngineStats::TPortionsStats& stats = portionInfo.IsActive()
        ? engineStats.StatsByType[portionInfo.GetMeta().Produced]
        : engineStats.StatsByType[TPortionMeta::EProduced::INACTIVE];

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

void TColumnEngineForLogs::UpdateDefaultSchema(const TSnapshot& snapshot, TIndexInfo&& info) {
    if (!GranulesTable) {
        ui32 indexId = info.GetId();
        GranulesTable = std::make_shared<TGranulesTable>(*this, indexId);
        ColumnsTable = std::make_shared<TColumnsTable>(indexId);
        CountersTable = std::make_shared<TCountersTable>(indexId);
    }
    VersionedIndex.AddIndex(snapshot, std::move(info));
}

bool TColumnEngineForLogs::Load(IDbWrapper& db, THashSet<TUnifiedBlobId>& lostBlobs, const THashSet<ui64>& pathsToDrop) {
    ClearIndex();
    {
        auto guard = GranulesStorage->StartPackModification();
        if (!LoadGranules(db)) {
            return false;
        }
        if (!LoadColumns(db, lostBlobs)) {
            return false;
        }
        if (!LoadCounters(db)) {
            return false;
        }
    }

    THashSet<ui64> emptyGranulePaths;
    for (const auto& [granule, spg] : Granules) {
        if (spg->Empty()) {
            EmptyGranules.insert(granule);
            emptyGranulePaths.insert(spg->PathId());
        }
        for (const auto& [_, portionInfo] : spg->GetPortions()) {
            UpdatePortionStats(*portionInfo, EStatsUpdateType::ADD);
            if (portionInfo->CheckForCleanup()) {
                CleanupPortions.emplace(portionInfo->GetAddress());
            }
        }
    }

    // Cleanup empty granules
    for (auto& pathId : emptyGranulePaths) {
        for (auto& emptyGranules : EmptyGranuleTracks(pathId)) {
            // keep first one => merge, keep nothing => drop.
            bool keepFirst = !pathsToDrop.contains(pathId);
            for (auto& [mark, granule] : emptyGranules) {
                if (keepFirst) {
                    keepFirst = false;
                    continue;
                }

                Y_VERIFY(Granules.contains(granule));
                auto spg = Granules[granule];
                Y_VERIFY(spg);
                GranulesTable->Erase(db, spg->Record);
                EraseGranule(pathId, granule, mark);
            }
        }
    }

    Y_VERIFY(!(LastPortion >> 63), "near to int overflow");
    Y_VERIFY(!(LastGranule >> 63), "near to int overflow");
    return true;
}

bool TColumnEngineForLogs::LoadGranules(IDbWrapper& db) {
    auto callback = [&](const TGranuleRecord& rec) {
        SetGranule(rec);
    };

    return GranulesTable->Load(db, callback);
}

bool TColumnEngineForLogs::LoadColumns(IDbWrapper& db, THashSet<TUnifiedBlobId>& lostBlobs) {
    TSnapshot lastSnapshot(0, 0);
    const TIndexInfo* currentIndexInfo = nullptr;
    return ColumnsTable->Load(db, [&](const TPortionInfo& portion, const TColumnChunkLoadContext& loadContext) {
        if (!currentIndexInfo || lastSnapshot != portion.GetMinSnapshot()) {
            currentIndexInfo = &VersionedIndex.GetSchema(portion.GetMinSnapshot())->GetIndexInfo();
            lastSnapshot = portion.GetMinSnapshot();
        }
        Y_VERIFY(portion.ValidSnapshotInfo());
        // Do not count the blob as lost since it exists in the index.
        lostBlobs.erase(loadContext.GetBlobRange().BlobId);
        // Locate granule and append the record.
        TColumnRecord rec(loadContext, *currentIndexInfo);
        GetGranulePtrVerified(portion.GetGranule())->AddColumnRecord(*currentIndexInfo, portion, rec, loadContext.GetPortionMeta());
    });
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

    return CountersTable->Load(db, callback);
}

std::shared_ptr<TInsertColumnEngineChanges> TColumnEngineForLogs::StartInsert(std::vector<TInsertedData>&& dataToIndex) noexcept {
    Y_VERIFY(dataToIndex.size());

    auto changes = std::make_shared<TInsertColumnEngineChanges>(DefaultMark(), std::move(dataToIndex), TSplitSettings());
    ui32 reserveGranules = 0;
    for (const auto& data : changes->GetDataToIndex()) {
        const ui64 pathId = data.PathId;

        if (changes->PathToGranule.contains(pathId)) {
            continue;
        }

        if (PathGranules.contains(pathId)) {
            // Abort inserting if the path has overloaded granules.
            if (GranulesStorage->GetOverloaded(pathId)) {
                AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "skip_insertion_due_to_granule_overload")("path_id", pathId);
                return {};
            }

            // TODO: cache PathToGranule for hot pathIds
            const auto& src = PathGranules[pathId];
            changes->PathToGranule[pathId].assign(src.begin(), src.end());
        } else {
            // It could reserve more than needed in case of the same pathId in DataToIndex
            ++reserveGranules;
        }
    }

    if (reserveGranules) {
        changes->FirstGranuleId = LastGranule + 1;
        LastGranule += reserveGranules;
    }

    return changes;
}

std::shared_ptr<TCompactColumnEngineChanges> TColumnEngineForLogs::StartCompaction(std::unique_ptr<TCompactionInfo>&& info,
                                                                            const TCompactionLimits& limits) noexcept {
    const ui64 pathId = info->GetGranule()->GetPathId();
    Y_VERIFY(PathGranules.contains(pathId));

    auto g = info->GetGranule();
    for (const auto& [mark, pathGranule] : PathGranules[pathId]) {
        if (pathGranule == g->GetGranuleId()) {
            TCompactionSrcGranule srcGranule = TCompactionSrcGranule(mark);
            auto changes = TChangesConstructor::BuildCompactionChanges(std::move(info), limits, LastSnapshot, srcGranule);
            NYDBTest::TControllers::GetColumnShardController()->OnStartCompaction(changes);
            return changes;
        }
    }
    Y_VERIFY(false);
    return nullptr;
}

std::shared_ptr<TCleanupColumnEngineChanges> TColumnEngineForLogs::StartCleanup(const TSnapshot& snapshot,
                                                                         THashSet<ui64>& pathsToDrop, ui32 maxRecords) noexcept {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "StartCleanup")("portions_count", CleanupPortions.size());
    auto changes = std::make_shared<TCleanupColumnEngineChanges>();
    ui32 affectedRecords = 0;

    // Add all portions from dropped paths
    THashSet<ui64> dropPortions;
    THashSet<ui64> emptyPaths;
    for (ui64 pathId : pathsToDrop) {
        if (!PathGranules.contains(pathId)) {
            emptyPaths.insert(pathId);
            continue;
        }

        for (const auto& [_, granule]: PathGranules[pathId]) {
            Y_VERIFY(Granules.contains(granule));
            auto spg = Granules[granule];
            Y_VERIFY(spg);
            for (auto& [portion, info] : spg->GetPortions()) {
                affectedRecords += info->NumRecords();
                changes->PortionsToDrop.push_back(*info);
                dropPortions.insert(portion);
            }

            if (affectedRecords > maxRecords) {
                break;
            }
        }

        if (affectedRecords > maxRecords) {
            changes->NeedRepeat = true;
            break;
        }
    }
    for (ui64 pathId : emptyPaths) {
        pathsToDrop.erase(pathId);
    }

    if (affectedRecords > maxRecords) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "StartCleanup")("portions_count", CleanupPortions.size())("portions_prepared", changes->PortionsToDrop.size());
        return changes;
    }

    // Add stale portions of alive paths
    THashSet<ui64> cleanGranules;
    std::shared_ptr<TGranuleMeta> granuleMeta;
    for (auto it = CleanupPortions.begin(); it != CleanupPortions.end();) {
        if (!granuleMeta || granuleMeta->GetGranuleId() != it->GetGranuleId()) {
            auto itGranule = Granules.find(it->GetGranuleId());
            if (itGranule == Granules.end()) {
                it = CleanupPortions.erase(it);
                continue;
            }
            granuleMeta = itGranule->second;
        }
        Y_VERIFY(granuleMeta);
        auto* portionInfo = granuleMeta->GetPortionPointer(it->GetPortionId());
        if (!portionInfo) {
            it = CleanupPortions.erase(it);
        } else if (portionInfo->CheckForCleanup(snapshot)) {
            affectedRecords += portionInfo->NumRecords();
            changes->PortionsToDrop.push_back(*portionInfo);
            it = CleanupPortions.erase(it);
            if (affectedRecords > maxRecords) {
                changes->NeedRepeat = true;
                break;
            }
        } else {
            AFL_TRACE(NKikimrServices::TX_COLUMNSHARD)("event", "StartCleanup/Check")("snapshot", snapshot.DebugString())("portions_snapshot", portionInfo->GetRemoveSnapshot().DebugString());
            Y_VERIFY(portionInfo->CheckForCleanup());
            ++it;
        }
    }
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "StartCleanup")("portions_count", CleanupPortions.size())("portions_prepared", changes->PortionsToDrop.size());

    return changes;
}

TDuration TColumnEngineForLogs::ProcessTiering(const ui64 pathId, const TTiering& ttl, TTieringProcessContext& context) const {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "ProcessTiering")("path_id", pathId)("ttl", ttl.GetDebugString());
    ui64 evictionSize = 0;
    ui64 dropBlobs = 0;
    auto& indexInfo = VersionedIndex.GetLastSchema()->GetIndexInfo();
    Y_VERIFY(context.Changes->Tiering.emplace(pathId, ttl).second);

    TDuration dWaiting = TDuration::Minutes(5);
    auto itGranules = PathGranules.find(pathId);
    if (itGranules == PathGranules.end()) {
        return dWaiting;
    }

    auto expireTimestampOpt = ttl.GetEvictInstant(context.Now);
    Y_VERIFY(expireTimestampOpt);
    auto expireTimestamp = *expireTimestampOpt;

    auto ttlColumnNames = ttl.GetTtlColumns();
    Y_VERIFY(ttlColumnNames.size() == 1); // TODO: support different ttl columns
    ui32 ttlColumnId = indexInfo.GetColumnId(*ttlColumnNames.begin());
    for (const auto& [ts, granule] : itGranules->second) {
        auto itGranule = Granules.find(granule);
        auto spg = itGranule->second;
        Y_VERIFY(spg);

        for (auto& [portion, info] : spg->GetPortions()) {
            if (!info->IsActive()) {
                continue;
            }
            if (context.BusyGranules.contains(info->GetGranule())) {
                AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "skip ttl through busy granule")("granule_id", info->GetGranule());
                continue;
            }

            context.AllowEviction = (evictionSize <= context.MaxEvictBytes);
            context.AllowDrop = (dropBlobs <= TCompactionLimits::MAX_BLOBS_TO_DELETE);
            const bool tryEvictPortion = context.AllowEviction && ttl.HasTiers() && info->EvictReady(TCompactionLimits::EVICT_HOT_PORTION_BYTES);

            if (auto max = info->MaxValue(ttlColumnId)) {
                bool keep = false;
                {
                    auto mpiOpt = ttl.ScalarToInstant(max);
                    Y_VERIFY(mpiOpt);
                    const TInstant maxTtlPortionInstant = *mpiOpt;
                    const TDuration d = maxTtlPortionInstant - expireTimestamp;
                    keep = !!d;
                    AFL_TRACE(NKikimrServices::TX_COLUMNSHARD)("event", "keep_detect")("max", maxTtlPortionInstant.Seconds())("expire", expireTimestamp.Seconds());
                    if (d && dWaiting > d) {
                        dWaiting = d;
                    }
                }

                AFL_TRACE(NKikimrServices::TX_COLUMNSHARD)("event", "scalar_less_result")("keep", keep)("tryEvictPortion", tryEvictPortion)("allowDrop", context.AllowDrop);
                if (keep && tryEvictPortion) {
                    TString tierName;
                    for (auto& tierRef : ttl.GetOrderedTiers()) {
                        auto& tierInfo = tierRef.Get();
                        if (!indexInfo.AllowTtlOverColumn(tierInfo.GetEvictColumnName())) {
                            SignalCounters.OnPortionNoTtlColumn(info->BlobsBytes());
                            continue;
                        }
                        auto mpiOpt = tierInfo.ScalarToInstant(max);
                        Y_VERIFY(mpiOpt);
                        const TInstant maxTieringPortionInstant = *mpiOpt;

                        const TDuration d = maxTieringPortionInstant - tierInfo.GetEvictInstant(context.Now);
                        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "tiering")("max", maxTieringPortionInstant.Seconds())
                            ("evict", tierInfo.GetEvictInstant(context.Now).Seconds());
                        if (d) {
                            if (dWaiting > d) {
                                dWaiting = d;
                            }
                            tierName = tierInfo.GetName();
                        } else {
                            break;
                        }
                    }
                    if (info->GetMeta().GetTierName() != tierName) {
                        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "tiering switch detected")("from", info->GetMeta().GetTierName())("to", tierName);
                        evictionSize += info->BlobsSizes().first;
                        const bool needExport = ttl.NeedExport(tierName);
                        context.Changes->AddPortionToEvict(*info, TPortionEvictionFeatures(tierName, pathId, needExport));
                        SignalCounters.OnPortionToEvict(info->BlobsBytes());
                    }
                }
                if (!keep && context.AllowDrop) {
                    dropBlobs += info->NumRecords();
                    context.Changes->PortionsToDrop.push_back(*info);
                    SignalCounters.OnPortionToDrop(info->BlobsBytes());
                }
            } else {
                AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "scalar_less_not_max");
                SignalCounters.OnPortionNoBorder(info->BlobsBytes());
            }
        }
    }
    Y_VERIFY(!!dWaiting);
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
                ("drop_portions", context.Changes->PortionsToDrop.size());
        }
    } else {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "stop scan")("reason", "no data in queue");
    }
    return hasChanges;
}

std::shared_ptr<TTTLColumnEngineChanges> TColumnEngineForLogs::StartTtl(const THashMap<ui64, TTiering>& pathEviction, const THashSet<ui64>& busyGranules, ui64 maxEvictBytes) noexcept {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "StartTtl")("external", pathEviction.size())
        ("internal", EvictionsController.MutableNextCheckInstantForTierings().size())
        ;
    auto changes = std::make_shared<TTTLColumnEngineChanges>();

    TTieringProcessContext context(maxEvictBytes, changes, busyGranules);
    bool hasExternalChanges = false;
    for (auto&& i : pathEviction) {
        context.DurationsForced[i.first] = ProcessTiering(i.first, i.second, context);
        hasExternalChanges = true;
    }

    {
        TLogContextGuard lGuard(TLogContextBuilder::Build()("queue", "ttl")("has_external", hasExternalChanges));
        DrainEvictionQueue(EvictionsController.MutableNextCheckInstantForTierings(), context);
    }

    if (changes->PortionsToDrop.empty() && !changes->GetPortionsToEvictCount()) {
        return nullptr;
    }

    if (!context.AllowEviction || !context.AllowDrop) {
        changes->NeedRepeat = true;
    }
    return changes;
}

std::vector<std::vector<std::pair<TMark, ui64>>> TColumnEngineForLogs::EmptyGranuleTracks(ui64 pathId) const {
    Y_VERIFY(PathGranules.contains(pathId));
    const auto& pathGranules = PathGranules.find(pathId)->second;

    std::vector<std::vector<std::pair<TMark, ui64>>> emptyGranules;
    ui64 emptyStart = 0;
    for (const auto& [mark, granule]: pathGranules) {
        Y_VERIFY(Granules.contains(granule));
        auto spg = Granules.find(granule)->second;
        Y_VERIFY(spg);

        if (spg->Empty()) {
            if (!emptyStart) {
                emptyGranules.push_back({});
                emptyStart = granule;
            }
            emptyGranules.back().emplace_back(mark, granule);
        } else if (emptyStart) {
            emptyStart = 0;
        }
    }

    return emptyGranules;
}

bool TColumnEngineForLogs::ApplyChanges(IDbWrapper& db, std::shared_ptr<TColumnEngineChanges> indexChanges, const TSnapshot& snapshot) noexcept {
    {
        TFinalizationContext context(LastGranule, LastPortion, snapshot);
        indexChanges->Compile(context);
    }
    {
        TApplyChangesContext context(db, snapshot);
        Y_VERIFY(indexChanges->ApplyChanges(*this, context));
    }
    CountersTable->Write(db, LAST_PORTION, LastPortion);
    CountersTable->Write(db, LAST_GRANULE, LastGranule);

    if (LastSnapshot < snapshot) {
        LastSnapshot = snapshot;
        CountersTable->Write(db, LAST_PLAN_STEP, LastSnapshot.GetPlanStep());
        CountersTable->Write(db, LAST_TX_ID, LastSnapshot.GetTxId());
    }
    return true;
}

void TColumnEngineForLogs::SetGranule(const TGranuleRecord& rec) {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "upsert_granule")("granule", rec.DebugString());
    const TMark mark(rec.Mark);

    // There should be only one granule with (PathId, Mark).
    AFL_VERIFY(PathGranules[rec.PathId].emplace(mark, rec.Granule).second)("event", "marker_duplication")("granule_id", rec.Granule)("old_granule_id", PathGranules[rec.PathId][mark]);

    // Allocate granule info and ensure that there is no granule with same id inserted before.
    AFL_VERIFY(Granules.emplace(rec.Granule, std::make_shared<TGranuleMeta>(rec, GranulesStorage, SignalCounters.RegisterGranuleDataCounters())).second)("event", "granule_duplication")
        ("granule_id", rec.Granule)("old_granule", Granules[rec.Granule]->DebugString());
}

void TColumnEngineForLogs::EraseGranule(ui64 pathId, ui64 granule, const TMark& mark) {
    Y_VERIFY(PathGranules.contains(pathId));
    auto it = Granules.find(granule);
    Y_VERIFY(it != Granules.end());
    Y_VERIFY(it->second->IsErasable());
    Granules.erase(it);
    EmptyGranules.erase(granule);
    PathGranules[pathId].erase(mark);
}

void TColumnEngineForLogs::UpsertPortion(const TPortionInfo& portionInfo, const TPortionInfo* exInfo) {
    if (exInfo) {
        UpdatePortionStats(portionInfo, EStatsUpdateType::DEFAULT, exInfo);
    } else {
        UpdatePortionStats(portionInfo, EStatsUpdateType::ADD);
    }

    GetGranulePtrVerified(portionInfo.GetGranule())->UpsertPortion(portionInfo);
}

bool TColumnEngineForLogs::ErasePortion(const TPortionInfo& portionInfo, bool updateStats) {
    Y_VERIFY(!portionInfo.Empty());
    const ui64 portion = portionInfo.GetPortion();
    auto it = Granules.find(portionInfo.GetGranule());
    Y_VERIFY(it != Granules.end());
    auto& spg = it->second;
    Y_VERIFY(spg);
    auto* p = spg->GetPortionPointer(portion);

    if (!p) {
        LOG_S_WARN("Portion erased already " << portionInfo << " at tablet " << TabletId);
        return false;
    } else {
        if (updateStats) {
            UpdatePortionStats(*p, EStatsUpdateType::ERASE);
        }
        Y_VERIFY(spg->ErasePortion(portion));
        return true;
    }
}

static TMap<TSnapshot, std::vector<std::shared_ptr<TPortionInfo>>> GroupPortionsBySnapshot(const THashMap<ui64, std::shared_ptr<TPortionInfo>>& portions, const TSnapshot& snapshot) {
    TMap<TSnapshot, std::vector<std::shared_ptr<TPortionInfo>>> out;
    for (const auto& [portion, portionInfo] : portions) {
        if (portionInfo->Empty()) {
            continue;
        }

        TSnapshot recSnapshot = portionInfo->GetMinSnapshot();
        TSnapshot recXSnapshot = portionInfo->GetRemoveSnapshot();

        bool visible = (recSnapshot <= snapshot);
        if (recXSnapshot.GetPlanStep()) {
            visible = visible && snapshot < recXSnapshot;
        }

        if (visible) {
            out[recSnapshot].push_back(portionInfo);
        }
        AFL_TRACE(NKikimrServices::TX_COLUMNSHARD)("event", "GroupPortionsBySnapshot")("analyze_portion", portionInfo->DebugString())("visible", visible)("snapshot", snapshot.DebugString());
    }
    return out;
}

std::shared_ptr<TSelectInfo> TColumnEngineForLogs::Select(ui64 pathId, TSnapshot snapshot,
                                                          const THashSet<ui32>& columnIds,
                                                          const TPKRangesFilter& pkRangesFilter) const
{
    auto out = std::make_shared<TSelectInfo>();
    if (!PathGranules.contains(pathId)) {
        return out;
    }

    auto& pathGranules = PathGranules.find(pathId)->second;
    if (pathGranules.empty()) {
        return out;
    }
    out->Granules.reserve(pathGranules.size());
    // TODO: out.Portions.reserve()
    std::optional<TMarksMap::const_iterator> previousIterator;
    const bool compositeMark = UseCompositeMarks();

    for (auto&& filter : pkRangesFilter) {
        std::optional<NArrow::TReplaceKey> indexKeyFrom = filter.KeyFrom(GetIndexKey());
        std::optional<NArrow::TReplaceKey> indexKeyTo = filter.KeyTo(GetIndexKey());

        std::shared_ptr<arrow::Scalar> keyFrom;
        std::shared_ptr<arrow::Scalar> keyTo;
        if (indexKeyFrom) {
            keyFrom = NArrow::TReplaceKey::ToScalar(*indexKeyFrom, 0);
        }
        if (indexKeyTo) {
            keyTo = NArrow::TReplaceKey::ToScalar(*indexKeyTo, 0);
        }

        auto it = pathGranules.begin();
        if (keyFrom) {
            it = pathGranules.lower_bound(*keyFrom);
            if (it != pathGranules.begin()) {
                if (it == pathGranules.end() || compositeMark || *keyFrom != it->first) {
                    // TODO: better check if we really need an additional granule before the range
                    --it;
                }
            }
        }

        if (previousIterator && (previousIterator == pathGranules.end() || it->first < (*previousIterator)->first)) {
            it = *previousIterator;
        }
        for (; it != pathGranules.end(); ++it) {
            auto& mark = it->first;
            ui64 granule = it->second;
            if (keyTo && mark > *keyTo) {
                break;
            }

            auto it = Granules.find(granule);
            Y_VERIFY(it != Granules.end());
            auto& spg = it->second;
            Y_VERIFY(spg);
            auto& portions = spg->GetPortions();
            bool granuleHasDataForSnaphsot = false;

            TMap<TSnapshot, std::vector<std::shared_ptr<TPortionInfo>>> orderedPortions = GroupPortionsBySnapshot(portions, snapshot);
            for (auto& [snap, vec] : orderedPortions) {
                for (const auto& portionInfo : vec) {
                    TPortionInfo outPortion = portionInfo->CopyWithFilteredColumns(columnIds);
                    Y_VERIFY(outPortion.Produced());
                    if (!pkRangesFilter.IsPortionInUsage(outPortion, VersionedIndex.GetLastSchema()->GetIndexInfo())) {
                        AFL_TRACE(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "portion_skipped")
                            ("granule", granule)("portion", portionInfo->GetPortion());
                        continue;
                    } else {
                        AFL_TRACE(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "portion_selected")
                            ("granule", granule)("portion", portionInfo->GetPortion());
                    }
                    out->Portions.emplace_back(std::move(outPortion));
                    granuleHasDataForSnaphsot = true;
                }
            }

            if (granuleHasDataForSnaphsot) {
                out->Granules.push_back(spg->Record);
            }
        }
        previousIterator = it;
    }

    return out;
}

std::unique_ptr<TCompactionInfo> TColumnEngineForLogs::Compact(const TCompactionLimits& limits, const THashSet<ui64>& busyGranuleIds) {
    const auto filter = [&](const ui64 granuleId) {
        if (busyGranuleIds.contains(granuleId)) {
            return false;
        }
        return GetGranulePtrVerified(granuleId)->NeedCompaction(limits);
    };
    auto gCompaction = GranulesStorage->GetGranuleForCompaction(filter);
    if (!gCompaction) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "no_granule_for_compaction");
        SignalCounters.NoCompactGranulesSelection->Add(1);
        return {};
    }
    std::shared_ptr<TGranuleMeta> compactGranule = GetGranulePtrVerified(*gCompaction);

    if (compactGranule->IsOverloaded(limits)) {
        SignalCounters.CompactOverloadGranulesSelection->Add(1);
    }
    const auto compactionType = compactGranule->GetCompactionType(limits);
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "take_granule")("granule", compactGranule->DebugString())("compaction", compactionType);
    switch (compactionType) {
        case TGranuleAdditiveSummary::ECompactionClass::NoCompaction:
        case TGranuleAdditiveSummary::ECompactionClass::WaitInternal:
        {
            SignalCounters.NoCompactGranulesSelection->Add(1);
            return {};
        }
        case TGranuleAdditiveSummary::ECompactionClass::Split:
        {
            SignalCounters.SplitCompactGranulesSelection->Add(1);
            return std::make_unique<TCompactionInfo>(compactGranule, false);
        }
        case TGranuleAdditiveSummary::ECompactionClass::Internal:
        {
            SignalCounters.InternalCompactGranulesSelection->Add(1);
            return std::make_unique<TCompactionInfo>(compactGranule, true);
        }
    }
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

TColumnEngineForLogs::TTieringProcessContext::TTieringProcessContext(const ui64 maxEvictBytes, std::shared_ptr<TTTLColumnEngineChanges> changes, const THashSet<ui64>& busyGranules)
    : Now(TlsActivationContext ? AppData()->TimeProvider->Now() : TInstant::Now())
    , MaxEvictBytes(maxEvictBytes)
    , Changes(changes)
    , BusyGranules(busyGranules)
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
