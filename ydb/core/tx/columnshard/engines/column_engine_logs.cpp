#include "column_engine_logs.h"
#include "filter.h"
#include "index_logic_logs.h"
#include "indexed_read_data.h"

#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>
#include <ydb/core/formats/arrow/one_batch_input_stream.h>
#include <ydb/core/formats/arrow/merging_sorted_input_stream.h>
#include <ydb/library/conclusion/status.h>

#include <concepts>

namespace NKikimr::NOlap {

namespace {

TConclusionStatus InitInGranuleMerge(const TMark& granuleMark, std::vector<TPortionInfo>& portions, const TCompactionLimits& limits,
                        TColumnEngineForLogs::TMarksGranules& marksGranules) {
    ui32 insertedCount = 0;

    THashSet<ui64> filtered;
    THashSet<ui64> goodCompacted;
    THashSet<ui64> nextToGood;
    {
        TMap<NArrow::TReplaceKey, std::vector<const TPortionInfo*>> points;

        for (const auto& portionInfo : portions) {
            if (portionInfo.IsInserted()) {
                ++insertedCount;
            } else if (portionInfo.BlobsSizes().second >= limits.GoodBlobSize) {
                goodCompacted.insert(portionInfo.Portion());
            }

            const NArrow::TReplaceKey& start = portionInfo.IndexKeyStart();
            const NArrow::TReplaceKey& end = portionInfo.IndexKeyEnd();

            points[start].push_back(&portionInfo);
            points[end].push_back(nullptr);
        }

        ui32 countInBucket = 0;
        ui64 bucketStartPortion = 0;
        bool isGood = false;
        int sum = 0;
        for (const auto& [key, vec] : points) {
            for (const auto* portionInfo : vec) {
                if (portionInfo) {
                    ++sum;
                    ui64 currentPortion = portionInfo->Portion();
                    if (!bucketStartPortion) {
                        bucketStartPortion = currentPortion;
                    }
                    ++countInBucket;

                    ui64 prevIsGood = isGood;
                    isGood = goodCompacted.contains(currentPortion);
                    if (prevIsGood && !isGood) {
                        nextToGood.insert(currentPortion);
                    }
                } else {
                    --sum;
                }
            }

            if (!sum) { // count(start) == count(end), start new range
                Y_VERIFY(bucketStartPortion);

                if (countInBucket == 1) {
                    // We do not want to merge big compacted portions with inserted ones if there's no intersections.
                    if (isGood) {
                        filtered.insert(bucketStartPortion);
                    }
                }
                countInBucket = 0;
                bucketStartPortion = 0;
            }
        }
    }

    Y_VERIFY(insertedCount);

    // Nothing to filter. Leave portions as is, no borders needed.
    if (filtered.empty() && goodCompacted.empty()) {
        return TConclusionStatus::Success();
    }

    // It's a map for SliceIntoGranules(). We use fake granule ids here to slice batch with borders.
    // We could merge inserted portions alltogether and slice result with filtered borders to prevent intersections.
    std::vector<TMark> borders;
    borders.push_back(granuleMark);

    std::vector<TPortionInfo> tmp;
    tmp.reserve(portions.size());
    for (auto& portionInfo : portions) {
        ui64 curPortion = portionInfo.Portion();

        // Prevent merge of compacted portions with no intersections
        if (filtered.contains(curPortion)) {
            const auto& start = portionInfo.IndexKeyStart();
            borders.emplace_back(TMark(start));
        } else {
            // nextToGood borders potentially split good compacted portions into 2 parts:
            // the first one without intersections and the second with them
            if (goodCompacted.contains(curPortion) || nextToGood.contains(curPortion)) {
                const auto& start = portionInfo.IndexKeyStart();
                borders.emplace_back(TMark(start));
            }

            tmp.emplace_back(std::move(portionInfo));
        }
    }
    tmp.swap(portions);

    if (borders.size() == 1) {
        Y_VERIFY(borders[0] == granuleMark);
        borders.clear();
    }

    marksGranules = TColumnEngineForLogs::TMarksGranules(std::move(borders));
    return TConclusionStatus::Success();
}

} // namespace


TColumnEngineForLogs::TMarksGranules::TMarksGranules(std::vector<TPair>&& marks) noexcept
    : Marks(std::move(marks))
{
    Y_VERIFY_DEBUG(std::is_sorted(Marks.begin(), Marks.end()));
}

TColumnEngineForLogs::TMarksGranules::TMarksGranules(std::vector<TMark>&& points) {
    std::sort(points.begin(), points.end());

    Marks.reserve(points.size());

    for (size_t i = 0, end = points.size(); i != end; ++i) {
        Marks.emplace_back(std::move(points[i]), i + 1);
    }
}

TColumnEngineForLogs::TMarksGranules::TMarksGranules(const TSelectInfo& selectInfo) {
    Marks.reserve(selectInfo.Granules.size());

    for (const auto& rec : selectInfo.Granules) {
        Marks.emplace_back(std::make_pair(rec.Mark, rec.Granule));
    }

    std::sort(Marks.begin(), Marks.end(), [](const TPair& a, const TPair& b) {
        return a.first < b.first;
    });
}

bool TColumnEngineForLogs::TMarksGranules::MakePrecedingMark(const TIndexInfo& indexInfo) {
    ui64 minGranule = 0;
    TMark minMark(TMark::MinBorder(indexInfo.GetIndexKey()));
    if (Marks.empty()) {
        Marks.emplace_back(std::move(minMark), minGranule);
        return true;
    }

    if (minMark < Marks[0].first) {
        std::vector<TPair> copy;
        copy.reserve(Marks.size() + 1);
        copy.emplace_back(std::move(minMark), minGranule);
        for (auto&& [mark, granule] : Marks) {
            copy.emplace_back(std::move(mark), granule);
        }
        Marks.swap(copy);
        return true;
    }
    return false;
}

THashMap<ui64, std::shared_ptr<arrow::RecordBatch>>
TColumnEngineForLogs::TMarksGranules::SliceIntoGranules(const std::shared_ptr<arrow::RecordBatch>& batch,
                                                        const TIndexInfo& indexInfo)
{
    return NOlap::TIndexLogicBase::SliceIntoGranules(batch, Marks, indexInfo);
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

    ui64 granule = portionInfo.Granule();
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
        metadataBytes += rec.Metadata.size();
        blobs.insert(rec.BlobRange.BlobId);
        deltaStats.BytesByColumn[rec.ColumnId] += rec.BlobRange.Size;
    }
    for (auto& rec : portionInfo.Meta.ColumnMeta) {
        deltaStats.RawBytesByColumn[rec.first] += rec.second.RawBytes;
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

    Y_VERIFY(!exPortionInfo || exPortionInfo->Meta.Produced != TPortionMeta::EProduced::UNSPECIFIED);
    Y_VERIFY(portionInfo.Meta.Produced != TPortionMeta::EProduced::UNSPECIFIED);

    TColumnEngineStats::TPortionsStats& srcStats = exPortionInfo
        ? (exPortionInfo->IsActive()
            ? engineStats.StatsByType[exPortionInfo->Meta.Produced]
            : engineStats.StatsByType[TPortionMeta::EProduced::INACTIVE])
        : engineStats.StatsByType[portionInfo.Meta.Produced];
    TColumnEngineStats::TPortionsStats& stats = portionInfo.IsActive()
        ? engineStats.StatsByType[portionInfo.Meta.Produced]
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
            UpdatePortionStats(portionInfo, EStatsUpdateType::ADD);
            if (portionInfo.CheckForCleanup()) {
                CleanupPortions.emplace(portionInfo.GetAddress());
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
        Y_VERIFY(SetGranule(rec, true));
    };

    return GranulesTable->Load(db, callback);
}

bool TColumnEngineForLogs::LoadColumns(IDbWrapper& db, THashSet<TUnifiedBlobId>& lostBlobs) {
    return ColumnsTable->Load(db, [&](const TColumnRecord& rec) {
        auto& indexInfo = GetIndexInfo();
        Y_VERIFY(rec.Valid());
        // Do not count the blob as lost since it exists in the index.
        lostBlobs.erase(rec.BlobRange.BlobId);
        // Locate granule and append the record.
        if (const auto gi = Granules.find(rec.Granule); gi != Granules.end()) {
            gi->second->AddColumnRecord(indexInfo, rec);
        } else {
            Y_VERIFY(false);
        }
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

std::shared_ptr<TColumnEngineChanges> TColumnEngineForLogs::StartInsert(const TCompactionLimits& limits, std::vector<TInsertedData>&& dataToIndex) {
    Y_VERIFY(dataToIndex.size());

    auto changes = TChanges::BuildInsertChanges(DefaultMark(), std::move(dataToIndex), LastSnapshot, limits);
    ui32 reserveGranules = 0;
    for (const auto& data : changes->DataToIndex) {
        const ui64 pathId = data.PathId;

        if (changes->PathToGranule.contains(pathId)) {
            continue;
        }

        if (PathGranules.contains(pathId)) {
            // Abort inserting if the path has overloaded granules.
            if (GranulesStorage->GetOverloaded(pathId)) {
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
        changes->ReservedGranuleIds = reserveGranules;
        LastGranule += reserveGranules;
    }

    return changes;
}

std::shared_ptr<TColumnEngineChanges> TColumnEngineForLogs::StartCompaction(std::unique_ptr<TCompactionInfo>&& info,
                                                                            const TCompactionLimits& limits) {
    Y_VERIFY(info);

    auto changes = TChanges::BuildCompactionChanges(DefaultMark(), std::move(info), limits, LastSnapshot);

    const auto& granuleInfo = changes->CompactionInfo->GetObject<TGranuleMeta>();

    changes->SwitchedPortions.reserve(granuleInfo.GetPortions().size());
    // Collect active portions for the granule.
    for (const auto& [_, portionInfo] : granuleInfo.GetPortions()) {
        if (portionInfo.IsActive()) {
            changes->SwitchedPortions.push_back(portionInfo);
            Y_VERIFY(portionInfo.Granule() == granuleInfo.GetGranuleId());
        }
    }

    const ui64 pathId = granuleInfo.Record.PathId;
    Y_VERIFY(PathGranules.contains(pathId));
    // Locate mark for the granule.
    for (const auto& [mark, pathGranule] : PathGranules[pathId]) {
        if (pathGranule == granuleInfo.GetGranuleId()) {
            changes->SrcGranule = TChanges::TSrcGranule(pathId, granuleInfo.GetGranuleId(), mark);
            break;
        }
    }
    Y_VERIFY(changes->SrcGranule);

    if (changes->CompactionInfo->InGranule()) {
        auto mergeInitResult = InitInGranuleMerge(changes->SrcGranule->Mark, changes->SwitchedPortions, limits, changes->MergeBorders);
        if (!mergeInitResult) {
            // Return granule to Compaction list. This is equal to single compaction worker behavior.
            changes->CompactionInfo->CompactionCanceled("cannot init in granule merge: " + mergeInitResult.GetErrorMessage());
            return {};
        }
    }
    NYDBTest::TControllers::GetColumnShardController()->OnStartCompaction(changes);
    Y_VERIFY(!changes->SwitchedPortions.empty());
    return changes;
}

std::shared_ptr<TColumnEngineChanges> TColumnEngineForLogs::StartCleanup(const TSnapshot& snapshot,
                                                                         const TCompactionLimits& limits,
                                                                         THashSet<ui64>& pathsToDrop,
                                                                         ui32 maxRecords) {
    auto changes = TChanges::BuildCleanupChanges(DefaultMark(), snapshot, limits);
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
                affectedRecords += info.NumRecords();
                changes->PortionsToDrop.push_back(info);
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
            Y_VERIFY(portionInfo->CheckForCleanup());
            ++it;
        }
    }

    return changes;
}

std::shared_ptr<TColumnEngineChanges> TColumnEngineForLogs::StartTtl(const THashMap<ui64, TTiering>& pathEviction, const std::shared_ptr<arrow::Schema>& schema,
                                                                     ui64 maxEvictBytes) {
    if (pathEviction.empty()) {
        return {};
    }

    auto changes = TChanges::BuildTtlChanges(DefaultMark());
    ui64 evicttionSize = 0;
    bool allowEviction = true;
    ui64 dropBlobs = 0;
    bool allowDrop = true;

    auto& indexInfo = GetIndexInfo();
    for (const auto& [pathId, ttl] : pathEviction) {
        if (!PathGranules.contains(pathId)) {
            continue; // It's not an error: allow TTL over multiple shards with different pathIds presented
        }

        auto expireTimestamp = ttl.EvictScalar(schema);
        Y_VERIFY(expireTimestamp);

        auto ttlColumnNames = ttl.GetTtlColumns();
        Y_VERIFY(ttlColumnNames.size() == 1); // TODO: support different ttl columns
        ui32 ttlColumnId = indexInfo.GetColumnId(*ttlColumnNames.begin());

        for (const auto& [ts, granule] : PathGranules[pathId]) {
            auto spg = Granules[granule];
            Y_VERIFY(spg);

            for (auto& [portion, info] : spg->GetPortions()) {
                if (!info.IsActive()) {
                    continue;
                }

                allowEviction = (evicttionSize <= maxEvictBytes);
                allowDrop = (dropBlobs <= TCompactionLimits::MAX_BLOBS_TO_DELETE);
                bool tryEvictPortion = allowEviction && ttl.HasTiers()
                    && info.EvictReady(TCompactionLimits::EVICT_HOT_PORTION_BYTES);

                if (auto max = info.MaxValue(ttlColumnId)) {
                    bool keep = NArrow::ScalarLess(expireTimestamp, max);
                    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "scalar_less_result")("keep", keep)("tryEvictPortion", tryEvictPortion)("allowDrop", allowDrop);
                    if (keep && tryEvictPortion) {
                        TString tierName;
                        for (auto& tierRef : ttl.GetOrderedTiers()) { // TODO: lower/upper_bound + move into TEviction
                            auto& tierInfo = tierRef.Get();
                            if (!indexInfo.AllowTtlOverColumn(tierInfo.GetEvictColumnName())) {
                                SignalCounters.OnPortionNoTtlColumn(info.BlobsBytes());
                                continue; // Ignore tiers with bad ttl column
                            }
                            if (NArrow::ScalarLess(tierInfo.EvictScalar(schema), max)) {
                                tierName = tierInfo.GetName();
                            } else {
                                break;
                            }
                        }
                        if (info.TierName != tierName) {
                            evicttionSize += info.BlobsSizes().first;
                            bool needExport = ttl.NeedExport(tierName);
                            changes->PortionsToEvict.emplace_back(
                                info, TPortionEvictionFeatures(tierName, pathId, needExport));
                            SignalCounters.OnPortionToEvict(info.BlobsBytes());
                        }
                    }
                    if (!keep && allowDrop) {
                        dropBlobs += info.NumRecords();
                        changes->PortionsToDrop.push_back(info);
                        SignalCounters.OnPortionToDrop(info.BlobsBytes());
                    }
                } else {
                    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "scalar_less_not_max");
                    SignalCounters.OnPortionNoBorder(info.BlobsBytes());
                }
            }
        }
    }

    if (changes->PortionsToDrop.empty() &&
        changes->PortionsToEvict.empty()) {
        return {};
    }

    if (!allowEviction || !allowDrop) {
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

bool TColumnEngineForLogs::ApplyChanges(IDbWrapper& db, std::shared_ptr<TColumnEngineChanges> indexChanges,
                                        const TSnapshot& snapshot) noexcept {
    auto changes = std::static_pointer_cast<TChanges>(indexChanges);

    // Update tmp granules with real ids
    auto granuleRemap = changes->TmpToNewGranules(LastGranule);
    ui64 portion = LastPortion;
    for (auto& portionInfo : changes->AppendedPortions) {
        portionInfo.UpdateRecords(++portion, granuleRemap);

        TPortionMeta::EProduced produced = TPortionMeta::INSERTED;
        // If it's a split compaction with moves appended portions are INSERTED (could have overlaps with others)
        if (changes->IsCompaction() && changes->PortionsToMove.empty()) {
            Y_VERIFY(changes->CompactionInfo);
            produced = changes->CompactionInfo->InGranule() ? TPortionMeta::COMPACTED : TPortionMeta::SPLIT_COMPACTED;
        }

        portionInfo.UpdateRecordsMeta(produced);
    }

    for (auto& [portionInfo, _] : changes->PortionsToEvict) {
        portionInfo.UpdateRecordsMeta(TPortionMeta::EVICTED);
    }

    for (auto& [_, id] : changes->PortionsToMove) {
        Y_VERIFY(granuleRemap.contains(id));
        id = granuleRemap[id];
    }

    // Set x-snapshot to switched portions
    if (changes->IsCompaction()) {
        Y_VERIFY(changes->CompactionInfo);
        for (auto& portionInfo : changes->SwitchedPortions) {
            Y_VERIFY(portionInfo.IsActive());
            portionInfo.SetStale(snapshot);
        }
    }

    if (!ApplyChanges(db, *changes, snapshot, false)) { // validate only
        if (changes->CompactionInfo) {
            changes->CompactionInfo->CompactionFailed("cannot apply changes");
        }
        return false;
    }
    bool ok = ApplyChanges(db, *changes, snapshot, true);
    Y_VERIFY(ok);
    if (changes->CompactionInfo) {
        changes->CompactionInfo->CompactionFinished();
    }

    // Save updated granules for cleanup
    if (changes->IsCompaction()) {
        Y_VERIFY(changes->CompactionInfo);
        for (auto& portionInfo : changes->SwitchedPortions) {
            CleanupPortions.insert(portionInfo.GetAddress());
        }
    }
    return true;
}

bool TColumnEngineForLogs::ApplyChanges(IDbWrapper& db, const TChanges& changes, const TSnapshot& snapshot, bool apply) noexcept {
    const std::vector<TPortionInfo>* switchedPortions = nullptr;
    if (changes.IsCompaction()) {
        Y_VERIFY(changes.CompactionInfo);
        switchedPortions = &changes.SwitchedPortions;

        if (!changes.CompactionInfo->InGranule()) {
            if (changes.NewGranules.empty()) {
                LOG_S_ERROR("Cannot split granule " << changes.SrcGranule->Granule << " at tablet " << TabletId);
                return false;
            }
        }
    } else if (changes.IsInsert() && !CanInsert(changes, snapshot)) {
        LOG_S_INFO("Cannot insert " << changes.AppendedPortions.size() << " portions at tablet " << TabletId);
        return false;
    }

    // Save new granules

    for (auto& [granule, p] : changes.NewGranules) {
        ui64 pathId = p.first;
        TMark mark = p.second;
        TGranuleRecord rec(pathId, granule, snapshot, mark.GetBorder());

        if (!SetGranule(rec, apply)) {
            LOG_S_ERROR("Cannot insert granule " << rec << " at tablet " << TabletId);
            return false;
        }
        if (apply) {
            GranulesTable->Write(db, rec);
        }
    }
    auto g = GranulesStorage->StartPackModification();
    // Update old portions (set stale snapshot)

    if (switchedPortions) {
        for (auto& portionInfo : *switchedPortions) {
            Y_VERIFY(!portionInfo.Empty());
            Y_VERIFY(!portionInfo.IsActive());

            ui64 granule = portionInfo.Granule();
            ui64 portion = portionInfo.Portion();
            if (!Granules.contains(granule)) {
                LOG_S_ERROR("Cannot update unknown granule " << granule << " at tablet " << TabletId);
                return false;
            }
            if (!Granules[granule]->GetPortions().contains(portion)) {
                LOG_S_ERROR("Cannot update unknown portion " << portionInfo << " at tablet " << TabletId);
                return false;
            }

            auto& granuleStart = Granules[granule]->Record.Mark;

            if (!apply) { // granule vs portion minPK
                const auto& portionStart = portionInfo.IndexKeyStart();
                if (portionStart < granuleStart) {
                    LOG_S_ERROR("Cannot update invalid portion " << portionInfo
                        << " start: " << TMark(portionStart).ToString()
                        << " granule start: " << TMark(granuleStart).ToString() << " at tablet " << TabletId);
                    return false;
                }
            }

            // In case of race with eviction portion could become evicted
            const TPortionInfo& oldInfo = Granules[granule]->GetPortionVerified(portion);

            if (!UpsertPortion(portionInfo, apply, &oldInfo)) {
                LOG_S_ERROR("Cannot update portion " << portionInfo << " at tablet " << TabletId);
                return false;
            }

            if (apply) {
                for (auto& record : portionInfo.Records) {
                    ColumnsTable->Write(db, record);
                }
            }
        }
    }

    // Update evicted portions
    // There could be race between compaction and eviction. Allow compaction and disallow eviction in this case.

    for (auto& [info, _] : changes.PortionsToEvict) {
        const auto& portionInfo = info;
        Y_VERIFY(!portionInfo.Empty());
        Y_VERIFY(portionInfo.IsActive());

        ui64 granule = portionInfo.Granule();
        ui64 portion = portionInfo.Portion();
        if (!Granules.contains(granule) || !Granules[granule]->GetPortions().contains(portion)) {
            LOG_S_ERROR("Cannot evict unknown portion " << portionInfo << " at tablet " << TabletId);
            return false;
        }

        // In case of race with compaction portion could become inactive
        const TPortionInfo& oldInfo = Granules[granule]->GetPortionVerified(portion);
        if (!oldInfo.IsActive()) {
            LOG_S_WARN("Cannot evict inactive portion " << oldInfo << " at tablet " << TabletId);
            return false;
        }
        Y_VERIFY(portionInfo.TierName != oldInfo.TierName);

        if (!UpsertPortion(portionInfo, apply, &oldInfo)) {
            LOG_S_ERROR("Cannot evict portion " << portionInfo << " at tablet " << TabletId);
            return false;
        }

        if (apply) {
            for (auto& record : portionInfo.Records) {
                ColumnsTable->Write(db, record);
            }
        }
    }

    // Move portions in granules (zero-copy switch + append into new granules)

    for (auto& [info, dstGranule] : changes.PortionsToMove) {
        const auto& portionInfo = info;

        ui64 granule = portionInfo.Granule();
        ui64 portion = portionInfo.Portion();
        if (!Granules.contains(granule) || !Granules[granule]->GetPortions().contains(portion)) {
            LOG_S_ERROR("Cannot move unknown portion " << portionInfo << " at tablet " << TabletId);
            return false;
        }

        // In case of race with eviction portion could become evicted
        const TPortionInfo oldInfo = Granules[granule]->GetPortionVerified(portion);

        if (!ErasePortion(portionInfo, apply, false)) {
            LOG_S_ERROR("Cannot erase moved portion " << portionInfo << " at tablet " << TabletId);
            return false;
        }

        TPortionInfo moved = portionInfo;
        moved.SetGranule(dstGranule);
        if (!UpsertPortion(moved, apply, &oldInfo)) {
            LOG_S_ERROR("Cannot insert moved portion " << moved << " at tablet " << TabletId);
            return false;
        }
        if (apply) {
            for (auto& record : portionInfo.Records) {
                ColumnsTable->Erase(db, record);
            }
            for (auto& record : moved.Records) {
                ColumnsTable->Write(db, record);
            }
        }
    }

    // Drop old portions

    for (auto& portionInfo : changes.PortionsToDrop) {
        if (!ErasePortion(portionInfo, apply)) {
            LOG_S_ERROR("Cannot erase portion " << portionInfo << " at tablet " << TabletId);
            return false;
        }
        if (apply) {
            for (auto& record : portionInfo.Records) {
                ColumnsTable->Erase(db, record);
            }
        }
    }

    // Save new portions (their column records)

    for (auto& portionInfo : changes.AppendedPortions) {
        Y_VERIFY(!portionInfo.Empty());

        if (!apply) {
            ui64 granule = portionInfo.Records[0].Granule;
            if (!Granules.contains(granule) && !changes.NewGranules.contains(granule)) {
                LOG_S_ERROR("Cannot write portion with unknown granule " << portionInfo << " at tablet " << TabletId);
                return false;
            }

            // granule vs portion minPK
            NArrow::TReplaceKey granuleStart = Granules.contains(granule)
                ? Granules[granule]->Record.Mark
                : changes.NewGranules.find(granule)->second.second.GetBorder();

            const auto& portionStart = portionInfo.IndexKeyStart();
            if (portionStart < granuleStart) {
                LOG_S_ERROR("Cannot insert invalid portion " << portionInfo
                    << " start: " << TMark(portionStart).ToString()
                    << " granule start: " << TMark(granuleStart).ToString() << " at tablet " << TabletId);
                return false;
            }
        }

        if (!UpsertPortion(portionInfo, apply)) {
            LOG_S_ERROR("Cannot insert portion " << portionInfo << " at tablet " << TabletId);
            return false;
        }

        if (apply) {
            for (auto& record : portionInfo.Records) {
                ColumnsTable->Write(db, record);
            }
        }
    }

    // Write counters

    if (apply) {
        LastPortion += changes.AppendedPortions.size();
        // Insert use early granules allocation. LastGranule increased there.
        if (!changes.IsInsert()) {
            LastGranule += changes.NewGranules.size();
        }
        CountersTable->Write(db, LAST_PORTION, LastPortion);
        CountersTable->Write(db, LAST_GRANULE, LastGranule);

        if (LastSnapshot < snapshot) {
            LastSnapshot = snapshot;
            CountersTable->Write(db, LAST_PLAN_STEP, LastSnapshot.GetPlanStep());
            CountersTable->Write(db, LAST_TX_ID, LastSnapshot.GetTxId());
        }
    }
    return true;
}

void TColumnEngineForLogs::FreeLocks(std::shared_ptr<TColumnEngineChanges> indexChanges) {
    auto changes = std::static_pointer_cast<TChanges>(indexChanges);

    if (changes->CompactionInfo) {
        changes->CompactionInfo->GetObject<TGranuleMeta>().AllowedInsertion();
    }
}

bool TColumnEngineForLogs::SetGranule(const TGranuleRecord& rec, bool apply) {
    const TMark mark(rec.Mark);

    if (apply) {
        // There should be only one granule with (PathId, Mark).
        Y_VERIFY(PathGranules[rec.PathId].emplace(mark, rec.Granule).second);

        // Allocate granule info and ensure that there is no granule with same id inserted before.
        Y_VERIFY(Granules.emplace(rec.Granule, std::make_shared<TGranuleMeta>(rec, GranulesStorage, SignalCounters.RegisterGranuleDataCounters())).second);
    } else {
        // Granule with same id already exists.
        if (Granules.contains(rec.Granule)) {
            return false;
        }

        // Granule with same (PathId, Mark) already exists.
        if (PathGranules.contains(rec.PathId) && PathGranules[rec.PathId].contains(mark)) {
            return false;
        }
    }

    return true;
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

bool TColumnEngineForLogs::UpsertPortion(const TPortionInfo& portionInfo, bool apply, const TPortionInfo* exInfo) {
    ui64 granule = portionInfo.Granule();

    if (!apply) {
        for (auto& record : portionInfo.Records) {
            if (granule != record.Granule || !record.Valid()) {
                return false;
            }
        }
        return true;
    }

    Y_VERIFY(portionInfo.Valid());
    auto& spg = Granules[granule];
    Y_VERIFY(spg);

    if (exInfo) {
        UpdatePortionStats(portionInfo, EStatsUpdateType::DEFAULT, exInfo);
    } else {
        UpdatePortionStats(portionInfo, EStatsUpdateType::ADD);
    }

    spg->UpsertPortion(portionInfo);
    return true; // It must return true if (apply == true)
}

bool TColumnEngineForLogs::ErasePortion(const TPortionInfo& portionInfo, bool apply, bool updateStats) {
    Y_VERIFY(!portionInfo.Empty());
    const ui64 portion = portionInfo.Portion();
    auto it = Granules.find(portionInfo.Granule());
    Y_VERIFY(it != Granules.end());
    auto& spg = it->second;
    Y_VERIFY(spg);
    auto* p = spg->GetPortionPointer(portion);

    if (!p) {
        LOG_S_WARN("Portion erased already " << portionInfo << " at tablet " << TabletId);
    } else if (apply) {
        if (updateStats) {
            UpdatePortionStats(*p, EStatsUpdateType::ERASE);
        }
        Y_VERIFY(spg->ErasePortion(portion));
    }
    return true;
}

bool TColumnEngineForLogs::CanInsert(const TChanges& changes, const TSnapshot& commitSnap) const {
    Y_UNUSED(commitSnap);
    // Does insert have granule in split?
    for (const auto& portionInfo : changes.AppendedPortions) {
        Y_VERIFY(!portionInfo.Empty());
        auto g = GetGranuleOptional(portionInfo.Granule());
        if (!!g && !g->IsInsertAllowed()) {
            LOG_S_NOTICE("Cannot insert into splitting granule " << portionInfo.Granule() << " at tablet " << TabletId);
            return false;
        }
    }
    // Does insert have already splitted granule?
    for (const auto& [pathId, tsGranules] : changes.PathToGranule) {
        if (PathGranules.contains(pathId)) {
            const auto& actualGranules = PathGranules.find(pathId)->second;
            size_t expectedSize = tsGranules.size();
            if (actualGranules.size() != expectedSize) {
                LOG_S_DEBUG("Cannot insert into splitted granules (actual: " << actualGranules.size()
                    << ", expected: " << expectedSize << ") at tablet " << TabletId);
                return false;
            }
        }
    }
    return true;
}

static TMap<TSnapshot, std::vector<const TPortionInfo*>> GroupPortionsBySnapshot(const THashMap<ui64, TPortionInfo>& portions, const TSnapshot& snapshot) {
    TMap<TSnapshot, std::vector<const TPortionInfo*>> out;
    for (const auto& [portion, portionInfo] : portions) {
        if (portionInfo.Empty()) {
            continue;
        }

        TSnapshot recSnapshot = portionInfo.GetSnapshot();
        TSnapshot recXSnapshot = portionInfo.GetXSnapshot();

        bool visible = (recSnapshot <= snapshot);
        if (recXSnapshot.GetPlanStep()) {
            visible = visible && snapshot < recXSnapshot;
        }

        if (visible) {
            out[recSnapshot].push_back(&portionInfo);
        }
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

            TMap<TSnapshot, std::vector<const TPortionInfo*>> orderedPortions = GroupPortionsBySnapshot(portions, snapshot);
            for (auto& [snap, vec] : orderedPortions) {
                for (const auto* portionInfo : vec) {
                    TPortionInfo outPortion;
                    outPortion.Meta = portionInfo->Meta;
                    outPortion.Records.reserve(columnIds.size());

                    for (auto& rec : portionInfo->Records) {
                        Y_VERIFY(rec.Valid());
                        if (columnIds.contains(rec.ColumnId)) {
                            outPortion.Records.push_back(rec);
                        }
                    }
                    Y_VERIFY(outPortion.Produced());
                    if (!pkRangesFilter.IsPortionInUsage(outPortion, GetIndexInfo())) {
                        AFL_TRACE(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "portion_skipped")
                            ("granule", granule)("portion", portionInfo->Portion());
                        continue;
                    } else {
                        AFL_TRACE(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "portion_selected")
                            ("granule", granule)("portion", portionInfo->Portion());
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

} // namespace NKikimr::NOlap
