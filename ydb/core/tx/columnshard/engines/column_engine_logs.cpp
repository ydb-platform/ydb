#include "column_engine_logs.h"
#include "indexed_read_data.h"
#include "filter.h"

#include <ydb/core/formats/one_batch_input_stream.h>
#include <ydb/core/formats/merging_sorted_input_stream.h>

#include <concepts>
#include <span>

namespace NKikimr::NOlap {

namespace {

std::shared_ptr<arrow::RecordBatch> GetEffectiveKey(const std::shared_ptr<arrow::RecordBatch>& batch,
                                                    const TIndexInfo& indexInfo) {
    // TODO: composite effective key
    auto columnName = indexInfo.GetPrimaryKey()[0].first;
    auto resBatch = NArrow::ExtractColumns(batch, {std::string(columnName.data(), columnName.size())});
    Y_VERIFY_S(resBatch, "No column '" << columnName << "' in batch " << batch->schema()->ToString());
    return resBatch;
}

arrow::ipc::IpcWriteOptions WriteOptions(const TCompression& compression) {
    auto& codec = compression.Codec;

    arrow::ipc::IpcWriteOptions options(arrow::ipc::IpcWriteOptions::Defaults());
    Y_VERIFY(arrow::util::Codec::IsAvailable(codec));
    arrow::Result<std::unique_ptr<arrow::util::Codec>> resCodec;
    if (compression.Level) {
        resCodec = arrow::util::Codec::Create(codec, *compression.Level);
        if (!resCodec.ok()) {
            resCodec = arrow::util::Codec::Create(codec);
        }
    } else {
        resCodec = arrow::util::Codec::Create(codec);
    }
    Y_VERIFY(resCodec.ok());

    options.codec.reset((*resCodec).release());
    options.use_threads = false;
    return options;
}

std::optional<NArrow::TReplaceKey> ExtractKey(const std::shared_ptr<TPredicate>& pkPredicate,
                                              const std::shared_ptr<arrow::Schema>& key) {
    if (pkPredicate) {
        Y_VERIFY(pkPredicate->Good());
        return NArrow::TReplaceKey::FromBatch(pkPredicate->Batch, key, 0);
    }
    return {};
}

// Although source batches are ordered only by PK (sorting key) resulting pathBatches are ordered by extended key.
// They have const snapshot columns that do not break sorting inside batch.
std::shared_ptr<arrow::RecordBatch> AddSpecials(const std::shared_ptr<arrow::RecordBatch>& srcBatch,
                                                const TIndexInfo& indexInfo, const TInsertedData& inserted)
{
    auto batch = TIndexInfo::AddSpecialColumns(srcBatch, inserted.PlanStep(), inserted.TxId());
    Y_VERIFY(batch);

    return NArrow::ExtractColumns(batch, indexInfo.ArrowSchemaWithSpecials());
}

bool UpdateEvictedPortion(TPortionInfo& portionInfo, const TIndexInfo& indexInfo, const THashMap<ui64, NKikimr::NOlap::TTiering>& tieringMap,
                          TPortionEvictionFeatures& evictFeatures, const THashMap<TBlobRange, TString>& srcBlobs,
                          TVector<TColumnRecord>& evictedRecords, TVector<TString>& newBlobs)
{
    Y_VERIFY(portionInfo.TierName != evictFeatures.TargetTierName);

    auto* tiering = tieringMap.FindPtr(evictFeatures.PathId);
    Y_VERIFY(tiering);
    auto compression = tiering->GetCompression(evictFeatures.TargetTierName);
    if (!compression) {
        // Noting to recompress. We have no other kinds of evictions yet.
        portionInfo.TierName = evictFeatures.TargetTierName;
        evictFeatures.DataChanges = false;
        return true;
    }

    Y_VERIFY(!evictFeatures.NeedExport);

    auto schema = indexInfo.ArrowSchemaWithSpecials();
    auto batch = portionInfo.AssembleInBatch(indexInfo, schema, srcBlobs);
    auto writeOptions = WriteOptions(*compression);

    TPortionInfo undo = portionInfo;
    size_t undoSize = newBlobs.size();

    for (auto& rec : portionInfo.Records) {
        auto colName = indexInfo.GetColumnName(rec.ColumnId);
        std::string name(colName.data(), colName.size());
        auto field = schema->GetFieldByName(name);

        auto blob = TPortionInfo::SerializeColumn(batch->GetColumnByName(name), field, writeOptions);
        if (blob.size() >= TPortionInfo::BLOB_BYTES_LIMIT) {
            portionInfo = undo;
            newBlobs.resize(undoSize);
            return false;
        }
        newBlobs.emplace_back(std::move(blob));
        rec.BlobRange = TBlobRange{};
    }

    for (auto& rec : undo.Records) {
        evictedRecords.emplace_back(std::move(rec));
    }

    portionInfo.AddMetadata(indexInfo, batch, evictFeatures.TargetTierName);
    return true;
}

TVector<TPortionInfo> MakeAppendedPortions(const ui64 pathId, const TIndexInfo& indexInfo,
                                           const THashMap<ui64, NKikimr::NOlap::TTiering>& tieringMap,
                                           const std::shared_ptr<arrow::RecordBatch> batch,
                                           const ui64 granule,
                                           const TSnapshot& minSnapshot,
                                           TVector<TString>& blobs) {
    Y_VERIFY(batch->num_rows());
    const auto schema = indexInfo.ArrowSchemaWithSpecials();
    TVector<TPortionInfo> out;

    TString tierName;
    TCompression compression = indexInfo.GetDefaultCompression();
    if (pathId) {
        if (auto* tiering = tieringMap.FindPtr(pathId)) {
            tierName = tiering->GetHottestTierName();
            if (const auto& tierCompression = tiering->GetCompression(tierName)) {
                compression = *tierCompression;
            }
        }
    }
    const auto writeOptions = WriteOptions(compression);

    std::shared_ptr<arrow::RecordBatch> portionBatch = batch;
    for (i32 pos = 0; pos < batch->num_rows();) {
        Y_VERIFY(portionBatch->num_rows());

        TPortionInfo portionInfo;
        portionInfo.Records.reserve(schema->num_fields());
        TVector<TString> portionBlobs;
        portionBlobs.reserve(schema->num_fields());

        // Serialize portion's columns into blobs

        bool ok = true;
        for (const auto& field : schema->fields()) {
            const auto& name = field->name();
            ui32 columnId = indexInfo.GetColumnId(TString(name.data(), name.size()));

            /// @warnign records are not valid cause of empty BlobId and zero Portion
            TColumnRecord record = TColumnRecord::Make(granule, columnId, minSnapshot, 0);
            auto blob = portionInfo.AddOneChunkColumn(portionBatch->GetColumnByName(name), field, std::move(record),
                                                      writeOptions);
            if (!blob.size()) {
                ok = false;
                break;
            }

            // TODO: combine small columns in one blob
            portionBlobs.emplace_back(std::move(blob));
        }

        if (ok) {
            portionInfo.AddMetadata(indexInfo, portionBatch, tierName);
            out.emplace_back(std::move(portionInfo));
            for (auto& blob : portionBlobs) {
                blobs.push_back(blob);
            }
            pos += portionBatch->num_rows();
            if (pos < batch->num_rows()) {
                portionBatch = batch->Slice(pos);
            }
        } else {
            const i64 halfLen = portionBatch->num_rows() / 2;
            Y_VERIFY(halfLen);
            portionBatch = batch->Slice(pos, halfLen);
        }
    }

    return out;
}

std::vector<std::shared_ptr<arrow::RecordBatch>> PortionsToBatches(const TIndexInfo& indexInfo,
                                                                   const TVector<TPortionInfo>& portions,
                                                                   const THashMap<TBlobRange, TString>& blobs,
                                                                   bool insertedOnly = false) {
    // TODO: schema changes
    const std::shared_ptr<arrow::Schema> schema = indexInfo.ArrowSchemaWithSpecials();

    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    batches.reserve(portions.size());

    for (auto& portionInfo : portions) {
        auto batch = portionInfo.AssembleInBatch(indexInfo, schema, blobs);
        if (!insertedOnly || portionInfo.IsInserted()) {
            batches.push_back(batch);
        }
    }
    return batches;
}

bool InitInGranuleMerge(const TMark& granuleMark, TVector<TPortionInfo>& portions, const TCompactionLimits& limits,
                        const TSnapshot& snap, TColumnEngineForLogs::TMarksGranules& marksGranules) {
    ui64 oldTimePlanStep = snap.PlanStep - TDuration::Seconds(limits.InGranuleCompactSeconds).MilliSeconds();
    ui32 insertedCount = 0;
    ui32 insertedNew = 0;

    THashSet<ui64> filtered;
    THashSet<ui64> goodCompacted;
    THashSet<ui64> nextToGood;
    {
        TMap<NArrow::TReplaceKey, TVector<const TPortionInfo*>> points;

        for (const auto& portionInfo : portions) {
            if (portionInfo.IsInserted()) {
                ++insertedCount;
                if (portionInfo.Snapshot().PlanStep > oldTimePlanStep) {
                    ++insertedNew;
                }
            } else if (portionInfo.BlobsSizes().second >= limits.GoodBlobSize) {
                goodCompacted.insert(portionInfo.Portion());
            }

            NArrow::TReplaceKey start = portionInfo.EffKeyStart();
            NArrow::TReplaceKey end = portionInfo.EffKeyEnd();

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
    // Trigger compaction if we have lots of inserted or if all inserted are old enough
    if (insertedNew && insertedCount < limits.InGranuleCompactInserts) {
        return false;
    }

    // Nothing to filter. Leave portions as is, no borders needed.
    if (filtered.empty() && goodCompacted.empty()) {
        return true;
    }

    // It's a map for SliceIntoGranules(). We use fake granule ids here to slice batch with borders.
    // We could merge inserted portions alltogether and slice result with filtered borders to prevent intersections.
    std::vector<TMark> borders;
    borders.push_back(granuleMark);

    TVector<TPortionInfo> tmp;
    tmp.reserve(portions.size());
    for (const auto& portionInfo : portions) {
        ui64 curPortion = portionInfo.Portion();

        // Prevent merge of compacted portions with no intersections
        if (filtered.contains(curPortion)) {
            auto start = portionInfo.EffKeyStart();
            borders.emplace_back(TMark(start));
        } else {
            // nextToGood borders potentially split good compacted portions into 2 parts:
            // the first one without intersections and the second with them
            if (goodCompacted.contains(curPortion) || nextToGood.contains(curPortion)) {
                auto start = portionInfo.EffKeyStart();
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
    return true;
}

TVector<const TPortionInfo*> GetActualPortions(const THashMap<ui64, TPortionInfo>& portions) {
    TVector<const TPortionInfo*> out;
    out.reserve(portions.size());
    for (auto& [portion, portionInfo] : portions) {
        if (portionInfo.IsActive()) {
            out.emplace_back(&portionInfo);
        }
    }
    return out;
}

THashMap<ui64, std::shared_ptr<arrow::RecordBatch>>
SliceIntoGranules(const std::shared_ptr<arrow::RecordBatch>& batch,
                  const std::vector<std::pair<TMark, ui64>>& granules,
                  const TIndexInfo& indexInfo)
{
    Y_VERIFY(batch);
    if (batch->num_rows() == 0) {
        return {};
    }

    THashMap<ui64, std::shared_ptr<arrow::RecordBatch>> out;

    if (granules.size() == 1) {
        out.emplace(granules[0].second, batch);
    } else {
        const auto effKey = GetEffectiveKey(batch, indexInfo);
        Y_VERIFY(effKey->num_columns() && effKey->num_rows());

        std::vector<NArrow::TRawReplaceKey> keys;
        {
            const auto& columns = effKey->columns();
            keys.reserve(effKey->num_rows());
            for (i64 i = 0; i < effKey->num_rows(); ++i) {
                keys.emplace_back(NArrow::TRawReplaceKey(&columns, i));
            }
        }

        i64 offset = 0;
        for (size_t i = 0; i < granules.size() && offset < effKey->num_rows(); ++i) {
            const i64 end = (i + 1 == granules.size())
                // Just take the number of elements in the key column for the last granule.
                ? effKey->num_rows()
                // Locate position of the next granule in the key.
                : NArrow::LowerBound(keys, granules[i + 1].first.Border, offset);

            if (const i64 size = end - offset) {
                Y_VERIFY(out.emplace(granules[i].second, batch->Slice(offset, size)).second);
            }

            offset = end;
        }
    }
    return out;
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
    TMark minMark(indexInfo.GetEffectiveKey());
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
    return NOlap::SliceIntoGranules(batch, Marks, indexInfo);
}


TColumnEngineForLogs::TColumnEngineForLogs(TIndexInfo&& info, ui64 tabletId, const TCompactionLimits& limits)
    : IndexInfo(std::move(info))
    , Limits(limits)
    , TabletId(tabletId)
    , LastPortion(0)
    , LastGranule(0)
{
    /// @note Setting replace and sorting key to PK we are able to:
    /// * apply REPLACE by MergeSort
    /// * apply PK predicate before REPLACE
    IndexInfo.SetAllKeys();
    MarkSchema = IndexInfo.GetEffectiveKey();

    ui32 indexId = IndexInfo.GetId();
    GranulesTable = std::make_shared<TGranulesTable>(*this, indexId);
    ColumnsTable = std::make_shared<TColumnsTable>(indexId);
    CountersTable = std::make_shared<TCountersTable>(indexId);
}

ui64 TColumnEngineForLogs::MemoryUsage() const {
    ui64 numPortions = Counters.Inserted.Portions +
        Counters.Compacted.Portions +
        Counters.SplitCompacted.Portions +
        Counters.Inactive.Portions +
        Counters.Evicted.Portions;

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
    Counters.OverloadedGranules = 0;
    for (auto& [pathId, set] : PathsGranulesOverloaded) {
        Counters.OverloadedGranules += set.size();
    }

    return Counters;
}

void TColumnEngineForLogs::UpdatePortionStats(const TPortionInfo& portionInfo, EStatsUpdateType updateType) {
    UpdatePortionStats(Counters, portionInfo, updateType);

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
    UpdatePortionStats(*PathStats[pathId], portionInfo, updateType);
}

void TColumnEngineForLogs::UpdatePortionStats(TColumnEngineStats& engineStats, const TPortionInfo& portionInfo,
                                              EStatsUpdateType updateType) const {
    ui64 columnRecords = portionInfo.Records.size();
    ui64 metadataBytes = 0;
    THashSet<TUnifiedBlobId> blobs;
    for (auto& rec : portionInfo.Records) {
        metadataBytes += rec.Metadata.size();
        blobs.insert(rec.BlobRange.BlobId);
    }

    ui32 rows = portionInfo.NumRows();
    ui64 rawBytes = portionInfo.RawBytesSum();
    ui64 bytes = 0;
    for (auto& blobId : blobs) {
        bytes += blobId.BlobSize();
    }

    TColumnEngineStats::TPortionsStats* srcStats = nullptr;
    switch (portionInfo.Meta.Produced) {
        case NOlap::TPortionMeta::UNSPECIFIED:
            Y_VERIFY(false); // unexpected
        case NOlap::TPortionMeta::INSERTED:
            srcStats = &engineStats.Inserted;
            break;
        case NOlap::TPortionMeta::COMPACTED:
            srcStats = &engineStats.Compacted;
            break;
        case NOlap::TPortionMeta::SPLIT_COMPACTED:
            srcStats = &engineStats.SplitCompacted;
            break;
        case NOlap::TPortionMeta::INACTIVE:
            srcStats = &engineStats.Inactive;
            break;
        case NOlap::TPortionMeta::EVICTED:
            srcStats = &engineStats.Evicted;
            break;
    }
    Y_VERIFY(srcStats);
    auto* stats = (updateType == EStatsUpdateType::EVICT)
        ? &engineStats.Evicted
        : (portionInfo.IsActive() ? srcStats : &engineStats.Inactive);

    bool isErase = updateType == EStatsUpdateType::ERASE;
    bool isLoad = updateType == EStatsUpdateType::LOAD;
    bool isAppended = portionInfo.IsActive() && (updateType != EStatsUpdateType::EVICT);

    if (isErase) { // PortionsToDrop
        engineStats.ColumnRecords -= columnRecords;
        engineStats.ColumnMetadataBytes -= metadataBytes;

        --stats->Portions;
        stats->Blobs -= blobs.size();
        stats->Rows -= rows;
        stats->Bytes -= bytes;
        stats->RawBytes -= rawBytes;
    } else if (isLoad || isAppended) { // AppendedPortions
        engineStats.ColumnRecords += columnRecords;
        engineStats.ColumnMetadataBytes += metadataBytes;

        ++stats->Portions;
        stats->Blobs += blobs.size();
        stats->Rows += rows;
        stats->Bytes += bytes;
        stats->RawBytes += rawBytes;
    } else { // SwitchedPortions || PortionsToEvict
        --srcStats->Portions;
        srcStats->Blobs -= blobs.size();
        srcStats->Rows -= rows;
        srcStats->Bytes -= bytes;
        srcStats->RawBytes -= rawBytes;

        ++stats->Portions;
        stats->Blobs += blobs.size();
        stats->Rows += rows;
        stats->Bytes += bytes;
        stats->RawBytes += rawBytes;
    }
}

void TColumnEngineForLogs::UpdateDefaultSchema(const TSnapshot& snapshot, TIndexInfo&& info) {
    // TODO(chertus): use step/txId for keeping older schema versions for older snapshots
    Y_UNUSED(snapshot);
    IndexInfo = std::move(info);
    // copied from constructor above
    IndexInfo.SetAllKeys();
}

bool TColumnEngineForLogs::Load(IDbWrapper& db, THashSet<TUnifiedBlobId>& lostBlobs, const THashSet<ui64>& pathsToDrop) {
    ClearIndex();

    if (!LoadGranules(db)) {
        return false;
    }
    if (!LoadColumns(db, lostBlobs)) {
        return false;
    }
    if (!LoadCounters(db)) {
        return false;
    }

#if 0 // Clear index data
    for (auto& [granule, meta] : Granules) {
        for (auto& [portion, portionInfo] : meta->Portions) {
            for (auto& rec : portionInfo.Records) {
                ColumnsTable->Erase(db, rec);
            }
        }
        GranulesTable->Erase(db, meta->Record);
    }
    CountersTable->Write(db, LAST_PORTION, 0);
    CountersTable->Write(db, LAST_GRANULE, 0);
    CountersTable->Write(db, LAST_PLAN_STEP, 0);
    CountersTable->Write(db, LAST_TX_ID, 0);
    return true;
#endif

    THashSet<ui64> emptyGranulePaths;
    for (auto& [granule, spg] : Granules) {
        if (spg->Empty()) {
            EmptyGranules.insert(granule);
            emptyGranulePaths.insert(spg->PathId());
        } else {
            CompactionGranules.insert(granule);
            CleanupGranules.insert(granule);
        }
        for (auto& [_, portionInfo] : spg->Portions) {
            UpdatePortionStats(portionInfo, EStatsUpdateType::LOAD);
        }
    }

    // Cleanup empty granules
    for (auto& pathId : emptyGranulePaths) {
        for (auto& emptyGranules : EmptyGranuleTracks(pathId)) {
            // keep first one => megre, keep nothing => drop.
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

    UpdateOverloaded(Granules);

    Y_VERIFY(!(LastPortion >> 63), "near to int overflow");
    Y_VERIFY(!(LastGranule >> 63), "near to int overflow");
    return true;
}

bool TColumnEngineForLogs::LoadGranules(IDbWrapper& db) {
    auto callback = [&](TGranuleRecord&& rec) {
        bool ok = SetGranule(rec, true);
        Y_VERIFY(ok);
    };

    return GranulesTable->Load(db, callback);
}

bool TColumnEngineForLogs::LoadColumns(IDbWrapper& db, THashSet<TUnifiedBlobId>& lostBlobs) {
    auto callback = [&](TColumnRecord&& row) {
        lostBlobs.erase(row.BlobRange.BlobId); // We have such a blob in index. It isn't lost.
        AddColumnRecord(row);
    };

    return ColumnsTable->Load(db, callback);
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
            LastSnapshot.PlanStep = value;
            break;
        case LAST_TX_ID:
            LastSnapshot.TxId = value;
            break;
        }
    };

    return CountersTable->Load(db, callback);
}

std::shared_ptr<TColumnEngineChanges> TColumnEngineForLogs::StartInsert(TVector<TInsertedData>&& dataToIndex) {
    Y_VERIFY(dataToIndex.size());

    auto changes = std::make_shared<TChanges>(*this, std::move(dataToIndex), Limits);
    ui32 reserveGranules = 0;

    changes->InitSnapshot = LastSnapshot;

    for (auto& data : changes->DataToIndex) {
        ui64 pathId = data.PathId;
        if (changes->PathToGranule.contains(pathId)) {
            continue;
        }

        if (PathGranules.contains(pathId)) {
            if (PathsGranulesOverloaded.contains(pathId)) {
                return {};
            }

            // TODO: cache PathToGranule for hot pathIds
            const auto& src = PathGranules[pathId];
            auto& dst = changes->PathToGranule[pathId];
            dst.reserve(src.size());
            for (const auto& [ts, granule] : src) {
                dst.emplace_back(std::make_pair(ts, granule));
            }
        } else {
            // It could reserve more then needed in case of the same pathId in DataToIndex
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
                                                                            const TSnapshot& outdatedSnapshot) {
    Y_VERIFY(info);

    auto changes = std::make_shared<TChanges>(*this, std::move(info), Limits);
    changes->InitSnapshot = LastSnapshot;

    Y_VERIFY(changes->CompactionInfo->Granules.size() == 1);

    ui64 granule = *changes->CompactionInfo->Granules.begin();
    auto& portions = changes->SwitchedPortions;
    {
        auto spg = Granules.find(granule)->second;
        Y_VERIFY(spg);

        auto actualPortions = GetActualPortions(spg->Portions);
        Y_VERIFY(!actualPortions.empty());
        portions.reserve(actualPortions.size());

        for (auto* portionInfo : actualPortions) {
            Y_VERIFY(!portionInfo->Empty());
            Y_VERIFY(portionInfo->Granule() == granule);
            portions.push_back(*portionInfo);
        }
    }

    Y_VERIFY(Granules.contains(granule));
    auto& spg = Granules[granule];
    Y_VERIFY(spg);
    ui64 pathId = spg->Record.PathId;
    Y_VERIFY(PathGranules.contains(pathId));

    for (const auto& [mark, pathGranule] : PathGranules[pathId]) {
        if (pathGranule == granule) {
            changes->SrcGranule = TChanges::TSrcGranule(pathId, granule, mark);
            break;
        }
    }
    Y_VERIFY(changes->SrcGranule);

    if (changes->CompactionInfo->InGranule) {
        TSnapshot completedSnap = (outdatedSnapshot < LastSnapshot) ? LastSnapshot : outdatedSnapshot;
        if (!InitInGranuleMerge(changes->SrcGranule->Mark, portions, Limits, completedSnap, changes->MergeBorders)) {
            return {};
        }
    } else {
        GranulesInSplit.insert(granule);
    }

    Y_VERIFY(!portions.empty());
    return changes;
}

std::shared_ptr<TColumnEngineChanges> TColumnEngineForLogs::StartCleanup(const TSnapshot& snapshot,
                                                                         THashSet<ui64>& pathsToDrop,
                                                                         ui32 maxRecords) {
    auto changes = std::make_shared<TChanges>(*this, snapshot, Limits);
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
            for (auto& [portion, info] : spg->Portions) {
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
    for (ui64 granule : CleanupGranules) {
        auto spg = Granules.find(granule)->second;
        Y_VERIFY(spg);

        bool isClean = true;
        for (auto& [portion, info] : spg->Portions) {
            if (info.IsActive() || dropPortions.contains(portion)) {
                continue;
            }

            isClean = false;
            if (info.XSnapshot() < snapshot) {
                affectedRecords += info.NumRecords();
                changes->PortionsToDrop.push_back(info);
            }

            if (affectedRecords > maxRecords) {
                break;
            }
        }

        if (isClean) {
            cleanGranules.insert(granule);
        }

        if (affectedRecords > maxRecords) {
            changes->NeedRepeat = true;
            break;
        }
    }

    for (ui64 granule : cleanGranules) {
        CleanupGranules.erase(granule);
    }

    return changes;
}

std::shared_ptr<TColumnEngineChanges> TColumnEngineForLogs::StartTtl(const THashMap<ui64, TTiering>& pathEviction, const std::shared_ptr<arrow::Schema>& schema,
                                                                     ui64 maxEvictBytes) {
    if (pathEviction.empty()) {
        return {};
    }

    TSnapshot fakeSnapshot = {1, 1}; // TODO: better snapshot
    auto changes = std::make_shared<TChanges>(*this, TColumnEngineChanges::TTL, fakeSnapshot);
    ui64 evicttionSize = 0;
    bool allowEviction = true;
    ui64 dropBlobs = 0;
    bool allowDrop = true;

    for (auto& [pathId, ttl] : pathEviction) {
        if (!PathGranules.contains(pathId)) {
            continue; // It's not an error: allow TTL over multiple shards with different pathIds presented
        }

        auto expireTimestamp = ttl.EvictScalar(schema);
        Y_VERIFY(expireTimestamp);

        auto ttlColumnNames = ttl.GetTtlColumns();
        Y_VERIFY(ttlColumnNames.size() == 1); // TODO: support different ttl columns
        ui32 ttlColumnId = IndexInfo.GetColumnId(*ttlColumnNames.begin());

        for (const auto& [ts, granule] : PathGranules[pathId]) {
            auto spg = Granules[granule];
            Y_VERIFY(spg);

            for (auto& [portion, info] : spg->Portions) {
                if (!info.IsActive()) {
                    continue;
                }

                allowEviction = (evicttionSize <= maxEvictBytes);
                allowDrop = (dropBlobs <= TCompactionLimits::MAX_BLOBS_TO_DELETE);
                bool tryEvictPortion = allowEviction && ttl.HasTiers()
                    && info.EvictReady(TCompactionLimits::EVICT_HOT_PORTION_BYTES);

                if (auto max = info.MaxValue(ttlColumnId)) {
                    bool keep = NArrow::ScalarLess(expireTimestamp, max);

                    if (keep && tryEvictPortion) {
                        TString tierName;
                        for (auto& tierRef : ttl.GetOrderedTiers()) { // TODO: lower/upper_bound + move into TEviction
                            auto& tierInfo = tierRef.Get();
                            if (!IndexInfo.AllowTtlOverColumn(tierInfo.GetEvictColumnName())) {
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
                        }
                    }
                    if (!keep && allowDrop) {
                        dropBlobs += info.NumRecords();
                        changes->PortionsToDrop.push_back(info);
                    }
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

TVector<TVector<std::pair<TMark, ui64>>> TColumnEngineForLogs::EmptyGranuleTracks(ui64 pathId) const {
    Y_VERIFY(PathGranules.contains(pathId));
    const auto& pathGranules = PathGranules.find(pathId)->second;

    TVector<TVector<std::pair<TMark, ui64>>> emptyGranules;
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

void TColumnEngineForLogs::UpdateOverloaded(const THashMap<ui64, std::shared_ptr<TGranuleMeta>>& granules) {
    for (auto [granule, spg] : granules) {
        if (!spg) {
            spg = Granules[granule];
        }
        Y_VERIFY(spg);
        ui64 pathId = spg->Record.PathId;

        ui64 size = 0;
        for (auto& [portion, portionInfo] : spg->Portions) {
            if (portionInfo.IsActive()) {
                size += portionInfo.BlobsSizes().first;
            }
        }

        if (size >= Limits.GranuleOverloadSize) {
            PathsGranulesOverloaded.emplace(pathId, granule);
        } else if (PathsGranulesOverloaded.contains(pathId)) {
            auto& granules = PathsGranulesOverloaded[pathId];
            granules.erase(granule);
            if (granules.empty()) {
                PathsGranulesOverloaded.erase(pathId);
            }
        }
    }
}

bool TColumnEngineForLogs::ApplyChanges(IDbWrapper& db, std::shared_ptr<TColumnEngineChanges> indexChanges,
                                        const TSnapshot& snapshot) {
    auto changes = std::static_pointer_cast<TChanges>(indexChanges);
    if (changes->ApplySnapshot.Valid()) {
        Y_VERIFY(changes->ApplySnapshot == snapshot);
    }

    // Update tmp granules with real ids
    auto granuleRemap = changes->TmpToNewGranules(LastGranule);
    ui64 portion = LastPortion;
    for (auto& portionInfo : changes->AppendedPortions) {
        portionInfo.UpdateRecords(++portion, granuleRemap, snapshot);

        TPortionMeta::EProduced produced = TPortionMeta::INSERTED;
        // If it's a split compaction with moves appended portions are INSERTED (could have overlaps with others)
        if (changes->IsCompaction() && changes->PortionsToMove.empty()) {
            Y_VERIFY(changes->CompactionInfo);
            produced = changes->CompactionInfo->InGranule ?
                TPortionMeta::COMPACTED : TPortionMeta::SPLIT_COMPACTED;
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
        return false;
    }
    bool ok = ApplyChanges(db, *changes, snapshot, true);
    Y_VERIFY(ok);

    // Save updated granules for comapaction
    if (changes->IsInsert()) {
        for (auto& portionInfo : changes->AppendedPortions) {
            CompactionGranules.insert(portionInfo.Granule());
        }
    }

    // Save updated granules for cleanup
    if (changes->IsCompaction()) {
        Y_VERIFY(changes->CompactionInfo);
        for (auto& portionInfo : changes->SwitchedPortions) {
            CleanupGranules.insert(portionInfo.Granule());
        }
    }

    // Update overloaded granules (only if tx would be applyed)
    if (changes->IsInsert() || changes->IsCompaction() || changes->IsCleanup()) {
        THashMap<ui64, std::shared_ptr<TGranuleMeta>> granules;
        if (changes->IsCleanup()) {
            for (auto& portionInfo : changes->PortionsToDrop) {
                granules[portionInfo.Granule()] = {};
            }
        } else if (changes->IsCompaction() && !changes->CompactionInfo->InGranule) {
            granules[changes->SrcGranule->Granule] = {};
        } else {
            for (auto& portionInfo : changes->AppendedPortions) {
                granules[portionInfo.Granule()] = {};
            }
        }
        UpdateOverloaded(granules);
    }
    return true;
}

bool TColumnEngineForLogs::ApplyChanges(IDbWrapper& db, const TChanges& changes, const TSnapshot& snapshot, bool apply) {
    const TVector<TPortionInfo>* switchedPortions = nullptr;
    if (changes.IsCompaction()) {
        Y_VERIFY(changes.CompactionInfo);
        switchedPortions = &changes.SwitchedPortions;

        if (changes.CompactionInfo->InGranule) {
#if 0
            if (changes.SwitchedPortions.size() <= changes.AppendedPortions.size()) {
                LOG_S_ERROR("Cannot compact granule " << changes.SrcGranule->Granule << " at tablet " << TabletId);
                return false;
            }
#endif
        } else {
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
        TGranuleRecord rec(pathId, granule, snapshot, mark.Border);

        if (!SetGranule(rec, apply)) {
            LOG_S_ERROR("Cannot insert granule " << rec << " at tablet " << TabletId);
            return false;
        }
        if (apply) {
            GranulesTable->Write(db, rec);
        }
    }

    // Update old portions (set stale snapshot)

    if (switchedPortions) {
        for (auto& portionInfo : *switchedPortions) {
            Y_VERIFY(!portionInfo.Empty());
            Y_VERIFY(!portionInfo.IsActive());

            ui64 granule = portionInfo.Granule();
            if (!Granules.contains(granule)) {
                LOG_S_ERROR("Cannot update portion " << portionInfo << " with unknown granule at tablet " << TabletId);
                return false;
            }

            auto& granuleStart = Granules[granule]->Record.Mark;

            if (!apply) { // granule vs portion minPK
                auto portionStart = portionInfo.EffKeyStart();
                if (portionStart < granuleStart) {
                    LOG_S_ERROR("Cannot update invalid portion " << portionInfo
                        << " start: " << TMark(portionStart).ToString()
                        << " granule start: " << TMark(granuleStart).ToString() << " at tablet " << TabletId);
                    return false;
                }
            }

            if (!UpsertPortion(portionInfo, apply)) {
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
        if (!Granules.contains(granule) || !Granules[granule]->Portions.contains(portion)) {
            LOG_S_ERROR("Cannot evict unknown portion " << portionInfo << " at tablet " << TabletId);
            return false;
        }

        // In case of race with compaction portion could become inactive
        // TODO: evict others instead of abort eviction
        auto& oldInfo = Granules[granule]->Portions[portion];
        if (!oldInfo.IsActive()) {
            LOG_S_WARN("Cannot evict inactive portion " << oldInfo << " at tablet " << TabletId);
            return false;
        }
        Y_VERIFY(portionInfo.TierName != oldInfo.TierName);

        if (apply) {
            UpdatePortionStats(oldInfo, EStatsUpdateType::EVICT);
        }

        if (!UpsertPortion(portionInfo, apply, false)) {
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

    for (auto& [info, granule] : changes.PortionsToMove) {
        const auto& portionInfo = info;
        if (!ErasePortion(portionInfo, apply, false)) {
            LOG_S_ERROR("Cannot erase moved portion " << portionInfo << " at tablet " << TabletId);
            return false;
        }
        TPortionInfo moved = portionInfo;
        moved.SetGranule(granule);
        if (!UpsertPortion(moved, apply, false)) {
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
                : changes.NewGranules.find(granule)->second.second.Border;

            auto portionStart = portionInfo.EffKeyStart();
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
            CountersTable->Write(db, LAST_PLAN_STEP, LastSnapshot.PlanStep);
            CountersTable->Write(db, LAST_TX_ID, LastSnapshot.TxId);
        }
    }
    return true;
}

void TColumnEngineForLogs::FreeLocks(std::shared_ptr<TColumnEngineChanges> indexChanges) {
    auto changes = std::static_pointer_cast<TChanges>(indexChanges);

    if (changes->IsCompaction()) {
        // Set granule not in split. Do not block writes in it.
        Y_VERIFY(changes->SrcGranule);
        GranulesInSplit.erase(changes->SrcGranule->Granule);
    }
}

bool TColumnEngineForLogs::SetGranule(const TGranuleRecord& rec, bool apply) {
    TMark mark(rec.Mark);

    if (!apply) {
        if (Granules.contains(rec.Granule)) {
            return false;
        }

        if (PathGranules.contains(rec.PathId) && PathGranules[rec.PathId].contains(mark)) {
            return false;
        }
        return true;
    }

    PathGranules[rec.PathId].emplace(mark, rec.Granule);
    auto& spg = Granules[rec.Granule];
    Y_VERIFY(!spg);
    spg = std::make_shared<TGranuleMeta>(rec);
    return true; // It must return true if (apply == true)
}

void TColumnEngineForLogs::EraseGranule(ui64 pathId, ui64 granule, const TMark& mark) {
    Y_VERIFY(PathGranules.contains(pathId));
    Y_VERIFY(Granules.contains(granule));

    Granules.erase(granule);
    EmptyGranules.erase(granule);
    CompactionGranules.erase(granule);
    PathGranules[pathId].erase(mark);
}

bool TColumnEngineForLogs::UpsertPortion(const TPortionInfo& portionInfo, bool apply, bool updateStats) {
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
    ui64 portion = portionInfo.Portion();
    auto& spg = Granules[granule];
    Y_VERIFY(spg);
    if (updateStats) {
        UpdatePortionStats(portionInfo);
    }
    spg->Portions[portion] = portionInfo;
    return true; // It must return true if (apply == true)
}

bool TColumnEngineForLogs::ErasePortion(const TPortionInfo& portionInfo, bool apply, bool updateStats) {
    Y_VERIFY(!portionInfo.Empty());
    ui64 granule = portionInfo.Granule();
    ui64 portion = portionInfo.Portion();

    if (!apply) {
        if (!Granules.contains(granule)) {
            return false;
        }
        return true;
    }

    auto& spg = Granules[granule];
    Y_VERIFY(spg);
    if (spg->Portions.contains(portion)) {
        if (updateStats) {
            UpdatePortionStats(spg->Portions[portion], EStatsUpdateType::ERASE);
        }
        spg->Portions.erase(portion);
    } else {
        LOG_S_ERROR("Erase for unknown portion " << portionInfo << " at tablet " << TabletId);
    }
    return true; // It must return true if (apply == true)
}

void TColumnEngineForLogs::AddColumnRecord(const TColumnRecord& rec) {
    Y_VERIFY(rec.Valid());
    auto& spg = Granules[rec.Granule];
#if 0
    if (!spg) {
        LOG_S_ERROR("No granule " << rec.Granule << " for record " << rec << " at tablet " << TabletId);
        Granules.erase(rec.Granule);
        return;
    }
#else
    Y_VERIFY(spg);
#endif
    auto& portionInfo = spg->Portions[rec.Portion];
    portionInfo.AddRecord(IndexInfo, rec);
}

bool TColumnEngineForLogs::CanInsert(const TChanges& changes, const TSnapshot& commitSnap) const {
    Y_UNUSED(commitSnap);
    // Does insert have granule in split?
    for (auto& portionInfo : changes.AppendedPortions) {
        Y_VERIFY(!portionInfo.Empty());
        ui64 granule = portionInfo.Granule();
        if (GranulesInSplit.contains(granule)) {
            LOG_S_NOTICE("Cannot insert into splitting granule " << granule << " at tablet " << TabletId);
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

TMap<TSnapshot, TVector<ui64>> TColumnEngineForLogs::GetOrderedPortions(ui64 granule, const TSnapshot& snapshot) const {
    Y_VERIFY(Granules.contains(granule));
    auto& spg = Granules.find(granule)->second;
    Y_VERIFY(spg);

    TMap<TSnapshot, TVector<ui64>> out;
    for (auto& [portion, portionInfo] : spg->Portions) {
        if (portionInfo.Empty()) {
            continue;
        }

        TSnapshot recSnapshot = portionInfo.Snapshot();
        TSnapshot recXSnapshot = portionInfo.XSnapshot();

        bool visible = (recSnapshot <= snapshot);
        if (recXSnapshot.PlanStep) {
            visible = visible && snapshot < recXSnapshot;
        }

        if (visible) {
            out[recSnapshot].push_back(portion);
        }
    }
    return out;
}

std::shared_ptr<TSelectInfo> TColumnEngineForLogs::Select(ui64 pathId, TSnapshot snapshot,
                                                          const THashSet<ui32>& columnIds,
                                                          std::shared_ptr<TPredicate> from,
                                                          std::shared_ptr<TPredicate> to) const {
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

    std::optional<NArrow::TReplaceKey> keyFrom = ExtractKey(from, GetIndexKey());
    std::optional<NArrow::TReplaceKey> keyTo = ExtractKey(to, GetIndexKey());

    // Apply FROM
    auto it = pathGranules.begin();
    if (keyFrom) {
        it = pathGranules.upper_bound(TMark(*keyFrom));
        --it;
    }
    for (; it != pathGranules.end(); ++it) {
        auto& mark = it->first;
        ui64 granule = it->second;

        // Apply TO
        if (keyTo && *keyTo < mark.Border) {
            break;
        }

        Y_VERIFY(Granules.contains(granule));
        auto& spg = Granules.find(granule)->second;
        Y_VERIFY(spg);
        auto& portions = spg->Portions;
        bool granuleHasDataForSnaphsot = false;

        TMap<TSnapshot, TVector<ui64>> orderedPortions = GetOrderedPortions(granule, snapshot);
        for (auto& [snap, vec] : orderedPortions) {
            for (auto& portion : vec) {
                auto& portionInfo = portions.find(portion)->second;

                TPortionInfo outPortion;
                outPortion.Meta = portionInfo.Meta;
                outPortion.Records.reserve(columnIds.size());

                for (auto& rec : portionInfo.Records) {
                    Y_VERIFY(rec.Valid());
                    if (columnIds.contains(rec.ColumnId)) {
                        outPortion.Records.push_back(rec);
                    }
                }
                Y_VERIFY(outPortion.Produced());
                out->Portions.emplace_back(std::move(outPortion));
                granuleHasDataForSnaphsot = true;
            }
        }

        if (granuleHasDataForSnaphsot) {
            out->Granules.push_back(spg->Record);
        }
    }

    return out;
}

static bool NeedSplit(const TVector<const TPortionInfo*>& actual, const TCompactionLimits& limits, ui32& inserted) {
    if (actual.size() < 2) {
        return false;
    }

    inserted = 0;
    ui64 sumSize = 0;
    ui64 sumMaxSize = 0;
    std::shared_ptr<arrow::Scalar> minPk0;
    std::shared_ptr<arrow::Scalar> maxPk0;
    if (actual.size()) {
        actual[0]->MinMaxValue(actual[0]->FirstPkColumn, minPk0, maxPk0);
    }
    bool pkEqual = !!minPk0 && !!maxPk0 && arrow::ScalarEquals(*minPk0, *maxPk0);
    for (auto* portionInfo : actual) {
        Y_VERIFY(portionInfo);
        if (pkEqual) {
            std::shared_ptr<arrow::Scalar> minPkCurrent;
            std::shared_ptr<arrow::Scalar> maxPkCurrent;
            portionInfo->MinMaxValue(portionInfo->FirstPkColumn, minPkCurrent, maxPkCurrent);
            pkEqual = !!minPkCurrent && !!maxPkCurrent && arrow::ScalarEquals(*minPk0, *minPkCurrent)
                && arrow::ScalarEquals(*maxPk0, *maxPkCurrent);
        }
        auto sizes = portionInfo->BlobsSizes();
        sumSize += sizes.first;
        sumMaxSize += sizes.second;
        if (portionInfo->IsInserted()) {
            ++inserted;
        }
    }

    return !pkEqual && (sumMaxSize >= limits.GranuleBlobSplitSize
        || sumSize >= limits.GranuleOverloadSize);
}

std::unique_ptr<TCompactionInfo> TColumnEngineForLogs::Compact(ui64& lastCompactedGranule) {
    if (CompactionGranules.empty()) {
        return {};
    }

    std::optional<ui64> outGranule;
    bool inGranule = true;

    auto it = CompactionGranules.upper_bound(lastCompactedGranule);
    if (it == CompactionGranules.end()) {
        it = CompactionGranules.begin();
    }

    while (!CompactionGranules.empty()) {
        Y_VERIFY(it != CompactionGranules.end());
        ui64 granule = *it;
        Y_VERIFY(Granules.contains(granule));
        auto spg = Granules.find(granule)->second;
        Y_VERIFY(spg);

        // We need only actual portions here (with empty XPlanStep:XTxId)
        auto actualPortions = GetActualPortions(spg->Portions);
        if (!actualPortions.empty()) {
            ui32 inserted = 0;
            bool needSplit = NeedSplit(actualPortions, Limits, inserted);
            if (needSplit) {
                inGranule = false;
                outGranule = granule;
                break;
            } else if (inserted) {
                outGranule = granule;
                break;
            }
        }

        it = CompactionGranules.erase(it);
        if (it == CompactionGranules.end()) {
            it = CompactionGranules.begin();
        }
    }

    if (outGranule) {
        auto info = std::make_unique<TCompactionInfo>();
        info->Granules.insert(*outGranule);
        info->InGranule = inGranule;
        lastCompactedGranule = *outGranule;
        return info;
    }
    return {};
}

TVector<TString> TColumnEngineForLogs::IndexBlobs(const TIndexInfo& indexInfo, const THashMap<ui64, NKikimr::NOlap::TTiering>& tieringMap,
                                                  std::shared_ptr<TColumnEngineChanges> indexChanges) {
    auto changes = std::static_pointer_cast<TChanges>(indexChanges);
    Y_VERIFY(!changes->DataToIndex.empty());
    Y_VERIFY(changes->AppendedPortions.empty());
    Y_VERIFY(indexInfo.IsSorted());

    TSnapshot& minSnapshot = changes->ApplySnapshot;
    THashMap<ui64, std::vector<std::shared_ptr<arrow::RecordBatch>>> pathBatches;
    for (auto& inserted : changes->DataToIndex) {
        TSnapshot insertSnap{inserted.PlanStep(), inserted.TxId()};
        Y_VERIFY(insertSnap.Valid());
        if (minSnapshot.IsZero() || insertSnap <= minSnapshot) {
            minSnapshot = insertSnap;
        }

        TBlobRange blobRange(inserted.BlobId, 0, inserted.BlobId.BlobSize());

        std::shared_ptr<arrow::RecordBatch> batch;
        if (auto it = changes->CachedBlobs.find(inserted.BlobId); it != changes->CachedBlobs.end()) {
            batch = it->second;
        } else if (auto* blobData = changes->Blobs.FindPtr(blobRange)) {
            Y_VERIFY(!blobData->empty(), "Blob data not present");
            batch = NArrow::DeserializeBatch(*blobData, indexInfo.ArrowSchema());
        } else {
            Y_VERIFY(blobData, "Data for range %s has not been read", blobRange.ToString().c_str());
        }
        Y_VERIFY(batch);

        batch = AddSpecials(batch, indexInfo, inserted);
        pathBatches[inserted.PathId].push_back(batch);
        Y_VERIFY_DEBUG(NArrow::IsSorted(pathBatches[inserted.PathId].back(), indexInfo.GetReplaceKey()));
    }
    Y_VERIFY(minSnapshot.Valid());

    TVector<TString> blobs;

    for (auto& [pathId, batches] : pathBatches) {
        changes->AddPathIfNotExists(pathId);

        // We could merge data here cause tablet limits indexing data portions
#if 0
        auto merged = NArrow::CombineSortedBatches(batches, indexInfo.SortDescription()); // insert: no replace
        Y_VERIFY(merged);
        Y_VERIFY_DEBUG(NArrow::IsSorted(merged, indexInfo.GetReplaceKey()));
#else
        auto merged = NArrow::CombineSortedBatches(batches, indexInfo.SortReplaceDescription());
        Y_VERIFY(merged);
        Y_VERIFY_DEBUG(NArrow::IsSortedAndUnique(merged, indexInfo.GetReplaceKey()));

#endif

        auto granuleBatches = SliceIntoGranules(merged, changes->PathToGranule[pathId], indexInfo);
        for (auto& [granule, batch] : granuleBatches) {
            auto portions = MakeAppendedPortions(pathId, indexInfo, tieringMap, batch, granule, minSnapshot, blobs);
            Y_VERIFY(portions.size() > 0);
            for (auto& portion : portions) {
                changes->AppendedPortions.emplace_back(std::move(portion));
            }
        }
    }

    Y_VERIFY(changes->PathToGranule.size() == pathBatches.size());
    return blobs;
}

static std::shared_ptr<arrow::RecordBatch> CompactInOneGranule(const TIndexInfo& indexInfo, ui64 granule,
                                                               const TVector<TPortionInfo>& portions,
                                                               const THashMap<TBlobRange, TString>& blobs) {
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    batches.reserve(portions.size());

    auto schema = indexInfo.ArrowSchemaWithSpecials();
    for (auto& portionInfo : portions) {
        Y_VERIFY(!portionInfo.Empty());
        Y_VERIFY(portionInfo.Granule() == granule);

        auto batch = portionInfo.AssembleInBatch(indexInfo, schema, blobs);
        batches.push_back(batch);
    }

    auto sortedBatch = NArrow::CombineSortedBatches(batches, indexInfo.SortReplaceDescription());
    Y_VERIFY_DEBUG(NArrow::IsSortedAndUnique(sortedBatch, indexInfo.GetReplaceKey()));

    return sortedBatch;
}

static TVector<TString> CompactInGranule(const TIndexInfo& indexInfo, const THashMap<ui64, NKikimr::NOlap::TTiering>& tieringMap,
                                         std::shared_ptr<TColumnEngineForLogs::TChanges> changes) {
    const ui64 pathId = changes->SrcGranule->PathId;
    TVector<TString> blobs;
    auto& switchedProtions = changes->SwitchedPortions;
    Y_VERIFY(switchedProtions.size());

    ui64 granule = switchedProtions[0].Granule();
    auto batch = CompactInOneGranule(indexInfo, granule, switchedProtions, changes->Blobs);

    TVector<TPortionInfo> portions;
    if (!changes->MergeBorders.Empty()) {
        Y_VERIFY(changes->MergeBorders.GetOrderedMarks().size() > 1);
        auto slices = changes->MergeBorders.SliceIntoGranules(batch, indexInfo);
        portions.reserve(slices.size());

        for (auto& [_, slice] : slices) {
            if (!slice || slice->num_rows() == 0) {
                continue;
            }
            auto tmp = MakeAppendedPortions(pathId, indexInfo, tieringMap, slice, granule, TSnapshot{}, blobs);
            for (auto&& portionInfo : tmp) {
                portions.emplace_back(std::move(portionInfo));
            }
        }
    } else {
        portions = MakeAppendedPortions(pathId, indexInfo, tieringMap, batch, granule, TSnapshot{}, blobs);
    }

    Y_VERIFY(portions.size() > 0);
    for (auto& portion : portions) {
        changes->AppendedPortions.emplace_back(std::move(portion));
    }

    return blobs;
}

/// @return vec({ts, batch}). ts0 <= ts1 <= ... <= tsN
/// @note We use ts from PK for split but there could be lots PK with the same ts.
static TVector<std::pair<TMark, std::shared_ptr<arrow::RecordBatch>>>
SliceGranuleBatches(const TIndexInfo& indexInfo,
                    const TColumnEngineForLogs::TChanges& changes,
                    const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches,
                    const TMark& ts0) {
    TVector<std::pair<TMark, std::shared_ptr<arrow::RecordBatch>>> out;

    // Extract unique effective keys and their counts
    i64 numRows = 0;
    TMap<NArrow::TReplaceKey, ui32> uniqKeyCount;
    for (const auto& batch : batches) {
        Y_VERIFY(batch);
        if (batch->num_rows() == 0) {
            continue;
        }

        numRows += batch->num_rows();

        const auto effKey = GetEffectiveKey(batch, indexInfo);
        Y_VERIFY(effKey->num_columns() && effKey->num_rows());

        auto effColumns = std::make_shared<NArrow::TArrayVec>(effKey->columns());
        for (int row = 0; row < effKey->num_rows(); ++row) {
            ++uniqKeyCount[NArrow::TReplaceKey(effColumns, row)];
        }
    }

    Y_VERIFY(uniqKeyCount.size());
    auto minTs = uniqKeyCount.begin()->first;
    auto maxTs = uniqKeyCount.rbegin()->first;
    Y_VERIFY(minTs >= ts0.Border);

    // It's an estimation of needed count cause numRows calculated before key replaces
    ui32 numSplitInto = changes.NumSplitInto(numRows);
    ui32 rowsInGranule = numRows / numSplitInto;
    Y_VERIFY(rowsInGranule);

    // Cannot split in case of one unique key
    if (uniqKeyCount.size() == 1) {
        // We have to split big batch of same key in several portions
        auto merged = NArrow::MergeSortedBatches(batches, indexInfo.SortReplaceDescription(), rowsInGranule);
        for (auto& batch : merged) {
            Y_VERIFY_DEBUG(NArrow::IsSortedAndUnique(batch, indexInfo.GetReplaceKey()));
            out.emplace_back(ts0, batch);
        }
        return out;
    }

    // Make split borders from uniq keys
    TVector<NArrow::TReplaceKey> borders;
    borders.reserve(numRows / rowsInGranule);
    {
        ui32 sumRows = 0;
        for (auto& [ts, num] : uniqKeyCount) {
            if (sumRows >= rowsInGranule) {
                borders.emplace_back(ts);
                sumRows = 0;
            }
            sumRows += num;
        }
        if (borders.empty()) {
            borders.emplace_back(maxTs); // huge trailing key
        }
        Y_VERIFY(borders.size());
    }

    // Find offsets in source batches
    TVector<TVector<int>> offsets(batches.size()); // vec[batch][border] = offset
    for (size_t i = 0; i < batches.size(); ++i) {
        const auto& batch = batches[i];
        auto& batchOffsets = offsets[i];
        batchOffsets.reserve(borders.size() + 1);

        const auto effKey = GetEffectiveKey(batch, indexInfo);
        Y_VERIFY(effKey->num_columns() && effKey->num_rows());

        std::vector<NArrow::TRawReplaceKey> keys;
        {
            const auto& columns = effKey->columns();
            keys.reserve(effKey->num_rows());
            for (i64 i = 0; i < effKey->num_rows(); ++i) {
                keys.emplace_back(NArrow::TRawReplaceKey(&columns, i));
            }
        }

        batchOffsets.push_back(0);
        for (const auto& border : borders) {
            int offset = NArrow::LowerBound(keys, border, batchOffsets.back());
            Y_VERIFY(offset >= batchOffsets.back());
            batchOffsets.push_back(offset);
        }

        Y_VERIFY(batchOffsets.size() == borders.size() + 1);
    }

    // Make merge-sorted granule batch for each splitted granule
    for (ui32 granuleNo = 0; granuleNo < borders.size() + 1; ++granuleNo) {
        std::vector<std::shared_ptr<arrow::RecordBatch>> granuleBatches;
        granuleBatches.reserve(batches.size());

        // Extract granule: slice source batches with offsets
        i64 granuleNumRows = 0;
        for (size_t i = 0; i < batches.size(); ++i) {
            const auto& batch = batches[i];
            auto& batchOffsets = offsets[i];

            int offset = batchOffsets[granuleNo];
            int end = batch->num_rows();
            if (granuleNo < borders.size()) {
                end = batchOffsets[granuleNo + 1];
            }
            int size = end - offset;
            Y_VERIFY(size >= 0);

            if (size) {
                auto slice = batch->Slice(offset, size);
                Y_VERIFY(slice->num_rows());
                granuleNumRows += slice->num_rows();
#if 1 // Check correctness
                const auto effKey = GetEffectiveKey(slice, indexInfo);
                Y_VERIFY(effKey->num_columns() && effKey->num_rows());

                auto startKey = granuleNo ? borders[granuleNo - 1] : minTs;
                Y_VERIFY(NArrow::TReplaceKey::FromBatch(effKey, 0) >= startKey);

                NArrow::TReplaceKey lastSliceKey = NArrow::TReplaceKey::FromBatch(effKey, effKey->num_rows() - 1);
                if (granuleNo < borders.size() - 1) {
                    const auto& endKey = borders[granuleNo];
                    Y_VERIFY(lastSliceKey < endKey);
                } else {
                    Y_VERIFY(lastSliceKey <= maxTs);
                }
#endif
                Y_VERIFY_DEBUG(NArrow::IsSorted(slice, indexInfo.GetReplaceKey()));
                granuleBatches.emplace_back(slice);
            }
        }

        // Merge slices. We have to split a big key batches in several ones here.
        if (granuleNumRows > 4 * rowsInGranule) {
            granuleNumRows = rowsInGranule;
        }
        auto merged = NArrow::MergeSortedBatches(granuleBatches, indexInfo.SortReplaceDescription(), granuleNumRows);
        for (auto& batch : merged) {
            Y_VERIFY_DEBUG(NArrow::IsSortedAndUnique(batch, indexInfo.GetReplaceKey()));

            auto startKey = ts0.Border;
            if (granuleNo) {
                startKey = borders[granuleNo - 1];
            }
#if 1 // Check correctness
            const auto effKey = GetEffectiveKey(batch, indexInfo);
            Y_VERIFY(effKey->num_columns() && effKey->num_rows());

            Y_VERIFY(NArrow::TReplaceKey::FromBatch(effKey, 0) >= startKey);
#endif
            out.emplace_back(TMark(startKey), batch);
        }
    }

    return out;
}

/// @param[in,out] portions unchanged or only inserted portions in the same orders
/// @param[in,out] tsIds    unchanged or marks from compacted portions ordered by mark
/// @param[in,out] toMove   unchanged or compacted portions ordered by primary key
static ui64 TryMovePortions(const TMark& ts0,
                            TVector<TPortionInfo>& portions,
                            std::vector<std::pair<TMark, ui64>>& tsIds,
                            TVector<std::pair<TPortionInfo, ui64>>& toMove)
{
    std::vector<TPortionInfo*> partitioned(portions.size());
    // Split portions by putting the inserted portions in the original order
    // at the beginning of the buffer and the compacted portions at the end.
    // The compacted portions will be put in the reversed order, but it will be sorted later.
    const auto [inserted, compacted] = [&]() {
        size_t l = 0;
        size_t r = portions.size();

        for (auto& portionInfo : portions) {
            partitioned[(portionInfo.IsInserted() ? l++ : --r)] = &portionInfo;
        }

        return std::make_tuple(std::span(partitioned.begin(), l), std::span(partitioned.begin() + l, partitioned.end()));
    }();

    // Do nothing if there are less than two compacted protions.
    if (compacted.size() < 2) {
        return 0;
    }
    // Order compacted portions by primary key.
    std::sort(compacted.begin(), compacted.end(), [](const TPortionInfo* a, const TPortionInfo* b) {
        return a->EffKeyStart() < b->EffKeyStart();
    });
    // Check that there are no gaps between two adjacent portions in term of primary key range.
    for (size_t i = 0; i < compacted.size() - 1; ++i) {
        if (compacted[i]->EffKeyEnd() >= compacted[i + 1]->EffKeyStart()) {
            return 0;
        }
    }

    toMove.reserve(compacted.size());
    ui64 numRows = 0;
    ui32 counter = 0;
    for (auto* portionInfo : compacted) {
        ui32 rows = portionInfo->NumRows();
        Y_VERIFY(rows);
        numRows += rows;
        tsIds.emplace_back((counter ? TMark(portionInfo->EffKeyStart()) : ts0), counter + 1);
        toMove.emplace_back(std::move(*portionInfo), counter);
        ++counter;
        // Ensure that std::move will take an effect.
        static_assert(std::swappable<decltype(*portionInfo)>);
    }

    std::vector<TPortionInfo> out;
    out.reserve(inserted.size());
    for (auto* portionInfo : inserted) {
        out.emplace_back(std::move(*portionInfo));
        // Ensure that std::move will take an effect.
        static_assert(std::swappable<decltype(*portionInfo)>);
    }
    portions.swap(out);

    return numRows;
}

static TVector<TString> CompactSplitGranule(const TIndexInfo& indexInfo, const THashMap<ui64, NKikimr::NOlap::TTiering>& tieringMap,
                                            const std::shared_ptr<TColumnEngineForLogs::TChanges>& changes)
{
    const ui64 pathId = changes->SrcGranule->PathId;
    const TMark ts0 = changes->SrcGranule->Mark;
    TVector<TPortionInfo>& portions = changes->SwitchedPortions;

    std::vector<std::pair<TMark, ui64>> tsIds;
    ui64 movedRows = TryMovePortions(ts0, portions, tsIds, changes->PortionsToMove);
    const auto& srcBatches = PortionsToBatches(indexInfo, portions, changes->Blobs, movedRows != 0);
    Y_VERIFY(srcBatches.size() == portions.size());

    TVector<TString> blobs;

    if (movedRows) {
        Y_VERIFY(changes->PortionsToMove.size() >= 2);
        Y_VERIFY(changes->PortionsToMove.size() == tsIds.size());
        Y_VERIFY(tsIds.begin()->first == ts0);

        // Calculate total number of rows.
        ui64 numRows = movedRows;
        for (const auto& batch : srcBatches) {
            numRows += batch->num_rows();
        }

        // Recalculate new granules borders (if they are larger then portions)
        ui32 numSplitInto = changes->NumSplitInto(numRows);
        if (numSplitInto < tsIds.size()) {
            const ui32 rowsInGranule = numRows / numSplitInto;
            Y_VERIFY(rowsInGranule);

            TVector<std::pair<TMark, ui64>> newTsIds;
            ui32 tmpGranule = 0;
            ui32 sumRows = 0;
            // Always insert mark of the source granule at the beginning.
            newTsIds.emplace_back(ts0, 1);

            for (size_t i = 0, end = tsIds.size(); i != end; ++i) {
                const TMark& ts = tsIds[i].first;
                // Make new granule if the current number of rows is exceeded the allowed number of rows in the granule
                // or there is the end of the ids and nothing was inserted so far.
                if (sumRows >= rowsInGranule || (i + 1 == end && newTsIds.size() == 1)) {
                    ++tmpGranule;
                    newTsIds.emplace_back(ts, tmpGranule + 1);
                    sumRows = 0;
                }

                auto& toMove = changes->PortionsToMove[i];
                sumRows += toMove.first.NumRows();
                toMove.second = tmpGranule;
            }

            tsIds.swap(newTsIds);
        }
        Y_VERIFY(tsIds.size() > 1);
        Y_VERIFY(tsIds[0] == std::make_pair(ts0, ui64(1)));
        TColumnEngineForLogs::TMarksGranules marksGranules(std::move(tsIds));

        // Slice inserted portions with granules' borders
        THashMap<ui64, std::vector<std::shared_ptr<arrow::RecordBatch>>> idBatches;
        TVector<TPortionInfo*> toSwitch;
        toSwitch.reserve(portions.size());
        for (size_t i = 0; i < portions.size(); ++i) {
            auto& portion = portions[i];
            auto& batch = srcBatches[i];
            auto slices = marksGranules.SliceIntoGranules(batch, indexInfo);

            THashSet<ui64> ids;
            for (auto& [id, slice] : slices) {
                if (slice && slice->num_rows()) {
                    ids.insert(id);
                    idBatches[id].emplace_back(std::move(slice));
                }
            }

            // Optimization: move not splitted inserted portions. Do not reappend them.
            if (ids.size() == 1) {
                ui64 id = *ids.begin();
                idBatches[id].resize(idBatches[id].size() - 1);
                ui64 tmpGranule = id - 1;
                changes->PortionsToMove.emplace_back(std::move(portion), tmpGranule);
            } else {
                toSwitch.push_back(&portion);
            }
        }

        // Update switchedPortions if we have moves
        if (toSwitch.size() != portions.size()) {
            TVector<TPortionInfo> tmp;
            tmp.reserve(toSwitch.size());
            for (auto* portionInfo : toSwitch) {
                tmp.emplace_back(std::move(*portionInfo));
            }
            portions.swap(tmp);
        }

        for (const auto& [mark, id] : marksGranules.GetOrderedMarks()) {
            ui64 tmpGranule = changes->SetTmpGranule(pathId, mark);

            for (const auto& batch : idBatches[id]) {
                // Cannot set snapshot here. It would be set in committing transaction in ApplyChanges().
                auto newPortions = MakeAppendedPortions(pathId, indexInfo, tieringMap, batch, tmpGranule, TSnapshot{}, blobs);
                Y_VERIFY(newPortions.size() > 0);
                for (auto& portion : newPortions) {
                    changes->AppendedPortions.emplace_back(std::move(portion));
                }
            }
        }
    } else {
        auto batches = SliceGranuleBatches(indexInfo, *changes, srcBatches, ts0);

        changes->SetTmpGranule(pathId, ts0);
        for (auto& [ts, batch] : batches) {
            // Tmp granule would be updated to correct value in ApplyChanges()
            ui64 tmpGranule = changes->SetTmpGranule(pathId, ts);

            // Cannot set snapshot here. It would be set in committing transaction in ApplyChanges().
            auto portions = MakeAppendedPortions(pathId, indexInfo, tieringMap, batch, tmpGranule, TSnapshot{}, blobs);
            Y_VERIFY(portions.size() > 0);
            for (auto& portion : portions) {
                changes->AppendedPortions.emplace_back(std::move(portion));
            }
        }
    }

    return blobs;
}

TVector<TString> TColumnEngineForLogs::CompactBlobs(const TIndexInfo& indexInfo, const THashMap<ui64, NKikimr::NOlap::TTiering>& tieringMap,
                                                    std::shared_ptr<TColumnEngineChanges> changes) {
    Y_VERIFY(changes);
    Y_VERIFY(changes->CompactionInfo);
    Y_VERIFY(changes->DataToIndex.empty()); // not used
    Y_VERIFY(!changes->Blobs.empty()); // src data
    Y_VERIFY(!changes->SwitchedPortions.empty()); // src meta
    Y_VERIFY(changes->AppendedPortions.empty()); // dst meta

    auto castedChanges = std::static_pointer_cast<TChanges>(changes);
    if (castedChanges->CompactionInfo->InGranule) {
        return CompactInGranule(indexInfo, tieringMap, castedChanges);
    }
    return CompactSplitGranule(indexInfo, tieringMap, castedChanges);
}

TVector<TString> TColumnEngineForLogs::EvictBlobs(const TIndexInfo& indexInfo, const THashMap<ui64, NKikimr::NOlap::TTiering>& tieringMap,
                                                  std::shared_ptr<TColumnEngineChanges> changes) {
    Y_VERIFY(changes);
    Y_VERIFY(!changes->Blobs.empty()); // src data
    Y_VERIFY(!changes->PortionsToEvict.empty()); // src meta
    Y_VERIFY(changes->EvictedRecords.empty()); // dst meta

    TVector<TString> newBlobs;
    TVector<std::pair<TPortionInfo, TPortionEvictionFeatures>> evicted;
    evicted.reserve(changes->PortionsToEvict.size());

    for (auto& [portionInfo, evictFeatures] : changes->PortionsToEvict) {
        Y_VERIFY(!portionInfo.Empty());
        Y_VERIFY(portionInfo.IsActive());

        if (UpdateEvictedPortion(portionInfo, indexInfo, tieringMap, evictFeatures, changes->Blobs,
                changes->EvictedRecords, newBlobs)) {
            Y_VERIFY(portionInfo.TierName == evictFeatures.TargetTierName);
            evicted.emplace_back(std::move(portionInfo), evictFeatures);
        }
    }

    changes->PortionsToEvict.swap(evicted);
    return newBlobs;
}

}
