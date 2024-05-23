#include "portion_info.h"
#include "constructor.h"
#include <ydb/core/tx/columnshard/blobs_reader/task.h>
#include <ydb/core/tx/columnshard/data_sharing/protos/data.pb.h>
#include <ydb/core/tx/columnshard/engines/column_engine.h>
#include <ydb/core/tx/columnshard/engines/db_wrapper.h>
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>
#include <ydb/core/tx/columnshard/engines/storage/chunks/column.h>
#include <ydb/core/tx/columnshard/engines/storage/chunks/data.h>
#include <ydb/core/formats/arrow/arrow_filter.h>
#include <ydb/core/formats/arrow/size_calcer.h>
#include <ydb/core/formats/arrow/simple_arrays_cache.h>

#include <util/system/tls.h>

namespace NKikimr::NOlap {

std::shared_ptr<arrow::Scalar> TPortionInfo::MaxValue(ui32 columnId) const {
    std::shared_ptr<arrow::Scalar> result;
    for (auto&& i : Records) {
        if (i.ColumnId == columnId) {
            if (!i.GetMeta().GetMax()) {
                return nullptr;
            }
            if (!result || NArrow::ScalarCompare(result, i.GetMeta().GetMax()) < 0) {
                result = i.GetMeta().GetMax();
            }
        }
    }
    return result;
}

ui64 TPortionInfo::GetColumnRawBytes(const std::vector<ui32>& columnIds, const bool validation) const {
    return GetColumnRawBytes(std::set<ui32>(columnIds.begin(), columnIds.end()), validation);
}

ui64 TPortionInfo::GetColumnRawBytes(const std::optional<std::set<ui32>>& entityIds, const bool validation) const {
    ui64 sum = 0;
    const auto aggr = [&](const TColumnRecord& r) {
        sum += r.GetMeta().GetRawBytes();
    };
    AggregateIndexChunksData(aggr, Records, entityIds, validation);
    return sum;
}

ui64 TPortionInfo::GetColumnBlobBytes(const std::optional<std::set<ui32>>& entityIds, const bool validation) const {
    ui64 sum = 0;
    const auto aggr = [&](const TColumnRecord& r) {
        sum += r.GetBlobRange().GetSize();
    };
    AggregateIndexChunksData(aggr, Records, entityIds, validation);
    return sum;
}

ui64 TPortionInfo::GetColumnBlobBytes(const std::vector<ui32>& columnIds, const bool validation) const {
    return GetColumnBlobBytes(std::set<ui32>(columnIds.begin(), columnIds.end()), validation);
}

ui64 TPortionInfo::GetIndexRawBytes(const std::optional<std::set<ui32>>& entityIds, const bool validation) const {
    ui64 sum = 0;
    const auto aggr = [&](const TIndexChunk& r) {
        sum += r.GetRawBytes();
    };
    AggregateIndexChunksData(aggr, Indexes, entityIds, validation);
    return sum;
}

TString TPortionInfo::DebugString(const bool withDetails) const {
    TStringBuilder sb;
    sb << "(portion_id:" << Portion << ";" <<
        "path_id:" << PathId << ";records_count:" << NumRows() << ";"
        "min_schema_snapshot:(" << MinSnapshotDeprecated.DebugString() << ");"
        "schema_version:" << SchemaVersion.value_or(0) << ";";
    if (withDetails) {
        sb <<
            "records_snapshot_min:(" << RecordSnapshotMin().DebugString() << ");" <<
            "records_snapshot_max:(" << RecordSnapshotMax().DebugString() << ");" <<
            "from:" << IndexKeyStart().DebugString() << ";" <<
            "to:" << IndexKeyEnd().DebugString() << ";";
    }
    sb <<
        "column_size:" << GetColumnBlobBytes() << ";" <<
        "index_size:" << GetIndexBlobBytes() << ";" <<
        "meta:(" << Meta.DebugString() << ");";
    if (RemoveSnapshot.Valid()) {
        sb << "remove_snapshot:(" << RemoveSnapshot.DebugString() << ");";
    }
    sb << "chunks:(" << Records.size() << ");";
    if (IS_TRACE_LOG_ENABLED(NKikimrServices::TX_COLUMNSHARD)) {
        std::vector<TBlobRange> blobRanges;
        for (auto&& i : Records) {
            blobRanges.emplace_back(RestoreBlobRange(i.BlobRange));
        }
        sb << "blobs:" << JoinSeq(",", blobRanges) << ";ranges_count:" << blobRanges.size() << ";";
        sb << "blob_ids:" << JoinSeq(",", BlobIds) << ";blobs_count:" << BlobIds.size() << ";";
    }
    return sb << ")";
}

std::vector<const NKikimr::NOlap::TColumnRecord*> TPortionInfo::GetColumnChunksPointers(const ui32 columnId) const {
    std::vector<const TColumnRecord*> result;
    for (auto&& c : Records) {
        if (c.ColumnId == columnId) {
            Y_ABORT_UNLESS(c.Chunk == result.size());
            Y_ABORT_UNLESS(c.GetMeta().GetNumRows());
            result.emplace_back(&c);
        }
    }
    return result;
}

void TPortionInfo::RemoveFromDatabase(IDbWrapper& db) const {
    db.ErasePortion(*this);
    for (auto& record : Records) {
        db.EraseColumn(*this, record);
    }
    for (auto& record : Indexes) {
        db.EraseIndex(*this, record);
    }
}

void TPortionInfo::SaveToDatabase(IDbWrapper& db, const ui32 firstPKColumnId, const bool saveOnlyMeta) const {
    FullValidation();
    db.WritePortion(*this);
   if (!saveOnlyMeta) {
        for (auto& record : Records) {
            db.WriteColumn(*this, record, firstPKColumnId);
        }
        for (auto& record : Indexes) {
            db.WriteIndex(*this, record);
        }
    }
}

std::vector<NKikimr::NOlap::TPortionInfo::TPage> TPortionInfo::BuildPages() const {
    std::vector<TPage> pages;
    struct TPart {
    public:
        const TColumnRecord* Record = nullptr;
        const TIndexChunk* Index = nullptr;
        const ui32 RecordsCount;
        TPart(const TColumnRecord* record, const ui32 recordsCount)
            : Record(record)
            , RecordsCount(recordsCount) {

        }
        TPart(const TIndexChunk* record, const ui32 recordsCount)
            : Index(record)
            , RecordsCount(recordsCount) {

        }
    };
    std::map<ui32, std::deque<TPart>> entities;
    std::map<ui32, ui32> currentCursor;
    ui32 currentSize = 0;
    ui32 currentId = 0;
    for (auto&& i : Records) {
        if (currentId != i.GetColumnId()) {
            currentSize = 0;
            currentId = i.GetColumnId();
        }
        currentSize += i.GetMeta().GetNumRows();
        ++currentCursor[currentSize];
        entities[i.GetColumnId()].emplace_back(&i, i.GetMeta().GetNumRows());
    }
    for (auto&& i : Indexes) {
        if (currentId != i.GetIndexId()) {
            currentSize = 0;
            currentId = i.GetIndexId();
        }
        currentSize += i.GetRecordsCount();
        ++currentCursor[currentSize];
        entities[i.GetIndexId()].emplace_back(&i, i.GetRecordsCount());
    }
    const ui32 entitiesCount = entities.size();
    ui32 predCount = 0;
    for (auto&& i : currentCursor) {
        if (i.second != entitiesCount) {
            continue;
        }
        std::vector<const TColumnRecord*> records;
        std::vector<const TIndexChunk*> indexes;
        for (auto&& c : entities) {
            ui32 readyCount = 0;
            while (readyCount < i.first - predCount && c.second.size()) {
                if (c.second.front().Record) {
                    records.emplace_back(c.second.front().Record);
                } else {
                    AFL_VERIFY(c.second.front().Index);
                    indexes.emplace_back(c.second.front().Index);
                }
                readyCount += c.second.front().RecordsCount;
                c.second.pop_front();
            }
            AFL_VERIFY(readyCount == i.first - predCount)("ready", readyCount)("cursor", i.first)("pred_cursor", predCount);
        }
        pages.emplace_back(std::move(records), std::move(indexes), i.first - predCount);
        predCount = i.first;
    }
    for (auto&& i : entities) {
        AFL_VERIFY(i.second.empty());
    }
    return pages;
}

ui64 TPortionInfo::GetMetadataMemorySize() const {
    return sizeof(TPortionInfo) + Records.size() * (sizeof(TColumnRecord) + 8) + Indexes.size() * sizeof(TIndexChunk) + BlobIds.size() * sizeof(TUnifiedBlobId)
        - sizeof(TPortionMeta) + Meta.GetMetadataMemorySize();
}

ui64 TPortionInfo::GetTxVolume() const {
    return 1024 + Records.size() * 256 + Indexes.size() * 256;
}

void TPortionInfo::SerializeToProto(NKikimrColumnShardDataSharingProto::TPortionInfo& proto) const {
    proto.SetPathId(PathId);
    proto.SetPortionId(Portion);
    proto.SetSchemaVersion(GetSchemaVersionVerified());
    *proto.MutableMinSnapshotDeprecated() = MinSnapshotDeprecated.SerializeToProto();
    if (!RemoveSnapshot.IsZero()) {
        *proto.MutableRemoveSnapshot() = RemoveSnapshot.SerializeToProto();
    }
    for (auto&& i : BlobIds) {
        *proto.AddBlobIds() = i.SerializeToProto();
    }

    *proto.MutableMeta() = Meta.SerializeToProto();

    for (auto&& r : Records) {
        *proto.AddRecords() = r.SerializeToProto();
    }

    for (auto&& r : Indexes) {
        *proto.AddIndexes() = r.SerializeToProto();
    }
}

TConclusionStatus TPortionInfo::DeserializeFromProto(const NKikimrColumnShardDataSharingProto::TPortionInfo& proto, const TIndexInfo& info) {
    PathId = proto.GetPathId();
    Portion = proto.GetPortionId();
    SchemaVersion = proto.GetSchemaVersion();
    for (auto&& i : proto.GetBlobIds()) {
        auto blobId = TUnifiedBlobId::BuildFromProto(i);
        if (!blobId) {
            return blobId;
        }
        BlobIds.emplace_back(blobId.DetachResult());
    }
    {
        auto parse = MinSnapshotDeprecated.DeserializeFromProto(proto.GetMinSnapshotDeprecated());
        if (!parse) {
            return parse;
        }
    }
    if (proto.HasRemoveSnapshot()) {
        auto parse = RemoveSnapshot.DeserializeFromProto(proto.GetRemoveSnapshot());
        if (!parse) {
            return parse;
        }
    }
    for (auto&& i : proto.GetRecords()) {
        auto parse = TColumnRecord::BuildFromProto(i, info.GetColumnFeaturesVerified(i.GetColumnId()));
        if (!parse) {
            return parse;
        }
        Records.emplace_back(std::move(parse.DetachResult()));
    }
    for (auto&& i : proto.GetIndexes()) {
        auto parse = TIndexChunk::BuildFromProto(i);
        if (!parse) {
            return parse;
        }
        Indexes.emplace_back(std::move(parse.DetachResult()));
    }
    return TConclusionStatus::Success();
}

TConclusion<TPortionInfo> TPortionInfo::BuildFromProto(const NKikimrColumnShardDataSharingProto::TPortionInfo& proto, const TIndexInfo& info) {
    TPortionMetaConstructor constructor;
    if (!constructor.LoadMetadata(proto.GetMeta(), info)) {
        return TConclusionStatus::Fail("cannot parse meta");
    }
    TPortionInfo result(constructor.Build());
    auto parse = result.DeserializeFromProto(proto, info);
    if (!parse) {
        return parse;
    }
    return result;
}

THashMap<NKikimr::NOlap::TChunkAddress, TString> TPortionInfo::DecodeBlobAddresses(NBlobOperations::NRead::TCompositeReadBlobs&& blobs, const TIndexInfo& indexInfo) const {
    THashMap<TChunkAddress, TString> result;
    for (auto&& i : blobs) {
        for (auto&& b : i.second) {
            bool found = false;
            TString columnStorageId;
            ui32 columnId = 0;
            for (auto&& record : Records) {
                if (RestoreBlobRange(record.GetBlobRange()) == b.first) {
                    if (columnId != record.GetColumnId()) {
                        columnStorageId = GetColumnStorageId(record.GetColumnId(), indexInfo);
                    }
                    if (columnStorageId != i.first) {
                        continue;
                    }
                    result.emplace(record.GetAddress(), std::move(b.second));
                    found = true;
                    break;
                }
            }
            if (found) {
                continue;
            }
            for (auto&& record : Indexes) {
                if (RestoreBlobRange(record.GetBlobRange()) == b.first) {
                    if (columnId != record.GetIndexId()) {
                        columnStorageId = indexInfo.GetIndexStorageId(record.GetIndexId());
                    }
                    if (columnStorageId != i.first) {
                        continue;
                    }
                    result.emplace(record.GetAddress(), std::move(b.second));
                    found = true;
                    break;
                }
            }
            AFL_VERIFY(found)("blobs", blobs.DebugString())("records", DebugString(true))("problem", b.first);
        }
    }
    return result;
}

const TString& TPortionInfo::GetColumnStorageId(const ui32 columnId, const TIndexInfo& indexInfo) const {
    return indexInfo.GetColumnStorageId(columnId, GetMeta().GetTierName());
}

const TString& TPortionInfo::GetEntityStorageId(const ui32 columnId, const TIndexInfo& indexInfo) const {
    return indexInfo.GetEntityStorageId(columnId, GetMeta().GetTierName());
}

ISnapshotSchema::TPtr TPortionInfo::GetSchema(const TVersionedIndex& index) const {
    AFL_VERIFY(SchemaVersion);
    if (SchemaVersion) {
        auto schema = index.GetSchema(SchemaVersion.value());
        AFL_VERIFY(!!schema)("details", TStringBuilder() << "cannot find schema for version " << SchemaVersion.value());
        return schema;
    }
    return index.GetSchema(MinSnapshotDeprecated);
}

void TPortionInfo::FillBlobRangesByStorage(THashMap<TString, THashSet<TBlobRange>>& result, const TIndexInfo& indexInfo) const {
    for (auto&& i : Records) {
        const TString& storageId = GetColumnStorageId(i.GetColumnId(), indexInfo);
        AFL_VERIFY(result[storageId].emplace(RestoreBlobRange(i.GetBlobRange())).second)("blob_id", RestoreBlobRange(i.GetBlobRange()).ToString());
    }
    for (auto&& i : Indexes) {
        const TString& storageId = indexInfo.GetIndexStorageId(i.GetIndexId());
        AFL_VERIFY(result[storageId].emplace(RestoreBlobRange(i.GetBlobRange())).second)("blob_id", RestoreBlobRange(i.GetBlobRange()).ToString());
    }
}

void TPortionInfo::FillBlobRangesByStorage(THashMap<TString, THashSet<TBlobRange>>& result, const TVersionedIndex& index) const {
    auto schema = GetSchema(index);
    return FillBlobRangesByStorage(result, schema->GetIndexInfo());
}

void TPortionInfo::FillBlobIdsByStorage(THashMap<TString, THashSet<TUnifiedBlobId>>& result, const TIndexInfo& indexInfo) const {
    THashMap<TString, THashSet<TBlobRangeLink16::TLinkId>> local;
    THashSet<TBlobRangeLink16::TLinkId>* currentHashLocal;
    THashSet<TUnifiedBlobId>* currentHashResult;
    ui32 lastEntityId = 0;
    TString lastStorageId;
    ui32 lastBlobIdx = BlobIds.size();
    for (auto&& i : Records) {
        if (lastEntityId != i.GetEntityId()) {
            const TString& storageId = GetColumnStorageId(i.GetEntityId(), indexInfo);
            if (storageId != lastStorageId) {
                currentHashResult = &result[storageId];
                currentHashLocal = &local[storageId];
                lastStorageId = storageId;
                lastBlobIdx = BlobIds.size();
            }
        }
        if (lastBlobIdx != i.GetBlobRange().GetBlobIdxVerified() && currentHashLocal->emplace(i.GetBlobRange().GetBlobIdxVerified()).second) {
            auto blobId = GetBlobId(i.GetBlobRange().GetBlobIdxVerified());
            AFL_VERIFY(currentHashResult->emplace(blobId).second)("blob_id", blobId.ToStringNew());
            lastBlobIdx = i.GetBlobRange().GetBlobIdxVerified();
        }
    }
    for (auto&& i : Indexes) {
        if (lastEntityId != i.GetEntityId()) {
            const TString& storageId = indexInfo.GetIndexStorageId(i.GetEntityId());
            if (storageId != lastStorageId) {
                currentHashResult = &result[storageId];
                currentHashLocal = &local[storageId];
                lastStorageId = storageId;
                lastBlobIdx = BlobIds.size();
            }
        }
        if (lastBlobIdx != i.GetBlobRange().GetBlobIdxVerified() && currentHashLocal->emplace(i.GetBlobRange().GetBlobIdxVerified()).second) {
            auto blobId = GetBlobId(i.GetBlobRange().GetBlobIdxVerified());
            AFL_VERIFY(currentHashResult->emplace(blobId).second)("blob_id", blobId.ToStringNew());
            lastBlobIdx = i.GetBlobRange().GetBlobIdxVerified();
        }
    }
}

void TPortionInfo::FillBlobIdsByStorage(THashMap<TString, THashSet<TUnifiedBlobId>>& result, const TVersionedIndex& index) const {
    auto schema = GetSchema(index);
    return FillBlobIdsByStorage(result, schema->GetIndexInfo());
}

THashMap<TString, THashMap<TUnifiedBlobId, std::vector<std::shared_ptr<IPortionDataChunk>>>> TPortionInfo::RestoreEntityChunks(NBlobOperations::NRead::TCompositeReadBlobs& blobs, const TIndexInfo& indexInfo) const {
    THashMap<TString, THashMap<TUnifiedBlobId, std::vector<std::shared_ptr<IPortionDataChunk>>>> result;
    for (auto&& c : GetRecords()) {
        const TString& storageId = GetColumnStorageId(c.GetColumnId(), indexInfo);
        auto& storageRecords = result[storageId];
        auto& blobRecords = storageRecords[GetBlobId(c.GetBlobRange().GetBlobIdxVerified())];
        blobRecords.emplace_back(std::make_shared<NChunks::TChunkPreparation>(blobs.Extract(storageId, RestoreBlobRange(c.GetBlobRange())), c, indexInfo.GetColumnFeaturesVerified(c.GetColumnId())));
        blobRecords.back()->SetChunkIdx(c.GetChunkIdx());
    }
    for (auto&& c : GetIndexes()) {
        const TString& storageId = indexInfo.GetIndexStorageId(c.GetIndexId());
        auto& storageRecords = result[storageId];
        auto& blobRecords = storageRecords[GetBlobId(c.GetBlobRange().GetBlobIdxVerified())];
        blobRecords.emplace_back(std::make_shared<NChunks::TPortionIndexChunk>(c.GetAddress(), c.GetRecordsCount(), c.GetRawBytes(), blobs.Extract(storageId, RestoreBlobRange(c.GetBlobRange()))));
        blobRecords.back()->SetChunkIdx(c.GetChunkIdx());
    }
    return result;
}

THashMap<TString, THashMap<NKikimr::NOlap::TUnifiedBlobId, std::vector<NKikimr::NOlap::TEntityChunk>>> TPortionInfo::GetEntityChunks(const TIndexInfo& indexInfo) const {
    THashMap<TString, THashMap<TUnifiedBlobId, std::vector<TEntityChunk>>> result;
    for (auto&& c : GetRecords()) {
        const TString& storageId = GetColumnStorageId(c.GetColumnId(), indexInfo);
        auto& storageRecords = result[storageId];
        auto& blobRecords = storageRecords[GetBlobId(c.GetBlobRange().GetBlobIdxVerified())];
        blobRecords.emplace_back(TEntityChunk(c.GetAddress(), c.GetMeta().GetNumRows(), c.GetMeta().GetRawBytes(), c.GetBlobRange()));
    }
    for (auto&& c : GetIndexes()) {
        const TString& storageId = indexInfo.GetIndexStorageId(c.GetIndexId());
        auto& storageRecords = result[storageId];
        auto& blobRecords = storageRecords[GetBlobId(c.GetBlobRange().GetBlobIdxVerified())];
        blobRecords.emplace_back(TEntityChunk(c.GetAddress(), c.GetRecordsCount(), c.GetRawBytes(), c.GetBlobRange()));
    }
    return result;
}

void TPortionInfo::ReorderChunks() {
    {
        auto pred = [](const TColumnRecord& l, const TColumnRecord& r) {
            return l.GetAddress() < r.GetAddress();
        };
        std::sort(Records.begin(), Records.end(), pred);
        std::optional<TChunkAddress> chunk;
        for (auto&& i : Records) {
            if (!chunk) {
                chunk = i.GetAddress();
            } else {
                AFL_VERIFY(*chunk < i.GetAddress());
                chunk = i.GetAddress();
            }
            AFL_VERIFY(chunk->GetEntityId());
        }
    }
    {
        auto pred = [](const TIndexChunk& l, const TIndexChunk& r) {
            return l.GetAddress() < r.GetAddress();
        };
        std::sort(Indexes.begin(), Indexes.end(), pred);
        std::optional<TChunkAddress> chunk;
        for (auto&& i : Indexes) {
            if (!chunk) {
                chunk = i.GetAddress();
            } else {
                AFL_VERIFY(*chunk < i.GetAddress());
                chunk = i.GetAddress();
            }
            AFL_VERIFY(chunk->GetEntityId());
        }
    }
}

void TPortionInfo::FullValidation() const {
    CheckChunksOrder(Records);
    CheckChunksOrder(Indexes);
    AFL_VERIFY(PathId);
    AFL_VERIFY(Portion);
    AFL_VERIFY(MinSnapshotDeprecated.Valid());
    std::set<ui32> blobIdxs;
    for (auto&& i : Records) {
        blobIdxs.emplace(i.GetBlobRange().GetBlobIdxVerified());
    }
    for (auto&& i : Indexes) {
        blobIdxs.emplace(i.GetBlobRange().GetBlobIdxVerified());
    }
    if (BlobIds.size()) {
        AFL_VERIFY(BlobIds.size() == blobIdxs.size());
        AFL_VERIFY(BlobIds.size() == *blobIdxs.rbegin() + 1);
    } else {
        AFL_VERIFY(blobIdxs.empty());
    }
}

ui64 TPortionInfo::GetMinMemoryForReadColumns(const std::optional<std::set<ui32>>& columnIds) const {
    ui32 columnId = 0;
    ui32 chunkIdx = 0;

    struct TDelta {
        i64 BlobBytes = 0;
        i64 RawBytes = 0;
        void operator+=(const TDelta& add) {
            BlobBytes += add.BlobBytes;
            RawBytes += add.RawBytes;
        }
    };

    std::map<ui64, TDelta> diffByPositions;
    ui64 position = 0;
    ui64 RawBytesCurrent = 0;
    ui64 BlobBytesCurrent = 0;
    std::optional<ui32> recordsCount;

    const auto doFlushColumn = [&]() {
        if (!recordsCount && position) {
            recordsCount = position;
        } else {
            AFL_VERIFY(*recordsCount == position);
        }
        if (position) {
            TDelta delta;
            delta.RawBytes = -1 * RawBytesCurrent;
            delta.BlobBytes = -1 * BlobBytesCurrent;
            diffByPositions[position] += delta;
        }
        position = 0;
        chunkIdx = 0;
        RawBytesCurrent = 0;
        BlobBytesCurrent = 0;
    };

    for (auto&& i : Records) {
        if (columnIds && !columnIds->contains(i.GetColumnId())) {
            continue;
        }
        if (columnId != i.GetColumnId()) {
            if (columnId) {
                doFlushColumn();
            }
            AFL_VERIFY(i.GetColumnId() > columnId);
            AFL_VERIFY(i.GetChunkIdx() == 0);
            columnId = i.GetColumnId();
        } else {
            AFL_VERIFY(i.GetChunkIdx() == chunkIdx + 1);
        }
        chunkIdx = i.GetChunkIdx();
        TDelta delta;
        delta.RawBytes = -1 * RawBytesCurrent + i.GetMeta().GetRawBytes();
        delta.BlobBytes = -1 * BlobBytesCurrent + i.GetBlobRange().Size;
        diffByPositions[position] += delta;
        position += i.GetMeta().GetNumRows();
        RawBytesCurrent = i.GetMeta().GetRawBytes();
        BlobBytesCurrent = i.GetBlobRange().Size;
    }
    if (columnId) {
        doFlushColumn();
    }
    i64 maxRawBytes = 0;
    TDelta current;
    for (auto&& i : diffByPositions) {
        current += i.second;
        AFL_VERIFY(current.BlobBytes >= 0);
        AFL_VERIFY(current.RawBytes >= 0);
        if (maxRawBytes < current.RawBytes) {
            maxRawBytes = current.RawBytes;
        }
    }
    AFL_VERIFY(current.BlobBytes == 0)("real", current.BlobBytes);
    AFL_VERIFY(current.RawBytes == 0)("real", current.RawBytes);
    return maxRawBytes;
}

namespace {
template <class TExternalBlobInfo>
TPortionInfo::TPreparedBatchData PrepareForAssembleImpl(const TPortionInfo& portion, const ISnapshotSchema& dataSchema, const ISnapshotSchema& resultSchema,
    THashMap<TChunkAddress, TExternalBlobInfo>& blobsData) {
    std::vector<TPortionInfo::TColumnAssemblingInfo> columns;
    auto arrowResultSchema = resultSchema.GetSchema();
    columns.reserve(arrowResultSchema->num_fields());
    const ui32 rowsCount = portion.GetRecordsCount();
    for (auto&& i : arrowResultSchema->fields()) {
        columns.emplace_back(rowsCount, dataSchema.GetColumnLoaderOptional(i->name()), resultSchema.GetColumnLoaderOptional(i->name()));
    }
    {
        int skipColumnId = -1;
        TPortionInfo::TColumnAssemblingInfo* currentAssembler = nullptr;
        for (auto& rec : portion.GetRecords()) {
            if (skipColumnId == (int)rec.ColumnId) {
                continue;
            }
            if (!currentAssembler || rec.ColumnId != currentAssembler->GetColumnId()) {
                const i32 resultPos = resultSchema.GetFieldIndex(rec.ColumnId);
                if (resultPos < 0) {
                    skipColumnId = rec.ColumnId;
                    continue;
                }
                AFL_VERIFY((ui32)resultPos < columns.size());
                currentAssembler = &columns[resultPos];
            }
            auto it = blobsData.find(rec.GetAddress());
            AFL_VERIFY(it != blobsData.end())("size", blobsData.size())("address", rec.GetAddress().DebugString());
            currentAssembler->AddBlobInfo(rec.Chunk, rec.GetMeta().GetNumRows(), std::move(it->second));
            blobsData.erase(it);
        }
    }

    // Make chunked arrays for columns
    std::vector<TPortionInfo::TPreparedColumn> preparedColumns;
    preparedColumns.reserve(columns.size());
    for (auto& c : columns) {
        preparedColumns.emplace_back(c.Compile());
    }

    return TPortionInfo::TPreparedBatchData(std::move(preparedColumns), arrowResultSchema, rowsCount);
}

}

namespace {
class TChunkAccessor {
private:
    const std::vector<TDeserializeChunkedArray::TChunk>& Chunks;
    const std::shared_ptr<TColumnLoader> Loader;
public:
    TChunkAccessor(const std::vector<TDeserializeChunkedArray::TChunk>& chunks, const std::shared_ptr<TColumnLoader>& loader)
        : Chunks(chunks)
        , Loader(loader)
    {

    }
    ui64 GetChunksCount() const {
        return Chunks.size();
    }
    ui64 GetChunkLength(const ui32 idx) const {
        return Chunks[idx].GetRecordsCount();
    }
    std::shared_ptr<arrow::Array> GetArray(const ui32 idx) const {
        return Chunks[idx].GetArrayVerified(Loader);
    }
};
}

ISnapshotSchema::TPtr TPortionInfo::TSchemaCursor::GetSchema(const TPortionInfoConstructor& portion) {
    if (!CurrentSchema || portion.GetMinSnapshotDeprecatedVerified() != LastSnapshot) {
        CurrentSchema = portion.GetSchema(VersionedIndex);
        LastSnapshot = portion.GetMinSnapshotDeprecatedVerified();
    }
    AFL_VERIFY(!!CurrentSchema);
    return CurrentSchema;
}

NArrow::NAccessor::IChunkedArray::TCurrentChunkAddress TDeserializeChunkedArray::DoGetChunk(const std::optional<TCurrentChunkAddress>& chunkCurrent, const ui64 position) const {
    TChunkAccessor accessor(Chunks, Loader);
    return SelectChunk(chunkCurrent, position, accessor);
}

TPortionInfo::TPreparedBatchData TPortionInfo::PrepareForAssemble(const ISnapshotSchema& dataSchema, const ISnapshotSchema& resultSchema, THashMap<TChunkAddress, TString>& blobsData) const {
    return PrepareForAssembleImpl(*this, dataSchema, resultSchema, blobsData);
}

TPortionInfo::TPreparedBatchData TPortionInfo::PrepareForAssemble(const ISnapshotSchema& dataSchema, const ISnapshotSchema& resultSchema, THashMap<TChunkAddress, TAssembleBlobInfo>& blobsData) const {
    return PrepareForAssembleImpl(*this, dataSchema, resultSchema, blobsData);
}

bool TPortionInfo::NeedShardingFilter(const TGranuleShardingInfo& shardingInfo) const {
    if (ShardingVersion && shardingInfo.GetSnapshotVersion() <= *ShardingVersion) {
        return false;
    }
    return true;
}

std::shared_ptr<TDeserializeChunkedArray> TPortionInfo::TPreparedColumn::AssembleForSeqAccess() const {
    Y_ABORT_UNLESS(!Blobs.empty());

    std::vector<TDeserializeChunkedArray::TChunk> chunks;
    chunks.reserve(Blobs.size());
    ui64 recordsCount = 0;
    for (auto& blob : Blobs) {
        chunks.push_back(blob.BuildDeserializeChunk(Loader));
        recordsCount += blob.GetExpectedRowsCountVerified();
    }

    return std::make_shared<TDeserializeChunkedArray>(recordsCount, Loader, std::move(chunks));
}

std::shared_ptr<arrow::ChunkedArray> TPortionInfo::TPreparedColumn::Assemble() const {
    Y_ABORT_UNLESS(!Blobs.empty());

    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    batches.reserve(Blobs.size());
    for (auto& blob : Blobs) {
        batches.push_back(blob.BuildRecordBatch(*Loader));
        Y_ABORT_UNLESS(batches.back());
    }

    auto res = arrow::Table::FromRecordBatches(batches);
    Y_VERIFY_S(res.ok(), res.status().message());
    return (*res)->column(0);
}

TDeserializeChunkedArray::TChunk TPortionInfo::TAssembleBlobInfo::BuildDeserializeChunk(const std::shared_ptr<TColumnLoader>& loader) const {
    if (NullRowsCount) {
        Y_ABORT_UNLESS(!Data);
        auto emptyBatch = NArrow::MakeEmptyBatch(loader->GetExpectedSchema(), NullRowsCount);
        AFL_VERIFY(emptyBatch->num_columns() == 1);
        return TDeserializeChunkedArray::TChunk(emptyBatch->column(0));
    } else {
        AFL_VERIFY(ExpectedRowsCount);
        return TDeserializeChunkedArray::TChunk(*ExpectedRowsCount, Data);
    }
}

std::shared_ptr<arrow::RecordBatch> TPortionInfo::TAssembleBlobInfo::BuildRecordBatch(const TColumnLoader& loader) const {
    if (NullRowsCount) {
        Y_ABORT_UNLESS(!Data);
        return NArrow::MakeEmptyBatch(loader.GetExpectedSchema(), NullRowsCount);
    } else {
        auto result = loader.Apply(Data);
        if (!result.ok()) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "cannot unpack batch")("error", result.status().ToString())("loader", loader.DebugString());
            return nullptr;
        }
        if (ExpectedRowsCount) {
            AFL_VERIFY((*result)->num_rows() == ExpectedRowsCount)("real", (*result)->num_rows())("expected", ExpectedRowsCount);
        }
        return *result;
    }
}

std::shared_ptr<NArrow::TGeneralContainer> TPortionInfo::TPreparedBatchData::AssembleForSeqAccess() const {
    std::vector<std::shared_ptr<NArrow::NAccessor::IChunkedArray>> columns;
    std::vector<std::shared_ptr<arrow::Field>> fields;
    for (auto&& i : Columns) {
        columns.emplace_back(i.AssembleForSeqAccess());
        fields.emplace_back(i.GetField());
    }

    return std::make_shared<NArrow::TGeneralContainer>(std::make_shared<arrow::Schema>(fields), std::move(columns));
}

std::shared_ptr<arrow::Table> TPortionInfo::TPreparedBatchData::AssembleTable(const TAssembleOptions& options) const {
    std::vector<std::shared_ptr<arrow::ChunkedArray>> columns;
    std::vector<std::shared_ptr<arrow::Field>> fields;
    for (auto&& i : Columns) {
        if (!options.IsAcceptedColumn(i.GetColumnId())) {
            continue;
        }
        std::shared_ptr<arrow::Scalar> scalar;
        if (options.IsConstantColumn(i.GetColumnId(), scalar)) {
            auto type = i.GetField()->type();
            std::shared_ptr<arrow::Array> arr;
            if (scalar) {
                arr = NArrow::TThreadSimpleArraysCache::GetConst(type, scalar, RowsCount);
            } else {
                arr = NArrow::TThreadSimpleArraysCache::GetNull(type, RowsCount);
            }
            columns.emplace_back(std::make_shared<arrow::ChunkedArray>(arr));
        } else {
            columns.emplace_back(i.Assemble());
        }
        fields.emplace_back(i.GetField());
    }

    return arrow::Table::Make(std::make_shared<arrow::Schema>(fields), columns);
}

std::shared_ptr<arrow::RecordBatch> TPortionInfo::TPreparedBatchData::Assemble(const TAssembleOptions& options) const {
    return NArrow::ToBatch(AssembleTable(options), true);
}

}
