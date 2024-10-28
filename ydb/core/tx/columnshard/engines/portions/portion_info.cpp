#include "column_record.h"
#include "constructor.h"
#include "portion_info.h"

#include <ydb/core/tx/columnshard/data_sharing/protos/data.pb.h>
#include <ydb/core/tx/columnshard/engines/db_wrapper.h>
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>
#include <ydb/core/tx/columnshard/engines/storage/chunks/data.h>

namespace NKikimr::NOlap {

ui64 TPortionInfo::GetColumnRawBytes(const std::set<ui32>& entityIds, const bool validation) const {
    ui64 sum = 0;
    const auto aggr = [&](const TColumnRecord& r) {
        sum += r.GetMeta().GetRawBytes();
    };
    AggregateIndexChunksData(aggr, Records, &entityIds, validation);
    return sum;
}

ui64 TPortionInfo::GetColumnBlobBytes(const std::set<ui32>& entityIds, const bool validation) const {
    ui64 sum = 0;
    const auto aggr = [&](const TColumnRecord& r) {
        sum += r.GetBlobRange().GetSize();
    };
    AggregateIndexChunksData(aggr, Records, &entityIds, validation);
    return sum;
}

ui64 TPortionInfo::GetColumnRawBytes() const {
    AFL_VERIFY(Precalculated);
    return PrecalculatedColumnRawBytes;
}

ui64 TPortionInfo::GetColumnBlobBytes() const {
    AFL_VERIFY(Precalculated);
    return PrecalculatedColumnBlobBytes;
}

ui64 TPortionInfo::GetIndexRawBytes(const std::set<ui32>& entityIds, const bool validation) const {
    ui64 sum = 0;
    const auto aggr = [&](const TIndexChunk& r) {
        sum += r.GetRawBytes();
    };
    AggregateIndexChunksData(aggr, Indexes, &entityIds, validation);
    return sum;
}

ui64 TPortionInfo::GetIndexRawBytes(const bool validation) const {
    ui64 sum = 0;
    const auto aggr = [&](const TIndexChunk& r) {
        sum += r.GetRawBytes();
    };
    AggregateIndexChunksData(aggr, Indexes, nullptr, validation);
    return sum;
}

TString TPortionInfo::DebugString(const bool withDetails) const {
    TStringBuilder sb;
    sb << "(portion_id:" << PortionId << ";"
       << "path_id:" << PathId << ";records_count:" << NumRows()
       << ";"
          "min_schema_snapshot:("
       << MinSnapshotDeprecated.DebugString()
       << ");"
          "schema_version:"
       << SchemaVersion.value_or(0)
       << ";"
          "level:"
       << GetMeta().GetCompactionLevel() << ";";
    if (withDetails) {
        sb << "records_snapshot_min:(" << RecordSnapshotMin().DebugString() << ");"
           << "records_snapshot_max:(" << RecordSnapshotMax().DebugString() << ");"
           << "from:" << IndexKeyStart().DebugString() << ";"
           << "to:" << IndexKeyEnd().DebugString() << ";";
    }
    sb << "column_size:" << GetColumnBlobBytes() << ";"
       << "index_size:" << GetIndexBlobBytes() << ";"
       << "meta:(" << Meta.DebugString() << ");";
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
    return sizeof(TPortionInfo) + Records.size() * (sizeof(TColumnRecord) + 8) + Indexes.size() * sizeof(TIndexChunk) +
           BlobIds.size() * sizeof(TUnifiedBlobId) - sizeof(TPortionMeta) + Meta.GetMetadataMemorySize();
}

ui64 TPortionInfo::GetTxVolume() const {
    return 1024 + Records.size() * 256 + Indexes.size() * 256;
}

void TPortionInfo::SerializeToProto(NKikimrColumnShardDataSharingProto::TPortionInfo& proto) const {
    proto.SetPathId(PathId);
    proto.SetPortionId(PortionId);
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

TConclusionStatus TPortionInfo::DeserializeFromProto(const NKikimrColumnShardDataSharingProto::TPortionInfo& proto) {
    PathId = proto.GetPathId();
    PortionId = proto.GetPortionId();
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
        auto parse = TColumnRecord::BuildFromProto(i);
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
    Precalculate();
    return TConclusionStatus::Success();
}

TConclusion<TPortionInfo> TPortionInfo::BuildFromProto(
    const NKikimrColumnShardDataSharingProto::TPortionInfo& proto, const TIndexInfo& indexInfo) {
    TPortionMetaConstructor constructor;
    if (!constructor.LoadMetadata(proto.GetMeta(), indexInfo)) {
        return TConclusionStatus::Fail("cannot parse meta");
    }
    TPortionInfo result(constructor.Build());
    auto parse = result.DeserializeFromProto(proto);
    if (!parse) {
        return parse;
    }
    return result;
}

const TString& TPortionInfo::GetColumnStorageId(const ui32 columnId, const TIndexInfo& indexInfo) const {
    if (HasInsertWriteId()) {
        return { NBlobOperations::TGlobal::DefaultStorageId };
    }
    return indexInfo.GetColumnStorageId(columnId, GetMeta().GetTierName());
}

const TString& TPortionInfo::GetEntityStorageId(const ui32 columnId, const TIndexInfo& indexInfo) const {
    if (HasInsertWriteId()) {
        return { NBlobOperations::TGlobal::DefaultStorageId };
    }
    return indexInfo.GetEntityStorageId(columnId, GetMeta().GetTierName());
}

const TString& TPortionInfo::GetIndexStorageId(const ui32 indexId, const TIndexInfo& indexInfo) const {
    if (HasInsertWriteId()) {
        return { NBlobOperations::TGlobal::DefaultStorageId };
    }
    return indexInfo.GetIndexStorageId(indexId);
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
    AFL_VERIFY(PortionId);
    AFL_VERIFY(MinSnapshotDeprecated.Valid());
    std::set<ui32> blobIdxs;
    for (auto&& i : Records) {
        blobIdxs.emplace(i.GetBlobRange().GetBlobIdxVerified());
    }
    for (auto&& i : Indexes) {
        if (auto bRange = i.GetBlobRangeOptional()) {
            blobIdxs.emplace(bRange->GetBlobIdxVerified());
        }
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

ISnapshotSchema::TPtr TPortionInfo::TSchemaCursor::GetSchema(const TPortionInfoConstructor& portion) {
    if (!CurrentSchema || portion.GetMinSnapshotDeprecatedVerified() != LastSnapshot) {
        CurrentSchema = portion.GetSchema(VersionedIndex);
        LastSnapshot = portion.GetMinSnapshotDeprecatedVerified();
    }
    AFL_VERIFY(!!CurrentSchema);
    return CurrentSchema;
}

bool TPortionInfo::NeedShardingFilter(const TGranuleShardingInfo& shardingInfo) const {
    if (ShardingVersion && shardingInfo.GetSnapshotVersion() <= *ShardingVersion) {
        return false;
    }
    return true;
}

NKikimr::NOlap::NSplitter::TEntityGroups TPortionInfo::GetEntityGroupsByStorageId(
    const TString& specialTier, const IStoragesManager& storages, const TIndexInfo& indexInfo) const {
    if (HasInsertWriteId()) {
        NSplitter::TEntityGroups groups(storages.GetDefaultOperator()->GetBlobSplitSettings(), IStoragesManager::DefaultStorageId);
        return groups;
    } else {
        return indexInfo.GetEntityGroupsByStorageId(specialTier, storages);
    }
}

void TPortionInfo::Precalculate() {
    AFL_VERIFY(!Precalculated);
    Precalculated = true;
    {
        PrecalculatedColumnRawBytes = 0;
        PrecalculatedColumnBlobBytes = 0;
        const auto aggr = [&](const TColumnRecord& r) {
            PrecalculatedColumnRawBytes += r.GetMeta().GetRawBytes();
            PrecalculatedColumnBlobBytes += r.BlobRange.GetSize();
        };
        AggregateIndexChunksData(aggr, Records, nullptr, true);
    }
}

}   // namespace NKikimr::NOlap
