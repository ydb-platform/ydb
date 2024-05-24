#pragma once
#include "column_record.h"
#include "constructor_meta.h"
#include "index_chunk.h"
#include "portion_info.h"

#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NOlap {
class TPortionInfo;
class TVersionedIndex;
class ISnapshotSchema;
class TIndexChunkLoadContext;
class TGranuleShardingInfo;

class TPortionInfoConstructor {
private:
    YDB_ACCESSOR(ui64, PathId, 0);
    std::optional<ui64> PortionId;

    TPortionMetaConstructor MetaConstructor;

    std::optional<TSnapshot> MinSnapshotDeprecated;
    std::optional<TSnapshot> RemoveSnapshot;
    std::optional<ui64> SchemaVersion;
    std::optional<ui64> ShardingVersion;

    std::vector<TIndexChunk> Indexes;
    YDB_ACCESSOR_DEF(std::vector<TColumnRecord>, Records);
    std::vector<TUnifiedBlobId> BlobIds;

public:
    void SetPortionId(const ui64 value) {
        AFL_VERIFY(value);
        PortionId = value;
    }

    void AddMetadata(const ISnapshotSchema& snapshotSchema, const std::shared_ptr<arrow::RecordBatch>& batch);

    void AddMetadata(const ISnapshotSchema& snapshotSchema, const NArrow::TFirstLastSpecialKeys& firstLastRecords, const NArrow::TMinMaxSpecialKeys& minMaxSpecial) {
        MetaConstructor.FillMetaInfo(firstLastRecords, minMaxSpecial, snapshotSchema.GetIndexInfo());
    }

    ui64 GetPortionIdVerified() const {
        AFL_VERIFY(PortionId);
        AFL_VERIFY(*PortionId);
        return *PortionId;
    }

    TPortionMetaConstructor& MutableMeta() {
        return MetaConstructor;
    }

    const TPortionMetaConstructor& GetMeta() const {
        return MetaConstructor;
    }

    TPortionInfoConstructor(const TPortionInfo& portion, const bool withBlobs, const bool withMetadata)
        : PathId(portion.GetPathId())
        , PortionId(portion.GetPortionId())
        , MinSnapshotDeprecated(portion.GetMinSnapshotDeprecated())
        , RemoveSnapshot(portion.GetRemoveSnapshotOptional())
        , SchemaVersion(portion.GetSchemaVersionOptional())
        , ShardingVersion(portion.GetShardingVersionOptional()) {
        if (withMetadata) {
            MetaConstructor = TPortionMetaConstructor(portion.Meta);
        }
        if (withBlobs) {
            Indexes = portion.GetIndexes();
            Records = portion.GetRecords();
            BlobIds = portion.BlobIds;
        }
    }

    TPortionInfoConstructor(TPortionInfo&& portion)
        : PathId(portion.GetPathId())
        , PortionId(portion.GetPortionId())
        , MinSnapshotDeprecated(portion.GetMinSnapshotDeprecated())
        , RemoveSnapshot(portion.GetRemoveSnapshotOptional())
        , SchemaVersion(portion.GetSchemaVersionOptional())
        , ShardingVersion(portion.GetShardingVersionOptional()) {
        MetaConstructor = TPortionMetaConstructor(portion.Meta);
        Indexes = std::move(portion.Indexes);
        Records = std::move(portion.Records);
        BlobIds = std::move(portion.BlobIds);
    }

    TPortionAddress GetAddress() const {
        return TPortionAddress(PathId, GetPortionIdVerified());
    }

    bool HasRemoveSnapshot() const {
        return !!RemoveSnapshot;
    }

    template <class TChunkInfo>
    static void CheckChunksOrder(const std::vector<TChunkInfo>& chunks) {
        ui32 entityId = 0;
        ui32 chunkIdx = 0;
        for (auto&& i : chunks) {
            if (entityId != i.GetEntityId()) {
                AFL_VERIFY(entityId < i.GetEntityId());
                AFL_VERIFY(i.GetChunkIdx() == 0);
                entityId = i.GetEntityId();
                chunkIdx = 0;
            } else {
                AFL_VERIFY(i.GetChunkIdx() == chunkIdx + 1);
                chunkIdx = i.GetChunkIdx();
            }
            AFL_VERIFY(i.GetEntityId());
        }
    }

    void Merge(TPortionInfoConstructor&& item) {
        AFL_VERIFY(item.PathId == PathId);
        AFL_VERIFY(item.PortionId == PortionId);
        if (item.MinSnapshotDeprecated) {
            if (MinSnapshotDeprecated) {
                AFL_VERIFY(*MinSnapshotDeprecated == *item.MinSnapshotDeprecated);
            } else {
                MinSnapshotDeprecated = item.MinSnapshotDeprecated;
            }
        }
        if (item.RemoveSnapshot) {
            if (RemoveSnapshot) {
                AFL_VERIFY(*RemoveSnapshot == *item.RemoveSnapshot);
            } else {
                RemoveSnapshot = item.RemoveSnapshot;
            }
        }
    }

    TPortionInfoConstructor(const ui64 pathId, const ui64 portionId)
        : PathId(pathId)
        , PortionId(portionId) {
        AFL_VERIFY(PathId);
        AFL_VERIFY(PortionId);
    }

    TPortionInfoConstructor(const ui64 pathId)
        : PathId(pathId) {
        AFL_VERIFY(PathId);
    }

    const TSnapshot& GetMinSnapshotDeprecatedVerified() const {
        AFL_VERIFY(!!MinSnapshotDeprecated);
        return *MinSnapshotDeprecated;
    }

    std::shared_ptr<ISnapshotSchema> GetSchema(const TVersionedIndex& index) const;

    void SetMinSnapshotDeprecated(const TSnapshot& snap) {
        Y_ABORT_UNLESS(snap.Valid());
        MinSnapshotDeprecated = snap;
    }

    void SetSchemaVersion(const ui64 version) {
//        AFL_VERIFY(version);
        SchemaVersion = version;
    }

    void SetShardingVersion(const ui64 version) {
//        AFL_VERIFY(version);
        ShardingVersion = version;
    }

    void SetRemoveSnapshot(const TSnapshot& snap) {
        AFL_VERIFY(!RemoveSnapshot);
        if (snap.Valid()) {
            RemoveSnapshot = snap;
        }
    }

    void SetRemoveSnapshot(const ui64 planStep, const ui64 txId) {
        SetRemoveSnapshot(TSnapshot(planStep, txId));
    }

    void LoadRecord(const TIndexInfo& indexInfo, const TColumnChunkLoadContext& loadContext);

    ui32 GetRecordsCount() const {
        ui32 result = 0;
        std::optional<ui32> columnIdFirst;
        for (auto&& i : Records) {
            if (!columnIdFirst || *columnIdFirst == i.ColumnId) {
                result += i.GetMeta().GetNumRows();
                columnIdFirst = i.ColumnId;
            }
        }
        AFL_VERIFY(columnIdFirst);
        return result;
    }

    TBlobRangeLink16::TLinkId RegisterBlobId(const TUnifiedBlobId& blobId) {
        AFL_VERIFY(blobId.IsValid());
        TBlobRangeLink16::TLinkId idx = 0;
        for (auto&& i : BlobIds) {
            if (i == blobId) {
                return idx;
            }
            ++idx;
        }
        BlobIds.emplace_back(blobId);
        return idx;
    }

    const TBlobRange RestoreBlobRange(const TBlobRangeLink16& linkRange) const {
        return linkRange.RestoreRange(GetBlobId(linkRange.GetBlobIdxVerified()));
    }

    const TUnifiedBlobId& GetBlobId(const TBlobRangeLink16::TLinkId linkId) const {
        AFL_VERIFY(linkId < BlobIds.size());
        return BlobIds[linkId];
    }

    ui32 GetBlobIdsCount() const {
        return BlobIds.size();
    }

    void RegisterBlobIdx(const TChunkAddress& address, const TBlobRangeLink16::TLinkId blobIdx) {
        for (auto&& i : Records) {
            if (i.GetColumnId() == address.GetEntityId() && i.GetChunkIdx() == address.GetChunkIdx()) {
                i.RegisterBlobIdx(blobIdx);
                return;
            }
        }
        for (auto&& i : Indexes) {
            if (i.GetIndexId() == address.GetEntityId() && i.GetChunkIdx() == address.GetChunkIdx()) {
                i.RegisterBlobIdx(blobIdx);
                return;
            }
        }
        AFL_VERIFY(false)("problem", "portion haven't address for blob registration")("address", address.DebugString());
    }

    TString DebugString() const {
        TStringBuilder sb;
        return sb;
    }

    void ReorderChunks() {
        {
            auto pred = [](const TColumnRecord& l, const TColumnRecord& r) {
                return l.GetAddress() < r.GetAddress();
            };
            std::sort(Records.begin(), Records.end(), pred);
            CheckChunksOrder(Records);
        }
        {
            auto pred = [](const TIndexChunk& l, const TIndexChunk& r) {
                return l.GetAddress() < r.GetAddress();
            };
            std::sort(Indexes.begin(), Indexes.end(), pred);
            CheckChunksOrder(Indexes);
        }
    }

    void FullValidation() const {
        AFL_VERIFY(Records.size());
        CheckChunksOrder(Records);
        CheckChunksOrder(Indexes);
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

    void LoadIndex(const TIndexChunkLoadContext& loadContext);

    const TColumnRecord& AppendOneChunkColumn(TColumnRecord&& record);

    void AddIndex(const TIndexChunk& chunk) {
        ui32 chunkIdx = 0;
        for (auto&& i : Indexes) {
            if (i.GetIndexId() == chunk.GetIndexId()) {
                AFL_VERIFY(chunkIdx == i.GetChunkIdx())("index_id", chunk.GetIndexId())("expected", chunkIdx)("real", i.GetChunkIdx());
                ++chunkIdx;
            }
        }
        AFL_VERIFY(chunkIdx == chunk.GetChunkIdx())("index_id", chunk.GetIndexId())("expected", chunkIdx)("real", chunk.GetChunkIdx());
        Indexes.emplace_back(chunk);
    }

    TPortionInfo Build(const bool needChunksNormalization);
};

class TPortionConstructors {
private:
    THashMap<ui64, THashMap<ui64, TPortionInfoConstructor>> Constructors;

public:
    THashMap<ui64, THashMap<ui64, TPortionInfoConstructor>>::iterator begin() {
        return Constructors.begin();
    }

    THashMap<ui64, THashMap<ui64, TPortionInfoConstructor>>::iterator end() {
        return Constructors.end();
    }

    TPortionInfoConstructor* GetConstructorVerified(const ui64 pathId, const ui64 portionId) {
        auto itPathId = Constructors.find(pathId);
        AFL_VERIFY(itPathId != Constructors.end());
        auto itPortionId = itPathId->second.find(portionId);
        AFL_VERIFY(itPortionId != itPathId->second.end());
        return &itPortionId->second;
    }

    TPortionInfoConstructor* AddConstructorVerified(TPortionInfoConstructor&& constructor) {
        const ui64 pathId = constructor.GetPathId();
        const ui64 portionId = constructor.GetPortionIdVerified();
        auto info = Constructors[pathId].emplace(portionId, std::move(constructor));
        AFL_VERIFY(info.second);
        return &info.first->second;
    }

    TPortionInfoConstructor* MergeConstructor(TPortionInfoConstructor&& constructor) {
        const ui64 pathId = constructor.GetPathId();
        const ui64 portionId = constructor.GetPortionIdVerified();
        auto itPathId = Constructors.find(pathId);
        if (itPathId == Constructors.end()) {
            return AddConstructorVerified(std::move(constructor));
        }
        auto itPortionId = itPathId->second.find(portionId);
        if (itPortionId == itPathId->second.end()) {
            return AddConstructorVerified(std::move(constructor));
        }
        itPortionId->second.Merge(std::move(constructor));
        return &itPortionId->second;
    }
};

}   // namespace NKikimr::NOlap
