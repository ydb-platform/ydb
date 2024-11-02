#pragma once
#include "column_record.h"
#include "constructor_meta.h"
#include "data_accessor.h"
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
    bool Constructed = false;
    YDB_ACCESSOR(ui64, PathId, 0);
    std::optional<ui64> PortionId;

    TPortionMetaConstructor MetaConstructor;

    std::optional<TSnapshot> MinSnapshotDeprecated;
    std::optional<TSnapshot> RemoveSnapshot;
    std::optional<ui64> SchemaVersion;
    std::optional<ui64> ShardingVersion;

    std::optional<TSnapshot> CommitSnapshot;
    std::optional<TInsertWriteId> InsertWriteId;

    std::vector<TIndexChunk> Indexes;
    YDB_ACCESSOR_DEF(std::vector<TColumnRecord>, Records);

    TPortionInfoConstructor(const TPortionInfoConstructor&) = default;
    TPortionInfoConstructor& operator=(const TPortionInfoConstructor&) = default;

public:
    TPortionInfoConstructor(TPortionInfoConstructor&&) noexcept = default;
    TPortionInfoConstructor& operator=(TPortionInfoConstructor&&) noexcept = default;

    class TTestCopier {
    public:
        static TPortionInfoConstructor Copy(const TPortionInfoConstructor& source) {
            return source;
        }
    };

    void SetPortionId(const ui64 value) {
        AFL_VERIFY(value);
        PortionId = value;
    }

    void AddMetadata(const ISnapshotSchema& snapshotSchema, const std::shared_ptr<arrow::RecordBatch>& batch);

    void AddMetadata(const ISnapshotSchema& snapshotSchema, const ui32 deletionsCount, const NArrow::TFirstLastSpecialKeys& firstLastRecords,
        const std::optional<NArrow::TMinMaxSpecialKeys>& minMaxSpecial) {
        MetaConstructor.FillMetaInfo(firstLastRecords, deletionsCount, minMaxSpecial, snapshotSchema.GetIndexInfo());
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

    TInsertWriteId GetInsertWriteIdVerified() const {
        AFL_VERIFY(InsertWriteId);
        return *InsertWriteId;
    }

    TPortionInfoConstructor(const TPortionInfo& portion, const bool withBlobs, const bool withMetadata, const bool withMetadataBlobs)
        : PathId(portion.GetPathId())
        , PortionId(portion.GetPortionId())
        , MinSnapshotDeprecated(portion.GetMinSnapshotDeprecated())
        , RemoveSnapshot(portion.GetRemoveSnapshotOptional())
        , SchemaVersion(portion.GetSchemaVersionOptional())
        , ShardingVersion(portion.GetShardingVersionOptional())
        , CommitSnapshot(portion.GetCommitSnapshotOptional())
        , InsertWriteId(portion.GetInsertWriteIdOptional()) {
        if (withMetadata) {
            MetaConstructor = TPortionMetaConstructor(portion.Meta, withMetadataBlobs);
        }
        if (withBlobs) {
            AFL_VERIFY(withMetadata);
            Indexes = portion.Indexes;
            Records = portion.Records;
        }
    }

    TPortionInfoConstructor(TPortionInfo&& portion)
        : PathId(portion.GetPathId())
        , PortionId(portion.GetPortionId())
        , MinSnapshotDeprecated(portion.GetMinSnapshotDeprecated())
        , RemoveSnapshot(portion.GetRemoveSnapshotOptional())
        , SchemaVersion(portion.GetSchemaVersionOptional())
        , ShardingVersion(portion.GetShardingVersionOptional()) {
        MetaConstructor = TPortionMetaConstructor(portion.Meta, true);
        Indexes = std::move(portion.Indexes);
        Records = std::move(portion.Records);
    }

    TPortionAddress GetAddress() const {
        return TPortionAddress(PathId, GetPortionIdVerified());
    }

    bool HasRemoveSnapshot() const {
        return !!RemoveSnapshot;
    }

    static void Validate(const TColumnRecord& rec) {
        AFL_VERIFY(rec.GetColumnId());
    }

    static ui32 GetRecordsCount(const TColumnRecord& rec) {
        return rec.GetMeta().GetRecordsCount();
    }

    static void Validate(const TIndexChunk& rec) {
        AFL_VERIFY(rec.GetIndexId());
        if (const auto* blobData = rec.GetBlobDataOptional()) {
            AFL_VERIFY(blobData->size());
        }
    }

    static ui32 GetRecordsCount(const TIndexChunk& rec) {
        return rec.GetRecordsCount();
    }

    template <class TChunkInfo>
    static void CheckChunksOrder(const std::vector<TChunkInfo>& chunks) {
        ui32 entityId = 0;
        ui32 chunkIdx = 0;

        const auto debugString = [&]() {
            TStringBuilder sb;
            for (auto&& i : chunks) {
                sb << i.GetAddress().DebugString() << ";";
            }
            return sb;
        };

        std::optional<ui32> recordsCount;
        ui32 recordsCountCurrent = 0;
        for (auto&& i : chunks) {
            Validate(i);
            if (entityId != i.GetEntityId()) {
                if (entityId) {
                    if (recordsCount) {
                        AFL_VERIFY(recordsCountCurrent == *recordsCount);
                    } else {
                        recordsCount = recordsCountCurrent;
                    }
                }
                AFL_VERIFY(entityId < i.GetEntityId())("entity", entityId)("next", i.GetEntityId())("details", debugString());
                AFL_VERIFY(i.GetChunkIdx() == 0);
                entityId = i.GetEntityId();
                chunkIdx = 0;
                recordsCountCurrent = 0;
            } else {
                AFL_VERIFY(i.GetChunkIdx() == chunkIdx + 1)("chunkIdx", chunkIdx)("i.GetChunkIdx()", i.GetChunkIdx())("entity", entityId)(
                                                  "details", debugString());
                chunkIdx = i.GetChunkIdx();
            }
            recordsCountCurrent += GetRecordsCount(i);
            AFL_VERIFY(i.GetEntityId());
        }
        if (recordsCount) {
            AFL_VERIFY(recordsCountCurrent == *recordsCount);
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

    void SetCommitSnapshot(const TSnapshot& snap) {
        AFL_VERIFY(!!InsertWriteId);
        AFL_VERIFY(!CommitSnapshot);
        AFL_VERIFY(snap.Valid());
        CommitSnapshot = snap;
    }

    void SetInsertWriteId(const TInsertWriteId value) {
        AFL_VERIFY(!InsertWriteId);
        AFL_VERIFY((ui64)value);
        InsertWriteId = value;
    }

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

    void LoadRecord(const TColumnChunkLoadContextV1& loadContext);

    ui32 GetRecordsCount() const {
        AFL_VERIFY(Records.size());
        ui32 result = 0;
        std::optional<ui32> columnIdFirst;
        for (auto&& i : Records) {
            if (!columnIdFirst || *columnIdFirst == i.ColumnId) {
                result += i.GetMeta().GetRecordsCount();
                columnIdFirst = i.ColumnId;
            }
        }
        AFL_VERIFY(columnIdFirst);
        return result;
    }

    TBlobRangeLink16::TLinkId RegisterBlobId(const TUnifiedBlobId& blobId) {
        return MetaConstructor.RegisterBlobId(blobId);
    }

    const TBlobRange RestoreBlobRange(const TBlobRangeLink16& linkRange) const {
        return MetaConstructor.RestoreBlobRange(linkRange);
    }

    const TBlobRange RestoreBlobRangeSlow(const TBlobRangeLink16& linkRange, const TChunkAddress& address) const {
        return MetaConstructor.RestoreBlobRangeSlow(linkRange, address);
    }

    const TUnifiedBlobId& GetBlobId(const TBlobRangeLink16::TLinkId linkId) const {
        return MetaConstructor.GetBlobId(linkId);
    }

    ui32 GetBlobIdsCount() const {
        return MetaConstructor.GetBlobIdsCount();
    }

    void RegisterBlobIdx(const TChunkAddress& address, const TBlobRangeLink16::TLinkId blobIdx) {
        return MetaConstructor.RegisterBlobIdx(address, blobIdx);
    }

    TString DebugString() const {
        TStringBuilder sb;
        sb << (PortionId ? *PortionId : 0) << ";";
        for (auto&& i : Records) {
            sb << i.DebugString() << ";";
        }
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
        MetaConstructor.ReorderBlobs();
    }

    void FullValidation() const {
        AFL_VERIFY(Records.size());
        CheckChunksOrder(Records);
        CheckChunksOrder(Indexes);
        if (MetaConstructor.BlobIdxs.size()) {
            AFL_VERIFY(MetaConstructor.BlobIdxs.size() <= Records.size() + Indexes.size())("blobs", MetaConstructor.BlobIdxs.size())(
                                                                               "records", Records.size())(
                                                               "indexes", Indexes.size());
        } else {
            std::set<ui32> blobIdxs;
            for (auto&& i : Records) {
                blobIdxs.emplace(i.GetBlobRange().GetBlobIdxVerified());
            }
            for (auto&& i : Indexes) {
                if (i.HasBlobRange()) {
                    blobIdxs.emplace(i.GetBlobRangeVerified().GetBlobIdxVerified());
                }
            }
            if (MetaConstructor.BlobIds.size()) {
                AFL_VERIFY(MetaConstructor.BlobIds.size() == blobIdxs.size());
                AFL_VERIFY(MetaConstructor.BlobIds.size() == *blobIdxs.rbegin() + 1);
            } else {
                AFL_VERIFY(blobIdxs.empty());
            }
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

    TPortionDataAccessor Build(const bool needChunksNormalization);
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
};

}   // namespace NKikimr::NOlap
