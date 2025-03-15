#pragma once
#include "column_record.h"
#include "constructor_portion.h"
#include "data_accessor.h"
#include "index_chunk.h"

#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NOlap {

class TPortionAccessorConstructor {
private:
    bool Constructed = false;
    TPortionInfoConstructor PortionInfo;
    std::vector<TIndexChunk> Indexes;
    std::vector<TColumnRecord> Records;

    class TAddressBlobId {
    private:
        TChunkAddress Address;
        YDB_READONLY(TBlobRangeLink16::TLinkId, BlobIdx, 0);

    public:
        const TChunkAddress& GetAddress() const {
            return Address;
        }

        TAddressBlobId(const TChunkAddress& address, const TBlobRangeLink16::TLinkId blobIdx)
            : Address(address)
            , BlobIdx(blobIdx) {
        }
    };
    std::vector<TAddressBlobId> BlobIdxs;
    bool NeedBlobIdxsSort = false;

    TPortionAccessorConstructor(const TPortionAccessorConstructor&) = default;
    TPortionAccessorConstructor& operator=(const TPortionAccessorConstructor&) = default;

    TPortionAccessorConstructor(TPortionDataAccessor&& accessor)
        : PortionInfo(accessor.GetPortionInfo(), true, true) {
        Indexes = accessor.ExtractIndexes();
        Records = accessor.ExtractRecords();
    }

    TPortionAccessorConstructor(
        const TPortionDataAccessor& accessor, const bool withBlobs, const bool withMetadata, const bool withMetadataBlobs)
        : PortionInfo(accessor.GetPortionInfo(), withMetadata, withMetadataBlobs) {
        if (withBlobs) {
            AFL_VERIFY(withMetadataBlobs && withMetadata);
            Indexes = accessor.GetIndexesVerified();
            Records = accessor.GetRecordsVerified();
        }
    }

    void ChunksValidation() const;

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
        if (NeedBlobIdxsSort) {
            auto pred = [](const TAddressBlobId& l, const TAddressBlobId& r) {
                return l.GetAddress() < r.GetAddress();
            };
            std::sort(BlobIdxs.begin(), BlobIdxs.end(), pred);
        }
    }

public:
    TPortionAccessorConstructor(const NColumnShard::TInternalPathId pathId)
        : PortionInfo(pathId)
    {

    }

    TPortionAccessorConstructor(TPortionInfoConstructor&& portionInfo)
        : PortionInfo(std::move(portionInfo))
    {

    }

    TPortionAccessorConstructor MakeCopy() const {
        return TPortionAccessorConstructor(*this);
    }

    static TPortionAccessorConstructor BuildForRewriteBlobs(const TPortionInfo& portion) {
        return TPortionAccessorConstructor(TPortionInfoConstructor(portion, true, false));
    }

    static TPortionDataAccessor BuildForLoading(
        const TPortionInfo::TConstPtr& portion, std::vector<TColumnChunkLoadContextV1>&& records, std::vector<TIndexChunkLoadContext>&& indexes);

    const std::vector<TColumnRecord>& GetRecords() const {
        return Records;
    }

    TPortionInfoConstructor& MutablePortionConstructor() {
        return PortionInfo;
    }

    std::vector<TColumnRecord>& TestMutableRecords() {
        return Records;
    }

    const std::vector<TColumnRecord>& TestGetRecords() const {
        return Records;
    }

    const TPortionInfoConstructor& GetPortionConstructor() const {
        return PortionInfo;
    }

    void RegisterBlobIdx(const TChunkAddress& address, const TBlobRangeLink16::TLinkId blobIdx) {
        if (BlobIdxs.size() && address < BlobIdxs.back().GetAddress()) {
            NeedBlobIdxsSort = true;
        }
        BlobIdxs.emplace_back(address, blobIdx);
    }

    TString DebugString() const {
        TStringBuilder sb;
        sb << PortionInfo.DebugString() << ";";
        for (auto&& i : Records) {
            sb << i.DebugString() << ";";
        }
        return sb;
    }

    const TBlobRange RestoreBlobRangeSlow(const TBlobRangeLink16& linkRange, const TChunkAddress& address) const {
        for (auto&& i : BlobIdxs) {
            if (i.GetAddress() == address) {
                return linkRange.RestoreRange(GetBlobId(i.GetBlobIdx()));
            }
        }
        AFL_VERIFY(false);
        return TBlobRange();
    }

    TPortionDataAccessor Build(const bool needChunksNormalization);

    TBlobRangeLink16::TLinkId RegisterBlobId(const TUnifiedBlobId& blobId) {
        return PortionInfo.MetaConstructor.RegisterBlobId(blobId);
    }

    const TBlobRange RestoreBlobRange(const TBlobRangeLink16& linkRange) const {
        return PortionInfo.MetaConstructor.RestoreBlobRange(linkRange);
    }

    const TUnifiedBlobId& GetBlobId(const TBlobRangeLink16::TLinkId linkId) const {
        return PortionInfo.MetaConstructor.GetBlobId(linkId);
    }

    ui32 GetBlobIdsCount() const {
        return PortionInfo.MetaConstructor.GetBlobIdsCount();
    }

    TPortionAccessorConstructor(TPortionAccessorConstructor&&) noexcept = default;
    TPortionAccessorConstructor& operator=(TPortionAccessorConstructor&&) noexcept = default;

    void LoadRecord(TColumnChunkLoadContextV1&& loadContext);
    void LoadIndex(TIndexChunkLoadContext&& loadContext);

    const TColumnRecord& AppendOneChunkColumn(TColumnRecord&& record) {
        Y_ABORT_UNLESS(record.ColumnId);
        Records.emplace_back(std::move(record));
        return Records.back();
    }

    ui32 CalcRecordsCount() const {
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

    bool HaveBlobsData() {
        return PortionInfo.HaveBlobsData() || Records.size() || Indexes.size();
    }

    void ClearRecords() {
        Records.clear();
    }

    void ClearIndexes() {
        Indexes.clear();
    }

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
};

}   // namespace NKikimr::NOlap
