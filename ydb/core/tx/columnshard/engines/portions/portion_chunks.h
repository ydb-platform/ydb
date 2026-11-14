#pragma once
#include "column_record.h"
#include "index_chunk.h"
#include "meta.h"

#include <ydb/library/accessor/accessor.h>

#include <memory>
#include <optional>
#include <set>
#include <vector>

namespace NKikimr::NOlap {

// Immutable chunk metadata of one portion. The portion itself (snapshots, schema, tier) is not stored here,
// so the same chunks can be reused after a tablet restart and paired with the portion loaded from the tablet.
class TPortionChunks: public TPortionMetaBase {
private:
    using TBase = TPortionMetaBase;
    TPortionAddress Address;
    std::vector<TColumnRecord> Records;
    std::vector<TIndexChunk> Indexes;
    std::optional<std::vector<ui32>> SliceBorderOffsets;

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
                AFL_VERIFY(i.GetChunkIdx() == chunkIdx + 1)("chunk", i.GetChunkIdx())("idx", chunkIdx);
                chunkIdx = i.GetChunkIdx();
            }
        }
    }

public:
    class TExtractContext {
    private:
        YDB_ACCESSOR_DEF(std::optional<std::set<ui32>>, ColumnIds);
        YDB_ACCESSOR_DEF(std::optional<std::set<ui32>>, IndexIds);

    public:
        TExtractContext() = default;
    };

    class TPage {
    private:
        YDB_READONLY_DEF(std::vector<const TColumnRecord*>, Records);
        YDB_READONLY_DEF(std::vector<const TIndexChunk*>, Indexes);
        YDB_READONLY(ui32, RecordsCount, 0);

    public:
        TPage(std::vector<const TColumnRecord*>&& records, std::vector<const TIndexChunk*>&& indexes, const ui32 recordsCount)
            : Records(std::move(records))
            , Indexes(std::move(indexes))
            , RecordsCount(recordsCount)
        {
        }
    };

    TPortionChunks(const TPortionAddress& address, std::vector<TUnifiedBlobId>&& blobIds, std::vector<TColumnRecord>&& records,
        std::vector<TIndexChunk>&& indexes, std::optional<std::vector<ui32>>&& sliceOffsets = std::nullopt)
        : TBase(std::move(blobIds))
        , Address(address)
        , Records(std::move(records))
        , Indexes(std::move(indexes))
        , SliceBorderOffsets(std::move(sliceOffsets))
    {
    }

    TPortionChunks(const TPortionAddress& address, const std::vector<TUnifiedBlobId>& blobIds, const std::vector<TColumnRecord>& records,
        const std::vector<TIndexChunk>& indexes)
        : TBase(blobIds)
        , Address(address)
        , Records(records)
        , Indexes(indexes)
    {
    }

    const TPortionAddress& GetAddress() const {
        return Address;
    }

    // The portion address can change before the chunks are published (data sharing rewrites path and portion id).
    void ResetAddress(const TPortionAddress& address) {
        Address = address;
    }

    ui64 GetMetadataSize() const {
        return Records.size() * sizeof(TColumnRecord) + Indexes.size() * sizeof(TIndexChunk) + TBase::GetMetadataMemorySize();
    }

    static std::vector<ui32> CalcSliceBorderOffsets(const std::vector<TColumnRecord>& records, const std::vector<TIndexChunk>& indexes) {
        return DoCalcSliceBorderOffsets(records, indexes);
    }

    template <class TAggregator, class TChunkInfo>
    static void AggregateIndexChunksData(
        const TAggregator& aggr, const std::vector<TChunkInfo>& chunks, const std::set<ui32>* columnIds, const bool validation) {
        if (columnIds) {
            auto itColumn = columnIds->begin();
            auto itRecord = chunks.begin();
            ui32 recordsInEntityCount = 0;
            while (itRecord != chunks.end() && itColumn != columnIds->end()) {
                if (itRecord->GetEntityId() < *itColumn) {
                    ++itRecord;
                } else if (*itColumn < itRecord->GetEntityId()) {
                    AFL_VERIFY(!validation || recordsInEntityCount)("problem", "validation")("reason", "no_chunks_for_column")(
                        "column_id", *itColumn);
                    ++itColumn;
                    recordsInEntityCount = 0;
                } else {
                    ++recordsInEntityCount;
                    aggr(*itRecord);
                    ++itRecord;
                }
            }
        } else {
            for (auto&& i : chunks) {
                aggr(i);
            }
        }
    }

    std::shared_ptr<TPortionChunks> Extract(const TExtractContext& context) const;

    std::vector<TColumnRecord> ExtractRecords() {
        return std::move(Records);
    }

    std::vector<TIndexChunk> ExtractIndexes() {
        return std::move(Indexes);
    }

    std::vector<TString> GetIndexInplaceDataOptional(const ui32 indexId) const {
        std::vector<TString> result;
        for (auto&& i : Indexes) {
            if (i.GetEntityId() == indexId) {
                result.emplace_back(i.GetBlobDataVerified());
            }
        }
        return result;
    }

    std::set<ui32> GetColumnIds() const {
        std::set<ui32> result;
        for (auto&& i : Records) {
            result.emplace(i.GetColumnId());
        }
        return result;
    }

    const std::vector<TColumnRecord>& GetRecordsVerified() const {
        return Records;
    }

    const std::vector<TIndexChunk>& GetIndexesVerified() const {
        return Indexes;
    }

    bool HasEntityAddress(const TChunkAddress& address) const;
    const TColumnRecord* GetRecordPointer(const TChunkAddress& address) const;
    TString DebugString() const;

    ui64 GetColumnRawBytes(const std::set<ui32>& entityIds, const bool validation = true) const;
    ui64 GetColumnBlobBytes(const std::set<ui32>& entityIds, const bool validation = true) const;
    ui64 GetIndexRawBytes(const std::set<ui32>& entityIds, const bool validation = true) const;
    ui64 GetIndexBlobBytes(const std::set<ui32>& entityIds, const bool validation = true) const;
    ui64 GetIndexBlobBytes(const bool validation = true) const;
    ui64 GetIndexRawBytes(const bool validation = true) const;

    std::vector<const TColumnRecord*> GetColumnChunksPointers(const ui32 columnId) const;
    std::vector<const TIndexChunk*> GetIndexChunksPointers(const ui32 indexId) const;

    void CheckOrder() const {
        CheckChunksOrder(Records);
        CheckChunksOrder(Indexes);
    }

    std::vector<TPage> BuildPages() const;
    ui64 GetMinMemoryForReadColumns(const std::optional<std::set<ui32>>& columnIds) const;

    ui32 GetSliceOffsetRows(const ui32 sliceBorderIdx) const {
        AFL_VERIFY(SliceBorderOffsets);
        AFL_VERIFY(sliceBorderIdx < SliceBorderOffsets->size())("idx", sliceBorderIdx)("size", SliceBorderOffsets->size());
        return (*SliceBorderOffsets)[sliceBorderIdx];
    }
};

}   // namespace NKikimr::NOlap
