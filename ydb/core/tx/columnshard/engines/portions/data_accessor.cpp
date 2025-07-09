#include "compacted.h"
#include "constructor_meta.h"
#include "data_accessor.h"

#include <ydb/core/formats/arrow/accessor/abstract/accessor.h>
#include <ydb/core/formats/arrow/accessor/composite/accessor.h>
#include <ydb/core/formats/arrow/accessor/plain/accessor.h>
#include <ydb/core/formats/arrow/accessor/sparsed/accessor.h>
#include <ydb/core/formats/arrow/common/container.h>
#include <ydb/core/tx/columnshard/blobs_reader/task.h>
#include <ydb/core/tx/columnshard/data_sharing/protos/data.pb.h>
#include <ydb/core/tx/columnshard/engines/db_wrapper.h>
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>
#include <ydb/core/tx/columnshard/engines/storage/chunks/column.h>
#include <ydb/core/tx/columnshard/engines/storage/chunks/data.h>

#include <ydb/library/formats/arrow/simple_arrays_cache.h>

namespace NKikimr::NOlap {

namespace {

template <class TExternalBlobInfo>
TPortionDataAccessor::TPreparedBatchData PrepareForAssembleImpl(const TPortionDataAccessor& portionData, const TPortionInfo& portionInfo,
    const ISnapshotSchema& dataSchema, const ISnapshotSchema& resultSchema, THashMap<TChunkAddress, TExternalBlobInfo>& blobsData,
    const std::optional<TSnapshot>& defaultSnapshot, const bool restoreAbsent) {
    std::vector<TPortionDataAccessor::TColumnAssemblingInfo> columns;
    columns.reserve(resultSchema.GetColumnIds().size());
    const ui32 rowsCount = portionInfo.GetRecordsCount();
    auto it = portionData.GetRecordsVerified().begin();

    for (auto&& i : resultSchema.GetColumnIds()) {
        while (it != portionData.GetRecordsVerified().end() && it->GetColumnId() < i) {
            ++it;
            continue;
        }
        if ((it == portionData.GetRecordsVerified().end() || i < it->GetColumnId())) {
            if (restoreAbsent || IIndexInfo::IsSpecialColumn(i)) {
                columns.emplace_back(rowsCount, dataSchema.GetColumnLoaderOptional(i), resultSchema.GetColumnLoaderVerified(i));
                portionInfo.FillDefaultColumn(columns.back(), defaultSnapshot);
            }
        }
        if (it == portionData.GetRecordsVerified().end()) {
            continue;
        } else if (it->GetColumnId() != i) {
            AFL_VERIFY(i < it->GetColumnId());
            continue;
        }
        columns.emplace_back(rowsCount, dataSchema.GetColumnLoaderOptional(i), resultSchema.GetColumnLoaderVerified(i));
        while (it != portionData.GetRecordsVerified().end() && it->GetColumnId() == i) {
            auto itBlobs = blobsData.find(it->GetAddress());
            AFL_VERIFY(itBlobs != blobsData.end())("size", blobsData.size())("address", it->GetAddress().DebugString());
            columns.back().AddBlobInfo(it->Chunk, it->GetMeta().GetRecordsCount(), std::move(itBlobs->second));
            blobsData.erase(itBlobs);

            ++it;
            continue;
        }
    }

    // Make chunked arrays for columns
    std::vector<TPortionDataAccessor::TPreparedColumn> preparedColumns;
    preparedColumns.reserve(columns.size());
    for (auto& c : columns) {
        preparedColumns.emplace_back(c.Compile());
    }

    return TPortionDataAccessor::TPreparedBatchData(std::move(preparedColumns), rowsCount);
}

}   // namespace

TPortionDataAccessor::TPreparedBatchData TPortionDataAccessor::PrepareForAssemble(const ISnapshotSchema& dataSchema,
    const ISnapshotSchema& resultSchema, THashMap<TChunkAddress, TString>& blobsData, const std::optional<TSnapshot>& defaultSnapshot,
    const bool restoreAbsent) const {
    return PrepareForAssembleImpl(*this, *PortionInfo, dataSchema, resultSchema, blobsData, defaultSnapshot, restoreAbsent);
}

TPortionDataAccessor::TPreparedBatchData TPortionDataAccessor::PrepareForAssemble(const ISnapshotSchema& dataSchema,
    const ISnapshotSchema& resultSchema, THashMap<TChunkAddress, TAssembleBlobInfo>& blobsData, const std::optional<TSnapshot>& defaultSnapshot,
    const bool restoreAbsent) const {
    return PrepareForAssembleImpl(*this, *PortionInfo, dataSchema, resultSchema, blobsData, defaultSnapshot, restoreAbsent);
}

void TPortionDataAccessor::FillBlobRangesByStorage(
    THashMap<ui32, THashMap<TString, THashSet<TBlobRange>>>& result, const TVersionedIndex& index, const THashSet<ui32>& entityIds) const {
    auto schema = PortionInfo->GetSchema(index);
    return FillBlobRangesByStorage(result, schema->GetIndexInfo(), entityIds);
}

void TPortionDataAccessor::FillBlobRangesByStorage(
    THashMap<ui32, THashMap<TString, THashSet<TBlobRange>>>& result, const TIndexInfo& indexInfo, const THashSet<ui32>& entityIds) const {
    for (auto&& i : GetRecordsVerified()) {
        if (!entityIds.contains(i.GetEntityId())) {
            continue;
        }
        const TString& storageId = PortionInfo->GetColumnStorageId(i.GetColumnId(), indexInfo);
        AFL_VERIFY(result[i.GetEntityId()][storageId].emplace(RestoreBlobRange(i.GetBlobRange())).second)(
            "blob_id", RestoreBlobRange(i.GetBlobRange()).ToString());
    }
    for (auto&& i : GetIndexesVerified()) {
        if (!entityIds.contains(i.GetEntityId())) {
            continue;
        }
        const TString& storageId = PortionInfo->GetIndexStorageId(i.GetIndexId(), indexInfo);
        auto bRange = i.GetBlobRangeVerified();
        AFL_VERIFY(result[i.GetEntityId()][storageId].emplace(RestoreBlobRange(bRange)).second)("blob_id", RestoreBlobRange(bRange).ToString());
    }
}

void TPortionDataAccessor::FillBlobRangesByStorage(THashMap<TString, THashSet<TBlobRange>>& result, const TVersionedIndex& index) const {
    auto schema = PortionInfo->GetSchema(index);
    return FillBlobRangesByStorage(result, schema->GetIndexInfo());
}

void TPortionDataAccessor::FillBlobRangesByStorage(
    THashMap<TString, THashSet<TBlobRange>>& result, const TIndexInfo& indexInfo, const std::set<ui32>* entityIds) const {
    for (auto&& i : GetRecordsVerified()) {
        if (entityIds && !entityIds->contains(i.GetColumnId())) {
            continue;
        }
        const TString& storageId = PortionInfo->GetColumnStorageId(i.GetColumnId(), indexInfo);
        AFL_VERIFY(result[storageId].emplace(RestoreBlobRange(i.GetBlobRange())).second)(
            "blob_id", RestoreBlobRange(i.GetBlobRange()).ToString());
    }
    for (auto&& i : GetIndexesVerified()) {
        if (entityIds && !entityIds->contains(i.GetIndexId())) {
            continue;
        }
        const TString& storageId = PortionInfo->GetIndexStorageId(i.GetIndexId(), indexInfo);
        if (auto bRange = i.GetBlobRangeOptional()) {
            AFL_VERIFY(result[storageId].emplace(RestoreBlobRange(*bRange)).second)("blob_id", RestoreBlobRange(*bRange).ToString());
        }
    }
}

void TPortionDataAccessor::FillBlobIdsByStorage(THashMap<TString, THashSet<TUnifiedBlobId>>& result, const TIndexInfo& indexInfo) const {
    THashMap<TString, THashSet<TBlobRangeLink16::TLinkId>> local;
    THashSet<TBlobRangeLink16::TLinkId>* currentHashLocal = nullptr;
    THashSet<TUnifiedBlobId>* currentHashResult = nullptr;
    std::optional<ui32> lastEntityId;
    TString lastStorageId;
    ui32 lastBlobIdx = GetBlobIdsCount();
    for (auto&& i : GetRecordsVerified()) {
        if (!lastEntityId || *lastEntityId != i.GetEntityId()) {
            const TString& storageId = PortionInfo->GetColumnStorageId(i.GetEntityId(), indexInfo);
            lastEntityId = i.GetEntityId();
            if (storageId != lastStorageId) {
                currentHashResult = &result[storageId];
                currentHashLocal = &local[storageId];
                lastStorageId = storageId;
                lastBlobIdx = GetBlobIdsCount();
            }
        }
        if (lastBlobIdx != i.GetBlobRange().GetBlobIdxVerified() && currentHashLocal->emplace(i.GetBlobRange().GetBlobIdxVerified()).second) {
            auto blobId = GetBlobId(i.GetBlobRange().GetBlobIdxVerified());
            AFL_VERIFY(currentHashResult);
            AFL_VERIFY(currentHashResult->emplace(blobId).second)("blob_id", blobId.ToStringNew());
            lastBlobIdx = i.GetBlobRange().GetBlobIdxVerified();
        }
    }
    for (auto&& i : GetIndexesVerified()) {
        if (!lastEntityId || *lastEntityId != i.GetEntityId()) {
            const TString& storageId = PortionInfo->GetIndexStorageId(i.GetEntityId(), indexInfo);
            lastEntityId = i.GetEntityId();
            if (storageId != lastStorageId) {
                currentHashResult = &result[storageId];
                currentHashLocal = &local[storageId];
                lastStorageId = storageId;
                lastBlobIdx = GetBlobIdsCount();
            }
        }
        if (auto bRange = i.GetBlobRangeOptional()) {
            if (lastBlobIdx != bRange->GetBlobIdxVerified() && currentHashLocal->emplace(bRange->GetBlobIdxVerified()).second) {
                auto blobId = GetBlobId(bRange->GetBlobIdxVerified());
                AFL_VERIFY(currentHashResult);
                AFL_VERIFY(currentHashResult->emplace(blobId).second)("blob_id", blobId.ToStringNew());
                lastBlobIdx = bRange->GetBlobIdxVerified();
            }
        }
    }
}

void TPortionDataAccessor::FillBlobIdsByStorage(THashMap<TString, THashSet<TUnifiedBlobId>>& result, const TVersionedIndex& index) const {
    auto schema = PortionInfo->GetSchema(index);
    return FillBlobIdsByStorage(result, schema->GetIndexInfo());
}

THashMap<TString, THashMap<TChunkAddress, std::shared_ptr<IPortionDataChunk>>> TPortionDataAccessor::RestoreEntityChunks(
    NBlobOperations::NRead::TCompositeReadBlobs& blobs, const TIndexInfo& indexInfo) const {
    THashMap<TString, THashMap<TChunkAddress, std::shared_ptr<IPortionDataChunk>>> result;
    for (auto&& c : GetRecordsVerified()) {
        const TString& storageId = PortionInfo->GetColumnStorageId(c.GetColumnId(), indexInfo);
        auto chunk = std::make_shared<NChunks::TChunkPreparation>(
            blobs.ExtractVerified(storageId, RestoreBlobRange(c.GetBlobRange())), c, indexInfo.GetColumnFeaturesVerified(c.GetColumnId()));
        chunk->SetChunkIdx(c.GetChunkIdx());
        AFL_VERIFY(result[storageId].emplace(c.GetAddress(), chunk).second);
    }
    for (auto&& c : GetIndexesVerified()) {
        const TString& storageId = indexInfo.GetIndexStorageId(c.GetIndexId());
        const TString blobData = [&]() -> TString {
            if (auto bRange = c.GetBlobRangeOptional()) {
                return blobs.ExtractVerified(storageId, RestoreBlobRange(*bRange));
            } else if (auto data = c.GetBlobDataOptional()) {
                return *data;
            } else {
                AFL_VERIFY(false);
                Y_UNREACHABLE();
            }
        }();
        auto chunk = std::make_shared<NChunks::TPortionIndexChunk>(c.GetAddress(), c.GetRecordsCount(), c.GetRawBytes(), blobData);
        chunk->SetChunkIdx(c.GetChunkIdx());

        AFL_VERIFY(result[storageId].emplace(c.GetAddress(), chunk).second);
    }
    return result;
}

THashMap<TChunkAddress, TString> TPortionDataAccessor::DecodeBlobAddressesImpl(
    NBlobOperations::NRead::TCompositeReadBlobs& blobs, const TIndexInfo& indexInfo) const {
    THashMap<TChunkAddress, TString> result;

    {
        TString storageId;
        ui64 columnId = 0;
        for (auto&& record : GetRecordsVerified()) {
            if (record.GetColumnId() != columnId) {
                AFL_VERIFY(record.GetColumnId() > columnId);
                columnId = record.GetColumnId();
                storageId = PortionInfo->GetColumnStorageId(columnId, indexInfo);
            }
            std::optional<TString> blob = blobs.ExtractOptional(storageId, RestoreBlobRange(record.GetBlobRange()));
            if (blob) {
                result.emplace(record.GetAddress(), std::move(*blob));
            }
        }
    }

    for (auto&& record : GetIndexesVerified()) {
        if (!record.HasBlobRange()) {
            continue;
        }
        std::optional<TString> blob =
            blobs.ExtractOptional(indexInfo.GetIndexStorageId(record.GetIndexId()), RestoreBlobRange(record.GetBlobRangeVerified()));
        if (blob) {
            result.emplace(record.GetAddress(), std::move(*blob));
        }
    }

    return result;
}

THashMap<TChunkAddress, TString> TPortionDataAccessor::DecodeBlobAddresses(
    NBlobOperations::NRead::TCompositeReadBlobs&& blobs, const TIndexInfo& indexInfo) const {
    THashMap<TChunkAddress, TString> result = DecodeBlobAddressesImpl(blobs, indexInfo);
    AFL_VERIFY(blobs.IsEmpty())("blobs", blobs.DebugString());
    return result;
}

std::vector<THashMap<TChunkAddress, TString>> TPortionDataAccessor::DecodeBlobAddresses(const std::vector<TPortionDataAccessor>& accessors,
    const std::vector<ISnapshotSchema::TPtr>& schemas, NBlobOperations::NRead::TCompositeReadBlobs&& blobs) {
    std::vector<THashMap<TChunkAddress, TString>> result;
    AFL_VERIFY(accessors.size() == schemas.size())("accessors", accessors.size())("info", schemas.size());
    for (ui64 i = 0; i < accessors.size(); ++i) {
        result.emplace_back(accessors[i].DecodeBlobAddressesImpl(blobs, schemas[i]->GetIndexInfo()));
    }
    AFL_VERIFY(blobs.IsEmpty())("blobs", blobs.DebugString());
    return result;
}

bool TPortionDataAccessor::HasEntityAddress(const TChunkAddress& address) const {
    {
        auto it = std::lower_bound(
            GetRecordsVerified().begin(), GetRecordsVerified().end(), address, [](const TColumnRecord& item, const TChunkAddress& address) {
                return item.GetAddress() < address;
            });
        if (it != GetRecordsVerified().end() && it->GetAddress() == address) {
            return true;
        }
    }
    {
        auto it = std::lower_bound(
            GetIndexesVerified().begin(), GetIndexesVerified().end(), address, [](const TIndexChunk& item, const TChunkAddress& address) {
                return item.GetAddress() < address;
            });
        if (it != GetIndexesVerified().end() && it->GetAddress() == address) {
            return true;
        }
    }
    return false;
}

const NKikimr::NOlap::TColumnRecord* TPortionDataAccessor::GetRecordPointer(const TChunkAddress& address) const {
    auto it = std::lower_bound(
        GetRecordsVerified().begin(), GetRecordsVerified().end(), address, [](const TColumnRecord& item, const TChunkAddress& address) {
            return item.GetAddress() < address;
        });
    if (it != GetRecordsVerified().end() && it->GetAddress() == address) {
        return &*it;
    }
    return nullptr;
}

TString TPortionDataAccessor::DebugString() const {
    TStringBuilder sb;
    sb << "chunks:(" << GetRecordsVerified().size() << ");";
    if (IS_TRACE_LOG_ENABLED(NKikimrServices::TX_COLUMNSHARD)) {
        std::vector<TBlobRange> blobRanges;
        for (auto&& i : GetRecordsVerified()) {
            blobRanges.emplace_back(RestoreBlobRange(i.BlobRange));
        }
        sb << "blobs:" << JoinSeq(",", blobRanges) << ";ranges_count:" << blobRanges.size() << ";";
    }
    return sb << ")";
}

ui64 TPortionDataAccessor::GetColumnRawBytes(const std::set<ui32>& entityIds, const bool validation /*= true*/) const {
    ui64 sum = 0;
    const auto aggr = [&](const TColumnRecord& r) {
        sum += r.GetMeta().GetRawBytes();
    };
    AggregateIndexChunksData(aggr, GetRecordsVerified(), &entityIds, validation);
    return sum;
}

ui64 TPortionDataAccessor::GetColumnBlobBytes(const std::set<ui32>& entityIds, const bool validation /*= true*/) const {
    ui64 sum = 0;
    const auto aggr = [&](const TColumnRecord& r) {
        sum += r.GetBlobRange().GetSize();
    };
    AggregateIndexChunksData(aggr, GetRecordsVerified(), &entityIds, validation);
    return sum;
}

ui64 TPortionDataAccessor::GetIndexRawBytes(const std::set<ui32>& entityIds, const bool validation /*= true*/) const {
    ui64 sum = 0;
    const auto aggr = [&](const TIndexChunk& r) {
        sum += r.GetRawBytes();
    };
    AggregateIndexChunksData(aggr, GetIndexesVerified(), &entityIds, validation);
    return sum;
}

ui64 TPortionDataAccessor::GetIndexRawBytes(const bool validation /*= true*/) const {
    ui64 sum = 0;
    const auto aggr = [&](const TIndexChunk& r) {
        sum += r.GetRawBytes();
    };
    AggregateIndexChunksData(aggr, GetIndexesVerified(), nullptr, validation);
    return sum;
}

std::vector<const TIndexChunk*> TPortionDataAccessor::GetIndexChunksPointers(const ui32 indexId) const {
    std::vector<const TIndexChunk*> result;
    for (auto&& c : GetIndexesVerified()) {
        if (c.GetIndexId() == indexId) {
            AFL_VERIFY(c.GetChunkIdx() == result.size());
            result.emplace_back(&c);
        }
    }
    return result;
}

std::vector<const TColumnRecord*> TPortionDataAccessor::GetColumnChunksPointers(const ui32 columnId) const {
    std::vector<const TColumnRecord*> result;
    for (auto&& c : GetRecordsVerified()) {
        if (c.ColumnId == columnId) {
            Y_ABORT_UNLESS(c.Chunk == result.size());
            Y_ABORT_UNLESS(c.GetMeta().GetRecordsCount());
            result.emplace_back(&c);
        }
    }
    return result;
}

std::vector<TPortionDataAccessor::TReadPage> TPortionDataAccessor::BuildReadPages(
    const ui64 memoryLimit, const std::set<ui32>& entityIds) const {
    class TEntityDelimiter {
    private:
        YDB_READONLY(ui32, IndexStart, 0);
        YDB_READONLY(ui32, EntityId, 0);
        YDB_READONLY(ui32, ChunkIdx, 0);
        YDB_READONLY(ui64, MemoryStartChunk, 0);
        YDB_READONLY(ui64, MemoryFinishChunk, 0);

    public:
        TEntityDelimiter(const ui32 indexStart, const ui32 entityId, const ui32 chunkIdx, const ui64 memStartChunk, const ui64 memFinishChunk)
            : IndexStart(indexStart)
            , EntityId(entityId)
            , ChunkIdx(chunkIdx)
            , MemoryStartChunk(memStartChunk)
            , MemoryFinishChunk(memFinishChunk) {
        }

        bool operator<(const TEntityDelimiter& item) const {
            return std::tie(IndexStart, EntityId, ChunkIdx) < std::tie(item.IndexStart, item.EntityId, item.ChunkIdx);
        }
    };

    class TGlobalDelimiter {
    private:
        YDB_READONLY(ui32, IndexStart, 0);
        YDB_ACCESSOR(ui64, UsedMemory, 0);
        YDB_ACCESSOR(ui64, WholeChunksMemory, 0);

    public:
        TGlobalDelimiter(const ui32 indexStart)
            : IndexStart(indexStart) {
        }
    };

    std::vector<TEntityDelimiter> delimiters;

    ui32 lastAppliedId = 0;
    ui32 currentRecordIdx = 0;
    bool needOne = false;
    const TColumnRecord* lastRecord = nullptr;
    for (auto&& i : GetRecordsVerified()) {
        if (lastAppliedId != i.GetEntityId()) {
            if (delimiters.size()) {
                AFL_VERIFY(delimiters.back().GetIndexStart() == PortionInfo->GetRecordsCount());
            }
            needOne = entityIds.contains(i.GetEntityId());
            currentRecordIdx = 0;
            lastAppliedId = i.GetEntityId();
            lastRecord = nullptr;
        }
        if (!needOne) {
            continue;
        }
        delimiters.emplace_back(
            currentRecordIdx, i.GetEntityId(), i.GetChunkIdx(), i.GetMeta().GetRawBytes(), lastRecord ? lastRecord->GetMeta().GetRawBytes() : 0);
        currentRecordIdx += i.GetMeta().GetRecordsCount();
        if (currentRecordIdx == PortionInfo->GetRecordsCount()) {
            delimiters.emplace_back(currentRecordIdx, i.GetEntityId(), i.GetChunkIdx() + 1, 0, i.GetMeta().GetRawBytes());
        }
        lastRecord = &i;
    }
    if (delimiters.empty()) {
        return { TPortionDataAccessor::TReadPage(0, PortionInfo->GetRecordsCount(), 0) };
    }
    std::sort(delimiters.begin(), delimiters.end());
    std::vector<TGlobalDelimiter> sumDelimiters;
    for (auto&& i : delimiters) {
        if (sumDelimiters.empty()) {
            sumDelimiters.emplace_back(i.GetIndexStart());
        } else if (sumDelimiters.back().GetIndexStart() != i.GetIndexStart()) {
            AFL_VERIFY(sumDelimiters.back().GetIndexStart() < i.GetIndexStart());
            TGlobalDelimiter backDelimiter(i.GetIndexStart());
            backDelimiter.MutableWholeChunksMemory() = sumDelimiters.back().GetWholeChunksMemory();
            backDelimiter.MutableUsedMemory() = sumDelimiters.back().GetUsedMemory();
            sumDelimiters.emplace_back(std::move(backDelimiter));
        }
        sumDelimiters.back().MutableWholeChunksMemory() += i.GetMemoryFinishChunk();
        sumDelimiters.back().MutableUsedMemory() += i.GetMemoryStartChunk();
    }
    std::vector<ui32> recordIdx = { 0 };
    std::vector<ui64> packMemorySize;
    const TGlobalDelimiter* lastBorder = &sumDelimiters.front();
    for (auto&& i : sumDelimiters) {
        const i64 sumMemory = (i64)i.GetUsedMemory() - (i64)lastBorder->GetWholeChunksMemory();
        AFL_VERIFY(sumMemory > 0);
        if (((ui64)sumMemory >= memoryLimit || i.GetIndexStart() == PortionInfo->GetRecordsCount()) && i.GetIndexStart()) {
            AFL_VERIFY(lastBorder->GetIndexStart() < i.GetIndexStart());
            recordIdx.emplace_back(i.GetIndexStart());
            packMemorySize.emplace_back(sumMemory);
            lastBorder = &i;
        }
    }
    AFL_VERIFY(recordIdx.front() == 0);
    AFL_VERIFY(recordIdx.back() == PortionInfo->GetRecordsCount())("real", JoinSeq(",", recordIdx))("expected", PortionInfo->GetRecordsCount());
    AFL_VERIFY(recordIdx.size() == packMemorySize.size() + 1);
    std::vector<TReadPage> pages;
    for (ui32 i = 0; i < packMemorySize.size(); ++i) {
        pages.emplace_back(recordIdx[i], recordIdx[i + 1] - recordIdx[i], packMemorySize[i]);
    }
    return pages;
}

std::vector<TPortionDataAccessor::TPage> TPortionDataAccessor::BuildPages() const {
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
    for (auto&& i : GetRecordsVerified()) {
        if (currentId != i.GetColumnId()) {
            currentSize = 0;
            currentId = i.GetColumnId();
        }
        currentSize += i.GetMeta().GetRecordsCount();
        ++currentCursor[currentSize];
        entities[i.GetColumnId()].emplace_back(&i, i.GetMeta().GetRecordsCount());
    }
    for (auto&& i : GetIndexesVerified()) {
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

ui64 TPortionDataAccessor::GetMinMemoryForReadColumns(const std::optional<std::set<ui32>>& columnIds) const {
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

    for (auto&& i : GetRecordsVerified()) {
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
        position += i.GetMeta().GetRecordsCount();
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

void TPortionDataAccessor::SaveToDatabase(IDbWrapper& db, const ui32 firstPKColumnId, const bool saveOnlyMeta) const {
    FullValidation();
    db.WritePortion(GetBlobIds(), *PortionInfo);
    if (!saveOnlyMeta) {
        NKikimrTxColumnShard::TIndexPortionBlobsInfo protoBlobs;
        for (auto&& i : GetBlobIds()) {
            *protoBlobs.AddBlobIds() = i.GetLogoBlobId().AsBinaryString();
        }

        NKikimrTxColumnShard::TIndexPortionAccessor protoData;
        for (auto& record : GetRecordsVerified()) {
            *protoData.AddChunks() = record.SerializeToDBProto();
        }
        db.WriteColumns(*PortionInfo, std::move(protoData), std::move(protoBlobs));

        for (auto& record : GetRecordsVerified()) {
            db.WriteColumn(*this, *PortionInfo, record, firstPKColumnId);
        }
        for (auto& record : GetIndexesVerified()) {
            db.WriteIndex(*this, *PortionInfo, record);
        }
    }
}

void TPortionDataAccessor::RemoveFromDatabase(IDbWrapper& db) const {
    db.ErasePortion(*PortionInfo);
    for (auto& record : GetRecordsVerified()) {
        db.EraseColumn(*PortionInfo, record);
    }
    for (auto& record : GetIndexesVerified()) {
        db.EraseIndex(*PortionInfo, record);
    }
}

void TPortionDataAccessor::FullValidation() const {
    CheckChunksOrder(GetRecordsVerified());
    CheckChunksOrder(GetIndexesVerified());

    PortionInfo->FullValidation();
    std::set<ui32> blobIdxs;
    for (auto&& i : GetRecordsVerified()) {
        TBlobRange::Validate(BlobIds, i.GetBlobRange()).Validate();
        blobIdxs.emplace(i.GetBlobRange().GetBlobIdxVerified());
    }
    AFL_VERIFY(GetRecordsVerified().size());
    for (auto&& i : GetIndexesVerified()) {
        if (auto bRange = i.GetBlobRangeOptional()) {
            TBlobRange::Validate(BlobIds, *bRange).Validate();
            blobIdxs.emplace(bRange->GetBlobIdxVerified());
        }
    }
    AFL_VERIFY(blobIdxs.size())("portion_info", PortionInfo->DebugString());
    AFL_VERIFY(BlobIds.size() == blobIdxs.size());
    AFL_VERIFY(BlobIds.size() == *blobIdxs.rbegin() + 1);
}

void TPortionDataAccessor::SerializeToProto(NKikimrColumnShardDataSharingProto::TPortionInfo& proto) const {
    PortionInfo->SerializeToProto(GetBlobIds(), proto);
    AFL_VERIFY(GetRecordsVerified().size());
    for (auto&& r : GetRecordsVerified()) {
        *proto.AddRecords() = r.SerializeToProto();
    }

    for (auto&& r : GetIndexesVerified()) {
        *proto.AddIndexes() = r.SerializeToProto();
    }
}

TConclusionStatus TPortionDataAccessor::DeserializeFromProto(const NKikimrColumnShardDataSharingProto::TPortionInfo& proto) {
    Records = std::vector<TColumnRecord>();
    Indexes = std::vector<TIndexChunk>();
    for (auto&& i : proto.GetRecords()) {
        auto parse = TColumnRecord::BuildFromProto(i);
        if (!parse) {
            return parse;
        }
        Records->emplace_back(std::move(parse.DetachResult()));
    }
    for (auto&& i : proto.GetIndexes()) {
        auto parse = TIndexChunk::BuildFromProto(i);
        if (!parse) {
            return parse;
        }
        Indexes->emplace_back(std::move(parse.DetachResult()));
    }
    return TConclusionStatus::Success();
}

TConclusion<TPortionDataAccessor> TPortionDataAccessor::BuildFromProto(
    const NKikimrColumnShardDataSharingProto::TPortionInfo& proto, const TIndexInfo& indexInfo, const IBlobGroupSelector& groupSelector) {
    TPortionMetaConstructor constructor;
    if (!constructor.LoadMetadata(proto.GetMeta(), indexInfo, groupSelector)) {
        return TConclusionStatus::Fail("cannot parse meta");
    }
    std::shared_ptr<TPortionInfo> resultPortion = std::make_shared<TCompactedPortionInfo>(constructor.Build());

    {
        auto parse = resultPortion->DeserializeFromProto(proto);
        if (!parse) {
            return parse;
        }
    }
    {
        TPortionDataAccessor result;
        result.PortionInfo = resultPortion;
        auto parse = result.DeserializeFromProto(proto);
        if (!parse) {
            return parse;
        }
        return result;
    }
}

TConclusion<std::shared_ptr<NArrow::NAccessor::IChunkedArray>> TPortionDataAccessor::TPreparedColumn::AssembleAccessor() const {
    Y_ABORT_UNLESS(!Blobs.empty());

    NArrow::NAccessor::TCompositeChunkedArray::TBuilder builder(GetField()->type());
    for (auto& blob : Blobs) {
        auto chunkedArray = blob.BuildRecordBatch(*Loader);
        if (chunkedArray.IsFail()) {
            return chunkedArray.AddMessageInfo("field: " + GetField()->name());
        }
        builder.AddChunk(chunkedArray.DetachResult());
    }
    return builder.Finish();
}

std::shared_ptr<NArrow::NAccessor::IChunkedArray> TPortionDataAccessor::TPreparedColumn::AssembleForSeqAccess() const {
    Y_ABORT_UNLESS(!Blobs.empty());

    std::vector<std::shared_ptr<NArrow::NAccessor::IChunkedArray>> chunks;
    chunks.reserve(Blobs.size());
    ui64 recordsCount = 0;
    for (auto& blob : Blobs) {
        chunks.push_back(blob.BuildDeserializeChunk(Loader));
        if (!!blob.GetData()) {
            recordsCount += blob.GetExpectedRowsCountVerified();
        } else {
            recordsCount += blob.GetDefaultRowsCount();
        }
    }

    if (chunks.size() == 1) {
        return chunks.front();
    } else {
        return std::make_shared<NArrow::NAccessor::TCompositeChunkedArray>(std::move(chunks), recordsCount, Loader->GetResultField()->type());
    }
}

std::shared_ptr<NArrow::NAccessor::IChunkedArray> TPortionDataAccessor::TAssembleBlobInfo::BuildDeserializeChunk(
    const std::shared_ptr<TColumnLoader>& loader) const {
    if (DefaultRowsCount) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "build_trivial");
        Y_ABORT_UNLESS(!Data);
        return std::make_shared<NArrow::NAccessor::TSparsedArray>(DefaultValue, loader->GetField()->type(), DefaultRowsCount);
    } else {
        AFL_VERIFY(ExpectedRowsCount);
        return std::make_shared<NArrow::NAccessor::TDeserializeChunkedArray>(*ExpectedRowsCount, loader, Data);
    }
}

TConclusion<std::shared_ptr<NArrow::NAccessor::IChunkedArray>> TPortionDataAccessor::TAssembleBlobInfo::BuildRecordBatch(
    const TColumnLoader& loader) const {
    if (DefaultRowsCount) {
        Y_ABORT_UNLESS(!Data);
        return std::make_shared<NArrow::NAccessor::TSparsedArray>(DefaultValue, loader.GetField()->type(), DefaultRowsCount);
    } else {
        AFL_VERIFY(ExpectedRowsCount);
        return loader.ApplyConclusion(Data, *ExpectedRowsCount).AddMessageInfo(::ToString(loader.GetAccessorConstructor()->GetType()));
    }
}

TConclusion<std::shared_ptr<NArrow::TGeneralContainer>> TPortionDataAccessor::TPreparedBatchData::AssembleToGeneralContainer(
    const std::set<ui32>& sequentialColumnIds) const {
    std::vector<std::shared_ptr<NArrow::NAccessor::IChunkedArray>> columns;
    std::vector<std::shared_ptr<arrow::Field>> fields;
    for (auto&& i : Columns) {
        //        NActors::TLogContextGuard lGuard = NActors::TLogContextBuilder::Build()("column", i.GetField()->ToString())("column_id", i.GetColumnId());
        if (sequentialColumnIds.contains(i.GetColumnId())) {
            columns.emplace_back(i.AssembleForSeqAccess());
        } else {
            auto conclusion = i.AssembleAccessor();
            if (conclusion.IsFail()) {
                return TConclusionStatus::Fail(conclusion.GetErrorMessage() + ";" + i.GetName());
            }
            columns.emplace_back(conclusion.DetachResult());
        }
        fields.emplace_back(i.GetField());
    }

    return std::make_shared<NArrow::TGeneralContainer>(fields, std::move(columns));
}

}   // namespace NKikimr::NOlap
