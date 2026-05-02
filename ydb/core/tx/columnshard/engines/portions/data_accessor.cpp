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
TPortionDataAccessor::TAssembleBlobInfo MakeAssembleBlobInfoWithMeta(THashMap<TChunkAddress, TExternalBlobInfo>& /*blobsData*/,
    typename THashMap<TChunkAddress, TExternalBlobInfo>::iterator itBlobs, const TColumnRecord& record) {
    TPortionDataAccessor::TAssembleBlobInfo blobInfo = [&]() -> TPortionDataAccessor::TAssembleBlobInfo {
        if constexpr (std::is_same_v<TExternalBlobInfo, TString>) {
            return TPortionDataAccessor::TAssembleBlobInfo(std::move(itBlobs->second));
        } else {
            return std::move(itBlobs->second);
        }
    }();
    if (record.GetMeta().GetAdditionalAccessorData()) {
        blobInfo.SetAdditionalAccessorData(record.GetMeta().GetAdditionalAccessorData());
    }
    return blobInfo;
}

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
            TPortionDataAccessor::TAssembleBlobInfo blobInfo = MakeAssembleBlobInfoWithMeta(blobsData, itBlobs, *it);
            blobsData.erase(itBlobs);
            columns.back().AddBlobInfo(it->Chunk, it->GetMeta().GetRecordsCount(), std::move(blobInfo));

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

// Page-aware variant: assembles only the chunks needed to cover rows
// [pageRange.Start, pageRange.End) of the portion.
//
// Page boundaries produced by BuildReadPages are at the UNION of per-column
// chunk-start positions, so a page boundary is a chunk boundary for AT LEAST
// ONE column but not necessarily for all columns.  Different columns may
// therefore have different leading offsets (colStartOffset) within their
// first included chunk.
//
// Two straddling cases are handled per column:
//
//   1. A chunk that straddles pageRange.End is included in full.
//      The assembled column has trailing rows after pageRange.End.
//
//   2. A chunk that straddles pageRange.Start is also included in full.
//      The assembled column has leading rows before pageRange.Start.
//
// Each column is individually sliced via per-column SliceOffset / SliceRows
// (set on TPreparedColumn) so that AssembleToGeneralContainer produces
// exactly pageRecordsCount rows per column, all corresponding to
// [pageRange.Start, pageRange.End) of the portion.
template <class TExternalBlobInfo>
TPortionDataAccessor::TPreparedBatchData PrepareForAssemblePageImpl(const TPortionDataAccessor& portionData, const TPortionInfo& portionInfo,
    const ISnapshotSchema& dataSchema, const ISnapshotSchema& resultSchema, THashMap<TChunkAddress, TExternalBlobInfo>& blobsData,
    const TPortionDataAccessor::TPageRange& pageRange, const std::optional<TSnapshot>& defaultSnapshot, const bool restoreAbsent) {

    // Assembles exactly the rows in [pageRange.Start, pageRange.End) from the portion.
    //
    // pageRange.End is an absolute row offset from the start of the portion.
    // pageRange.Start is the first row of the page (also absolute).
    //
    // For each column we:
    //   1. Skip chunks whose entire content falls before pageRange.Start.
    //   2. Include chunks that intersect [pageRange.Start, pageRange.End).
    //      A chunk that straddles pageRange.Start or pageRange.End is included
    //      in full; the assembled column may have extra leading and/or trailing
    //      rows that are trimmed per-column afterwards.
    //   3. Require that blobsData contains an entry for every included chunk.
    //
    // Each TPreparedColumn carries per-column slice parameters:
    //   ColumnSliceOffset = colStartOffset (leading rows before pageRange.Start)
    //   ColumnSliceRows   = pageRecordsCount
    // so that AssembleToGeneralContainer slices each column individually,
    // producing exactly pageRecordsCount rows per column.

    const ui32 pageRecordsCount = pageRange.GetRecordsCount();

    // We collect columns and their per-column slice offsets in parallel vectors.
    // presentColumnSliceOffsets[k] holds the colStartOffset for the k-th entry
    // in `columns` that corresponds to a present (non-absent) column.
    // Absent columns are filled with exactly pageRecordsCount default rows and
    // need no slicing.
    std::vector<TPortionDataAccessor::TColumnAssemblingInfo> columns;
    columns.reserve(resultSchema.GetColumnIds().size());
    // Per-column: index into `columns` → colStartOffset.  Only for present columns.
    std::vector<std::pair<size_t, ui32>> presentColumnSliceOffsets;

    auto it = portionData.GetRecordsVerified().begin();

    // Track the maximum assembled row count across all present columns.
    // For absent columns we always use pageRecordsCount (default fill).
    ui32 maxAssembledRows = pageRecordsCount;

    for (auto&& i : resultSchema.GetColumnIds()) {
        while (it != portionData.GetRecordsVerified().end() && it->GetColumnId() < i) {
            ++it;
            continue;
        }
        if ((it == portionData.GetRecordsVerified().end() || i < it->GetColumnId())) {
            // Column is absent from the portion entirely — fill with defaults for
            // pageRecordsCount rows.
            if (restoreAbsent || IIndexInfo::IsSpecialColumn(i)) {
                columns.emplace_back(pageRecordsCount, dataSchema.GetColumnLoaderOptional(i), resultSchema.GetColumnLoaderVerified(i));
                portionInfo.FillDefaultColumn(columns.back(), defaultSnapshot);
            }
        }
        if (it == portionData.GetRecordsVerified().end()) {
            continue;
        } else if (it->GetColumnId() != i) {
            AFL_VERIFY(i < it->GetColumnId());
            continue;
        }

        // Scan chunks to find those that intersect [pageRange.Start, pageRange.End).
        // Skip chunks entirely before pageRange.Start; include chunks that start before
        // pageRange.End (including any straddling chunk).
        // chunkRowOffset accumulates the total rows of included chunks.
        ui32 chunkRowOffset = 0;  // total rows of all included chunks for this column
        ui32 colStartOffset = 0;  // leading rows of the first included chunk before pageRange.Start
        {
            auto itCol = it;
            ui32 absoluteRow = 0;  // absolute row offset from start of portion
            bool firstIncluded = true;
            while (itCol != portionData.GetRecordsVerified().end() && itCol->GetColumnId() == i) {
                const ui32 chunkRows = itCol->GetMeta().GetRecordsCount();
                const ui32 chunkStart = absoluteRow;
                const ui32 chunkEnd = absoluteRow + chunkRows;

                if (chunkEnd <= pageRange.Start) {
                    // Chunk is entirely before the page — skip.
                    absoluteRow = chunkEnd;
                    ++itCol;
                    continue;
                }
                if (chunkStart >= pageRange.End) {
                    // Chunk is entirely after the page — stop.
                    break;
                }
                // Chunk intersects [pageRange.Start, pageRange.End).
                // Include it in full (straddling chunks are trimmed afterwards).
                if (firstIncluded) {
                    // Record how many rows of this chunk precede pageRange.Start.
                    colStartOffset = (pageRange.Start > chunkStart) ? (pageRange.Start - chunkStart) : 0;
                    firstIncluded = false;
                }
                chunkRowOffset += chunkRows;
                absoluteRow = chunkEnd;
                ++itCol;
            }
        }
        // chunkRowOffset is the total rows in included chunks for this column.
        // After skipping colStartOffset leading rows, we must have at least
        // pageRecordsCount rows remaining.
        AFL_VERIFY(chunkRowOffset >= colStartOffset + pageRecordsCount)
            ("chunk_row_offset", chunkRowOffset)("col_start_offset", colStartOffset)
            ("page_records", pageRecordsCount)
            ("page_start", pageRange.Start)("page_end", pageRange.End)
            ("column_id", i);
        if (chunkRowOffset > maxAssembledRows) {
            maxAssembledRows = chunkRowOffset;
        }

        // Create the TColumnAssemblingInfo with the actual row count and
        // add all included chunks to it.
        const size_t columnIdx = columns.size();
        columns.emplace_back(chunkRowOffset, dataSchema.GetColumnLoaderOptional(i), resultSchema.GetColumnLoaderVerified(i));
        presentColumnSliceOffsets.emplace_back(columnIdx, colStartOffset);

        ui32 chunkIdx = 0;
        ui32 absoluteRow = 0;
        auto itCol = it;
        while (itCol != portionData.GetRecordsVerified().end() && itCol->GetColumnId() == i) {
            const ui32 chunkRows = itCol->GetMeta().GetRecordsCount();
            const ui32 chunkStart = absoluteRow;
            const ui32 chunkEnd = absoluteRow + chunkRows;

            if (chunkEnd <= pageRange.Start) {
                // Chunk is entirely before the page — skip (no blob needed).
                absoluteRow = chunkEnd;
                ++itCol;
                continue;
            }
            if (chunkStart >= pageRange.End) {
                // Chunk is entirely after the page — stop.
                break;
            }
            // Chunk intersects [pageRange.Start, pageRange.End) — blob required.
            auto itBlobs = blobsData.find(itCol->GetAddress());
            AFL_VERIFY(itBlobs != blobsData.end())("size", blobsData.size())("address", itCol->GetAddress().DebugString())
                ("chunk_start", chunkStart)("chunk_end", chunkEnd)
                ("page_start", pageRange.Start)("page_end", pageRange.End);
            TPortionDataAccessor::TAssembleBlobInfo blobInfo = MakeAssembleBlobInfoWithMeta(blobsData, itBlobs, *itCol);
            columns.back().AddBlobInfo(chunkIdx, chunkRows, std::move(blobInfo));
            blobsData.erase(itBlobs);
            ++chunkIdx;
            absoluteRow = chunkEnd;
            ++itCol;
        }

        // Advance the main iterator past all chunks of this column.
        while (it != portionData.GetRecordsVerified().end() && it->GetColumnId() == i) {
            ++it;
        }
    }

    // Compile columns and set per-column slice parameters on present columns.
    std::vector<TPortionDataAccessor::TPreparedColumn> preparedColumns;
    preparedColumns.reserve(columns.size());
    for (auto& c : columns) {
        preparedColumns.emplace_back(c.Compile());
    }
    // Set per-column slice on each present column so that
    // AssembleToGeneralContainer slices each column individually to produce
    // exactly pageRecordsCount rows starting at colStartOffset.
    for (auto& [idx, colOffset] : presentColumnSliceOffsets) {
        preparedColumns[idx].SetColumnSlice(colOffset, pageRecordsCount);
    }

    // No batch-level SliceOffset/SliceRows needed — per-column slicing handles
    // the different leading offsets.  maxAssembledRows is passed as RowsCount
    // for metadata purposes only.
    return TPortionDataAccessor::TPreparedBatchData(std::move(preparedColumns), maxAssembledRows);
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

TPortionDataAccessor::TPreparedBatchData TPortionDataAccessor::PrepareForAssemble(const ISnapshotSchema& dataSchema,
    const ISnapshotSchema& resultSchema, THashMap<TChunkAddress, TString>& blobsData, const TPageRange& pageRange,
    const std::optional<TSnapshot>& defaultSnapshot, const bool restoreAbsent) const {
    return PrepareForAssemblePageImpl(*this, *PortionInfo, dataSchema, resultSchema, blobsData, pageRange, defaultSnapshot, restoreAbsent);
}

TPortionDataAccessor::TPreparedBatchData TPortionDataAccessor::PrepareForAssemble(const ISnapshotSchema& dataSchema,
    const ISnapshotSchema& resultSchema, THashMap<TChunkAddress, TAssembleBlobInfo>& blobsData, const TPageRange& pageRange,
    const std::optional<TSnapshot>& defaultSnapshot, const bool restoreAbsent) const {
    return PrepareForAssemblePageImpl(*this, *PortionInfo, dataSchema, resultSchema, blobsData, pageRange, defaultSnapshot, restoreAbsent);
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
        const TString& storageId = PortionInfo->GetIndexStorageId(c.GetIndexId(), indexInfo);
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
            blobs.ExtractOptional(PortionInfo->GetEntityStorageId(record.GetIndexId(), indexInfo), RestoreBlobRange(record.GetBlobRangeVerified()));
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

std::vector<THashMap<TChunkAddress, TString>> TPortionDataAccessor::DecodeBlobAddresses(const std::vector<std::shared_ptr<TPortionDataAccessor>>& accessors,
    const std::vector<ISnapshotSchema::TPtr>& schemas, NBlobOperations::NRead::TCompositeReadBlobs&& blobs) {
    std::vector<THashMap<TChunkAddress, TString>> result;
    AFL_VERIFY(accessors.size() == schemas.size())("accessors", accessors.size())("info", schemas.size());
    for (ui64 i = 0; i < accessors.size(); ++i) {
        result.emplace_back(accessors[i]->DecodeBlobAddressesImpl(blobs, schemas[i]->GetIndexInfo()));
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

ui64 TPortionDataAccessor::GetIndexBlobBytes(const std::set<ui32>& entityIds, const bool validation /*= true*/) const {
    ui64 sum = 0;
    const auto aggr = [&](const TIndexChunk& r) {
        sum += r.GetDataSize();
    };
    AggregateIndexChunksData(aggr, GetIndexesVerified(), &entityIds, validation);
    return sum;
}

ui64 TPortionDataAccessor::GetIndexBlobBytes(const bool validation /*= true*/) const {
    ui64 sum = 0;
    const auto aggr = [&](const TIndexChunk& r) {
        sum += r.GetDataSize();
    };
    AggregateIndexChunksData(aggr, GetIndexesVerified(), nullptr, validation);
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
    SliceBorderOffsets = DoCalcSliceBorderOffsets(*Records, *Indexes);
    return TConclusionStatus::Success();
}

TConclusion<std::shared_ptr<TPortionDataAccessor>> TPortionDataAccessor::BuildFromProto(
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
        std::shared_ptr<TPortionDataAccessor> result(new TPortionDataAccessor);
        result->PortionInfo = resultPortion;
        auto parse = result->DeserializeFromProto(proto);
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
        return std::make_shared<NArrow::NAccessor::TDeserializeChunkedArray>(
            *ExpectedRowsCount, loader, Data, false, GetAdditionalAccessorData());
    }
}

TConclusion<std::shared_ptr<NArrow::NAccessor::IChunkedArray>> TPortionDataAccessor::TAssembleBlobInfo::BuildRecordBatch(
    const TColumnLoader& loader) const {
    if (DefaultRowsCount) {
        Y_ABORT_UNLESS(!Data);
        return std::make_shared<NArrow::NAccessor::TSparsedArray>(DefaultValue, loader.GetField()->type(), DefaultRowsCount);
    } else {
        AFL_VERIFY(ExpectedRowsCount);
        return loader.ApplyConclusion(Data, *ExpectedRowsCount, std::nullopt, GetAdditionalAccessorData())
            .AddMessageInfo(::ToString(loader.GetAccessorConstructor()->GetType()));
    }
}

TConclusion<std::shared_ptr<NArrow::TGeneralContainer>> TPortionDataAccessor::TPreparedBatchData::AssembleToGeneralContainer(
    const std::set<ui32>& sequentialColumnIds) const {
    std::vector<std::shared_ptr<NArrow::NAccessor::IChunkedArray>> columns;
    std::vector<std::shared_ptr<arrow::Field>> fields;
    // Track whether any column has per-column slice parameters.
    bool hasPerColumnSlice = false;
    for (auto&& i : Columns) {
        //        NActors::TLogContextGuard lGuard = NActors::TLogContextBuilder::Build()("column", i.GetField()->ToString())("column_id", i.GetColumnId());
        std::shared_ptr<NArrow::NAccessor::IChunkedArray> colArray;
        if (sequentialColumnIds.contains(i.GetColumnId())) {
            colArray = i.AssembleForSeqAccess();
        } else {
            auto conclusion = i.AssembleAccessor();
            if (conclusion.IsFail()) {
                return TConclusionStatus::Fail(conclusion.GetErrorMessage() + ";" + i.GetName());
            }
            colArray = conclusion.DetachResult();
        }
        // Apply per-column slicing if set.  This is used by the page-aware
        // assembly path when columns have different chunk boundaries and
        // therefore different assembled row counts / leading offsets.
        if (i.GetColumnSliceRows()) {
            hasPerColumnSlice = true;
            const ui32 colOffset = i.GetColumnSliceOffset();
            const ui32 colRows = *i.GetColumnSliceRows();
            const ui64 available = colArray->GetRecordsCount();
            AFL_VERIFY(colOffset + colRows <= available)
                ("col_slice_offset", colOffset)("col_slice_rows", colRows)
                ("available", available)("column_id", i.GetColumnId());
            if (colOffset > 0 || colRows < available) {
                colArray = colArray->ISlice(colOffset, colRows);
            }
        }
        columns.emplace_back(std::move(colArray));
        fields.emplace_back(i.GetField());
    }

    auto container = std::make_shared<NArrow::TGeneralContainer>(fields, std::move(columns));

    // If per-column slicing was used, all columns are already trimmed to the
    // same row count — no batch-level slice is needed.
    // Otherwise, apply the legacy batch-level Slice(SliceOffset, SliceRows).
    if (!hasPerColumnSlice && SliceRows) {
        const ui32 available = container->GetRecordsCount();
        AFL_VERIFY(SliceOffset + *SliceRows <= available)
            ("slice_offset", SliceOffset)("slice_rows", *SliceRows)("available", available);
        if (SliceOffset > 0 || *SliceRows < available) {
            return std::make_shared<NArrow::TGeneralContainer>(container->Slice(SliceOffset, *SliceRows));
        }
    }

    return container;
}

}   // namespace NKikimr::NOlap
