#include "with_blobs.h"
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>
#include <ydb/core/tx/columnshard/engines/scheme/filtered_scheme.h>

namespace NKikimr::NOlap {

void TPortionInfoWithBlobs::TBlobInfo::RestoreChunk(const TPortionInfoWithBlobs& owner, const IPortionColumnChunk::TPtr& chunk) {
    Y_ABORT_UNLESS(!ResultBlob);
    const TString& data = chunk->GetData();
    Size += data.size();
    auto address = TChunkAddress(chunk->GetColumnId(), chunk->GetChunkIdx());
    Y_ABORT_UNLESS(owner.GetPortionInfo().GetRecordPointer(address));
    Y_ABORT_UNLESS(Chunks.emplace(address, chunk).second);
    ChunksOrdered.emplace_back(chunk);
}

const TColumnRecord& TPortionInfoWithBlobs::TBlobInfo::AddChunk(TPortionInfoWithBlobs& owner, const IPortionColumnChunk::TPtr& chunk) {
    Y_ABORT_UNLESS(!ResultBlob);
    TBlobRange bRange;
    const TString& data = chunk->GetData();

    bRange.Offset = Size;
    bRange.Size = data.size();

    TColumnRecord rec(TChunkAddress(chunk->GetColumnId(), chunk->GetChunkIdx()), bRange, chunk->BuildSimpleChunkMeta());

    Size += data.size();

    Y_ABORT_UNLESS(Chunks.emplace(rec.GetAddress(), chunk).second);
    ChunksOrdered.emplace_back(chunk);
    auto& result = owner.PortionInfo.AppendOneChunkColumn(std::move(rec));
    return result;
}

void TPortionInfoWithBlobs::TBlobInfo::RegisterBlobId(TPortionInfoWithBlobs& owner, const TUnifiedBlobId& blobId) {
    auto it = owner.PortionInfo.Records.begin();
    for (auto&& i : Chunks) {
        bool found = false;
        for (; it != owner.PortionInfo.Records.end(); ++it) {
            if (it->ColumnId == i.first.GetColumnId() && it->Chunk == i.first.GetChunk()) {
                it->BlobRange.BlobId = blobId;
                found = true;
                break;
            }
        }
        AFL_VERIFY(found)("address", i.second->DebugString());
    }
}

void TPortionInfoWithBlobs::TBlobInfo::ExtractColumnChunks(const ui32 columnId, std::map<TChunkAddress, IPortionColumnChunk::TPtr>& resultMap) {
    const auto pred = [this, &resultMap, columnId](const IPortionColumnChunk::TPtr& chunk) {
        if (chunk->GetColumnId() == columnId) {
            resultMap.emplace(chunk->GetChunkAddress(), chunk);
            Chunks.erase(chunk->GetChunkAddress());
            return true;
        } else {
            return false;
        }
    };
    ChunksOrdered.erase(std::remove_if(ChunksOrdered.begin(), ChunksOrdered.end(), pred), ChunksOrdered.end());
}

std::shared_ptr<arrow::RecordBatch> TPortionInfoWithBlobs::GetBatch(const ISnapshotSchema::TPtr& data, const ISnapshotSchema& result, const std::set<std::string>& columnNames) const {
    Y_ABORT_UNLESS(data);
    if (columnNames.empty()) {
        if (!CachedBatch) {
            THashMap<TBlobRange, TString> blobs;
            for (auto&& i : PortionInfo.Records) {
                blobs[i.BlobRange] = GetBlobByRangeVerified(i.ColumnId, i.Chunk);
                Y_ABORT_UNLESS(blobs[i.BlobRange].size() == i.BlobRange.Size);
            }
            CachedBatch = PortionInfo.AssembleInBatch(*data, result, blobs);
            Y_DEBUG_ABORT_UNLESS(NArrow::IsSortedAndUnique(*CachedBatch, result.GetIndexInfo().GetReplaceKey()));
        }
        return *CachedBatch;
    } else if (CachedBatch) {
        std::vector<TString> columnNamesString;
        for (auto&& i : columnNames) {
            columnNamesString.emplace_back(i.data(), i.size());
        }
        auto result = NArrow::ExtractColumns(*CachedBatch, columnNamesString);
        Y_ABORT_UNLESS(result);
        return result;
    } else {
        auto filteredSchema = std::make_shared<TFilteredSnapshotSchema>(data, columnNames);
        THashMap<TBlobRange, TString> blobs;
        for (auto&& i : PortionInfo.Records) {
            blobs[i.BlobRange] = GetBlobByRangeVerified(i.ColumnId, i.Chunk);
            Y_ABORT_UNLESS(blobs[i.BlobRange].size() == i.BlobRange.Size);
        }
        return PortionInfo.AssembleInBatch(*data, *filteredSchema, blobs);
    }
}

NKikimr::NOlap::TPortionInfoWithBlobs TPortionInfoWithBlobs::RestorePortion(const TPortionInfo& portion, const THashMap<TBlobRange, TString>& blobs) {
    TPortionInfoWithBlobs result(portion);
    const auto pred = [](const TColumnRecord& l, const TColumnRecord& r) {
        return l.GetAddress() < r.GetAddress();
    };
    std::sort(result.PortionInfo.Records.begin(), result.PortionInfo.Records.end(), pred);

    THashMap<TUnifiedBlobId, std::vector<const TColumnRecord*>> recordsByBlob;
    for (auto&& c : result.PortionInfo.Records) {
        auto& blobRecords = recordsByBlob[c.BlobRange.BlobId];
        blobRecords.emplace_back(&c);
    }

    const auto predOffset = [](const TColumnRecord* l, const TColumnRecord* r) {
        return l->BlobRange.Offset < r->BlobRange.Offset;
    };

    for (auto&& i : recordsByBlob) {
        std::sort(i.second.begin(), i.second.end(), predOffset);
        auto builder = result.StartBlob();
        for (auto&& d : i.second) {
            auto itBlob = blobs.find(d->BlobRange);
            Y_ABORT_UNLESS(itBlob != blobs.end());
            builder.RestoreChunk(std::make_shared<TSimpleOrderedColumnChunk>(*d, itBlob->second));
        }
    }
    return result;
}

std::vector<NKikimr::NOlap::TPortionInfoWithBlobs> TPortionInfoWithBlobs::RestorePortions(const std::vector<TPortionInfo>& portions, const THashMap<TBlobRange, TString>& blobs) {
    std::vector<TPortionInfoWithBlobs> result;
    for (auto&& i : portions) {
        result.emplace_back(RestorePortion(i, blobs));
    }
    return result;
}

NKikimr::NOlap::TPortionInfoWithBlobs TPortionInfoWithBlobs::BuildByBlobs(std::vector<std::vector<IPortionColumnChunk::TPtr>>& chunksByBlobs,
    std::shared_ptr<arrow::RecordBatch> batch, const ui64 granule, const TSnapshot& snapshot, const std::shared_ptr<NOlap::IBlobsStorageOperator>& bStorageOperator)
{
    TPortionInfoWithBlobs result(TPortionInfo(granule, 0, snapshot, bStorageOperator), batch);
    for (auto& blob : chunksByBlobs) {
        auto blobInfo = result.StartBlob();
        for (auto&& chunk : blob) {
            blobInfo.AddChunk(chunk);
        }
    }

    const auto pred = [](const TColumnRecord& l, const TColumnRecord& r) {
        return l.GetAddress() < r.GetAddress();
    };
    std::sort(result.GetPortionInfo().Records.begin(), result.GetPortionInfo().Records.end(), pred);
    return result;
}

std::optional<NKikimr::NOlap::TPortionInfoWithBlobs> TPortionInfoWithBlobs::ChangeSaver(ISnapshotSchema::TPtr currentSchema, const TSaverContext& saverContext) const {
    TPortionInfoWithBlobs result(PortionInfo, CachedBatch);
    result.PortionInfo.Records.clear();
    std::optional<TPortionInfoWithBlobs::TBlobInfo::TBuilder> bBuilder;
    for (auto& rec : PortionInfo.Records) {
        auto field = currentSchema->GetFieldByColumnIdVerified(rec.ColumnId);

        const TString blobOriginal = GetBlobByRangeVerified(rec.ColumnId, rec.Chunk);
        {
            auto rb = NArrow::TStatusValidator::GetValid(currentSchema->GetColumnLoaderVerified(rec.ColumnId)->Apply(blobOriginal));
            auto columnSaver = currentSchema->GetColumnSaver(rec.ColumnId, saverContext);
            const TString newBlob = columnSaver.Apply(rb);
            if (newBlob.size() >= TPortionInfo::BLOB_BYTES_LIMIT) {
                return {};
            }
            if (!bBuilder || result.GetBlobs().back().GetSize() + newBlob.size() >= TPortionInfo::BLOB_BYTES_LIMIT) {
                bBuilder = result.StartBlob();
            }
            Y_ABORT_UNLESS(rb);
            Y_ABORT_UNLESS(rb->num_columns() == 1);

            bBuilder->AddChunk(std::make_shared<TSimpleOrderedColumnChunk>(rec, newBlob));
        }
    }
    const auto pred = [](const TColumnRecord& l, const TColumnRecord& r) {
        return l.GetAddress() < r.GetAddress();
    };
    std::sort(result.PortionInfo.Records.begin(), result.PortionInfo.Records.end(), pred);

    return result;
}

std::vector<NKikimr::NOlap::IPortionColumnChunk::TPtr> TPortionInfoWithBlobs::GetColumnChunks(const ui32 columnId) const {
    std::map<TChunkAddress, IPortionColumnChunk::TPtr> sortedChunks;
    for (auto&& b : GetBlobs()) {
        for (auto&& i : b.GetChunks()) {
            if (i.second->GetColumnId() == columnId) {
                sortedChunks.emplace(i.first, i.second);
            }
        }
    }
    std::vector<IPortionColumnChunk::TPtr> result;
    for (auto&& i : sortedChunks) {
        AFL_VERIFY(i.second->GetChunkIdx() == result.size())("idx", i.second->GetChunkIdx())("size", result.size());
        result.emplace_back(i.second);
    }
    return result;
}

bool TPortionInfoWithBlobs::ExtractColumnChunks(const ui32 columnId, std::vector<const TColumnRecord*>& records, std::vector<IPortionColumnChunk::TPtr>& chunks) {
    records = GetPortionInfo().GetColumnChunksPointers(columnId);
    if (records.empty()) {
        return false;
    }
    std::map<TChunkAddress, IPortionColumnChunk::TPtr> chunksMap;
    for (auto&& i : Blobs) {
        i.ExtractColumnChunks(columnId, chunksMap);
    }
    std::vector<IPortionColumnChunk::TPtr> chunksLocal;
    for (auto&& i : chunksMap) {
        Y_ABORT_UNLESS(i.first.GetColumnId() == columnId);
        Y_ABORT_UNLESS(i.first.GetChunk() == chunksLocal.size());
        chunksLocal.emplace_back(i.second);
    }
    std::swap(chunksLocal, chunks);
    return true;
}

}
