#include "with_blobs.h"
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>

namespace NKikimr::NOlap {

void TPortionInfoWithBlobs::TBlobInfo::RestoreChunk(const TPortionInfoWithBlobs& owner, TSimpleOrderedColumnChunk&& chunk) {
    Y_VERIFY(!ResultBlob);
    Size += chunk.GetData().size();
    auto address = chunk.GetChunkAddress();
    Y_VERIFY(owner.GetPortionInfo().GetRecordPointer(address));
    Y_VERIFY(ChunksOrdered.empty() || ChunksOrdered.back()->GetOffset() < chunk.GetOffset());
    auto dataInsert = Chunks.emplace(address, std::move(chunk));
    Y_VERIFY(dataInsert.second);
    ChunksOrdered.emplace_back(&dataInsert.first->second);
}

const TColumnRecord& TPortionInfoWithBlobs::TBlobInfo::AddChunk(TPortionInfoWithBlobs& owner, TOrderedColumnChunk&& chunk, const TIndexInfo& info) {
    Y_VERIFY(!ResultBlob);
    auto rec = TColumnRecord(chunk.GetChunkAddress(), chunk.GetColumn(), info);

    Y_VERIFY(chunk.GetOffset() == Size);
    rec.BlobRange.Offset = chunk.GetOffset();
    rec.BlobRange.Size = chunk.GetData().size();
    Size += chunk.GetData().size();

    auto dataInsert = Chunks.emplace(rec.GetAddress(), std::move(chunk));
    Y_VERIFY(dataInsert.second);
    ChunksOrdered.emplace_back(&dataInsert.first->second);
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
        AFL_VERIFY(found)("address", i.second.DebugString());
    }
}

std::shared_ptr<arrow::RecordBatch> TPortionInfoWithBlobs::GetBatch(const ISnapshotSchema& data, const ISnapshotSchema& result) const {
    if (!CachedBatch) {
        THashMap<TBlobRange, TString> blobs;
        for (auto&& i : PortionInfo.Records) {
            blobs[i.BlobRange] = GetBlobByRangeVerified(i.ColumnId, i.Chunk);
            Y_VERIFY(blobs[i.BlobRange].size() == i.BlobRange.Size);
        }
        CachedBatch = PortionInfo.AssembleInBatch(data, result, blobs);
        Y_VERIFY_DEBUG(NArrow::IsSortedAndUnique(*CachedBatch, result.GetIndexInfo().GetReplaceKey()));
    }
    return *CachedBatch;
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
            Y_VERIFY(itBlob != blobs.end());
            builder.RestoreChunk(TSimpleOrderedColumnChunk(d->GetAddress(), d->BlobRange.Offset, itBlob->second));
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

NKikimr::NOlap::TPortionInfoWithBlobs TPortionInfoWithBlobs::BuildByBlobs(std::vector<std::vector<TOrderedColumnChunk>>& chunksByBlobs,
    std::shared_ptr<arrow::RecordBatch> batch, const ui64 granule, const TSnapshot& snapshot, const TIndexInfo& info)
{
    TPortionInfoWithBlobs result(TPortionInfo(granule, 0, snapshot), batch);
    for (auto& blob : chunksByBlobs) {
        auto blobInfo = result.StartBlob();
        for (auto&& chunk : blob) {
            blobInfo.AddChunk(std::move(chunk), info);
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
    ui64 offset = 0;
    for (auto& rec : PortionInfo.Records) {
        auto field = currentSchema->GetFieldByColumnIdVerified(rec.ColumnId);

        const TString blobOriginal = GetBlobByRangeVerified(rec.ColumnId, rec.Chunk);
        {
            auto rb = NArrow::TStatusValidator::GetValid(currentSchema->GetColumnLoader(rec.ColumnId)->Apply(blobOriginal));
            auto columnSaver = currentSchema->GetColumnSaver(rec.ColumnId, saverContext);
            const TString newBlob = columnSaver.Apply(rb);
            if (newBlob.size() >= TPortionInfo::BLOB_BYTES_LIMIT) {
                return {};
            }
            if (!bBuilder || result.GetBlobs().back().GetSize() + newBlob.size() >= TPortionInfo::BLOB_BYTES_LIMIT) {
                bBuilder = result.StartBlob();
                offset = 0;
            }
            Y_VERIFY(rb);
            Y_VERIFY(rb->num_columns() == 1);

            bBuilder->AddChunk(TOrderedColumnChunk(rec.GetAddress(), offset, newBlob, rb->column(0)), currentSchema->GetIndexInfo());
            offset += newBlob.size();
        }
    }
    const auto pred = [](const TColumnRecord& l, const TColumnRecord& r) {
        return l.GetAddress() < r.GetAddress();
    };
    std::sort(result.PortionInfo.Records.begin(), result.PortionInfo.Records.end(), pred);

    return result;
}

}
