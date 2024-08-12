#include "write_with_blobs.h"
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>

namespace NKikimr::NOlap {

void TWritePortionInfoWithBlobsConstructor::TBlobInfo::AddChunk(TWritePortionInfoWithBlobsConstructor& owner, const std::shared_ptr<IPortionDataChunk>& chunk) {
    AFL_VERIFY(chunk);
    Y_ABORT_UNLESS(!Finished);
    const TString& data = chunk->GetData();

    TBlobRangeLink16 bRange(Size, data.size());
    Size += data.size();

    Y_ABORT_UNLESS(Chunks.emplace(chunk->GetChunkAddressVerified(), chunk).second);
    ChunksOrdered.emplace_back(chunk);

    chunk->AddIntoPortionBeforeBlob(bRange, owner.GetPortionConstructor());
}

void TWritePortionInfoWithBlobsResult::TBlobInfo::RegisterBlobId(TWritePortionInfoWithBlobsResult& owner, const TUnifiedBlobId& blobId) const {
    const TBlobRangeLink16::TLinkId idx = owner.GetPortionConstructor().RegisterBlobId(blobId);
    for (auto&& i : Chunks) {
        owner.GetPortionConstructor().RegisterBlobIdx(i, idx);
    }
}

TWritePortionInfoWithBlobsConstructor TWritePortionInfoWithBlobsConstructor::BuildByBlobs(std::vector<TSplittedBlob>&& chunks,
    const THashMap<ui32, std::shared_ptr<IPortionDataChunk>>& inplaceChunks,
    const ui64 granule, const ui64 schemaVersion, const TSnapshot& snapshot, const std::shared_ptr<IStoragesManager>& operators)
{
    TPortionInfoConstructor constructor(granule);
    constructor.SetMinSnapshotDeprecated(snapshot);
    constructor.SetSchemaVersion(schemaVersion);
    return BuildByBlobs(std::move(chunks), inplaceChunks, std::move(constructor), operators);
}

TWritePortionInfoWithBlobsConstructor TWritePortionInfoWithBlobsConstructor::BuildByBlobs(
    std::vector<TSplittedBlob>&& chunks, const THashMap<ui32, std::shared_ptr<IPortionDataChunk>>& inplaceChunks, TPortionInfoConstructor&& constructor, const std::shared_ptr<IStoragesManager>& operators) {
    TWritePortionInfoWithBlobsConstructor result(std::move(constructor));
    for (auto&& blob : chunks) {
        auto storage = operators->GetOperatorVerified(blob.GetGroupName());
        auto blobInfo = result.StartBlob(storage);
        for (auto&& chunk : blob.GetChunks()) {
            blobInfo.AddChunk(chunk);
        }
    }
    for (auto&& [_, i] : inplaceChunks) {
        result.GetPortionConstructor().AddIndex(
            TIndexChunk(i->GetEntityId(), i->GetChunkIdxVerified(), i->GetRecordsCountVerified(), i->GetRawBytesVerified(), i->GetData()));
    }

    return result;
}

std::vector<std::shared_ptr<IPortionDataChunk>> TWritePortionInfoWithBlobsConstructor::GetEntityChunks(const ui32 entityId) const {
    std::map<TChunkAddress, std::shared_ptr<IPortionDataChunk>> sortedChunks;
    for (auto&& b : GetBlobs()) {
        for (auto&& i : b.GetChunks()) {
            if (i.second->GetEntityId() == entityId) {
                sortedChunks.emplace(i.first, i.second);
            }
        }
    }
    std::vector<std::shared_ptr<IPortionDataChunk>> result;
    for (auto&& i : sortedChunks) {
        AFL_VERIFY(i.second->GetChunkIdxVerified() == result.size())("idx", i.second->GetChunkIdxVerified())("size", result.size());
        result.emplace_back(i.second);
    }
    return result;
}

TString TWritePortionInfoWithBlobsResult::GetBlobByRangeVerified(const ui32 entityId, const ui32 chunkIdx) const {
    AFL_VERIFY(!!PortionConstructor);
    for (auto&& rec : PortionConstructor->GetRecords()) {
        if (rec.GetEntityId() != entityId || rec.GetChunkIdx() != chunkIdx) {
            continue;
        }
        for (auto&& i : Blobs) {
            for (auto&& c : i.GetChunks()) {
                if (c == TChunkAddress(entityId, chunkIdx)) {
                    return i.GetResultBlob().substr(rec.BlobRange.Offset, rec.BlobRange.Size);
                }
            }
        }
        AFL_VERIFY(false);
    }
    AFL_VERIFY(false);
    return "";
}

}
