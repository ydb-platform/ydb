#include "write_with_blobs.h"
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>

namespace NKikimr::NOlap {

void TWritePortionInfoWithBlobs::TBlobInfo::AddChunk(TWritePortionInfoWithBlobs& owner, const std::shared_ptr<IPortionDataChunk>& chunk) {
    AFL_VERIFY(chunk);
    Y_ABORT_UNLESS(!ResultBlob);
    const TString& data = chunk->GetData();

    TBlobRangeLink16 bRange(Size, data.size());
    Size += data.size();

    Y_ABORT_UNLESS(Chunks.emplace(chunk->GetChunkAddressVerified(), chunk).second);
    ChunksOrdered.emplace_back(chunk);

    chunk->AddIntoPortionBeforeBlob(bRange, owner.GetPortionConstructor());
}

void TWritePortionInfoWithBlobs::TBlobInfo::RegisterBlobId(TWritePortionInfoWithBlobs& owner, const TUnifiedBlobId& blobId) {
    const TBlobRangeLink16::TLinkId idx = owner.GetPortionConstructor().RegisterBlobId(blobId);
    for (auto&& i : Chunks) {
        owner.GetPortionConstructor().RegisterBlobIdx(i.first, idx);
    }
}

TWritePortionInfoWithBlobs TWritePortionInfoWithBlobs::BuildByBlobs(std::vector<TSplittedBlob>&& chunks,
    const ui64 granule, const ui64 schemaVersion, const TSnapshot& snapshot, const std::shared_ptr<IStoragesManager>& operators)
{
    TPortionInfoConstructor constructor(granule);
    constructor.SetMinSnapshotDeprecated(snapshot);
    constructor.SetSchemaVersion(schemaVersion);
    return BuildByBlobs(std::move(chunks), std::move(constructor), operators);
}

TWritePortionInfoWithBlobs TWritePortionInfoWithBlobs::BuildByBlobs(std::vector<TSplittedBlob>&& chunks, TPortionInfoConstructor&& constructor, const std::shared_ptr<IStoragesManager>& operators) {
    TWritePortionInfoWithBlobs result(std::move(constructor));
    for (auto&& blob : chunks) {
        auto storage = operators->GetOperatorVerified(blob.GetGroupName());
        auto blobInfo = result.StartBlob(storage);
        for (auto&& chunk : blob.GetChunks()) {
            blobInfo.AddChunk(chunk);
        }
    }
    return result;
}

std::vector<std::shared_ptr<IPortionDataChunk>> TWritePortionInfoWithBlobs::GetEntityChunks(const ui32 entityId) const {
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

void TWritePortionInfoWithBlobs::FillStatistics(const TIndexInfo& index) {
    NStatistics::TPortionStorage storage;
    for (auto&& i : index.GetStatisticsByName()) {
        THashMap<ui32, std::vector<std::shared_ptr<IPortionDataChunk>>> data;
        for (auto&& entityId : i.second->GetEntityIds()) {
            data.emplace(entityId, GetEntityChunks(entityId));
        }
        i.second->FillStatisticsData(data, storage, index);
    }
    GetPortionConstructor().MutableMeta().SetStatisticsStorage(std::move(storage));
}

}
