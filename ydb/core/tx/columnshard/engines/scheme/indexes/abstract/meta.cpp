#include "fetcher.h"
#include "meta.h"

#include <ydb/core/tx/columnshard/engines/portions/index_chunk.h>

namespace NKikimr::NOlap::NIndexes {

bool IIndexMeta::DeserializeFromProto(const NKikimrSchemeOp::TOlapIndexDescription& proto) {
    if (!proto.GetId()) {
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("error", "cannot parse secondary data builder")("reason", "incorrect id - 0");
        return false;
    }
    if (!proto.GetName()) {
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("error", "cannot parse secondary data builder")("reason", "incorrect name - empty string");
        return false;
    }
    IndexId = proto.GetId();
    IndexName = proto.GetName();
    StorageId = proto.GetStorageId() ? proto.GetStorageId() : IStoragesManager::DefaultStorageId;
    return DoDeserializeFromProto(proto);
}

void IIndexMeta::SerializeToProto(NKikimrSchemeOp::TOlapIndexDescription& proto) const {
    AFL_VERIFY(IndexId);
    proto.SetId(IndexId);
    AFL_VERIFY(IndexName);
    proto.SetName(IndexName);
    if (StorageId) {
        proto.SetStorageId(StorageId);
    }
    return DoSerializeToProto(proto);
}

NJson::TJsonValue IIndexMeta::SerializeDataToJson(const TIndexChunk& iChunk, const TIndexInfo& indexInfo) const {
    NJson::TJsonValue result = NJson::JSON_MAP;
    result.InsertValue("entity_id", iChunk.GetEntityId());
    result.InsertValue("chunk_idx", iChunk.GetChunkIdx());
    if (iChunk.HasBlobData()) {
        result.InsertValue("data", DoSerializeDataToJson(iChunk.GetBlobDataVerified(), indexInfo));
    }
    return result;
}

std::shared_ptr<NReader::NCommon::IKernelFetchLogic> IIndexMeta::DoBuildFetchTask(const TIndexDataAddress& dataAddress,
    const std::vector<TChunkOriginalData>& chunks, const TIndexesCollection& collection, const std::shared_ptr<IIndexMeta>& selfPtr,
    const std::shared_ptr<IStoragesManager>& storagesManager) const {
    if (collection.HasIndexData(dataAddress)) {
        return nullptr;
    } else {
        const TIndexColumnChunked* chunkedIndex = collection.GetIndexDataOptional(GetIndexId());
        std::vector<TIndexChunkFetching> fetchingChunks;
        AFL_VERIFY(!chunkedIndex || chunkedIndex->GetChunksCount() == chunks.size());
        ui32 idx = 0;
        for (auto&& i : chunks) {
            const std::shared_ptr<IIndexHeader> header = chunkedIndex ? chunkedIndex->GetHeader(idx) : selfPtr->BuildHeader(i).DetachResult();
            fetchingChunks.emplace_back(TIndexChunkFetching(GetStorageId(), dataAddress, i, header));
            ++idx;
        }
        return std::make_shared<TIndexFetcherLogic>(dataAddress, std::move(fetchingChunks), selfPtr, storagesManager);
    }
}

}   // namespace NKikimr::NOlap::NIndexes
