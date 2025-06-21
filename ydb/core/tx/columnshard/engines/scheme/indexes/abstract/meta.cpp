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

std::shared_ptr<NReader::NCommon::IKernelFetchLogic> IIndexMeta::DoBuildFetchTask(
    const THashSet<NRequest::TOriginalDataAddress>& dataAddresses, const std::shared_ptr<IIndexMeta>& selfPtr,
    const std::shared_ptr<IStoragesManager>& storagesManager) const {
    return std::make_shared<TIndexFetcherLogic>(dataAddresses, selfPtr, storagesManager);
}

std::optional<ui64> IIndexMeta::CalcCategory(const TString& subColumnName) const {
    return DoCalcCategory(subColumnName);
}

TConclusion<std::vector<std::shared_ptr<IPortionDataChunk>>> IIndexMeta::BuildIndexOptional(
    const THashMap<ui32, std::vector<std::shared_ptr<IPortionDataChunk>>>& data, const ui32 recordsCount, const TIndexInfo& indexInfo) const {
    auto conclusion = DoBuildIndexOptional(data, recordsCount, indexInfo);
    if (conclusion.IsFail()) {
        return conclusion;
    }
    ui32 checkRecordsCount = 0;
    for (auto&& i : *conclusion) {
        checkRecordsCount += i->GetRecordsCountVerified();
    }
    AFL_VERIFY(checkRecordsCount == recordsCount);
    return conclusion;
}

}   // namespace NKikimr::NOlap::NIndexes
