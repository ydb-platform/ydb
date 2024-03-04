#include "meta.h"
#include <ydb/core/tx/columnshard/blobs_action/abstract/storages_manager.h>
#include <ydb/core/tx/columnshard/engines/portions/column_record.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>
#include <ydb/core/formats/arrow/hash/xx_hash.h>
#include <ydb/core/formats/arrow/hash/calcer.h>
#include <ydb/core/formats/arrow/serializer/abstract.h>
#include <ydb/core/formats/arrow/size_calcer.h>

namespace NKikimr::NOlap::NIndexes {

void TPortionIndexChunk::DoAddIntoPortionBeforeBlob(const TBlobRangeLink16& bRange, TPortionInfo& portionInfo) const {
    AFL_VERIFY(!bRange.IsValid());
    portionInfo.AddIndex(TIndexChunk(GetEntityId(), GetChunkIdx(), RecordsCount, RawBytes, bRange));
}

std::shared_ptr<NKikimr::NOlap::IPortionDataChunk> TIndexByColumns::DoBuildIndex(const ui32 indexId, std::map<ui32, std::vector<std::shared_ptr<IPortionDataChunk>>>& data, const TIndexInfo& indexInfo) const {
    AFL_VERIFY(Serializer);
    AFL_VERIFY(data.size());
    std::vector<TChunkedColumnReader> columnReaders;
    for (auto&& i : ColumnIds) {
        auto it = data.find(i);
        AFL_VERIFY(it != data.end());
        columnReaders.emplace_back(it->second, indexInfo.GetColumnLoaderVerified(i));
    }
    ui32 recordsCount = 0;
    for (auto&& i : data.begin()->second) {
        recordsCount += i->GetRecordsCountVerified();
    }
    TChunkedBatchReader reader(std::move(columnReaders));
    std::shared_ptr<arrow::RecordBatch> indexBatch = DoBuildIndexImpl(reader);
    const TString indexData = Serializer->SerializeFull(indexBatch);
    return std::make_shared<TPortionIndexChunk>(indexId, recordsCount, NArrow::GetBatchDataSize(indexBatch), indexData);
}

bool TIndexByColumns::DoDeserializeFromProto(const NKikimrSchemeOp::TOlapIndexDescription& /*proto*/) {
    Serializer = NArrow::NSerialization::TSerializerContainer::GetDefaultSerializer();
    return true;
}

TIndexByColumns::TIndexByColumns(const ui32 indexId, const TString& indexName, const std::set<ui32>& columnIds)
    : TBase(indexId, indexName)
    , ColumnIds(columnIds)
{
    Serializer = NArrow::NSerialization::TSerializerContainer::GetDefaultSerializer();
}

NKikimr::TConclusionStatus TIndexByColumns::CheckSameColumnsForModification(const IIndexMeta& newMeta) const {
    const auto* bMeta = dynamic_cast<const TIndexByColumns*>(&newMeta);
    if (!bMeta) {
        return TConclusionStatus::Fail("cannot read meta as appropriate class: " + GetClassName() + ". Meta said that class name is " + newMeta.GetClassName());
    }
    if (bMeta->ColumnIds.size() != ColumnIds.size()) {
        return TConclusionStatus::Fail("columns count is different");
    }
    for (auto&& i : bMeta->ColumnIds) {
        if (!ColumnIds.contains(i)) {
            return TConclusionStatus::Fail("columns set is different or column was recreated in database");
        }
    }
    return TConclusionStatus::Success();
}

bool IIndexMeta::DeserializeFromProto(const NKikimrSchemeOp::TOlapIndexDescription& proto) {
    IndexId = proto.GetId();
    AFL_VERIFY(IndexId);
    IndexName = proto.GetName();
    AFL_VERIFY(IndexName);
    StorageId = proto.GetStorageId() ? proto.GetStorageId() : IStoragesManager::DefaultStorageId;
    return DoDeserializeFromProto(proto);
}

}   // namespace NKikimr::NOlap::NIndexes