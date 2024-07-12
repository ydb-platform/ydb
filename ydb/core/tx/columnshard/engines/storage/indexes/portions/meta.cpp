#include "meta.h"
#include <ydb/core/tx/columnshard/engines/storage/chunks/data.h>
#include <ydb/core/tx/columnshard/engines/portions/constructor.h>
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>
#include <ydb/core/formats/arrow/size_calcer.h>

namespace NKikimr::NOlap::NIndexes {

void TPortionIndexChunk::DoAddIntoPortionBeforeBlob(
    const TBlobRangeLink16& bRange, TPortionInfoConstructor& portionInfo) const {
    AFL_VERIFY(!bRange.IsValid());
    portionInfo.AddIndex(TIndexChunk(GetEntityId(), GetChunkIdxVerified(), RecordsCount, RawBytes, bRange));
}

void TPortionIndexChunk::DoAddInplaceIntoPortion(TPortionInfoConstructor& portionInfo) const {
    portionInfo.AddIndex(TIndexChunk(GetEntityId(), GetChunkIdxVerified(), RecordsCount, RawBytes, GetData()));
}

std::shared_ptr<NKikimr::NOlap::IPortionDataChunk> TIndexByColumns::DoBuildIndex(
    const THashMap<ui32, std::vector<std::shared_ptr<IPortionDataChunk>>>& data, const TIndexInfo& indexInfo) const {
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
    const TString indexData = DoBuildIndexImpl(reader);
    return std::make_shared<NChunks::TPortionIndexChunk>(TChunkAddress(GetIndexId(), 0), recordsCount, indexData.size(), indexData);
}

bool TIndexByColumns::DoDeserializeFromProto(const NKikimrSchemeOp::TOlapIndexDescription& /*proto*/) {
    Serializer = NArrow::NSerialization::TSerializerContainer::GetDefaultSerializer();
    return true;
}

TIndexByColumns::TIndexByColumns(const ui32 indexId, const TString& indexName, const std::set<ui32>& columnIds, const TString& storageId)
    : TBase(indexId, indexName, storageId)
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

}   // namespace NKikimr::NOlap::NIndexes