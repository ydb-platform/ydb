#include "meta.h"

#include <ydb/core/formats/arrow/size_calcer.h>
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>
#include <ydb/core/tx/columnshard/engines/storage/chunks/data.h>

namespace NKikimr::NOlap::NIndexes {

TConclusion<std::vector<std::shared_ptr<IPortionDataChunk>>> TIndexByColumns::DoBuildIndexOptional(
    const THashMap<ui32, std::vector<std::shared_ptr<IPortionDataChunk>>>& data, const ui32 recordsCount, const TIndexInfo& indexInfo) const {
    AFL_VERIFY(Serializer);
    AFL_VERIFY(data.size());
    std::vector<TChunkedColumnReader> columnReaders;
    for (auto&& i : ColumnIds) {
        auto it = data.find(i);
        if (it == data.end()) {
            AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "index_data_absent")("column_id", i)("index_name", GetIndexName())(
                "index_id", GetIndexId());
            return std::vector<std::shared_ptr<IPortionDataChunk>>();
        }
        columnReaders.emplace_back(it->second, indexInfo.GetColumnLoaderVerified(i));
    }
    TChunkedBatchReader reader(std::move(columnReaders));
    return DoBuildIndexImpl(reader, recordsCount);
}

bool TIndexByColumns::DoDeserializeFromProto(const NKikimrSchemeOp::TOlapIndexDescription& /*proto*/) {
    Serializer = NArrow::NSerialization::TSerializerContainer::GetDefaultSerializer();
    return true;
}

TIndexByColumns::TIndexByColumns(
    const ui32 indexId, const TString& indexName, const ui32 columnId, const TString& storageId, const TReadDataExtractorContainer& extractor)
    : TBase(indexId, indexName, storageId)
    , DataExtractor(extractor)
    , ColumnIds({ columnId }) {
    Serializer = NArrow::NSerialization::TSerializerContainer::GetDefaultSerializer();
}

NKikimr::TConclusionStatus TIndexByColumns::CheckSameColumnsForModification(const IIndexMeta& newMeta) const {
    const auto* bMeta = dynamic_cast<const TIndexByColumns*>(&newMeta);
    if (!bMeta) {
        return TConclusionStatus::Fail(
            "cannot read meta as appropriate class: " + GetClassName() + ". Meta said that class name is " + newMeta.GetClassName());
    }
    if (bMeta->ColumnIds.size() != 1) {
        return TConclusionStatus::Fail("one column per index is necessary");
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
