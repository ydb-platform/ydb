#include "meta.h"

#include <ydb/core/formats/arrow/accessor/plain/accessor.h>
#include <ydb/core/formats/arrow/size_calcer.h>
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>
#include <ydb/core/tx/columnshard/engines/storage/chunks/column.h>
#include <ydb/core/tx/columnshard/engines/storage/chunks/data.h>

#include <ydb/library/formats/arrow/simple_arrays_cache.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD

namespace NKikimr::NOlap::NIndexes {

TConclusion<std::vector<std::shared_ptr<NChunks::TPortionIndexChunk>>> TIndexByColumns::DoBuildIndexOptional(
    const THashMap<ui32, std::vector<std::shared_ptr<IPortionDataChunk>>>& data, const ui32 recordsCount, const TIndexInfo& indexInfo) const {
    AFL_VERIFY(Serializer);
    AFL_VERIFY(data.size());
    std::vector<TChunkedColumnReader> columnReaders;
    for (auto&& i : ColumnIds) {
        auto it = data.find(i);
        if (it == data.end()) {
            YDB_LOG_WARN("",
                {"event", "index_data_absent"},
                {"columnId", i},
                {"indexName", GetIndexName()},
                {"indexId", GetIndexId()});
            const auto fill = indexInfo.GetColumnExternalDefaultValueVerified(i);
            if (fill) {
                // A portion written without this column reads it as the schema default. Index the same
                // constant: an all-zero index would skip rows that are equal to the default.
                const auto loader = indexInfo.GetColumnLoaderVerified(i);
                const auto& features = indexInfo.GetColumnFeaturesVerified(i);
                auto array = NArrow::TThreadSimpleArraysCache::Get(features.GetArrowField()->type(), fill, recordsCount);
                auto trivial = std::make_shared<NArrow::NAccessor::TTrivialArray>(array);
                const auto loadContext = loader->BuildAccessorContext(recordsCount);
                auto constructed = loader->GetAccessorConstructor()->Construct(trivial, loadContext);
                if (constructed.IsFail()) {
                    return TConclusionStatus::Fail(constructed.GetErrorMessage());
                }
                auto blobAndMeta = loader->GetAccessorConstructor()->SerializeToBlobAndMeta(*constructed, loadContext);
                std::vector<std::shared_ptr<IPortionDataChunk>> constChunks = { std::make_shared<NChunks::TChunkPreparation>(
                    std::move(blobAndMeta.Blob), *constructed, TChunkAddress(i, 0), features, std::move(blobAndMeta.Meta)) };
                columnReaders.emplace_back(constChunks, loader);
                continue;
            }
            // Absent column with an empty fill is read as NULL. An all-zero index skips every value.
            TString indexData(1, '\0');
            return std::vector<std::shared_ptr<NChunks::TPortionIndexChunk>>(
                { std::make_shared<NChunks::TPortionIndexChunk>(TChunkAddress(GetIndexId(), 0), recordsCount, indexData.size(), indexData) });
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

TIndexByColumns::TIndexByColumns(const ui32 indexId, const TString& indexName, const ui32 columnId, const TString& storageId,
    const bool inheritPortionStorage, const TReadDataExtractorContainer& extractor)
    : TBase(indexId, indexName, storageId, inheritPortionStorage)
    , DataExtractor(extractor)
    , ColumnIds({ columnId })
{
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
