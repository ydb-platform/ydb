#include "meta.h"

#include <ydb/core/formats/arrow/size_calcer.h>
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>
#include <ydb/core/tx/columnshard/engines/scheme/versions/abstract_scheme.h>
#include <ydb/core/tx/columnshard/engines/storage/chunks/column.h>
#include <ydb/core/tx/columnshard/engines/storage/chunks/data.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD

namespace NKikimr::NOlap::NIndexes {

TConclusion<std::vector<std::shared_ptr<NChunks::TPortionIndexChunk>>> TIndexByColumns::DoBuildIndexOptional(
    const THashMap<ui32, std::vector<std::shared_ptr<IPortionDataChunk>>>& data, const ui32 recordsCount, const TIndexInfo& indexInfo) const {
    YDB_LOG_VERIFY(Serializer);
    YDB_LOG_VERIFY(data.size());
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
                const auto loader = indexInfo.GetColumnLoaderVerified(i);
                const auto& features = indexInfo.GetColumnFeaturesVerified(i);
                THashMap<ui32, std::pair<std::shared_ptr<NArrow::NAccessor::IChunkedArray>, NArrow::NAccessor::TBlobWithAdditionalAccessorData>>
                    serializedBySize;
                std::vector<std::shared_ptr<IPortionDataChunk>> constChunks;
                ui16 chunkIdx = 0;
                for (auto&& chunk : ISnapshotSchema::BuildAbsentColumnChunks(features.GetArrowField()->type(), fill, recordsCount)) {
                    auto it = serializedBySize.find(chunk->GetRecordsCount());
                    if (it == serializedBySize.end()) {
                        const auto loadContext = loader->BuildAccessorContext(chunk->GetRecordsCount());
                        auto constructed = loader->GetAccessorConstructor()->Construct(chunk, loadContext);
                        if (constructed.IsFail()) {
                            return TConclusionStatus::Fail(constructed.GetErrorMessage());
                        }
                        auto blobAndMeta = loader->GetAccessorConstructor()->SerializeToBlobAndMeta(*constructed, loadContext);
                        it = serializedBySize.emplace(chunk->GetRecordsCount(), std::make_pair(*constructed, std::move(blobAndMeta))).first;
                    }
                    constChunks.emplace_back(std::make_shared<NChunks::TChunkPreparation>(
                        it->second.second.Blob, it->second.first, TChunkAddress(i, chunkIdx++), features, it->second.second.Meta));
                }
                columnReaders.emplace_back(constChunks, loader);
                continue;
            }
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
