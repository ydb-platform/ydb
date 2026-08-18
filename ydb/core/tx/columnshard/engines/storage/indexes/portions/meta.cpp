#include "meta.h"

#include <ydb/core/formats/arrow/size_calcer.h>
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>
#include <ydb/core/tx/columnshard/engines/storage/chunks/data.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD

namespace NKikimr::NOlap::NIndexes {

TConclusion<std::vector<std::shared_ptr<NChunks::TPortionIndexChunk>>> TIndexByColumns::DoBuildIndexOptional(
    const THashMap<ui32, std::vector<std::shared_ptr<IPortionDataChunk>>>& data, const ui32 recordsCount, const TIndexInfo& indexInfo,
    const std::optional<ui64> chunkSizeLimit) const {
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
            // Possible situation during a merge operation when a column is added to the table in the new schema
            // indexData can't be empty in this case, because merger saves it, so set it to 0 (skip all values)
            TString indexData(1, '\0');
            return std::vector<std::shared_ptr<NChunks::TPortionIndexChunk>>(
                { std::make_shared<NChunks::TPortionIndexChunk>(TChunkAddress(GetIndexId(), 0), recordsCount, indexData.size(), indexData) });
        }
        columnReaders.emplace_back(it->second, indexInfo.GetColumnLoaderVerified(i));
    }
    TChunkedBatchReader reader(std::move(columnReaders));
    // Per-source-chunk emission: one index chunk per column chunk, each bounded by chunkSizeLimit, applied by
    // the scan to its own record range. Only index types implementing DoBuildIndexChunkData participate (the
    // first std::nullopt falls back to the whole-portion build); inplace indexes get no limit and stay single.
    if (chunkSizeLimit && reader.GetColumnsCount() == 1) {
        std::vector<std::shared_ptr<NChunks::TPortionIndexChunk>> result;
        ui32 chunkIdx = 0;
        bool supported = true;
        for (reader.Start(); reader.IsCorrect(); reader.ReadNext(reader.begin()->GetCurrentChunk()->GetRecordsCount())) {
            const auto& columnChunk = reader.begin()->GetCurrentChunk();
            const ui32 chunkRecords = columnChunk->GetRecordsCount();
            std::optional<TString> indexData = DoBuildIndexChunkData(columnChunk, chunkRecords, *chunkSizeLimit);
            if (!indexData) {
                AFL_VERIFY(result.empty())("index_id", GetIndexId());
                supported = false;
                break;
            }
            result.emplace_back(std::make_shared<NChunks::TPortionIndexChunk>(
                TChunkAddress(GetIndexId(), chunkIdx++), chunkRecords, indexData->size(), std::move(*indexData)));
        }
        if (supported) {
            return result;
        }
    }
    return DoBuildIndexImpl(reader, recordsCount, chunkSizeLimit);
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
