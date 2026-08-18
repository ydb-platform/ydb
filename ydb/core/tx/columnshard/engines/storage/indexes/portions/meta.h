#pragma once
#include "extractor/abstract.h"

#include <ydb/core/tx/columnshard/engines/scheme/abstract/index_info.h>
#include <ydb/core/tx/columnshard/engines/scheme/indexes/abstract/meta.h>
#include <ydb/core/tx/columnshard/splitter/abstract/chunks.h>

namespace NKikimr::NOlap::NIndexes {

class TIndexByColumns: public IIndexMeta {
private:
    using TBase = IIndexMeta;
    std::shared_ptr<NArrow::NSerialization::ISerializer> Serializer;
    TReadDataExtractorContainer DataExtractor;
    std::set<ui32> ColumnIds;

protected:
    const TReadDataExtractorContainer& GetDataExtractor() const {
        return DataExtractor;
    }

    TReadDataExtractorContainer& MutableDataExtractor() {
        return DataExtractor;
    }

    virtual std::vector<std::shared_ptr<NChunks::TPortionIndexChunk>> DoBuildIndexImpl(
        TChunkedBatchReader& reader, const ui32 recordsCount, const std::optional<ui64> chunkSizeLimit) const = 0;

    virtual TConclusion<std::vector<std::shared_ptr<NChunks::TPortionIndexChunk>>> DoBuildIndexOptional(
        const THashMap<ui32, std::vector<std::shared_ptr<IPortionDataChunk>>>& data, const ui32 recordsCount, const TIndexInfo& indexInfo,
        const std::optional<ui64> chunkSizeLimit) const override final;
    virtual bool DoDeserializeFromProto(const NKikimrSchemeOp::TOlapIndexDescription& proto) override;

    TConclusionStatus CheckSameColumnsForModification(const IIndexMeta& newMeta) const;

public:
    void AddColumnId(const ui32 columnId) {
        AFL_VERIFY(ColumnIds.emplace(columnId).second);
        AFL_VERIFY(ColumnIds.size() == 1);
    }

    ui32 GetColumnId() const {
        AFL_VERIFY(ColumnIds.size() == 1)("size", ColumnIds.size());
        return *ColumnIds.begin();
    }

    std::optional<ui32> GetSingleColumnId() const override {
        if (ColumnIds.size() == 1) {
            return *ColumnIds.begin();
        }

        return std::nullopt;
    }

    const std::set<ui32>& GetColumnIds() const {
        return ColumnIds;
    }

    TIndexByColumns() = default;
    TIndexByColumns(const ui32 indexId, const TString& indexName, const ui32 columnId, const TString& storageId,
        const bool inheritPortionStorage, const TReadDataExtractorContainer& extractor);
};

// Groups pre-collected source chunks ((chunk, recordsCount) pairs) into consecutive batches of at most
// maxRecordsPerChunk records and emits one index chunk per batch built by buildChunkData(chunks, begin, end,
// batchRecords). Shared by indexes that split an oversized payload by record subranges; the scan applies
// every produced chunk to its own record range.
template <class TChunks, class TBuildChunkData>
std::vector<std::shared_ptr<NChunks::TPortionIndexChunk>> BuildIndexChunksBatched(
    const ui32 indexId, const TChunks& chunks, const ui32 maxRecordsPerChunk, const TBuildChunkData& buildChunkData) {
    std::vector<std::shared_ptr<NChunks::TPortionIndexChunk>> result;
    ui32 chunkIdx = 0;
    for (ui32 pos = 0; pos < chunks.size();) {
        ui32 batchRecords = chunks[pos].second;
        ui32 end = pos + 1;
        while (end < chunks.size() && batchRecords + chunks[end].second <= maxRecordsPerChunk) {
            batchRecords += chunks[end].second;
            ++end;
        }
        TString indexData = buildChunkData(chunks, pos, end, batchRecords);
        result.emplace_back(
            std::make_shared<NChunks::TPortionIndexChunk>(TChunkAddress(indexId, chunkIdx++), batchRecords, indexData.size(), indexData));
        pos = end;
    }
    return result;
}

}   // namespace NKikimr::NOlap::NIndexes
