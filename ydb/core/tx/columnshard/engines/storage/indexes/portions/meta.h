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

    virtual TString DoBuildIndexImpl(TChunkedBatchReader& reader, const ui32 recordsCount) const = 0;

    virtual TConclusion<std::shared_ptr<IPortionDataChunk>> DoBuildIndexOptional(
        const THashMap<ui32, std::vector<std::shared_ptr<IPortionDataChunk>>>& data, const ui32 recordsCount,
        const TIndexInfo& indexInfo) const override final;
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

    const std::set<ui32>& GetColumnIds() const {
        return ColumnIds;
    }
    TIndexByColumns() = default;
    TIndexByColumns(const ui32 indexId, const TString& indexName, const ui32 columnId, const TString& storageId,
        const TReadDataExtractorContainer& extractor);
};

}   // namespace NKikimr::NOlap::NIndexes
