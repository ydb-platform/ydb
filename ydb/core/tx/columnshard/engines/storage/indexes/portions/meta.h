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

    virtual TConclusion<std::shared_ptr<IPortionDataChunk>> DoBuildIndexOptional(const THashMap<ui32, std::vector<std::shared_ptr<IPortionDataChunk>>>& data,
        const ui32 recordsCount, const TIndexInfo& indexInfo) const override final;
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


class TSkipIndex: public TIndexByColumns {
private:
    using TBase = TIndexByColumns;
public:
    using EOperation = NArrow::NSSA::EIndexCheckOperation;

private:
    virtual bool DoIsAppropriateFor(const TString& subColumnName, const EOperation op) const = 0;
    virtual bool DoCheckValue(
        const TString& data, const std::optional<ui64> cat, const std::shared_ptr<arrow::Scalar>& value, const EOperation op) const = 0;

public:
    bool CheckValue(const TString& data, const std::optional<ui64> cat, const std::shared_ptr<arrow::Scalar>& value, const EOperation op) const {
        return DoCheckValue(data, cat, value, op);
    }

    virtual bool IsSkipIndex() const override final {
        return true;
    }

    bool IsAppropriateFor(const NRequest::TOriginalDataAddress& addr, const EOperation op) const {
        if (GetColumnId() != addr.GetColumnId()) {
            return false;
        }
        return DoIsAppropriateFor(addr.GetSubColumnName(), op);
    }
    using TBase::TBase;
};

}   // namespace NKikimr::NOlap::NIndexes
