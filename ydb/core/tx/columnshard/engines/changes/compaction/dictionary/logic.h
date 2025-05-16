#pragma once
#include <ydb/core/formats/arrow/accessor/abstract/accessor.h>
#include <ydb/core/formats/arrow/accessor/common/const.h>
#include <ydb/core/formats/arrow/accessor/dictionary/accessor.h>
#include <ydb/core/formats/arrow/accessor/sparsed/accessor.h>
#include <ydb/core/tx/columnshard/engines/changes/compaction/abstract/merger.h>
#include <ydb/core/tx/columnshard/engines/storage/chunks/column.h>

namespace NKikimr::NOlap::NCompaction::NDictionary {

class TIterator: public IColumnMerger::TBaseIterator<NArrow::NAccessor::TDictionaryArray> {
private:
    using TBase = IColumnMerger::TBaseIterator<NArrow::NAccessor::TDictionaryArray>;
    std::optional<arrow::Type::type> CurrentRecordsType;
    virtual void OnInitArray(const std::shared_ptr<NArrow::NAccessor::TDictionaryArray>& arr) override {
        CurrentRecordsType = arr->GetRecords()->type()->id();
    }

public:
    arrow::Type::type GetCurrentRecordsType() const {
        AFL_VERIFY(!!CurrentRecordsType);
        return *CurrentRecordsType;
    }

    using TBase::TBase;
};

class TMerger: public IColumnMerger {
private:
    using TBase = IColumnMerger;

    static inline auto Registrator = TFactory::TRegistrator<TMerger>(NArrow::NAccessor::TGlobalConst::DictionaryAccessorName);
    std::vector<TIterator> Iterators;
    std::vector<std::vector<ui32>> RemapIndexes;
    std::shared_ptr<arrow::Array> ArrayVariantsFull;

    virtual void DoStart(const std::vector<std::shared_ptr<NArrow::NAccessor::IChunkedArray>>& input, TMergingContext& mergeContext) override;
    virtual TColumnPortionResult DoExecute(const TChunkMergeContext& context, TMergingContext& mergeContext) override;

public:
    using TBase::TBase;
};

}   // namespace NKikimr::NOlap::NCompaction::NDictionary
