#pragma once
#include "abstract.h"
#include "functions.h"
#include "kernel_logic.h"

namespace NKikimr::NArrow::NSSA {

class TOriginalColumnProcessor: public IResourceProcessor {
private:
    using TBase = IResourceProcessor;

    YDB_ACCESSOR(ui32, ColumnId, 0);
    YDB_ACCESSOR_DEF(TString, ColumnName);

    virtual TConclusionStatus DoExecute(
        const std::shared_ptr<TAccessorsCollection>& /*resources*/, const TProcessorContext& /*context*/) const override {
        AFL_VERIFY(false);
        return TConclusionStatus::Success();
    }

    virtual bool IsAggregation() const override {
        return false;
    }

public:
    TOriginalColumnProcessor(const ui32 columnId, const TString& columnName)
        : TBase({}, { columnId }, EProcessorType::Original)
        , ColumnName(columnName) {
        AFL_VERIFY(!!ColumnName);
    }

    virtual std::optional<TFetchingInfo> BuildFetchTask(const ui32 columnId, const NAccessor::IChunkedArray::EType arrType,
        const std::shared_ptr<TAccessorsCollection>& resources) const override {
        AFL_VERIFY(false);
        return TBase::BuildFetchTask(columnId, arrType, resources);
    }
};

}   // namespace NKikimr::NArrow::NSSA
