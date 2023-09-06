#pragma once
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_base.h>
#include <ydb/core/formats/arrow/arrow_filter.h>
#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NOlap {

class TFetchedData {
protected:
    YDB_READONLY_DEF(std::shared_ptr<arrow::RecordBatch>, Batch);
public:
    TFetchedData(const std::shared_ptr<arrow::RecordBatch>& batch)
        : Batch(batch)
    {
    }
};

class TFilterStageData: public TFetchedData {
private:
    using TBase = TFetchedData;
    YDB_READONLY_DEF(std::shared_ptr<NArrow::TColumnFilter>, AppliedFilter);
    YDB_READONLY_DEF(std::shared_ptr<NArrow::TColumnFilter>, NotAppliedEarlyFilter);
public:
    bool IsEmptyFilter() const {
        return (AppliedFilter && AppliedFilter->IsTotalDenyFilter()) || (NotAppliedEarlyFilter && NotAppliedEarlyFilter->IsTotalDenyFilter());
    }

    std::shared_ptr<NArrow::TColumnFilter> GetActualFilter() const {
        if (NotAppliedEarlyFilter) {
            return NotAppliedEarlyFilter;
        } else {
            return AppliedFilter;
        }
    }

    TFilterStageData(std::shared_ptr<NArrow::TColumnFilter> appliedFilter, std::shared_ptr<NArrow::TColumnFilter> earlyFilter, std::shared_ptr<arrow::RecordBatch> batch)
        : TBase(batch)
        , AppliedFilter(appliedFilter)
        , NotAppliedEarlyFilter(earlyFilter)
    {

    }
};

class TFetchStageData: public TFetchedData {
private:
    using TBase = TFetchedData;
public:
    using TBase::TBase;
};

}
