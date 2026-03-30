#pragma once

#include "context.h"

#include <ydb/core/formats/arrow/reader/batch_iterator.h>

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering {
    
class TFilterAccumulator: TMoveOnly {
public:
    enum class EFetchingStage {
        FILTERS = 0,
        ACCESSORS = 1,
        COLUMN_DATA = 2,
    };

private:
    const TEvRequestFilter::TPtr OriginalRequest;
    bool Done = false;
    std::shared_ptr<NColumnShard::TDuplicateFilteringCounters> Counters;
    TInstant StartTime;

public:
    TFilterAccumulator(const TEvRequestFilter::TPtr& request, std::shared_ptr<NColumnShard::TDuplicateFilteringCounters> counters);
    ~TFilterAccumulator();

    void AddFilter(NArrow::TColumnFilter&& filter);
    bool IsDone() const;
    void Abort(const TString& error);
    const TEvRequestFilter::TPtr& GetRequest() const;
    TString DebugString() const;
};

class TFiltersBuilder {
private:
    struct TFilterInfo {
        ui64 RowsCount = 0;
        NArrow::TColumnFilter Filter;
    };

    THashMap<ui64, TFilterInfo> Filters;
    THashMap<ui64, std::shared_ptr<TFilterAccumulator>> WaitingPortions;
    YDB_READONLY(ui64, RowsAdded, 0);
    YDB_READONLY(ui64, RowsSkipped, 0);

    void AddImpl(const ui64 portionId, const bool value);

public:
    TFiltersBuilder() = default;

    void AddRecord(const NArrow::NMerger::TBatchIterator& cursor);
    void SkipRecord(const NArrow::NMerger::TBatchIterator& cursor);
    void ValidateDataSchema(const std::shared_ptr<arrow::Schema>& /*schema*/) const;
    bool IsBufferExhausted() const;
    bool NotifyReadyFilter(std::shared_ptr<TFilterAccumulator>& constructor);
    void AddSource(const ui64 portionId, ui64 rowsCount);
    void AddWaitingPortion(const ui64 portionId, std::shared_ptr<TFilterAccumulator>& constructor);
    void Abort(const TString& error);
};

}