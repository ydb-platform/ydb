#pragma once

#include "events.h"

#include <ydb/core/formats/arrow/reader/batch_iterator.h>

namespace NKikimr::NOlap::NReader::NTrivial::NDuplicateFiltering {

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
    THashMap<ui64, NArrow::TColumnFilter> ReadyFilters;
    YDB_READONLY(ui64, RowsAdded, 0);
    YDB_READONLY(ui64, RowsSkipped, 0);

    void AddImpl(const ui64 portionId, const bool value);

public:
    TFiltersBuilder() = default;

    void AddRecord(const NArrow::NMerger::TBatchIterator& cursor);
    void SkipRecord(const NArrow::NMerger::TBatchIterator& cursor);
    void ValidateDataSchema(const std::shared_ptr<arrow::Schema>& /*schema*/) const;
    bool IsBufferExhausted() const;
    void AddSource(const ui64 portionId, ui64 rowsCount);
    TString DebugString() const;
    ui64 CountSources() const;

    THashMap<ui64, NArrow::TColumnFilter>&& ExtractReadyFilters();
};

class TFiltersStore {
private:
    const bool IsReverse;
    THashMap<ui64, std::shared_ptr<TFilterAccumulator>> WaitingPortions;
    THashMap<ui64, NArrow::TColumnFilter> ReadyFilters;
    std::shared_ptr<NColumnShard::TDuplicateFilteringCounters> Counters;

private:
     NArrow::TColumnFilter MakeOrderedFilter(NArrow::TColumnFilter&& filter);

public:
    TFiltersStore(const bool reverse, const std::shared_ptr<NColumnShard::TDuplicateFilteringCounters>& counters);
    bool NotifyReadyFilter(std::shared_ptr<TFilterAccumulator>& constructor);
    void AddReadyFilter(const ui64 portionId, NArrow::TColumnFilter&& filter);
    void AddWaitingPortion(const ui64 portionId, std::shared_ptr<TFilterAccumulator>& constructor);
    void Abort(const TString& error);
    
    ~TFiltersStore();
};

}