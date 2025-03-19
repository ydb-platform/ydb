#pragma once

#include "purecalc_filter.h"

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <ydb/core/fq/libs/row_dispatcher/format_handler/common/common.h>

namespace NFq::NRowDispatcher {

class IFilteredDataConsumer : public IPurecalcFilterConsumer {
public:
    using TPtr = TIntrusivePtr<IFilteredDataConsumer>;

public:
    virtual NActors::TActorId GetFilterId() const = 0;
    virtual const TVector<ui64>& GetColumnIds() const = 0;
    virtual std::optional<ui64> GetNextMessageOffset() const = 0;

    virtual void OnFilteredBatch(ui64 firstRow, ui64 lastRow) = 0;  // inclusive interval [firstRow, lastRow]

    virtual void OnFilterStarted() = 0;
    virtual void OnFilteringError(TStatus status) = 0;
};

class ITopicFilters : public TThrRefBase, public TNonCopyable {
public:
    using TPtr = TIntrusivePtr<ITopicFilters>;

public:
    // columnIndex - mapping from stable column id to index in values array
    virtual void FilterData(const TVector<ui64>& columnIndex, const TVector<ui64>& offsets, const TVector<const TVector<NYql::NUdf::TUnboxedValue>*>& values, ui64 numberRows) = 0;
    virtual void OnCompileResponse(TEvRowDispatcher::TEvPurecalcCompileResponse::TPtr ev) = 0;

    virtual TStatus AddFilter(IFilteredDataConsumer::TPtr filter) = 0;
    virtual void RemoveFilter(NActors::TActorId filterId) = 0;

    virtual TFiltersStatistic GetStatistics() = 0;
};

struct TTopicFiltersConfig {
    NActors::TActorId CompileServiceId;
};

ITopicFilters::TPtr CreateTopicFilters(NActors::TActorId owner, const TTopicFiltersConfig& config, NMonitoring::TDynamicCounterPtr counters);

}  // namespace NFq::NRowDispatcher
