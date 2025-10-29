#pragma once

#include "consumer.h"

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <ydb/core/fq/libs/row_dispatcher/events/data_plane.h>
#include <ydb/core/fq/libs/row_dispatcher/format_handler/common/common.h>

namespace NFq::NRowDispatcher {

class ITopicFilters : public TThrRefBase, public TNonCopyable {
public:
    using TPtr = TIntrusivePtr<ITopicFilters>;

public:
    // columnIndex - mapping from stable column id to index in values array
    virtual void ProcessData(const TVector<ui64>& columnIndex, const TVector<ui64>& offsets, const TVector<std::span<NYql::NUdf::TUnboxedValue>>& values, ui64 numberRows) = 0;
    virtual void OnCompileResponse(TEvRowDispatcher::TEvPurecalcCompileResponse::TPtr& ev) = 0;

    virtual TStatus AddPrograms(IProcessedDataConsumer::TPtr consumer, IProgramHolder::TPtr programHolder) = 0;
    virtual void RemoveProgram(NActors::TActorId clientId) = 0;

    virtual void FillStatistics(TFiltersStatistic&) = 0;
};

struct TTopicFiltersConfig {
    NActors::TActorId CompileServiceId;
};

ITopicFilters::TPtr CreateTopicFilters(NActors::TActorId owner, TTopicFiltersConfig config, NMonitoring::TDynamicCounterPtr counters);

}  // namespace NFq::NRowDispatcher
