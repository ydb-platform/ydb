#pragma once

#include <ydb/core/kqp/common/kqp_streaming_query_controller.h>
#include <ydb/core/kqp/executer_actor/kqp_executer_stats.h>
#include <ydb/core/kqp/gateway/kqp_gateway.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <util/generic/ptr.h>

namespace NKikimr::NKqp {

TVector<TPqTopicResolverSource> CollectPqSources(
    const TVector<IKqpGateway::TPhysicalTxData>& transactions, const TString& database);

constexpr TDuration StreamingQueryUpdateCountersPeriod = TDuration::Seconds(1);

struct IStreamingQueryCounters : public TThrRefBase {

    virtual ~IStreamingQueryCounters() = default;

    virtual void Update(const TAggExecStat& stats) = 0;
};

TIntrusivePtr<IStreamingQueryCounters> MakeStreamingQueryCounters(const ::NMonitoring::TDynamicCounterPtr& counters, const TString& path);

} // namespace NKikimr::NKqp
