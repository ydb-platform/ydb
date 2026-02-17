#include "vhost_stats_simple.h"

namespace NYdb::NBS::NBlockStore {

void TVHostStatsSimple::RequestStarted(
    TLog& log,
    TMetricRequest& metricRequest,
    TCallContext& callContext)
{
    Y_UNUSED(log);
    Y_UNUSED(callContext);

    auto& stat = AccessStats(metricRequest.RequestType);
    stat.InflightCount.fetch_add(1);
    stat.InflightBytes.fetch_add(metricRequest.GetRequestBytes());
}

void TVHostStatsSimple::RequestCompleted(
    TLog& log,
    TMetricRequest& metricRequest,
    TCallContext& callContext,
    const NProto::TError& error)
{
    Y_UNUSED(log);
    Y_UNUSED(callContext);
    Y_UNUSED(error);

    auto& stat = AccessStats(metricRequest.RequestType);
    if (error.GetCode() == S_OK) {
        stat.ExecuteCount.fetch_add(1);
        stat.ExecuteBytes.fetch_add(metricRequest.GetRequestBytes());
    }
    stat.InflightCount.fetch_sub(1);
    stat.InflightBytes.fetch_sub(metricRequest.GetRequestBytes());
}

const TVHostStatsSimple::TStat& TVHostStatsSimple::GetStats(
    EBlockStoreRequest requestType) const
{
    return Stats[static_cast<size_t>(requestType)];
}

TVHostStatsSimple::TStat& TVHostStatsSimple::AccessStats(
    EBlockStoreRequest requestType)
{
    return Stats[static_cast<size_t>(requestType)];
}

}   // namespace NYdb::NBS::NBlockStore
