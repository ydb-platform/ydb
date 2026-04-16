#include "vhost_stats_test.h"

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

void TTestVHostStats::RequestStarted(
    TLog& log,
    TMetricRequest& metricRequest,
    TCallContext& callContext)
{
    if (RequestStartedHandler) {
        RequestStartedHandler(log, metricRequest, callContext);
    }
}

void TTestVHostStats::RequestCompleted(
    TLog& log,
    TMetricRequest& metricRequest,
    TCallContext& callContext,
    const NProto::TError& error)
{
    if (RequestCompletedHandler) {
        RequestCompletedHandler(log, metricRequest, callContext, error);
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
