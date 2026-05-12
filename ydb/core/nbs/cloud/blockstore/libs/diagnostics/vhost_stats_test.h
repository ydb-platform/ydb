#pragma once

#include "public.h"

#include "vhost_stats.h"

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

class TTestVHostStats final: public IVHostStats
{
public:
    using TRequestStartedHandler = std::function<void(
        TLog& log,
        TMetricRequest& metricRequest,
        TCallContext& callContext)>;
    using TRequestCompletedHandler = std::function<void(
        TLog& log,
        TMetricRequest& metricRequest,
        TCallContext& callContext,
        const NProto::TError& error)>;

    TRequestStartedHandler RequestStartedHandler;
    TRequestCompletedHandler RequestCompletedHandler;

    void RequestStarted(
        TLog& log,
        TMetricRequest& metricRequest,
        TCallContext& callContext) override;

    void RequestCompleted(
        TLog& log,
        TMetricRequest& metricRequest,
        TCallContext& callContext,
        const NProto::TError& error) override;
};

}   // namespace NYdb::NBS::NBlockStore
