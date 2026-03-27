#pragma once

#include "vhost_stats.h"

#include <ydb/core/nbs/cloud/blockstore/libs/service/request.h>

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

class TVHostStatsSimple: public IVHostStats
{
public:
    struct TStat
    {
        std::atomic<ui64> ExecuteCount = 0;
        std::atomic<ui64> InflightCount = 0;
        std::atomic<ui64> ExecuteBytes = 0;
        std::atomic<ui64> InflightBytes = 0;
    };

private:
    TStat Stats[static_cast<size_t>(EBlockStoreRequest::MAX)] = {};

public:
    ~TVHostStatsSimple() override = default;

    void RequestStarted(
        TLog& log,
        TMetricRequest& metricRequest,
        TCallContext& callContext) override;

    void RequestCompleted(
        TLog& log,
        TMetricRequest& metricRequest,
        TCallContext& callContext,
        const NProto::TError& error) override;

    const TStat& GetStats(EBlockStoreRequest requestType) const;

private:
    TStat& AccessStats(EBlockStoreRequest requestType);
};

using TVHostStatsSimplePtr = std::shared_ptr<TVHostStatsSimple>;

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
