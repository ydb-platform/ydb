#include "stats_helpers.h"

#include <ydb/core/nbs/cloud/blockstore/compat/libs/service/request.h>
#include <ydb/core/nbs/cloud/blockstore/compat/libs/service/request_helpers.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

TRequestCounters MakeRequestCounters(
    ITimerPtr timer,
    TRequestCounters::EOptions options,
    EHistogramCounterOptions histogramCounterOptions,
    const TVector<TSizeInterval>& executionTimeSizeClasses)
{
    return TRequestCounters(
        std::move(timer),
        BlockStoreRequestsCount,
        [](TRequestCounters::TRequestType t)
        {
            Y_DEBUG_ABORT_UNLESS(t < BlockStoreRequestsCount);
            const auto bt = static_cast<EBlockStoreRequest>(t);
            return GetBlockStoreRequestName(bt);
        },
        [](TRequestCounters::TRequestType t)
        {
            Y_DEBUG_ABORT_UNLESS(t < BlockStoreRequestsCount);
            const auto bt = static_cast<EBlockStoreRequest>(t);
            return IsNonLocalReadWriteRequest(bt);
        },
        [](TRequestCounters::TRequestType t)
        {
            Y_DEBUG_ABORT_UNLESS(t < BlockStoreRequestsCount);
            const auto bt = static_cast<EBlockStoreRequest>(t);
            return bt == EBlockStoreRequest::StartEndpoint;
        },
        options,
        histogramCounterOptions,
        executionTimeSizeClasses);
}

}   // namespace NCloud::NBlockStore
