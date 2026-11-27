#pragma once

#include "public.h"

#include <ydb/core/nbs/cloud/blockstore/compat/libs/diagnostics/public.h>
#include <ydb/core/nbs/cloud/blockstore/compat/libs/service/request.h>
#include <ydb/core/nbs/cloud/blockstore/libs/common/public.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/scheduler.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/timer.h>
#include <ydb/core/nbs/cloud/storage/core/libs/diagnostics/public.h>

#include <util/datetime/base.h>

namespace NCloud::NBlockStore::NClient {

using NYdb::NBS::ILoggingServicePtr;
using NYdb::NBS::ISchedulerPtr;
using NYdb::NBS::ITimerPtr;

////////////////////////////////////////////////////////////////////////////////

struct TRetryState
{
    const TInstant Started = TInstant::Now();

    TDuration RetryTimeout;
    ui32 Retries = 0;
    bool DoneInstantRetry = false;
};

////////////////////////////////////////////////////////////////////////////////

struct TRetrySpec
{
    bool ShouldRetry = false;
    bool IsRetriableError = false;
    TDuration Backoff;
};

////////////////////////////////////////////////////////////////////////////////

struct IRetryPolicy
{
    virtual ~IRetryPolicy() = default;

    virtual TRetrySpec ShouldRetry(
        TRetryState& state,
        const NProto::TError& error) = 0;
};

////////////////////////////////////////////////////////////////////////////////

IRetryPolicyPtr CreateRetryPolicy(
    TClientAppConfigPtr config,
    std::optional<NProto::EStorageMediaKind> mediaKind);

IBlockStorePtr CreateDurableClient(
    TClientAppConfigPtr config,
    IBlockStorePtr client,
    IRetryPolicyPtr retryPolicy,
    ILoggingServicePtr logging,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    IRequestStatsPtr requestStats,
    IVolumeStatsPtr volumeStats);

}   // namespace NCloud::NBlockStore::NClient
