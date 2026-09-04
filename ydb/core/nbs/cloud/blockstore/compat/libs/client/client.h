#pragma once

#include "public.h"

#include <ydb/core/nbs/cloud/blockstore/compat/libs/diagnostics/public.h>
#include <ydb/core/nbs/cloud/blockstore/compat/libs/service/request.h>
#include <ydb/core/nbs/cloud/blockstore/libs/common/public.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/scheduler.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/startable.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/timer.h>
#include <ydb/core/nbs/cloud/storage/core/libs/diagnostics/public.h>
#include <ydb/core/nbs/cloud/storage/core/libs/grpc/public.h>

#include <library/cpp/threading/future/future.h>

namespace NCloud::NBlockStore::NClient {

using NYdb::NBS::ICertificateProviderPtr;
using NYdb::NBS::ILoggingServicePtr;
using NYdb::NBS::IMonitoringServicePtr;
using NYdb::NBS::ISchedulerPtr;
using NYdb::NBS::IStartable;
using NYdb::NBS::ITimerPtr;
using NYdb::NBS::TResultOrError;

////////////////////////////////////////////////////////////////////////////////

struct IClient: public IStartable
{
    virtual IBlockStorePtr CreateEndpoint() = 0;

    virtual IBlockStorePtr CreateDataEndpoint() = 0;

    virtual IBlockStorePtr CreateDataEndpoint(const TString& socketPath) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IMultiHostClient: public IStartable
{
    virtual IBlockStorePtr
    CreateEndpoint(const TString& host, ui32 port, bool isSecure) = 0;

    virtual IBlockStorePtr
    CreateDataEndpoint(const TString& host, ui32 port, bool isSecure) = 0;
};

////////////////////////////////////////////////////////////////////////////////

TResultOrError<IClientPtr> CreateClient(
    TClientAppConfigPtr config,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring,
    IServerStatsPtr clientStats,
    ICertificateProviderPtr certificateProvider);

TResultOrError<IMultiHostClientPtr> CreateMultiHostClient(
    TClientAppConfigPtr config,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring,
    IServerStatsPtr clientStats,
    ICertificateProviderPtr certificateProvider);

}   // namespace NCloud::NBlockStore::NClient
