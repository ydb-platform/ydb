#pragma once

#include "public.h"

#include <ydb/core/nbs/cloud/blockstore/libs/diagnostics/public.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/public.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/affinity.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/startable.h>
#include <ydb/core/nbs/cloud/storage/core/protos/media.pb.h>

#include <library/cpp/threading/future/future.h>

#include <util/system/sysstat.h>

namespace NYdb::NBS::NBlockStore::NVhost {

////////////////////////////////////////////////////////////////////////////////

struct TStorageOptions
{
    TString DeviceName;
    TString DiskId;
    TString ClientId;
    ui32 BlockSize = 0;
    ui64 BlocksCount = 0;
    ui32 VhostQueuesCount = 0;
    bool UnalignedRequestsDisabled = false;
    bool CreateOverlappedRequestsGuard = true;
    NProto::EStorageMediaKind StorageMediaKind = NProto::STORAGE_MEDIA_DEFAULT;
    bool DiscardEnabled = false;
    ui32 MaxZeroBlocksSubRequestSize = 0;
    ui32 OptimalIoSize = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IServer: public IStartable
{
    virtual NThreading::TFuture<NProto::TError> StartEndpoint(
        TString socketPath,
        IStoragePtr storage,
        const TStorageOptions& options) = 0;

    virtual NThreading::TFuture<NProto::TError> StopEndpoint(
        const TString& socketPath) = 0;

    virtual NProto::TError UpdateEndpoint(
        const TString& socketPath,
        ui64 blocksCount) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct TServerConfig
{
    size_t ThreadsCount = 1;
    ui32 SocketAccessMode = S_IRGRP | S_IWGRP | S_IRUSR | S_IWUSR;
    TAffinity Affinity;
};

////////////////////////////////////////////////////////////////////////////////

IServerPtr CreateServer(
    ILoggingServicePtr logging,
    IVHostStatsPtr vhostStats,
    IVhostQueueFactoryPtr vhostQueueFactory,
    IDeviceHandlerFactoryPtr deviceHandlerFactory,
    TServerConfig serverConfig,
    TVhostCallbacks callbacks);

}   // namespace NYdb::NBS::NBlockStore::NVhost
