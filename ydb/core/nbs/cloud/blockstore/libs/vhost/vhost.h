#pragma once

#include "public.h"

#include <ydb/core/nbs/cloud/blockstore/libs/diagnostics/public.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/request.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/guarded_sglist.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <atomic>

namespace NYdb::NBS::NBlockStore::NVhost {

////////////////////////////////////////////////////////////////////////////////

struct TVhostRequest
{
    enum EResult
    {
        SUCCESS,
        IOERR,
        CANCELLED,
    };

    const EBlockStoreRequest Type = EBlockStoreRequest::ReadBlocks;
    const ui64 From = 0;
    const ui64 Length = 0;
    void* const Cookie = nullptr;

    TGuardedSgList SgList;

    TVhostRequest(
        EBlockStoreRequest type,
        ui64 from,
        ui64 length,
        TSgList sgList,
        void* cookie);

    virtual ~TVhostRequest() = default;

    virtual void Complete(EResult result) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IVhostDevice
{
    virtual ~IVhostDevice() = default;

    virtual bool Start() = 0;
    virtual NThreading::TFuture<NProto::TError> Stop() = 0;
    virtual void Update(ui64 blocksCount) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IVhostQueue
{
    // Number of endpoints currently using this queue. Read by the server
    // for load-balanced executor selection; managed atomically by
    // TEndpoint's constructor/destructor without any external locking.
    std::atomic<ui32> AssignedEndpointsCount{0};

    virtual ~IVhostQueue() = default;

    virtual int Run() = 0;
    virtual void Stop() = 0;

    virtual TVhostRequestPtr DequeueRequest() = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IVhostQueueFactory
{
    virtual ~IVhostQueueFactory() = default;

    virtual IVhostQueuePtr CreateQueue() = 0;

    // Create a vhost block device backed by the given set of queues.
    // The single cookie is placed into the TVhostRequest::Cookie of
    // every request dequeued from any of the device's queues; the
    // routing to the right caller-side endpoint is provided by libvhost
    // via vhd_request.vdev (which always points to this device, since
    // a queue can be shared between devices).
    virtual IVhostDevicePtr CreateDevice(
        TString socketPath,
        TString deviceName,
        ui32 blockSize,
        ui64 blocksCount,
        bool discardEnabled,
        ui32 optimalIoSize,
        TVector<IVhostQueuePtr> queues,
        void* cookie,
        const TVhostCallbacks& callbacks) = 0;
};

////////////////////////////////////////////////////////////////////////////////

void InitVhostLog(ILoggingServicePtr logging);

IVhostQueueFactoryPtr CreateVhostQueueFactory();

}   // namespace NYdb::NBS::NBlockStore::NVhost
