#pragma once

#include "public.h"

#include "vhost.h"

#include <library/cpp/threading/future/future.h>

namespace NYdb::NBS::NBlockStore::NVhost {

////////////////////////////////////////////////////////////////////////////////

struct ITestVhostDevice
{
    virtual ~ITestVhostDevice() = default;

    virtual bool IsStopped() = 0;

    // Sends a synthetic test request to the device. The request is routed to
    // the queue picked by round-robin by default; an explicit queue index may
    // be passed to test specific queues.
    virtual NThreading::TFuture<TVhostRequest::EResult> SendTestRequest(
        EBlockStoreRequest type,
        ui64 from,
        ui64 length,
        TSgList sgList) = 0;

    virtual NThreading::TFuture<TVhostRequest::EResult> SendTestRequest(
        ui32 queueIndex,
        EBlockStoreRequest type,
        ui64 from,
        ui64 length,
        TSgList sgList) = 0;

    virtual void DisableAutostop(bool disable) = 0;

    [[nodiscard]] virtual ui32 GetOptimalIoSize() const = 0;

    [[nodiscard]] virtual ui32 GetQueuesCount() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct ITestVhostQueue
{
    virtual ~ITestVhostQueue() = default;

    virtual bool IsRun() = 0;

    virtual TVector<std::shared_ptr<ITestVhostDevice>> GetDevices() = 0;

    virtual void Break() = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct TTestVhostQueueFactory final: public IVhostQueueFactory
{
    TManualEvent FailedEvent;
    TVector<std::shared_ptr<ITestVhostQueue>> Queues;

    IVhostQueuePtr CreateQueue() override;

    IVhostDevicePtr CreateDevice(
        TString socketPath,
        TString deviceName,
        ui32 blockSize,
        ui64 blocksCount,
        bool discardEnabled,
        ui32 optimalIoSize,
        TVector<IVhostQueuePtr> queues,
        void* cookie,
        const TVhostCallbacks& callbacks) override;
};

}   // namespace NYdb::NBS::NBlockStore::NVhost
