#include "vhost_test.h"

#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>

#include <util/folder/path.h>
#include <util/system/mutex.h>
#include <util/thread/lfqueue.h>

#include <atomic>

namespace NYdb::NBS::NBlockStore::NVhost {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TTestVhostRequest final: public TVhostRequest
{
private:
    TPromise<EResult> Promise;

public:
    TTestVhostRequest(
        TPromise<EResult> promise,
        EBlockStoreRequest type,
        ui64 from,
        ui64 length,
        TSgList sgList,
        void* cookie)
        : TVhostRequest(type, from, length, std::move(sgList), cookie)
        , Promise(std::move(promise))
    {}

    void Complete(EResult result) override
    {
        Promise.SetValue(result);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TTestVhostQueue;
using TTestVhostQueuePtr = std::shared_ptr<TTestVhostQueue>;

////////////////////////////////////////////////////////////////////////////////

// Test queue enqueues pending requests in a lock-free queue and dequeues them
// when the server executor polls it.
class TTestVhostQueue final
    : public ITestVhostQueue
    , public IVhostQueue
{
private:
    TManualEvent& FailedEvent;

    enum EState
    {
        Undefined = 0,
        Running = 1,
        Stopped = 2,
        Broken = 3,
    };

    std::atomic<EState> State = Undefined;

    TLockFreeQueue<TVhostRequest*> Requests;

    TMutex Lock;
    // Devices that registered this queue during CreateDevice().
    TVector<std::weak_ptr<ITestVhostDevice>> Devices;

public:
    explicit TTestVhostQueue(TManualEvent& failedEvent)
        : FailedEvent(failedEvent)
    {}

    ~TTestVhostQueue() override
    {
        TVhostRequest* req = nullptr;
        while (Requests.Dequeue(&req)) {
            delete req;
        }
    }

    int Run() override
    {
        EState expected = Undefined;
        State.compare_exchange_strong(expected, Running);

        switch (State.load()) {
            case Running:
                return -EAGAIN;
            case Stopped:
                return 0;
            case Broken:
                FailedEvent.Signal();
                return -1;
            default:
                Y_ABORT();
        }
    }

    void Stop() override
    {
        EState expected = Running;
        bool wasRun = State.compare_exchange_strong(expected, Stopped);
        Y_ABORT_UNLESS(wasRun || State.load() == Broken);
    }

    TVhostRequestPtr DequeueRequest() override
    {
        if (State.load() != Running) {
            return nullptr;
        }

        TVhostRequest* request = nullptr;
        if (Requests.Dequeue(&request)) {
            return std::unique_ptr<TVhostRequest>(request);
        }
        return nullptr;
    }

    bool IsRun() override
    {
        return State.load() == Running;
    }

    TVector<std::shared_ptr<ITestVhostDevice>> GetDevices() override
    {
        TVector<std::shared_ptr<ITestVhostDevice>> res;
        with_lock (Lock) {
            for (auto& device: Devices) {
                if (auto ptr = device.lock()) {
                    res.push_back(ptr);
                }
            }
        }
        return res;
    }

    void Break() override
    {
        State.store(Broken);
    }

    // Called by the test device when a request needs to be enqueued to this
    // queue for later processing by the executor thread.
    void EnqueueRequest(TVhostRequest* request)
    {
        Requests.Enqueue(request);
    }

    void RegisterDevice(std::shared_ptr<ITestVhostDevice> device)
    {
        with_lock (Lock) {
            Devices.push_back(device);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

// Test device is backed by multiple TTestVhostQueue's. SendTestRequest picks
// a queue (explicitly or round-robin) and enqueues the request there with
// the device's single cookie. This mirrors the real vhost path where the
// caller-side identifier is per-device (libvhost provides per-request
// device routing via vhd_request.vdev).
class TTestVhostDevice final
    : public ITestVhostDevice
    , public IVhostDevice
{
private:
    const TString SocketPath;
    TVector<TTestVhostQueuePtr> Queues;
    void* const Cookie;
    const ui32 OptimalIoSize;

    std::atomic<ui64> Counter = 0;

    TMutex Lock;
    TVector<TFuture<TVhostRequest::EResult>> Futures;

    std::atomic_flag Stopped = false;
    TPromise<void> Autostop;

public:
    TTestVhostDevice(
        TString socketPath,
        TVector<TTestVhostQueuePtr> queues,
        void* cookie,
        ui32 optimalIoSize)
        : SocketPath(std::move(socketPath))
        , Queues(std::move(queues))
        , Cookie(cookie)
        , OptimalIoSize(optimalIoSize)
    {
        Autostop = NewPromise<void>();
        Autostop.SetValue();
    }

    bool Start() override
    {
        TFsPath(SocketPath).Touch();
        return true;
    }

    TFuture<NProto::TError> Stop() override
    {
        Y_UNUSED(Stopped.test_and_set());
        return Autostop.GetFuture().Apply(
            [this](const auto&)
            {
                with_lock (Lock) {
                    return WaitAll(Futures).Apply([](const auto&)
                                                  { return NProto::TError(); });
                }
            });
    }

    void Update(ui64 blocksCount) override
    {
        Y_UNUSED(blocksCount);
    }

    bool IsStopped() override
    {
        return Stopped.test();
    }

    void DisableAutostop(bool disable) override
    {
        if (disable) {
            auto promise = NewPromise<void>();
            Autostop.Swap(promise);
        } else {
            Autostop.SetValue();
        }
    }

    TFuture<TVhostRequest::EResult> SendTestRequest(
        EBlockStoreRequest type,
        ui64 from,
        ui64 length,
        TSgList sgList) override
    {
        ui32 queueIndex =
            static_cast<ui32>(Counter.fetch_add(1) % Queues.size());
        return SendTestRequest(
            queueIndex,
            type,
            from,
            length,
            std::move(sgList));
    }

    TFuture<TVhostRequest::EResult> SendTestRequest(
        ui32 queueIndex,
        EBlockStoreRequest type,
        ui64 from,
        ui64 length,
        TSgList sgList) override
    {
        Y_ABORT_UNLESS(queueIndex < Queues.size());

        auto promise = NewPromise<TVhostRequest::EResult>();
        auto future = promise.GetFuture();

        with_lock (Lock) {
            if (Stopped.test()) {
                promise.SetValue(TVhostRequest::CANCELLED);
                return future;
            }
            Futures.push_back(future);
        }

        auto request = std::make_unique<TTestVhostRequest>(
            std::move(promise),
            type,
            from,
            length,
            std::move(sgList),
            Cookie);
        Queues[queueIndex]->EnqueueRequest(request.release());
        return future;
    }

    ui32 GetOptimalIoSize() const override
    {
        return OptimalIoSize;
    }

    ui32 GetQueuesCount() const override
    {
        return static_cast<ui32>(Queues.size());
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IVhostQueuePtr TTestVhostQueueFactory::CreateQueue()
{
    auto queue = std::make_shared<TTestVhostQueue>(FailedEvent);
    Queues.push_back(queue);
    return queue;
}

IVhostDevicePtr TTestVhostQueueFactory::CreateDevice(
    TString socketPath,
    TString deviceName,
    ui32 blockSize,
    ui64 blocksCount,
    bool discardEnabled,
    ui32 optimalIoSize,
    TVector<IVhostQueuePtr> queues,
    void* cookie,
    const TVhostCallbacks& callbacks)
{
    Y_UNUSED(deviceName);
    Y_UNUSED(blockSize);
    Y_UNUSED(blocksCount);
    Y_UNUSED(discardEnabled);
    Y_UNUSED(callbacks);

    Y_ABORT_UNLESS(!queues.empty());

    TVector<TTestVhostQueuePtr> testQueues;
    testQueues.reserve(queues.size());
    for (auto& queue: queues) {
        auto typed = std::dynamic_pointer_cast<TTestVhostQueue>(queue);
        Y_ABORT_UNLESS(typed);
        testQueues.push_back(std::move(typed));
    }

    auto device = std::make_shared<TTestVhostDevice>(
        std::move(socketPath),
        testQueues,
        cookie,
        optimalIoSize);

    for (auto& queue: testQueues) {
        queue->RegisterDevice(device);
    }
    return device;
}

}   // namespace NYdb::NBS::NBlockStore::NVhost
