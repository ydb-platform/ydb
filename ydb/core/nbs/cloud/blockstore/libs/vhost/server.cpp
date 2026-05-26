#include "server.h"

#include "vhost.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/diagnostics/vhost_stats.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/context.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/device_handler.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/overlapped_requests_guard_wrapper.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/partition_direct_service.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/split_requests_wrapper.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/helpers.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/thread.h>
#include <ydb/core/nbs/cloud/storage/core/libs/diagnostics/logging.h>

#include <ydb/library/actors/wilson/wilson_trace.h>

#include <util/folder/path.h>
#include <util/generic/map.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>
#include <util/system/mutex.h>
#include <util/system/thread.h>

#include <atomic>

namespace NYdb::NBS::NBlockStore::NVhost {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TReadBlocksLocalMethod
{
    using TResponse = TReadBlocksLocalResponse;

    static TFuture<TReadBlocksLocalResponse> Execute(
        IDeviceHandler& deviceHandler,
        TCallContextPtr ctx,
        TVhostRequest& vhostRequest)
    {
        TString checkpointId;
        return deviceHandler.Read(
            std::move(ctx),
            vhostRequest.From,
            vhostRequest.Length,
            vhostRequest.SgList,
            checkpointId);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TWriteBlocksLocalMethod
{
    using TResponse = TWriteBlocksLocalResponse;

    static TFuture<TWriteBlocksLocalResponse> Execute(
        IDeviceHandler& deviceHandler,
        TCallContextPtr ctx,
        TVhostRequest& vhostRequest)
    {
        return deviceHandler.Write(
            std::move(ctx),
            vhostRequest.From,
            vhostRequest.Length,
            vhostRequest.SgList);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TZeroBlocksMethod
{
    using TResponse = TZeroBlocksLocalResponse;

    static TFuture<TZeroBlocksLocalResponse> Execute(
        IDeviceHandler& deviceHandler,
        TCallContextPtr ctx,
        TVhostRequest& vhostRequest)
    {
        return deviceHandler.Zero(
            std::move(ctx),
            vhostRequest.From,
            vhostRequest.Length);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TRequest
    : public TIntrusiveListItem<TRequest>
    , TAtomicRefCount<TRequest>
{
    const TVhostRequestPtr VhostRequest;
    const TCallContextPtr CallContext;
    TMetricRequest MetricRequest;
    NWilson::TSpan RootSpan;

    std::atomic_flag Completed = false;

    TRequest(
        ui64 requestId,
        TVhostRequestPtr vhostRequest,
        const TStorageOptions& options,
        NWilson::TSpan rootSpan)
        : VhostRequest(std::move(vhostRequest))
        , CallContext(MakeIntrusive<TCallContext>(requestId))
        , MetricRequest(
              VhostRequest->Type,
              options.ClientId,
              options.DiskId,
              VhostRequest->From,
              VhostRequest->Length,
              options.BlockSize)
        , RootSpan(std::move(rootSpan))
    {
        CallContext->RootTraceId = RootSpan.GetTraceId();
        RootSpan.Attribute("DiskId", options.DiskId);
        RootSpan.Attribute("From", static_cast<i64>(VhostRequest->From));
        RootSpan.Attribute("Length", static_cast<i64>(VhostRequest->Length));
    }
};

using TRequestPtr = TIntrusivePtr<TRequest>;

////////////////////////////////////////////////////////////////////////////////

struct TAppContext
{
    IVHostStatsPtr VHostStats;
    IVhostQueueFactoryPtr VhostQueueFactory;
    IDeviceHandlerFactoryPtr DeviceHandlerFactory;
    TServerConfig Config;
    TVhostCallbacks Callbacks;

    TLog Log;

    std::atomic_flag ShouldStop = false;
};

////////////////////////////////////////////////////////////////////////////////

// The device is served by up to VhostQueuesCount vhost queues, with the
// actual queue count capped by the executor pool size. Each queue is
// processed by a different executor thread, but all queues share the same
// IDeviceHandler (and therefore the same storage-wrapper chain: aligned
// device handler, fast-path service, overlapped-requests guard, etc.).
// The handler is reentrant from multiple threads.
class TEndpoint final: public std::enable_shared_from_this<TEndpoint>
{
private:
    TAppContext& AppCtx;
    // Single device handler shared by all vhost queues of this endpoint.
    const IDeviceHandlerPtr DeviceHandler;
    const IPartitionDirectServicePtr PartitionDirectService;
    const TString SocketPath;
    const TStorageOptions Options;
    const ui32 SocketAccessMode;
    // Queues assigned to this endpoint. Kept here so that the endpoint's
    // lifetime governs the per-queue assignment counter (atomic, no locks
    // required for inc/dec).
    const TVector<IVhostQueuePtr> Queues;
    IVhostDevicePtr VhostDevice;

    TIntrusiveList<TRequest> RequestsInFlight;
    TAdaptiveLock RequestsLock;

    std::atomic_flag Stopped = false;

public:
    TEndpoint(
        TAppContext& appCtx,
        IDeviceHandlerPtr deviceHandler,
        IPartitionDirectServicePtr partitionDirectService,
        TString socketPath,
        const TStorageOptions& options,
        ui32 socketAccessMode,
        TVector<IVhostQueuePtr> queues)
        : AppCtx(appCtx)
        , DeviceHandler(std::move(deviceHandler))
        , PartitionDirectService(std::move(partitionDirectService))
        , SocketPath(std::move(socketPath))
        , Options(options)
        , SocketAccessMode(socketAccessMode)
        , Queues(std::move(queues))
    {
        Y_ABORT_UNLESS(DeviceHandler);
        Y_ABORT_UNLESS(!Queues.empty());
        for (const auto& queue: Queues) {
            queue->AssignedEndpointsCount.fetch_add(
                1,
                std::memory_order_relaxed);
        }
    }

    ~TEndpoint()
    {
        for (const auto& queue: Queues) {
            queue->AssignedEndpointsCount.fetch_sub(
                1,
                std::memory_order_relaxed);
        }
    }

    // The cookie attached to every request dispatched through this
    // endpoint's vhost device.
    void* GetCookie()
    {
        return this;
    }

    void SetVhostDevice(IVhostDevicePtr vhostDevice)
    {
        Y_ABORT_UNLESS(VhostDevice == nullptr);
        VhostDevice = std::move(vhostDevice);
    }

    NProto::TError Start()
    {
        TFsPath(SocketPath).DeleteIfExists();

        bool started = VhostDevice->Start();

        if (!started) {
            NProto::TError error;
            error.SetCode(E_FAIL);
            error.SetMessage(
                TStringBuilder()
                << "could not register block device " << SocketPath.Quote());
            return error;
        }

        auto err = Chmod(SocketPath.c_str(), SocketAccessMode);

        if (err != 0) {
            NProto::TError error;
            error.SetCode(MAKE_SYSTEM_ERROR(err));
            error.SetMessage(
                TStringBuilder()
                << "failed to chmod socket " << SocketPath.Quote());
            return error;
        }

        return NProto::TError();
    }

    TFuture<NProto::TError> Stop(bool deleteSocket)
    {
        if (Stopped.test_and_set()) {
            return MakeFuture(MakeError(S_ALREADY));
        }

        auto future = VhostDevice->Stop();

        auto cancelError = MakeError(E_CANCELLED, "Vhost endpoint is stopping");
        with_lock (RequestsLock) {
            TLog& Log = AppCtx.Log;
            STORAGE_INFO(
                "Stop endpoint " << SocketPath.Quote() << " with "
                                 << RequestsInFlight.Size()
                                 << " inflight requests");

            RequestsInFlight.ForEach(
                [&](TRequest* request)
                {
                    CompleteRequest(*request, cancelError);
                    request->Unlink();
                });
        }

        if (deleteSocket) {
            TLog& Log = AppCtx.Log;
            future = future.Apply(
                [socketPath = SocketPath, Log](const auto& f)
                {
                    STORAGE_INFO(
                        "Deletion socket while stopping endpoint "
                        << socketPath.Quote());
                    TFsPath(socketPath).DeleteIfExists();
                    return f.GetValue();
                });
        }

        return future;
    }

    void Update(ui64 blocksCount)
    {
        TLog& Log = AppCtx.Log;
        STORAGE_INFO(
            "Update vhost endpoint " << SocketPath.Quote()
                                     << " with blocks count = " << blocksCount);
        VhostDevice->Update(blocksCount);
    }

    ui32 GetVhostQueuesCount() const
    {
        return Options.VhostQueuesCount;
    }

    // Processes a request dequeued from any vhost queue. All queues share
    // a single device handler.
    void ProcessRequest(TVhostRequestPtr vhostRequest)
    {
        const auto requestType = vhostRequest->Type;
        auto request = RegisterRequest(std::move(vhostRequest));
        if (!request) {
            return;
        }

        switch (requestType) {
            case EBlockStoreRequest::WriteBlocks:
                ProcessRequest<TWriteBlocksLocalMethod>(std::move(request));
                break;
            case EBlockStoreRequest::ReadBlocks:
                ProcessRequest<TReadBlocksLocalMethod>(std::move(request));
                break;
            case EBlockStoreRequest::ZeroBlocks:
                ProcessRequest<TZeroBlocksMethod>(std::move(request));
                break;
            default:
                Y_ABORT(
                    "Unexpected request type: %d",
                    static_cast<int>(requestType));
                break;
        }
    }

private:
    template <typename TMethod>
    void ProcessRequest(TRequestPtr request)
    {
        auto future = TMethod::Execute(
            *DeviceHandler,
            request->CallContext,
            *request->VhostRequest);

        future.Apply(
            [weakPtr = weak_from_this(), req = std::move(request)](
                const TFuture<typename TMethod::TResponse>& f)
            {
                const auto& response = f.GetValue();
                if (auto p = weakPtr.lock()) {
                    p->CompleteRequest(*req, response.Error);
                    p->UnregisterRequest(*req);
                }
                return f.GetValue();
            });
    }

    TRequestPtr RegisterRequest(TVhostRequestPtr vhostRequest)
    {
        const auto requestType = vhostRequest->Type;
        auto request = MakeIntrusive<TRequest>(
            CreateRequestId(),
            std::move(vhostRequest),
            Options,
            PartitionDirectService->CreteRootSpan(ToStringBuf(requestType)));

        AppCtx.VHostStats->RequestStarted(
            AppCtx.Log,
            request->MetricRequest,
            *request->CallContext);

        with_lock (RequestsLock) {
            if (!Stopped.test()) {
                RequestsInFlight.PushBack(request.Get());
                return request;
            }
        }

        auto error = MakeError(E_CANCELLED, "Vhost endpoint was stopped");
        CompleteRequest(*request, error);
        return nullptr;
    }

    void CompleteRequest(TRequest& request, const NProto::TError& error)
    {
        if (request.Completed.test_and_set()) {
            return;
        }

        auto statsError = error;
        auto vhostResult = GetResult(statsError);

        AppCtx.VHostStats->RequestCompleted(
            AppCtx.Log,
            request.MetricRequest,
            *request.CallContext,
            statsError);

        request.VhostRequest->Complete(vhostResult);
    }

    void UnregisterRequest(TRequest& request)
    {
        with_lock (RequestsLock) {
            request.Unlink();
        }
    }

    TVhostRequest::EResult GetResult(NProto::TError& error)
    {
        if (!HasError(error)) {
            return TVhostRequest::SUCCESS;
        }

        // Keep the logic synchronized with
        // TAlignedDeviceHandler::ReportCriticalError().
        bool cancelError = error.GetCode() == E_CANCELLED ||
                           GetErrorKind(error) == EErrorKind::ErrorRetriable;

        bool stopEndpoint = AppCtx.ShouldStop.test() || Stopped.test();

        if (stopEndpoint && cancelError) {
            auto flags = error.GetFlags();
            SetProtoFlag(flags, NProto::EF_SILENT);
            error.SetFlags(flags);
            return TVhostRequest::CANCELLED;
        }

        return TVhostRequest::IOERR;
    }
};

using TEndpointPtr = std::shared_ptr<TEndpoint>;

////////////////////////////////////////////////////////////////////////////////

// TExecutor runs a single vhost request queue in its own thread.
class TExecutor final: public ISimpleThread
{
private:
    const TString Name;
    const IVhostQueuePtr VhostQueue;
    TAffinity Affinity;

public:
    TExecutor(
        TString name,
        IVhostQueuePtr vhostQueue,
        const TAffinity& affinity)
        : Name(std::move(name))
        , VhostQueue(std::move(vhostQueue))
        , Affinity(affinity)
    {}

    void Shutdown()
    {
        VhostQueue->Stop();
        Join();
    }

    const IVhostQueuePtr& GetQueue() const
    {
        return VhostQueue;
    }

private:
    void* ThreadProc() override
    {
        TAffinityGuard affinityGuard(Affinity);

        NYdb::NBS::SetCurrentThreadName(Name);

        while (true) {
            int res = RunRequestQueue();
            if (res != -EAGAIN) {
                if (res < 0) {
                    // ReportVhostQueueRunningError({{"return_code", -res}});
                }
                break;
            }

            while (auto req = VhostQueue->DequeueRequest()) {
                ProcessRequest(std::move(req));
            }
        }

        return nullptr;
    }

    int RunRequestQueue()
    {
        return VhostQueue->Run();
    }

    void ProcessRequest(TVhostRequestPtr vhostRequest)
    {
        auto* endpoint = reinterpret_cast<TEndpoint*>(vhostRequest->Cookie);
        Y_ABORT_UNLESS(endpoint);
        endpoint->ProcessRequest(std::move(vhostRequest));
    }
};

using TExecutorPtr = std::unique_ptr<TExecutor>;

////////////////////////////////////////////////////////////////////////////////

// Server owns the pool of executors (one per thread) and the set of active
// endpoints. For every endpoint it picks the N least-loaded executors
// (N = VhostQueuesCount) so that each of the endpoint's queues is served by
// a different thread, achieving true parallelism between queues.
class TServer final
    : public TAppContext
    , public IServer
    , public std::enable_shared_from_this<TServer>
{
private:
    TMutex Lock;

    TVector<TExecutorPtr> Executors;

    TMap<TString, TEndpointPtr> Endpoints;
    TMap<TString, TEndpointPtr> StoppingEndpoints;

public:
    TServer(
        ILoggingServicePtr logging,
        IVHostStatsPtr vhostStats,
        IVhostQueueFactoryPtr vhostQueueFactory,
        IDeviceHandlerFactoryPtr deviceHandlerFactory,
        TServerConfig config,
        TVhostCallbacks callbacks);

    ~TServer() override;

    void Start() override;
    void Stop() override;

    TFuture<NProto::TError> StartEndpoint(
        TString socketPath,
        IPartitionDirectServicePtr partitionDirectService,
        IStoragePtr storage,
        const TStorageOptions& options) override;

    TFuture<NProto::TError> StopEndpoint(const TString& socketPath) override;

    NProto::TError UpdateEndpoint(
        const TString& socketPath,
        ui64 blocksCount) override;

private:
    void InitExecutors();

    // Picks `count` distinct executors with the lowest number of assigned
    // queues. Must be called under Lock.
    TVector<TExecutor*> PickExecutors(ui32 count);

    void StopAllEndpoints();

    void HandleStoppedEndpoint(
        const TString& socketPath,
        const NProto::TError& error);

    IStoragePtr CreateWrappers(
        const TStorageOptions& options,
        IStoragePtr storage);

    // Creates a single device handler shared by all vhost queues of the
    // endpoint. The whole storage-wrapper chain (aligned device handler,
    // fast-path service, overlapped-requests guard, etc.) is built once per
    // endpoint and reused across queues.
    IDeviceHandlerPtr CreateDeviceHandler(
        const TStorageOptions& options,
        IStoragePtr storage);
};

////////////////////////////////////////////////////////////////////////////////

TServer::TServer(
    ILoggingServicePtr logging,
    IVHostStatsPtr vhostStats,
    IVhostQueueFactoryPtr vhostQueueFactory,
    IDeviceHandlerFactoryPtr deviceHandlerFactory,
    TServerConfig config,
    TVhostCallbacks callbacks)
{
    Log = logging->CreateLog("BLOCKSTORE_VHOST");
    VHostStats = std::move(vhostStats);
    VhostQueueFactory = std::move(vhostQueueFactory);
    DeviceHandlerFactory = std::move(deviceHandlerFactory);
    Config = std::move(config);
    Callbacks = std::move(callbacks);

    InitExecutors();
}

TServer::~TServer()
{
    Stop();
}

void TServer::Start()
{
    STORAGE_INFO("Start");

    for (auto& executor: Executors) {
        executor->Start();
    }
}

void TServer::Stop()
{
    if (ShouldStop.test_and_set()) {
        return;
    }

    STORAGE_INFO("Shutting down");

    StopAllEndpoints();

    for (auto& executor: Executors) {
        executor->Shutdown();
    }
}

TFuture<NProto::TError> TServer::StartEndpoint(
    TString socketPath,
    IPartitionDirectServicePtr partitionDirectService,
    IStoragePtr storage,
    const TStorageOptions& options)
{
    if (ShouldStop.test()) {
        NProto::TError error;
        error.SetCode(E_FAIL);
        error.SetMessage("Vhost server is stopped");
        return MakeFuture(error);
    }

    const ui32 requestedQueuesCount = Max<ui32>(1, options.VhostQueuesCount);
    const ui32 queuesCount =
        Min<ui32>(requestedQueuesCount, static_cast<ui32>(Executors.size()));

    TVector<IVhostQueuePtr> queues;

    with_lock (Lock) {
        auto it = Endpoints.find(socketPath);
        if (it != Endpoints.end()) {
            NProto::TError error;
            error.SetCode(S_ALREADY);
            error.SetMessage(
                TStringBuilder() << "endpoint " << socketPath.Quote()
                                 << " has already been started");
            return MakeFuture(error);
        }

        auto pickedExecutors = PickExecutors(queuesCount);
        Y_ABORT_UNLESS(pickedExecutors.size() == queuesCount);

        queues.reserve(queuesCount);
        for (auto* executor: pickedExecutors) {
            queues.push_back(executor->GetQueue());
        }
    }

    // Single device handler shared by all vhost queues of this endpoint.
    // The whole storage-wrapper chain is built once per endpoint.
    auto deviceHandler = CreateDeviceHandler(options, std::move(storage));

    auto endpoint = std::make_shared<TEndpoint>(
        *this,
        std::move(deviceHandler),
        std::move(partitionDirectService),
        socketPath,
        options,
        Config.SocketAccessMode,
        queues);

    auto vhostDevice = VhostQueueFactory->CreateDevice(
        socketPath,
        options.DeviceName.empty() ? options.DiskId : options.DeviceName,
        options.BlockSize,
        options.BlocksCount,
        options.DiscardEnabled,
        options.OptimalIoSize,
        std::move(queues),
        endpoint->GetCookie(),
        Callbacks);
    endpoint->SetVhostDevice(std::move(vhostDevice));

    auto error = SafeExecute<NProto::TError>([&] { return endpoint->Start(); });
    if (HasError(error)) {
        return MakeFuture(error);
    }

    with_lock (Lock) {
        auto [it, inserted] =
            Endpoints.emplace(std::move(socketPath), std::move(endpoint));
        Y_ABORT_UNLESS(inserted);
    }

    return MakeFuture<NProto::TError>();
}

TFuture<NProto::TError> TServer::StopEndpoint(const TString& socketPath)
{
    if (ShouldStop.test()) {
        NProto::TError error;
        error.SetCode(E_FAIL);
        error.SetMessage("Vhost server is stopped");
        return MakeFuture(error);
    }

    TEndpointPtr endpoint;

    with_lock (Lock) {
        auto it = Endpoints.find(socketPath);
        if (it == Endpoints.end()) {
            NProto::TError error;
            error.SetCode(S_ALREADY);
            error.SetMessage(
                TStringBuilder() << "endpoint " << socketPath.Quote()
                                 << " has already been stopped");
            return MakeFuture(error);
        }

        endpoint = std::move(it->second);
        Endpoints.erase(it);

        StoppingEndpoints.emplace(socketPath, endpoint);
    }

    auto ptr = shared_from_this();
    return endpoint->Stop(true).Apply(
        [ptr = std::move(ptr), socketPath](const auto& future)
        {
            const auto& error = future.GetValue();
            ptr->HandleStoppedEndpoint(socketPath, error);
            return error;
        });
}

NProto::TError TServer::UpdateEndpoint(
    const TString& socketPath,
    ui64 blocksCount)
{
    if (ShouldStop.test()) {
        NProto::TError error;
        error.SetCode(E_FAIL);
        error.SetMessage("Vhost server is stopped");
        return error;
    }

    TEndpointPtr endpoint;

    with_lock (Lock) {
        auto it = Endpoints.find(socketPath);
        if (it == Endpoints.end()) {
            NProto::TError error;
            error.SetCode(S_FALSE);
            error.SetMessage(
                TStringBuilder()
                << "endpoint " << socketPath.Quote() << " not started");
            return error;
        }

        endpoint = it->second;
    }

    if (endpoint) {
        endpoint->Update(blocksCount);
    }
    return NProto::TError{};
}

void TServer::StopAllEndpoints()
{
    TVector<TString> sockets;
    TVector<TFuture<NProto::TError>> futures;

    with_lock (Lock) {
        for (auto& it: Endpoints) {
            const auto& socketPath = it.first;
            auto& endpoint = it.second;

            auto future = endpoint->Stop(false);
            sockets.push_back(socketPath);
            futures.push_back(future);

            StoppingEndpoints.emplace(socketPath, std::move(endpoint));
        }

        Endpoints.clear();
    }

    WaitAll(futures).Wait();

    for (size_t i = 0; i < sockets.size(); ++i) {
        const auto& socketPath = sockets[i];
        const auto& future = futures[i];
        HandleStoppedEndpoint(socketPath, future.GetValue());
    }
}

void TServer::HandleStoppedEndpoint(
    const TString& socketPath,
    const NProto::TError& error)
{
    if (HasError(error)) {
        STORAGE_ERROR(
            "Failed to stop endpoint: " << socketPath.Quote()
                                        << ". Error: " << error);
    }

    with_lock (Lock) {
        auto it = StoppingEndpoints.find(socketPath);
        if (it != StoppingEndpoints.end()) {
            StoppingEndpoints.erase(it);
        }
    }
}

void TServer::InitExecutors()
{
    for (size_t i = 1; i <= Config.ThreadsCount; ++i) {
        auto vhostQueue = VhostQueueFactory->CreateQueue();

        auto executor = std::make_unique<TExecutor>(
            TStringBuilder() << "VHOST" << i,
            std::move(vhostQueue),
            Config.Affinity);

        Executors.push_back(std::move(executor));
    }
}

TVector<TExecutor*> TServer::PickExecutors(ui32 count)
{
    Y_ABORT_UNLESS(count > 0);
    Y_ABORT_UNLESS(!Executors.empty());
    Y_ABORT_UNLESS(count <= Executors.size());

    // Snapshot each executor's assigned-endpoint count into a multimap so
    // the ordering is computed against a stable set of values, even if
    // other threads are concurrently (de)assigning endpoints. The multimap
    // sorts by key ascending, so the first `count` entries are the least
    // loaded executors.
    TMultiMap<ui32, TExecutor*> byLoad;
    for (const auto& executor: Executors) {
        const ui32 load = executor->GetQueue()->AssignedEndpointsCount.load();
        byLoad.emplace(load, executor.get());
    }

    TVector<TExecutor*> picked;
    picked.reserve(count);
    for (const auto& [load, executor]: byLoad) {
        if (picked.size() == count) {
            break;
        }
        picked.push_back(executor);
    }
    return picked;
}

IStoragePtr TServer::CreateWrappers(
    const TStorageOptions& options,
    IStoragePtr storage)
{
    storage = CreateSplitRequestsStorageWrapper(std::move(storage));
    if (options.CreateOverlappedRequestsGuard) {
        storage =
            CreateOverlappedRequestsGuardStorageWrapper(std::move(storage));
    }
    return storage;
}

IDeviceHandlerPtr TServer::CreateDeviceHandler(
    const TStorageOptions& options,
    IStoragePtr storage)
{
    TDeviceHandlerParams params{
        .Storage = CreateWrappers(options, std::move(storage)),
        .DiskId = options.DiskId,
        .ClientId = options.ClientId,
        .BlockSize = options.BlockSize,
        .BlockCount = options.BlocksCount,
        .BlocksPerStripeCount = options.StripeSize / options.BlockSize,
        .VChunkSize = options.VChunkSize,
        .MaxZeroBlocksSubRequestSize = options.MaxZeroBlocksSubRequestSize,
        .UnalignedRequestsDisabled = options.UnalignedRequestsDisabled,
        .StorageMediaKind = options.StorageMediaKind};

    return DeviceHandlerFactory->CreateDeviceHandler(std::move(params));
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IServerPtr CreateServer(
    ILoggingServicePtr logging,
    IVHostStatsPtr vhostStats,
    IVhostQueueFactoryPtr vhostQueueFactory,
    IDeviceHandlerFactoryPtr deviceHandlerFactory,
    TServerConfig config,
    TVhostCallbacks callbacks)
{
    return std::make_shared<TServer>(
        std::move(logging),
        std::move(vhostStats),
        std::move(vhostQueueFactory),
        std::move(deviceHandlerFactory),
        std::move(config),
        std::move(callbacks));
}

}   // namespace NYdb::NBS::NBlockStore::NVhost
