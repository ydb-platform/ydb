#include "server.h"

#include "vhost.h"

#include <ydb/core/nbs/cloud/blockstore/libs/diagnostics/vhost_stats.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/context.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/device_handler.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/overlapped_requests_guard_wrapper.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/helpers.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/thread.h>
#include <ydb/core/nbs/cloud/storage/core/libs/diagnostics/logging.h>

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

    std::atomic_flag Completed = false;

    TRequest(
        ui64 requestId,
        TVhostRequestPtr vhostRequest,
        const TStorageOptions& options)
        : VhostRequest(std::move(vhostRequest))
        , CallContext(MakeIntrusive<TCallContext>(requestId))
        , MetricRequest(
              VhostRequest->Type,
              options.ClientId,
              options.DiskId,
              VhostRequest->From,
              VhostRequest->Length,
              options.BlockSize)
    {}
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

class TEndpoint final: public std::enable_shared_from_this<TEndpoint>
{
private:
    TAppContext& AppCtx;
    const IDeviceHandlerPtr DeviceHandler;
    const TString SocketPath;
    const TStorageOptions Options;
    const ui32 SocketAccessMode;
    IVhostDevicePtr VhostDevice;

    TIntrusiveList<TRequest> RequestsInFlight;
    TAdaptiveLock RequestsLock;

    std::atomic_flag Stopped = false;

public:
    TEndpoint(
        TAppContext& appCtx,
        IDeviceHandlerPtr deviceHandler,
        TString socketPath,
        const TStorageOptions& options,
        ui32 socketAccessMode)
        : AppCtx(appCtx)
        , DeviceHandler(std::move(deviceHandler))
        , SocketPath(std::move(socketPath))
        , Options(options)
        , SocketAccessMode(socketAccessMode)
    {}

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
        auto request = MakeIntrusive<TRequest>(
            CreateRequestId(),
            std::move(vhostRequest),
            Options);

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

class TExecutor final: public ISimpleThread
{
private:
    TAppContext& AppCtx;
    const TString Name;
    const IVhostQueuePtr VhostQueue;
    const ui32 SocketAccessMode;
    TAffinity Affinity;

    TMap<TString, TEndpointPtr> Endpoints;

public:
    TExecutor(
        TAppContext& appCtx,
        TString name,
        IVhostQueuePtr vhostQueue,
        ui32 socketAccessMode,
        const TAffinity& affinity)
        : AppCtx(appCtx)
        , Name(std::move(name))
        , VhostQueue(std::move(vhostQueue))
        , SocketAccessMode(socketAccessMode)
        , Affinity(affinity)
    {}

    void Shutdown()
    {
        VhostQueue->Stop();
        Join();
    }

    TEndpointPtr CreateEndpoint(
        const TString& socketPath,
        const TStorageOptions& options,
        IStoragePtr storage)
    {
        TDeviceHandlerParams params{
            .Storage = CreateWrappers(options, std::move(storage)),
            .DiskId = options.DiskId,
            .ClientId = options.ClientId,
            .BlockSize = options.BlockSize,
            .MaxZeroBlocksSubRequestSize = options.MaxZeroBlocksSubRequestSize,
            .UnalignedRequestsDisabled = options.UnalignedRequestsDisabled,
            .StorageMediaKind = options.StorageMediaKind};

        auto deviceHandler =
            AppCtx.DeviceHandlerFactory->CreateDeviceHandler(std::move(params));

        auto endpoint = std::make_shared<TEndpoint>(
            AppCtx,
            std::move(deviceHandler),
            socketPath,
            options,
            SocketAccessMode);

        auto vhostDevice = VhostQueue->CreateDevice(
            socketPath,
            options.DeviceName.empty() ? options.DiskId : options.DeviceName,
            options.BlockSize,
            options.BlocksCount,
            options.VhostQueuesCount,
            options.DiscardEnabled,
            options.OptimalIoSize,
            endpoint.get(),
            AppCtx.Callbacks);
        endpoint->SetVhostDevice(std::move(vhostDevice));

        return endpoint;
    }

    void AddEndpoint(const TString& socketPath, TEndpointPtr endpoint)
    {
        auto [it, inserted] =
            Endpoints.emplace(socketPath, std::move(endpoint));
        Y_ABORT_UNLESS(inserted);
    }

    TEndpointPtr RemoveEndpoint(const TString& socketPath)
    {
        auto it = Endpoints.find(socketPath);
        Y_ABORT_UNLESS(it != Endpoints.end());

        auto endpoint = std::move(it->second);
        Endpoints.erase(it);

        return endpoint;
    }

    TEndpointPtr GetEndpoint(const TString& socketPath)
    {
        auto it = Endpoints.find(socketPath);
        if (it == Endpoints.end()) {
            return nullptr;
        }

        return it->second;
    }

    ui32 GetVhostQueuesCount() const
    {
        ui32 queuesCount = 0;
        for (const auto& it: Endpoints) {
            queuesCount += it.second->GetVhostQueuesCount();
        }
        return queuesCount;
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
        endpoint->ProcessRequest(std::move(vhostRequest));
    }

    IStoragePtr CreateWrappers(
        const TStorageOptions& options,
        IStoragePtr storage)
    {
        if (options.CreateOverlappedRequestsGuard) {
            storage =
                CreateOverlappedRequestsGuardStorageWrapper(std::move(storage));
        }
        return storage;
    }
};

using TExecutorPtr = std::unique_ptr<TExecutor>;

////////////////////////////////////////////////////////////////////////////////

class TServer final
    : public TAppContext
    , public IServer
    , public std::enable_shared_from_this<TServer>
{
private:
    TMutex Lock;

    TVector<TExecutorPtr> Executors;

    TMap<TString, TExecutor*> EndpointMap;

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
        IStoragePtr storage,
        const TStorageOptions& options) override;

    TFuture<NProto::TError> StopEndpoint(const TString& socketPath) override;

    NProto::TError UpdateEndpoint(
        const TString& socketPath,
        ui64 blocksCount) override;

private:
    void InitExecutors();

    TExecutor* PickExecutor() const;

    void StopAllEndpoints();

    void HandleStoppedEndpoint(
        const TString& socketPath,
        const NProto::TError& error);
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
    IStoragePtr storage,
    const TStorageOptions& options)
{
    if (ShouldStop.test()) {
        NProto::TError error;
        error.SetCode(E_FAIL);
        error.SetMessage("Vhost server is stopped");
        return MakeFuture(error);
    }

    TExecutor* executor;

    with_lock (Lock) {
        auto it = EndpointMap.find(socketPath);
        if (it != EndpointMap.end()) {
            NProto::TError error;
            error.SetCode(S_ALREADY);
            error.SetMessage(
                TStringBuilder() << "endpoint " << socketPath.Quote()
                                 << " has already been started");
            return MakeFuture(error);
        }

        executor = PickExecutor();
        Y_ABORT_UNLESS(executor);
    }

    auto endpoint =
        executor->CreateEndpoint(socketPath, options, std::move(storage));

    auto error = SafeExecute<NProto::TError>([&] { return endpoint->Start(); });
    if (HasError(error)) {
        return MakeFuture(error);
    }

    with_lock (Lock) {
        executor->AddEndpoint(socketPath, std::move(endpoint));

        auto [it, inserted] =
            EndpointMap.emplace(std::move(socketPath), executor);
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
        auto it = EndpointMap.find(socketPath);
        if (it == EndpointMap.end()) {
            NProto::TError error;
            error.SetCode(S_ALREADY);
            error.SetMessage(
                TStringBuilder() << "endpoint " << socketPath.Quote()
                                 << " has already been stopped");
            return MakeFuture(error);
        }

        auto* executor = it->second;
        EndpointMap.erase(it);

        endpoint = executor->RemoveEndpoint(socketPath);
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
        auto it = EndpointMap.find(socketPath);
        if (it == EndpointMap.end()) {
            NProto::TError error;
            error.SetCode(S_FALSE);
            error.SetMessage(
                TStringBuilder()
                << "endpoint " << socketPath.Quote() << " not started");
            return error;
        }

        auto* executor = it->second;
        endpoint = executor->GetEndpoint(socketPath);
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
        for (const auto& it: EndpointMap) {
            const auto& socketPath = it.first;
            auto* executor = it.second;

            auto endpoint = executor->RemoveEndpoint(socketPath);
            StoppingEndpoints.emplace(socketPath, endpoint);

            auto future = endpoint->Stop(false);
            sockets.push_back(socketPath);
            futures.push_back(future);
        }

        EndpointMap.clear();
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
            *this,
            TStringBuilder() << "VHOST" << i,
            std::move(vhostQueue),
            Config.SocketAccessMode,
            Config.Affinity);

        Executors.push_back(std::move(executor));
    }
}

TExecutor* TServer::PickExecutor() const
{
    TExecutor* result = nullptr;

    for (const auto& executor: Executors) {
        if (result == nullptr ||
            executor->GetVhostQueuesCount() < result->GetVhostQueuesCount())
        {
            result = executor.get();
        }
    }

    return result;
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
