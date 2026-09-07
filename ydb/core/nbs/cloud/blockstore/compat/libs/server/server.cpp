#include "server.h"

#include "config.h"
#include "client_storage_factory.h"

#include <ydb/core/nbs/cloud/blockstore/compat/public/api/grpc/service.grpc.pb.h>

#include <ydb/core/nbs/cloud/blockstore/compat/libs/diagnostics/config.h>
#include <ydb/core/nbs/cloud/blockstore/compat/libs/diagnostics/critical_events.h>
#include <ydb/core/nbs/cloud/blockstore/compat/libs/diagnostics/incomplete_requests.h>
#include <ydb/core/nbs/cloud/blockstore/compat/libs/diagnostics/server_stats.h>
#include <ydb/core/nbs/cloud/blockstore/compat/libs/service/context.h>
#include <ydb/core/nbs/cloud/blockstore/compat/libs/service/request_helpers.h>
#include <ydb/core/nbs/cloud/blockstore/compat/libs/service/request.h>
#include <ydb/core/nbs/cloud/blockstore/compat/libs/service/service.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/thread.h>
#include <ydb/core/nbs/cloud/storage/core/libs/diagnostics/logging.h>
#include <ydb/core/nbs/cloud/storage/core/libs/uds/client_storage.h>
#include <ydb/core/nbs/cloud/storage/core/libs/uds/endpoint_poller.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>
#include <ydb/core/nbs/cloud/storage/core/libs/grpc/auth_metadata.h>
#include <ydb/core/nbs/cloud/storage/core/libs/grpc/completion.h>
#include <ydb/core/nbs/cloud/storage/core/libs/grpc/credentials.h>
#include <ydb/core/nbs/cloud/storage/core/libs/grpc/executor.h>
#include <ydb/core/nbs/cloud/storage/core/libs/grpc/init.h>
#include <ydb/core/nbs/cloud/storage/core/libs/grpc/keepalive.h>
#include <ydb/core/nbs/cloud/storage/core/libs/grpc/time_point_specialization.h>
#include <ydb/core/nbs/cloud/storage/core/libs/grpc/utils.h>

#include <contrib/libs/grpc/include/grpcpp/completion_queue.h>
#include <contrib/libs/grpc/include/grpcpp/resource_quota.h>
#include <contrib/libs/grpc/include/grpcpp/security/auth_metadata_processor.h>
#include <contrib/libs/grpc/include/grpcpp/security/server_credentials.h>
#include <contrib/libs/grpc/include/grpcpp/server.h>
#include <contrib/libs/grpc/include/grpcpp/server_builder.h>
#include <contrib/libs/grpc/include/grpcpp/server_context.h>
#include <contrib/libs/grpc/include/grpcpp/server_posix.h>
#include <contrib/libs/grpc/include/grpcpp/support/status.h>

#include <ydb/library/actors/prof/tag.h>

#include <library/cpp/string_utils/quote/quote.h>

#include <util/datetime/cputimer.h>
#include <util/folder/path.h>
#include <util/generic/hash_set.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/network/init.h>
#include <util/string/join.h>
#include <util/system/file.h>
#include <util/system/mutex.h>
#include <util/system/spinlock.h>
#include <util/system/thread.h>

namespace NCloud::NBlockStore::NServer {

using namespace NMonitoring;
using namespace NThreading;

using NYdb::NBS::E_ARGUMENT;
using NYdb::NBS::E_FAIL;
using NYdb::NBS::E_GRPC_UNAVAILABLE;
using NYdb::NBS::E_GRPC_UNIMPLEMENTED;
using NYdb::NBS::E_REJECTED;
using NYdb::NBS::FACILITY_GRPC;
using NYdb::NBS::CreateInsecureServerCredentials;
using NYdb::NBS::EnqueueCompletion;
using NYdb::NBS::FormatError;
using NYdb::NBS::GetAuthToken;
using NYdb::NBS::GetRequestSource;
using NYdb::NBS::HasError;
using NYdb::NBS::ILoggingServicePtr;
using NYdb::NBS::SEVERITY_ERROR;
using NYdb::NBS::ICertificateProviderPtr;
using NYdb::NBS::TAuthMetadataProcessor;
using NYdb::NBS::TErrorResponse;
using NYdb::NBS::TGrpcInitializer;
using NYdb::NBS::TKeepAliveOption;
using NYdb::NBS::TRequestSourceKinds;
using NYdb::NBS::TServiceError;
using NYdb::NBS::TryParseSourceFd;

namespace NImpl {

////////////////////////////////////////////////////////////////////////////////

void PrepareRequestHeaders(
    NCloud::NProto::ERequestSource source,
    TStringBuf peer,
    TStringBuf authToken,
    NProto::THeaders& headers)
{
    auto& internal = *headers.MutableInternal();

    internal.Clear();
    internal.SetRequestSource(source);
    internal.SetPeer(UrlUnescapeRet(peer));

    if (source == NProto::SOURCE_SECURE_CONTROL_CHANNEL) {
        internal.SetAuthToken(TString(authToken));
    }
}

}   // namespace NImpl

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

namespace NHeaders {
    const grpc::string ClientId = "x-nbs-client-id";
    const grpc::string IdempotenceId = "x-nbs-idempotence-id";
    const grpc::string RequestId = "x-nbs-request-id";
    const grpc::string Timestamp = "x-nbs-timestamp";
    const grpc::string TraceId = "x-nbs-trace-id";
    const grpc::string RequestTimeout = "x-nbs-request-timeout";
}

////////////////////////////////////////////////////////////////////////////////

const TRequestSourceKinds RequestSourceKinds = {
    { "INSECURE_CONTROL_CHANNEL",
      NYdb::NBS::NProto::SOURCE_INSECURE_CONTROL_CHANNEL },
    { "SECURE_CONTROL_CHANNEL",
      NYdb::NBS::NProto::SOURCE_SECURE_CONTROL_CHANNEL },
    { "TCP_DATA_CHANNEL", NYdb::NBS::NProto::SOURCE_TCP_DATA_CHANNEL },
    { "FD_DATA_CHANNEL", NYdb::NBS::NProto::SOURCE_FD_DATA_CHANNEL },
    { "FD_CONTROL_CHANNEL", NYdb::NBS::NProto::SOURCE_FD_CONTROL_CHANNEL },
};

////////////////////////////////////////////////////////////////////////////////

struct TServerRequestHandlerBase
    : public NYdb::NBS::NGrpc::TRequestHandlerBase
{
    TCallContextPtr CallContext = MakeIntrusive<TCallContext>();

    TMetricRequest MetricRequest;

    enum {
        WaitingForRequest = 0,
        ExecutingRequest = 1,
        ExecutionCompleted = 2,
        ExecutionCancelled = 3,
        SendingResponse = 4,
        RequestCompleted = 5,
    };

    TAtomic RequestState = WaitingForRequest;

    NProto::TError Error;

    TServerRequestHandlerBase(EBlockStoreRequest requestType)
        : MetricRequest(requestType)
    {}
};

////////////////////////////////////////////////////////////////////////////////

class TSessionStorage;

struct TAppContext
{
    TServerAppConfigPtr Config;
    ILoggingServicePtr Logging;
    ICertificateProviderPtr CertificateProvider;
    TLog Log;
    IBlockStorePtr Service;
    IBlockStorePtr UdsService;

    std::unique_ptr<grpc::Server> Server;

    std::shared_ptr<TSessionStorage> SessionStorage;

    IServerStatsPtr ServerStats;

    TAtomic ShouldStop = 0;

    TString CellId;
};

////////////////////////////////////////////////////////////////////////////////

using TRequestsInFlight =
    NYdb::NBS::NGrpc::TRequestsInFlight<TServerRequestHandlerBase>;

using TExecutorContext = NYdb::NBS::NGrpc::
    TExecutorContext<grpc::ServerCompletionQueue, TRequestsInFlight>;

using TExecutor = NYdb::NBS::NGrpc::TExecutor<
    grpc::ServerCompletionQueue,
    TRequestsInFlight,
    NYdb::NBS::TExecutorCounters::TExecutorScope>;

////////////////////////////////////////////////////////////////////////////////

template <typename TService>
constexpr bool IsDataService()
{
    return false;
}

template <>
constexpr bool IsDataService<NProto::TBlockStoreDataService::AsyncService>()
{
    return true;
}

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_DECLARE_METHOD(name, ...)                                   \
    struct T##name##Method                                                     \
    {                                                                          \
        static constexpr EBlockStoreRequest Request = EBlockStoreRequest::name;\
                                                                               \
        using TRequest = NProto::T##name##Request;                             \
        using TResponse = NProto::T##name##Response;                           \
                                                                               \
        template <typename T, typename ...TArgs>                               \
        static void Prepare(T& service, TArgs&& ...args)                       \
        {                                                                      \
            service.Request##name(std::forward<TArgs>(args)...);               \
        }                                                                      \
                                                                               \
        template <typename T, typename ...TArgs>                               \
        static TFuture<TResponse> Execute(T& service, TArgs&& ...args)         \
        {                                                                      \
            return service.name(std::forward<TArgs>(args)...);                 \
        }                                                                      \
    };                                                                         \
// BLOCKSTORE_DECLARE_METHOD

BLOCKSTORE_GRPC_SERVICE(BLOCKSTORE_DECLARE_METHOD)

#undef BLOCKSTORE_DECLARE_METHOD

////////////////////////////////////////////////////////////////////////////////

using NYdb::NBS::NServer::IClientStorage;
using NYdb::NBS::NServer::IClientStoragePtr;

////////////////////////////////////////////////////////////////////////////////

class TSessionStorage;

IClientStoragePtr CreateEndpointClientStorage(
    const std::shared_ptr<TSessionStorage>& sessionStorage,
    IBlockStorePtr service);

////////////////////////////////////////////////////////////////////////////////

class TSessionStorage final
    : public IClientStorageFactory
    , public std::enable_shared_from_this<TSessionStorage>
{
    struct TClientInfo
    {
        ui32 Fd = 0;
        IBlockStorePtr Service;
        NProto::ERequestSource Source = NProto::SOURCE_FD_DATA_CHANNEL;
    };

private:
    TAppContext& AppCtx;

    TMutex Lock;
    THashMap<ui32, TClientInfo> ClientInfos;

public:
    TSessionStorage(TAppContext& appCtx)
        : AppCtx(appCtx)
    {}

    void AddClient(
        const TSocketHolder& socket,
        IBlockStorePtr sessionService,
        NProto::ERequestSource source)
    {
        if (AtomicGet(AppCtx.ShouldStop)) {
            return;
        }

        TSocketHolder dupSocket;

        with_lock (Lock) {
            auto it = FindClient(socket);
            Y_ABORT_UNLESS(it == ClientInfos.end());

            // create duplicate socket. we poll socket to know when
            // client disconnects and dupSocket is passed to GRPC to read
            // messages.
            dupSocket = SafeCreateDuplicate(socket);

            auto client = TClientInfo {
                (ui32)socket,
                std::move(sessionService),
                source
            };
            auto res = ClientInfos.emplace((ui32)dupSocket, std::move(client));
            Y_ABORT_UNLESS(res.second);
        }

        TLog& Log = AppCtx.Log;
        STORAGE_DEBUG("Accept client. Unix socket fd = " << (ui32)dupSocket);
        grpc::AddInsecureChannelFromFd(AppCtx.Server.get(), dupSocket.Release());
    }

    void RemoveClient(const TSocketHolder& socket)
    {
        if (AtomicGet(AppCtx.ShouldStop)) {
            return;
        }

        with_lock (Lock) {
            auto it = FindClient(socket);
            Y_ABORT_UNLESS(it != ClientInfos.end());
            ClientInfos.erase(it);
        }
    }

    IBlockStorePtr GetSessionService(ui32 fd, NProto::ERequestSource& source)
    {
        with_lock (Lock) {
            auto it = ClientInfos.find(fd);
            if (it != ClientInfos.end()) {
                const auto& clientInfo = it->second;
                source = clientInfo.Source;
                return clientInfo.Service;
            }
        }
        return nullptr;
    }

    IClientStoragePtr CreateClientStorage(IBlockStorePtr service)
    {
        return CreateEndpointClientStorage(
            shared_from_this(),
            std::move(service));
    }

private:
    static TSocketHolder CreateDuplicate(const TSocketHolder& socket)
    {
        auto fileHandle = TFileHandle(socket);
        auto duplicateFd = fileHandle.Duplicate();
        fileHandle.Release();
        return TSocketHolder(duplicateFd);
    }

    // Grpc can release socket before we call RemoveClient. So it is possible
    // to observe the situation when CreateDuplicate returns fd which was not
    // yet removed from ClientInfo's. So completed RemoveClient will close fd
    // related to another connection.
    TSocketHolder SafeCreateDuplicate(const TSocketHolder& socket)
    {
        TList<TSocketHolder> holders;

        while (true) {
            TSocketHolder holder = CreateDuplicate(socket);
            if (ClientInfos.find(holder) == ClientInfos.end()) {
                return holder;
            }

            holders.push_back(std::move(holder));
        }
    }

    THashMap<ui32, TClientInfo>::iterator FindClient(const TSocketHolder& socket)
    {
        ui32 fd = socket;
        for (auto it = ClientInfos.begin(); it != ClientInfos.end(); ++it) {
            const auto& clientInfo = it->second;
            if (clientInfo.Fd == fd) {
                return it;
            }
        }
        return ClientInfos.end();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TClientStorage final
    : public IClientStorage
{
    std::shared_ptr<TSessionStorage> Storage;
    IBlockStorePtr Service;

public:
    TClientStorage(
            std::shared_ptr<TSessionStorage> storage,
            IBlockStorePtr service)
        : Storage(std::move(storage))
        , Service(std::move(service))
    {}

    void AddClient(
        const TSocketHolder& clientSocket,
        NYdb::NBS::NProto::ERequestSource source) override
    {
        Storage->AddClient(
            clientSocket,
            Service,
            static_cast<NCloud::NProto::ERequestSource>(source));
    }

    void RemoveClient(const TSocketHolder& clientSocket) override
    {
        Storage->RemoveClient(clientSocket);
    }
};

IClientStoragePtr CreateEndpointClientStorage(
    const std::shared_ptr<TSessionStorage>& sessionStorage,
    IBlockStorePtr service)
{
    return std::make_shared<TClientStorage>(
        sessionStorage,
        std::move(service));
}

////////////////////////////////////////////////////////////////////////////////

template<typename TMethod>
struct TRequestDataHolder
{};

template<>
struct TRequestDataHolder<TMountVolumeMethod>
{
    TString ClientId;
    TString InstanceId;
};

template<>
struct TRequestDataHolder<TUnmountVolumeMethod>
{
    TString DiskId;
    TString ClientId;
};

template<>
struct TRequestDataHolder<TAlterVolumeMethod>
{
    TString DiskId;
    TString CloudId;
    TString FolderId;
};

////////////////////////////////////////////////////////////////////////////////

template <typename TService, typename TMethod>
class TRequestHandler final
    : public TServerRequestHandlerBase
{
    using TRequest = typename TMethod::TRequest;
    using TResponse = typename TMethod::TResponse;

private:
    TAppContext& AppCtx;
    TExecutorContext& ExecCtx;
    TService& Service;

    std::unique_ptr<grpc::ServerContext> Context = std::make_unique<grpc::ServerContext>();
    grpc::ServerAsyncResponseWriter<TResponse> Writer;

    std::shared_ptr<TRequest> Request = std::make_shared<TRequest>();
    TFuture<TResponse> Response;

    static constexpr ui32 InvalidFd = INVALID_SOCKET;
    ui32 SourceFd = InvalidFd;
    IBlockStorePtr SessionService;

    [[no_unique_address]] TRequestDataHolder<TMethod> DataHolder;

public:
    TRequestHandler(
            TExecutorContext& execCtx,
            TAppContext& appCtx,
            TService& service)
        : TServerRequestHandlerBase(TMethod::Request)
        , AppCtx(appCtx)
        , ExecCtx(execCtx)
        , Service(service)
        , Writer(Context.get())
    {}

    static void Start(
        TExecutorContext& execCtx,
        TAppContext& appCtx,
        TService& service)
    {
        using THandler = TRequestHandler<TService, TMethod>;
        execCtx.StartRequestHandler<THandler>(appCtx, service);
    }

    void Process(bool ok) override
    {
        if (AtomicGet(AppCtx.ShouldStop) == 0 &&
            AtomicGet(RequestState) == WaitingForRequest)
        {
            // There always should be handler waiting for request.
            // Spawn new request only when handling request from server queue.
            Start(ExecCtx, AppCtx, Service);
        }

        if (!ok) {
            auto prevState = AtomicSwap(&RequestState, RequestCompleted);
            if (prevState != WaitingForRequest) {
                CompleteRequest();
            }
        }

        for (;;) {
            switch (AtomicGet(RequestState)) {
                case WaitingForRequest:
                    if (AtomicCas(&RequestState, ExecutingRequest, WaitingForRequest)) {
                        // fix NBS-2490
                        if (AtomicGet(AppCtx.ShouldStop)) {
                            Cancel();
                            return;
                        }

                        PrepareRequestContext();
                        ExecuteRequest();

                        // request is in progress now
                        return;
                    }
                    break;

                case ExecutionCompleted:
                    if (AtomicCas(&RequestState, SendingResponse, ExecutionCompleted)) {
                        try {
                            const auto& response = Response.GetValue();

                            // 'mute' fatal errors for stopped endpoints
                            if (HasError(response) && EndpointIsStopped()) {
                                SendResponse(TErrorResponse(E_GRPC_UNAVAILABLE, TStringBuilder()
                                    << "Endpoint has been stopped (fd = " << SourceFd << ")."
                                    << " Service error: " << response.GetError()));
                            } else {
                                SendResponse(response);
                            }
                        } catch (const TServiceError& e) {
                            SendResponse(GetErrorResponse(e));
                        } catch (...) {
                            SendResponse(GetErrorResponse(CurrentExceptionMessage()));
                        }

                        // request is in progress now
                        return;
                    }
                    break;

                case ExecutionCancelled:
                    if (AtomicCas(&RequestState, SendingResponse, ExecutionCancelled)) {
                        // cancel inflight requests due to server shutting down
                        SendError(grpc::Status(
                            grpc::StatusCode::UNAVAILABLE,
                            "Server shutting down"));

                        // request is in progress now
                        return;
                    }
                    break;

                case SendingResponse:
                    if (AtomicCas(&RequestState, RequestCompleted, SendingResponse)) {
                        CompleteRequest();
                    }
                    break;

                case RequestCompleted:
                    // request completed and could be safely destroyed
                    ExecCtx.RequestsInFlight.Unregister(this);

                    // free grpc server context as soon as we completed request
                    // because request handler may be destructed later than
                    // grpc shutdown
                    Context.reset();
                    return;
            }
        }
    }

    void Cancel() override
    {
        if (AtomicCas(&RequestState, ExecutionCancelled, ExecutingRequest)) {
            // will be processed on executor thread
            EnqueueCompletion(ExecCtx.CompletionQueue.get(), AcquireCompletionTag());
            return;
        }

        if (Context->c_call()) {
            Context->TryCancel();
        }
    }

    void PrepareRequest()
    {
        TMethod::Prepare(
            Service,
            Context.get(),
            Request.get(),
            &Writer,
            ExecCtx.CompletionQueue.get(),
            ExecCtx.CompletionQueue.get(),
            this);
    }

private:
    void PrepareRequestContext()
    {
        auto& headers = *Request->MutableHeaders();

        const auto& metadata = Context->client_metadata();
        if (!metadata.empty()) {
            if (auto value = GetMetadata(metadata, NHeaders::ClientId)) {
                headers.SetClientId(TString{value});
            }
            if (auto value = GetMetadata(metadata, NHeaders::IdempotenceId)) {
                headers.SetIdempotenceId(TString{value});
            }
            if (auto value = GetMetadata(metadata, NHeaders::TraceId)) {
                headers.SetTraceId(TString{value});
            }
            if (auto value = GetMetadata(metadata, NHeaders::RequestId)) {
                ui64 requestId;
                if (TryFromString(value, requestId)) {
                    headers.SetRequestId(requestId);
                }
            }
            if (auto value = GetMetadata(metadata, NHeaders::Timestamp)) {
                ui64 timestamp;
                if (TryFromString(value, timestamp)) {
                    headers.SetTimestamp(timestamp);
                }
            }
            if (auto value = GetMetadata(metadata, NHeaders::RequestTimeout)) {
                ui32 requestTimeout;
                if (TryFromString(value, requestTimeout)) {
                    headers.SetRequestTimeout(requestTimeout);
                }
            }
        }

        auto now = TInstant::Now();

        auto timestamp = TInstant::MicroSeconds(headers.GetTimestamp());
        if (!timestamp || timestamp > now || now - timestamp > TDuration::Seconds(1)) {
            // fix request timestamp
            timestamp = now;
            headers.SetTimestamp(timestamp.MicroSeconds());
        }

        auto requestTimeout = TDuration::MilliSeconds(headers.GetRequestTimeout());
        if (!requestTimeout) {
            requestTimeout = AppCtx.Config->GetRequestTimeout();
            headers.SetRequestTimeout(requestTimeout.MilliSeconds());
        }

        CallContext->RequestId = headers.GetRequestId();
        if (!CallContext->RequestId) {
            CallContext->RequestId = CreateRequestId();
            headers.SetRequestId(CallContext->RequestId);
        }

        auto clientId = GetClientId(*Request);
        auto diskId = GetDiskId(*Request);
        ui64 startIndex = 0;
        ui64 requestBytes = 0;

        if constexpr (IsReadWriteRequest(TMethod::Request)) {
            startIndex = GetStartIndex(*Request);
            requestBytes = CalculateBytesCount(
                *Request,
                AppCtx.ServerStats->GetBlockSize(diskId));
        }

        if constexpr (std::is_same<TMethod, TDescribeVolumeMethod>()) {
            const auto& cellId = Request->GetHeaders().GetCellId();
            if (cellId) {
                MetricRequest.CellRequest = true;
            }
        }

        AppCtx.ServerStats->PrepareMetricRequest(
            MetricRequest,
            std::move(clientId),
            std::move(diskId),
            startIndex,
            requestBytes,
            false // unaligned
        );

        OnRequestPreparation(*Request, DataHolder);
    }

    void ValidateRequest()
    {
        auto authContext = Context->auth_context();
        Y_ABORT_UNLESS(authContext);

        const auto transportSource =
            GetRequestSource(*authContext, RequestSourceKinds);

        TMaybe<NProto::ERequestSource> source;
        if (transportSource) {
            source = static_cast<NProto::ERequestSource>(*transportSource);
        }

        if (!source) {
            ui32 fd = 0;
            auto peer = Context->peer();
            bool result = TryParseSourceFd(peer, &fd);

            if (!result) {
                STORAGE_THROW_SERVICE_ERROR(E_FAIL)
                    << "failed to parse request source fd: " << peer;
            }

            auto src = NProto::SOURCE_FD_DATA_CHANNEL;
            SessionService = AppCtx.SessionStorage->GetSessionService(fd, src);
            if (SessionService == nullptr) {
                STORAGE_THROW_SERVICE_ERROR(E_GRPC_UNAVAILABLE)
                    << "endpoint has been stopped (fd = " << fd << ").";
            }

            source = src;
            SourceFd = fd;
        }

        const bool isDataChannel = IsDataChannel(*source);
        const bool isDataService = IsDataService<TService>();

        if (isDataChannel != isDataService) {
            STORAGE_THROW_SERVICE_ERROR(E_GRPC_UNIMPLEMENTED)
                << "mismatched request channel";
        }

        if (Request->GetHeaders().HasInternal()) {
            STORAGE_THROW_SERVICE_ERROR(E_ARGUMENT)
                << "internal field should not be set by client";
        }

        if (*source == NProto::SOURCE_TCP_DATA_CHANNEL &&
            TMethod::Request != EBlockStoreRequest::UploadClientMetrics)
        {
            STORAGE_THROW_SERVICE_ERROR(E_ARGUMENT)
                << "unsupported request in tcp data channel: "
                << GetBlockStoreRequestName(TMethod::Request).Quote();
        }

        TString authToken;
        if (*source == NProto::SOURCE_SECURE_CONTROL_CHANNEL) {
            authToken = GetAuthToken(Context->client_metadata());
        }

        NImpl::PrepareRequestHeaders(
            *source,
            Context->peer(),
            authToken,
            *Request->MutableHeaders());

        if constexpr (std::is_same<TMethod, TDescribeVolumeMethod>()) {
            const auto& cellId = Request->GetHeaders().GetCellId();
            if (cellId && cellId != AppCtx.CellId) {
                const auto* msg = "DescribeVolume response cell id mismatch";
                ReportWrongCellIdInDescribeVolume(
                    msg,
                    {{"disk", Request->GetDiskId()},
                     {"expected", AppCtx.CellId},
                     {"actual", cellId}});

                STORAGE_THROW_SERVICE_ERROR(E_REJECTED) << msg;
            }
        }
    }

    bool EndpointIsStopped() const
    {
        if (SourceFd == InvalidFd) {
            return false;
        }

        NProto::ERequestSource source;
        auto service = AppCtx.SessionStorage->GetSessionService(SourceFd, source);
        return service == nullptr;
    }

    void ExecuteRequest()
    {
        TString message;
        if (IsControlRequest(MetricRequest.RequestType)) {
            message = TStringBuilder() << *Request;
        }

        MetricRequest.Peer = UrlUnescapeRet(Context->peer());

        AppCtx.ServerStats->RequestStarted(
            AppCtx.Log,
            MetricRequest,
            *CallContext,
            message);

        try {
            ValidateRequest();

            Response = TMethod::Execute(
                SessionService ? *SessionService : *AppCtx.Service,
                CallContext,
                std::move(Request));
        } catch (const TServiceError& e) {
            Response = MakeFuture(GetErrorResponse(e));
        } catch (...) {
            Response = MakeFuture(GetErrorResponse(CurrentExceptionMessage()));
        }

        auto* tag = AcquireCompletionTag();
        Response.Subscribe(
            [=, this] (const auto& response) {
                Y_UNUSED(response);

                if (AtomicCas(&RequestState, ExecutionCompleted, ExecutingRequest)) {
                    // will be processed on executor thread
                    EnqueueCompletion(ExecCtx.CompletionQueue.get(), tag);
                    return;
                }

                ReleaseCompletionTag();
            });
    }

    void SendResponse(const TResponse& response)
    {
        if (response.HasError()) {
            Error = response.GetError();
        }

        OnRequestCompletion(response, DataHolder);

        AppCtx.ServerStats->ResponseSent(MetricRequest, *CallContext);

        Writer.Finish(response, grpc::Status::OK, AcquireCompletionTag());
    }

    void SendError(const grpc::Status& status)
    {
        Error.SetCode(MAKE_GRPC_ERROR(status.error_code()));
        Error.SetMessage(ToString(status.error_message()));

        AppCtx.ServerStats->ResponseSent(MetricRequest, *CallContext);

        Writer.FinishWithError(status, AcquireCompletionTag());
    }

    void CompleteRequest()
    {
        AppCtx.ServerStats->RequestCompleted(
            AppCtx.Log,
            MetricRequest,
            *CallContext,
            Error);
    }

    static TStringBuf GetMetadata(
        const std::multimap<grpc::string_ref, grpc::string_ref>& metadata,
        const grpc::string_ref& key)
    {
        auto it = metadata.find(key);
        if (it != metadata.end()) {
            return { it->second.data(), it->second.size() };
        }
        return {};
    }

    TResponse GetErrorResponse(const TServiceError& e)
    {
        TResponse response;

        auto& error = *response.MutableError();
        error.SetCode(e.GetCode());
        error.SetMessage(e.what());

        return response;
    }

    static TResponse GetErrorResponse(TString message)
    {
        TResponse response;

        auto& error = *response.MutableError();
        error.SetCode(E_FAIL);
        error.SetMessage(std::move(message));

        return response;
    }


    template <typename T>
    void OnRequestPreparation(const TRequest& request, T& data)
    {
        Y_UNUSED(request);
        Y_UNUSED(data);
    }

    template <>
    void OnRequestPreparation(
        const TRequest& request,
        TRequestDataHolder<TMountVolumeMethod>& data)
    {
        data.ClientId = GetClientId(request);
        data.InstanceId = request.GetInstanceId();
    }

    template <>
    void OnRequestPreparation(
        const TRequest& request,
        TRequestDataHolder<TUnmountVolumeMethod>& data)
    {
        data.ClientId = GetClientId(request);
        data.DiskId = request.GetDiskId();
    }

    template <>
    void OnRequestPreparation(
        const TRequest& request,
        TRequestDataHolder<TAlterVolumeMethod>& data)
    {
        data.DiskId = request.GetDiskId();
        data.CloudId = request.GetCloudId();
        data.FolderId = request.GetFolderId();
    }


    template <typename T>
    void OnRequestCompletion(const TResponse& response, T& data)
    {
        Y_UNUSED(response);
        Y_UNUSED(data);
    }

    template <>
    void OnRequestCompletion(
        const TResponse& response,
        TRequestDataHolder<TMountVolumeMethod>& data)
    {
        if (!HasError(response) && response.HasVolume()) {
            AppCtx.ServerStats->MountVolume(
                response.GetVolume(),
                data.ClientId,
                data.InstanceId);
        }
    }

    template <>
    void OnRequestCompletion(
        const TResponse& response,
        TRequestDataHolder<TAlterVolumeMethod>& data)
    {
        if (!HasError(response)) {
            AppCtx.ServerStats->AlterVolume(
                data.DiskId,
                data.CloudId,
                data.FolderId);
        }
    }

    template <>
    void OnRequestCompletion(
        const TResponse& response,
        TRequestDataHolder<TUnmountVolumeMethod>& data)
    {
        if (!HasError(response)) {
            AppCtx.ServerStats->UnmountVolume(data.DiskId, data.ClientId);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TServer final
    : public TAppContext
    , public IServer
{
private:
    TGrpcInitializer GrpcInitializer;

    NProto::TBlockStoreService::AsyncService ControlService;
    NProto::TBlockStoreDataService::AsyncService DataService;

    TVector<std::unique_ptr<TExecutor>> Executors;

    std::unique_ptr<NYdb::NBS::NServer::TEndpointPoller> EndpointPoller;

public:
    TServer(
        TServerAppConfigPtr config,
        ILoggingServicePtr logging,
        IServerStatsPtr serverStats,
        IBlockStorePtr service,
        IBlockStorePtr udsService,
        TServerOptions options,
        ICertificateProviderPtr certificateProvider);

    ~TServer() override;

    void Start() override;
    void Stop() override;

    IClientStorageFactoryPtr GetClientStorageFactory() override;

    size_t CollectRequests(
        const TIncompleteRequestsCollector& collector) override;

private:
    template <typename TMethod, typename TService>
    void StartRequest(TService& service);
    void StartRequests();

    std::shared_ptr<grpc::ServerCredentials> CreateSecureServerCredentials();

    void StartListenUnixSocket(const TString& unixSocketPath, ui32 backlog);
    void StopListenUnixSocket();
};

////////////////////////////////////////////////////////////////////////////////

TServer::TServer(
    TServerAppConfigPtr config,
    ILoggingServicePtr logging,
    IServerStatsPtr serverStats,
    IBlockStorePtr service,
    IBlockStorePtr udsService,
    TServerOptions options,
    ICertificateProviderPtr certificateProvider)
{
    Config = std::move(config);
    Log = logging->CreateLog("BLOCKSTORE_SERVER");
    Logging = std::move(logging);
    CertificateProvider = std::move(certificateProvider);
    ServerStats = std::move(serverStats);
    Service = std::move(service);
    UdsService = std::move(udsService);
    SessionStorage = std::make_shared<TSessionStorage>(*this);
    CellId = std::move(options.CellId);
}

TServer::~TServer()
{
    Stop();
}

void TServer::Start()
{
    grpc::ServerBuilder builder;
    builder.RegisterService(&ControlService);
    builder.RegisterService(&DataService);

    ui32 maxMessageSize = Config->GetMaxMessageSize();
    if (maxMessageSize) {
        builder.SetMaxSendMessageSize(maxMessageSize);
        builder.SetMaxReceiveMessageSize(maxMessageSize);
    }

    ui32 memoryQuotaBytes = Config->GetMemoryQuotaBytes();
    if (memoryQuotaBytes) {
        grpc::ResourceQuota quota("memory_bound");
        quota.Resize(memoryQuotaBytes);

        builder.SetResourceQuota(quota);
    }

    if (Config->GetKeepAliveEnabled()) {
        builder.SetOption(std::make_unique<TKeepAliveOption>(
            Config->GetKeepAliveIdleTimeout(),
            Config->GetKeepAliveProbeTimeout(),
            Config->GetKeepAliveProbesCount()));
    }

    if (auto arg = Config->GetGrpcKeepAliveTime()) {
        builder.AddChannelArgument(GRPC_ARG_KEEPALIVE_TIME_MS, arg);
    }

    if (auto arg = Config->GetGrpcKeepAliveTimeout()) {
        builder.AddChannelArgument(GRPC_ARG_KEEPALIVE_TIMEOUT_MS, arg);
    }

    if (auto arg = Config->GetGrpcKeepAlivePermitWithoutCalls()) {
        builder.AddChannelArgument(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS, arg);
    }

    if (auto arg = Config->GetGrpcHttp2MinRecvPingIntervalWithoutData()) {
        builder.AddChannelArgument(GRPC_ARG_HTTP2_MIN_RECV_PING_INTERVAL_WITHOUT_DATA_MS, arg);
    }

    if (auto arg = Config->GetGrpcHttp2MinSentPingIntervalWithoutData()) {
        builder.AddChannelArgument(GRPC_ARG_HTTP2_MIN_SENT_PING_INTERVAL_WITHOUT_DATA_MS, arg);
    }

    if (auto port = Config->GetPort()) {
        auto address = Join(":", Config->GetHost(), port);
        STORAGE_INFO("Listen on (insecure control) " << address);

        auto credentials = CreateInsecureServerCredentials();
        credentials->SetAuthMetadataProcessor(
            std::make_shared<TAuthMetadataProcessor>(
                RequestSourceKinds,
                NYdb::NBS::NProto::SOURCE_INSECURE_CONTROL_CHANNEL));

        builder.AddListeningPort(
            address,
            std::move(credentials));
    }

    if (auto port = Config->GetSecurePort()) {
        auto host = Config->GetSecureHost() ? Config->GetSecureHost() : Config->GetHost();
        auto address = Join(":", host, port);
        STORAGE_INFO("Listen on (secure control) " << address);

        auto credentials = CreateSecureServerCredentials();
        credentials->SetAuthMetadataProcessor(
            std::make_shared<TAuthMetadataProcessor>(
                RequestSourceKinds,
                NYdb::NBS::NProto::SOURCE_SECURE_CONTROL_CHANNEL));

        builder.AddListeningPort(
            address,
            std::move(credentials));
    }

    if (auto port = Config->GetDataPort()) {
        auto address = Join(":", Config->GetDataHost(), port);
        STORAGE_INFO("Listen on (data) " << address);

        auto credentials = CreateInsecureServerCredentials();
        credentials->SetAuthMetadataProcessor(
            std::make_shared<TAuthMetadataProcessor>(
                RequestSourceKinds,
                NYdb::NBS::NProto::SOURCE_TCP_DATA_CHANNEL));

        builder.AddListeningPort(
            address,
            std::move(credentials));
    }

    ui32 threadsCount = Config->GetThreadsCount();
    for (size_t i = 1; i <= threadsCount; ++i) {
        auto executor = std::make_unique<TExecutor>(
            TStringBuilder() << "SRV" << i,
            builder.AddCompletionQueue(),
            Log,
            ServerStats->StartExecutor());
        executor->Start();
        Executors.push_back(std::move(executor));
    }

    Server = builder.BuildAndStart();
    if (!Server) {
        STORAGE_THROW_SERVICE_ERROR(E_FAIL)
            << "could not start gRPC server";
    }

    auto unixSocketPath = Config->GetUnixSocketPath();
    if (unixSocketPath) {
        ui32 backlog = Config->GetUnixSocketBacklog();
        StartListenUnixSocket(unixSocketPath, backlog);
    }

    StartRequests();
}

std::shared_ptr<grpc::ServerCredentials> TServer::CreateSecureServerCredentials()
{
    return CertificateProvider->CreateSecureServerCredentials();
}

void TServer::StartListenUnixSocket(
    const TString& unixSocketPath,
    ui32 backlog)
{
    STORAGE_INFO("Listen on (control) " << unixSocketPath.Quote());

    EndpointPoller =
        std::make_unique<NYdb::NBS::NServer::TEndpointPoller>();
    EndpointPoller->Start();

    auto error = EndpointPoller->StartListenEndpoint(
        unixSocketPath,
        backlog,
        S_IRGRP | S_IWGRP | S_IRUSR | S_IWUSR, // accessMode
        true,   // multiClient
        NYdb::NBS::NProto::SOURCE_FD_CONTROL_CHANNEL,
        SessionStorage->CreateClientStorage(UdsService));

    if (HasError(error)) {
        ReportEndpointStartingError(
            FormatError(error),
            {{"unix_socket_path", unixSocketPath}});
        StopListenUnixSocket();
    }
}

void TServer::StopListenUnixSocket()
{
    if (EndpointPoller) {
        EndpointPoller->Stop();
        EndpointPoller.reset();
    }
}

void TServer::Stop()
{
    if (AtomicSwap(&ShouldStop, 1) == 1) {
        return;
    }

    STORAGE_INFO("Shutting down");

    StopListenUnixSocket();

    auto deadline = Config->GetShutdownTimeout().ToDeadLine();

    if (Server) {
        Server->Shutdown(deadline);
    }

    for (;;) {
        size_t requestsCount = 0;
        for (auto& executor: Executors) {
            requestsCount += executor->RequestsInFlight.GetCount();
        }

        if (!requestsCount) {
            break;
        }

        if (deadline <= TInstant::Now()) {
            STORAGE_WARN("Some requests are still active on shutdown: " << requestsCount);
            break;
        }

        Sleep(TDuration::MilliSeconds(100));
    }

    for (auto& executor: Executors) {
        executor->Shutdown();
    }

    Executors.clear();
}

IClientStorageFactoryPtr TServer::GetClientStorageFactory()
{
    return SessionStorage;
}

template <typename TMethod, typename TService>
void TServer::StartRequest(TService& service)
{
    ui32 preparedRequestsCount = Config->GetPreparedRequestsCount();
    for (auto& executor: Executors) {
        for (size_t i = 0; i < preparedRequestsCount; ++i) {
            TRequestHandler<TService, TMethod>::Start(*executor, *this, service);
        }
    }
}

void TServer::StartRequests()
{
#define BLOCKSTORE_START_REQUEST(name, service, ...)                           \
    StartRequest<T##name##Method>(service);                                    \
// BLOCKSTORE_START_REQUEST

    BLOCKSTORE_GRPC_SERVICE(BLOCKSTORE_START_REQUEST, ControlService)
    BLOCKSTORE_GRPC_DATA_SERVICE(BLOCKSTORE_START_REQUEST, DataService)

#undef BLOCKSTORE_START_REQUEST
}

size_t TServer::CollectRequests(const TIncompleteRequestsCollector& collector)
{
    size_t count = 0;
    for (auto& executor: Executors) {
        const auto now = GetCycleCount();
        executor->RequestsInFlight.ForEach([&](const auto* handler) {
            auto requestTime = handler->CallContext->CalcRequestTime(now);
            if (requestTime) {
                collector(
                    *handler->CallContext,
                    handler->MetricRequest.VolumeInfo,
                    handler->MetricRequest.MediaKind,
                    handler->MetricRequest.RequestType,
                    requestTime);
            }
            ++count;
        });
    }
    return count;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IServerPtr CreateServer(
    TServerAppConfigPtr config,
    ILoggingServicePtr logging,
    IServerStatsPtr serverStats,
    IBlockStorePtr service,
    IBlockStorePtr udsService,
    TServerOptions options,
    ICertificateProviderPtr certificateProvider)
{
    return std::make_shared<TServer>(
        std::move(config),
        std::move(logging),
        std::move(serverStats),
        std::move(service),
        std::move(udsService),
        std::move(options),
        std::move(certificateProvider));
}

}   // namespace NCloud::NBlockStore::NServer
