#pragma once

#include <ydb/public/sdk/cpp/src/client/impl/internal/internal_header.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/common_client/ssl_credentials.h>

#include "actions.h"
#include "params.h"

#include <ydb/public/api/grpc/ydb_discovery_v1.grpc.pb.h>
#include <ydb/public/sdk/cpp/src/client/impl/internal/common/client_pid.h>
#include <ydb/public/sdk/cpp/src/client/impl/internal/db_driver_state/state.h>
#include <ydb/public/sdk/cpp/src/client/impl/internal/rpc_request_settings/settings.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/resources/ydb_resources.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/extension_common/extension.h>

#include <ydb/public/sdk/cpp/src/library/issue/yql_issue_message.h>

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <optional>

namespace NYdb::inline Dev {

namespace NMetrics {
    class IMetricRegistry;
} // namespace NMetrics

namespace NTrace {
    class ITraceProvider;
} // namespace NTrace

constexpr TDeadline::Duration GRPC_KEEP_ALIVE_TIMEOUT_FOR_DISCOVERY = std::chrono::seconds(10);
constexpr TDeadline::Duration INITIAL_DEFERRED_CALL_DELAY = std::chrono::milliseconds(10); // The delay before first deferred service call
constexpr TDeadline::Duration GET_ENDPOINTS_TIMEOUT = std::chrono::seconds(10); // Time wait for ListEndpoints request, after this time we pass error to client

using NYdbGrpc::TCallMeta;
using NYdbGrpc::IQueueClientContextPtr;
using NYdbGrpc::IQueueClientContextProvider;
using NYdbGrpc::IQueueClientCallbackGuard;
using NYdbGrpc::TQueueClientCallbackGuardFactory;

class ICredentialsProvider;

// Deferred callbacks
using TDeferredResultCb = std::function<void(google::protobuf::Any*, TPlainStatus status)>;

class TDriverStopState {
public:
    bool TryEnterCallback() noexcept;
    void LeaveCallback() noexcept;

    void WaitCallbacksDrained();
    void MarkStopped() noexcept;

private:
    std::mutex Mutex_;
    std::condition_variable Drained_;
    ui64 InFlightCallbacks_ = 0;
    bool Stopped_ = false;
};

class TSdkCallbackGuard final : public IQueueClientCallbackGuard {
public:
    explicit TSdkCallbackGuard(std::shared_ptr<TDriverStopState> stopState = {});
    ~TSdkCallbackGuard();

    bool IsEntered() const noexcept override;

private:
    std::shared_ptr<TDriverStopState> StopState_;
    bool Entered_ = false;
};

// Runs onEntered() while the driver is not stopping, otherwise onStopped().
// The single choke point behind every SDK-level guarded callback: it decides
// run-vs-substitute and keeps the in-flight-callback drain counter correct.
template<class TOnEntered, class TOnStopped>
void RunGuarded(const std::shared_ptr<TDriverStopState>& stopState, TOnEntered&& onEntered, TOnStopped&& onStopped) {
    TSdkCallbackGuard guard(stopState);
    if (guard.IsEntered()) {
        std::forward<TOnEntered>(onEntered)();
    } else {
        std::forward<TOnStopped>(onStopped)();
    }
}

std::string GetAuthInfo(TDbDriverStatePtr p);
std::string CreateSDKBuildInfo();

class TGRpcConnectionsImpl
    : public IQueueClientContextProvider
    , public IInternalClient
{
    friend class TDeferredAction;
    friend class TDriver;
    friend struct TGRpcConnectionsDeleter;
public:
    TGRpcConnectionsImpl(std::shared_ptr<IConnectionsParams> params);
    ~TGRpcConnectionsImpl();

    static bool IsCurrentThreadInSdkCallback() noexcept;

    // Runs action() now if the caller is on a normal thread, otherwise defers it to a
    // fresh thread that first waits for all in-flight callbacks to drain. Used for
    // Stop()/delete triggered from within a callback, where running inline would
    // deadlock (self-join) or free the driver under a live callback frame.
    static void DeferOrRunNow(std::shared_ptr<TDriverStopState> stopState, std::function<void()> action);

public:
    void AddPeriodicTask(TPeriodicCb&& cb, TDeadline::Duration period) override;
    void PostToResponseQueue(std::function<void()>&& f) override;

    void ScheduleDelayedTask(TSimpleCb&& fn, TDeadline deadline);
    void ScheduleDelayedTask(TSimpleCb&& fn, TDeadline::Duration delay);

    NThreading::TFuture<bool> ScheduleFuture(
        TDuration timeout,
        IQueueClientContextPtr token = nullptr
    );
    void ScheduleCallback(
        TDuration timeout,
        std::function<void(bool)> callback,
        IQueueClientContextPtr token = nullptr
    );
    // The idea is: sometimes we need to work with multiple databases simultaneously
    // This method returns DbDriverState (or just db state) for given database credentials pair
    // this state is used to keep data related to particular database.
    TDbDriverStatePtr GetDriverState(
        const std::optional<std::string>& database,
        const std::optional<std::string>& discoveryEndpoint,
        const std::optional<EDiscoveryMode>& discoveryMode,
        const std::optional<TSslCredentials>& sslCredentials,
        const std::optional<std::shared_ptr<ICredentialsProviderFactory>>& credentialsProviderFactory
    );
    IQueueClientContextPtr CreateContext() override;
    TQueueClientCallbackGuardFactory GetCallbackGuardFactory() override;
    bool TryCreateContext(IQueueClientContextPtr& context);
    void WaitIdle();
    void Stop(bool wait = false);

    template<typename TService>
    using TServiceConnection = NYdbGrpc::TServiceConnection<TService>;

    static void SetGrpcKeepAlive(NYdbGrpc::TGRpcClientConfig& config, const TDeadline::Duration& timeout, bool permitWithoutCalls);

    static void SetGrpcCompressionAlgorithm(NYdbGrpc::TGRpcClientConfig& config, EGrpcCompressionAlgorithm algorithm);

    template<typename TService>
    std::pair<std::unique_ptr<TServiceConnection<TService>>, TEndpointKey> GetServiceConnection(
        TDbDriverStatePtr dbState, const TEndpointKey& preferredEndpoint,
        TRpcRequestSettings::TEndpointPolicy endpointPolicy)
    {
        auto clientConfig = NYdbGrpc::TGRpcClientConfig(dbState->DiscoveryEndpoint);
        const auto& sslCredentials = dbState->SslCredentials;
        clientConfig.SslCredentials = {.pem_root_certs = TStringType{sslCredentials.CaCert}, .pem_private_key = TStringType{sslCredentials.PrivateKey}, .pem_cert_chain = TStringType{sslCredentials.Cert}};
        clientConfig.EnableSsl = sslCredentials.IsEnabled;

        clientConfig.MemQuota = MemoryQuota_;

        if (MaxMessageSize_ > 0) {
            clientConfig.MaxMessageSize = MaxMessageSize_;
        }
        if (MaxInboundMessageSize_ > 0) {
            clientConfig.MaxInboundMessageSize = MaxInboundMessageSize_;
        }
        if (MaxOutboundMessageSize_ > 0) {
            clientConfig.MaxOutboundMessageSize = MaxOutboundMessageSize_;
        }

        clientConfig.LoadBalancingPolicy = GRpcLoadBalancingPolicy_;

        SetGrpcCompressionAlgorithm(clientConfig, GRpcCompressionAlgorithm_);

        if (dbState->DiscoveryMode != EDiscoveryMode::Off) {
            if (std::is_same<TService,Ydb::Discovery::V1::DiscoveryService>()
                || dbState->Database.empty()
                || endpointPolicy == TRpcRequestSettings::TEndpointPolicy::UseDiscoveryEndpoint)
            {
                SetGrpcKeepAlive(clientConfig, GRPC_KEEP_ALIVE_TIMEOUT_FOR_DISCOVERY, GRpcKeepAlivePermitWithoutCalls_);
            } else {
                auto endpoint = dbState->EndpointPool.GetEndpoint(preferredEndpoint, endpointPolicy == TRpcRequestSettings::TEndpointPolicy::UsePreferredEndpointStrictly);
                if (!endpoint) {
                    return {nullptr, TEndpointKey()};
                }
                clientConfig.Locator = endpoint.Endpoint;
                clientConfig.SslTargetNameOverride = endpoint.SslTargetNameOverride;
                if (GRpcKeepAliveTimeout_ > TDeadline::Duration::zero()) {
                    SetGrpcKeepAlive(clientConfig, GRpcKeepAliveTimeout_, GRpcKeepAlivePermitWithoutCalls_);
                }
            }
        }

        if (UsePerChannelTcpConnection_) {
            clientConfig.IntChannelParams[GRPC_ARG_USE_LOCAL_SUBCHANNEL_POOL] = 1;
        }

        std::unique_ptr<TServiceConnection<TService>> conn;
#ifndef YDB_GRPC_BYPASS_CHANNEL_POOL
        ChannelPool_.GetStubsHolderLocked(
            clientConfig.Locator, clientConfig, [&conn, this](NYdbGrpc::TStubsHolder& holder) mutable {
            conn.reset(GRpcClientLow_.CreateGRpcServiceConnection<TService>(holder).release());
        });
#else
        conn = std::move(GRpcClientLow_.CreateGRpcServiceConnection<TService>(clientConfig));
#endif
        return {std::move(conn), TEndpointKey(clientConfig.Locator, 0)};
    }

    template<class TService, class TRequest, class TResponse>
    using TSimpleRpc =
        typename NYdbGrpc::TSimpleRequestProcessor<
            typename TService::Stub,
            TRequest,
            TResponse>::TAsyncRequest;

    template<typename TResponse>
    void RunResponseCallback(
        TResponseCb<TResponse>& callback,
        TResponse* response,
        TPlainStatus status,
        const std::shared_ptr<TDriverStopState>& stopState)
    {
        RunGuarded(stopState,
            [&] { callback(response, std::move(status)); },
            [&] { callback(nullptr, MakeClientStoppedStatus()); });
    }

    template<typename TCallback, typename TProcessor>
    void RunStreamCallback(
        TCallback& callback,
        TPlainStatus status,
        TProcessor processor,
        const std::shared_ptr<TDriverStopState>& stopState)
    {
        RunGuarded(stopState,
            [&] { callback(std::move(status), std::move(processor)); },
            [&] { callback(MakeClientStoppedStatus(), nullptr); });
    }

    template<typename TService, typename TCallback>
    void RunServiceConnectionCallback(
        TCallback& callback,
        TPlainStatus status,
        std::unique_ptr<TServiceConnection<TService>> serviceConnection,
        TEndpointKey endpoint,
        const std::shared_ptr<TDriverStopState>& stopState)
    {
        RunGuarded(stopState,
            [&] { callback(std::move(status), std::move(serviceConnection), std::move(endpoint)); },
            [&] { callback(MakeClientStoppedStatus(), std::unique_ptr<TServiceConnection<TService>>{nullptr}, TEndpointKey{}); });
    }

    template<typename TRequest>
    class TRequestWrapper {
    public:
        // Implicit conversion from rvalue reference
        TRequestWrapper(TRequest&& request) 
            : Storage_(std::move(request))
        {}

        // Implicit conversion from pointer. Means that request is allocated on Arena
        TRequestWrapper(TRequest* request)
            : Storage_(request)
        {}

        // Copy constructor
        TRequestWrapper(const TRequestWrapper& other) = default;
        
        // Move constructor
        TRequestWrapper(TRequestWrapper&& other) = default;
        
        // Copy assignment
        TRequestWrapper& operator=(const TRequestWrapper& other) = default;
        
        // Move assignment
        TRequestWrapper& operator=(TRequestWrapper&& other) = default;

        template<typename TService, typename TResponse>
        void DoRequest(
            std::unique_ptr<TServiceConnection<TService>>& serviceConnection,
            NYdbGrpc::TAdvancedResponseCallback<TResponse>&& responseCbLow,
            typename NYdbGrpc::TSimpleRequestProcessor<typename TService::Stub, TRequest, TResponse>::TAsyncRequest rpc,
            const TCallMeta& meta,
            IQueueClientContext* context) 
        {
            if (auto ptr = std::get_if<TRequest*>(&Storage_)) {
                serviceConnection->DoAdvancedRequest(**ptr, 
                    std::move(responseCbLow), rpc, meta, context);
            } else {
                serviceConnection->DoAdvancedRequest(std::move(std::get<TRequest>(Storage_)), 
                    std::move(responseCbLow), rpc, meta, context);
            }
        }

    private:
        std::variant<TRequest*, TRequest> Storage_;
    };

    template<typename TService, typename TRequest, typename TResponse>
    void Run(
        TRequestWrapper<TRequest>&& requestWrapper,
        TResponseCb<TResponse>&& userResponseCb,
        TSimpleRpc<TService, TRequest, TResponse> rpc,
        TDbDriverStatePtr dbState,
        const TRpcRequestSettings& requestSettings,
        std::shared_ptr<IQueueClientContext> context = nullptr)
    {
        using NYdbGrpc::TGrpcStatus;
        using TConnection = std::unique_ptr<TServiceConnection<TService>>;
        Y_ABORT_UNLESS(dbState);

        if (auto tlsValidationStatus = ValidateClientTlsCredentials(dbState)) {
            RunResponseCallback<TResponse>(userResponseCb, nullptr, std::move(*tlsValidationStatus), StopState_);
            return;
        }

        if (!TryCreateContext(context)) {
            RunResponseCallback<TResponse>(userResponseCb, nullptr, MakeClientStoppedStatus(), StopState_);
            return;
        }

        if (dbState->StatCollector.IsCollecting()) {
            std::weak_ptr<TDbDriverState> weakState = dbState;
            const auto startTime = TInstant::Now();
            userResponseCb = std::move([cb = std::move(userResponseCb), weakState, startTime](TResponse* response, TPlainStatus status) {
                Y_ABORT_UNLESS(!status.Ok() || response);
                const auto resultSize = response ? response->ByteSizeLong() : 0;
                cb(response, status);

                if (auto state = weakState.lock()) {
                    state->StatCollector.IncRequestLatency(TInstant::Now() - startTime);
                    state->StatCollector.IncResultSize(resultSize);
                }
            });
        }

        WithServiceConnection<TService>(
            [this, requestWrapper = std::move(requestWrapper), userResponseCb = std::move(userResponseCb), rpc, 
             requestSettings, context = std::move(context), dbState]
                (TPlainStatus status, TConnection serviceConnection, TEndpointKey endpoint) mutable -> void {
                    if (!status.Ok()) {
                        context.reset();
                        RunResponseCallback<TResponse>(userResponseCb, nullptr, std::move(status), StopState_);
                        return;
                    }

                    Y_ABORT_UNLESS(serviceConnection != nullptr);

                    TCallMeta meta;

                    try {
                        meta = MakeCallMeta(requestSettings, dbState);
                    } catch (const TYdbException& e) {
                        context.reset();
                        RunResponseCallback<TResponse>(
                            userResponseCb,
                            nullptr,
                            TPlainStatus(dynamic_cast<const TAuthenticationError*>(&e) ? EStatus::CLIENT_UNAUTHENTICATED : EStatus::UNAVAILABLE, e.what()),
                            StopState_);
                        return;
                    }

                    dbState->StatCollector.IncGRpcInFlight();
                    dbState->StatCollector.IncGRpcInFlightByHost(endpoint.GetEndpoint());

                    NYdbGrpc::TAdvancedResponseCallback<TResponse> responseCbLow =
                        [this, context, userResponseCb = std::move(userResponseCb), endpoint, dbState]
                        (const grpc::ClientContext& ctx, TGrpcStatus&& grpcStatus, TResponse&& response) mutable -> void {
                            dbState->StatCollector.DecGRpcInFlight();
                            dbState->StatCollector.DecGRpcInFlightByHost(endpoint.GetEndpoint());

                            if (NYdbGrpc::IsGRpcStatusGood(grpcStatus)) {
                                std::multimap<std::string, std::string> metadata;

                                for (const auto& [name, value] : ctx.GetServerInitialMetadata()) {
                                    metadata.emplace(
                                        std::string(name.begin(), name.end()),
                                        std::string(value.begin(), value.end()));
                                }
                                for (const auto& [name, value] : ctx.GetServerTrailingMetadata()) {
                                    metadata.emplace(
                                        std::string(name.begin(), name.end()),
                                        std::string(value.begin(), value.end()));
                                }

                                auto resp = new TResult<TResponse>(
                                    std::move(response),
                                    std::move(grpcStatus),
                                    std::move(userResponseCb),
                                    this,
                                    std::move(context),
                                    endpoint.GetEndpoint(),
                                    std::move(metadata));

                                EnqueueResponse(resp);
                            } else {
                                dbState->StatCollector.IncReqFailDueTransportError();
                                dbState->StatCollector.IncTransportErrorsByHost(endpoint.GetEndpoint());

                                auto resp = new TGRpcErrorResponse<TResponse>(
                                    std::move(grpcStatus),
                                    std::move(userResponseCb),
                                    this,
                                    std::move(context),
                                    endpoint.GetEndpoint());

                                dbState->EndpointPool.BanEndpoint(endpoint.GetEndpoint());

                                EnqueueResponse(resp);
                            }
                        };

                    requestWrapper.DoRequest(serviceConnection, std::move(responseCbLow), rpc, meta, context.get());
            }, dbState, requestSettings.PreferredEndpoint, requestSettings.EndpointPolicy);
    }

    template<typename TService, typename TRequest, typename TResponse>
    void RunDeferred(
        TRequestWrapper<TRequest>&& requestWrapper,
        TDeferredOperationCb&& userResponseCb,
        TSimpleRpc<TService, TRequest, TResponse> rpc,
        TDbDriverStatePtr dbState,
        TDeadline::Duration delay,
        const TRpcRequestSettings& requestSettings,
        bool poll = false,
        std::shared_ptr<IQueueClientContext> context = nullptr)
    {
        if (!TryCreateContext(context)) {
            userResponseCb(nullptr, MakeClientStoppedStatus());
            return;
        }

        auto responseCb = [this, userResponseCb = std::move(userResponseCb), dbState, delay, deadline = requestSettings.Deadline, poll, context]
            (TResponse* response, TPlainStatus status) mutable
        {
            if (response) {
                Ydb::Operations::Operation* operation = response->mutable_operation();
                Y_ABORT_UNLESS(operation);
                if (!operation->ready() && poll) {
                    auto action = MakeIntrusive<TDeferredAction>(
                        operation->id(),
                        std::move(userResponseCb),
                        this,
                        std::move(context),
                        delay,
                        deadline,
                        dbState,
                        status.Endpoint);

                    action->Start();
                } else {
                    NYdb::NIssue::TIssues opIssues;
                    NYdb::NIssue::IssuesFromMessage(operation->issues(), opIssues);
                    context.reset();
                    userResponseCb(operation, TPlainStatus{static_cast<EStatus>(operation->status()), std::move(opIssues),
                        status.Endpoint, std::move(status.Metadata)});
                }
            } else {
                context.reset();
                userResponseCb(nullptr, status);
            }
        };

        Run<TService, TRequest, TResponse>(
            std::move(requestWrapper),
            responseCb,
            rpc,
            dbState,
            requestSettings,
            std::move(context));
    }

    // Run request using discovery endpoint.
    // Mostly usefull to make calls from credential provider
    template<typename TService, typename TRequest, typename TResponse>
    static void RunOnDiscoveryEndpoint(
        std::shared_ptr<ICoreFacility> facility,
        TRequest&& request,
        TResponseCb<TResponse>&& responseCb,
        TSimpleRpc<TService, TRequest, TResponse> rpc,
        TRpcRequestSettings requestSettings)
    {
        requestSettings.EndpointPolicy = TRpcRequestSettings::TEndpointPolicy::UseDiscoveryEndpoint;
        requestSettings.UseAuth = false;
        // TODO: Change implementation of Run, to use ICoreFacility and remove this cast
        auto dbState = std::dynamic_pointer_cast<TDbDriverState>(facility);
        Y_ABORT_UNLESS(dbState);
        auto self = dynamic_cast<TGRpcConnectionsImpl*>(dbState->Client);
        Y_ABORT_UNLESS(self);
        self->Run<TService, TRequest, TResponse>(
            std::move(request),
            std::move(responseCb),
            rpc,
            dbState,
            requestSettings,
            nullptr);
    }

    template<typename TService, typename TRequest, typename TResponse>
    void RunDeferred(
        TRequestWrapper<TRequest>&& requestWrapper,
        TDeferredResultCb&& userResponseCb,
        TSimpleRpc<TService, TRequest, TResponse> rpc,
        TDbDriverStatePtr dbState,
        TDeadline::Duration delay,
        const TRpcRequestSettings& requestSettings,
        std::shared_ptr<IQueueClientContext> context = nullptr)
    {
        auto operationCb = [userResponseCb = std::move(userResponseCb)](Ydb::Operations::Operation* operation, TPlainStatus status) mutable {
            if (operation) {
                status.SetCostInfo(std::move(*operation->mutable_cost_info()));
                userResponseCb(operation->mutable_result(), std::move(status));
            } else {
                userResponseCb(nullptr, std::move(status));
            }
        };

        RunDeferred<TService, TRequest, TResponse>(
            std::move(requestWrapper),
            operationCb,
            rpc,
            dbState,
            delay,
            requestSettings,
            true, // poll
            context);
    }

    template<class TService, class TRequest, class TResponse, template<typename TA, typename TB, typename TC> class TStream>
    using TStreamRpc =
        typename TStream<
            typename TService::Stub,
            TRequest,
            TResponse>::TAsyncRequest;

    template<class TService, class TRequest, class TResponse, class TCallback>
    void StartReadStream(
        const TRequest& request,
        TCallback responseCb,
        TStreamRpc<TService, TRequest, TResponse, NYdbGrpc::TStreamRequestReadProcessor> rpc,
        TDbDriverStatePtr dbState,
        const TRpcRequestSettings& requestSettings,
        std::shared_ptr<IQueueClientContext> context = nullptr)
    {
        using NYdbGrpc::TGrpcStatus;
        using TConnection = std::unique_ptr<TServiceConnection<TService>>;
        using TProcessor = typename NYdbGrpc::IStreamRequestReadProcessor<TResponse>::TPtr;

        if (auto tlsValidationStatus = ValidateClientTlsCredentials(dbState)) {
            RunStreamCallback(responseCb, std::move(*tlsValidationStatus), nullptr, StopState_);
            return;
        }

        if (!TryCreateContext(context)) {
            RunStreamCallback(responseCb, MakeClientStoppedStatus(), nullptr, StopState_);
            return;
        }

        WithServiceConnection<TService>(
            [this, request, responseCb = std::move(responseCb), rpc, requestSettings, context = std::move(context), dbState](TPlainStatus status, TConnection serviceConnection, TEndpointKey endpoint) mutable {
                if (!status.Ok()) {
                    context.reset();
                    RunStreamCallback(responseCb, std::move(status), nullptr, StopState_);
                    return;
                }

                Y_ABORT_UNLESS(serviceConnection != nullptr);

                TCallMeta meta;
                try {
                    meta = MakeCallMeta(requestSettings, dbState);
                } catch (const TYdbException& e) {
                    context.reset();
                    RunStreamCallback(
                        responseCb,
                        TPlainStatus(dynamic_cast<const TAuthenticationError*>(&e) ? EStatus::CLIENT_UNAUTHENTICATED : EStatus::UNAVAILABLE, e.what()),
                        nullptr,
                        StopState_);
                    return;
                }

                dbState->StatCollector.IncGRpcInFlight();
                dbState->StatCollector.IncGRpcInFlightByHost(endpoint.GetEndpoint());

                auto lowCallback = [responseCb = std::move(responseCb), dbState, endpoint, stopState = StopState_]
                    (TGrpcStatus grpcStatus, TProcessor processor) mutable {
                        dbState->StatCollector.DecGRpcInFlight();
                        dbState->StatCollector.DecGRpcInFlightByHost(endpoint.GetEndpoint());

                        if (grpcStatus.Ok()) {
                            Y_ABORT_UNLESS(processor);
                            auto finishedCallback = [dbState, endpoint] (TGrpcStatus grpcStatus) {
                                if (!grpcStatus.Ok() && grpcStatus.GRpcStatusCode != grpc::StatusCode::CANCELLED) {
                                    dbState->EndpointPool.BanEndpoint(endpoint.GetEndpoint());
                                }
                            };
                            processor->AddFinishedCallback(std::move(finishedCallback));
                            TPlainStatus status(std::move(grpcStatus), endpoint.GetEndpoint(), {});
                            RunGuarded(stopState,
                                [&] { responseCb(std::move(status), std::move(processor)); },
                                [&] { responseCb(MakeClientStoppedStatus(), nullptr); });
                        } else {
                            dbState->StatCollector.IncReqFailDueTransportError();
                            dbState->StatCollector.IncTransportErrorsByHost(endpoint.GetEndpoint());
                            if (grpcStatus.GRpcStatusCode != grpc::StatusCode::CANCELLED) {
                                dbState->EndpointPool.BanEndpoint(endpoint.GetEndpoint());
                            }
                            TPlainStatus status(std::move(grpcStatus), endpoint.GetEndpoint(), {});
                            RunGuarded(stopState,
                                [&] { responseCb(std::move(status), nullptr); },
                                [&] { responseCb(MakeClientStoppedStatus(), nullptr); });
                        }
                    };

                serviceConnection->template DoStreamRequest<TRequest, TResponse>(
                    request,
                    std::move(lowCallback),
                    std::move(rpc),
                    std::move(meta),
                    context.get());
            }, dbState, requestSettings.PreferredEndpoint, requestSettings.EndpointPolicy);
    }

    template<class TService, class TRequest, class TResponse, class TCallback>
    void StartBidirectionalStream(
        TCallback connectedCallback,
        TStreamRpc<TService, TRequest, TResponse, NYdbGrpc::TStreamRequestReadWriteProcessor> rpc,
        TDbDriverStatePtr dbState,
        const TRpcRequestSettings& requestSettings,
        std::shared_ptr<IQueueClientContext> context = nullptr)
    {
        using NYdbGrpc::TGrpcStatus;
        using TConnection = std::unique_ptr<TServiceConnection<TService>>;
        using TProcessor = typename NYdbGrpc::IStreamRequestReadWriteProcessor<TRequest, TResponse>::TPtr;

        if (auto tlsValidationStatus = ValidateClientTlsCredentials(dbState)) {
            RunStreamCallback(connectedCallback, std::move(*tlsValidationStatus), nullptr, StopState_);
            return;
        }

        if (!TryCreateContext(context)) {
            RunStreamCallback(connectedCallback, MakeClientStoppedStatus(), nullptr, StopState_);
            return;
        }

        WithServiceConnection<TService>(
            [this, connectedCallback = std::move(connectedCallback), rpc, requestSettings, context = std::move(context), dbState]
                (TPlainStatus status, TConnection serviceConnection, TEndpointKey endpoint) mutable {
                    if (!status.Ok()) {
                        context.reset();
                        RunStreamCallback(connectedCallback, std::move(status), nullptr, StopState_);
                        return;
                    }

                    Y_ABORT_UNLESS(serviceConnection != nullptr);

                    TCallMeta meta;
                    try {
                        meta = MakeCallMeta(requestSettings, dbState);
                    } catch (const TYdbException& e) {
                        context.reset();
                        RunStreamCallback(
                            connectedCallback,
                            TPlainStatus(dynamic_cast<const TAuthenticationError*>(&e) ? EStatus::CLIENT_UNAUTHENTICATED : EStatus::UNAVAILABLE, e.what()),
                            nullptr,
                            StopState_);
                        return;
                    }

                    dbState->StatCollector.IncGRpcInFlight();
                    dbState->StatCollector.IncGRpcInFlightByHost(endpoint.GetEndpoint());

                    auto lowCallback = [connectedCallback = std::move(connectedCallback), dbState, endpoint, stopState = StopState_]
                        (TGrpcStatus grpcStatus, TProcessor processor) {
                            dbState->StatCollector.DecGRpcInFlight();
                            dbState->StatCollector.DecGRpcInFlightByHost(endpoint.GetEndpoint());

                            if (grpcStatus.Ok()) {
                                Y_ABORT_UNLESS(processor);
                                auto finishedCallback = [dbState, endpoint] (TGrpcStatus grpcStatus) {
                                    if (!grpcStatus.Ok() && grpcStatus.GRpcStatusCode != grpc::StatusCode::CANCELLED) {
                                        dbState->EndpointPool.BanEndpoint(endpoint.GetEndpoint());
                                    }
                                };
                                processor->AddFinishedCallback(std::move(finishedCallback));
                                TPlainStatus status(std::move(grpcStatus), endpoint.GetEndpoint(), {});
                                RunGuarded(stopState,
                                    [&] { connectedCallback(std::move(status), std::move(processor)); },
                                    [&] { connectedCallback(MakeClientStoppedStatus(), nullptr); });
                            } else {
                                dbState->StatCollector.IncReqFailDueTransportError();
                                dbState->StatCollector.IncTransportErrorsByHost(endpoint.GetEndpoint());
                                if (grpcStatus.GRpcStatusCode != grpc::StatusCode::CANCELLED) {
                                    dbState->EndpointPool.BanEndpoint(endpoint.GetEndpoint());
                                }
                                TPlainStatus status(std::move(grpcStatus), endpoint.GetEndpoint(), {});
                                RunGuarded(stopState,
                                    [&] { connectedCallback(std::move(status), nullptr); },
                                    [&] { connectedCallback(MakeClientStoppedStatus(), nullptr); });
                            }
                        };

                    serviceConnection->template DoStreamRequest<TRequest, TResponse>(
                        std::move(lowCallback),
                        std::move(rpc),
                        std::move(meta),
                        context.get());
            }, dbState, requestSettings.PreferredEndpoint, requestSettings.EndpointPolicy);
    }

    TAsyncListEndpointsResult GetEndpoints(TDbDriverStatePtr dbState) override;
    TListEndpointsResult MutateDiscovery(TListEndpointsResult result, const TDbDriverState* dbDriverState);

#ifndef YDB_GRPC_BYPASS_CHANNEL_POOL
    void DeleteChannels(const std::vector<std::string>& endpoints) override {
        for (const auto& endpoint : endpoints) {
            ChannelPool_.DeleteChannel(endpoint);
        }
    }
#endif

    bool GetDrainOnDtors() const;
    TBalancingPolicy::TImpl GetBalancingSettings() const override;
    bool StartStatCollecting(::NMonitoring::IMetricRegistry* sensorsRegistry) override;
    ::NMonitoring::TMetricRegistry* GetMetricRegistry() override;
    void RegisterExtension(IExtension* extension);
    void RegisterExtensionApi(IExtensionApi* api);
    std::shared_ptr<NMetrics::IMetricRegistry> GetExternalMetricRegistry() const override;
    std::shared_ptr<NTrace::ITraceProvider> GetTraceProvider() const;

    void SetDiscoveryMutator(IDiscoveryMutatorApi::TMutatorCb&& cb);
    const TLog& GetLog() const override;

private:
    static std::optional<TPlainStatus> ValidateClientTlsCredentials(const TDbDriverStatePtr& dbState) {
        Y_ABORT_UNLESS(dbState);
        if (dbState->AreClientTlsCredentialsValid()) {
            return std::nullopt;
        }
        std::string msg = "Client TLS credentials validation failed";
        const auto& detail = dbState->GetClientTlsValidationDetail();
        if (!detail.empty()) {
            msg += ": ";
            msg += detail;
        }
        return TPlainStatus(EStatus::TRANSPORT_UNAVAILABLE, msg);
    }

    template <typename TService, typename TCallback>
    void WithServiceConnection(TCallback callback, TDbDriverStatePtr dbState,
        const TEndpointKey& preferredEndpoint, TRpcRequestSettings::TEndpointPolicy endpointPolicy)
    {
        using TConnection = std::unique_ptr<TServiceConnection<TService>>;
        TConnection serviceConnection;
        TEndpointKey endpoint;
        std::tie(serviceConnection, endpoint) = GetServiceConnection<TService>(dbState, preferredEndpoint, endpointPolicy);
        if (!serviceConnection) {
            if (dbState->DiscoveryMode == EDiscoveryMode::Off) {
                TStringStream errString;
                errString << "No endpoint for database " << dbState->Database;
                errString << ", cluster endpoint " << dbState->DiscoveryEndpoint;
                dbState->StatCollector.IncReqFailNoEndpoint();
                RunServiceConnectionCallback<TService>(
                    callback,
                    TPlainStatus(EStatus::UNAVAILABLE, errString.Str()),
                    TConnection{nullptr},
                    TEndpointKey{},
                    StopState_);
            } else if (dbState->DiscoveryMode == EDiscoveryMode::Sync) {
                TStringStream errString;
                errString << "Endpoint list is empty for database " << dbState->Database;
                errString << ", cluster endpoint " << dbState->DiscoveryEndpoint;
                TPlainStatus discoveryStatus;
                {
                    std::shared_lock guard(dbState->LastDiscoveryStatusRWLock);
                    discoveryStatus = dbState->LastDiscoveryStatus;
                }

                // Discovery returns success but no serviceConnection - in this case we unable to continue processing
                if (discoveryStatus.Ok()) {
                    errString << " while last discovery returned success status. Unable to continue processing.";
                    discoveryStatus = TPlainStatus(EStatus::UNAVAILABLE, errString.Str());
                } else {
                    errString << ".";
                    discoveryStatus.Issues.AddIssues({NYdb::NIssue::TIssue(errString.Str())});
                }
                dbState->StatCollector.IncReqFailNoEndpoint();
                RunServiceConnectionCallback<TService>(
                    callback,
                    std::move(discoveryStatus),
                    TConnection{nullptr},
                    TEndpointKey{},
                    StopState_);
            } else {
                int64_t newVal;
                int64_t val;
                do {
                    val = QueuedRequests_.load();
                    if (val >= MaxQueuedRequests_) {
                        dbState->StatCollector.IncReqFailQueueOverflow();
                        RunServiceConnectionCallback<TService>(
                            callback,
                            TPlainStatus(EStatus::CLIENT_LIMITS_REACHED, "Requests queue limit reached"),
                            TConnection{nullptr},
                            TEndpointKey{},
                            StopState_);
                        return;
                    }
                    newVal = val + 1;
                } while (!QueuedRequests_.compare_exchange_weak(val, newVal));

                // UpdateAsync guarantees one update in progress for state.
                auto asyncResult = dbState->EndpointPool.UpdateAsync();
                const bool needUpdateChannels = asyncResult.second;
                auto stopState = StopState_;
                asyncResult.first.Subscribe([this, callback = std::move(callback), needUpdateChannels, dbState, preferredEndpoint, endpointPolicy, stopState = std::move(stopState)]
                    (const NThreading::TFuture<TEndpointUpdateResult>& future) mutable {
                    --QueuedRequests_;
                    RunGuarded(stopState,
                        [&] {
                            const auto& updateResult = future.GetValue();
                            if (needUpdateChannels) {
#ifndef YDB_GRPC_BYPASS_CHANNEL_POOL
                                DeleteChannels(updateResult.Removed);
#endif
                            }
                            auto discoveryStatus = updateResult.DiscoveryStatus;
                            if (discoveryStatus.Status == EStatus::SUCCESS) {
                                WithServiceConnection<TService>(std::move(callback), dbState, preferredEndpoint, endpointPolicy);
                            } else {
                                callback(
                                    TPlainStatus(discoveryStatus.Status, std::move(discoveryStatus.Issues)),
                                    TConnection{nullptr},
                                    TEndpointKey{});
                            }
                        },
                        [&] { callback(MakeClientStoppedStatus(), TConnection{nullptr}, TEndpointKey{}); });
                });
            }
            return;
        }

        RunServiceConnectionCallback<TService>(
            callback,
            TPlainStatus{},
            std::move(serviceConnection),
            std::move(endpoint),
            StopState_);
    }

    void EnqueueResponse(IObjectInQueue* action);
    void StopResponseQueue();

private:
    TCallMeta MakeCallMeta(const TRpcRequestSettings& requestSettings, const TDbDriverStatePtr& dbState) const;

    std::mutex ExtensionsLock_;
    ::NMonitoring::TMetricRegistry* MetricRegistryPtr_ = nullptr;

    const std::size_t ClientThreadsNum_;
    std::shared_ptr<IExecutor> ResponseQueue_;
    std::once_flag ResponseQueueStopOnce_;
    std::shared_ptr<TDriverStopState> StopState_;

    const std::string DefaultDiscoveryEndpoint_;
    const TSslCredentials SslCredentials_;
    const std::string DefaultDatabase_;
    std::shared_ptr<ICredentialsProviderFactory> DefaultCredentialsProviderFactory_;
    TDbDriverStateTracker StateTracker_;
    const EDiscoveryMode DefaultDiscoveryMode_;
    const std::int64_t MaxQueuedRequests_;
    const std::int64_t MaxQueuedResponses_;
    const bool DrainOnDtors_;
    const TBalancingPolicy::TImpl BalancingSettings_;
    const TDeadline::Duration GRpcKeepAliveTimeout_;
    const bool GRpcKeepAlivePermitWithoutCalls_;
    const std::string GRpcLoadBalancingPolicy_;
    const EGrpcCompressionAlgorithm GRpcCompressionAlgorithm_;
    const std::uint64_t MemoryQuota_;
    const std::uint64_t MaxInboundMessageSize_;
    const std::uint64_t MaxOutboundMessageSize_;
    const std::uint64_t MaxMessageSize_;

    std::atomic_int64_t QueuedRequests_;
    const NYdbGrpc::TTcpKeepAliveSettings TcpKeepAliveSettings_;
    const bool TcpNoDelay_;
    const TDeadline::Duration SocketIdleTimeout_;
#ifndef YDB_GRPC_BYPASS_CHANNEL_POOL
    NYdbGrpc::TChannelPool ChannelPool_;
#endif
    // State for default database, token pair
    TDbDriverStatePtr DefaultState_;

    std::vector<std::unique_ptr<IExtension>> Extensions_;
    std::vector<std::unique_ptr<IExtensionApi>> ExtensionApis_;
    std::shared_ptr<NMetrics::IMetricRegistry> MetricRegistry_;
    std::shared_ptr<NTrace::ITraceProvider> TraceProvider_;

    IDiscoveryMutatorApi::TMutatorCb DiscoveryMutatorCb;

    const std::string BuildInfoWithoutObservability_;
    const std::string BuildInfo_;

    const std::size_t NetworkThreadsNum_;
    bool UsePerChannelTcpConnection_;
    // Must be the last member (first called destructor)
    NYdbGrpc::TGRpcClientLow GRpcClientLow_;
    TLog Log;
};

struct TGRpcConnectionsDeleter {
    void operator()(TGRpcConnectionsImpl* connections) const noexcept;
};

} // namespace NYdb
