#pragma once

#include <ydb/public/sdk/cpp/client/impl/ydb_internal/internal_header.h>
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/common/ssl_credentials.h>

#include "actions.h"
#include "params.h"

#include <ydb/public/api/grpc/ydb_discovery_v1.grpc.pb.h>
#include <ydb/public/sdk//cpp/client/impl/ydb_internal/common/client_pid.h>
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/common/string_helpers.h>
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/db_driver_state/state.h>
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/rpc_request_settings/settings.h>
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/thread_pool/pool.h>
#include <ydb/public/sdk/cpp/client/resources/ydb_resources.h>
#include <ydb/public/sdk/cpp/client/ydb_extension/extension.h>

#include <ydb/library/yql/public/issue/yql_issue_message.h>

namespace NYdb {

constexpr TDuration GRPC_KEEP_ALIVE_TIMEOUT_FOR_DISCOVERY = TDuration::Seconds(10);
constexpr TDuration INITIAL_DEFERRED_CALL_DELAY = TDuration::MilliSeconds(10); // The delay before first deferred service call
constexpr TDuration GET_ENDPOINTS_TIMEOUT = TDuration::Seconds(10); // Time wait for ListEndpoints request, after this time we pass error to client

using NYdbGrpc::TCallMeta;
using NYdbGrpc::IQueueClientContextPtr;
using NYdbGrpc::IQueueClientContextProvider;

class ICredentialsProvider;

// Deferred callbacks
using TDeferredResultCb = std::function<void(google::protobuf::Any*, TPlainStatus status)>;

TStringType GetAuthInfo(TDbDriverStatePtr p);
void SetDatabaseHeader(TCallMeta& meta, const TStringType& database);
TStringType CreateSDKBuildInfo();

class TGRpcConnectionsImpl
    : public IQueueClientContextProvider
    , public IInternalClient
{
    friend class TDeferredAction;
public:
    TGRpcConnectionsImpl(std::shared_ptr<IConnectionsParams> params);
    ~TGRpcConnectionsImpl();

    void AddPeriodicTask(TPeriodicCb&& cb, TDuration period) override;
    void ScheduleOneTimeTask(TSimpleCb&& fn, TDuration timeout);
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
        const TMaybe<TStringType>& database,
        const TMaybe<TStringType>& discoveryEndpoint,
        const TMaybe<EDiscoveryMode>& discoveryMode,
        const TMaybe<TSslCredentials>& sslCredentials,
        const TMaybe<std::shared_ptr<ICredentialsProviderFactory>>& credentialsProviderFactory
    );
    IQueueClientContextPtr CreateContext() override;
    bool TryCreateContext(IQueueClientContextPtr& context);
    void WaitIdle();
    void Stop(bool wait = false);

    template<typename TService>
    using TServiceConnection = NYdbGrpc::TServiceConnection<TService>;

    static void SetGrpcKeepAlive(NYdbGrpc::TGRpcClientConfig& config, const TDuration& timeout, bool permitWithoutCalls);

    template<typename TService>
    std::pair<std::unique_ptr<TServiceConnection<TService>>, TEndpointKey> GetServiceConnection(
        TDbDriverStatePtr dbState, const TEndpointKey& preferredEndpoint,
        TRpcRequestSettings::TEndpointPolicy endpointPolicy)
    {
        auto clientConfig = NYdbGrpc::TGRpcClientConfig(dbState->DiscoveryEndpoint);
        const auto& sslCredentials = dbState->SslCredentials;
        clientConfig.SslCredentials = {.pem_root_certs = sslCredentials.CaCert, .pem_private_key = sslCredentials.PrivateKey, .pem_cert_chain = sslCredentials.Cert};
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
            if (GRpcKeepAliveTimeout_) {
                SetGrpcKeepAlive(clientConfig, GRpcKeepAliveTimeout_, GRpcKeepAlivePermitWithoutCalls_);
            }
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

    template<typename TService, typename TRequest, typename TResponse>
    void Run(
        TRequest&& request,
        TResponseCb<TResponse>&& userResponseCb,
        TSimpleRpc<TService, TRequest, TResponse> rpc,
        TDbDriverStatePtr dbState,
        const TRpcRequestSettings& requestSettings,
        std::shared_ptr<IQueueClientContext> context = nullptr)
    {
        using NYdbGrpc::TGrpcStatus;
        using TConnection = std::unique_ptr<TServiceConnection<TService>>;
        Y_ABORT_UNLESS(dbState);

        if (!TryCreateContext(context)) {
            TPlainStatus status(EStatus::CLIENT_CANCELLED, "Client is stopped");
            userResponseCb(nullptr, TPlainStatus{status.Status, std::move(status.Issues)});
            return;
        }

        if (dbState->StatCollector.IsCollecting()) {
            std::weak_ptr<TDbDriverState> weakState = dbState;
            const auto startTime = TInstant::Now();
            userResponseCb = std::move([cb = std::move(userResponseCb), weakState, startTime](TResponse* response, TPlainStatus status) {
                const auto resultSize = response ? response->ByteSizeLong() : 0;
                cb(response, status);

                if (auto state = weakState.lock()) {
                    state->StatCollector.IncRequestLatency(TInstant::Now() - startTime);
                    state->StatCollector.IncResultSize(resultSize);
                }
            });
        }

        WithServiceConnection<TService>(
            [this, request = std::move(request), userResponseCb = std::move(userResponseCb), rpc, requestSettings, context = std::move(context), dbState]
            (TPlainStatus status, TConnection serviceConnection, TEndpointKey endpoint) mutable -> void {
                if (!status.Ok()) {
                    userResponseCb(
                        nullptr,
                        std::move(status));
                    return;
                }

                TCallMeta meta;
                meta.Timeout = requestSettings.ClientTimeout;
        #ifndef YDB_GRPC_UNSECURE_AUTH
                meta.CallCredentials = dbState->CallCredentials;
        #else
                if (requestSettings.UseAuth && dbState->CredentialsProvider && dbState->CredentialsProvider->IsValid()) {
                    try {
                        meta.Aux.push_back({ YDB_AUTH_TICKET_HEADER, GetAuthInfo(dbState) });
                    } catch (const std::exception& e) {
                        userResponseCb(
                            nullptr,
                            TPlainStatus(
                                EStatus::CLIENT_UNAUTHENTICATED,
                                TStringBuilder() << "Can't get Authentication info from CredentialsProvider. " << e.what()
                            )
                        );
                        return;
                    }
                }
        #endif
                if (!requestSettings.TraceId.empty()) {
                    meta.Aux.push_back({YDB_TRACE_ID_HEADER, requestSettings.TraceId});
                }

                if (!requestSettings.RequestType.empty()) {
                    meta.Aux.push_back({YDB_REQUEST_TYPE_HEADER, requestSettings.RequestType});
                }

                if (!dbState->Database.empty()) {
                    SetDatabaseHeader(meta, dbState->Database);
                }

                static const TStringType clientPid = GetClientPIDHeaderValue();

                meta.Aux.push_back({YDB_SDK_BUILD_INFO_HEADER, CreateSDKBuildInfo()});
                meta.Aux.push_back({YDB_CLIENT_PID, clientPid});
                meta.Aux.insert(meta.Aux.end(), requestSettings.Header.begin(), requestSettings.Header.end());

                dbState->StatCollector.IncGRpcInFlight();
                dbState->StatCollector.IncGRpcInFlightByHost(endpoint.GetEndpoint());

                NYdbGrpc::TAdvancedResponseCallback<TResponse> responseCbLow =
                    [this, context, userResponseCb = std::move(userResponseCb), endpoint, dbState]
                    (const grpc::ClientContext& ctx, TGrpcStatus&& grpcStatus, TResponse&& response) mutable -> void {
                        dbState->StatCollector.DecGRpcInFlight();
                        dbState->StatCollector.DecGRpcInFlightByHost(endpoint.GetEndpoint());

                        if (NYdbGrpc::IsGRpcStatusGood(grpcStatus)) {
                            std::multimap<TStringType, TStringType> metadata;

                            for (const auto& [name, value] : ctx.GetServerInitialMetadata()) {
                                metadata.emplace(
                                    TStringType(name.begin(), name.end()),
                                    TStringType(value.begin(), value.end()));
                            }
                            for (const auto& [name, value] : ctx.GetServerTrailingMetadata()) {
                                metadata.emplace(
                                    TStringType(name.begin(), name.end()),
                                    TStringType(value.begin(), value.end()));
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

                serviceConnection->DoAdvancedRequest(std::move(request), std::move(responseCbLow), rpc, meta,
                    context.get());
            }, dbState, requestSettings.PreferredEndpoint, requestSettings.EndpointPolicy);
    }

    template<typename TService, typename TRequest, typename TResponse>
    void RunDeferred(
        TRequest&& request,
        TDeferredOperationCb&& userResponseCb,
        TSimpleRpc<TService, TRequest, TResponse> rpc,
        TDbDriverStatePtr dbState,
        TDuration deferredTimeout,
        const TRpcRequestSettings& requestSettings,
        bool poll = false,
        std::shared_ptr<IQueueClientContext> context = nullptr)
    {
        if (!TryCreateContext(context)) {
            TPlainStatus status(EStatus::CLIENT_CANCELLED, "Client is stopped");
            userResponseCb(nullptr, status);
            return;
        }

        auto responseCb = [this, userResponseCb = std::move(userResponseCb), dbState, deferredTimeout, poll, context]
            (TResponse* response, TPlainStatus status) mutable
        {
            if (response) {
                Ydb::Operations::Operation* operation = response->mutable_operation();
                if (!operation->ready() && poll) {
                    auto action = MakeIntrusive<TDeferredAction>(
                        operation->id(),
                        std::move(userResponseCb),
                        this,
                        std::move(context),
                        deferredTimeout,
                        dbState,
                        status.Endpoint);

                    action->Start();
                } else {
                    NYql::TIssues opIssues;
                    NYql::IssuesFromMessage(operation->issues(), opIssues);
                    userResponseCb(operation, TPlainStatus{static_cast<EStatus>(operation->status()), std::move(opIssues),
                        status.Endpoint, std::move(status.Metadata)});
                }
            } else {
                userResponseCb(nullptr, status);
            }
        };

        Run<TService, TRequest, TResponse>(
            std::move(request),
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
        TRequest&& request,
        TDeferredResultCb&& userResponseCb,
        TSimpleRpc<TService, TRequest, TResponse> rpc,
        TDbDriverStatePtr dbState,
        TDuration deferredTimeout,
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
            std::move(request),
            operationCb,
            rpc,
            dbState,
            deferredTimeout,
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

        if (!TryCreateContext(context)) {
            responseCb(TPlainStatus(EStatus::CLIENT_CANCELLED, "Client is stopped"), nullptr);
            return;
        }

        WithServiceConnection<TService>(
            [request, responseCb = std::move(responseCb), rpc, requestSettings, context = std::move(context), dbState](TPlainStatus status, TConnection serviceConnection, TEndpointKey endpoint) mutable {
                if (!status.Ok()) {
                    responseCb(std::move(status), nullptr);
                    return;
                }

                TCallMeta meta;
                meta.Timeout = requestSettings.ClientTimeout;
#ifndef YDB_GRPC_UNSECURE_AUTH
                meta.CallCredentials = dbState->CallCredentials;
#else
                if (requestSettings.UseAuth && dbState->CredentialsProvider && dbState->CredentialsProvider->IsValid()) {
                    try {
                        meta.Aux.push_back({ YDB_AUTH_TICKET_HEADER, GetAuthInfo(dbState) });
                    } catch (const std::exception& e) {
                        responseCb(
                            TPlainStatus(
                                EStatus::CLIENT_UNAUTHENTICATED,
                                TStringBuilder() << "Can't get Authentication info from CredentialsProvider. " << e.what()
                            ),
                            nullptr
                        );
                        return;
                    }
                }
#endif
                if (!requestSettings.TraceId.empty()) {
                    meta.Aux.push_back({YDB_TRACE_ID_HEADER, requestSettings.TraceId});
                }

                if (!requestSettings.RequestType.empty()) {
                    meta.Aux.push_back({YDB_REQUEST_TYPE_HEADER, requestSettings.RequestType});
                }

                if (!dbState->Database.empty()) {
                    SetDatabaseHeader(meta, dbState->Database);
                }

                dbState->StatCollector.IncGRpcInFlight();
                dbState->StatCollector.IncGRpcInFlightByHost(endpoint.GetEndpoint());

                auto lowCallback = [responseCb = std::move(responseCb), dbState, endpoint]
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
                            // TODO: Add headers for streaming calls.
                            TPlainStatus status(std::move(grpcStatus), endpoint.GetEndpoint(), {});
                            responseCb(std::move(status), std::move(processor));
                        } else {
                            dbState->StatCollector.IncReqFailDueTransportError();
                            dbState->StatCollector.IncTransportErrorsByHost(endpoint.GetEndpoint());
                            if (grpcStatus.GRpcStatusCode != grpc::StatusCode::CANCELLED) {
                                dbState->EndpointPool.BanEndpoint(endpoint.GetEndpoint());
                            }
                            // TODO: Add headers for streaming calls.
                            TPlainStatus status(std::move(grpcStatus), endpoint.GetEndpoint(), {});
                            responseCb(std::move(status), nullptr);
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

        if (!TryCreateContext(context)) {
            connectedCallback(TPlainStatus(EStatus::CLIENT_CANCELLED, "Client is stopped"), nullptr);
            return;
        }

        WithServiceConnection<TService>(
            [connectedCallback = std::move(connectedCallback), rpc, requestSettings, context = std::move(context), dbState]
            (TPlainStatus status, TConnection serviceConnection, TEndpointKey endpoint) mutable {
                if (!status.Ok()) {
                    connectedCallback(std::move(status), nullptr);
                    return;
                }

                TCallMeta meta;
        #ifndef YDB_GRPC_UNSECURE_AUTH
                meta.CallCredentials = dbState->CallCredentials;
        #else
                if (requestSettings.UseAuth && dbState->CredentialsProvider && dbState->CredentialsProvider->IsValid()) {
                    try {
                        meta.Aux.push_back({ YDB_AUTH_TICKET_HEADER, GetAuthInfo(dbState) });
                    } catch (const std::exception& e) {
                        connectedCallback(
                            TPlainStatus(
                                EStatus::CLIENT_UNAUTHENTICATED,
                                TStringBuilder() << "Can't get Authentication info from CredentialsProvider. " << e.what()
                            ),
                            nullptr
                        );
                        return;
                    }
                }
        #endif
                if (!requestSettings.TraceId.empty()) {
                    meta.Aux.push_back({YDB_TRACE_ID_HEADER, requestSettings.TraceId});
                }

                if (!requestSettings.RequestType.empty()) {
                    meta.Aux.push_back({YDB_REQUEST_TYPE_HEADER, requestSettings.RequestType});
                }

                if (!dbState->Database.empty()) {
                    SetDatabaseHeader(meta, dbState->Database);
                }

                static const TStringType clientPid = GetClientPIDHeaderValue();

                meta.Aux.push_back({YDB_SDK_BUILD_INFO_HEADER, CreateSDKBuildInfo()});
                meta.Aux.push_back({YDB_CLIENT_PID, clientPid});
                meta.Aux.insert(meta.Aux.end(), requestSettings.Header.begin(), requestSettings.Header.end());

                dbState->StatCollector.IncGRpcInFlight();
                dbState->StatCollector.IncGRpcInFlightByHost(endpoint.GetEndpoint());

                auto lowCallback = [connectedCallback = std::move(connectedCallback), dbState, endpoint]
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
                            // TODO: Add headers for streaming calls.
                            TPlainStatus status(std::move(grpcStatus), endpoint.GetEndpoint(), {});
                            connectedCallback(std::move(status), std::move(processor));
                        } else {
                            dbState->StatCollector.IncReqFailDueTransportError();
                            dbState->StatCollector.IncTransportErrorsByHost(endpoint.GetEndpoint());
                            if (grpcStatus.GRpcStatusCode != grpc::StatusCode::CANCELLED) {
                                dbState->EndpointPool.BanEndpoint(endpoint.GetEndpoint());
                            }
                            // TODO: Add headers for streaming calls.
                            TPlainStatus status(std::move(grpcStatus), endpoint.GetEndpoint(), {});
                            connectedCallback(std::move(status), nullptr);
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
    TListEndpointsResult MutateDiscovery(TListEndpointsResult result, const TDbDriverState& dbDriverState);

#ifndef YDB_GRPC_BYPASS_CHANNEL_POOL
    void DeleteChannels(const std::vector<std::string>& endpoints) override {
        for (const auto& endpoint : endpoints) {
            ChannelPool_.DeleteChannel(ToStringType(endpoint));
        }
    }
#endif

    bool GetDrainOnDtors() const;
    TBalancingSettings GetBalancingSettings() const override;
    bool StartStatCollecting(::NMonitoring::IMetricRegistry* sensorsRegistry) override;
    ::NMonitoring::TMetricRegistry* GetMetricRegistry() override;
    void RegisterExtension(IExtension* extension);
    void RegisterExtensionApi(IExtensionApi* api);
    void SetDiscoveryMutator(IDiscoveryMutatorApi::TMutatorCb&& cb);
    const TLog& GetLog() const override;

private:
    template <typename TService, typename TCallback>
    void WithServiceConnection(TCallback callback, TDbDriverStatePtr dbState,
        const TEndpointKey& preferredEndpoint, TRpcRequestSettings::TEndpointPolicy endpointPolicy)
    {
        using TConnection = std::unique_ptr<TServiceConnection<TService>>;
        TConnection serviceConnection;
        TEndpointKey endpoint;
        std::tie(serviceConnection, endpoint) = GetServiceConnection<TService>(dbState, preferredEndpoint, endpointPolicy);
        if (!serviceConnection) {
            if (dbState->DiscoveryMode == EDiscoveryMode::Sync) {
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
                    errString <<".";
                    discoveryStatus.Issues.AddIssues({NYql::TIssue(errString.Str())});
                }
                dbState->StatCollector.IncReqFailNoEndpoint();
                callback(
                    discoveryStatus,
                    TConnection{nullptr},
                    TEndpointKey{ });
            } else {
                int64_t newVal;
                int64_t val;
                do {
                    val = QueuedRequests_.load();
                    if (val >= MaxQueuedRequests_) {
                        dbState->StatCollector.IncReqFailQueueOverflow();
                        callback(
                            TPlainStatus(EStatus::CLIENT_LIMITS_REACHED, "Requests queue limit reached"),
                            TConnection{nullptr},
                            TEndpointKey{ });
                        return;
                    }
                    newVal = val + 1;
                } while (!QueuedRequests_.compare_exchange_weak(val, newVal));

                // UpdateAsync guarantee one update in progress for state
                auto asyncResult = dbState->EndpointPool.UpdateAsync();
                const bool needUpdateChannels = asyncResult.second;
                asyncResult.first.Subscribe([this, callback = std::move(callback), needUpdateChannels, dbState, preferredEndpoint, endpointPolicy]
                    (const NThreading::TFuture<TEndpointUpdateResult>& future) mutable {
                    --QueuedRequests_;
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
                            TEndpointKey{ });
                    }
                });
            }
            return;
        }

        callback(
            TPlainStatus{ },
            std::move(serviceConnection),
            std::move(endpoint));
    }

    void EnqueueResponse(IObjectInQueue* action);

private:
    std::mutex ExtensionsLock_;
    ::NMonitoring::TMetricRegistry* MetricRegistryPtr_ = nullptr;

    std::unique_ptr<IThreadPool> ResponseQueue_;

    const TStringType DefaultDiscoveryEndpoint_;
    const TSslCredentials SslCredentials_;
    const TStringType DefaultDatabase_;
    std::shared_ptr<ICredentialsProviderFactory> DefaultCredentialsProviderFactory_;
    TDbDriverStateTracker StateTracker_;
    const EDiscoveryMode DefaultDiscoveryMode_;
    const i64 MaxQueuedRequests_;
    const bool DrainOnDtors_;
    const TBalancingSettings BalancingSettings_;
    const TDuration GRpcKeepAliveTimeout_;
    const bool GRpcKeepAlivePermitWithoutCalls_;
    const ui64 MemoryQuota_;
    const ui64 MaxInboundMessageSize_;
    const ui64 MaxOutboundMessageSize_;
    const ui64 MaxMessageSize_;

    std::atomic_int64_t QueuedRequests_;
#ifndef YDB_GRPC_BYPASS_CHANNEL_POOL
    NYdbGrpc::TChannelPool ChannelPool_;
#endif
    // State for default database, token pair
    TDbDriverStatePtr DefaultState_;

    std::vector<std::unique_ptr<IExtension>> Extensions_;
    std::vector<std::unique_ptr<IExtensionApi>> ExtensionApis_;

    IDiscoveryMutatorApi::TMutatorCb DiscoveryMutatorCb;

    // Must be the last member (first called destructor)
    NYdbGrpc::TGRpcClientLow GRpcClientLow_;
    TLog Log;
};

} // namespace NYdb
