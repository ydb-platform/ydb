#define INCLUDE_YDB_INTERNAL_H
#include "grpc_connections.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/runtime/runtime.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/exceptions/exceptions.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/resources/ydb_resources.h>
#include <ydb/public/sdk/cpp/src/client/impl/internal/common/client_pid.h>
#include <ydb/public/sdk/cpp/src/client/impl/observability/constants.h>

#include <util/generic/scope.h>

#include <string>
#include <thread>
#include <utility>

namespace NYdb::inline Dev {

bool IsSdkRuntimeCallback() noexcept;

namespace {

using TCredentialsWaitResult = TGRpcConnectionsImpl::TCredentialsWaitResult;

TPlainStatus InitFailedStatus(const std::exception* e = nullptr) {
    TStringBuilder message;
    message << "Credentials provider initialization failed";
    if (e) {
        message << ". " << e->what();
    }
    return TPlainStatus(EStatus::CLIENT_UNAUTHENTICATED, message);
}

TCredentialsWaitResult ReadyResult(const NThreading::TFuture<void>& future) {
    try {
        future.GetValue();
        return {};
    } catch (const std::exception& e) {
        return InitFailedStatus(&e);
    } catch (...) {
        return InitFailedStatus();
    }
}

NThreading::TFuture<void> MakeFreshReadyFuture() {
    auto promise = NThreading::NewPromise<void>();
    auto future = promise.GetFuture();
    promise.SetValue();
    return future;
}

TDuration GetTimeout(TDeadline deadline) {
    const auto remaining = deadline.GetTimePoint() - TDeadline::Clock::now();
    return remaining <= TDeadline::Duration::zero()
        ? TDuration::Zero()
        : TDuration::MicroSeconds(
            std::chrono::ceil<std::chrono::microseconds>(remaining).count());
}

} // anonymous namespace

void TGRpcConnectionsImpl::PostCallbackImpl(
    TCallbackCounter counter,
    std::function<void()> callback) {
    GetSdkRuntime().PostImpl(
        [producer = TCallbackProducer(std::move(counter)), callback = std::move(callback)]() mutable {
            Y_SCOPE_EXIT(&callback) {
                callback = nullptr;
            };
            callback();
        });
}

NThreading::TFuture<void> TGRpcConnectionsImpl::CredentialsReadyToWaitFor(
    const TDbDriverStatePtr& dbState,
    const TRpcRequestSettings& requestSettings,
    const IQueueClientContextPtr& context) const {
    if (!requestSettings.UseAuth) {
        return {};
    }
    auto ready = dbState->GetCredentialsReady();
    return ready.HasValue() && !(context && context->IsCancelled())
               ? NThreading::TFuture<void>{}
               : ready;
}

void TGRpcConnectionsImpl::DeferUntilCredentialsReady(
    const TRpcRequestSettings& requestSettings,
    IQueueClientContextPtr& context,
    NThreading::TFuture<void> credentialsReady,
    TCredentialsCallback callback) {
    if (!TryCreateContext(context)) {
        callback(MakeClientStoppedStatus());
        return;
    }

    auto cancelled = NThreading::NewPromise<void>();
    if (!credentialsReady.IsReady()) {
        context->SubscribeCancel([cancelled]() mutable {
            cancelled.TrySetValue();
        });
    } else if (context->IsCancelled()) {
        cancelled.SetValue();
    }

    auto scheduleCallback = [this, scheduleContext = context, callbackCounter = CallbackCounter_](TDeadline deadline, std::function<void(bool)> callback) {
        PostCallbackImpl(
            callbackCounter,
            [this, deadline, scheduleContext, callback = std::move(callback)]() mutable {
                // Register directly on the alarm: a ready future may run Subscribe() inline.
                ScheduleCallback(GetTimeout(deadline), std::move(callback), scheduleContext);
            });
    };

    NThreading::TFuture<TCredentialsWaitResult> wait;
    if (credentialsReady.IsReady()) {
        auto status = ReadyResult(credentialsReady);
        wait = NThreading::MakeFuture(status || !cancelled.HasValue()
            ? std::move(status)
            : TCredentialsWaitResult(MakeClientStoppedStatus()));
    } else {
        auto result = NThreading::NewPromise<TCredentialsWaitResult>();
        wait = result.GetFuture();
        credentialsReady.Subscribe([result](const NThreading::TFuture<void>& future) mutable {
            result.TrySetValue(ReadyResult(future));
        });
        cancelled.GetFuture().Subscribe([result](const NThreading::TFuture<void>&) mutable {
            result.TrySetValue(MakeClientStoppedStatus());
        });
        if (requestSettings.Deadline != TDeadline::Max()) {
            scheduleCallback(requestSettings.Deadline,
                [result](bool scheduledSuccessfully) mutable {
                    result.TrySetValue(scheduledSuccessfully
                        ? TPlainStatus(EStatus::CLIENT_DEADLINE_EXCEEDED,
                            "Request deadline exceeded while waiting for credentials")
                        : MakeClientStoppedStatus());
                });
        }
    }

    wait.Subscribe([callback = std::move(callback), scheduleCallback = std::move(scheduleCallback)]
        (const NThreading::TFuture<TCredentialsWaitResult>& future) mutable {
        scheduleCallback(TDeadline::Now(),
            [callback = std::move(callback), status = future.GetValue()]
            (bool scheduledSuccessfully) mutable {
                callback(scheduledSuccessfully
                    ? std::move(status)
                    : TCredentialsWaitResult(MakeClientStoppedStatus()));
            });
    });
}

bool IsTokenCorrect(const std::string& in) {
    for (char c : in) {
        if (!(IsAsciiAlnum(c) || IsAsciiPunct(c) || c == ' ')) {
            return false;
        }
    }
    return true;
}

std::string GetAuthInfo(TDbDriverStatePtr p) {
    try {
        auto credentialsProvider = p->GetCredentialsProvider();
        if (!credentialsProvider) {
            throw TAuthenticationError("Credentials provider is not initialized");
        }
        auto token = credentialsProvider->GetAuthInfo();
        if (!IsTokenCorrect(token)) {
            throw TAuthenticationError("token is incorrect, illegal characters found");
        }
        return token;
    } catch (const TYdbException&) {
        throw;
    } catch (const std::exception& e) {
        throw TAuthenticationError(TStringBuilder() << "Can't get Authentication info from CredentialsProvider. " << e.what());
    }
}

std::string CreateSDKBuildInfo() {
    return std::string("ydb-cpp-sdk/") + GetSdkSemver();
}

std::string BuildFullBuildInfo(const IConnectionsParams& params, bool includeObservability) {
    auto result = CreateSDKBuildInfo();
    if (includeObservability && params.GetTraceProvider()) {
        result += " ydb-sdk-tracing/";
        result += NObservability::kTracingChainVersion;
    }
    if (includeObservability && params.GetExternalMetricRegistry()) {
        result += " ydb-sdk-metrics/";
        result += NObservability::kMetricsChainVersion;
    }
    auto extra = params.GetBuildInfoExtra();
    if (!extra.empty()) {
        result += ';';
        result += extra;
    }
    return result;
}

TGRpcConnectionsImpl::TGRpcConnectionsImpl(std::shared_ptr<IConnectionsParams> params)
    : CallbackCounter_(std::make_shared<std::atomic_uint64_t>(0))
    , StopNotification_(MakeFreshReadyFuture())
    , ClientThreadsNum_(params->GetClientThreadsNum())
    , DefaultDiscoveryEndpoint_(params->GetEndpoint())
    , SslCredentials_(params->GetSslCredentials())
    , DefaultDatabase_(params->GetDatabase())
    , DefaultCredentialsProviderFactory_(params->GetCredentialsProviderFactory())
    , StateTracker_(this)
    , DefaultDiscoveryMode_(params->GetDiscoveryMode())
    , MaxQueuedRequests_(params->GetMaxQueuedRequests())
    , MaxQueuedResponses_(params->GetMaxQueuedResponses())
    , DrainOnDtors_(params->GetDrinOnDtors())
    , BalancingSettings_(params->GetBalancingSettings())
    , GRpcKeepAliveTimeout_(TDeadline::SafeDurationCast(params->GetGRpcKeepAliveTimeout()))
    , GRpcKeepAlivePermitWithoutCalls_(params->GetGRpcKeepAlivePermitWithoutCalls())
    , GRpcLoadBalancingPolicy_(params->GetGRpcLoadBalancingPolicy())
    , GRpcCompressionAlgorithm_(params->GetGRpcCompressionAlgorithm())
    , MemoryQuota_(params->GetMemoryQuota())
    , MaxInboundMessageSize_(params->GetMaxInboundMessageSize())
    , MaxOutboundMessageSize_(params->GetMaxOutboundMessageSize())
    , MaxMessageSize_(params->GetMaxMessageSize())
    , QueuedRequests_(0)
    , TcpKeepAliveSettings_(params->GetTcpKeepAliveSettings())
    , TcpNoDelay_(params->GetTcpNoDelay())
    , SocketIdleTimeout_(TDeadline::SafeDurationCast(params->GetSocketIdleTimeout()))
#ifndef YDB_GRPC_BYPASS_CHANNEL_POOL
    , ChannelPool_(TcpKeepAliveSettings_, params->GetSocketIdleTimeout(), TcpNoDelay_)
#endif
    , MetricRegistry_(params->GetExternalMetricRegistry())
    , TraceProvider_(params->GetTraceProvider())
    , BuildInfoWithoutObservability_(BuildFullBuildInfo(*params, false))
    , BuildInfo_(BuildFullBuildInfo(*params, true))
    , NetworkThreadsNum_(params->GetNetworkThreadsNum())
    , UsePerChannelTcpConnection_(params->GetUsePerChannelTcpConnection())
    , GRpcClientLow_(NetworkThreadsNum_)
    , Log(params->GetLog()) {
}

void TGRpcConnectionsImpl::Start() {
#ifndef YDB_GRPC_BYPASS_CHANNEL_POOL
    if (SocketIdleTimeout_ != TDeadline::Duration::max()) {
        auto channelPoolUpdateWrapper = [this](NYdb::NIssue::TIssues&&, EStatus status) mutable {
            if (status != EStatus::SUCCESS) {
                return false;
            }

            ChannelPool_.DeleteExpiredStubsHolders();
            return true;
        };
        AddPeriodicTask(channelPoolUpdateWrapper, SocketIdleTimeout_ / 10);
    }
#endif
    if (!DefaultDatabase_.empty()) {
        DefaultState_ = StateTracker_.GetDriverState(
            DefaultDatabase_,
            DefaultDiscoveryEndpoint_,
            DefaultDiscoveryMode_,
            SslCredentials_,
            DefaultCredentialsProviderFactory_
        );
    }
}

bool TGRpcConnectionsImpl::IsCurrentThreadInSdkCallback() noexcept {
    return IsSdkRuntimeCallback() || NYdbGrpc::IsGRpcCompletionThread();
}

void TGRpcConnectionsDeleter::operator()(TGRpcConnectionsImpl* connections) const noexcept {
    auto states = connections->StateTracker_.GetStates();
    connections->Stop();
    try {
        std::thread([connections, states = std::move(states)]() mutable {
            connections->StopNotification_.Wait();
            connections->GRpcClientLow_.Stop(true);
            connections->WaitCallbacks();
            for (auto& state : states) {
                state->ResetCredentials();
            }
            connections->WaitCallbacks();
            connections->DefaultState_.reset();
            states.clear();
            connections->StateTracker_.WaitEmpty();
            connections->WaitCallbacks();
            delete connections;
        }).detach();
    } catch (...) {
        Y_ABORT("Failed to destroy YDB driver asynchronously");
    }
}

void TGRpcConnectionsImpl::AddPeriodicTask(TPeriodicCb&& cb, TDeadline::Duration period) {
    auto context = CreateContext();
    if (!context) {
        cb(NYdb::NIssue::TIssues{}, EStatus::CLIENT_CANCELLED);
    } else {
        MakeIntrusive<TPeriodicAction>(
            std::move(cb),
            this,
            std::move(context),
            period)
            ->Start();
    }
}

void TGRpcConnectionsImpl::PostToResponseQueue(std::function<void()>&& f) {
    PostCallback(
        [callback = std::move(f)]() mutable {
            callback();
        });
}

void TGRpcConnectionsImpl::ScheduleDelayedTask(TSimpleCb&& fn, TDeadline deadline) {
    std::shared_ptr<IQueueClientContext> context;
    if (!TryCreateContext(context)) {
        GetSdkRuntime().AsyncSleep(GetTimeout(deadline)).Subscribe(
            [fn = std::move(fn)](const NThreading::TFuture<void>&) mutable {
                fn();
            });
        return;
    }

    auto cbLow = [this, fn = std::move(fn), context](bool ok) mutable {
        if (!ok) {
            return;
        }

        PostCallback([fn = std::move(fn), context = std::move(context)]() mutable {
            fn();
        });
    };

    if (deadline <= TDeadline::Now()) {
        cbLow(true);
        return;
    }

    MakeIntrusive<TDelayedAction>(
        std::move(cbLow),
        this,
        std::move(context),
        deadline)->Start();
}

void TGRpcConnectionsImpl::ScheduleDelayedTask(TSimpleCb&& fn, TDeadline::Duration delay) {
    ScheduleDelayedTask(std::move(fn), TDeadline::AfterDuration(delay));
}

void TGRpcConnectionsImpl::ScheduleCallback(
    TDuration timeout,
    std::function<void(bool)> callback,
    IQueueClientContextPtr context) {
    if (!context) {
        context = CreateContext();
    }
    if (!context) {
        callback(false);
        return;
    }

    auto completion = [this, callback = std::move(callback)](bool ok) mutable {
        PostCallback([callback = std::move(callback), ok]() mutable {
            callback(ok);
        });
    };
    MakeIntrusive<TDelayedAction>(
        std::move(completion), this, std::move(context), TDeadline::AfterDuration(timeout))
        ->Start();
}

TDbDriverStatePtr TGRpcConnectionsImpl::GetDriverState(
    const std::optional<std::string>& database,
    const std::optional<std::string>& discoveryEndpoint,
    const std::optional<EDiscoveryMode>& discoveryMode,
    const std::optional<TSslCredentials>& sslCredentials,
    const std::optional<std::shared_ptr<ICredentialsProviderFactory>>& credentialsProviderFactory) {
    return StateTracker_.GetDriverState(
        database.value_or(DefaultDatabase_),
        discoveryEndpoint.value_or(DefaultDiscoveryEndpoint_),
        discoveryMode.value_or(DefaultDiscoveryMode_),
        sslCredentials.value_or(SslCredentials_),
        credentialsProviderFactory.value_or(DefaultCredentialsProviderFactory_));
}

IQueueClientContextPtr TGRpcConnectionsImpl::CreateContext() {
    if (IsStopping()) {
        return {};
    }

    auto context = GRpcClientLow_.CreateContext();
    if (context && IsStopping()) {
        context->Cancel();
        context.reset();
    }
    return context;
}

bool TGRpcConnectionsImpl::TryCreateContext(IQueueClientContextPtr& context) {
    if (!context) {
        // Keep CQ running until the request is complete
        context = CreateContext();
    }
    return bool(context);
}

bool TGRpcConnectionsImpl::IsStopping() const noexcept {
    return Stopping_.load(std::memory_order_acquire);
}

void TGRpcConnectionsImpl::Stop() {
    if (!Stopping_.exchange(true, std::memory_order_acq_rel)) {
        try {
            StopNotification_ = StateTracker_.SendNotification(TDbDriverState::ENotifyType::STOP);
        } catch (...) {
            // StopNotification_ is already ready; shutdown must stay non-throwing.
            (void)0;
        }
        GRpcClientLow_.StopGracefully(false);
    }
}

void TGRpcConnectionsImpl::WaitCallbacks() const noexcept {
    auto count = CallbackCounter_->load(std::memory_order_acquire);
    while (count) {
        CallbackCounter_->wait(count, std::memory_order_acquire);
        count = CallbackCounter_->load(std::memory_order_acquire);
    }
}

void TGRpcConnectionsImpl::SetGrpcKeepAlive(NYdbGrpc::TGRpcClientConfig& config, const TDeadline::Duration& timeout, bool permitWithoutCalls) {
    std::uint64_t timeoutMs = std::chrono::duration_cast<std::chrono::milliseconds>(timeout).count();
    config.IntChannelParams[GRPC_ARG_KEEPALIVE_TIME_MS] = timeoutMs;
    config.IntChannelParams[GRPC_ARG_KEEPALIVE_TIMEOUT_MS] = timeoutMs;
    config.IntChannelParams[GRPC_ARG_HTTP2_MAX_PINGS_WITHOUT_DATA] = 0;
    config.IntChannelParams[GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS] = permitWithoutCalls ? 1 : 0;
}

void TGRpcConnectionsImpl::SetGrpcCompressionAlgorithm(NYdbGrpc::TGRpcClientConfig& config, EGrpcCompressionAlgorithm algorithm) {
    switch (algorithm) {
        case EGrpcCompressionAlgorithm::None:
            config.CompressionAlgorithm = GRPC_COMPRESS_NONE;
            break;
        case EGrpcCompressionAlgorithm::Deflate:
            config.CompressionAlgorithm = GRPC_COMPRESS_DEFLATE;
            break;
        case EGrpcCompressionAlgorithm::Gzip:
            config.CompressionAlgorithm = GRPC_COMPRESS_GZIP;
            break;
    }
}

TAsyncListEndpointsResult TGRpcConnectionsImpl::GetEndpoints(TDbDriverStatePtr dbState) {
    Ydb::Discovery::ListEndpointsRequest request;
    request.set_database(TStringType{dbState->Database});

    auto promise = NThreading::NewPromise<TListEndpointsResult>();
    std::weak_ptr<TDbDriverState> weakState = dbState;

    auto extractor = [this, promise, weakState](google::protobuf::Any* any, TPlainStatus status) mutable {
        Ydb::Discovery::ListEndpointsResult result;
        if (any) {
            any->UnpackTo(&result);
        }
        auto strong = weakState.lock();
        TListEndpointsResult value{result, status};
        if (strong && value.DiscoveryStatus.IsTransportError()) {
            strong->StatCollector.IncDiscoveryFailDueTransportError();
        }
        NThreading::NImpl::SetValue(promise, [&] {
            return MutateDiscovery(std::move(value), strong.get());
        });
    };

    TRpcRequestSettings rpcSettings;
    rpcSettings.Deadline = TDeadline::AfterDuration(GET_ENDPOINTS_TIMEOUT);
    rpcSettings.IncludeObservabilityInBuildInfo = true;

    RunDeferred<Ydb::Discovery::V1::DiscoveryService, Ydb::Discovery::ListEndpointsRequest, Ydb::Discovery::ListEndpointsResponse>(
        std::move(request),
        extractor,
        &Ydb::Discovery::V1::DiscoveryService::Stub::AsyncListEndpoints,
        dbState->shared_from_this(),
        INITIAL_DEFERRED_CALL_DELAY,
        rpcSettings);

    return promise.GetFuture();
}

TListEndpointsResult TGRpcConnectionsImpl::MutateDiscovery(TListEndpointsResult result, const TDbDriverState* dbDriverState) {
    std::lock_guard lock(ExtensionsLock_);
    if (!DiscoveryMutatorCb || !dbDriverState) {
        return result;
    }

    auto endpoint = result.DiscoveryStatus.Endpoint;
    auto ydbStatus = NYdb::TStatus(std::move(result.DiscoveryStatus));

    auto aux = IDiscoveryMutatorApi::TAuxInfo {
        .Database = dbDriverState->Database,
        .DiscoveryEndpoint = dbDriverState->DiscoveryEndpoint
    };

    ydbStatus = DiscoveryMutatorCb(&result.Result, std::move(ydbStatus), aux);

    auto issues = ydbStatus.GetIssues();

    auto plainStatus = TPlainStatus(ydbStatus.GetStatus(), std::move(issues), endpoint, {});
    result.DiscoveryStatus = plainStatus;
    return result;
}

bool TGRpcConnectionsImpl::GetDrainOnDtors() const {
    return DrainOnDtors_;
}

TBalancingPolicy::TImpl TGRpcConnectionsImpl::GetBalancingSettings() const {
    return BalancingSettings_;
}

bool TGRpcConnectionsImpl::StartStatCollecting(NMonitoring::IMetricRegistry* sensorsRegistry) {
    {
        std::lock_guard lock(ExtensionsLock_);
        if (MetricRegistryPtr_) {
            return false;
        }
        if (auto ptr = dynamic_cast<NMonitoring::TMetricRegistry*>(sensorsRegistry)) {
            MetricRegistryPtr_ = ptr;
        } else {
            std::cerr << "Unknown IMetricRegistry impl" << std::endl;
            return false;
        }
    }

    StateTracker_.SetMetricRegistry(MetricRegistryPtr_);
    return true;
}

NMonitoring::TMetricRegistry* TGRpcConnectionsImpl::GetMetricRegistry() {
    std::lock_guard lock(ExtensionsLock_);
    return MetricRegistryPtr_;
}

void TGRpcConnectionsImpl::RegisterExtension(IExtension* extension) {
    Extensions_.emplace_back(extension);
}

void TGRpcConnectionsImpl::RegisterExtensionApi(IExtensionApi* api) {
    ExtensionApis_.emplace_back(api);
}

std::shared_ptr<NMetrics::IMetricRegistry> TGRpcConnectionsImpl::GetExternalMetricRegistry() const {
    return MetricRegistry_;
}

std::shared_ptr<NTrace::ITraceProvider> TGRpcConnectionsImpl::GetTraceProvider() const {
    return TraceProvider_;
}

void TGRpcConnectionsImpl::SetDiscoveryMutator(IDiscoveryMutatorApi::TMutatorCb&& cb) {
    std::lock_guard lock(ExtensionsLock_);
    DiscoveryMutatorCb = std::move(cb);
}

const TLog& TGRpcConnectionsImpl::GetLog() const {
    return Log;
}

TCallMeta TGRpcConnectionsImpl::MakeCallMeta(const TRpcRequestSettings& requestSettings, const TDbDriverStatePtr& dbState) const {
    TCallMeta meta;
    meta.Timeout = requestSettings.Deadline;
#ifndef YDB_GRPC_UNSECURE_AUTH
    if (requestSettings.UseAuth) {
        meta.CallCredentials = dbState->GetCallCredentials();
    }
#else
    auto credentialsProvider = dbState->GetCredentialsProvider();
    if (requestSettings.UseAuth && credentialsProvider && credentialsProvider->IsValid()) {
        meta.Aux.push_back({YDB_AUTH_TICKET_HEADER, GetAuthInfo(dbState)});
    }
#endif
    if (!requestSettings.TraceId.empty()) {
        meta.Aux.push_back({YDB_TRACE_ID_HEADER, requestSettings.TraceId});
    }

    if (!requestSettings.RequestType.empty()) {
        meta.Aux.push_back({YDB_REQUEST_TYPE_HEADER, requestSettings.RequestType});
    }

    if (!requestSettings.TraceParent.empty()) {
        meta.Aux.push_back({OTEL_TRACE_HEADER, requestSettings.TraceParent});
    } else if (TraceProvider_) {
        if (auto tracer = TraceProvider_->GetTracer(std::string(NObservability::Tracer::kSdkName))) {
            auto traceParent = tracer->GetCurrentTraceparent();
            if (!traceParent.empty()) {
                meta.Aux.push_back({OTEL_TRACE_HEADER, std::move(traceParent)});
            }
        }
    }

    if (!dbState->Database.empty()) {
        // See TDbDriverStateTracker::GetDriverState to find place where we do quote non ASCII characters
        meta.Aux.push_back({YDB_DATABASE_HEADER, dbState->Database});
    }

    static const std::string clientPid = GetClientPIDHeaderValue();

    meta.Aux.push_back({
        YDB_SDK_BUILD_INFO_HEADER,
        requestSettings.IncludeObservabilityInBuildInfo ? BuildInfo_ : BuildInfoWithoutObservability_});
    meta.Aux.push_back({YDB_CLIENT_PID, clientPid});
    meta.Aux.insert(meta.Aux.end(), requestSettings.Header.begin(), requestSettings.Header.end());

    return meta;
}

} // namespace NYdb
