#define INCLUDE_YDB_INTERNAL_H
#include "grpc_connections.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/exceptions/exceptions.h>
#include <ydb/public/sdk/cpp/src/client/impl/observability/constants.h>

#include <string>
#include <utility>


namespace NYdb::inline Dev {

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

TPlainStatus InitCancelledStatus() {
    return TPlainStatus(EStatus::CLIENT_CANCELLED, "Client is stopped");
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

} // anonymous namespace

NThreading::TFuture<void> TGRpcConnectionsImpl::CredentialsReadyToWaitFor(
    const TDbDriverStatePtr& dbState,
    const TRpcRequestSettings& requestSettings,
    const IQueueClientContextPtr& context) const
{
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
    TCredentialsCallback callback)
{
    if (!TryCreateContext(context)) {
        callback(InitCancelledStatus());
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

    auto scheduleContext = context;
    auto scheduleCallback = [this, scheduleContext, driverScope = DriverScope_]
        (TDeadline deadline, std::function<void(bool)> callback) {
        // Future continuations may outlive TGRpcConnectionsImpl. Acquire the guard before
        // dereferencing this, so destruction either waits for scheduling or prevents it.
        driverScope->RunGuarded(
            [&] {
                const auto now = TDeadline::Clock::now();
                const auto timeout = deadline.GetTimePoint() <= now
                    ? TDuration::Zero()
                    : TDuration::MicroSeconds(std::chrono::duration_cast<std::chrono::microseconds>(
                        deadline.GetTimePoint() - now).count());
                // Register the callback directly on the alarm. ScheduleFuture(timeout).Subscribe(...)
                // may run the callback inline when the future becomes ready before Subscribe().
                ScheduleCallback(timeout, std::move(callback), scheduleContext);
            },
            [&] { callback(false); });
    };

    NThreading::TFuture<TCredentialsWaitResult> wait;
    if (credentialsReady.IsReady()) {
        auto status = ReadyResult(credentialsReady);
        wait = NThreading::MakeFuture(status || !cancelled.HasValue()
            ? std::move(status)
            : TCredentialsWaitResult(InitCancelledStatus()));
    } else {
        auto result = NThreading::NewPromise<TCredentialsWaitResult>();
        wait = result.GetFuture();
        credentialsReady.Subscribe([result](const NThreading::TFuture<void>& future) mutable {
            result.TrySetValue(ReadyResult(future));
        });
        cancelled.GetFuture().Subscribe([result](const NThreading::TFuture<void>&) mutable {
            result.TrySetValue(InitCancelledStatus());
        });
        if (requestSettings.Deadline != TDeadline::Max()) {
            scheduleCallback(requestSettings.Deadline,
                [result](bool scheduledSuccessfully) mutable {
                    result.TrySetValue(scheduledSuccessfully
                        ? TPlainStatus(EStatus::CLIENT_DEADLINE_EXCEEDED,
                            "Request deadline exceeded while waiting for credentials")
                        : InitCancelledStatus());
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
                    : TCredentialsWaitResult(InitCancelledStatus()));
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
    } catch (const TAuthenticationError& e) {
        throw;
    } catch (const TYdbException& e) {
        throw;
    } catch (const std::exception& e) {
        throw TAuthenticationError(TStringBuilder() << "Can't get Authentication info from CredentialsProvider. " << e.what());
    }
}

void SetDatabaseHeader(TCallMeta& meta, const std::string& database) {
    // See TDbDriverStateTracker::GetDriverState to find place where we do quote non ASCII characters
    meta.Aux.push_back({YDB_DATABASE_HEADER, database});
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

template<class TDerived>
class TScheduledObject : public TThrRefBase {
    using TSelf = TScheduledObject<TDerived>;
    using TPtr = TIntrusivePtr<TSelf>;

    Y_FORCE_INLINE TDerived* Derived() {
        return static_cast<TDerived*>(this);
    }

    void Complete(bool ok) {
        bool entered = true;
        std::unique_ptr<NYdbGrpc::IQueueClientCallbackGuard> guard;
        if (CallbackGuardFactory) {
            guard = CallbackGuardFactory();
            entered = !guard || guard->IsEntered();
        }
        Derived()->OnComplete(entered ? ok : false);
    }

protected:
    TScheduledObject() { }

    void Start(TDuration timeout, IQueueClientContextProvider* provider) {
        CallbackGuardFactory = provider->GetCallbackGuardFactory();
        auto context = provider->CreateContext();
        if (!context) {
            Complete(false);
            return;
        }

        auto deadline = gpr_time_add(
            gpr_now(GPR_CLOCK_MONOTONIC),
            gpr_time_from_micros(timeout.MicroSeconds(), GPR_TIMESPAN));

        {
            std::lock_guard guard(Mutex);
            Context = context;
            Alarm.Set(context->CompletionQueue(), deadline, OnAlarmTag.Prepare());
        }

        context->SubscribeCancel([self = TPtr(this)] {
            self->Alarm.Cancel();
        });
    }

private:
    void OnAlarm(bool ok) {
        {
            std::lock_guard guard(Mutex);
            // Break circular dependencies
            Context.reset();
        }

        Complete(ok);
    }

private:
    std::mutex Mutex;
    IQueueClientContextPtr Context;
    grpc::Alarm Alarm;
    TQueueClientCallbackGuardFactory CallbackGuardFactory;

private:
    using TFixedEvent = NYdbGrpc::TQueueClientFixedEvent<TSelf>;

    TFixedEvent OnAlarmTag = { this, &TSelf::OnAlarm };
};

class TScheduledCallback : public TScheduledObject<TScheduledCallback> {
    using TBase = TScheduledObject<TScheduledCallback>;

public:
    using TCallback = std::function<void(bool)>;

    TScheduledCallback(TCallback&& callback)
        : Callback(std::move(callback))
    { }

    void Start(TDuration timeout, IQueueClientContextProvider* provider) {
        TBase::Start(timeout, provider);
    }

    void OnComplete(bool ok) {
        auto callback = std::move(Callback);
        callback(ok);
    }

private:
    TCallback Callback;
};

class TScheduledFuture : public TScheduledObject<TScheduledFuture> {
    using TBase = TScheduledObject<TScheduledFuture>;

public:
    TScheduledFuture()
        : Promise(NThreading::NewPromise<bool>())
    { }

    NThreading::TFuture<bool> Start(TDuration timeout, IQueueClientContextProvider* provider) {
        auto future = Promise.GetFuture();

        TBase::Start(timeout, provider);

        return future;
    }

    void OnComplete(bool ok) {
        Promise.SetValue(ok);
        Promise = { };
    }

private:
    NThreading::TPromise<bool> Promise;
};

TGRpcConnectionsImpl::TGRpcConnectionsImpl(std::shared_ptr<IConnectionsParams> params)
    : MetricRegistryPtr_(nullptr)
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
    , Log(params->GetLog())
    , DriverScope_(GetSdkRuntime().CreateDriverScope(GRpcClientLow_))
{
#ifndef YDB_GRPC_BYPASS_CHANNEL_POOL
    if (SocketIdleTimeout_ != TDeadline::Duration::max()) {
        auto channelPoolUpdateWrapper = [this]
            (NYdb::NIssue::TIssues&&, EStatus status) mutable
        {
            if (status != EStatus::SUCCESS) {
                return false;
            }

            ChannelPool_.DeleteExpiredStubsHolders();
            return true;
        };
        AddPeriodicTask(channelPoolUpdateWrapper, SocketIdleTimeout_ / 10);
    }
#endif
    if (params->GetExecutor()) {
        ResponseQueue_ = params->GetExecutor();
    } else {
        // TAdaptiveThreadPool ignores params
        ResponseQueue_ = CreateThreadPoolExecutor(ClientThreadsNum_, MaxQueuedRequests_);
    }

    ResponseQueue_->Start();
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

TGRpcConnectionsImpl::~TGRpcConnectionsImpl() {
    Stop(true);
    DriverScope_->CloseCallbacksAndWait();
}

bool TGRpcConnectionsImpl::IsCurrentThreadInSdkCallback() noexcept {
    return TDriverScope::IsCurrentThreadInCallback();
}

void TGRpcConnectionsDeleter::operator()(TGRpcConnectionsImpl* connections) const noexcept {
    if (!connections) {
        return;
    }

    connections->DriverScope_->DeferOrRun([connections] {
        delete connections;
    });
}

void TGRpcConnectionsImpl::AddPeriodicTask(TPeriodicCb&& cb, TDeadline::Duration period) {
    std::shared_ptr<IQueueClientContext> context;
    if (!TryCreateContext(context)) {
        NYdb::NIssue::TIssues issues;
        DriverScope_->RunGuarded(
            [&] { cb(std::move(issues), EStatus::CLIENT_INTERNAL_ERROR); },
            [&] { cb(std::move(issues), EStatus::CLIENT_CANCELLED); });
    } else {
        auto action = MakeIntrusive<TPeriodicAction>(
            std::move(cb),
            this,
            std::move(context),
            period);
        action->Start();
    }
}

void TGRpcConnectionsImpl::PostToResponseQueue(std::function<void()>&& f) {
    auto driverScope = DriverScope_;
    ResponseQueue_->Post([f = std::move(f), driverScope = std::move(driverScope)]() mutable {
        driverScope->RunGuarded(
            [&] { auto callback = std::move(f); callback(); },
            [] {});
    });
}

void TGRpcConnectionsImpl::ScheduleDelayedTask(TSimpleCb&& fn, TDeadline deadline) {
    auto cbLow = [this, fn = std::move(fn)](bool ok) mutable {
        if (!ok) {
            return;
        }

        // Enqueue to user pool
        auto resp = new TSimpleCbResult(std::move(fn));
        EnqueueResponse(resp);
    };

    std::shared_ptr<IQueueClientContext> context;
    if (!TryCreateContext(context)) {
        cbLow(false);
        return;
    }

    if (deadline <= TDeadline::Now()) {
        cbLow(true);
        return;
    }

    auto action = MakeIntrusive<TDelayedAction>(
        std::move(cbLow),
        this,
        std::move(context),
        deadline);
    action->Start();
}

void TGRpcConnectionsImpl::ScheduleDelayedTask(TSimpleCb&& fn, TDeadline::Duration delay) {
    ScheduleDelayedTask(std::move(fn), TDeadline::AfterDuration(delay));
}

NThreading::TFuture<bool> TGRpcConnectionsImpl::ScheduleFuture(
        TDuration timeout,
        IQueueClientContextPtr context)
{
    IQueueClientContextProvider* provider = context.get();
    if (!provider) {
        provider = this;
    }

    return MakeIntrusive<TScheduledFuture>()
        ->Start(timeout, provider);
}

void TGRpcConnectionsImpl::ScheduleCallback(
        TDuration timeout,
        std::function<void(bool)> callback,
        IQueueClientContextPtr context)
{
    IQueueClientContextProvider* provider = context.get();
    if (!provider) {
        provider = this;
    }

    return MakeIntrusive<TScheduledCallback>(std::move(callback))
        ->Start(timeout, provider);
}

TDbDriverStatePtr TGRpcConnectionsImpl::GetDriverState(
    const std::optional<std::string>& database,
    const std::optional<std::string>& discoveryEndpoint,
    const std::optional<EDiscoveryMode>& discoveryMode,
    const std::optional<TSslCredentials>& sslCredentials,
    const std::optional<std::shared_ptr<ICredentialsProviderFactory>>& credentialsProviderFactory
) {
    return StateTracker_.GetDriverState(
        database.value_or(DefaultDatabase_),
        discoveryEndpoint.value_or(DefaultDiscoveryEndpoint_),
        discoveryMode.value_or(DefaultDiscoveryMode_),
        sslCredentials.value_or(SslCredentials_),
        credentialsProviderFactory.value_or(DefaultCredentialsProviderFactory_));
}

IQueueClientContextPtr TGRpcConnectionsImpl::CreateContext() {
    return DriverScope_->CreateContext();
}

TQueueClientCallbackGuardFactory TGRpcConnectionsImpl::GetCallbackGuardFactory() {
    return DriverScope_->GetCallbackGuardFactory();
}

bool TGRpcConnectionsImpl::TryCreateContext(IQueueClientContextPtr& context) {
    if (!context) {
        // Keep CQ running until the request is complete
        context = CreateContext();
        if (!context) {
            return false;
        }
    }
    return true;
}

void TGRpcConnectionsImpl::Stop(bool wait) {
    auto driverScope = DriverScope_;
    StateTracker_.SendNotification(
        TDbDriverState::ENotifyType::STOP,
        [driverScope = std::move(driverScope)](TDbDriverState::TCb& cb) {
            return driverScope->RunGuarded(
                [&] { return cb(); },
                [] { return NThreading::MakeFuture(); });
        }).Wait();
    DriverScope_->Cancel();
    GRpcClientLow_.Stop(wait);
    if (wait) {
        StopResponseQueue();
        DriverScope_->WaitCallbacksDrained();
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

    auto extractor = [promise]
        (google::protobuf::Any* any, TPlainStatus status) mutable {
            Ydb::Discovery::ListEndpointsResult result;
            if (any) {
                any->UnpackTo(&result);
            }
            TListEndpointsResult val{result, status};
            promise.SetValue(std::move(val));
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

    std::weak_ptr<TDbDriverState> weakState = dbState;

    auto driverScope = DriverScope_;
    return promise.GetFuture().Apply([this, weakState, driverScope = std::move(driverScope)](NThreading::TFuture<TListEndpointsResult> future){
        auto strong = weakState.lock();
        auto result = future.ExtractValue();
        return driverScope->RunGuarded(
            [&] {
                if (strong && result.DiscoveryStatus.IsTransportError()) {
                    strong->StatCollector.IncDiscoveryFailDueTransportError();
                }
                return NThreading::MakeFuture<TListEndpointsResult>(MutateDiscovery(std::move(result), strong.get()));
            },
            [&] { return NThreading::MakeFuture<TListEndpointsResult>(std::move(result)); });
    });
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

void TGRpcConnectionsImpl::EnqueueResponse(IObjectInQueue* action) {
    auto driverScope = DriverScope_;
    ResponseQueue_->Post([action, driverScope = std::move(driverScope)]() {
        driverScope->RunGuarded(
            [&] { action->Process(nullptr); },
            [&] {
                if (auto* response = dynamic_cast<TQueueResponse*>(action)) {
                    response->Cancel();
                } else {
                    delete action;
                }
            });
    });
}

void TGRpcConnectionsImpl::StopResponseQueue() {
    std::call_once(ResponseQueueStopOnce_, [this] {
        ResponseQueue_->Stop();
    });
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
