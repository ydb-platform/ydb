#define INCLUDE_YDB_INTERNAL_H
#include "grpc_connections.h"

#include <ydb/public/sdk/cpp/client/ydb_types/exceptions/exceptions.h>

namespace NYdb {

bool IsTokenCorrect(const TStringType& in) {
    for (char c : in) {
        if (!(IsAsciiAlnum(c) || IsAsciiPunct(c) || c == ' '))
            return false;
    }
    return true;
}

TStringType GetAuthInfo(TDbDriverStatePtr p) {
    auto token = p->CredentialsProvider->GetAuthInfo();
    if (!IsTokenCorrect(token)) {
        throw TContractViolation("token is incorrect, illegal characters found");
    }
    return token;
}

void SetDatabaseHeader(TCallMeta& meta, const TStringType& database) {
    // See TDbDriverStateTracker::GetDriverState to find place where we do quote non ASCII characters
    meta.Aux.push_back({YDB_DATABASE_HEADER, database});
}

TStringType CreateSDKBuildInfo() {
    return TStringType("ydb-cpp-sdk/") + GetSdkSemver();
}

template<class TDerived>
class TScheduledObject : public TThrRefBase {
    using TSelf = TScheduledObject<TDerived>;
    using TPtr = TIntrusivePtr<TSelf>;

    Y_FORCE_INLINE TDerived* Derived() {
        return static_cast<TDerived*>(this);
    }

protected:
    TScheduledObject() { }

    void Start(TDuration timeout, IQueueClientContextProvider* provider) {
        auto context = provider->CreateContext();
        if (!context) {
            Derived()->OnComplete(false);
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

        Derived()->OnComplete(ok);
    }

private:
    std::mutex Mutex;
    IQueueClientContextPtr Context;
    grpc::Alarm Alarm;

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
        Callback(ok);
        Callback = { };
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
    , ResponseQueue_(CreateThreadPool(params->GetClientThreadsNum()))
    , DefaultDiscoveryEndpoint_(params->GetEndpoint())
    , SslCredentials_(params->GetSslCredentials())
    , DefaultDatabase_(params->GetDatabase())
    , DefaultCredentialsProviderFactory_(params->GetCredentialsProviderFactory())
    , StateTracker_(this)
    , DefaultDiscoveryMode_(params->GetDiscoveryMode())
    , MaxQueuedRequests_(params->GetMaxQueuedRequests())
    , DrainOnDtors_(params->GetDrinOnDtors())
    , BalancingSettings_(params->GetBalancingSettings())
    , GRpcKeepAliveTimeout_(params->GetGRpcKeepAliveTimeout())
    , GRpcKeepAlivePermitWithoutCalls_(params->GetGRpcKeepAlivePermitWithoutCalls())
    , MemoryQuota_(params->GetMemoryQuota())
    , MaxInboundMessageSize_(params->GetMaxInboundMessageSize())
    , MaxOutboundMessageSize_(params->GetMaxOutboundMessageSize())
    , MaxMessageSize_(params->GetMaxMessageSize())
    , QueuedRequests_(0)
#ifndef YDB_GRPC_BYPASS_CHANNEL_POOL
    , ChannelPool_(params->GetTcpKeepAliveSettings(), params->GetSocketIdleTimeout())
#endif
    , GRpcClientLow_(params->GetNetworkThreadsNum())
    , Log(params->GetLog())
{
#ifndef YDB_GRPC_BYPASS_CHANNEL_POOL
    if (params->GetSocketIdleTimeout() != TDuration::Max()) {
        auto channelPoolUpdateWrapper = [this]
            (NYql::TIssues&&, EStatus status) mutable
        {
            if (status != EStatus::SUCCESS) {
                return false;
            }

            ChannelPool_.DeleteExpiredStubsHolders();
            return true;
        };
        AddPeriodicTask(channelPoolUpdateWrapper, params->GetSocketIdleTimeout() * 0.1);
    }
#endif
    //TAdaptiveThreadPool ignores params
    ResponseQueue_->Start(params->GetClientThreadsNum(), params->GetMaxQueuedResponses());
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
    GRpcClientLow_.Stop(true);
    ResponseQueue_->Stop();
}

void TGRpcConnectionsImpl::AddPeriodicTask(TPeriodicCb&& cb, TDuration period) {
    std::shared_ptr<IQueueClientContext> context;
    if (!TryCreateContext(context)) {
        NYql::TIssues issues;
        cb(std::move(issues), EStatus::CLIENT_INTERNAL_ERROR);
    } else {
        auto action = MakeIntrusive<TPeriodicAction>(
            std::move(cb),
            this,
            std::move(context),
            period);
        action->Start();
    }
}

void TGRpcConnectionsImpl::ScheduleOneTimeTask(TSimpleCb&& fn, TDuration timeout) {
    auto cbLow = [this, fn = std::move(fn)](NYql::TIssues&&, EStatus status) mutable {
        if (status != EStatus::SUCCESS) {
            return false;
        }

        std::shared_ptr<IQueueClientContext> context;

        if (!TryCreateContext(context)) {
            // Shutting down, fn must handle it
            fn();
        } else {
            // Enqueue to user pool
            auto resp = new TSimpleCbResult(
                std::move(fn),
                this,
                std::move(context));
            EnqueueResponse(resp);
        }

        return false;
    };

    if (timeout) {
        AddPeriodicTask(std::move(cbLow), timeout);
    } else {
        cbLow(NYql::TIssues(), EStatus::SUCCESS);
    }
}

NThreading::TFuture<bool> TGRpcConnectionsImpl::ScheduleFuture(
        TDuration timeout,
        IQueueClientContextPtr context)
{
    IQueueClientContextProvider* provider = context.get();
    if (!provider) {
        provider = &GRpcClientLow_;
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
        provider = &GRpcClientLow_;
    }

    return MakeIntrusive<TScheduledCallback>(std::move(callback))
        ->Start(timeout, provider);
}

TDbDriverStatePtr TGRpcConnectionsImpl::GetDriverState(
    const TMaybe<TStringType>& database,
    const TMaybe<TStringType>& discoveryEndpoint,
    const TMaybe<EDiscoveryMode>& discoveryMode,
    const TMaybe<TSslCredentials>& sslCredentials,
    const TMaybe<std::shared_ptr<ICredentialsProviderFactory>>& credentialsProviderFactory
) {
    return StateTracker_.GetDriverState(
        database ? database.GetRef() : DefaultDatabase_,
        discoveryEndpoint ? discoveryEndpoint.GetRef() : DefaultDiscoveryEndpoint_,
        discoveryMode ? discoveryMode.GetRef() : DefaultDiscoveryMode_,
        sslCredentials ? sslCredentials.GetRef() : SslCredentials_,
        credentialsProviderFactory ? credentialsProviderFactory.GetRef() : DefaultCredentialsProviderFactory_);
}

IQueueClientContextPtr TGRpcConnectionsImpl::CreateContext() {
    return GRpcClientLow_.CreateContext();
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

void TGRpcConnectionsImpl::WaitIdle() {
    GRpcClientLow_.WaitIdle();
}

void TGRpcConnectionsImpl::Stop(bool wait) {
    StateTracker_.SendNotification(TDbDriverState::ENotifyType::STOP).Wait();
    GRpcClientLow_.Stop(wait);
}

void TGRpcConnectionsImpl::SetGrpcKeepAlive(NYdbGrpc::TGRpcClientConfig& config, const TDuration& timeout, bool permitWithoutCalls) {
    ui64 timeoutMs = timeout.MilliSeconds();
    config.IntChannelParams[GRPC_ARG_KEEPALIVE_TIME_MS] = timeoutMs >> 3;
    config.IntChannelParams[GRPC_ARG_KEEPALIVE_TIMEOUT_MS] = timeoutMs;
    config.IntChannelParams[GRPC_ARG_HTTP2_MAX_PINGS_WITHOUT_DATA] = 0;
    config.IntChannelParams[GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS] = permitWithoutCalls ? 1 : 0;
}

TAsyncListEndpointsResult TGRpcConnectionsImpl::GetEndpoints(TDbDriverStatePtr dbState) {
    Ydb::Discovery::ListEndpointsRequest request;
    request.set_database(dbState->Database);

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
    rpcSettings.ClientTimeout = GET_ENDPOINTS_TIMEOUT;

    RunDeferred<Ydb::Discovery::V1::DiscoveryService, Ydb::Discovery::ListEndpointsRequest, Ydb::Discovery::ListEndpointsResponse>(
        std::move(request),
        extractor,
        &Ydb::Discovery::V1::DiscoveryService::Stub::AsyncListEndpoints,
        dbState->shared_from_this(),
        INITIAL_DEFERRED_CALL_DELAY,
        rpcSettings);

    std::weak_ptr<TDbDriverState> weakState = dbState;

    return promise.GetFuture().Apply([this, weakState](NThreading::TFuture<TListEndpointsResult> future){
        auto strong = weakState.lock();
        auto result = future.ExtractValue();
        if (strong && result.DiscoveryStatus.IsTransportError()) {
            strong->StatCollector.IncDiscoveryFailDueTransportError();
        }
        return NThreading::MakeFuture<TListEndpointsResult>(MutateDiscovery(std::move(result), *strong));
    });
}

TListEndpointsResult TGRpcConnectionsImpl::MutateDiscovery(TListEndpointsResult result, const TDbDriverState& dbDriverState) {
    std::lock_guard lock(ExtensionsLock_);
    if (!DiscoveryMutatorCb)
        return result;

    auto endpoint = result.DiscoveryStatus.Endpoint;
    auto ydbStatus = NYdb::TStatus(std::move(result.DiscoveryStatus));

    auto aux = IDiscoveryMutatorApi::TAuxInfo {
        .Database = dbDriverState.Database,
        .DiscoveryEndpoint = dbDriverState.DiscoveryEndpoint
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

TBalancingSettings TGRpcConnectionsImpl::GetBalancingSettings() const {
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
            Cerr << "Unknown IMetricRegistry impl" << Endl;
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

void TGRpcConnectionsImpl::SetDiscoveryMutator(IDiscoveryMutatorApi::TMutatorCb&& cb) {
    std::lock_guard lock(ExtensionsLock_);
    DiscoveryMutatorCb = std::move(cb);
}

const TLog& TGRpcConnectionsImpl::GetLog() const {
    return Log;
}

void TGRpcConnectionsImpl::EnqueueResponse(IObjectInQueue* action) {
    Y_ENSURE(ResponseQueue_->Add(action));
}

} // namespace NYdb
