#include <ydb/public/api/grpc/ydb_federation_discovery_v1.grpc.pb.h>

#include <ydb/public/sdk/cpp/client/ydb_federated_topic/impl/federation_observer.h>

namespace NYdb::NFederatedTopic {

constexpr TDuration REDISCOVERY_DELAY = TDuration::Seconds(30);

TFederatedDbObserver::TFederatedDbObserver(std::shared_ptr<TGRpcConnectionsImpl> connections, const TFederatedTopicClientSettings& settings)
    : TClientImplCommon(std::move(connections), settings)
    , FederatedDbState(std::make_shared<TFederatedDbState>())
    , PromiseToInitState(NThreading::NewPromise())
    , FederationDiscoveryRetryPolicy(settings.RetryPolicy_)
{
    RpcSettings.ClientTimeout = settings.ConnectionTimeout_;
    RpcSettings.EndpointPolicy = TRpcRequestSettings::TEndpointPolicy::UseDiscoveryEndpoint;
    RpcSettings.UseAuth = true;
}

TFederatedDbObserver::~TFederatedDbObserver() {
    Stop();
}

std::shared_ptr<TFederatedDbState> TFederatedDbObserver::GetState() {
    with_lock(Lock) {
        return FederatedDbState;
    }
}

NThreading::TFuture<void> TFederatedDbObserver::WaitForFirstState() {
    return PromiseToInitState.GetFuture();
}

void TFederatedDbObserver::Start() {
    with_lock(Lock) {
        if (Stopping) {
            return;
        }
        ScheduleFederationDiscoveryImpl(TDuration::Zero());
    }
}

void TFederatedDbObserver::Stop() {
    NGrpc::IQueueClientContextPtr ctx;
    with_lock(Lock) {
        Stopping = true;
        ctx = std::exchange(FederationDiscoveryDelayContext, nullptr);
    }
    if (ctx) {
        ctx->Cancel();
    }
}

// If observer is stale it will never update state again because of client retry policy
bool TFederatedDbObserver::IsStale() const {
    with_lock(Lock) {
        return PromiseToInitState.HasValue() && !FederatedDbState->Status.IsSuccess();
    }
}

Ydb::FederationDiscovery::ListFederationDatabasesRequest TFederatedDbObserver::ComposeRequest() const {
    return {};
}

void TFederatedDbObserver::RunFederationDiscoveryImpl() {
    Y_VERIFY(Lock.IsLocked());

    FederationDiscoveryDelayContext = Connections_->CreateContext();
    if (!FederationDiscoveryDelayContext) {
        Stopping = true;
        // TODO log DRIVER_IS_STOPPING_DESCRIPTION
        return;
    }

    auto extractor = [self = shared_from_this()]
        (google::protobuf::Any* any, TPlainStatus status) mutable {

        Ydb::FederationDiscovery::ListFederationDatabasesResult result;
        if (any) {
            any->UnpackTo(&result);
        }
        self->OnFederationDiscovery(std::move(status), std::move(result));
    };

    Connections_->RunDeferred<Ydb::FederationDiscovery::V1::FederationDiscoveryService,
                             Ydb::FederationDiscovery::ListFederationDatabasesRequest,
                             Ydb::FederationDiscovery::ListFederationDatabasesResponse>(
        ComposeRequest(),
        std::move(extractor),
        &Ydb::FederationDiscovery::V1::FederationDiscoveryService::Stub::AsyncListFederationDatabases,
        DbDriverState_,
        {},  // no polling unready operations, so no need in delay parameter
        RpcSettings,
        FederationDiscoveryDelayContext);
}

void TFederatedDbObserver::ScheduleFederationDiscoveryImpl(TDuration delay) {
    Y_VERIFY(Lock.IsLocked());
    auto cb = [self = shared_from_this()](bool ok) {
        if (ok) {
            with_lock(self->Lock) {
                if (self->Stopping) {
                    return;
                }
                self->RunFederationDiscoveryImpl();
            }
        }
    };

    FederationDiscoveryDelayContext = Connections_->CreateContext();
    if (!FederationDiscoveryDelayContext) {
        Stopping = true;
        // TODO log DRIVER_IS_STOPPING_DESCRIPTION
        return;
    }
    Connections_->ScheduleCallback(delay,
                                  std::move(cb),
                                  FederationDiscoveryDelayContext);

}

void TFederatedDbObserver::OnFederationDiscovery(TStatus&& status, Ydb::FederationDiscovery::ListFederationDatabasesResult&& result) {
    with_lock(Lock) {
        if (Stopping) {
            // TODO log something
            return;
        }

        if (!status.IsSuccess()) {
            // if UNIMPLEMENTED - fall back to single db mode:
            //   - initialize FederatedDbState with original db + endpoint
            //   - no more updates: no reschedules

            // TODO
            // update counters errors

            if (!FederationDiscoveryRetryState) {
                FederationDiscoveryRetryState = FederationDiscoveryRetryPolicy->CreateRetryState();
            }
            TMaybe<TDuration> retryDelay = FederationDiscoveryRetryState->GetNextRetryDelay(status.GetStatus());
            if (retryDelay) {
                ScheduleFederationDiscoveryImpl(*retryDelay);
                return;
            }
        } else {
            ScheduleFederationDiscoveryImpl(REDISCOVERY_DELAY);
        }

        // TODO validate new state and check if differs from previous

        auto newInfo = std::make_shared<TFederatedDbState>(std::move(result), std::move(status));
        // TODO update only if new state differs
        std::swap(FederatedDbState, newInfo);
    }

    if (!PromiseToInitState.HasValue()) {
        PromiseToInitState.SetValue();
    }
}

} // namespace NYdb::NFederatedTopic
