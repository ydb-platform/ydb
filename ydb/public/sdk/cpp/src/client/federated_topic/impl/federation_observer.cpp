#include <ydb/public/sdk/cpp/src/client/topic/common/log_lazy.h>
#include <ydb/public/api/grpc/ydb_federation_discovery_v1.grpc.pb.h>

#include <ydb/public/sdk/cpp/src/client/federated_topic/impl/federation_observer.h>

namespace NYdb::inline Dev::NFederatedTopic {

constexpr TDuration REDISCOVERY_DELAY = TDuration::Seconds(30);

TFederatedDbObserverImpl::TFederatedDbObserverImpl(std::shared_ptr<TGRpcConnectionsImpl> connections, const TFederatedTopicClientSettings& settings)
    : TClientImplCommon(std::move(connections), settings)
    , FederatedDbState(std::make_shared<TFederatedDbState>())
    , PromiseToInitState(NThreading::NewPromise())
    , FederationDiscoveryRetryPolicy(settings.RetryPolicy_)
{
    RpcSettings.ClientTimeout = settings.ConnectionTimeout_;
    RpcSettings.EndpointPolicy = TRpcRequestSettings::TEndpointPolicy::UseDiscoveryEndpoint;
    RpcSettings.UseAuth = true;
}

TFederatedDbObserverImpl::~TFederatedDbObserverImpl() {
    Stop();
}

std::shared_ptr<TFederatedDbState> TFederatedDbObserverImpl::GetState() {
    std::lock_guard guard(Lock);
    return FederatedDbState;
}

NThreading::TFuture<void> TFederatedDbObserverImpl::WaitForFirstState() {
    return PromiseToInitState.GetFuture();
}

void TFederatedDbObserverImpl::Start() {
    std::lock_guard guard(Lock);
    if (Stopping) {
        return;
    }
    ScheduleFederationDiscoveryImpl(TDuration::Zero());
}

void TFederatedDbObserverImpl::Stop() {
    NYdbGrpc::IQueueClientContextPtr ctx;
    {
        std::lock_guard guard(Lock);
        Stopping = true;
        ctx = std::exchange(FederationDiscoveryDelayContext, nullptr);
    }
    if (ctx) {
        ctx->Cancel();
    }
}

// If observer is stale it will never update state again because of client retry policy
bool TFederatedDbObserverImpl::IsStale() const {
    std::lock_guard guard(const_cast<TSpinLock&>(Lock));
    return PromiseToInitState.HasValue() && !FederatedDbState->Status.IsSuccess();
}

Ydb::FederationDiscovery::ListFederationDatabasesRequest TFederatedDbObserverImpl::ComposeRequest() const {
    return {};
}

void TFederatedDbObserverImpl::RunFederationDiscoveryImpl() {
    Y_ABORT_UNLESS(Lock.IsLocked());

    FederationDiscoveryDelayContext = Connections_->CreateContext();
    if (!FederationDiscoveryDelayContext) {
        Stopping = true;
        // TODO log DRIVER_IS_STOPPING_DESCRIPTION
        return;
    }

    auto extractor = [selfCtx = SelfContext]
        (google::protobuf::Any* any, TPlainStatus status) mutable {
        if (auto self = selfCtx->LockShared()) {
            Ydb::FederationDiscovery::ListFederationDatabasesResult result;
            if (any) {
                any->UnpackTo(&result);
            }
            self->OnFederationDiscovery(std::move(status), std::move(result));
        }
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

void TFederatedDbObserverImpl::ScheduleFederationDiscoveryImpl(TDuration delay) {
    Y_ABORT_UNLESS(Lock.IsLocked());
    auto cb = [selfCtx = SelfContext](bool ok) {
        if (ok) {
            if (auto self = selfCtx->LockShared()) {
                std::lock_guard guard(self->Lock);
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

void TFederatedDbObserverImpl::OnFederationDiscovery(TStatus&& status, Ydb::FederationDiscovery::ListFederationDatabasesResult&& result) {
    {
        std::lock_guard guard(Lock);
        if (Stopping) {
            // TODO log something
            return;
        }

        // BAD_REQUEST may be returned from FederationDiscovery:
        //   1) The request was meant for a non-federated topic: fall back to single db mode.
        //   2) The database path in the request is simply wrong: the client should get the BAD_REQUEST status.
        if (status.GetStatus() == EStatus::CLIENT_CALL_UNIMPLEMENTED || status.GetStatus() == EStatus::BAD_REQUEST) {
            LOG_LAZY(DbDriverState_->Log, TLOG_INFO, TStringBuilder()
                << "OnFederationDiscovery fall back to single mode, database=" << DbDriverState_->Database);
            FederatedDbState->Status = TPlainStatus{};  // SUCCESS
            FederatedDbState->ControlPlaneEndpoint = DbDriverState_->DiscoveryEndpoint;
            auto dbState = Connections_->GetDriverState(std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt);
            FederatedDbState->ControlPlaneEndpoint = dbState->DiscoveryEndpoint;
            // FederatedDbState->SelfLocation = ???;
            auto db = std::make_shared<Ydb::FederationDiscovery::DatabaseInfo>();
            db->set_path(TStringType{DbDriverState_->Database});
            db->set_endpoint(TStringType{DbDriverState_->DiscoveryEndpoint});
            db->set_status(Ydb::FederationDiscovery::DatabaseInfo_Status_AVAILABLE);
            db->set_weight(100);
            FederatedDbState->DbInfos.emplace_back(std::move(db));

        } else {
            if (status.IsSuccess()) {
                ScheduleFederationDiscoveryImpl(REDISCOVERY_DELAY);
            } else {
                LOG_LAZY(DbDriverState_->Log, TLOG_ERR, TStringBuilder()
                    << "OnFederationDiscovery: Got error. Status: " << status.GetStatus()
                    << ". Description: " << status.GetIssues().ToOneLineString());
                if (!FederationDiscoveryRetryState) {
                    FederationDiscoveryRetryState = FederationDiscoveryRetryPolicy->CreateRetryState();
                }

                if (auto d = FederationDiscoveryRetryState->GetNextRetryDelay(status.GetStatus())) {
                    ScheduleFederationDiscoveryImpl(*d);
                    return;
                }
                // If there won't be another retry, we replace FederatedDbState with the unsuccessful one
                // and set the PromiseToInitState to make the observer stale (see IsStale method).
            }

            // TODO validate new state and check if differs from previous
            auto newInfo = std::make_shared<TFederatedDbState>(std::move(result), std::move(status));

            // TODO update only if new state differs
            std::swap(FederatedDbState, newInfo);
        }
    }

    if (!PromiseToInitState.HasValue()) {
        PromiseToInitState.SetValue();
    }
}

IOutputStream& operator<<(IOutputStream& out, TFederatedDbState const& state) {
    out << "{ Status: " << state.Status.GetStatus()
        << " SelfLocation: \"" << state.SelfLocation << '"';
    if (auto const& issues = state.Status.GetIssues(); !issues.Empty()) {
        out << " Issues: { " << issues.ToOneLineString() << " }";
    }
    if (!state.DbInfos.empty()) {
        out << " DbInfos: [ ";
        bool first = true;
        for (auto const& info : state.DbInfos) {
            if (first) {
                first = false;
            } else {
                out << " ";
            }
            out << "{ " << info->ShortDebugString() << " }";
        }
        out << " ]";
    }
    return out << " }";
}

} // namespace NYdb::NFederatedTopic
