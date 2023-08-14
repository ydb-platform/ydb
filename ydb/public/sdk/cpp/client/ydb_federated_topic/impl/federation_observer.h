#pragma once

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/make_request/make.h>
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/grpc_connections/grpc_connections.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <ydb/public/api/protos/ydb_federation_discovery.pb.h>

#include <ydb/public/sdk/cpp/client/ydb_common_client/impl/client.h>
#include <ydb/public/sdk/cpp/client/ydb_federated_topic/federated_topic.h>

#include <util/system/spinlock.h>
#include <util/generic/hash.h>

#include <deque>
#include <memory>

namespace NYdb::NFederatedTopic {

struct TFederatedDbState {
public:
    using TDbInfo = Ydb::FederationDiscovery::DatabaseInfo;

    TStatus Status;
    TString ControlPlaneEndpoint;
    TString SelfLocation;
    std::vector<std::shared_ptr<TDbInfo>> DbInfos;

public:
    TFederatedDbState() : Status(EStatus::STATUS_UNDEFINED, {}) {}
    TFederatedDbState(Ydb::FederationDiscovery::ListFederationDatabasesResult result, TStatus status)
        : Status(std::move(status))
        , ControlPlaneEndpoint(result.control_plane_endpoint())
        , SelfLocation(result.self_location())
        {
            // TODO remove copy
            for (const auto& db : result.federation_databases()) {
                DbInfos.push_back(std::make_shared<TDbInfo>(db));
            }
        }
};


class TFederatedDbObserver : public TClientImplCommon<TFederatedDbObserver> {
public:
    static constexpr TDuration REDISCOVER_DELAY = TDuration::Seconds(60);

public:
    TFederatedDbObserver(std::shared_ptr<TGRpcConnectionsImpl> connections, const TFederatedTopicClientSettings& settings);

    ~TFederatedDbObserver();

    std::shared_ptr<TFederatedDbState> GetState();

    NThreading::TFuture<void> WaitForFirstState();

    void Start();
    void Stop();

    bool IsStale() const;

private:
    Ydb::FederationDiscovery::ListFederationDatabasesRequest ComposeRequest() const;
    void RunFederationDiscoveryImpl();
    void ScheduleFederationDiscoveryImpl(TDuration delay);
    void OnFederationDiscovery(TStatus&& status, Ydb::FederationDiscovery::ListFederationDatabasesResult&& result);

private:
    std::shared_ptr<TFederatedDbState> FederatedDbState;
    NThreading::TPromise<void> PromiseToInitState;
    TRpcRequestSettings RpcSettings;
    TSpinLock Lock;

    NTopic::IRetryPolicy::TPtr FederationDiscoveryRetryPolicy;
    NTopic::IRetryPolicy::IRetryState::TPtr FederationDiscoveryRetryState;
    NGrpc::IQueueClientContextPtr FederationDiscoveryDelayContext;

    bool Stopping = false;
};

} // namespace NYdb::NFederatedTopic
