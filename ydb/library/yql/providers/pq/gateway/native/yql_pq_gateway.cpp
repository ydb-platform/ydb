#include "yql_pq_gateway.h"
#include "yql_pq_session.h"

#include <ydb/library/yql/utils/log/context.h>

#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>

#include <util/system/mutex.h>

#include <memory>

namespace NYql {

class TPqNativeGateway : public IPqGateway {
public:
    explicit TPqNativeGateway(const TPqGatewayServices& services);
    ~TPqNativeGateway();

    NThreading::TFuture<void> OpenSession(const TString& sessionId, const TString& username) override;
    NThreading::TFuture<void> CloseSession(const TString& sessionId) override;

    NPq::NConfigurationManager::TAsyncDescribePathResult DescribePath(
        const TString& sessionId,
        const TString& cluster,
        const TString& database,
        const TString& path,
        const TString& token) override;

    NThreading::TFuture<TListStreams> ListStreams(
        const TString& sessionId,
        const TString& cluster,
        const TString& database,
        const TString& token,
        ui32 limit,
        const TString& exclusiveStartStreamName = {}) override;

    void UpdateClusterConfigs(
        const TString& clusterName,
        const TString& endpoint,
        const TString& database,
        bool secure) override;

private:
    void InitClusterConfigs();
    TPqSession::TPtr GetExistingSession(const TString& sessionId) const;

private:
    mutable TMutex Mutex;
    const NKikimr::NMiniKQL::IFunctionRegistry* FunctionRegistry = nullptr;
    TPqGatewayConfigPtr Config;
    IMetricsRegistryPtr Metrics;
    ISecuredServiceAccountCredentialsFactory::TPtr CredentialsFactory;
    ::NPq::NConfigurationManager::IConnections::TPtr CmConnections;
    NYdb::TDriver YdbDriver;
    TPqClusterConfigsMapPtr ClusterConfigs;
    THashMap<TString, TPqSession::TPtr> Sessions;
};

TPqNativeGateway::TPqNativeGateway(const TPqGatewayServices& services)
    : FunctionRegistry(services.FunctionRegistry)
    , Config(services.Config)
    , Metrics(services.Metrics)
    , CredentialsFactory(services.CredentialsFactory)
    , CmConnections(services.CmConnections)
    , YdbDriver(services.YdbDriver)
{
    Y_UNUSED(FunctionRegistry);
    InitClusterConfigs();
}

void TPqNativeGateway::InitClusterConfigs() {
    ClusterConfigs = std::make_shared<TPqClusterConfigsMap>();
    for (const auto& cfg : Config->GetClusterMapping()) {
        auto& config = (*ClusterConfigs)[cfg.GetName()];
        config = cfg;
    }
}

void TPqNativeGateway::UpdateClusterConfigs(
    const TString& clusterName,
    const TString& endpoint,
    const TString& database,
    bool secure)
{
    with_lock (Mutex) {
        const auto foundCluster = ClusterConfigs->find(clusterName);
        Y_ABORT_UNLESS(foundCluster != ClusterConfigs->end());
        auto& cluster = foundCluster->second;
        cluster.SetEndpoint(endpoint);
        cluster.SetDatabase(database);
        cluster.SetUseSsl(secure);
    }
}

NThreading::TFuture<void> TPqNativeGateway::OpenSession(const TString& sessionId, const TString& username) {
    with_lock (Mutex) {
        auto [sessionIt, isNewSession] = Sessions.emplace(sessionId,
                                                          MakeIntrusive<TPqSession>(sessionId,
                                                                                    username,
                                                                                    CmConnections,
                                                                                    YdbDriver,
                                                                                    ClusterConfigs,
                                                                                    CredentialsFactory));
        if (!isNewSession) {
            YQL_LOG_CTX_THROW yexception() << "Session already exists: " << sessionId;
        }
    }
    return NThreading::MakeFuture();
}

NThreading::TFuture<void> TPqNativeGateway::CloseSession(const TString& sessionId) {
    with_lock (Mutex) {
        Sessions.erase(sessionId);
    }

    return NThreading::MakeFuture();
}

TPqSession::TPtr TPqNativeGateway::GetExistingSession(const TString& sessionId) const {
    with_lock (Mutex) {
        auto sessionIt = Sessions.find(sessionId);
        if (sessionIt == Sessions.end()) {
            YQL_LOG_CTX_THROW yexception() << "Pq gateway session was not found: " << sessionId;
        }
        return sessionIt->second;
    }
}

NPq::NConfigurationManager::TAsyncDescribePathResult TPqNativeGateway::DescribePath(const TString& sessionId, const TString& cluster, const TString& database, const TString& path, const TString& token) {
    return GetExistingSession(sessionId)->DescribePath(cluster, database, path, token);
}

NThreading::TFuture<IPqGateway::TListStreams> TPqNativeGateway::ListStreams(const TString& sessionId, const TString& cluster, const TString& database, const TString& token, ui32 limit, const TString& exclusiveStartStreamName) {
    return GetExistingSession(sessionId)->ListStreams(cluster, database, token, limit, exclusiveStartStreamName);
}

IPqGateway::TPtr CreatePqNativeGateway(const TPqGatewayServices& services) {
    return MakeIntrusive<TPqNativeGateway>(services);
}

TPqNativeGateway::~TPqNativeGateway() {
    Sessions.clear();
}

} // namespace NYql
