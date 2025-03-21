#include "yql_pq_gateway.h"
#include "yql_pq_session.h"

#include <yql/essentials/utils/log/context.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>

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

    void UpdateClusterConfigs(const TPqGatewayConfigPtr& config) override;

    ITopicClient::TPtr GetTopicClient(const NYdb::TDriver& driver, const NYdb::NTopic::TTopicClientSettings& settings) override;
    NYdb::NTopic::TTopicClientSettings GetTopicClientSettings() const override;

private:
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
    TMaybe<NYdb::NTopic::TTopicClientSettings> CommonTopicClientSettings;
};

TPqNativeGateway::TPqNativeGateway(const TPqGatewayServices& services)
    : FunctionRegistry(services.FunctionRegistry)
    , Config(services.Config)
    , Metrics(services.Metrics)
    , CredentialsFactory(services.CredentialsFactory)
    , CmConnections(services.CmConnections)
    , YdbDriver(services.YdbDriver)
    , CommonTopicClientSettings(services.CommonTopicClientSettings)
{
    Y_UNUSED(FunctionRegistry);
    UpdateClusterConfigs(Config);
}

void TPqNativeGateway::UpdateClusterConfigs(const TPqGatewayConfigPtr& config) {
    ClusterConfigs = std::make_shared<TPqClusterConfigsMap>();
    for (const auto& cfg : config->GetClusterMapping()) {
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

ITopicClient::TPtr TPqNativeGateway::GetTopicClient(const NYdb::TDriver& driver, const NYdb::NTopic::TTopicClientSettings& settings = NYdb::NTopic::TTopicClientSettings()) {
    return MakeIntrusive<TNativeTopicClient>(driver, settings);
}

NYdb::NTopic::TTopicClientSettings TPqNativeGateway::GetTopicClientSettings() const {
    return CommonTopicClientSettings ? *CommonTopicClientSettings : NYdb::NTopic::TTopicClientSettings();
}

TPqNativeGateway::~TPqNativeGateway() {
    Sessions.clear();
}

class TPqNativeGatewayFactory : public IPqGatewayFactory {
public:
    TPqNativeGatewayFactory(const NYql::TPqGatewayServices& services)
        : Services(services) {}

    IPqGateway::TPtr CreatePqGateway() override {
        return CreatePqNativeGateway(Services);
    }
    const NYql::TPqGatewayServices Services;
};

IPqGatewayFactory::TPtr CreatePqNativeGatewayFactory(const NYql::TPqGatewayServices& services) {
    return MakeIntrusive<TPqNativeGatewayFactory>(services);
}


} // namespace NYql
