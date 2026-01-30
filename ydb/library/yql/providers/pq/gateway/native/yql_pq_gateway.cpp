#include "yql_pq_gateway.h"
#include "yql_pq_session.h"

#include <ydb/library/yql/providers/pq/gateway/clients/external/yql_pq_federated_topic_client.h>
#include <ydb/library/yql/providers/pq/gateway/clients/external/yql_pq_topic_client.h>
#include <ydb/library/yverify_stream/yverify_stream.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>

#include <yql/essentials/providers/common/proto/gateways_config.pb.h>
#include <yql/essentials/utils/log/context.h>

#include <util/system/mutex.h>

#include <memory>

namespace NYql {

namespace {

using namespace NYdb;
using namespace NYdb::NTopic;
using namespace NYdb::NFederatedTopic;

class TPqNativeGateway final : public IPqGateway {
public:
    explicit TPqNativeGateway(const TPqGatewayServices& services)
        : Config(services.Config)
        , Metrics(services.Metrics)
        , CredentialsFactory(services.CredentialsFactory)
        , CmConnections(services.CmConnections)
        , YdbDriver(services.YdbDriver)
        , CommonTopicClientSettings(services.CommonTopicClientSettings)
        , LocalTopicClientFactory(services.LocalTopicClientFactory)
    {
        UpdateClusterConfigs(Config);
    }

    ~TPqNativeGateway() {
        Sessions.clear();
    }

    NThreading::TFuture<void> OpenSession(const TString& sessionId, const TString& username) final {
        with_lock (Mutex) {
            auto [sessionIt, isNewSession] = Sessions.emplace(sessionId, MakeIntrusive<TPqSession>(
                sessionId,
                username,
                CmConnections,
                YdbDriver,
                ClusterConfigs,
                CredentialsFactory,
                LocalTopicClientFactory
            ));
            if (!isNewSession) {
                YQL_LOG_CTX_THROW yexception() << "Session already exists: " << sessionId;
            }
        }
        return NThreading::MakeFuture();
    }

    NThreading::TFuture<void> CloseSession(const TString& sessionId) final {
        with_lock (Mutex) {
            Sessions.erase(sessionId);
        }

        return NThreading::MakeFuture();
    }

    NPq::NConfigurationManager::TAsyncDescribePathResult DescribePath(const TString& sessionId, const TString& cluster, const TString& database, const TString& path, const TString& token) final {
        return GetExistingSession(sessionId)->DescribePath(cluster, database, path, token);
    }

    NThreading::TFuture<TListStreams> ListStreams(const TString& sessionId, const TString& cluster, const TString& database, const TString& token, ui32 limit, const TString& exclusiveStartStreamName) final {
        return GetExistingSession(sessionId)->ListStreams(cluster, database, token, limit, exclusiveStartStreamName);
    }

    TAsyncDescribeFederatedTopicResult DescribeFederatedTopic(const TString& sessionId, const TString& cluster, const TString& database, const TString& path, const TString& token) final {
        return GetExistingSession(sessionId)->DescribeFederatedTopic(cluster, database, path, token);
    }

    void UpdateClusterConfigs(const TString& clusterName, const TString& endpoint, const TString& database, bool secure) final {
        with_lock (Mutex) {
            const auto foundCluster = ClusterConfigs->find(clusterName);
            Y_ABORT_UNLESS(foundCluster != ClusterConfigs->end());
            auto& cluster = foundCluster->second;
            cluster.SetEndpoint(endpoint);
            cluster.SetDatabase(database);
            cluster.SetUseSsl(secure);
        }
    }

    void UpdateClusterConfigs(const TPqGatewayConfigPtr& config) final {   
        ClusterConfigs = std::make_shared<TPqClusterConfigsMap>();
        for (const auto& cfg : config->GetClusterMapping()) {
            AddCluster(cfg);
        }
    }

    void AddCluster(const NYql::TPqClusterConfig& cluster) final {
        Y_ABORT_UNLESS(ClusterConfigs);
        (*ClusterConfigs)[cluster.GetName()] = cluster;
    }

    ITopicClient::TPtr GetTopicClient(const TDriver& driver, const TTopicClientSettings& settings) final {
        const bool hasEndpoint = HasEndpoint(driver, settings);
        if (!hasEndpoint && LocalTopicClientFactory) {
            return LocalTopicClientFactory->CreateTopicClient(settings);
        }

        Y_VALIDATE(hasEndpoint, "Missing endpoint value for topic client and local topics are not allowed");
        return CreateExternalTopicClient(driver, settings);
    }

    IFederatedTopicClient::TPtr GetFederatedTopicClient(const TDriver& driver, const TFederatedTopicClientSettings& settings) final {
        const bool hasEndpoint = HasEndpoint(driver, settings);
        if (!hasEndpoint && LocalTopicClientFactory) {
            return LocalTopicClientFactory->CreateFederatedTopicClient(settings);
        }

        Y_VALIDATE(hasEndpoint, "Missing endpoint value for federated topic client and local topics are not allowed");
        return CreateExternalFederatedTopicClient(driver, settings);
    }

    TTopicClientSettings GetTopicClientSettings() const final {
        return CommonTopicClientSettings.GetOrElse({});
    }

    TFederatedTopicClientSettings GetFederatedTopicClientSettings() const final {
        TFederatedTopicClientSettings settings;

        if (!CommonTopicClientSettings) {
            return settings;
        }

        settings.DefaultCompressionExecutor(CommonTopicClientSettings->DefaultCompressionExecutor_);
        settings.DefaultHandlersExecutor(CommonTopicClientSettings->DefaultHandlersExecutor_);

#define COPY_OPTIONAL_SETTINGS(NAME)                        \
    if (CommonTopicClientSettings->NAME##_) {               \
        settings.NAME(*CommonTopicClientSettings->NAME##_); \
    }

        COPY_OPTIONAL_SETTINGS(CredentialsProviderFactory);
        COPY_OPTIONAL_SETTINGS(SslCredentials);
        COPY_OPTIONAL_SETTINGS(DiscoveryMode);

#undef COPY_OPTIONAL_SETTINGS

        return settings;
    }

private:
    static bool HasEndpoint(const TDriver& driver, const TCommonClientSettings& settings) {
        if (!driver.GetConfig().GetEndpoint().empty()) {
            return true;
        }
        return !settings.DiscoveryEndpoint_.value_or("").empty();
    }

    TPqSession::TPtr GetExistingSession(const TString& sessionId) const {
        with_lock (Mutex) {
            auto sessionIt = Sessions.find(sessionId);
            if (sessionIt == Sessions.end()) {
                YQL_LOG_CTX_THROW yexception() << "PQ gateway session was not found: " << sessionId;
            }
            return sessionIt->second;
        }
    }

private:
    const TPqGatewayConfigPtr Config;
    const IMetricsRegistryPtr Metrics;
    const ISecuredServiceAccountCredentialsFactory::TPtr CredentialsFactory;
    const ::NPq::NConfigurationManager::IConnections::TPtr CmConnections;
    const TDriver YdbDriver;
    const TMaybe<TTopicClientSettings> CommonTopicClientSettings;
    const IPqLocalClientFactory::TPtr LocalTopicClientFactory;
    mutable TMutex Mutex;
    TPqClusterConfigsMapPtr ClusterConfigs;
    THashMap<TString, TPqSession::TPtr> Sessions;
};

} // anonymous namespace

IPqGateway::TPtr CreatePqNativeGateway(const TPqGatewayServices& services) {
    return MakeIntrusive<TPqNativeGateway>(services);
}

} // namespace NYql
