#pragma once

#include <ydb/library/yql/providers/common/token_accessor/client/factory.h>
#include <ydb/library/yql/providers/pq/cm_client/client.h>
#include <ydb/library/yql/providers/pq/gateway/abstract/yql_pq_gateway.h>
#include <ydb/library/yql/providers/pq/gateway/clients/local/yql_pq_local_topic_client_factory.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/datastreams/datastreams.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>

#include <util/generic/ptr.h>
#include <util/system/mutex.h>

namespace NYql {

class TPqClusterConfig;
using TPqClusterConfigsMap = THashMap<TString, TPqClusterConfig>;
using TPqClusterConfigsMapPtr = std::shared_ptr<TPqClusterConfigsMap>;

class TPqSession final : public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<TPqSession>;

    TPqSession(const TString& sessionId, const TString& username, const NPq::NConfigurationManager::IConnections::TPtr& cmConnections,
        const NYdb::TDriver& ydbDriver, const TPqClusterConfigsMapPtr& clusterConfigs, ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
        IPqLocalClientFactory::TPtr localTopicClientFactory);

    NPq::NConfigurationManager::TAsyncDescribePathResult DescribePath(const TString& cluster, const TString& database, const TString& path, const TString& token);

    NThreading::TFuture<IPqGateway::TListStreams> ListStreams(const TString& cluster, const TString& database, const TString& token, ui32 limit, const TString& exclusiveStartStreamName);

    IPqGateway::TAsyncDescribeFederatedTopicResult DescribeFederatedTopic(const TString& cluster, const TString& database, const TString& path, const TString& token);

private:
    const NPq::NConfigurationManager::IClient::TPtr& GetConfigManagerClient(const TString& cluster, const TPqClusterConfig& cfg, std::shared_ptr<NYdb::ICredentialsProviderFactory> credentialsProviderFactory);

    NYdb::NDataStreams::V1::TDataStreamsClient& GetDsClient(const TString& cluster, const TString& database, const TPqClusterConfig& cfg, std::shared_ptr<NYdb::ICredentialsProviderFactory> credentialsProviderFactory);

    NYdb::NFederatedTopic::TFederatedTopicClient& GetYdbFederatedPqClient(const TString& cluster, const TString& database, const TPqClusterConfig& cfg, std::shared_ptr<NYdb::ICredentialsProviderFactory> credentialsProviderFactory);

    NYdb::NTopic::TTopicClient& GetYdbPqClient(const TString& cluster, const TString& database, const TPqClusterConfig& cfg, std::shared_ptr<NYdb::ICredentialsProviderFactory> credentialsProviderFactory);

private:
    const TString SessionId;
    const TString UserName;
    const NPq::NConfigurationManager::IConnections::TPtr CmConnections;
    const NYdb::TDriver YdbDriver;
    const TPqClusterConfigsMapPtr ClusterConfigs;
    const ISecuredServiceAccountCredentialsFactory::TPtr CredentialsFactory;
    const IPqLocalClientFactory::TPtr LocalTopicClientFactory;

    TMutex Mutex;
    THashMap<TString, NPq::NConfigurationManager::IClient::TPtr> ClusterCmClients; // Cluster -> CM Client.
    THashMap<std::pair<TString, TString>, NYdb::NDataStreams::V1::TDataStreamsClient> ClusterDbDsClients; // Cluster, Database -> DS Client
    THashMap<std::pair<TString, TString>, NYdb::NTopic::TTopicClient> ClusterDbYdbPqClients; // Cluster, Database -> Topic Client.
    THashMap<std::pair<TString, TString>, NYdb::NFederatedTopic::TFederatedTopicClient> ClusterDbYdbFederatedPqClients; // Cluster, Database -> Topic Client.
};

} // namespace NYql
