#pragma once

#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/persqueue.h> 

#include <ydb/library/yql/providers/common/token_accessor/client/factory.h> 
#include <ydb/library/yql/providers/common/proto/gateways_config.pb.h>
#include <ydb/library/yql/providers/pq/cm_client/interface/client.h>

#include <util/generic/ptr.h>
#include <util/system/mutex.h>

namespace NYql {

using TPqClusterConfigsMap = THashMap<TString, NYql::TPqClusterConfig>;
using TPqClusterConfigsMapPtr = std::shared_ptr<TPqClusterConfigsMap>;

class TPqSession : public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<TPqSession>;

    explicit TPqSession(const TString& sessionId,
                        const TString& username,
                        const NPq::NConfigurationManager::IConnections::TPtr& cmConnections,
                        const NYdb::TDriver& ydbDriver,
                        const TPqClusterConfigsMapPtr& clusterConfigs,
                        ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory)
        : SessionId(sessionId)
        , UserName(username)
        , CmConnections(cmConnections)
        , YdbDriver(ydbDriver)
        , ClusterConfigs(clusterConfigs)
        , CredentialsFactory(credentialsFactory)
    {
    }

    NPq::NConfigurationManager::TAsyncDescribePathResult DescribePath(const TString& cluster, const TString& database, const TString& path, const TString& token);

private:
    const NPq::NConfigurationManager::IClient::TPtr& GetConfigManagerClient(const TString& cluster, const NYql::TPqClusterConfig& cfg, std::shared_ptr<NYdb::ICredentialsProviderFactory> credentialsProviderFactory);
    NYdb::NPersQueue::TPersQueueClient& GetYdbPqClient(const TString& cluster, const TString& database, const NYql::TPqClusterConfig& cfg, std::shared_ptr<NYdb::ICredentialsProviderFactory> credentialsProviderFactory);

private:
    const TString SessionId;
    const TString UserName;
    const NPq::NConfigurationManager::IConnections::TPtr CmConnections;
    const NYdb::TDriver YdbDriver;
    const TPqClusterConfigsMapPtr ClusterConfigs;
    const ISecuredServiceAccountCredentialsFactory::TPtr CredentialsFactory;

    TMutex Mutex;
    THashMap<TString, NPq::NConfigurationManager::IClient::TPtr> ClusterCmClients; // Cluster -> CM Client.
    THashMap<TString, NYdb::NPersQueue::TPersQueueClient> ClusterYdbPqClients; // Cluster -> PQ Client.
};

} // namespace NYql
