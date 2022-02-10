#include "yql_pq_session.h"

#include <ydb/library/yql/utils/yql_panic.h>

namespace NYql {

namespace {
NPq::NConfigurationManager::TClientOptions GetCmClientOptions(const NYql::TPqClusterConfig& cfg, std::shared_ptr<NYdb::ICredentialsProviderFactory> credentialsProviderFactory) {
    NPq::NConfigurationManager::TClientOptions opts;
    opts
        .SetEndpoint(cfg.GetConfigManagerEndpoint())
        .SetCredentialsProviderFactory(credentialsProviderFactory) 
        .SetEnableSsl(cfg.GetUseSsl()); 

    return opts;
}

NYdb::NPersQueue::TPersQueueClientSettings GetYdbPqClientOptions(const TString& database, const NYql::TPqClusterConfig& cfg, std::shared_ptr<NYdb::ICredentialsProviderFactory> credentialsProviderFactory) {
    NYdb::NPersQueue::TPersQueueClientSettings opts;
    opts
        .DiscoveryEndpoint(cfg.GetEndpoint())
        .Database(database) 
        .EnableSsl(cfg.GetUseSsl())
        .CredentialsProviderFactory(credentialsProviderFactory);

    return opts;
}
}

const NPq::NConfigurationManager::IClient::TPtr& TPqSession::GetConfigManagerClient(const TString& cluster, const NYql::TPqClusterConfig& cfg, std::shared_ptr<NYdb::ICredentialsProviderFactory> credentialsProviderFactory) {
    auto& client = ClusterCmClients[cluster];
    if (!client && CmConnections) {
        client = CmConnections->GetClient(GetCmClientOptions(cfg, credentialsProviderFactory));
    }
    return client;
}

NYdb::NPersQueue::TPersQueueClient& TPqSession::GetYdbPqClient(const TString& cluster, const TString& database, const NYql::TPqClusterConfig& cfg, std::shared_ptr<NYdb::ICredentialsProviderFactory> credentialsProviderFactory) {
    const auto clientIt = ClusterYdbPqClients.find(cluster);
    if (clientIt != ClusterYdbPqClients.end()) {
        return clientIt->second;
    }
    return ClusterYdbPqClients.emplace(cluster, NYdb::NPersQueue::TPersQueueClient(YdbDriver, GetYdbPqClientOptions(database, cfg, credentialsProviderFactory))).first->second;
}

NPq::NConfigurationManager::TAsyncDescribePathResult TPqSession::DescribePath(const TString& cluster, const TString& database, const TString& path, const TString& token) {
    const auto* config = ClusterConfigs->FindPtr(cluster);
    if (!config) {
        ythrow yexception() << "Pq cluster `" << cluster << "` does not exist";
    }

    YQL_ENSURE(config->GetEndpoint(), "Can't describe topic `" << cluster << "`.`" << path << "`: no endpoint");

    std::shared_ptr<NYdb::ICredentialsProviderFactory> credentialsProviderFactory = CreateCredentialsProviderFactoryForStructuredToken(CredentialsFactory, token, config->GetAddBearerToToken());
    with_lock (Mutex) {
        if (config->GetClusterType() == TPqClusterConfig::CT_PERS_QUEUE) {
            const NPq::NConfigurationManager::IClient::TPtr& client = GetConfigManagerClient(cluster, *config, credentialsProviderFactory);
            if (!client) {
                NThreading::TPromise<::NPq::NConfigurationManager::TDescribePathResult> result = NThreading::NewPromise<::NPq::NConfigurationManager::TDescribePathResult>();
                result.SetException(
                    std::make_exception_ptr(
                        NPq::NConfigurationManager::TException(NPq::NConfigurationManager::EStatus::INTERNAL_ERROR)
                            << "Pq configuration manager is not supported"));
                return result;
            }
            return client->DescribePath(path);
        }

        return GetYdbPqClient(cluster, database, *config, credentialsProviderFactory).DescribeTopic(path).Apply([cluster, path](const NYdb::NPersQueue::TAsyncDescribeTopicResult& describeTopicResultFuture) {
            const NYdb::NPersQueue::TDescribeTopicResult& describeTopicResult = describeTopicResultFuture.GetValue();
            if (!describeTopicResult.IsSuccess()) {
                throw yexception() << "Failed to describe topic `" << cluster << "`.`" << path << "`: " << describeTopicResult.GetIssues().ToString();
            }
            NPq::NConfigurationManager::TTopicDescription desc(path);
            desc.PartitionsCount = describeTopicResult.TopicSettings().PartitionsCount();
            return NPq::NConfigurationManager::TDescribePathResult::Make<NPq::NConfigurationManager::TTopicDescription>(std::move(desc));
        });
    }
}

} // namespace NYql
