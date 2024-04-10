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

NYdb::NTopic::TTopicClientSettings GetYdbPqClientOptions(const TString& database, const NYql::TPqClusterConfig& cfg, std::shared_ptr<NYdb::ICredentialsProviderFactory> credentialsProviderFactory) {
    NYdb::NTopic::TTopicClientSettings opts;
    opts
        .DiscoveryEndpoint(cfg.GetEndpoint())
        .Database(database)
        .SslCredentials(NYdb::TSslCredentials(cfg.GetUseSsl()))
        .CredentialsProviderFactory(credentialsProviderFactory);

    return opts;
}

NYdb::TCommonClientSettings GetDsClientOptions(const TString& database, const NYql::TPqClusterConfig& cfg, std::shared_ptr<NYdb::ICredentialsProviderFactory> credentialsProviderFactory) {
    NYdb::TCommonClientSettings opts;
    opts
        .DiscoveryEndpoint(cfg.GetEndpoint())
        .Database(database)
        .SslCredentials(NYdb::TSslCredentials(cfg.GetUseSsl()))
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

NYdb::NTopic::TTopicClient& TPqSession::GetYdbPqClient(const TString& cluster, const TString& database, const NYql::TPqClusterConfig& cfg, std::shared_ptr<NYdb::ICredentialsProviderFactory> credentialsProviderFactory) {
    const auto clientIt = ClusterYdbPqClients.find(cluster);
    if (clientIt != ClusterYdbPqClients.end()) {
        return clientIt->second;
    }
    return ClusterYdbPqClients.emplace(cluster, NYdb::NTopic::TTopicClient(YdbDriver, GetYdbPqClientOptions(database, cfg, credentialsProviderFactory))).first->second;
}

NYdb::NDataStreams::V1::TDataStreamsClient& TPqSession::GetDsClient(const TString& cluster, const TString& database, const NYql::TPqClusterConfig& cfg, std::shared_ptr<NYdb::ICredentialsProviderFactory> credentialsProviderFactory) {
    const auto clientIt = ClusterDsClients.find(cluster);
    if (clientIt != ClusterDsClients.end()) {
        return clientIt->second;
    }
    return ClusterDsClients.emplace(cluster, NYdb::NDataStreams::V1::TDataStreamsClient(YdbDriver, GetDsClientOptions(database, cfg, credentialsProviderFactory))).first->second;
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

        return GetYdbPqClient(cluster, database, *config, credentialsProviderFactory).DescribeTopic(path).Apply([cluster, path](const NYdb::NTopic::TAsyncDescribeTopicResult& describeTopicResultFuture) {
            const NYdb::NTopic::TDescribeTopicResult& describeTopicResult = describeTopicResultFuture.GetValue();
            if (!describeTopicResult.IsSuccess()) {
                throw yexception() << "Failed to describe topic `" << cluster << "`.`" << path << "`: " << describeTopicResult.GetIssues().ToString();
            }
            NPq::NConfigurationManager::TTopicDescription desc(path);
            desc.PartitionsCount = describeTopicResult.GetTopicDescription().GetTotalPartitionsCount();
            return NPq::NConfigurationManager::TDescribePathResult::Make<NPq::NConfigurationManager::TTopicDescription>(std::move(desc));
        });
    }
}

NThreading::TFuture<IPqGateway::TListStreams> TPqSession::ListStreams(const TString& cluster, const TString& database, const TString& token, ui32 limit, const TString& exclusiveStartStreamName) {
    const auto* config = ClusterConfigs->FindPtr(cluster);
    if (!config) {
        ythrow yexception() << "Pq cluster `" << cluster << "` does not exist";
    }

    YQL_ENSURE(config->GetEndpoint(), "Can't get list topics for " << cluster << ": no endpoint");

    std::shared_ptr<NYdb::ICredentialsProviderFactory> credentialsProviderFactory = CreateCredentialsProviderFactoryForStructuredToken(CredentialsFactory, token, config->GetAddBearerToToken());
    with_lock (Mutex) {
        if (config->GetClusterType() == TPqClusterConfig::CT_PERS_QUEUE) {
            const NPq::NConfigurationManager::IClient::TPtr& client = GetConfigManagerClient(cluster, *config, credentialsProviderFactory);
            if (!client) {
                NThreading::TPromise<IPqGateway::TListStreams> result = NThreading::NewPromise<IPqGateway::TListStreams>();
                result.SetException(
                    std::make_exception_ptr(
                        yexception()
                            << "Pq configuration manager is not supported"));
                return result;
            }
            return client->DescribePath("/").Apply([](const auto& future) {
                auto response = future.GetValue();
                if (!response.IsPath()) {
                    throw yexception() << "response does not contain object of type path";
                }
                return IPqGateway::TListStreams{};
            });
        }

        return GetDsClient(cluster, database, *config, credentialsProviderFactory)
                .ListStreams(NYdb::NDataStreams::V1::TListStreamsSettings{ .Limit_ = limit, .ExclusiveStartStreamName_ = exclusiveStartStreamName})
                .Apply([](const auto& future) {
                    auto& response = future.GetValue();
                    if (!response.IsSuccess()) {
                        throw yexception() << response.GetIssues().ToString();
                    }
                    const auto& result = response.GetResult();
                    IPqGateway::TListStreams listStrems;
                    listStrems.Names.insert(listStrems.Names.end(), result.stream_names().begin(), result.stream_names().end());
                    return listStrems;
                });
    }
}

} // namespace NYql
