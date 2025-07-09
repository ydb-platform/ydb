#include "yql_pq_session.h"

#include <yql/essentials/utils/yql_panic.h>

#include <library/cpp/threading/future/wait/wait.h>

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

NYdb::NFederatedTopic::TFederatedTopicClientSettings GetYdbFederatedPqClientOptions(const TString& database, const NYql::TPqClusterConfig& cfg, std::shared_ptr<NYdb::ICredentialsProviderFactory> credentialsProviderFactory) {
    NYdb::NFederatedTopic::TFederatedTopicClientSettings opts;
    opts
        .DiscoveryEndpoint(cfg.GetEndpoint())
        .Database(database)
        .SslCredentials(NYdb::TSslCredentials(cfg.GetUseSsl()))
        .CredentialsProviderFactory(credentialsProviderFactory);

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

NYdb::NFederatedTopic::TFederatedTopicClient& TPqSession::GetYdbFederatedPqClient(const TString& cluster, const TString& database, const NYql::TPqClusterConfig& cfg, std::shared_ptr<NYdb::ICredentialsProviderFactory> credentialsProviderFactory) {
    const auto clientIt = ClusterYdbFederatedPqClients.find(cluster);
    if (clientIt != ClusterYdbFederatedPqClients.end()) {
        return clientIt->second;
    }
    return ClusterYdbFederatedPqClients.emplace(cluster, NYdb::NFederatedTopic::TFederatedTopicClient(YdbDriver, GetYdbFederatedPqClientOptions(database, cfg, credentialsProviderFactory))).first->second;
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

IPqGateway::TAsyncDescribeFederatedTopicResult TPqSession::DescribeFederatedTopic(const TString& cluster, const TString& requestedDatabase, const TString& requestedPath, const TString& token) {
    const auto* config = ClusterConfigs->FindPtr(cluster);
    if (!config) {
        ythrow yexception() << "Pq cluster `" << cluster << "` does not exist";
    }

    TString database = requestedDatabase;
    TString path = requestedPath;
    if (config->GetClusterType() == TPqClusterConfig::CT_PERS_QUEUE && requestedDatabase == "/Root") {
        // RTMR compatibility
        // It uses cluster specified in gateways.conf with ClusterType unset (CT_PERS_QUEUE by default) and default Database and its own read/write code
        auto pos = requestedPath.find('/');
        Y_ENSURE(pos != TString::npos, "topic name is expected in format <account>/<path>");
        database = "/logbroker-federation/" + requestedPath.substr(0, pos);
        path = requestedPath.substr(pos + 1);
    }
    YQL_ENSURE(config->GetEndpoint(), "Can't describe topic `" << cluster << "`.`" << path << "`: no endpoint");

    std::shared_ptr<NYdb::ICredentialsProviderFactory> credentialsProviderFactory = CreateCredentialsProviderFactoryForStructuredToken(CredentialsFactory, token, config->GetAddBearerToToken());
    with_lock (Mutex) {
        return GetYdbFederatedPqClient(cluster, database, *config, credentialsProviderFactory)
            .GetAllClusterInfo()
            .Apply([
                ydbDriver = YdbDriver, credentialsProviderFactory,
                cluster, database, path,
                topicSettings = GetYdbPqClientOptions(database, *config, credentialsProviderFactory)
            ](const auto& futureClusterInfo) mutable {
                auto allClustersInfo = futureClusterInfo.GetValue();
                Y_ENSURE(!allClustersInfo.empty());
                std::vector<NYdb::NTopic::TAsyncDescribeTopicResult> futures;
                IPqGateway::TDescribeFederatedTopicResult results;
                results.reserve(allClustersInfo.size());
                futures.reserve(allClustersInfo.size());
                std::vector<std::string> paths;
                paths.reserve(allClustersInfo.size());
                for (auto& clusterInfo: allClustersInfo) {
                    auto& clusterTopicPath = paths.emplace_back(path);
                    clusterInfo.AdjustTopicPath(clusterTopicPath);
                    if (!clusterInfo.IsAvailableForRead()) {
                        futures.emplace_back(NThreading::MakeErrorFuture<NYdb::NTopic::TDescribeTopicResult>(std::make_exception_ptr(NThreading::TFutureException() << "Cluster " << clusterInfo.Name << " is unavailable for read")));
                    } else {
                        clusterInfo.AdjustTopicClientSettings(topicSettings);
                        futures.emplace_back(NYdb::NTopic::TTopicClient(ydbDriver, topicSettings).DescribeTopic(clusterTopicPath));
                    }
                    results.emplace_back(std::move(clusterInfo));
                }
                Y_ENSURE(results.size() == allClustersInfo.size());
                Y_ENSURE(paths.size() == allClustersInfo.size());
                Y_ENSURE(futures.size() == allClustersInfo.size());
                // XXX This produces circular dependency until the future is fired
                // futures references allFutureDescribe
                // lambda references futures[]
                // allFutureDescribe contains lambda
                auto allFutureDescribes = NThreading::WaitAll(futures);
                return allFutureDescribes.Apply([futures = std::move(futures), paths = std::move(paths), results = std::move(results), cluster, database, path](const auto& ) mutable {
                    TStringBuilder ex;
                    auto addErrorHeader = [&]() {
                        if (ex.empty()) {
                            ex << "Failed to describe topic `" << cluster << "`.`" << path << "` in the database `" << database << "`: ";
                        } else {
                            ex << "; ";
                        }
                    };
                    bool gotAnyTopic = false;
                    for (size_t i = 0; i != results.size(); ++i) {
                        auto& futureDescribe = futures[i];
                        auto addErrorCluster = [&]() {
                            addErrorHeader();
                            ex << "#" << i << " (name '" << results[i].Info.Name << "' endpoint '" << results[i].Info.Endpoint << "' path `" << paths[i] << "`): ";
                        };
                        try {
                            auto describeTopicResult = futureDescribe.ExtractValue();
                            if (!describeTopicResult.IsSuccess()) {
                                addErrorCluster();
                                ex << describeTopicResult.GetIssues().ToString();
                                continue;
                            }
                            results[i].PartitionsCount = describeTopicResult.GetTopicDescription().GetTotalPartitionsCount();
                            gotAnyTopic = true;
                        } catch (...) {
                            addErrorCluster();
                            ex << "Got exception: " << FormatCurrentException();
                        }
                    }
                    if (!gotAnyTopic) {
                        addErrorHeader();
                        ex << "No working cluster found\n";
                        throw yexception() << ex;
                    }
                    return results;
              });
         });
     }
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

        return GetYdbPqClient(cluster, database, *config, credentialsProviderFactory).DescribeTopic(path).Apply([cluster, path, database](const NYdb::NTopic::TAsyncDescribeTopicResult& describeTopicResultFuture) {
            const NYdb::NTopic::TDescribeTopicResult& describeTopicResult = describeTopicResultFuture.GetValue();
            if (!describeTopicResult.IsSuccess()) {
                throw yexception() << "Failed to describe topic `" << cluster << "`.`" << path << "` in the database `" << database << "`: " << describeTopicResult.GetIssues().ToString();
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
