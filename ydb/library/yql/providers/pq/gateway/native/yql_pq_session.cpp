#include "yql_pq_session.h"

#include <ydb/library/yverify_stream/yverify_stream.h>

#include <yql/essentials/utils/yql_panic.h>
#include <yql/essentials/providers/common/proto/gateways_config.pb.h>

#include <library/cpp/threading/future/wait/wait.h>

namespace NYql {

using namespace NYdb;
using namespace NYdb::NTopic;
using namespace NYdb::NFederatedTopic;

namespace {

NPq::NConfigurationManager::TClientOptions GetCmClientOptions(const TPqClusterConfig& cfg, std::shared_ptr<ICredentialsProviderFactory> credentialsProviderFactory) {
    NPq::NConfigurationManager::TClientOptions opts;
    opts
        .SetEndpoint(cfg.GetConfigManagerEndpoint())
        .SetCredentialsProviderFactory(credentialsProviderFactory)
        .SetEnableSsl(cfg.GetUseSsl());

    return opts;
}

TFederatedTopicClientSettings GetYdbFederatedPqClientOptions(const TString& database, const TPqClusterConfig& cfg, std::shared_ptr<ICredentialsProviderFactory> credentialsProviderFactory) {
    TFederatedTopicClientSettings opts;
    opts
        .DiscoveryEndpoint(cfg.GetEndpoint())
        .Database(database)
        .SslCredentials(TSslCredentials(cfg.GetUseSsl()))
        .CredentialsProviderFactory(credentialsProviderFactory);

    return opts;
}

TTopicClientSettings GetYdbPqClientOptions(const TString& database, const TPqClusterConfig& cfg, std::shared_ptr<ICredentialsProviderFactory> credentialsProviderFactory) {
    TTopicClientSettings opts;
    opts
        .DiscoveryEndpoint(cfg.GetEndpoint())
        .Database(database)
        .SslCredentials(TSslCredentials(cfg.GetUseSsl()))
        .CredentialsProviderFactory(credentialsProviderFactory);

    return opts;
}

TCommonClientSettings GetDsClientOptions(const TString& database, const TPqClusterConfig& cfg, std::shared_ptr<ICredentialsProviderFactory> credentialsProviderFactory) {
    TCommonClientSettings opts;
    opts
        .DiscoveryEndpoint(cfg.GetEndpoint())
        .Database(database)
        .SslCredentials(TSslCredentials(cfg.GetUseSsl()))
        .CredentialsProviderFactory(credentialsProviderFactory);

    return opts;
}

} // anonymous namespace

TPqSession::TPqSession(const TString& sessionId, const TString& username, const NPq::NConfigurationManager::IConnections::TPtr& cmConnections,
    const TDriver& ydbDriver, const TPqClusterConfigsMapPtr& clusterConfigs, ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
    IPqLocalClientFactory::TPtr localTopicClientFactory)
    : SessionId(sessionId)
    , UserName(username)
    , CmConnections(cmConnections)
    , YdbDriver(ydbDriver)
    , ClusterConfigs(clusterConfigs)
    , CredentialsFactory(credentialsFactory)
    , LocalTopicClientFactory(std::move(localTopicClientFactory))
{}

NPq::NConfigurationManager::TAsyncDescribePathResult TPqSession::DescribePath(const TString& cluster, const TString& database, const TString& path, const TString& token) {
    const auto* config = ClusterConfigs->FindPtr(cluster);
    if (!config) {
        ythrow yexception() << "Pq cluster `" << cluster << "` does not exist";
    }

    YQL_ENSURE(config->GetEndpoint(), "Can't describe topic `" << cluster << "`.`" << path << "`: no endpoint");

    std::shared_ptr<ICredentialsProviderFactory> credentialsProviderFactory = CreateCredentialsProviderFactoryForStructuredToken(CredentialsFactory, token, config->GetAddBearerToToken());
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

        return GetYdbPqClient(cluster, database, *config, credentialsProviderFactory).DescribeTopic(path).Apply([cluster, path, database](const TAsyncDescribeTopicResult& describeTopicResultFuture) {
            const TDescribeTopicResult& describeTopicResult = describeTopicResultFuture.GetValue();
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

    std::shared_ptr<ICredentialsProviderFactory> credentialsProviderFactory = CreateCredentialsProviderFactoryForStructuredToken(CredentialsFactory, token, config->GetAddBearerToToken());
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
            .ListStreams(NDataStreams::V1::TListStreamsSettings{ .Limit_ = limit, .ExclusiveStartStreamName_ = exclusiveStartStreamName})
            .Apply([](const auto& future) {
                auto& response = future.GetValue();
                if (!response.IsSuccess()) {
                    throw yexception() << response.GetIssues().ToString();
                }
                const auto& result = response.GetResult();
                IPqGateway::TListStreams listStreams;
                listStreams.Names.insert(listStreams.Names.end(), result.stream_names().begin(), result.stream_names().end());
                return listStreams;
            });
    }
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

    std::shared_ptr<ICredentialsProviderFactory> credentialsProviderFactory = CreateCredentialsProviderFactoryForStructuredToken(CredentialsFactory, token, config->GetAddBearerToToken());
    if (!config->GetEndpoint() && LocalTopicClientFactory) {
        return LocalTopicClientFactory->CreateTopicClient(GetYdbPqClientOptions(database, *config, credentialsProviderFactory))->DescribeTopic(path)
            .Apply([path](const TAsyncDescribeTopicResult& f) {
                IPqGateway::TClusterInfo info = {.Info = {.Status = TFederatedTopicClient::TClusterInfo::EStatus::AVAILABLE}};

                TString error;
                const auto setError = [&error, path](const TString& msg) {
                    error = TStringBuilder() << "Failed to describe local topic `" << path << "`: " << msg;
                };

                try {
                    const auto& response = f.GetValue();
                    if (!response.IsSuccess()) {
                        setError(response.GetIssues().ToString());
                    }
                    info.PartitionsCount = response.GetTopicDescription().GetTotalPartitionsCount();
                } catch (...) {
                    setError(FormatCurrentException());
                }

                if (error) {
                    throw yexception() << error;
                }

                return std::vector<IPqGateway::TClusterInfo>{{std::move(info)}};
            });
    }
    YQL_ENSURE(config->GetEndpoint(), "Can't describe topic `" << cluster << "`.`" << path << "`: no endpoint, and local topics are not enabled");

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
                std::vector<TAsyncDescribeTopicResult> futures;
                IPqGateway::TDescribeFederatedTopicResult results;
                results.reserve(allClustersInfo.size());
                futures.reserve(allClustersInfo.size());
                std::vector<std::string> paths;
                paths.reserve(allClustersInfo.size());
                for (auto& clusterInfo : allClustersInfo) {
                    auto& clusterTopicPath = paths.emplace_back(path);
                    clusterInfo.AdjustTopicPath(clusterTopicPath);
                    if (!clusterInfo.IsAvailableForRead()) {
                        futures.emplace_back(NThreading::MakeErrorFuture<TDescribeTopicResult>(std::make_exception_ptr(NThreading::TFutureException() << "Cluster " << clusterInfo.Name << " is unavailable for read")));
                    } else {
                        clusterInfo.AdjustTopicClientSettings(topicSettings);
                        futures.emplace_back(TTopicClient(ydbDriver, topicSettings).DescribeTopic(clusterTopicPath));
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

const NPq::NConfigurationManager::IClient::TPtr& TPqSession::GetConfigManagerClient(const TString& cluster, const TPqClusterConfig& cfg, std::shared_ptr<ICredentialsProviderFactory> credentialsProviderFactory) {
    auto& client = ClusterCmClients[cluster];
    if (!client && CmConnections) {
        Y_VALIDATE(cfg.GetEndpoint(), "Can't get config manager client for cluster `" << cluster << "`: no endpoint");
        client = CmConnections->GetClient(GetCmClientOptions(cfg, credentialsProviderFactory));
    }
    return client;
}

NDataStreams::V1::TDataStreamsClient& TPqSession::GetDsClient(const TString& cluster, const TString& database, const TPqClusterConfig& cfg, std::shared_ptr<ICredentialsProviderFactory> credentialsProviderFactory) {
    const auto clientIt = ClusterDsClients.find(cluster);
    if (clientIt != ClusterDsClients.end()) {
        return clientIt->second;
    }
    Y_VALIDATE(cfg.GetEndpoint(), "Can't get data streams client for cluster `" << cluster << "`: no endpoint");
    return ClusterDsClients.emplace(cluster, NDataStreams::V1::TDataStreamsClient(YdbDriver, GetDsClientOptions(database, cfg, credentialsProviderFactory))).first->second;
}

TFederatedTopicClient& TPqSession::GetYdbFederatedPqClient(const TString& cluster, const TString& database, const TPqClusterConfig& cfg, std::shared_ptr<ICredentialsProviderFactory> credentialsProviderFactory) {
    const auto clientIt = ClusterYdbFederatedPqClients.find(cluster);
    if (clientIt != ClusterYdbFederatedPqClients.end()) {
        return clientIt->second;
    }
    Y_VALIDATE(cfg.GetEndpoint(), "Can't get federated topic client for cluster `" << cluster << "`: no endpoint");
    return ClusterYdbFederatedPqClients.emplace(cluster, TFederatedTopicClient(YdbDriver, GetYdbFederatedPqClientOptions(database, cfg, credentialsProviderFactory))).first->second;
}

TTopicClient& TPqSession::GetYdbPqClient(const TString& cluster, const TString& database, const TPqClusterConfig& cfg, std::shared_ptr<ICredentialsProviderFactory> credentialsProviderFactory) {
    const auto clientIt = ClusterYdbPqClients.find(cluster);
    if (clientIt != ClusterYdbPqClients.end()) {
        return clientIt->second;
    }
    Y_VALIDATE(cfg.GetEndpoint(), "Can't get topic client for cluster `" << cluster << "`: no endpoint");
    return ClusterYdbPqClients.emplace(cluster, TTopicClient(YdbDriver, GetYdbPqClientOptions(database, cfg, credentialsProviderFactory))).first->second;
}

} // namespace NYql
