#include "yql_pq_dummy_gateway.h"

#include <ydb/library/yql/providers/pq/gateway/clients/file/yql_pq_file_federated_topic_client.h>
#include <ydb/library/yql/providers/pq/gateway/clients/file/yql_pq_file_topic_client.h>

#include <util/generic/is_in.h>
#include <util/generic/yexception.h>

#include <exception>

namespace NYql {

using namespace NYdb;
using namespace NYdb::NFederatedTopic;
using namespace NYdb::NTopic;

TDummyPqGateway::TDummyPqGateway(bool skipDatabasePrefix)
    : AllowSkipDatabasePrefix(skipDatabasePrefix)
{}

TDummyPqGateway& TDummyPqGateway::AddDummyTopic(const TDummyTopic& topic) {
    with_lock (Mutex) {
        Y_ENSURE(topic.Cluster);
        Y_ENSURE(topic.TopicName);
        const auto key = std::make_pair(topic.Cluster, topic.TopicName);
        Y_ENSURE(Topics.emplace(key, topic).second, "Already inserted dummy topic {" << topic.Cluster << ", " << topic.Path << "}");
        return *this;
    }
}

NThreading::TFuture<void> TDummyPqGateway::OpenSession(const TString& sessionId, const TString& username) {
    Y_UNUSED(username);

    with_lock (Mutex) {
        Y_ENSURE(sessionId);
        Y_ENSURE(!IsIn(OpenedSessions, sessionId), "Session " << sessionId << " is already opened in pq gateway");
        OpenedSessions.insert(sessionId);
    }

    return NThreading::MakeFuture();
}

NThreading::TFuture<void> TDummyPqGateway::CloseSession(const TString& sessionId) {
    with_lock (Mutex) {
        Y_ENSURE(IsIn(OpenedSessions, sessionId), "Session " << sessionId << " is not opened in pq gateway");
        OpenedSessions.erase(sessionId);
    }

    return NThreading::MakeFuture();
}

NPq::NConfigurationManager::TAsyncDescribePathResult TDummyPqGateway::DescribePath(const TString& sessionId, const TString& cluster, const TString& database, const TString& path, const TString& token) {
    Y_UNUSED(token);

    const auto& clusterCanonized = SkipDatabasePrefix(cluster, database);
    const auto& pathCanonized = SkipDatabasePrefix(path, database);

    with_lock (Mutex) {
        Y_ENSURE(IsIn(OpenedSessions, sessionId), "Session " << sessionId << " is not opened in pq gateway");
        const auto key = std::make_pair(clusterCanonized, pathCanonized);
        if (const auto* topic = Topics.FindPtr(key)) {
            NPq::NConfigurationManager::TTopicDescription desc(path);
            desc.PartitionsCount = topic->PartitionsCount;
            return NThreading::MakeFuture<NPq::NConfigurationManager::TDescribePathResult>(
                NPq::NConfigurationManager::TDescribePathResult::Make<NPq::NConfigurationManager::TTopicDescription>(desc));
        }
        return NThreading::MakeErrorFuture<NPq::NConfigurationManager::TDescribePathResult>(
            std::make_exception_ptr(NPq::NConfigurationManager::TException{NPq::NConfigurationManager::EStatus::NOT_FOUND} << "Topic " << pathCanonized << " is not found on cluster " << clusterCanonized << " in database " << database));
    }
}

IPqGateway::TAsyncDescribeFederatedTopicResult TDummyPqGateway::DescribeFederatedTopic(const TString& sessionId, const TString& cluster, const TString& database, const TString& path, const TString& token) {
    Y_UNUSED(token);

    const auto& clusterCanonized = SkipDatabasePrefix(cluster, database);
    const auto& pathCanonized = SkipDatabasePrefix(path, database);

    with_lock (Mutex) {
        Y_ENSURE(IsIn(OpenedSessions, sessionId), "Session " << sessionId << " is not opened in pq gateway");
        const auto key = std::make_pair(clusterCanonized, pathCanonized);
        if (const auto* topic = Topics.FindPtr(key)) {
            IPqGateway::TDescribeFederatedTopicResult result;
            auto& cluster = result.emplace_back();
            cluster.PartitionsCount = topic->PartitionsCount;
            return NThreading::MakeFuture<TDescribeFederatedTopicResult>(result);
        }
        return NThreading::MakeErrorFuture<IPqGateway::TDescribeFederatedTopicResult>(
            std::make_exception_ptr(yexception() << "Topic " << pathCanonized << " is not found on cluster " << clusterCanonized << " in database " << database));
    }
}

NThreading::TFuture<IPqGateway::TListStreams> TDummyPqGateway::ListStreams(const TString& sessionId, const TString& cluster, const TString& database, const TString& token, ui32 limit, const TString& exclusiveStartStreamName) {
    Y_UNUSED(sessionId, cluster, database, token, limit, exclusiveStartStreamName);
    return NThreading::MakeFuture<IPqGateway::TListStreams>();
}

void TDummyPqGateway::UpdateClusterConfigs(const TString& clusterName, const TString& endpoint, const TString& database, bool secure) {
    Y_UNUSED(clusterName, endpoint, database, secure);
}

void TDummyPqGateway::UpdateClusterConfigs(const TPqGatewayConfigPtr& config) {
     Y_UNUSED(config);
}

void TDummyPqGateway::AddCluster(const NYql::TPqClusterConfig& cluster) {
    Y_UNUSED(cluster);
}

ITopicClient::TPtr TDummyPqGateway::GetTopicClient(const TDriver& driver, const TTopicClientSettings& settings) {
    return CreateFileTopicClient(Topics, {
        .Database = settings.Database_.value_or(driver.GetConfig().GetDatabase()),
        .SkipDatabasePrefix = AllowSkipDatabasePrefix,
    });
}

IFederatedTopicClient::TPtr TDummyPqGateway::GetFederatedTopicClient(const TDriver& driver, const TFederatedTopicClientSettings& settings) {
    return CreateFileFederatedTopicClient(Topics, settings, {
        .Database = settings.Database_.value_or(driver.GetConfig().GetDatabase()),
        .SkipDatabasePrefix = AllowSkipDatabasePrefix,
    });
}

TTopicClientSettings TDummyPqGateway::GetTopicClientSettings() const {
    return {};
}

TFederatedTopicClientSettings TDummyPqGateway::GetFederatedTopicClientSettings() const {
    return {};
}

TString TDummyPqGateway::SkipDatabasePrefix(const TString& path, const TString& database) const {
    return AllowSkipDatabasePrefix ? NYql::SkipDatabasePrefix(path, database) : path;
}

TDummyPqGateway::TPtr CreatePqFileGateway(bool skipDatabasePrefix) {
    return MakeIntrusive<TDummyPqGateway>(skipDatabasePrefix);
}

} // namespace NYql
