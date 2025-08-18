#include "yql_pq_dummy_gateway.h"
#include "yql_pq_file_topic_client.h"

#include <util/generic/is_in.h>
#include <util/generic/yexception.h>

#include <exception>

namespace NYql {

struct TDummyFederatedTopicClient : public IFederatedTopicClient {
    using TClusterNPath = TDummyPqGateway::TClusterNPath;
    TDummyFederatedTopicClient(const NYdb::NTopic::TFederatedTopicClientSettings& settings = {}, const THashMap<TClusterNPath, TDummyTopic>& topics = {})
        : Topics_(topics)
        , FederatedClientSettings_(settings) {}

    NThreading::TFuture<std::vector<NYdb::NFederatedTopic::TFederatedTopicClient::TClusterInfo>> GetAllTopicClusters() override {
        std::vector<NYdb::NFederatedTopic::TFederatedTopicClient::TClusterInfo> dbInfo;
        dbInfo.emplace_back(
                "",
                FederatedClientSettings_.DiscoveryEndpoint_.value_or(""),
                FederatedClientSettings_.Database_.value_or(""),
                NYdb::NFederatedTopic::TFederatedTopicClient::TClusterInfo::EStatus::AVAILABLE);
        return NThreading::MakeFuture(std::move(dbInfo));
    }

    std::shared_ptr<NYdb::NTopic::IWriteSession> CreateWriteSession(const NYdb::NFederatedTopic::TFederatedWriteSessionSettings& settings) override {
        if (!FileTopicClient_) {
            FileTopicClient_ = MakeIntrusive<TFileTopicClient>(std::move(Topics_));
        }
        return FileTopicClient_->CreateWriteSession(settings);
    }
private:
    THashMap<TClusterNPath, TDummyTopic> Topics_;
    NYdb::NFederatedTopic::TFederatedTopicClientSettings FederatedClientSettings_;
    TFileTopicClient::TPtr FileTopicClient_;
};

TDummyPqGateway::TDummyPqGateway(bool skipDatabasePrefix)
    : SkipDatabasePrefix(skipDatabasePrefix)
{}

NThreading::TFuture<void> TDummyPqGateway::OpenSession(const TString& sessionId, const TString& username) {
    with_lock (Mutex) {
        Y_ENSURE(sessionId);
        Y_UNUSED(username);

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

    const auto& clusterCanonized = CanonizeCluster(cluster, database);

    with_lock (Mutex) {
        Y_ENSURE(IsIn(OpenedSessions, sessionId), "Session " << sessionId << " is not opened in pq gateway");
        const auto key = std::make_pair(clusterCanonized, path);
        if (const auto* topic = Topics.FindPtr(key)) {
            NPq::NConfigurationManager::TTopicDescription desc(path);
            desc.PartitionsCount = topic->PartitionsCount;
            return NThreading::MakeFuture<NPq::NConfigurationManager::TDescribePathResult>(
                NPq::NConfigurationManager::TDescribePathResult::Make<NPq::NConfigurationManager::TTopicDescription>(desc));
        }
        return NThreading::MakeErrorFuture<NPq::NConfigurationManager::TDescribePathResult>(
            std::make_exception_ptr(NPq::NConfigurationManager::TException{NPq::NConfigurationManager::EStatus::NOT_FOUND} << "Topic " << path << " is not found on cluster " << clusterCanonized << " in database " << database));
    }
}

IPqGateway::TAsyncDescribeFederatedTopicResult TDummyPqGateway::DescribeFederatedTopic(const TString& sessionId, const TString& cluster, const TString& database, const TString& path, const TString& token) {
    Y_UNUSED(token);

    const auto& clusterCanonized = CanonizeCluster(cluster, database);

    with_lock (Mutex) {
        Y_ENSURE(IsIn(OpenedSessions, sessionId), "Session " << sessionId << " is not opened in pq gateway");
        const auto key = std::make_pair(clusterCanonized, path);
        if (const auto* topic = Topics.FindPtr(key)) {
            IPqGateway::TDescribeFederatedTopicResult result;
            auto& cluster = result.emplace_back();
            cluster.PartitionsCount = topic->PartitionsCount;
            return NThreading::MakeFuture<TDescribeFederatedTopicResult>(result);
        }
        return NThreading::MakeErrorFuture<IPqGateway::TDescribeFederatedTopicResult>(
            std::make_exception_ptr(yexception() << "Topic " << path << " is not found on cluster " << clusterCanonized << " in database " << database));
    }
}

NThreading::TFuture<IPqGateway::TListStreams> TDummyPqGateway::ListStreams(const TString& sessionId, const TString& cluster, const TString& database, const TString& token, ui32 limit, const TString& exclusiveStartStreamName) {
    Y_UNUSED(sessionId, cluster, database, token, limit, exclusiveStartStreamName);
    return NThreading::MakeFuture<IPqGateway::TListStreams>();
}

TDummyPqGateway& TDummyPqGateway::AddDummyTopic(const TDummyTopic& topic) {
    with_lock (Mutex) {
        Y_ENSURE(topic.Cluster);
        Y_ENSURE(topic.TopicName);
        const auto key = std::make_pair(topic.Cluster, topic.TopicName);
        Y_ENSURE(Topics.emplace(key, topic).second, "Already inserted dummy topic {" << topic.Cluster << ", " << topic.Path << "}");
        return *this;
    }
}

IPqGateway::TPtr CreatePqFileGateway(bool skipDatabasePrefix) {
    return MakeIntrusive<TDummyPqGateway>(skipDatabasePrefix);
}

ITopicClient::TPtr TDummyPqGateway::GetTopicClient(const NYdb::TDriver&, const NYdb::NTopic::TTopicClientSettings&) {
    return MakeIntrusive<TFileTopicClient>(Topics);
}

IFederatedTopicClient::TPtr TDummyPqGateway::GetFederatedTopicClient(const NYdb::TDriver&, const NYdb::NFederatedTopic::TFederatedTopicClientSettings& settings) {
    return MakeIntrusive<TDummyFederatedTopicClient>(settings, Topics);
}
NYdb::NFederatedTopic::TFederatedTopicClientSettings TDummyPqGateway::GetFederatedTopicClientSettings() const {
    return {};
}

void TDummyPqGateway::UpdateClusterConfigs(
    const TString& clusterName,
    const TString& endpoint,
    const TString& database,
    bool secure)
{
    Y_UNUSED(clusterName);
    Y_UNUSED(endpoint);
    Y_UNUSED(database);
    Y_UNUSED(secure);
}

void TDummyPqGateway::UpdateClusterConfigs(const TPqGatewayConfigPtr& config) {
     Y_UNUSED(config);
}

NYdb::NTopic::TTopicClientSettings TDummyPqGateway::GetTopicClientSettings() const {
    return NYdb::NTopic::TTopicClientSettings();
}

TString TDummyPqGateway::CanonizeCluster(const TString& cluster, const TString& database) const {
    TStringBuf clusterCanonized(cluster);

    if (SkipDatabasePrefix) {
        clusterCanonized.SkipPrefix(database);
        clusterCanonized.SkipPrefix("/");
    }

    return TString(clusterCanonized);
}

class TPqFileGatewayFactory : public IPqGatewayFactory {
public:
    TPqFileGatewayFactory(const TDummyPqGateway::TPtr pqFileGateway)
        : PqFileGateway(pqFileGateway) {}

    IPqGateway::TPtr CreatePqGateway() override {
        return PqFileGateway;
    }
private:
    const TDummyPqGateway::TPtr PqFileGateway;
};

IPqGatewayFactory::TPtr CreatePqFileGatewayFactory(const TDummyPqGateway::TPtr pqFileGateway) {
    return MakeIntrusive<TPqFileGatewayFactory>(pqFileGateway);
}

} // namespace NYql
