#include "yql_pq_dummy_gateway.h"
#include "yql_pq_file_topic_client.h"

#include <util/generic/is_in.h>
#include <util/generic/yexception.h>

#include <exception>

namespace NYql {

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
    Y_UNUSED(database);
    Y_UNUSED(token);
    with_lock (Mutex) {
        Y_ENSURE(IsIn(OpenedSessions, sessionId), "Session " << sessionId << " is not opened in pq gateway");
        const auto key = std::make_pair(cluster, path);
        if (const auto* topic = Topics.FindPtr(key)) {
            NPq::NConfigurationManager::TTopicDescription desc(path);
            desc.PartitionsCount = topic->PartitionsCount;
            return NThreading::MakeFuture<NPq::NConfigurationManager::TDescribePathResult>(
                NPq::NConfigurationManager::TDescribePathResult::Make<NPq::NConfigurationManager::TTopicDescription>(desc));
        }
        return NThreading::MakeErrorFuture<NPq::NConfigurationManager::TDescribePathResult>(
            std::make_exception_ptr(NPq::NConfigurationManager::TException{NPq::NConfigurationManager::EStatus::NOT_FOUND} << "Topic " << path << " is not found on cluster " << cluster));
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

IPqGateway::TPtr CreatePqFileGateway() {
    return MakeIntrusive<TDummyPqGateway>();
}

ITopicClient::TPtr TDummyPqGateway::GetTopicClient(const NYdb::TDriver&, const NYdb::NTopic::TTopicClientSettings&) {
    return MakeIntrusive<TFileTopicClient>(Topics);
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
