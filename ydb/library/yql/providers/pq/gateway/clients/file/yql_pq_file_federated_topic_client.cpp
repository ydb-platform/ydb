#include "yql_pq_file_federated_topic_client.h"
#include "yql_pq_file_topic_client.h"

#include <util/generic/hash.h>

namespace NYql {

namespace {

using namespace NYdb::NFederatedTopic;
using namespace NYdb::NTopic;

class TDummyFederatedTopicClient final : public IFederatedTopicClient {
public:
    TDummyFederatedTopicClient(const THashMap<TClusterNPath, TDummyTopic>& topics, const TFederatedTopicClientSettings& settings)
        : Topics(topics)
        , FederatedClientSettings(settings)
    {}

    NThreading::TFuture<std::vector<TFederatedTopicClient::TClusterInfo>> GetAllTopicClusters() final {
        return NThreading::MakeFuture(std::vector(1, TFederatedTopicClient::TClusterInfo{
            "",
            FederatedClientSettings.DiscoveryEndpoint_.value_or(""),
            FederatedClientSettings.Database_.value_or(""),
            TFederatedTopicClient::TClusterInfo::EStatus::AVAILABLE
        }));
    }

    std::shared_ptr<IWriteSession> CreateWriteSession(const TFederatedWriteSessionSettings& settings) final {
        if (!TopicClient) {
            TopicClient = CreateFileTopicClient(Topics);
        }

        return TopicClient->CreateWriteSession(settings);
    }

private:
    const THashMap<TClusterNPath, TDummyTopic> Topics;
    const TFederatedTopicClientSettings FederatedClientSettings;
    ITopicClient::TPtr TopicClient;
};

} // anonymous namespace

IFederatedTopicClient::TPtr CreateFileFederatedTopicClient(const THashMap<TClusterNPath, TDummyTopic>& topics, const TFederatedTopicClientSettings& settings) {
    return MakeIntrusive<TDummyFederatedTopicClient>(topics, settings);
}

} // namespace NYql
