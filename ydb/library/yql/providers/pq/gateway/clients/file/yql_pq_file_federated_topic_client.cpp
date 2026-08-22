#include "yql_pq_file_federated_topic_client.h"
#include "yql_pq_file_topic_client.h"

#include <util/generic/hash.h>

namespace NYql {

namespace {

using namespace NYdb::NFederatedTopic;
using namespace NYdb::NTopic;

class TDummyFederatedTopicClient final : public IFederatedTopicClient {
public:
    TDummyFederatedTopicClient(const THashMap<TClusterNPath, TDummyTopic>& topics, const TFederatedTopicClientSettings& settings, const TFileTopicClientSettings& fileClientSettings)
        : Topics(topics)
        , FederatedClientSettings(settings)
        , FileClientSettings(fileClientSettings)
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
            TopicClient = CreateFileTopicClient(Topics, FileClientSettings);
        }

        return TopicClient->CreateWriteSession(settings);
    }

private:
    const THashMap<TClusterNPath, TDummyTopic> Topics;
    const TFederatedTopicClientSettings FederatedClientSettings;
    const TFileTopicClientSettings FileClientSettings;
    ITopicClient::TPtr TopicClient;
};

} // anonymous namespace

IFederatedTopicClient::TPtr CreateFileFederatedTopicClient(const THashMap<TClusterNPath, TDummyTopic>& topics, const TFederatedTopicClientSettings& settings, const TFileTopicClientSettings& fileClientSettings) {
    return MakeIntrusive<TDummyFederatedTopicClient>(topics, settings, fileClientSettings);
}

} // namespace NYql
