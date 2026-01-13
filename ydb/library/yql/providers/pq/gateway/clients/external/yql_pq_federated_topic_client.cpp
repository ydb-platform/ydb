#include "yql_pq_federated_topic_client.h"

namespace NYql {

namespace {

using namespace NYdb;
using namespace NYdb::NFederatedTopic;
using namespace NYdb::NTopic;

class TNativeFederatedTopicClient final : public IFederatedTopicClient {
public:
    TNativeFederatedTopicClient(const TDriver& driver, const TFederatedTopicClientSettings& settings)
        : FederatedClient(driver, settings)
    {}

    NThreading::TFuture<std::vector<TFederatedTopicClient::TClusterInfo>> GetAllTopicClusters() final {
        return FederatedClient.GetAllClusterInfo();
    }

    std::shared_ptr<IWriteSession> CreateWriteSession(const TFederatedWriteSessionSettings& settings) final {
        return FederatedClient.CreateWriteSession(settings);
    }

private:
    TFederatedTopicClient FederatedClient;
};

} // anonymous namespace

IFederatedTopicClient::TPtr CreateExternalFederatedTopicClient(const TDriver& driver, const TFederatedTopicClientSettings& settings) {
    return MakeIntrusive<TNativeFederatedTopicClient>(driver, settings);
}

} // namespace NYql
