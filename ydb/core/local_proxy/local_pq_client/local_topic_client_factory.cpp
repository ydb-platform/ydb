#include "local_topic_client_factory.h"
#include "local_deferred_publish_client.h"
#include "local_federated_topic_client.h"
#include "local_topic_client.h"

namespace NKikimr::NKqp {

namespace {

using namespace NYdb;
using namespace NYdb::NFederatedTopic;
using namespace NYdb::NTopic;

class TPqLocalClientFactory final : public NYql::IPqLocalClientFactory {
public:
    explicit TPqLocalClientFactory(const TLocalTopicClientSettings& settings)
        : Settings(settings)
    {}

    NYql::ITopicClient::TPtr CreateTopicClient(const TTopicClientSettings& clientSettings) final {
        return CreateLocalTopicClient(Settings, clientSettings);
    }

    NYql::IFederatedTopicClient::TPtr CreateFederatedTopicClient(const TFederatedTopicClientSettings& clientSettings) final {
        return CreateLocalFederatedTopicClient(Settings, clientSettings);
    }

    NYql::IDeferredPublishClient::TPtr CreateDeferredPublishClient(const TCommonClientSettings& settings) final {
        return CreateLocalDeferredPublishClient(Settings, settings);
    }

private:
    const TLocalTopicClientSettings Settings;
};

} // anonymous namespace

NYql::IPqLocalClientFactory::TPtr CreateLocalTopicClientFactory(const TLocalTopicClientSettings& settings) {
    return MakeIntrusive<TPqLocalClientFactory>(settings);
}

} // namespace NKikimr::NKqp
