#include "local_topic_client_factory.h"
#include "local_topic_client.h"

namespace NKikimr::NKqp {

namespace {

using namespace NYdb::NTopic;

class TPqLocalClientFactory final : public NYql::IPqLocalClientFactory {
public:
    explicit TPqLocalClientFactory(const TLocalTopicClientSettings& settings)
        : Settings(settings)
    {}

    NYql::ITopicClient::TPtr CreateTopicClient(const TTopicClientSettings& clientSettings) final {
        return CreateLocalTopicClient(Settings, clientSettings);
    }

private:
    const TLocalTopicClientSettings Settings;
};

} // anonymous namespace

NYql::IPqLocalClientFactory::TPtr CreateLocalTopicClientFactory(const TLocalTopicClientSettings& settings) {
    return MakeIntrusive<TPqLocalClientFactory>(settings);
}

} // namespace NKikimr::NKqp
