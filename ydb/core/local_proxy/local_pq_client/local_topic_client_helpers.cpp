#include "local_topic_client_helpers.h"

#include <ydb/library/yverify_stream/yverify_stream.h>

namespace NKikimr::NKqp {

using namespace NYdb;

namespace {

std::shared_ptr<ICredentialsProvider> CreateCredentialsProvider(const std::optional<std::shared_ptr<ICredentialsProviderFactory>>& maybeFactory) {
    if (const auto factory = maybeFactory.value_or(nullptr)) {
        return factory->CreateProvider();
    }
    return nullptr;
}

} // anonymous namespace

TLocalTopicClientBase::TLocalTopicClientBase(const TLocalTopicClientSettings& localSettings, const TCommonClientSettings& clientSettings)
    : ActorSystem(localSettings.ActorSystem)
    , ChannelBufferSize(localSettings.ChannelBufferSize)
    , Database(clientSettings.Database_.value_or(""))
    , CredentialsProvider(CreateCredentialsProvider(clientSettings.CredentialsProviderFactory_))
{
    Y_VALIDATE(ActorSystem, "Actor system is not set");
    Y_VALIDATE(ChannelBufferSize, "Channel buffer size is not set");
    Y_VALIDATE(Database, "Database is not set");
    Y_VALIDATE(clientSettings.DiscoveryEndpoint_.value_or("").empty(), "Discovery endpoint is not allowed for local topics");
}

} // namespace NKikimr::NKqp
