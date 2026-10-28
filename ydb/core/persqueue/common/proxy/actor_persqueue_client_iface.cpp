#include "actor_persqueue_client_iface.h"

namespace NKikimr::NPQ {

NYdb::NTopic::TTopicClient IPersQueueMirrorReaderFactory::GetTopicClient(const NKikimrPQ::TMirrorPartitionConfig& config, std::shared_ptr<NYdb::ICredentialsProviderFactory> credentialsProviderFactory) const {
    NYdb::NTopic::TTopicClientSettings clientSettings = NYdb::NTopic::TTopicClientSettings()
        .DiscoveryEndpoint(TStringBuilder() << config.GetEndpoint() << ":" << config.GetEndpointPort())
        .DiscoveryMode(NYdb::EDiscoveryMode::Async)
        .CredentialsProviderFactory(std::move(credentialsProviderFactory))
        .SslCredentials(NYdb::TSslCredentials(config.GetUseSecureConnection()));
    if (config.HasDatabase()) {
        clientSettings.Database(config.GetDatabase());
    }
    return NYdb::NTopic::TTopicClient(*Driver, clientSettings);
}

} // namespace NKikimr::NPQ
