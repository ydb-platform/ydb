#include <ydb/public/sdk/cpp/client/ydb_federated_topic/federated_topic.h>
#include <ydb/public/sdk/cpp/client/ydb_federated_topic/impl/federated_topic_impl.h>

namespace NYdb::NFederatedTopic {

NTopic::TTopicClientSettings FromFederated(const TFederatedTopicClientSettings& settings) {
    return NTopic::TTopicClientSettings()
        .DefaultCompressionExecutor(settings.DefaultCompressionExecutor_)
        .DefaultHandlersExecutor(settings.DefaultHandlersExecutor_);
}

TFederatedTopicClient::TFederatedTopicClient(const TDriver& driver, const TFederatedTopicClientSettings& settings)
    : Impl_(std::make_shared<TImpl>(CreateInternalInterface(driver), settings))
{
}

std::shared_ptr<IFederatedReadSession> TFederatedTopicClient::CreateFederatedReadSession(const TFederatedReadSessionSettings& settings) {
    return Impl_->CreateFederatedReadSession(settings);
}

// std::shared_ptr<NTopic::ISimpleBlockingWriteSession> TFederatedTopicClient::CreateSimpleBlockingFederatedWriteSession(
//     const TFederatedWriteSessionSettings& settings) {
//     return Impl_->CreateSimpleFederatedWriteSession(settings);
// }

// std::shared_ptr<NTopic::IWriteSession> TFederatedTopicClient::CreateFederatedWriteSession(const TFederatedWriteSessionSettings& settings) {
//     return Impl_->CreateFederatedWriteSession(settings);
// }

} // namespace NYdb::NFederatedTopic
