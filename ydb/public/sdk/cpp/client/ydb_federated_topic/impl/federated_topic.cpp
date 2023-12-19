#include <ydb/public/sdk/cpp/client/ydb_federated_topic/federated_topic.h>
#include <ydb/public/sdk/cpp/client/ydb_federated_topic/impl/federated_topic_impl.h>

namespace NYdb::NFederatedTopic {

// TFederatedReadSessionSettings
// Read policy settings

using TReadOriginalSettings = TFederatedReadSessionSettings::TReadOriginalSettings;
TReadOriginalSettings& TReadOriginalSettings::AddDatabase(TString database) {
    Databases.push_back(std::move(database));
    return *this;
}

TReadOriginalSettings& TReadOriginalSettings::AddDatabases(std::vector<TString> databases) {
    std::move(std::begin(databases), std::end(databases), std::back_inserter(Databases));
    return *this;
}

TReadOriginalSettings& TReadOriginalSettings::AddLocal() {
    Databases.push_back("_local");
    return *this;
}

TFederatedReadSessionSettings& TFederatedReadSessionSettings::ReadOriginal(TReadOriginalSettings settings) {
    std::swap(Databases, settings.Databases);
    ReadMirroredEnabled = false;
    return *this;
}

TFederatedReadSessionSettings& TFederatedReadSessionSettings::ReadMirrored(TString database) {
    Databases.clear();
    Databases.push_back(std::move(database));
    ReadMirroredEnabled = true;
    return *this;
}

// TFederatedTopicClient

NTopic::TTopicClientSettings FromFederated(const TFederatedTopicClientSettings& settings) {
    return NTopic::TTopicClientSettings()
        .DefaultCompressionExecutor(settings.DefaultCompressionExecutor_)
        .DefaultHandlersExecutor(settings.DefaultHandlersExecutor_);
}

TFederatedTopicClient::TFederatedTopicClient(const TDriver& driver, const TFederatedTopicClientSettings& settings)
    : Impl_(std::make_shared<TImpl>(CreateInternalInterface(driver), settings))
{
}

std::shared_ptr<IFederatedReadSession> TFederatedTopicClient::CreateReadSession(const TFederatedReadSessionSettings& settings) {
    return Impl_->CreateReadSession(settings);
}

// std::shared_ptr<NTopic::ISimpleBlockingWriteSession> TFederatedTopicClient::CreateSimpleBlockingWriteSession(
//     const TFederatedWriteSessionSettings& settings) {
//     return Impl_->CreateSimpleBlockingWriteSession(settings);
// }

std::shared_ptr<NTopic::IWriteSession> TFederatedTopicClient::CreateWriteSession(const TFederatedWriteSessionSettings& settings) {
    return Impl_->CreateWriteSession(settings);
}

} // namespace NYdb::NFederatedTopic
