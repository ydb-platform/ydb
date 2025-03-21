#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/federated_topic/federated_topic.h>
#include <ydb/public/sdk/cpp/src/client/federated_topic/impl/federated_topic_impl.h>

namespace NYdb::inline Dev::NFederatedTopic {

// TFederatedReadSessionSettings
// Read policy settings

using TReadOriginalSettings = TFederatedReadSessionSettings::TReadOriginalSettings;
TReadOriginalSettings& TReadOriginalSettings::AddDatabase(const std::string& database) {
    Databases.insert(std::move(database));
    return *this;
}

TReadOriginalSettings& TReadOriginalSettings::AddDatabases(const std::vector<std::string>& databases) {
    std::move(std::begin(databases), std::end(databases), std::inserter(Databases, Databases.end()));
    return *this;
}

TReadOriginalSettings& TReadOriginalSettings::AddLocal() {
    Databases.insert("_local");
    return *this;
}

TFederatedReadSessionSettings& TFederatedReadSessionSettings::ReadOriginal(TReadOriginalSettings settings) {
    std::swap(DatabasesToReadFrom, settings.Databases);
    ReadMirroredEnabled = false;
    return *this;
}

TFederatedReadSessionSettings& TFederatedReadSessionSettings::ReadMirrored(const std::string& database) {
    if (database == "_local") {
        ythrow TContractViolation("Reading from local database not supported, use specific database");
    }
    DatabasesToReadFrom.clear();
    DatabasesToReadFrom.insert(std::move(database));
    ReadMirroredEnabled = true;
    return *this;
}

// TFederatedTopicClient

NTopic::TTopicClientSettings FromFederated(const TFederatedTopicClientSettings& fedSettings) {
    auto settings = NTopic::TTopicClientSettings()
        .DefaultCompressionExecutor(fedSettings.DefaultCompressionExecutor_)
        .DefaultHandlersExecutor(fedSettings.DefaultHandlersExecutor_);

    if (fedSettings.CredentialsProviderFactory_) {
        settings.CredentialsProviderFactory(*fedSettings.CredentialsProviderFactory_);
    }
    if (fedSettings.SslCredentials_) {
        settings.SslCredentials(*fedSettings.SslCredentials_);
    }
    if (fedSettings.DiscoveryMode_) {
        settings.DiscoveryMode(*fedSettings.DiscoveryMode_);
    }
    return settings;
}

TFederatedTopicClient::TFederatedTopicClient(const TDriver& driver, const TFederatedTopicClientSettings& settings)
    : Impl_(std::make_shared<TImpl>(CreateInternalInterface(driver), settings))
{
    ProvideCodec(NTopic::ECodec::GZIP, std::make_unique<NTopic::TGzipCodec>());
    ProvideCodec(NTopic::ECodec::LZOP, std::make_unique<NTopic::TUnsupportedCodec>());
    ProvideCodec(NTopic::ECodec::ZSTD, std::make_unique<NTopic::TZstdCodec>());
}

void TFederatedTopicClient::ProvideCodec(NTopic::ECodec codecId, std::unique_ptr<NTopic::ICodec>&& codecImpl) {
    return Impl_->ProvideCodec(codecId, std::move(codecImpl));
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

void TFederatedTopicClient::OverrideCodec(NTopic::ECodec codecId, std::unique_ptr<NTopic::ICodec>&& codecImpl) {
    return Impl_->OverrideCodec(codecId, std::move(codecImpl));
}

NThreading::TFuture<std::vector<TFederatedTopicClient::TClusterInfo>> TFederatedTopicClient::GetAllClusterInfo() {
    return Impl_->GetAllClusterInfo();
}

void TFederatedTopicClient::TClusterInfo::AdjustTopicClientSettings(NTopic::TTopicClientSettings& settings) const {
    if (Name.empty()) {
        return;
    }
    settings.DiscoveryEndpoint(Endpoint);
    settings.Database(Path);
}

void TFederatedTopicClient::TClusterInfo::AdjustTopicPath(std::string& path) const {
    if (Name.empty()) {
        return;
    }
    if (path.empty() || path[0] != '/') {
        path = Path + '/' + path;
    }
}

bool TFederatedTopicClient::TClusterInfo::IsAvailableForRead() const {
    return Status == TClusterInfo::EStatus::AVAILABLE || Status == TClusterInfo::EStatus::READ_ONLY;
}

bool TFederatedTopicClient::TClusterInfo::IsAvailableForWrite() const {
    return Status == TClusterInfo::EStatus::AVAILABLE;
}

std::vector<NTopic::TTopicClient> TFederatedTopicClient::GetAllTopicClients(const TDriver& driver, const std::vector<TClusterInfo>& clusterInfos, NTopic::TTopicClientSettings& clientSettings) {
    std::vector<NTopic::TTopicClient> clients;
    clients.reserve(clusterInfos.size());
    for (auto& info: clusterInfos) {
        info.AdjustTopicClientSettings(clientSettings);
        clients.emplace_back(driver, clientSettings);
    }
    return clients;
}

std::vector<TAsyncDescribeTopicResult> TFederatedTopicClient::DescribeAllTopics(const std::string& path, std::vector<NTopic::TTopicClient>& topicClients, const std::vector<TClusterInfo>& clusterInfos, NTopic::TDescribeTopicSettings& describeSettings) {
    Y_ENSURE(topicClients.size() == clusterInfos.size());
    std::vector<TAsyncDescribeTopicResult> results;
    results.reserve(topicClients.size());
    for (size_t i = 0; i < topicClients.size(); ++i) {
        const auto& info = clusterInfos[i];
        if (!info.IsAvailableForRead()) {
            results.emplace_back(NThreading::MakeErrorFuture<NTopic::TDescribeTopicResult>(std::exception_ptr())); // FIXME
            continue;
        }
        std::string adjustedPath = path;
        info.AdjustTopicPath(adjustedPath);
        results.emplace_back(topicClients[i].DescribeTopic(adjustedPath, describeSettings));
    }
    return results;
}

std::vector<std::shared_ptr<NTopic::IReadSession>> TFederatedTopicClient::CreateAllTopicsReadSessions(std::vector<NTopic::TTopicClient>& topicClients, const std::vector<TClusterInfo>& clusterInfos, std::vector<NTopic::TReadSessionSettings>& readSettings) {
    std::vector<std::shared_ptr<NTopic::IReadSession>> readSessions;
    readSessions.reserve(topicClients.size());
    for (size_t i = 0; i < topicClients.size(); ++i) {
        const auto& info = clusterInfos[i];
        if (!info.IsAvailableForRead()) {
            readSessions.emplace_back();
            continue;
        }
        for (auto& topicSettings: readSettings[i].Topics_) {
            info.AdjustTopicPath(topicSettings.Path_);
        }
        readSessions.emplace_back(topicClients[i].CreateReadSession(readSettings[i]));
    }
    return readSessions;
}

} // namespace NYdb::NFederatedTopic
