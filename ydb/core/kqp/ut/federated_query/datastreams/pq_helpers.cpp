#include "pq_helpers.h"

namespace NTestUtils {

namespace {

struct TMockPartitionSession : public NYdb::NTopic::TPartitionSession {
    TMockPartitionSession(const TString& topicPath, ui64 partId) {
        PartitionSessionId = 0;
        TopicPath = topicPath;
        PartitionId = partId;
    }

    void RequestStatus() override {
        Y_ENSURE(false, "Not implemented");
    }
};

class TMockTopicReadSession : public NYdb::NTopic::IReadSession {
public:
    TMockTopicReadSession(const TMockPqGatewaySettings::TTopic& topic, NYdb::NTopic::TPartitionSession::TPtr session)
        : Topic(topic)
        , Session(session)
    {}

    NThreading::TFuture<void> WaitEvent() override {
        return NThreading::MakeFuture();
    }

    std::vector<NYdb::NTopic::TReadSessionEvent::TEvent> GetEvents(bool block = false, std::optional<size_t> maxEventsCount = std::nullopt, size_t maxByteSize = std::numeric_limits<size_t>::max()) override {
        if (maxEventsCount && *maxEventsCount == 0) {
            return {};
        }

        if (auto event = GetEvent(block, maxByteSize)) {
            return {std::move(*event)};
        }

        return {};
    }

    std::vector<NYdb::NTopic::TReadSessionEvent::TEvent> GetEvents(const NYdb::NTopic::TReadSessionGetEventSettings& settings) override {
        return GetEvents(settings.Block_, settings.MaxEventsCount_, settings.MaxByteSize_);
    }

    std::optional<NYdb::NTopic::TReadSessionEvent::TEvent> GetEvent(bool block = false, size_t maxByteSize = std::numeric_limits<size_t>::max()) override {
        Y_UNUSED(block, maxByteSize);

        return Topic.EventGen({
            .Session = Session
        });
    }

    std::optional<NYdb::NTopic::TReadSessionEvent::TEvent> GetEvent(const NYdb::NTopic::TReadSessionGetEventSettings& settings) override {
        return GetEvent(settings.Block_, settings.MaxByteSize_);
    }

    bool Close(TDuration timeout = TDuration::Max()) override {
        Y_UNUSED(timeout);
        return true;
    }

    NYdb::NTopic::TReaderCounters::TPtr GetCounters() const override {
        const auto result = MakeIntrusive<NYdb::NTopic::TReaderCounters>();
        NYdb::NTopic::MakeCountersNotNull(*result);
        return result;
    }

    std::string GetSessionId() const override {
        return ToString(Session->GetPartitionSessionId());
    }

private:
    const TMockPqGatewaySettings::TTopic Topic;
    const NYdb::NTopic::TPartitionSession::TPtr Session;
};

class TMockTopicClient : public NYql::ITopicClient {
public:
    explicit TMockTopicClient(const std::unordered_map<TString, TMockPqGatewaySettings::TTopic>& topics)
        : Topics(topics)
    {}

    NYdb::TAsyncStatus CreateTopic(const TString& path, const NYdb::NTopic::TCreateTopicSettings& settings = {}) override {
        Y_UNUSED(path, settings);
        Y_ENSURE(false, "Not implemented");
    }

    NYdb::TAsyncStatus AlterTopic(const TString& path, const NYdb::NTopic::TAlterTopicSettings& settings = {}) override {
        Y_UNUSED(path, settings);
        Y_ENSURE(false, "Not implemented");
    }

    NYdb::TAsyncStatus DropTopic(const TString& path, const NYdb::NTopic::TDropTopicSettings& settings = {}) override {
        Y_UNUSED(path, settings);
        Y_ENSURE(false, "Not implemented");
    }

    NYdb::NTopic::TAsyncDescribeTopicResult DescribeTopic(const TString& path, const NYdb::NTopic::TDescribeTopicSettings& settings = {}) override {
        Y_UNUSED(path, settings);
        Y_ENSURE(false, "Not implemented");
    }

    NYdb::NTopic::TAsyncDescribeConsumerResult DescribeConsumer(const TString& path, const TString& consumer, const NYdb::NTopic::TDescribeConsumerSettings& settings = {}) override {
        Y_UNUSED(path, consumer, settings);
        Y_ENSURE(false, "Not implemented");
    }

    NYdb::NTopic::TAsyncDescribePartitionResult DescribePartition(const TString& path, i64 partitionId, const NYdb::NTopic::TDescribePartitionSettings& settings = {}) override {
        Y_UNUSED(path, partitionId, settings);
        Y_ENSURE(false, "Not implemented");
    }

    std::shared_ptr<NYdb::NTopic::IReadSession> CreateReadSession(const NYdb::NTopic::TReadSessionSettings& settings) override {
        Y_ENSURE(settings.Topics_.size() == 1, "Expected only one topic to read, but got " << settings.Topics_.size());

        const auto& topic = settings.Topics_.front();
        Y_ENSURE(topic.PartitionIds_.size() == 1, "Expected only one partition to read, but got " << topic.PartitionIds_.size());

        const auto& path = TString(topic.Path_);
        const auto it = Topics.find(path);
        Y_ENSURE(it != Topics.end(), "Topic " << settings.Topics_.front().Path_ << " is not found for open read session");

        return std::make_shared<TMockTopicReadSession>(it->second, MakeIntrusive<TMockPartitionSession>(path, topic.PartitionIds_.front()));
    }

    std::shared_ptr<NYdb::NTopic::ISimpleBlockingWriteSession> CreateSimpleBlockingWriteSession(const NYdb::NTopic::TWriteSessionSettings& settings) override {
        Y_UNUSED(settings);
        Y_ENSURE(false, "Not implemented");
    }

    std::shared_ptr<NYdb::NTopic::IWriteSession> CreateWriteSession(const NYdb::NTopic::TWriteSessionSettings& settings) override {
        Y_UNUSED(settings);
        Y_ENSURE(false, "Not implemented");
    }

    NYdb::TAsyncStatus CommitOffset(const TString& path, ui64 partitionId, const TString& consumerName, ui64 offset, const NYdb::NTopic::TCommitOffsetSettings& settings = {}) override {
        Y_UNUSED(path, partitionId, consumerName, offset, settings);
        Y_ENSURE(false, "Not implemented");
    }

private:
    const std::unordered_map<TString, TMockPqGatewaySettings::TTopic> Topics;
};

class TMockFederatedTopicClient : public NYql::IFederatedTopicClient {
public:
    explicit TMockFederatedTopicClient(const std::unordered_map<TString, TMockPqGatewaySettings::TTopic>& topics)
        : Topics(topics)
    {}

    NThreading::TFuture<std::vector<NYdb::NFederatedTopic::TFederatedTopicClient::TClusterInfo>> GetAllTopicClusters() override {
        Y_ENSURE(false, "Not implemented");
    }

    std::shared_ptr<NYdb::NTopic::IWriteSession> CreateWriteSession(const NYdb::NFederatedTopic::TFederatedWriteSessionSettings& settings) override {
        Y_UNUSED(settings);
        Y_ENSURE(false, "Not implemented");
    }

private:
    const std::unordered_map<TString, TMockPqGatewaySettings::TTopic> Topics;
};

}  // anonymous namespace

TMockPqGateway::TMockPqGateway(const TMockPqGatewaySettings& settings)
    : Settings(settings)
{
    for (const auto& [cluster, settings] : Settings.Clusters) {
        Y_ENSURE(Settings.Topics.contains(settings.Database), "Topic settings for database " << settings.Database << " not found (specified in cluster " << cluster << ")");
    }
}

TMockPqGateway::~TMockPqGateway() {
    for (const auto& [cluster, settings] : Settings.Clusters) {
        Y_ABORT_UNLESS(settings.ExpectedQueriesToCluster == 0, "Missed queries to cluster");
    }
}

NThreading::TFuture<void> TMockPqGateway::OpenSession(const TString& sessionId, const TString& username) {
    Y_ENSURE(sessionId);
    Y_UNUSED(username);

    with_lock (Mutex) {
        Y_ENSURE(Sessions.emplace(sessionId).second, "Session " << sessionId << " is already opened in pq gateway");
    }

    return NThreading::MakeFuture();
}

NThreading::TFuture<void> TMockPqGateway::CloseSession(const TString& sessionId) {
    with_lock (Mutex) {
        Y_ENSURE(Sessions.erase(sessionId), "Session " << sessionId << " is not opened in pq gateway");
    }

    return NThreading::MakeFuture();
}

NPq::NConfigurationManager::TAsyncDescribePathResult TMockPqGateway::DescribePath(const TString& sessionId, const TString& cluster, const TString& database, const TString& path, const TString& token) {
    Y_UNUSED(sessionId, cluster, database, path, token);

    CheckTopicPath(cluster, path);

    NPq::NConfigurationManager::TTopicDescription result(path);
    result.PartitionsCount = 1;
    return NThreading::MakeFuture<NPq::NConfigurationManager::TDescribePathResult>(NPq::NConfigurationManager::TDescribePathResult::Make<NPq::NConfigurationManager::TTopicDescription>(result));
}

NThreading::TFuture<TMockPqGateway::TListStreams> TMockPqGateway::ListStreams(const TString& sessionId, const TString& cluster, const TString& database, const TString& token, ui32 limit, const TString& exclusiveStartStreamName) {
    Y_UNUSED(sessionId, cluster, database, token, limit, exclusiveStartStreamName);
    Y_ENSURE(false, "Not implemented");
}

TMockPqGateway::TAsyncDescribeFederatedTopicResult TMockPqGateway::DescribeFederatedTopic(const TString& sessionId, const TString& cluster, const TString& database, const TString& path, const TString& token) {
    Y_UNUSED(sessionId, database, token);

    CheckTopicPath(cluster, path);

    return NThreading::MakeFuture<TDescribeFederatedTopicResult>(IPqGateway::TDescribeFederatedTopicResult{{
        .PartitionsCount = 1,
    }});
}

NYql::ITopicClient::TPtr TMockPqGateway::GetTopicClient(const NYdb::TDriver& driver, const NYdb::NTopic::TTopicClientSettings& settings) {
    Y_UNUSED(driver, settings);
    Y_ENSURE(settings.Database_);

    return MakeIntrusive<TMockTopicClient>(GetDatabaseTopics(TString(*settings.Database_)));
}

NYql::IFederatedTopicClient::TPtr TMockPqGateway::GetFederatedTopicClient(const NYdb::TDriver& driver, const NYdb::NFederatedTopic::TFederatedTopicClientSettings& settings) {
    Y_UNUSED(driver, settings);
    Y_ENSURE(settings.Database_);

    return MakeIntrusive<TMockFederatedTopicClient>(GetDatabaseTopics(TString(*settings.Database_)));
}

void TMockPqGateway::UpdateClusterConfigs(const TString& clusterName, const TString& endpoint, const TString& database, bool secure) {
    Y_UNUSED(clusterName, endpoint, database, secure);
    Y_ENSURE(false, "Not implemented");
}

void TMockPqGateway::UpdateClusterConfigs(const NYql::TPqGatewayConfigPtr& config) {
    Y_UNUSED(config);
    Y_ENSURE(false, "Not implemented");
}

void TMockPqGateway::AddCluster(const NYql::TPqClusterConfig& cluster) {
    const auto& name = cluster.GetName();

    with_lock (Mutex) {
        const auto clusterIt = Settings.Clusters.find(name);
        Y_ENSURE(clusterIt != Settings.Clusters.end(), "Cluster " << name << " is not registered in pq gateway");

        auto& expectedQueries = clusterIt->second.ExpectedQueriesToCluster;
        Y_ENSURE(expectedQueries > 0, "Too many queries to cluster " << name);
        expectedQueries--;
    }
}

NYdb::NTopic::TTopicClientSettings TMockPqGateway::GetTopicClientSettings() const {
    return NYdb::NTopic::TTopicClientSettings();
}

NYdb::NFederatedTopic::TFederatedTopicClientSettings TMockPqGateway::GetFederatedTopicClientSettings() const {
    return NYdb::NFederatedTopic::TFederatedTopicClientSettings();
}

void TMockPqGateway::CheckTopicPath(const TString& cluster, const TString& path) const {
    with_lock (Mutex) {
        const auto clusterIt = Settings.Clusters.find(cluster);
        Y_ENSURE(clusterIt != Settings.Clusters.end(), "Cluster " << cluster << " is not registered in pq gateway");

        const auto& topicDatabase = clusterIt->second.Database;
        Y_ENSURE(Settings.Topics.at(clusterIt->second.Database).contains(path), "Topic " << path << " is not registered in cluster " << cluster << " (database: " << topicDatabase << ")");
    }
}

std::unordered_map<TString, TMockPqGatewaySettings::TTopic> TMockPqGateway::GetDatabaseTopics(const TString& database) const {
    with_lock (Mutex) {
        const auto it = Settings.Topics.find(database);
        Y_ENSURE(it != Settings.Topics.end(), "Database " << database << " is not registered in pq gateway");
        return it->second;
    }
}

NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent MakePqMessage(ui64 offset, const TString& data, const TMockPqSession& meta) {
    const auto now = TInstant::Now();

    return NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent({
        NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage(
            data,
            nullptr,
            NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessageInformation(
                offset,
                "",
                0,
                now,
                now,
                MakeIntrusive<NYdb::NTopic::TWriteSessionMeta>(),
                MakeIntrusive<NYdb::NTopic::TMessageMeta>(),
                data.size(),
                ""
            ),
            meta.Session
        )
    }, {}, meta.Session);
}

} // namespace NTestUtils
