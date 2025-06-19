#include "topic_sdk_test_setup.h"

namespace NYdb::inline Dev::NTopic::NTests {

TTopicSdkTestSetup::TTopicSdkTestSetup(const std::string& testCaseName, const NKikimr::Tests::TServerSettings& settings, bool createTopic)
    : Database_("/Root")
    , TopicPath_(GetTopicParent() + "/" + "test-topic")
    , ConsumerName_("test-consumer")
    , Server_(settings, false)
{
    Log_.SetFormatter([testCaseName](ELogPriority priority, TStringBuf message) {
        return TStringBuilder() << TInstant::Now() << " :" << testCaseName << " " << priority << ": " << message << Endl;
    });

    Server_.StartServer(true, GetDatabase());

    Log_ << "TTopicSdkTestSetup started";

    if (createTopic) {
        CreateTopic();
        Log_ << "Topic created";
    }
}

void TTopicSdkTestSetup::CreateTopicWithAutoscale(const std::optional<std::string>& path,
                                                  const std::optional<std::string>& consumer,
                                                  std::size_t partitionCount,
                                                  std::optional<std::size_t> maxPartitionCount) {
    CreateTopic(path, consumer, partitionCount, maxPartitionCount);
}

void TTopicSdkTestSetup::CreateTopic(const std::optional<std::string>& path, const std::optional<std::string>& consumer,
                                     std::size_t partitionCount, std::optional<std::size_t> maxPartitionCount,
                                     const TDuration retention, bool important)
{
    std::string topicPath = path.value_or(TopicPath_);
    std::string consumerName = consumer.value_or(ConsumerName_);

    TTopicClient client(MakeDriver());

    TCreateTopicSettings topics;
    topics
        .RetentionPeriod(retention)
        .BeginConfigurePartitioningSettings()
        .MinActivePartitions(partitionCount)
        .MaxActivePartitions(maxPartitionCount.value_or(partitionCount));

    if (maxPartitionCount.has_value() && maxPartitionCount.value() > partitionCount) {
        topics
            .BeginConfigurePartitioningSettings()
            .BeginConfigureAutoPartitioningSettings()
            .Strategy(EAutoPartitioningStrategy::ScaleUp);
    }

    TConsumerSettings<TCreateTopicSettings> consumers(topics, consumerName);
    consumers.Important(important);
    topics.AppendConsumers(consumers);

    auto status = client.CreateTopic(topicPath, topics).GetValueSync();
    UNIT_ASSERT(status.IsSuccess());

    Server_.WaitInit(TString{topicPath});
}

TTopicDescription TTopicSdkTestSetup::DescribeTopic(const std::optional<std::string>& path)
{
    TTopicClient client(MakeDriver());

    TDescribeTopicSettings settings;
    settings.IncludeStats(true);
    settings.IncludeLocation(true);

    auto status = client.DescribeTopic(path.value_or(TopicPath_), settings).GetValueSync();
    UNIT_ASSERT(status.IsSuccess());

    return status.GetTopicDescription();
}

TConsumerDescription TTopicSdkTestSetup::DescribeConsumer(const std::optional<std::string>& path,
                                                          const std::optional<std::string>& consumer)
{
    TTopicClient client(MakeDriver());

    TDescribeConsumerSettings settings;
    settings.IncludeStats(true);
    settings.IncludeLocation(true);

    auto status = client.DescribeConsumer(path.value_or(TopicPath_), consumer.value_or(ConsumerName_), settings).GetValueSync();
    UNIT_ASSERT(status.IsSuccess());

    return status.GetConsumerDescription();
}

void TTopicSdkTestSetup::Write(const std::string& message, std::uint32_t partitionId,
                               const std::optional<std::string> producer,
                               std::optional<std::uint64_t> seqNo) {
    TTopicClient client(MakeDriver());

    TWriteSessionSettings settings;
    settings.Path(TopicPath_);
    settings.PartitionId(partitionId);
    settings.DeduplicationEnabled(producer.has_value());
    if (producer) {
        settings.ProducerId(producer.value())
            .MessageGroupId(producer.value());
    }
    auto session = client.CreateSimpleBlockingWriteSession(settings);

    UNIT_ASSERT(session->Write(message, seqNo));

    session->Close(TDuration::Seconds(5));
}

TTopicSdkTestSetup::TReadResult TTopicSdkTestSetup::Read(const std::string& topic, const std::string& consumer,
    std::function<bool (NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent&)> handler,
    std::optional<size_t> partition, const TDuration timeout) {
    TTopicClient client(MakeDriver());

    auto topicSettings = TTopicReadSettings(topic);
    if (partition) {
        topicSettings.AppendPartitionIds(partition.value());
    }

    auto settins = TReadSessionSettings()
        .AutoPartitioningSupport(true)
        .AppendTopics(topicSettings)
        .ConsumerName(consumer);

    auto reader = client.CreateReadSession(settins);

    TInstant deadlineTime = TInstant::Now() + timeout;

    TReadResult result;
    result.Reader = reader;

    bool continueFlag = true;
    while(continueFlag && deadlineTime > TInstant::Now()) {
        for (auto event : reader->GetEvents(false)) {
            if (auto* x = std::get_if<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent>(&event)) {
                Cerr << "SESSION EVENT " << x->DebugString() << Endl << Flush;
                if (!handler(*x)) {
                    continueFlag = false;
                    break;
                }
            } else if (auto* x = std::get_if<NYdb::NTopic::TReadSessionEvent::TStartPartitionSessionEvent>(&event)) {
                Cerr << "SESSION EVENT " << x->DebugString() << Endl << Flush;
                x->Confirm();
                result.StartPartitionSessionEvents.push_back(*x);
            } else if (auto* x = std::get_if<NYdb::NTopic::TReadSessionEvent::TCommitOffsetAcknowledgementEvent>(&event)) {
                Cerr << "SESSION EVENT " << x->DebugString() << Endl << Flush;
            } else if (auto* x = std::get_if<NYdb::NTopic::TReadSessionEvent::TPartitionSessionStatusEvent>(&event)) {
                Cerr << "SESSION EVENT " << x->DebugString() << Endl << Flush;
            } else if (auto* x = std::get_if<NYdb::NTopic::TReadSessionEvent::TStopPartitionSessionEvent>(&event)) {
                Cerr << "SESSION EVENT " << x->DebugString() << Endl << Flush;
                x->Confirm();
            } else if (auto* x = std::get_if<NYdb::NTopic::TReadSessionEvent::TPartitionSessionClosedEvent>(&event)) {
                Cerr << "SESSION EVENT " << x->DebugString() << Endl << Flush;
            } else if (auto* x = std::get_if<NYdb::NTopic::TReadSessionEvent::TEndPartitionSessionEvent>(&event)) {
                Cerr << "SESSION EVENT " << x->DebugString() << Endl << Flush;
                x->Confirm();
            } else if (auto* x = std::get_if<NYdb::NTopic::TSessionClosedEvent>(&event)) {
                Cerr << "SESSION EVENT " << x->DebugString() << Endl << Flush;
            } else {
                Cerr << "SESSION EVENT unhandled \n";
            }
        }

        Sleep(TDuration::MilliSeconds(250));
    }

    result.Timeout = continueFlag;

    return result;
}

TStatus TTopicSdkTestSetup::Commit(const std::string& path, const std::string& consumerName, size_t partitionId, size_t offset, std::optional<std::string> sessionId) {
    TTopicClient client(MakeDriver());

    TCommitOffsetSettings commitSettings {.ReadSessionId_ = sessionId};
    return client.CommitOffset(path, partitionId, consumerName, offset, commitSettings).GetValueSync();
}


std::string TTopicSdkTestSetup::GetEndpoint() const {
    return "localhost:" + ToString(Server_.GrpcPort);
}

std::string TTopicSdkTestSetup::GetTopicPath() const {
    return TopicPath_;
}

std::string TTopicSdkTestSetup::GetConsumerName() const {
    return ConsumerName_;
}

std::string TTopicSdkTestSetup::GetTopicParent() const {
    return GetDatabase();
}

std::string TTopicSdkTestSetup::GetDatabase() const {
    return Database_;
}

std::vector<std::uint32_t> TTopicSdkTestSetup::GetNodeIds() {
    std::vector<std::uint32_t> nodeIds;
    for (std::uint32_t i = 0; i < Server_.GetRuntime()->GetNodeCount(); ++i) {
        nodeIds.push_back(Server_.GetRuntime()->GetNodeId(i));
    }
    return nodeIds;
}

std::uint16_t TTopicSdkTestSetup::GetPort() const {
    return Server_.GrpcPort;
}

::NPersQueue::TTestServer& TTopicSdkTestSetup::GetServer() {
    return Server_;
}

NActors::TTestActorRuntime& TTopicSdkTestSetup::GetRuntime() {
    return *Server_.CleverServer->GetRuntime();
}

TLog& TTopicSdkTestSetup::GetLog() {
    return Log_;
}

TDriverConfig TTopicSdkTestSetup::MakeDriverConfig() const
{
    TDriverConfig config;
    config.SetEndpoint(GetEndpoint());
    config.SetDatabase(GetDatabase());
    config.SetAuthToken("root@builtin");
    config.SetLog(std::make_unique<TStreamLogBackend>(&Cerr));
    return config;
}

NKikimr::Tests::TServerSettings TTopicSdkTestSetup::MakeServerSettings()
{
    auto settings = NKikimr::NPersQueueTests::PQSettings(0);
    settings.SetDomainName("Root");
    settings.SetNodeCount(1);
    settings.PQConfig.SetTopicsAreFirstClassCitizen(true);
    settings.PQConfig.SetRoot("/Root");
    settings.PQConfig.SetDatabase("/Root");

    settings.SetLoggerInitializer([](NActors::TTestActorRuntime& runtime) {
        runtime.SetLogPriority(NKikimrServices::PQ_READ_PROXY, NActors::NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::PQ_WRITE_PROXY, NActors::NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::PQ_MIRRORER, NActors::NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::PQ_METACACHE, NActors::NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::PERSQUEUE, NActors::NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::PERSQUEUE_CLUSTER_TRACKER, NActors::NLog::PRI_DEBUG);
    });

    return settings;
}

TDriver TTopicSdkTestSetup::MakeDriver() const {
    return MakeDriver(MakeDriverConfig());
}

TDriver TTopicSdkTestSetup::MakeDriver(const TDriverConfig& config) const
{
    return TDriver(config);
}

TTopicClient TTopicSdkTestSetup::MakeClient() const
{
    return TTopicClient(MakeDriver());
}

NYdb::NTable::TTableClient TTopicSdkTestSetup::MakeTableClient() const
{
    return NYdb::NTable::TTableClient(MakeDriver(), NYdb::NTable::TClientSettings()
            .UseQueryCache(false));
}

}
