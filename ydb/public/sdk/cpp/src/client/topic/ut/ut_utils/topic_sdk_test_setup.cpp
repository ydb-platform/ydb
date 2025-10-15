#include "topic_sdk_test_setup.h"

using namespace NYdb;
using namespace NYdb::NTopic;
using namespace NYdb::NTopic::NTests;

TTopicSdkTestSetup::TTopicSdkTestSetup(const TString& testCaseName, const NKikimr::Tests::TServerSettings& settings, bool createTopic)
    : Database("/Root")
    , Server(settings, false)
{
    Log.SetFormatter([testCaseName](ELogPriority priority, TStringBuf message) {
        return TStringBuilder() << TInstant::Now() << " :" << testCaseName << " " << priority << ": " << message << Endl;
    });

    Server.StartServer(true, GetDatabase());

    Log << "TTopicSdkTestSetup started";

    if (createTopic) {
        CreateTopic();
        Log << "Topic created";
    }
}

void TTopicSdkTestSetup::CreateTopicWithAutoscale(const std::string& path, const std::string& consumer, size_t partitionCount, size_t maxPartitionCount) {
    CreateTopic(path, consumer, partitionCount, maxPartitionCount);
}

void TTopicSdkTestSetup::CreateTopic(const std::string& path, const std::string& consumer, size_t partitionCount, std::optional<size_t> maxPartitionCount, const TDuration retention, bool important)
{
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

    TConsumerSettings<TCreateTopicSettings> consumers(topics, consumer);
    consumers.Important(important);
    topics.AppendConsumers(consumers);

    auto status = client.CreateTopic(path, topics).GetValueSync();
    UNIT_ASSERT(status.IsSuccess());

    Server.WaitInit(TString{path});
}

TTopicDescription TTopicSdkTestSetup::DescribeTopic(const std::string& path)
{
    TTopicClient client(MakeDriver());

    TDescribeTopicSettings settings;
    settings.IncludeStats(true);
    settings.IncludeLocation(true);

    auto status = client.DescribeTopic(path, settings).GetValueSync();
    UNIT_ASSERT(status.IsSuccess());

    return status.GetTopicDescription();
}

TConsumerDescription TTopicSdkTestSetup::DescribeConsumer(const std::string& path, const std::string& consumer)
{
    TTopicClient client(MakeDriver());

    TDescribeConsumerSettings settings;
    settings.IncludeStats(true);
    settings.IncludeLocation(true);

    auto status = client.DescribeConsumer(path, consumer, settings).GetValueSync();
    UNIT_ASSERT(status.IsSuccess());

    return status.GetConsumerDescription();
}

void TTopicSdkTestSetup::Write(const std::string& message, ui32 partitionId, const std::optional<std::string> producer, std::optional<ui64> seqNo) {
    TTopicClient client(MakeDriver());

    TWriteSessionSettings settings;
    settings.Path(TEST_TOPIC);
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

TTopicSdkTestSetup::ReadResult TTopicSdkTestSetup::Read(const std::string& topic, const std::string& consumer,
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

    ReadResult result;
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


TString TTopicSdkTestSetup::GetEndpoint() const {
    return "localhost:" + ToString(Server.GrpcPort);
}

TString TTopicSdkTestSetup::GetTopicPath(const TString& name) const {
    return GetTopicParent() + "/" + name;
}

TString TTopicSdkTestSetup::GetTopicParent() const {
    return GetDatabase();
}

TString TTopicSdkTestSetup::GetDatabase() const {
    return Database;
}

::NPersQueue::TTestServer& TTopicSdkTestSetup::GetServer() {
    return Server;
}

NActors::TTestActorRuntime& TTopicSdkTestSetup::GetRuntime() {
    return *Server.CleverServer->GetRuntime();
}

TLog& TTopicSdkTestSetup::GetLog() {
    return Log;
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
        runtime.SetLogPriority(NKikimrServices::PQ_TX, NActors::NLog::PRI_DEBUG);
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
