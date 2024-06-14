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

void TTopicSdkTestSetup::CreateTopic(const TString& path, const TString& consumer, size_t partitionCount, std::optional<size_t> maxPartitionCount)
{
    TTopicClient client(MakeDriver());

    TCreateTopicSettings topics;
    topics
        .BeginConfigurePartitioningSettings()
        .MinActivePartitions(partitionCount)
        .MaxActivePartitions(maxPartitionCount.value_or(partitionCount));

    if (maxPartitionCount.has_value() && maxPartitionCount.value() > partitionCount) {
        topics
            .BeginConfigurePartitioningSettings()
            .BeginConfigureAutoscalingSettings()
            .Strategy(EAutoscalingStrategy::ScaleUp);
    }

    TConsumerSettings<TCreateTopicSettings> consumers(topics, consumer);
    topics.AppendConsumers(consumers);

    auto status = client.CreateTopic(path, topics).GetValueSync();
    UNIT_ASSERT(status.IsSuccess());

    Server.WaitInit(path);
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
    config.SetLog(MakeHolder<TStreamLogBackend>(&Cerr));
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