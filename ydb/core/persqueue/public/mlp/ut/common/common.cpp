#include "common.h"

namespace NKikimr::NPQ::NMLP {


std::shared_ptr<TTopicSdkTestSetup> CreateSetup() {
    auto setup = std::make_shared<TTopicSdkTestSetup>("TODO");
    setup->GetServer().EnableLogs({
            NKikimrServices::PQ_MLP_READER,
            NKikimrServices::PQ_MLP_WRITER,
            NKikimrServices::PQ_MLP_COMMITTER,
            NKikimrServices::PQ_MLP_UNLOCKER,
            NKikimrServices::PQ_MLP_DEADLINER,
            NKikimrServices::PQ_MLP_PURGER,
            NKikimrServices::PQ_MLP_CONSUMER,
            NKikimrServices::PQ_MLP_ENRICHER,
            NKikimrServices::PQ_MLP_DLQ_MOVER,
        },
        NActors::NLog::PRI_DEBUG
    );
    setup->GetServer().EnableLogs({
            NKikimrServices::PERSQUEUE,
            NKikimrServices::PERSQUEUE_READ_BALANCER,
            NKikimrServices::PQ_WRITE_PROXY
        },
        NActors::NLog::PRI_INFO
    );

    setup->GetRuntime().GetAppData().PQConfig.SetBalancerWakeupIntervalSec(1);
    setup->GetRuntime().GetAppData().PQConfig.SetBalancerStatsWakeupIntervalSec(1);

    return setup;
}

void ExecuteDDL(TTopicSdkTestSetup& setup, const TString& query) {
    TDriver driver(setup.MakeDriverConfig());
    TQueryClient client(driver);
    auto session = client.GetSession().GetValueSync().GetSession();

    Cerr << "DDL: " << query << Endl << Flush;
    auto res = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
    UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());

    driver.Stop(true);
}

TStatus CreateTopic(std::shared_ptr<TTopicSdkTestSetup>& setup, const TString& topicName,
    NYdb::NTopic::TCreateTopicSettings& settings) {
    auto driver = TDriver(setup->MakeDriverConfig());
    auto client = TTopicClient(driver);

    auto result = client.CreateTopic(topicName, settings).GetValueSync();
    driver.Stop(true);
    return result;
}

TStatus CreateTopic(std::shared_ptr<TTopicSdkTestSetup>& setup, const TString& topicName, const TString& consumerName, size_t partitionCount,
        bool keepMessagesOrder, bool autopartitioning) {
    return CreateTopic(setup, topicName, NYdb::NTopic::TCreateTopicSettings()
            .BeginConfigurePartitioningSettings()
                .MinActivePartitions(partitionCount)
                .MaxActivePartitions(128)
                .BeginConfigureAutoPartitioningSettings()
                    .Strategy(autopartitioning ? EAutoPartitioningStrategy::ScaleUp : EAutoPartitioningStrategy::Disabled)
                    .StabilizationWindow(TDuration::Seconds(1))
                    .UpUtilizationPercent(2)
                    .DownUtilizationPercent(1)
                .EndConfigureAutoPartitioningSettings()
            .EndConfigurePartitioningSettings()
            .BeginAddSharedConsumer(consumerName)
                .KeepMessagesOrder(keepMessagesOrder)
                .BeginDeadLetterPolicy()
                    .Enable()
                    .BeginCondition()
                        .MaxProcessingAttempts(10)
                    .EndCondition()
                    .DeleteAction()
                .EndDeadLetterPolicy()
            .EndAddConsumer()
        );
}

TStatus AlterTopic(std::shared_ptr<TTopicSdkTestSetup>& setup, const TString& topicName,
    NYdb::NTopic::TAlterTopicSettings& settings) {

    auto driver = TDriver(setup->MakeDriverConfig());
    auto client = TTopicClient(driver);

    auto result = client.AlterTopic(topicName, settings).GetValueSync();
    driver.Stop(true);
    return result;
}

TActorId CreateReaderActor(NActors::TTestActorRuntime& runtime, TReaderSettings&& settings) {
    auto edgeId = runtime.AllocateEdgeActor();
    auto readerId = runtime.Register(CreateReader(edgeId, std::move(settings)));
    runtime.EnableScheduleForActor(readerId);
    runtime.DispatchEvents();

    return readerId;
}

TActorId CreateWriterActor(NActors::TTestActorRuntime& runtime, TWriterSettings&& settings) {
    auto edgeId = runtime.AllocateEdgeActor();
    auto actorId = runtime.Register(CreateWriter(edgeId, std::move(settings)));
    runtime.EnableScheduleForActor(actorId);
    runtime.DispatchEvents();

    return actorId;
}

TActorId CreateCommitterActor(NActors::TTestActorRuntime& runtime, TCommitterSettings&& settings) {
    auto edgeId = runtime.AllocateEdgeActor();
    auto readerId = runtime.Register(CreateCommitter(edgeId, std::move(settings)));
    runtime.EnableScheduleForActor(readerId);
    runtime.DispatchEvents();

    return readerId;
}

TActorId CreateUnlockerActor(NActors::TTestActorRuntime& runtime, TUnlockerSettings&& settings) {
    auto edgeId = runtime.AllocateEdgeActor();
    auto readerId = runtime.Register(CreateUnlocker(edgeId, std::move(settings)));
    runtime.EnableScheduleForActor(readerId);
    runtime.DispatchEvents();

    return readerId;
}

TActorId CreateMessageDeadlineChangerActor(NActors::TTestActorRuntime& runtime, TMessageDeadlineChangerSettings&& settings) {
    auto edgeId = runtime.AllocateEdgeActor();
    auto readerId = runtime.Register(CreateMessageDeadlineChanger(edgeId, std::move(settings)));
    runtime.EnableScheduleForActor(readerId);
    runtime.DispatchEvents();

    return readerId;
}

TActorId CreatePurgerActor(NActors::TTestActorRuntime& runtime, TPurgerSettings&& settings) {
    auto edgeId = runtime.AllocateEdgeActor();
    auto readerId = runtime.Register(CreatePurger(edgeId, std::move(settings)));
    runtime.EnableScheduleForActor(readerId);
    runtime.DispatchEvents();

    return readerId;
}

TActorId CreateDescriberActor(NActors::TTestActorRuntime& runtime, const TString& databasePath, const TString& topicPath) {
    auto edgeId = runtime.AllocateEdgeActor();
    auto readerId = runtime.Register(NDescriber::CreateDescriberActor(edgeId, databasePath, {topicPath}));
    runtime.EnableScheduleForActor(readerId);
    runtime.DispatchEvents();

    return readerId;
}

THolder<TEvPQ::TEvMLPReadResponse> WaitResult(NActors::TTestActorRuntime& runtime) {
    return runtime.GrabEdgeEvent<TEvPQ::TEvMLPReadResponse>();
}

THolder<TEvReadResponse> GetReadResponse(NActors::TTestActorRuntime& runtime, TDuration timeout) {
    return runtime.GrabEdgeEvent<TEvReadResponse>(timeout);
}

THolder<TEvPurgeResponse> GetPurgeResponse(NActors::TTestActorRuntime& runtime, TDuration timeout) {
    return runtime.GrabEdgeEvent<TEvPurgeResponse>(timeout);
}

THolder<TEvWriteResponse> GetWriteResponse(NActors::TTestActorRuntime& runtime, TDuration timeout) {
    return runtime.GrabEdgeEvent<TEvWriteResponse>(timeout);
}

THolder<TEvChangeResponse> GetChangeResponse(NActors::TTestActorRuntime& runtime, TDuration timeout) {
    return runtime.GrabEdgeEvent<TEvChangeResponse>(timeout);
}

THolder<NDescriber::TEvDescribeTopicsResponse> GetDescriberResponse(NActors::TTestActorRuntime& runtime, TDuration timeout) {
    return runtime.GrabEdgeEvent<NDescriber::TEvDescribeTopicsResponse>(timeout);
}

void AssertReadError(NActors::TTestActorRuntime& runtime, Ydb::StatusIds::StatusCode errorCode, const TString& message, TDuration timeout) {
    auto response = GetReadResponse(runtime, timeout);
    if (!response) {
        UNIT_FAIL("Timeout");
    }

    UNIT_ASSERT_VALUES_EQUAL_C(Ydb::StatusIds::StatusCode_Name(response->Status),
        Ydb::StatusIds::StatusCode_Name(errorCode), response->ErrorDescription);
    UNIT_ASSERT_VALUES_EQUAL(response->ErrorDescription, message);
}

void AssertPurgeError(NActors::TTestActorRuntime& runtime, Ydb::StatusIds::StatusCode errorCode, const TString& message, TDuration timeout) {
    auto response = GetPurgeResponse(runtime, timeout);
    if (!response) {
        UNIT_FAIL("Timeout");
    }

    UNIT_ASSERT_VALUES_EQUAL_C(Ydb::StatusIds::StatusCode_Name(response->Status),
        Ydb::StatusIds::StatusCode_Name(errorCode), response->ErrorDescription);
    UNIT_ASSERT_VALUES_EQUAL(response->ErrorDescription, message);
}

void AssertPurgeOK(NActors::TTestActorRuntime& runtime, TDuration timeout) {
    auto response = GetPurgeResponse(runtime, timeout);
    if (!response) {
        UNIT_FAIL("Timeout");
    }

    UNIT_ASSERT_VALUES_EQUAL_C(Ydb::StatusIds::StatusCode_Name(response->Status),
        Ydb::StatusIds::StatusCode_Name(Ydb::StatusIds::SUCCESS), response->ErrorDescription);
}

void WriteMany(std::shared_ptr<TTopicSdkTestSetup> setup, const std::string& topic, ui32 partitionId, size_t messageSize, size_t messageCount) {
    TTopicClient client(setup->MakeDriver());

    TWriteSessionSettings settings;
    settings.Path(topic);
    settings.PartitionId(partitionId);
    settings.Codec(NYdb::NTopic::ECodec::RAW);
    settings.DeduplicationEnabled(true);
    settings.ProducerId("test-producer")
        .MessageGroupId("test-producer");
    auto session = client.CreateSimpleBlockingWriteSession(settings);

    for(; messageCount; --messageCount) {
        auto message = NUnitTest::RandomString(messageSize);
        UNIT_ASSERT(session->Write(message));
    }

    session->Close();
}

ui64 GetTabletId(std::shared_ptr<TTopicSdkTestSetup>& setup, const TString& database, const TString& topic, ui32 partitionId) {
    CreateDescriberActor(setup->GetRuntime(), database, topic);
    auto result = GetDescriberResponse(setup->GetRuntime());
    UNIT_ASSERT_VALUES_EQUAL(result->Topics[topic].Status, NDescriber::EStatus::SUCCESS);
    return result->Topics[topic].Info->PartitionGraph->GetPartition(partitionId)->TabletId;
}

ui64 GetPQRBTabletId(std::shared_ptr<TTopicSdkTestSetup>& setup, const TString& database, const TString& topic) {
    CreateDescriberActor(setup->GetRuntime(), database, topic);
    auto result = GetDescriberResponse(setup->GetRuntime());
    UNIT_ASSERT_VALUES_EQUAL(result->Topics[topic].Status, NDescriber::EStatus::SUCCESS);
    return result->Topics[topic].Info->Description.GetBalancerTabletID();
}

THolder<NKikimr::TEvPQ::TEvGetMLPConsumerStateResponse> GetConsumerState(std::shared_ptr<TTopicSdkTestSetup>& setup,
    const TString& database, const TString& topic, const TString& consumer, ui32 partitionId) {
    auto tabletId = GetTabletId(setup, database, topic, partitionId);

    ForwardToTablet(setup->GetRuntime(), tabletId, setup->GetRuntime().AllocateEdgeActor(),
        new NKikimr::TEvPQ::TEvGetMLPConsumerStateRequest(topic, consumer, partitionId));
    return setup->GetRuntime().GrabEdgeEvent<NKikimr::TEvPQ::TEvGetMLPConsumerStateResponse>();
}

void ReloadPQTablet(std::shared_ptr<TTopicSdkTestSetup>& setup, const TString& database, const TString& topic, ui32 partitionId) {
    Cerr << ">>>>>> reload PQ tablet" << Endl;

    auto& runtime = setup->GetRuntime();
    auto tabletId = GetTabletId(setup, database, topic, partitionId);
    ForwardToTablet(runtime, tabletId, runtime.AllocateEdgeActor(), new TEvents::TEvPoison());
    Sleep(TDuration::Seconds(1));
}

void ReloadPQRBTablet(std::shared_ptr<TTopicSdkTestSetup>& setup, const TString& database, const TString& topic) {
    Cerr << ">>>>> reload PQRB tablet" << Endl;

    auto& runtime = setup->GetRuntime();
    auto tabletId = GetPQRBTabletId(setup, database, topic);
    ForwardToTablet(runtime, tabletId, runtime.AllocateEdgeActor(), new TEvents::TEvPoison());
    Sleep(TDuration::Seconds(1));
}

}
