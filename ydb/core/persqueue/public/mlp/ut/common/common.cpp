#include "common.h"

namespace NKikimr::NPQ::NMLP {


namespace {

void EnableMlpLogs(TTopicSdkTestSetup& setup) {
    setup.GetServer().EnableLogs({
            NKikimrServices::PQ_MLP_READER,
            NKikimrServices::PQ_MLP_WRITER,
            NKikimrServices::PQ_MLP_COMMITTER,
            NKikimrServices::PQ_MLP_UNLOCKER,
            NKikimrServices::PQ_MLP_DEADLINER,
            NKikimrServices::PQ_MLP_PURGER,
            NKikimrServices::PQ_MLP_CONSUMER,
            NKikimrServices::PQ_MLP_ENRICHER,
            NKikimrServices::PQ_MLP_DLQ_MOVER,
            NKikimrServices::PQ_MLP_DESCRIBER,
        },
        NActors::NLog::PRI_DEBUG
    );
    setup.GetServer().EnableLogs({
            NKikimrServices::PERSQUEUE,
            NKikimrServices::PERSQUEUE_READ_BALANCER,
            NKikimrServices::PQ_WRITE_PROXY
        },
        NActors::NLog::PRI_INFO
    );

    setup.GetRuntime().GetAppData().PQConfig.SetBalancerWakeupIntervalSec(1);
    setup.GetRuntime().GetAppData().PQConfig.SetBalancerStatsWakeupIntervalSec(1);
}

} // namespace

IThreadPool& GetMlpPipeDispatchPool() {
    struct TPoolHolder {
        TThreadPool Pool;
        TPoolHolder() {
            Pool.Start(2);
        }
    };
    // Function-local static: one pool for all RunWithDispatch instantiations, init is thread-safe.
    static TPoolHolder holder;
    return holder.Pool;
}

std::shared_ptr<TTopicSdkTestSetup> CreateSetup() {
    auto setup = std::make_shared<TTopicSdkTestSetup>("TODO");
    EnableMlpLogs(*setup);
    return setup;
}

std::shared_ptr<TMlpPipeSetup> CreatePipeSetup() {
    auto settings = TTopicSdkTestSetup::MakeServerSettings();
    settings.SetUseRealThreads(false);

    auto setup = std::make_shared<TMlpPipeSetup>();
    setup->Server = std::make_unique<TTestServer>(settings, /*start=*/false);
    setup->Server->StartServer(/*doClientInit=*/false, TString("/Root"));

    auto& runtime = setup->GetRuntime();
    runtime.SetLogPriority(NKikimrServices::PQ_MLP_READER, NActors::NLog::PRI_DEBUG);
    runtime.SetLogPriority(NKikimrServices::PQ_MLP_WRITER, NActors::NLog::PRI_DEBUG);
    runtime.SetLogPriority(NKikimrServices::PQ_MLP_COMMITTER, NActors::NLog::PRI_DEBUG);
    runtime.SetLogPriority(NKikimrServices::PQ_MLP_UNLOCKER, NActors::NLog::PRI_DEBUG);
    runtime.SetLogPriority(NKikimrServices::PQ_MLP_DEADLINER, NActors::NLog::PRI_DEBUG);
    runtime.SetLogPriority(NKikimrServices::PQ_MLP_PURGER, NActors::NLog::PRI_DEBUG);
    runtime.SetLogPriority(NKikimrServices::PQ_MLP_CONSUMER, NActors::NLog::PRI_DEBUG);
    runtime.SetLogPriority(NKikimrServices::PQ_MLP_ENRICHER, NActors::NLog::PRI_DEBUG);
    runtime.SetLogPriority(NKikimrServices::PQ_MLP_DLQ_MOVER, NActors::NLog::PRI_DEBUG);
    runtime.SetLogPriority(NKikimrServices::PQ_MLP_DESCRIBER, NActors::NLog::PRI_DEBUG);
    runtime.SetLogPriority(NKikimrServices::PERSQUEUE, NActors::NLog::PRI_INFO);
    runtime.SetLogPriority(NKikimrServices::PERSQUEUE_READ_BALANCER, NActors::NLog::PRI_INFO);
    runtime.SetLogPriority(NKikimrServices::PQ_WRITE_PROXY, NActors::NLog::PRI_INFO);
    runtime.GetAppData().PQConfig.SetBalancerWakeupIntervalSec(1);
    runtime.GetAppData().PQConfig.SetBalancerStatsWakeupIntervalSec(1);

    // Simulated time starts at 0; sync to wall clock so MLP retention (Now - retention)
    // does not treat fresh messages as already expired.
    runtime.UpdateCurrentTime(TInstant::Now());

    // FFC topics do not need PQ config tables / cluster tracker (much faster under fake threads).
    setup->Server->AnnoyingClient->SetNoConfigMode();
    RunWithDispatch(runtime, [&] {
        setup->Server->AnnoyingClient->FullInit();
        return true;
    });
    return setup;
}

void WriteViaMlp(std::shared_ptr<TMlpPipeSetup>& setup, const TString& topic, const TString& body) {
    auto& runtime = setup->GetRuntime();
    CreateWriterActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = topic,
        .Messages = {
            {
                .Index = 0,
                .MessageBody = body,
            }
        }
    });
    // Longer timeout: under UseRealThreads=false MLP consumer may flood the mailbox.
    auto response = GetWriteResponse(runtime, TDuration::Seconds(30));
    UNIT_ASSERT(response);
    UNIT_ASSERT_VALUES_EQUAL(response->DescribeStatus, NDescriber::EStatus::SUCCESS);
    UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].Status, Ydb::StatusIds::SUCCESS);
}

ui64 GetTabletId(std::shared_ptr<TMlpPipeSetup>& setup, const TString& database, const TString& topic,
    ui32 partitionId)
{
    CreateDescriberActor(setup->GetRuntime(), database, topic);
    auto result = GetDescriberResponse(setup->GetRuntime(), TDuration::Seconds(30));
    UNIT_ASSERT(result);
    UNIT_ASSERT_VALUES_EQUAL(result->Topics[topic].Status, NDescriber::EStatus::SUCCESS);
    return result->Topics[topic].Info->PartitionGraph->GetPartition(partitionId)->TabletId;
}

TStatus CreatePipeTopic(std::shared_ptr<TMlpPipeSetup>& setup, const TString& topicName,
    const TString& consumerName, size_t partitionCount)
{
    auto& runtime = setup->GetRuntime();
    auto config = setup->MakeDriverConfig();
    // Required for SDK calls under UseRealThreads=false.
    config.SetDiscoveryMode(EDiscoveryMode::Async);

    return RunWithDispatch(runtime, [&] {
        auto driver = TDriver(config);
        auto client = TTopicClient(driver);
        auto result = client.CreateTopic(topicName, NYdb::NTopic::TCreateTopicSettings()
            .BeginConfigurePartitioningSettings()
                .MinActivePartitions(partitionCount)
                .MaxActivePartitions(128)
                .BeginConfigureAutoPartitioningSettings()
                    .Strategy(EAutoPartitioningStrategy::Disabled)
                .EndConfigureAutoPartitioningSettings()
            .EndConfigurePartitioningSettings()
            .BeginAddSharedConsumer(consumerName)
                .KeepMessagesOrder(false)
                .BeginDeadLetterPolicy()
                    .Enable()
                    .BeginCondition()
                        .MaxProcessingAttempts(10)
                    .EndCondition()
                    .DeleteAction()
                .EndDeadLetterPolicy()
            .EndAddConsumer()
        ).GetValueSync();
        driver.Stop(true);
        return result;
    });
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

namespace {

template <typename TCreateActor>
TActorId RegisterMlpActor(NActors::TTestActorRuntime& runtime, TCreateActor&& createActor) {
    auto edgeId = runtime.AllocateEdgeActor();
    auto actorId = runtime.Register(createActor(edgeId));
    runtime.EnableScheduleForActor(actorId);
    // With UseRealThreads=false an idle MLP consumer can keep the mailbox non-empty;
    // unbounded DispatchEvents then hits the event limit. GrabEdgeEvent drives the run.
    if (runtime.IsRealThreads()) {
        runtime.DispatchEvents();
    }
    return actorId;
}

} // namespace

TActorId CreateReaderActor(NActors::TTestActorRuntime& runtime, TReaderSettings&& settings) {
    return RegisterMlpActor(runtime, [&](const TActorId& edgeId) {
        return CreateReader(edgeId, std::move(settings));
    });
}

TActorId CreateWriterActor(NActors::TTestActorRuntime& runtime, TWriterSettings&& settings) {
    return RegisterMlpActor(runtime, [&](const TActorId& edgeId) {
        return CreateWriter(edgeId, std::move(settings));
    });
}

TActorId CreateCommitterActor(NActors::TTestActorRuntime& runtime, TCommitterSettings&& settings) {
    return RegisterMlpActor(runtime, [&](const TActorId& edgeId) {
        return CreateCommitter(edgeId, std::move(settings));
    });
}

TActorId CreateUnlockerActor(NActors::TTestActorRuntime& runtime, TUnlockerSettings&& settings) {
    return RegisterMlpActor(runtime, [&](const TActorId& edgeId) {
        return CreateUnlocker(edgeId, std::move(settings));
    });
}

TActorId CreateMessageDeadlineChangerActor(NActors::TTestActorRuntime& runtime, TMessageDeadlineChangerSettings&& settings) {
    return RegisterMlpActor(runtime, [&](const TActorId& edgeId) {
        return CreateMessageDeadlineChanger(edgeId, std::move(settings));
    });
}

TActorId CreatePurgerActor(NActors::TTestActorRuntime& runtime, TPurgerSettings&& settings) {
    return RegisterMlpActor(runtime, [&](const TActorId& edgeId) {
        return CreatePurger(edgeId, std::move(settings));
    });
}

TActorId CreateDescriberActor(NActors::TTestActorRuntime& runtime, TDescribeSettings&& settings) {
    return RegisterMlpActor(runtime, [&](const TActorId& edgeId) {
        return CreateDescriber(edgeId, std::move(settings));
    });
}

TActorId CreateDescriberActor(NActors::TTestActorRuntime& runtime, const TString& databasePath, const TString& topicPath) {
    return RegisterMlpActor(runtime, [&](const TActorId& edgeId) {
        return NDescriber::CreateDescriberActor(edgeId, databasePath, {topicPath});
    });
}

THolder<TEvPQ::TEvMLPReadResponse> WaitResult(NActors::TTestActorRuntime& runtime) {
    return runtime.GrabEdgeEvent<TEvPQ::TEvMLPReadResponse>();
}

namespace {

template <typename TEvent>
THolder<TEvent> ExpectEdgeEvent(NActors::TTestActorRuntime& runtime, TDuration timeout, const char* name) {
    auto response = runtime.GrabEdgeEvent<TEvent>(timeout);
    UNIT_ASSERT_C(response, TStringBuilder() << name << " timed out after " << timeout);
    return response;
}

} // namespace

THolder<TEvReadResponse> GetReadResponse(NActors::TTestActorRuntime& runtime, TDuration timeout) {
    return ExpectEdgeEvent<TEvReadResponse>(runtime, timeout, "GetReadResponse");
}

THolder<TEvPurgeResponse> GetPurgeResponse(NActors::TTestActorRuntime& runtime, TDuration timeout) {
    return ExpectEdgeEvent<TEvPurgeResponse>(runtime, timeout, "GetPurgeResponse");
}

THolder<TEvDescribeResponse> GetDescribeResponse(NActors::TTestActorRuntime& runtime, TDuration timeout) {
    return ExpectEdgeEvent<TEvDescribeResponse>(runtime, timeout, "GetDescribeResponse");
}

THolder<TEvWriteResponse> GetWriteResponse(NActors::TTestActorRuntime& runtime, TDuration timeout) {
    return ExpectEdgeEvent<TEvWriteResponse>(runtime, timeout, "GetWriteResponse");
}

THolder<TEvChangeResponse> GetChangeResponse(NActors::TTestActorRuntime& runtime, TDuration timeout) {
    return ExpectEdgeEvent<TEvChangeResponse>(runtime, timeout, "GetChangeResponse");
}

THolder<NDescriber::TEvDescribeTopicsResponse> GetDescriberResponse(NActors::TTestActorRuntime& runtime, TDuration timeout) {
    return ExpectEdgeEvent<NDescriber::TEvDescribeTopicsResponse>(runtime, timeout, "GetDescriberResponse");
}

void AssertReadError(NActors::TTestActorRuntime& runtime, Ydb::StatusIds::StatusCode errorCode, const TString& message, TDuration timeout) {
    auto response = GetReadResponse(runtime, timeout);

    UNIT_ASSERT_VALUES_EQUAL_C(Ydb::StatusIds::StatusCode_Name(response->Status),
        Ydb::StatusIds::StatusCode_Name(errorCode), response->ErrorDescription);
    UNIT_ASSERT_VALUES_EQUAL(response->ErrorDescription, message);
}

void AssertPurgeError(NActors::TTestActorRuntime& runtime, Ydb::StatusIds::StatusCode errorCode, const TString& message, TDuration timeout) {
    auto response = GetPurgeResponse(runtime, timeout);

    UNIT_ASSERT_VALUES_EQUAL_C(Ydb::StatusIds::StatusCode_Name(response->Status),
        Ydb::StatusIds::StatusCode_Name(errorCode), response->ErrorDescription);
    UNIT_ASSERT_VALUES_EQUAL(response->ErrorDescription, message);
}

void AssertPurgeOK(NActors::TTestActorRuntime& runtime, TDuration timeout) {
    auto response = GetPurgeResponse(runtime, timeout);

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

void WriteManyGroups(const std::shared_ptr<TTopicSdkTestSetup>& setup, const std::string& topic, size_t messageSize, size_t messageCount, size_t groupCount) {
    Y_ASSERT(groupCount > 0);
    std::vector<TWriterSettings::TMessage> messages;
    messages.reserve(messageCount);
    for (size_t i = 0; i < messageCount; ++i) {
        messages.push_back({
            .Index = i,
            .MessageBody = NUnitTest::RandomString(messageSize),
            .MessageGroupId = TStringBuilder() << "message_group_id_" << (i % groupCount),
        });
    }
    auto& runtime = setup->GetRuntime();
    TWriterSettings settings{
        .DatabasePath = setup->GetDatabase(),
        .TopicName = ToString(topic),
        .Messages = std::move(messages),
    };
    CreateWriterActor(runtime, std::move(settings));
    auto response = GetWriteResponse(runtime);
    UNIT_ASSERT_VALUES_EQUAL(response->DescribeStatus, NDescriber::EStatus::SUCCESS);
    UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), messageCount);
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
    auto response = setup->GetRuntime().GrabEdgeEvent<NKikimr::TEvPQ::TEvGetMLPConsumerStateResponse>(TDuration::Seconds(30));
    UNIT_ASSERT_C(response, "GetConsumerState timed out");
    return response;
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

void ModifyTopicAcl(TTopicSdkTestSetup& setup, const TString& topicName, const NACLib::TDiffACL& acl) {
    setup.GetServer().AnnoyingClient->ModifyACL("/Root", topicName, acl.SerializeAsString());
}

TPipeBreakGuard::TPipeBreakGuard(
    NActors::TTestActorRuntime& runtime,
    std::unordered_set<ui32> innerEventTypes,
    size_t maxBreaks,
    std::optional<ui64> onlyTabletId)
    : Broken_(std::make_shared<std::atomic<size_t>>(0))
{
    auto broken = Broken_;
    auto types = std::make_shared<std::unordered_set<ui32>>(std::move(innerEventTypes));
    auto* rt = &runtime;

    Observer_ = runtime.AddObserver<TEvPipeCache::TEvForward>(
        [rt, broken, types, maxBreaks, onlyTabletId](TEvPipeCache::TEvForward::TPtr& ev) {
            if (!ev || !ev->Get()->Ev) {
                return;
            }
            if (!types->contains(ev->Get()->Ev->Type())) {
                return;
            }
            const ui64 tabletId = ev->Get()->TabletId;
            if (onlyTabletId && tabletId != *onlyTabletId) {
                return;
            }
            if (broken->load() >= maxBreaks) {
                return;
            }

            broken->fetch_add(1);
            const ui64 subscribeCookie = ev->Get()->Options.SubscribeCookie;

            rt->Send(new IEventHandle(
                ev->Sender,
                ev->Recipient,
                new TEvPipeCache::TEvDeliveryProblem(tabletId, true /*notDelivered*/),
                0,
                subscribeCookie));
            ev.Reset();
        });
}

size_t TPipeBreakGuard::BrokenCount() const {
    return Broken_->load();
}

}
