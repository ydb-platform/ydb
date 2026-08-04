#include <ydb/core/fq/libs/ydb/ydb.h>
#include <ydb/core/fq/libs/events/events.h>
#include <ydb/core/fq/libs/row_dispatcher/row_dispatcher.h>
#include <ydb/core/fq/libs/row_dispatcher/actors_factory.h>
#include <ydb/core/fq/libs/row_dispatcher/events/data_plane.h>
#include <ydb/core/fq/libs/row_dispatcher/events/topic_session_stats.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/testlib/basics/helpers.h>
#include <ydb/core/testlib/actor_helpers.h>
#include <library/cpp/testing/unittest/registar.h>
#include <ydb/library/yql/providers/pq/gateway/native/yql_pq_gateway.h>
#include <ydb/core/kqp/federated_query/kqp_federated_query_helpers.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>

namespace {

using namespace NKikimr;
using namespace NFq;

struct TTestActorFactory : public NFq::NRowDispatcher::IActorFactory {
    TTestActorFactory(NActors::TTestActorRuntime& runtime)
        : Runtime(runtime)
    {}

    NActors::TActorId PopActorId() {
        UNIT_ASSERT(!ActorIds.empty());
        auto result = ActorIds.front();
        ActorIds.pop();
        return result;
    }

    NActors::TActorId RegisterTopicSession(
        const TString& /*readGroup*/,
        const TString& /*topicPath*/,
        const TString& /*endpoint*/,
        const TString& /*database*/,
        const TRowDispatcherSettings& /*config*/,
        const NKikimr::NMiniKQL::IFunctionRegistry* /*functionRegistry*/,
        NActors::TActorId /*rowDispatcherActorId*/,
        NActors::TActorId /*compileServiceActorId*/,
        ui32 /*partitionId*/,
        NYdb::TDriver /*driver*/,
        std::shared_ptr<NYdb::ICredentialsProviderFactory> /*credentialsProviderFactory*/,
        const ::NMonitoring::TDynamicCounterPtr& /*counters*/,
        const ::NMonitoring::TDynamicCounterPtr& /*counters*/,
        const NYql::IPqStaticGateway::TPtr& /*pqGateway*/,
        ui64 /*maxBufferSize*/,
        bool /*enableStreamingQueriesCounters*/) const override {
        auto actorId  = Runtime.AllocateEdgeActor();
        ActorIds.push(actorId);
        return actorId;
    }

    NActors::TTestActorRuntime& Runtime;
    mutable TQueue<NActors::TActorId> ActorIds;
};

class TFixture : public NUnitTest::TBaseFixture {
    const ui64 NodesCount = 2;
public:
    TFixture()
        : Runtime(NodesCount)
        , FunctionRegistry(NKikimr::NMiniKQL::CreateFunctionRegistry(&PrintBackTrace, NKikimr::NMiniKQL::CreateBuiltinRegistry(), false, {}))
    {}

    void SetUp(NUnitTest::TTestContext&) override {
        TIntrusivePtr<TTableNameserverSetup> nameserverTable(new TTableNameserverSetup());
        TPortManager pm;
        for (ui32 i = 0; i < NodesCount; ++i) {
            nameserverTable->StaticNodeTable[Runtime.GetNodeId(i)] = std::pair<TString, ui32>("127.0.0." + std::to_string(i + 1), pm.GetPort(12001 + i));
        }
        const TActorId nameserviceId = GetNameserviceActorId();
        for (ui32 i = 0; i < NodesCount; ++i) {
            TActorSetupCmd nameserviceSetup(CreateNameserverTable(nameserverTable), TMailboxType::Simple, 0);
            Runtime.AddLocalService(nameserviceId, std::move(nameserviceSetup), i);
        }

        TAutoPtr<TAppPrepare> app = new TAppPrepare();
        Runtime.Initialize(app->Unwrap());
        Runtime.SetLogPriority(NKikimrServices::FQ_ROW_DISPATCHER, NLog::PRI_TRACE);
        NConfig::TRowDispatcherConfig config;
        config.SetEnabled(true);
        config.SetSendStatusPeriodSec(1);
        auto& coordinatorConfig = *config.MutableCoordinator();
        coordinatorConfig.SetCoordinationNodePath("RowDispatcher");
        auto& database = *coordinatorConfig.MutableDatabase();
        database.SetEndpoint("YDB_ENDPOINT");
        database.SetDatabase("YDB_DATABASE");
        database.SetToken("");

        auto credFactory = NKikimr::CreateYdbCredentialsProviderFactory;
        auto yqSharedResources = NFq::TYqSharedResources::Cast(NFq::CreateYqSharedResourcesImpl({}, credFactory, MakeIntrusive<NMonitoring::TDynamicCounters>()));
   
        NYql::IStructuredTokenCredentialsFactory::TPtr credentialsFactory = NYql::CreateStructuredTokenCredentialsFactory();
        Coordinator1 = Runtime.AllocateEdgeActor();
        Coordinator2 = Runtime.AllocateEdgeActor();
        EdgeActor = Runtime.AllocateEdgeActor();
        ReadActorId1 = Runtime.AllocateEdgeActor();
        ReadActorId2 = Runtime.AllocateEdgeActor();
        ReadActorId3 = Runtime.AllocateEdgeActor(1);
        TestActorFactory = MakeIntrusive<TTestActorFactory>(Runtime);

        NYql::TPqGatewayServices pqServices(
            yqSharedResources->UserSpaceYdbDriver,
            nullptr,
            credentialsFactory,
            std::make_shared<NYql::TPqGatewayConfig>(),
            nullptr);

        RowDispatcher = Runtime.Register(NewRowDispatcher(
            config,
            NKikimr::CreateYdbCredentialsProviderFactory,
            credentialsFactory,
            "Tenant",
            TestActorFactory,
            FunctionRegistry.Get(),
            MakeIntrusive<NMonitoring::TDynamicCounters>(),
            MakeIntrusive<NMonitoring::TDynamicCounters>(),
            CreatePqNativeGateway(pqServices),
            yqSharedResources->UserSpaceYdbDriver
            ).release());

        Runtime.EnableScheduleForActor(RowDispatcher);

        TDispatchOptions options;
        options.FinalEvents.emplace_back(NActors::TEvents::TSystem::Bootstrap, 1);
        Runtime.DispatchEvents(options);
    }

    void TearDown(NUnitTest::TTestContext& /* context */) override {
    }

    NYql::NPq::NProto::TDqPqTopicSource BuildPqTopicSourceSettings(
        TString endpoint,
        TString database,
        TString topic,
        TString readGroup)
    {
        NYql::NPq::NProto::TDqPqTopicSource settings;
        settings.SetTopicPath(topic);
        settings.SetConsumerName("PqConsumer");
        settings.SetEndpoint(endpoint);
        settings.MutableToken()->SetName("token");
        settings.SetDatabase(database);
        settings.SetReadGroup(readGroup);
        return settings;
    }

    void MockAddSession(const NYql::NPq::NProto::TDqPqTopicSource& source, const std::set<ui32>& partitionIds, TActorId readActorId, ui64 generation = 1) {
        auto event = new NFq::TEvRowDispatcher::TEvStartSession(
            source,
            partitionIds,
            "Token",
            {},         // readOffset,
            0,          // StartingMessageTimestamp;
            "QueryId");
        event->Record.MutableTransportMeta()->SetSeqNo(1);
        Runtime.Send(new IEventHandle(RowDispatcher, readActorId, event, 0, generation));
    }

    void MockStopSession(const NYql::NPq::NProto::TDqPqTopicSource& source, TActorId readActorId) {
        auto event = std::make_unique<NFq::TEvRowDispatcher::TEvStopSession>();
        event->Record.MutableSource()->CopyFrom(source);
        event->Record.MutableTransportMeta()->SetSeqNo(1);
        Runtime.Send(new IEventHandle(RowDispatcher, readActorId, event.release(), 0, 1));
    }

    void MockNoSession(TActorId readActorId, ui64 generation) {
        auto event = std::make_unique<NFq::TEvRowDispatcher::TEvNoSession>();
        Runtime.Send(new IEventHandle(RowDispatcher, readActorId, event.release(), 0, generation));
    }

    void MockNewDataArrived(ui64 partitionId, TActorId topicSessionId, TActorId readActorId) {
        auto event = std::make_unique<NFq::TEvRowDispatcher::TEvNewDataArrived>();
        event->Record.SetPartitionId(partitionId);
        event->ReadActorId = readActorId;
        Runtime.Send(new IEventHandle(RowDispatcher, topicSessionId, event.release()));
    }

    void MockMessageBatch(ui64 partitionId, TActorId topicSessionId, TActorId readActorId, ui64 generation) {
        auto event = std::make_unique<NFq::TEvRowDispatcher::TEvMessageBatch>();
        event->Record.SetPartitionId(partitionId);
        event->ReadActorId = readActorId;
        Runtime.Send(new IEventHandle(RowDispatcher, topicSessionId, event.release(), 0, generation));
    }

    void MockSessionError(TActorId topicSessionId, TActorId readActorId, ui32 partitionId, bool isFatalError = false) {
        auto event = std::make_unique<NFq::TEvRowDispatcher::TEvSessionError>();
        event->ReadActorId = readActorId;
        event->IsFatalError = isFatalError;
        event->Record.SetPartitionId(partitionId);
        Runtime.Send(new IEventHandle(RowDispatcher, topicSessionId, event.release()));
    }
    
    void MockHeartbeat(ui64 partitionId, TActorId readActorId, ui64 generation) {
        auto event = std::make_unique<NFq::TEvRowDispatcher::TEvHeartbeat>();
        event->Record.SetPartitionId(partitionId);
        Runtime.Send(new IEventHandle(RowDispatcher, readActorId, event.release(), 0, generation));
    }

    void MockGetNextBatch(ui64 partitionId, TActorId readActorId, ui64 generation, ui64 seqNo = 2) {
        auto event = std::make_unique<NFq::TEvRowDispatcher::TEvGetNextBatch>();
        event->Record.SetPartitionId(partitionId);
        event->Record.MutableTransportMeta()->SetSeqNo(seqNo);
        Runtime.Send(new IEventHandle(RowDispatcher, readActorId, event.release(), 0, generation));
    }

    void MockUndelivered(TActorId readActorId, ui64 generation) {
        auto event = std::make_unique<NActors::TEvents::TEvUndelivered>(0, NActors::TEvents::TEvUndelivered::ReasonActorUnknown);
        Runtime.Send(new IEventHandle(RowDispatcher, readActorId, event.release(), 0, generation));
    }

    void ExpectStartSession(NActors::TActorId actorId) {
        auto eventHolder = Runtime.GrabEdgeEvent<NFq::TEvRowDispatcher::TEvStartSession>(actorId);
        UNIT_ASSERT(eventHolder.Get() != nullptr);
    }

    void ExpectPoisonPill(NActors::TActorId actorId) {
        auto eventHolder = Runtime.GrabEdgeEvent<NActors::TEvents::TEvPoisonPill>(actorId);
        UNIT_ASSERT(eventHolder.Get() != nullptr);
    }

    void ExpectStopSession(NActors::TActorId actorId) {
        auto eventHolder = Runtime.GrabEdgeEvent<NFq::TEvRowDispatcher::TEvStopSession>(actorId);
        UNIT_ASSERT(eventHolder.Get() != nullptr);
    }

    void ExpectGetNextBatch(NActors::TActorId topicSessionId, ui64 partitionId) {
        auto eventHolder = Runtime.GrabEdgeEvent<NFq::TEvRowDispatcher::TEvGetNextBatch>(topicSessionId);
        UNIT_ASSERT(eventHolder.Get() != nullptr);
        UNIT_ASSERT(eventHolder->Get()->Record.GetPartitionId() == partitionId);
    }

    void ExpectNewDataArrived(NActors::TActorId readActorId, ui64 partitionId) {
        auto eventHolder = Runtime.GrabEdgeEvent<NFq::TEvRowDispatcher::TEvNewDataArrived>(readActorId);
        UNIT_ASSERT(eventHolder.Get() != nullptr);
        UNIT_ASSERT(eventHolder->Get()->Record.GetPartitionId() == partitionId);
    }

    void ExpectStartSessionAck(NActors::TActorId readActorId, ui64 expectedGeneration = 1) {
        auto eventHolder = Runtime.GrabEdgeEvent<NFq::TEvRowDispatcher::TEvStartSessionAck>(readActorId);
        UNIT_ASSERT(eventHolder.Get() != nullptr);
        UNIT_ASSERT(eventHolder->Cookie == expectedGeneration);
    }

    void ExpectMessageBatch(NActors::TActorId readActorId) {
        auto eventHolder = Runtime.GrabEdgeEvent<NFq::TEvRowDispatcher::TEvMessageBatch>(readActorId);
        UNIT_ASSERT(eventHolder.Get() != nullptr);
    }

    void ExpectSessionError(NActors::TActorId readActorId) {
        auto eventHolder = Runtime.GrabEdgeEvent<NFq::TEvRowDispatcher::TEvSessionError>(readActorId);
        UNIT_ASSERT(eventHolder.Get() != nullptr);
    }

    void ExpectNoSession(NActors::TActorId readActorId, ui64 expectedGeneration) {
        auto eventHolder = Runtime.GrabEdgeEvent<NFq::TEvRowDispatcher::TEvNoSession>(readActorId);
        UNIT_ASSERT(eventHolder.Get() != nullptr);
        UNIT_ASSERT(eventHolder->Cookie == expectedGeneration);
    }

    NActors::TActorId ExpectRegisterTopicSession() {
        auto actorId = TestActorFactory->PopActorId();
        return actorId;
    }

    void ProcessData(NActors::TActorId readActorId, ui64 partId, NActors::TActorId topicSessionActorId, ui64 generation = 1, ui64 seqNo = 1) {
        MockNewDataArrived(partId, topicSessionActorId, readActorId);
        ExpectNewDataArrived(readActorId, partId);

        MockGetNextBatch(partId, readActorId, generation, seqNo);
        ExpectGetNextBatch(topicSessionActorId, partId);

        MockMessageBatch(partId, topicSessionActorId, readActorId, generation);
        ExpectMessageBatch(readActorId);
    }

    // Send a mock TEvSessionStatistic from a topic session actor to the RowDispatcher.
    // This simulates the topic session reporting current queue state for a given partition.
    void MockSessionStatistic(
        NActors::TActorId topicSessionId,
        const NYql::NPq::NProto::TDqPqTopicSource& source,
        NActors::TActorId readActorId,
        ui32 partitionId,
        i64 queuedBytes)
    {
        NFq::TTopicSessionStatistic stat;
        stat.SessionKey.ReadGroup = source.GetReadGroup();
        stat.SessionKey.Endpoint = source.GetEndpoint();
        stat.SessionKey.Database = source.GetDatabase();
        stat.SessionKey.TopicPath = source.GetTopicPath();
        stat.SessionKey.PartitionId = partitionId;
        stat.Common.QueuedBytes = queuedBytes;

        NFq::TTopicSessionClientStatistic clientStat;
        clientStat.ReadActorId = readActorId;
        clientStat.PartitionId = partitionId;
        clientStat.QueuedBytes = queuedBytes;
        stat.Clients.push_back(clientStat);

        auto event = std::make_unique<NFq::TEvRowDispatcher::TEvSessionStatistic>(stat);
        Runtime.Send(new IEventHandle(RowDispatcher, topicSessionId, event.release()));
    }

    // Grab the next TEvStatistics event delivered to the given read actor
    // and return the QueuedBytes field from it.
    ui64 ExpectStatisticsQueuedBytes(NActors::TActorId readActorId) {
        auto eventHolder = Runtime.GrabEdgeEvent<NFq::TEvRowDispatcher::TEvStatistics>(
            readActorId, TDuration::Seconds(5));
        UNIT_ASSERT(eventHolder.Get() != nullptr);
        return eventHolder->Get()->Record.GetQueuedBytes();
    }

    TActorSystemStub actorSystemStub;
    NActors::TTestActorRuntime Runtime;
    const NKikimr::NMiniKQL::IFunctionRegistry::TPtr FunctionRegistry;
    NActors::TActorId RowDispatcher;
    NActors::TActorId Coordinator1;
    NActors::TActorId Coordinator2;
    NActors::TActorId EdgeActor;
    NActors::TActorId ReadActorId1;
    NActors::TActorId ReadActorId2;
    NActors::TActorId ReadActorId3;
    TIntrusivePtr<TTestActorFactory> TestActorFactory;

    NYql::NPq::NProto::TDqPqTopicSource Source1 = BuildPqTopicSourceSettings("Endpoint1", "Database1", "topic", "connection_id1");
    NYql::NPq::NProto::TDqPqTopicSource Source2 = BuildPqTopicSourceSettings("Endpoint2", "Database1", "topic", "connection_id1");
    NYql::NPq::NProto::TDqPqTopicSource Source1Connection2 = BuildPqTopicSourceSettings("Endpoint1", "Database1", "topic", "connection_id2");

    ui32 PartitionId0 = 100;
    ui32 PartitionId1 = 101;
};

Y_UNIT_TEST_SUITE(RowDispatcherTests) {
    Y_UNIT_TEST_F(OneClientOneSession, TFixture) {
        MockAddSession(Source1, {PartitionId0}, ReadActorId1);
        auto topicSessionId = ExpectRegisterTopicSession();
        ExpectStartSessionAck(ReadActorId1);
        ExpectStartSession(topicSessionId);

        ProcessData(ReadActorId1, PartitionId0, topicSessionId);

        MockStopSession(Source1, ReadActorId1);
        ExpectStopSession(topicSessionId);
    }

    Y_UNIT_TEST_F(TwoClientOneSession, TFixture) {
        MockAddSession(Source1, {PartitionId0}, ReadActorId1);
        auto topicSessionId = ExpectRegisterTopicSession();
        ExpectStartSessionAck(ReadActorId1);
        ExpectStartSession(topicSessionId);

        MockAddSession(Source1, {PartitionId0}, ReadActorId2);
        ExpectStartSessionAck(ReadActorId2);
        ExpectStartSession(topicSessionId);

        ProcessData(ReadActorId1, PartitionId0, topicSessionId);
        ProcessData(ReadActorId2, PartitionId0, topicSessionId);

        MockSessionError(topicSessionId, ReadActorId1, PartitionId0);
        ExpectSessionError(ReadActorId1);

        MockSessionError(topicSessionId, ReadActorId2, PartitionId0);
        ExpectSessionError(ReadActorId2);
    }

    Y_UNIT_TEST_F(SessionError, TFixture) {
        MockAddSession(Source1, {PartitionId0}, ReadActorId1);
        auto topicSessionId = ExpectRegisterTopicSession();
        ExpectStartSessionAck(ReadActorId1);
        ExpectStartSession(topicSessionId);

        MockSessionError(topicSessionId, ReadActorId1, PartitionId0);
        ExpectSessionError(ReadActorId1);
    }

    Y_UNIT_TEST_F(CoordinatorSubscribe, TFixture) {
        Runtime.Send(new IEventHandle(RowDispatcher, EdgeActor, new NFq::TEvRowDispatcher::TEvCoordinatorChanged(Coordinator1, 10)));
        Runtime.Send(new IEventHandle(RowDispatcher, EdgeActor, new NFq::TEvRowDispatcher::TEvCoordinatorChanged(Coordinator2, 9)));    // ignore

        Runtime.Send(new IEventHandle(RowDispatcher, ReadActorId1, new NFq::TEvRowDispatcher::TEvCoordinatorChangesSubscribe));

        auto eventHolder = Runtime.GrabEdgeEvent<NFq::TEvRowDispatcher::TEvCoordinatorChanged>(ReadActorId1);
        UNIT_ASSERT(eventHolder.Get() != nullptr);
        UNIT_ASSERT(eventHolder->Get()->CoordinatorActorId == Coordinator1);
    }

    Y_UNIT_TEST_F(CoordinatorSubscribeBeforeCoordinatorChanged, TFixture) {
        Runtime.Send(new IEventHandle(RowDispatcher, ReadActorId1, new NFq::TEvRowDispatcher::TEvCoordinatorChangesSubscribe));
        Runtime.Send(new IEventHandle(RowDispatcher, ReadActorId2, new NFq::TEvRowDispatcher::TEvCoordinatorChangesSubscribe));

        Runtime.Send(new IEventHandle(RowDispatcher, EdgeActor, new NFq::TEvRowDispatcher::TEvCoordinatorChanged(Coordinator1, 0)));

        auto eventHolder = Runtime.GrabEdgeEvent<NFq::TEvRowDispatcher::TEvCoordinatorChanged>(ReadActorId1);
        UNIT_ASSERT(eventHolder.Get() != nullptr);
        UNIT_ASSERT(eventHolder->Get()->CoordinatorActorId == Coordinator1);

        eventHolder = Runtime.GrabEdgeEvent<NFq::TEvRowDispatcher::TEvCoordinatorChanged>(ReadActorId2);
        UNIT_ASSERT(eventHolder.Get() != nullptr);
        UNIT_ASSERT(eventHolder->Get()->CoordinatorActorId == Coordinator1);
    }

    Y_UNIT_TEST_F(TwoClients4Sessions, TFixture) {

        MockAddSession(Source1, {PartitionId0, PartitionId1}, ReadActorId1);
        auto topicSession1 = ExpectRegisterTopicSession();
        auto topicSession2 = ExpectRegisterTopicSession();
        ExpectStartSessionAck(ReadActorId1);
        ExpectStartSession(topicSession1);
        ExpectStartSession(topicSession2);

        MockAddSession(Source2, {PartitionId0, PartitionId1}, ReadActorId2);
        auto topicSession3 = ExpectRegisterTopicSession();
        auto topicSession4 = ExpectRegisterTopicSession();
        ExpectStartSessionAck(ReadActorId2);
        ExpectStartSession(topicSession3);
        ExpectStartSession(topicSession4);

        ProcessData(ReadActorId1, PartitionId0, topicSession1);
        ProcessData(ReadActorId1, PartitionId1, topicSession2);
        ProcessData(ReadActorId2, PartitionId0, topicSession3);
        ProcessData(ReadActorId2, PartitionId1, topicSession4);

        MockSessionError(topicSession1, ReadActorId1, PartitionId0);
        ExpectSessionError(ReadActorId1);

        ProcessData(ReadActorId2, PartitionId0, topicSession3);
        ProcessData(ReadActorId2, PartitionId1, topicSession4);
        
        MockStopSession(Source2, ReadActorId2);
        ExpectStopSession(topicSession3);

        MockStopSession(Source2, ReadActorId2);
        ExpectStopSession(topicSession4);

        // Ignore data after StopSession
        MockMessageBatch(PartitionId1, topicSession4, ReadActorId2, 1);
    }

    Y_UNIT_TEST_F(ReinitConsumerIfNewGeneration, TFixture) {
        MockAddSession(Source1, {PartitionId0}, ReadActorId1, 1);
        auto topicSessionId = ExpectRegisterTopicSession();
        ExpectStartSessionAck(ReadActorId1);
        ExpectStartSession(topicSessionId);
        ProcessData(ReadActorId1, PartitionId0, topicSessionId);

        // ignore StartSession with same generation
        MockAddSession(Source1, {PartitionId0}, ReadActorId1, 1);

        // reinit consumer
        MockAddSession(Source1, {PartitionId0}, ReadActorId1, 2);
        ExpectStartSessionAck(ReadActorId1, 2);
    }

    Y_UNIT_TEST_F(HandleTEvUndelivered, TFixture) {
        MockAddSession(Source1, {PartitionId0, PartitionId1}, ReadActorId1, 1);
        auto topicSession1 = ExpectRegisterTopicSession();
        auto topicSession2 = ExpectRegisterTopicSession();
        ExpectStartSessionAck(ReadActorId1, 1);
        ExpectStartSession(topicSession1);
        ExpectStartSession(topicSession2);

        MockAddSession(Source1, {PartitionId0, PartitionId1}, ReadActorId2, 1);
        ExpectStartSessionAck(ReadActorId2, 1);
        ExpectStartSession(topicSession1);
        ExpectStartSession(topicSession2);

        ProcessData(ReadActorId1, PartitionId0, topicSession1, 1);
        ProcessData(ReadActorId1, PartitionId1, topicSession2, 1);
        ProcessData(ReadActorId2, PartitionId0, topicSession1, 1);
        ProcessData(ReadActorId2, PartitionId1, topicSession2, 1);

        MockUndelivered(ReadActorId1, 1);
        ExpectStopSession(topicSession1);
        ExpectStopSession(topicSession2);

        MockUndelivered(ReadActorId2, 1);
        ExpectStopSession(topicSession1);
        ExpectStopSession(topicSession2);
    }

    Y_UNIT_TEST_F(TwoClientTwoConnection, TFixture) {
        MockAddSession(Source1, {PartitionId0}, ReadActorId1);
        auto session1 = ExpectRegisterTopicSession();
        ExpectStartSessionAck(ReadActorId1);
        ExpectStartSession(session1);

        MockAddSession(Source1Connection2, {PartitionId0}, ReadActorId2);
        auto session2 = ExpectRegisterTopicSession();
        ExpectStartSessionAck(ReadActorId2);
        ExpectStartSession(session2);

        ProcessData(ReadActorId1, PartitionId0, session1);
        ProcessData(ReadActorId2, PartitionId0, session2);

        MockStopSession(Source1, ReadActorId1);
        ExpectStopSession(session1);

        MockStopSession(Source1Connection2, ReadActorId2);
        ExpectStopSession(session2);
    }

    Y_UNIT_TEST_F(ProcessNoSession, TFixture) {
        ui64 generation = 42;
        MockAddSession(Source1, {PartitionId0}, ReadActorId3, generation);
        auto topicSessionId = ExpectRegisterTopicSession();
        ExpectStartSessionAck(ReadActorId3, generation);
        ExpectStartSession(topicSessionId);
        ProcessData(ReadActorId3, PartitionId0, topicSessionId, generation, 2);

        MockNoSession(ReadActorId3, generation - 1); // Ignore NoSession with wrong generation.
        ProcessData(ReadActorId3, PartitionId0, topicSessionId, generation, 3);

        MockNoSession(ReadActorId3, generation);
        ExpectStopSession(topicSessionId);
    }

    Y_UNIT_TEST_F(IgnoreWrongPartitionId, TFixture) {
        MockAddSession(Source1, {PartitionId0}, ReadActorId1);
        auto topicSessionId = ExpectRegisterTopicSession();
        ExpectStartSessionAck(ReadActorId1);
        ExpectStartSession(topicSessionId);

        MockNewDataArrived(PartitionId1, topicSessionId, ReadActorId1);

        MockStopSession(Source1, ReadActorId1);
        ExpectStopSession(topicSessionId);
    }

    Y_UNIT_TEST_F(SessionFatalError, TFixture) {
        MockAddSession(Source1, {PartitionId0, PartitionId1}, ReadActorId1);
        auto session0 = ExpectRegisterTopicSession();
        auto session1 = ExpectRegisterTopicSession();
        ExpectStartSessionAck(ReadActorId1);
        ExpectStartSession(session0);
        ExpectStartSession(session1);

        MockAddSession(Source1, {PartitionId0, PartitionId1}, ReadActorId2);
        ExpectStartSessionAck(ReadActorId2);
        ExpectStartSession(session0);
        ExpectStartSession(session1);

        MockSessionError(session0, ReadActorId1, PartitionId0, true);       // consumer (ReadActorId1) deleted
        ExpectSessionError(ReadActorId1);
        ExpectPoisonPill(session0);
        ExpectStopSession(session1);

        // 1 topic session / 1 consumer (ReadActorId2) 

        ProcessData(ReadActorId2, PartitionId1, session1);                  // still working

        MockAddSession(Source1, {PartitionId0, PartitionId1}, ReadActorId1);
        auto new_session0 = ExpectRegisterTopicSession();
        ExpectStartSession(new_session0);
        ExpectStartSession(session1);

        // 2 topic session / 2 consumer 

        MockSessionError(session0, ReadActorId2, PartitionId0, true);      // late event, delete ReadActorId2 consumer
        ExpectSessionError(ReadActorId2);

         // 2 topic session / 1 consumer 

        MockAddSession(Source1, {PartitionId0, PartitionId1}, ReadActorId2);
        ExpectStartSession(new_session0);
        ExpectStartSession(session1);
        ProcessData(ReadActorId1, PartitionId0, new_session0);
        ProcessData(ReadActorId2, PartitionId0, new_session0);
        ProcessData(ReadActorId1, PartitionId1, session1);
        ProcessData(ReadActorId2, PartitionId1, session1);
    }

    Y_UNIT_TEST_F(HeartbeatAfterConsumerDeleted, TFixture) {
        ui64 generation = 1;
        
        MockAddSession(Source1, {PartitionId0}, ReadActorId1, generation);
        auto topicSessionId = ExpectRegisterTopicSession();
        ExpectStartSessionAck(ReadActorId1, generation);
        ExpectStartSession(topicSessionId);
        
        MockSessionError(topicSessionId, ReadActorId1, PartitionId0);

        MockHeartbeat(PartitionId0, ReadActorId1, generation);
        ExpectNoSession(ReadActorId1, generation);
    }

    // Regression test for YQ-5407:
    // When TEvSendStatistic fires, only partitions with StatisticsUpdated=true contribute
    // their QueuedBytes to the aggregated sum sent to the read actor. If a partition has
    // queued data but its StatisticsUpdated flag was cleared in the previous cycle (because
    // it didn't receive a new TEvSessionStatistic), its bytes are dropped from the total,
    // causing query.input_queued_bytes to underreport the actual queue size.
    //
    // The correct behavior: QueuedBytes is a snapshot value and should always be included
    // in the sum regardless of StatisticsUpdated — only incremental fields (FilteredBytes,
    // ReadBytes) should be gated on StatisticsUpdated.
    Y_UNIT_TEST_F(QueuedBytesFromAllPartitionsIncludedInStatistics, TFixture) {
        // Set up a consumer with two partitions.
        MockAddSession(Source1, {PartitionId0, PartitionId1}, ReadActorId1);
        auto topicSession0 = ExpectRegisterTopicSession();  // for PartitionId0 (100)
        auto topicSession1 = ExpectRegisterTopicSession();  // for PartitionId1 (101)
        ExpectStartSessionAck(ReadActorId1);
        ExpectStartSession(topicSession0);
        ExpectStartSession(topicSession1);

        const i64 queuedBytes0 = 100;
        const i64 queuedBytes1 = 50;

        // Both partitions report their current queue state to the RowDispatcher.
        // This sets StatisticsUpdated=true for both partitions in the consumer.
        MockSessionStatistic(topicSession0, Source1, ReadActorId1, PartitionId0, queuedBytes0);
        MockSessionStatistic(topicSession1, Source1, ReadActorId1, PartitionId1, queuedBytes1);

        // Wait for TEvSendStatistic to fire (period = 1 second, configured in SetUp).
        // After this, both partitions will have StatisticsUpdated=false again.
        // The read actor should receive the total: 100 + 50 = 150 bytes.
        Runtime.SimulateSleep(TDuration::Seconds(2));
        ui64 firstQueuedBytes = ExpectStatisticsQueuedBytes(ReadActorId1);
        UNIT_ASSERT_VALUES_EQUAL_C(firstQueuedBytes, queuedBytes0 + queuedBytes1,
            "First statistics send: expected combined queued bytes from both partitions");

        // Now only partition 0 sends a new statistics update (data was consumed from partition 1
        // but we haven't received a new stat update yet — StatisticsUpdated remains false for it).
        MockSessionStatistic(topicSession0, Source1, ReadActorId1, PartitionId0, queuedBytes0);
        // Partition 1 does NOT send a new stat, so StatisticsUpdated stays false for it.
        // Its last-known QueuedBytes (50) should still be reported in the next aggregation.

        // Wait for the second TEvSendStatistic to fire.
        // BUG: Currently reports only 100 (just PartitionId0) because PartitionId1 has
        // StatisticsUpdated=false and gets skipped.
        // EXPECTED (correct): should report 150 = 100 + 50 (both partitions' snapshot values).
        Runtime.SimulateSleep(TDuration::Seconds(2));
        ui64 secondQueuedBytes = ExpectStatisticsQueuedBytes(ReadActorId1);
        UNIT_ASSERT_VALUES_EQUAL_C(secondQueuedBytes, queuedBytes0 + queuedBytes1,
            "Second statistics send: QueuedBytes from all partitions (including ones without "
            "recent updates) must be included — QueuedBytes is a snapshot, not an increment");
    }
}

}

