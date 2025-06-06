#include <ydb/core/fq/libs/ydb/ydb.h>
#include <ydb/core/fq/libs/events/events.h>
#include <ydb/core/fq/libs/row_dispatcher/row_dispatcher.h>
#include <ydb/core/fq/libs/row_dispatcher/actors_factory.h>
#include <ydb/core/fq/libs/row_dispatcher/events/data_plane.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/testlib/basics/helpers.h>
#include <ydb/core/testlib/actor_helpers.h>
#include <library/cpp/testing/unittest/registar.h>
#include <ydb/library/yql/providers/pq/gateway/native/yql_pq_gateway.h>

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
        const NConfig::TRowDispatcherConfig& /*config*/,
        NActors::TActorId /*rowDispatcherActorId*/,
        NActors::TActorId /*compileServiceActorId*/,
        ui32 /*partitionId*/,
        NYdb::TDriver /*driver*/,
        std::shared_ptr<NYdb::ICredentialsProviderFactory> /*credentialsProviderFactory*/,
        const ::NMonitoring::TDynamicCounterPtr& /*counters*/,
        const ::NMonitoring::TDynamicCounterPtr& /*counters*/,
        const NYql::IPqGateway::TPtr& /*pqGateway*/,
        ui64 /*maxBufferSize*/) const override {
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
    : Runtime(NodesCount) {}

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
        NConfig::TRowDispatcherCoordinatorConfig& coordinatorConfig = *config.MutableCoordinator();
        coordinatorConfig.SetCoordinationNodePath("RowDispatcher");
        auto& database = *coordinatorConfig.MutableDatabase();
        database.SetEndpoint("YDB_ENDPOINT");
        database.SetDatabase("YDB_DATABASE");
        database.SetToken("");

        auto credFactory = NKikimr::CreateYdbCredentialsProviderFactory;
        auto yqSharedResources = NFq::TYqSharedResources::Cast(NFq::CreateYqSharedResourcesImpl({}, credFactory, MakeIntrusive<NMonitoring::TDynamicCounters>()));
   
        NYql::ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory;
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
            nullptr,
            std::make_shared<NYql::TPqGatewayConfig>(),
            nullptr);

        RowDispatcher = Runtime.Register(NewRowDispatcher(
            config,
            NKikimr::CreateYdbCredentialsProviderFactory,
            yqSharedResources,
            credentialsFactory,
            "Tenant",
            TestActorFactory,
            MakeIntrusive<NMonitoring::TDynamicCounters>(),
            MakeIntrusive<NMonitoring::TDynamicCounters>(),
            CreatePqNativeGateway(pqServices)
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

    TActorSystemStub actorSystemStub;
    NActors::TTestActorRuntime Runtime;
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
}

}

