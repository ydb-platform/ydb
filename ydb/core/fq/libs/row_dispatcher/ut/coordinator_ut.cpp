#include <ydb/core/fq/libs/actors/nodes_manager.h>
#include <ydb/core/fq/libs/actors/nodes_manager_events.h>
#include <ydb/core/fq/libs/ydb/ydb.h>
#include <ydb/core/fq/libs/events/events.h>
#include <ydb/core/fq/libs/row_dispatcher/coordinator.h>
#include <ydb/core/fq/libs/row_dispatcher/events/data_plane.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/testlib/basics/helpers.h>
#include <ydb/core/testlib/actor_helpers.h>
#include <library/cpp/testing/unittest/registar.h>

#include <google/protobuf/util/message_differencer.h>

namespace {

using namespace NKikimr;
using namespace NFq;

class TFixture : public NUnitTest::TBaseFixture {

public:
    TFixture()
    : Runtime(4) {}

    void SetUp(NUnitTest::TTestContext&) override {
        TAutoPtr<TAppPrepare> app = new TAppPrepare();
        Runtime.Initialize(app->Unwrap());
        Runtime.SetLogPriority(NKikimrServices::FQ_ROW_DISPATCHER, NLog::PRI_TRACE);
        auto credFactory = NKikimr::CreateYdbCredentialsProviderFactory;
        auto yqSharedResources = NFq::TYqSharedResources::Cast(NFq::CreateYqSharedResourcesImpl({}, credFactory, MakeIntrusive<NMonitoring::TDynamicCounters>()));
   
        LocalRowDispatcherId = Runtime.AllocateEdgeActor(0);
        RowDispatcher1Id = Runtime.AllocateEdgeActor(1);
        RowDispatcher2Id = Runtime.AllocateEdgeActor(2);
        ReadActor1 = Runtime.AllocateEdgeActor(0);
        ReadActor2 = Runtime.AllocateEdgeActor(0);
        NodesManager = Runtime.AllocateEdgeActor(0);

        NConfig::TRowDispatcherCoordinatorConfig config;
        config.SetCoordinationNodePath("RowDispatcher");
        config.SetTopicPartitionsLimitPerNode(1);
        auto& database = *config.MutableDatabase();
        database.SetEndpoint("YDB_ENDPOINT");
        database.SetDatabase("YDB_DATABASE");
        database.SetToken("");
        config.SetRebalancingTimeoutSec(1);

        Coordinator = Runtime.Register(NewCoordinator(
            LocalRowDispatcherId,
            config,
            yqSharedResources,
            "Tenant",
            MakeIntrusive<NMonitoring::TDynamicCounters>(),
            NodesManager
            ).release());

        Runtime.EnableScheduleForActor(Coordinator);

        TDispatchOptions options;
        options.FinalEvents.emplace_back(NActors::TEvents::TSystem::Bootstrap, 1);
        Runtime.DispatchEvents(options);
    }

    void TearDown(NUnitTest::TTestContext& /* context */) override {
    }

    NYql::NPq::NProto::TDqPqTopicSource BuildPqTopicSourceSettings(
        const TString& endpoint, const TString& readGroup, const TString& topic, const TVector<ui64>& nodeIds)
    {
        NYql::NPq::NProto::TDqPqTopicSource settings;
        settings.SetTopicPath(topic);
        settings.SetConsumerName("PqConsumer");
        settings.SetEndpoint(endpoint);
        settings.MutableToken()->SetName("token");
        settings.SetDatabase("Database");
        settings.SetReadGroup(readGroup);
        for (auto id : nodeIds) {
            settings.AddNodeIds(id);
        }
        return settings;
    }

    void ExpectCoordinatorChangesSubscribe() {
        auto eventHolder = Runtime.GrabEdgeEvent<NFq::TEvRowDispatcher::TEvCoordinatorChangesSubscribe>(LocalRowDispatcherId, TDuration::Seconds(5));
        UNIT_ASSERT(eventHolder.Get() != nullptr);
    }

    void Ping(NActors::TActorId rowDispatcherId) {
        auto event = new NActors::TEvents::TEvPing();
        Runtime.Send(new NActors::IEventHandle(Coordinator, rowDispatcherId, event));

        // TODO: GrabEdgeEvent is not working with events on other nodes ?!
        //auto eventHolder = Runtime.GrabEdgeEvent<NActors::TEvents::TEvPong>(rowDispatcherId, TDuration::Seconds(5));
        //UNIT_ASSERT(eventHolder.Get() != nullptr);
    }

    void MockRequest(NActors::TActorId readActorId, const TString& endpoint, const TString& readGroup, const TString& topicName, const std::vector<ui64>& partitionId,
        const TVector<ui64>& nodeIds = {}) {
        auto event = new NFq::TEvRowDispatcher::TEvCoordinatorRequest(
            BuildPqTopicSourceSettings(endpoint, readGroup, topicName, nodeIds),
            partitionId);
        Runtime.Send(new NActors::IEventHandle(Coordinator, readActorId, event));
    }

    NFq::NRowDispatcherProto::TEvGetAddressResponse ExpectResult(NActors::TActorId readActorId) {
        auto eventPtr = Runtime.GrabEdgeEvent<NFq::TEvRowDispatcher::TEvCoordinatorResult>(readActorId, TDuration::Seconds(5));
        UNIT_ASSERT(eventPtr.Get() != nullptr);
        NFq::NRowDispatcherProto::TEvGetAddressResponse result;
        result.CopyFrom(eventPtr->Get()->Record);
        return result;
    }

    void ExpectDistributionReset(NActors::TActorId readActorId) {
        auto eventPtr = Runtime.GrabEdgeEvent<NFq::TEvRowDispatcher::TEvCoordinatorDistributionReset>(readActorId, TDuration::Seconds(5));
        UNIT_ASSERT(eventPtr.Get() != nullptr);
    }

    void ProcessNodesManagerRequest(ui64 nodesCount) {
        auto eventHolder = Runtime.GrabEdgeEvent<NFq::TEvNodesManager::TEvGetNodesRequest>(NodesManager, TDuration::Seconds(5));
        UNIT_ASSERT(eventHolder.Get() != nullptr);

        auto event = new NFq::TEvNodesManager::TEvGetNodesResponse();
        for (ui64 i = 0; i < nodesCount; ++i) {
            event->NodeIds.push_back(i);
        }
        Runtime.Send(new NActors::IEventHandle(Coordinator, NodesManager, event));
    }

    TActorSystemStub actorSystemStub;
    NActors::TTestActorRuntime Runtime;
    NActors::TActorId Coordinator;
    NActors::TActorId LocalRowDispatcherId;
    NActors::TActorId RowDispatcher1Id;
    NActors::TActorId RowDispatcher2Id;
    NActors::TActorId ReadActor1;
    NActors::TActorId ReadActor2;
    NActors::TActorId NodesManager;
};

Y_UNIT_TEST_SUITE(CoordinatorTests) {
    Y_UNIT_TEST_F(Route, TFixture) {

        ExpectCoordinatorChangesSubscribe();

        ProcessNodesManagerRequest(3);

        TSet<NActors::TActorId> rowDispatcherIds{RowDispatcher1Id, RowDispatcher2Id, LocalRowDispatcherId};
        for (auto id : rowDispatcherIds) {
            Ping(id);
        }

        MockRequest(ReadActor1, "endpoint", "read_group", "topic1", {0});
        auto result1 = ExpectResult(ReadActor1);
        
        MockRequest(ReadActor2, "endpoint", "read_group", "topic1", {0});
        auto result2 = ExpectResult(ReadActor2);

        UNIT_ASSERT(result1.PartitionsSize() == 1);
        UNIT_ASSERT(result2.PartitionsSize() == 1);
        UNIT_ASSERT(google::protobuf::util::MessageDifferencer::Equals(result1, result2));

        MockRequest(ReadActor2, "endpoint", "read_group", "topic1", {1});
        auto result3 = ExpectResult(ReadActor2);

        TActorId actualRowDispatcher1 = ActorIdFromProto(result1.GetPartitions(0).GetActorId());
        TActorId actualRowDispatcher2 = ActorIdFromProto(result2.GetPartitions(0).GetActorId());
        TActorId actualRowDispatcher3 = ActorIdFromProto(result3.GetPartitions(0).GetActorId());

        UNIT_ASSERT(rowDispatcherIds.contains(actualRowDispatcher1));
        UNIT_ASSERT(rowDispatcherIds.contains(actualRowDispatcher2));
        UNIT_ASSERT(rowDispatcherIds.contains(actualRowDispatcher3));
        UNIT_ASSERT(actualRowDispatcher1 != actualRowDispatcher3);

        // RowDispatchers is restarted.
        // Skip Disconnected/Coonnected in test.
        auto newDispatcher1Id = Runtime.AllocateEdgeActor(1);
        Ping(newDispatcher1Id);

        auto newDispatcher2Id = Runtime.AllocateEdgeActor(1);
        Ping(newDispatcher2Id);

        MockRequest(ReadActor1, "endpoint", "read_group", "topic1", {0});
        auto result4 = ExpectResult(ReadActor1);

        MockRequest(ReadActor2, "endpoint", "read_group", "topic1", {1});
        auto result5 = ExpectResult(ReadActor2);

        UNIT_ASSERT(!google::protobuf::util::MessageDifferencer::Equals(result1, result4)
            || !google::protobuf::util::MessageDifferencer::Equals(result3, result5));
    }

    Y_UNIT_TEST_F(RouteTwoTopicWichSameName, TFixture) {
        ExpectCoordinatorChangesSubscribe();
        ProcessNodesManagerRequest(3);
        TSet<NActors::TActorId> rowDispatcherIds{RowDispatcher1Id, RowDispatcher2Id, LocalRowDispatcherId};
        for (auto id : rowDispatcherIds) {
            Ping(id);
        }

        MockRequest(ReadActor1, "endpoint1", "read_group", "topic1", {0, 1, 2});
        ExpectResult(ReadActor1);

        MockRequest(ReadActor2, "endpoint2", "read_group", "topic1", {3});
        ExpectResult(ReadActor2);
    }

    Y_UNIT_TEST_F(WaitNodesConnected, TFixture) {
        ExpectCoordinatorChangesSubscribe();
        ProcessNodesManagerRequest(3);
        Ping(RowDispatcher1Id);

        MockRequest(ReadActor1, "endpoint", "read_group", "topic1", {0});
        Sleep(TDuration::MilliSeconds(1000));
        Ping(RowDispatcher2Id);
        ExpectResult(ReadActor1);
    }

    Y_UNIT_TEST_F(ProcessMappingWithNodeIds, TFixture) {
        ExpectCoordinatorChangesSubscribe();
        ProcessNodesManagerRequest(3);
        TSet<NActors::TActorId> rowDispatcherIds{RowDispatcher1Id, RowDispatcher2Id, LocalRowDispatcherId};
        for (auto id : rowDispatcherIds) {
            Ping(id);
        }

        MockRequest(ReadActor1, "endpoint1", "read_group", "topic1", {0, 1, 2}, {RowDispatcher1Id.NodeId()});
        auto result1 = ExpectResult(ReadActor1);
        UNIT_ASSERT_VALUES_EQUAL(result1.IssuesSize(), 0);
        TActorId actorId = ActorIdFromProto(result1.GetPartitions(0).GetActorId());
        UNIT_ASSERT_VALUES_EQUAL(actorId.NodeId(), RowDispatcher1Id.NodeId());

        MockRequest(ReadActor1, "endpoint1", "read_group2", "topic1", {0, 1, 2}, {RowDispatcher2Id.NodeId()});
        auto result2 = ExpectResult(ReadActor1);
        UNIT_ASSERT_VALUES_EQUAL(result2.IssuesSize(), 0);
        actorId = ActorIdFromProto(result2.GetPartitions(0).GetActorId());
        UNIT_ASSERT_VALUES_EQUAL(actorId.NodeId(), RowDispatcher2Id.NodeId());
    }

    Y_UNIT_TEST_F(RebalanceAfterNewNodeConnected, TFixture) {
        ExpectCoordinatorChangesSubscribe();
        ProcessNodesManagerRequest(1);
        TSet<NActors::TActorId> rowDispatcherIds{LocalRowDispatcherId};
        for (auto id : rowDispatcherIds) {
            Ping(id);
        }
        MockRequest(ReadActor1, "endpoint", "read_group", "topic1", {0});
        auto rdActor1 = ActorIdFromProto(ExpectResult(ReadActor1).GetPartitions(0).GetActorId());
        MockRequest(ReadActor2, "endpoint", "read_group", "topic1", {1});
        auto rdActor2 = ActorIdFromProto(ExpectResult(ReadActor2).GetPartitions(0).GetActorId());
        UNIT_ASSERT_VALUES_EQUAL(rdActor1, rdActor2);

        Ping(RowDispatcher1Id);
        ExpectDistributionReset(ReadActor1);
        ExpectDistributionReset(ReadActor2);

        MockRequest(ReadActor1, "endpoint", "read_group", "topic1", {0});
        rdActor1 = ActorIdFromProto(ExpectResult(ReadActor1).GetPartitions(0).GetActorId());
        MockRequest(ReadActor2, "endpoint", "read_group", "topic1", {1});
        rdActor2 = ActorIdFromProto(ExpectResult(ReadActor2).GetPartitions(0).GetActorId());
        UNIT_ASSERT(rdActor1 != rdActor2);
    }

    Y_UNIT_TEST_F(RebalanceAfterNodeDisconnected, TFixture) {
        ExpectCoordinatorChangesSubscribe();
        ProcessNodesManagerRequest(3);
        TSet<NActors::TActorId> rowDispatcherIds{RowDispatcher1Id, RowDispatcher2Id, LocalRowDispatcherId};
        for (auto id : rowDispatcherIds) {
            Ping(id);
        }
        
        MockRequest(ReadActor1, "endpoint1", "read_group", "topic1", {0, 1, 2});
        auto result1 = ExpectResult(ReadActor1);
        UNIT_ASSERT(result1.PartitionsSize() == 3);

        auto event = new NActors::TEvInterconnect::TEvNodeDisconnected(RowDispatcher2Id.NodeId());
        Runtime.Send(new NActors::IEventHandle(Coordinator, RowDispatcher2Id, event));

        ExpectDistributionReset(ReadActor1);

        MockRequest(ReadActor1, "endpoint1", "read_group", "topic1", {0, 1, 2});
        result1 = ExpectResult(ReadActor1);
        UNIT_ASSERT(result1.PartitionsSize() == 2);
    }
}

}

