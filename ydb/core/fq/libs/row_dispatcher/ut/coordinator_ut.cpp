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
        Runtime.SetLogPriority(NKikimrServices::YQ_ROW_DISPATCHER, NLog::PRI_TRACE);
        auto credFactory = NKikimr::CreateYdbCredentialsProviderFactory;
        auto yqSharedResources = NFq::TYqSharedResources::Cast(NFq::CreateYqSharedResourcesImpl({}, credFactory, MakeIntrusive<NMonitoring::TDynamicCounters>()));
   
        LocalRowDispatcherId = Runtime.AllocateEdgeActor(0);
        RowDispatcher1Id = Runtime.AllocateEdgeActor(1);
        RowDispatcher2Id = Runtime.AllocateEdgeActor(2);
        ReadActor1 = Runtime.AllocateEdgeActor(0);
        ReadActor2 = Runtime.AllocateEdgeActor(0);

        NConfig::TRowDispatcherCoordinatorConfig config;
        config.SetEnabled(true);
        auto& storage = *config.MutableStorage();
        storage.SetEndpoint("YDB_ENDPOINT");
        storage.SetDatabase("YDB_DATABASE");
        storage.SetToken("");
        storage.SetTablePrefix("tablePrefix");

        Coordinator = Runtime.Register(NewCoordinator(
            LocalRowDispatcherId,
            config,
            yqSharedResources,
            "Tenant",
            MakeIntrusive<NMonitoring::TDynamicCounters>()
            ).release());

        Runtime.EnableScheduleForActor(Coordinator);

        TDispatchOptions options;
        options.FinalEvents.emplace_back(NActors::TEvents::TSystem::Bootstrap, 1);
        Runtime.DispatchEvents(options);
    }

    void TearDown(NUnitTest::TTestContext& /* context */) override {
    }

    NYql::NPq::NProto::TDqPqTopicSource BuildPqTopicSourceSettings(
        TString topic)
    {
        NYql::NPq::NProto::TDqPqTopicSource settings;
        settings.SetTopicPath(topic);
        settings.SetConsumerName("PqConsumer");
        settings.SetEndpoint("Endpoint");
        settings.MutableToken()->SetName("token");
        settings.SetDatabase("Database");
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

    void MockRequest(NActors::TActorId readActorId, TString topicName, const std::vector<ui64>& partitionId) {
        auto event = new NFq::TEvRowDispatcher::TEvCoordinatorRequest(
            BuildPqTopicSourceSettings(topicName),
            partitionId);
        Runtime.Send(new NActors::IEventHandle(Coordinator, readActorId, event));
    }

    NFq::NRowDispatcherProto::TEvGetAddressResponse ExpectResult(NActors::TActorId readActorId/*, std::map<NActors::TActorId, std::set<ui64>> expectedResult*/) {
        auto eventPtr = Runtime.GrabEdgeEvent<NFq::TEvRowDispatcher::TEvCoordinatorResult>(readActorId, TDuration::Seconds(5));
        UNIT_ASSERT(eventPtr.Get() != nullptr);
        NFq::NRowDispatcherProto::TEvGetAddressResponse result;
        result.CopyFrom(eventPtr->Get()->Record);
        return result;
    }

    TActorSystemStub actorSystemStub;
    NActors::TTestActorRuntime Runtime;
    NActors::TActorId Coordinator;
    NActors::TActorId LocalRowDispatcherId;
    NActors::TActorId RowDispatcher1Id;
    NActors::TActorId RowDispatcher2Id;
    NActors::TActorId ReadActor1;
    NActors::TActorId ReadActor2;

    NYql::NPq::NProto::TDqPqTopicSource Source1 = BuildPqTopicSourceSettings("Source1");
};

Y_UNIT_TEST_SUITE(CoordinatorTests) {
    Y_UNIT_TEST_F(Route, TFixture) {

        ExpectCoordinatorChangesSubscribe();

        TSet<NActors::TActorId> rowDispatcherIds{RowDispatcher1Id, RowDispatcher2Id, LocalRowDispatcherId};
        for (auto id : rowDispatcherIds) {
            Ping(id);
        }

        MockRequest(ReadActor1, "topic1", {0});
        auto result1 = ExpectResult(ReadActor1);
        
        MockRequest(ReadActor2, "topic1", {0});
        auto result2 = ExpectResult(ReadActor2);

        UNIT_ASSERT(result1.PartitionsSize() == 1);
        UNIT_ASSERT(result2.PartitionsSize() == 1);
        UNIT_ASSERT(google::protobuf::util::MessageDifferencer::Equals(result1, result2));

        MockRequest(ReadActor2, "topic1", {1});
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

        MockRequest(ReadActor1, "topic1", {0});
        auto result4 = ExpectResult(ReadActor1);

        MockRequest(ReadActor2, "topic1", {1});
        auto result5 = ExpectResult(ReadActor2);

        UNIT_ASSERT(!google::protobuf::util::MessageDifferencer::Equals(result1, result4)
            || !google::protobuf::util::MessageDifferencer::Equals(result3, result5));
    }
}

}

