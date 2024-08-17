#include <ydb/core/fq/libs/ydb/ydb.h>
#include <ydb/core/fq/libs/events/events.h>
#include <ydb/core/fq/libs/row_dispatcher/leader_election.h>
#include <ydb/core/fq/libs/row_dispatcher/events/data_plane.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/testlib/basics/helpers.h>
#include <ydb/core/testlib/actor_helpers.h>
#include <library/cpp/testing/unittest/registar.h>

namespace {

using namespace NKikimr;
using namespace NFq;

class TFixture : public NUnitTest::TBaseFixture {

public:
    TFixture()
    : Runtime(1, false) {}

    void SetUp(NUnitTest::TTestContext&) override {
        TAutoPtr<TAppPrepare> app = new TAppPrepare();
        Runtime.Initialize(app->Unwrap());
        Runtime.SetLogPriority(NKikimrServices::YQ_ROW_DISPATCHER, NLog::PRI_DEBUG);
        auto credFactory = NKikimr::CreateYdbCredentialsProviderFactory;
        auto yqSharedResources = NFq::TYqSharedResources::Cast(NFq::CreateYqSharedResourcesImpl({}, credFactory, MakeIntrusive<NMonitoring::TDynamicCounters>()));
   
        RowDispatcher = Runtime.AllocateEdgeActor();
        Coordinator1 = Runtime.AllocateEdgeActor();
        Coordinator2 = Runtime.AllocateEdgeActor();
        Coordinator3 = Runtime.AllocateEdgeActor();

        NConfig::TRowDispatcherCoordinatorConfig config;
        config.SetEnabled(true);
        auto& storage = *config.MutableStorage();
        storage.SetEndpoint(GetEnv("YDB_ENDPOINT"));
        storage.SetDatabase(GetEnv("YDB_DATABASE"));
        storage.SetToken("");
        storage.SetTablePrefix("tablePrefix");
                
        LeaderElection1 = Runtime.Register(NewLeaderElection(
            RowDispatcher,
            Coordinator1,
            config,
            NKikimr::CreateYdbCredentialsProviderFactory,
            yqSharedResources,
            "Tenant"
            ).release());

        LeaderElection2 = Runtime.Register(NewLeaderElection(
            RowDispatcher,
            Coordinator2,
            config,
            NKikimr::CreateYdbCredentialsProviderFactory,
            yqSharedResources,
            "Tenant"
            ).release());


        LeaderElection3 = Runtime.Register(NewLeaderElection(
            RowDispatcher,
            Coordinator3,
            config,
            NKikimr::CreateYdbCredentialsProviderFactory,
            yqSharedResources,
            "Tenant"
            ).release());

        Runtime.EnableScheduleForActor(LeaderElection1);
        Runtime.EnableScheduleForActor(LeaderElection2);
        Runtime.EnableScheduleForActor(LeaderElection3);

        TDispatchOptions options;
        options.FinalEvents.emplace_back(NActors::TEvents::TSystem::Bootstrap, 3);
        Runtime.DispatchEvents(options);
        
    }

    void TearDown(NUnitTest::TTestContext& /* context */) override {
    }

    NActors::TActorId ExpectCoordinatorChanged() {
        auto eventHolder = Runtime.GrabEdgeEvent<NFq::TEvRowDispatcher::TEvCoordinatorChanged>(RowDispatcher/*, TDuration::Seconds(20)*/);
        UNIT_ASSERT(eventHolder.Get() != nullptr);
        return eventHolder.Get()->Get()->CoordinatorActorId;
    }

    TActorSystemStub actorSystemStub;
    NActors::TTestActorRuntime Runtime;
    NActors::TActorId RowDispatcher;
    NActors::TActorId LeaderElection1;
    NActors::TActorId LeaderElection2;
    NActors::TActorId LeaderElection3;
    NActors::TActorId Coordinator1;
    NActors::TActorId Coordinator2;
    NActors::TActorId Coordinator3;
    NActors::TActorId LeaderDetector;
};

Y_UNIT_TEST_SUITE(LeaderElectionTests) {
    Y_UNIT_TEST_F(Test1, TFixture) {

        auto coordinatorId1 = ExpectCoordinatorChanged();
        auto coordinatorId2 = ExpectCoordinatorChanged();
        auto coordinatorId3 = ExpectCoordinatorChanged();
        UNIT_ASSERT(coordinatorId1 == coordinatorId2);
        UNIT_ASSERT(coordinatorId2 == coordinatorId3);

        NActors::TActorId currentLeader;
        NActors::TActorId notActive;
        if (coordinatorId1 == Coordinator1) {
            currentLeader = LeaderElection1;
        } else if (coordinatorId1 == Coordinator2) {
            currentLeader = LeaderElection2;
        } else {
            currentLeader = LeaderElection3;
        }

        Runtime.Send(new IEventHandle(currentLeader, RowDispatcher, new NActors::TEvents::TEvPoisonPill()));
        auto coordinatorId4 = ExpectCoordinatorChanged();
        auto coordinatorId5 = ExpectCoordinatorChanged();
        UNIT_ASSERT(coordinatorId4 == coordinatorId5);
        UNIT_ASSERT(coordinatorId4 != coordinatorId1);

        if (coordinatorId4 == Coordinator1) {
            currentLeader = LeaderElection1;
        } else if (coordinatorId4 == Coordinator2) {
            currentLeader = LeaderElection2;
        } else {
            currentLeader = LeaderElection3;
        }

        Runtime.Send(new IEventHandle(currentLeader, RowDispatcher, new NActors::TEvents::TEvPoisonPill()));
        auto coordinatorId6 = ExpectCoordinatorChanged();
        UNIT_ASSERT(coordinatorId6 != coordinatorId4);
    }
}

}

