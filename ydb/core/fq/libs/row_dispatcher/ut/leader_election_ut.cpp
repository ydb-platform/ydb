#include <ydb/core/fq/libs/ydb/ydb.h>
#include <ydb/core/fq/libs/events/events.h>
#include <ydb/core/fq/libs/row_dispatcher/leader_election.h>
#include <ydb/core/fq/libs/row_dispatcher/events/data_plane.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/testlib/basics/helpers.h>
#include <ydb/core/testlib/actor_helpers.h>
#include <library/cpp/testing/unittest/registar.h>

#include <ydb/core/testlib/test_client.h>

namespace {

using namespace NKikimr;
using namespace NFq;

class TFixture : public NUnitTest::TBaseFixture {

public:
    TFixture() {}
   // : Runtime(1, false) {}

    void SetUp(NUnitTest::TTestContext&) override {
        MsgBusPort = PortManager.GetPort(2134);
        GrpcPort = PortManager.GetPort(2135);
        NKikimrProto::TAuthConfig authConfig;
        ServerSettings = MakeHolder<Tests::TServerSettings>(MsgBusPort, authConfig);
        ServerSettings->NodeCount = 1;
        Server = MakeHolder<Tests::TServer>(*ServerSettings);
        Client = MakeHolder<Tests::TClient>(*ServerSettings);
        Server->GetRuntime()->SetLogPriority(NKikimrServices::FQ_ROW_DISPATCHER, NActors::NLog::PRI_DEBUG);
        Server->EnableGRpc(GrpcPort);
        Client->InitRootScheme();

        // TAutoPtr<TAppPrepare> app = new TAppPrepare();
        // Runtime.Initialize(app->Unwrap());
        // Runtime.SetLogPriority(NKikimrServices::FQ_ROW_DISPATCHER, NLog::PRI_DEBUG);
         auto credFactory = NKikimr::CreateYdbCredentialsProviderFactory;
         YqSharedResources = NFq::TYqSharedResources::Cast(NFq::CreateYqSharedResourcesImpl({}, credFactory, MakeIntrusive<NMonitoring::TDynamicCounters>()));
   
         RowDispatcher = Server->GetRuntime()->AllocateEdgeActor();
         Coordinator1 = Server->GetRuntime()->AllocateEdgeActor();
         Coordinator2 = Server->GetRuntime()->AllocateEdgeActor();
         Coordinator3 = Server->GetRuntime()->AllocateEdgeActor();
    }

    void Init(bool localMode = false) {
        NFq::NConfig::TRowDispatcherCoordinatorConfig config;
        config.SetCoordinationNodePath("row_dispatcher");
        config.SetLocalMode(localMode);
        auto& database = *config.MutableDatabase();
        database.SetEndpoint(GetEnv("YDB_ENDPOINT"));
        database.SetDatabase(Server->GetRuntime()->GetAppData().TenantName);
        database.SetToken("");
                
        LeaderElection1 = Server->GetRuntime()->Register(NewLeaderElection(
            RowDispatcher,
            Coordinator1,
            config,
            NKikimr::CreateYdbCredentialsProviderFactory,
            YqSharedResources->UserSpaceYdbDriver,
            "/tenant",
            MakeIntrusive<NMonitoring::TDynamicCounters>()
            ).release());

        LeaderElection2 = Server->GetRuntime()->Register(NewLeaderElection(
            RowDispatcher,
            Coordinator2,
            config,
            NKikimr::CreateYdbCredentialsProviderFactory,
            YqSharedResources->UserSpaceYdbDriver,
            "/tenant",
            MakeIntrusive<NMonitoring::TDynamicCounters>()
            ).release());

        LeaderElection3 = Server->GetRuntime()->Register(NewLeaderElection(
            RowDispatcher,
            Coordinator3,
            config,
            NKikimr::CreateYdbCredentialsProviderFactory,
            YqSharedResources->UserSpaceYdbDriver,
            "/tenant",
            MakeIntrusive<NMonitoring::TDynamicCounters>()
            ).release());

        // Runtime.EnableScheduleForActor(LeaderElection1);
        // Runtime.EnableScheduleForActor(LeaderElection2);
        // Runtime.EnableScheduleForActor(LeaderElection3);

        // TDispatchOptions options;
        // options.FinalEvents.emplace_back(NActors::TEvents::TSystem::Bootstrap, 3);
        // Runtime.DispatchEvents(options);
    }

    void TearDown(NUnitTest::TTestContext& /* context */) override {
    }

    NActors::TActorId ExpectCoordinatorChanged() {
        auto eventHolder = Server->GetRuntime()->GrabEdgeEvent<NFq::TEvRowDispatcher::TEvCoordinatorChanged>(RowDispatcher);
        UNIT_ASSERT(eventHolder.Get() != nullptr);
        return eventHolder.Get()->Get()->CoordinatorActorId;
    }

 //   TActorSystemStub actorSystemStub;
  //  NActors::TTestActorRuntime Runtime;
    NActors::TActorId RowDispatcher;
    NActors::TActorId LeaderElection1;
    NActors::TActorId LeaderElection2;
    NActors::TActorId LeaderElection3;
    NActors::TActorId Coordinator1;
    NActors::TActorId Coordinator2;
    NActors::TActorId Coordinator3;
    NActors::TActorId LeaderDetector;
    TYqSharedResources::TPtr YqSharedResources;

    TPortManager PortManager;
    ui16 MsgBusPort = 0;
    ui16 GrpcPort = 0;
    THolder<Tests::TServerSettings> ServerSettings;
    THolder<Tests::TServer> Server;
    THolder<Tests::TClient> Client;
};

Y_UNIT_TEST_SUITE(LeaderElectionTests) {
    Y_UNIT_TEST_F(Test1, TFixture) {
        Init();

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

        Server->GetRuntime()->Send(new IEventHandle(currentLeader, RowDispatcher, new NActors::TEvents::TEvPoisonPill()));
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

        Server->GetRuntime()->Send(new IEventHandle(currentLeader, RowDispatcher, new NActors::TEvents::TEvPoisonPill()));
        auto coordinatorId6 = ExpectCoordinatorChanged();
        UNIT_ASSERT(coordinatorId6 != coordinatorId4);
    }

    Y_UNIT_TEST_F(TestLocalMode, TFixture) {
        Init(true);
        auto coordinatorId1 = ExpectCoordinatorChanged();
        auto coordinatorId2 = ExpectCoordinatorChanged();
        auto coordinatorId3 = ExpectCoordinatorChanged();
        TSet<NActors::TActorId> set {coordinatorId1, coordinatorId2, coordinatorId3};
        UNIT_ASSERT(set.size() == 3);
    }
}

}
