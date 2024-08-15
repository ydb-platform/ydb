#include <ydb/core/fq/libs/ydb/ydb.h>
#include <ydb/core/fq/libs/events/events.h>
#include <ydb/core/fq/libs/row_dispatcher/leader_election.h>
#include <ydb/core/fq/libs/row_dispatcher/leader_detector.h>
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
        Coordinator = Runtime.AllocateEdgeActor();
        Cerr << "RowDispatcher id " << RowDispatcher << Endl;
        Cerr << "Coordinator id " << Coordinator << Endl;

        NConfig::TRowDispatcherCoordinatorConfig config;
        config.SetEnabled(true);
        auto& storage = *config.MutableStorage();
        storage.SetEndpoint(GetEnv("YDB_ENDPOINT"));
        storage.SetDatabase(GetEnv("YDB_DATABASE"));
        storage.SetToken("");
        storage.SetTablePrefix("tablePrefix");
                
        LeaderElection1 = Runtime.Register(NewLeaderElection(
            RowDispatcher,
            Coordinator,
            config,
            NKikimr::CreateYdbCredentialsProviderFactory,
            yqSharedResources,
            "Tenant"
            ).release());

        LeaderElection2 = Runtime.Register(NewLeaderElection(
            RowDispatcher,
            Coordinator,
            config,
            NKikimr::CreateYdbCredentialsProviderFactory,
            yqSharedResources,
            "Tenant"
            ).release());

        LeaderDetector = Runtime.Register(NewLeaderDetector(
            RowDispatcher,
            config,
            NKikimr::CreateYdbCredentialsProviderFactory,
            yqSharedResources,
            "Tenant"
            ).release());

        Runtime.EnableScheduleForActor(LeaderElection1);
        Runtime.EnableScheduleForActor(LeaderElection2);
        Runtime.EnableScheduleForActor(LeaderDetector);

        TDispatchOptions options;
        options.FinalEvents.emplace_back(NActors::TEvents::TSystem::Bootstrap, 3);
        Runtime.DispatchEvents(options);
        
    }

    void TearDown(NUnitTest::TTestContext& /* context */) override {
    }

    void ExpectCoordinatorChanged() {
        auto eventHolder = Runtime.GrabEdgeEvent<NFq::TEvRowDispatcher::TEvCoordinatorChanged>(RowDispatcher/*, TDuration::Seconds(20)*/);
        UNIT_ASSERT(eventHolder.Get() != nullptr);
    }

    TActorSystemStub actorSystemStub;
    NActors::TTestActorRuntime Runtime;
    NActors::TActorId RowDispatcher;
    NActors::TActorId LeaderElection1;
    NActors::TActorId LeaderElection2;
    NActors::TActorId Coordinator;
    NActors::TActorId LeaderDetector;
};

Y_UNIT_TEST_SUITE(LeaderElectionTests) {
    Y_UNIT_TEST_F(Test1, TFixture) {

      //  std::this_thread::sleep_for(std::chrono::milliseconds(10000));
        ExpectCoordinatorChanged();
    }

}

}

