#include <ydb/core/fq/libs/ydb/ydb.h>
#include <ydb/core/fq/libs/events/events.h>
#include <ydb/core/fq/libs/row_dispatcher/row_dispatcher.h>
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
    : Runtime(1) {}

    void SetUp(NUnitTest::TTestContext&) override {
        TAutoPtr<TAppPrepare> app = new TAppPrepare();
        Runtime.Initialize(app->Unwrap());
        Runtime.SetLogPriority(NKikimrServices::YQ_ROW_DISPATCHER, NLog::PRI_DEBUG);
        NConfig::TRowDispatcherConfig config;
        config.SetEnabled(true);
        NConfig::TCommonConfig commonConfig;
        NKikimr::TYdbCredentialsProviderFactory credentialsProviderFactory;
        auto credFactory = NKikimr::CreateYdbCredentialsProviderFactory;
        auto yqSharedResources = NFq::TYqSharedResources::Cast(NFq::CreateYqSharedResourcesImpl({}, credFactory, MakeIntrusive<NMonitoring::TDynamicCounters>()));
   
        NYql::ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory;

        RowDispatcher = Runtime.Register(NewRowDispatcher(
            config,
            commonConfig,
            credentialsProviderFactory,
            yqSharedResources,
            credentialsFactory,
            "Tenant"
            ).release());

        EdgeActor = Runtime.AllocateEdgeActor();
        ReadActor1 = Runtime.AllocateEdgeActor();
        ReadActor2 = Runtime.AllocateEdgeActor();
        Runtime.EnableScheduleForActor(RowDispatcher);

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
        // if (watermarksPeriod) {
        //     settings.MutableWatermarks()->SetEnabled(true);
        //     settings.MutableWatermarks()->SetGranularityUs(watermarksPeriod->MicroSeconds());
        // }
        // settings.MutableWatermarks()->SetIdlePartitionsEnabled(idlePartitionsEnabled);
        // settings.MutableWatermarks()->SetLateArrivalDelayUs(lateArrivalDelay.MicroSeconds());

        return settings;
    }

    void AddSession(const TString& topic, ui64 partitionId, TActorId readActorId) {
        auto event = new NFq::TEvRowDispatcher::TEvAddConsumer(
            BuildPqTopicSourceSettings(topic),
            partitionId,          // partitionId
            "Token",
            true,       // AddBearerToToken
            Nothing(),  // readOffset,
            0);         // StartingMessageTimestamp;

        Runtime.Send(new IEventHandle(RowDispatcher, readActorId, event));
    }


    TActorSystemStub actorSystemStub;
    NActors::TTestActorRuntime Runtime;
    NActors::TActorId RowDispatcher;
    NActors::TActorId EdgeActor;
    NActors::TActorId ReadActor1;
    NActors::TActorId ReadActor2;
};

Y_UNIT_TEST_SUITE(RowDispatcherTests) {
    Y_UNIT_TEST_F(OneClientStartStop, TFixture) {

        auto ev = std::make_unique<NFq::TEvRowDispatcher::TEvCoordinatorChanged>(EdgeActor);
        Runtime.Send(new IEventHandle(RowDispatcher, EdgeActor, ev.release()));

        AddSession("topic", 0, ReadActorId1);
        AddSession("topic", 0, ReadActorId2);
       
    }

}

}

