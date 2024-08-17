#include <ydb/core/fq/libs/ydb/ydb.h>
#include <ydb/core/fq/libs/events/events.h>
#include <ydb/core/fq/libs/row_dispatcher/row_dispatcher.h>
#include <ydb/core/fq/libs/row_dispatcher/actors_factory.h>
#include <ydb/core/fq/libs/row_dispatcher/events/data_plane.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/testlib/basics/helpers.h>
#include <ydb/core/testlib/actor_helpers.h>
#include <library/cpp/testing/unittest/registar.h>

namespace {

using namespace NKikimr;
using namespace NFq;

struct TTestActorFactory : public NFq::NRowDispatcher::IActorFactory {
    TTestActorFactory(NActors::TTestActorRuntime& runtime, NActors::TActorId edge)
        : Runtime(runtime)
        , Edge(edge)
    {}

    NActors::TActorId GetActorId() {
        UNIT_ASSERT(!ActorIds.empty());
        auto result = ActorIds.front();
        ActorIds.pop();
        return result;
    }

    NActors::TActorId RegisterTopicSession(
        const NConfig::TRowDispatcherConfig& /*config*/,
        NActors::TActorId /*rowDispatcherActorId*/,
        ui32 /*partitionId*/,
        NYdb::TDriver /*driver*/,
        std::shared_ptr<NYdb::ICredentialsProviderFactory> /*credentialsProviderFactory*/) const override {
        auto actorId  = Runtime.AllocateEdgeActor();
        ActorIds.push(actorId);
        Cerr << "RegisterTopicSession , actor id " << actorId << Endl;
        return actorId;
    }

    NActors::TTestActorRuntime& Runtime;
    mutable TQueue<NActors::TActorId> ActorIds;
    NActors::TActorId Edge;
};

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
        EdgeActor = Runtime.AllocateEdgeActor();
        ReadActorId1 = Runtime.AllocateEdgeActor();
        ReadActorId2 = Runtime.AllocateEdgeActor();


        TestActorFactory = MakeIntrusive<TTestActorFactory>(Runtime, EdgeActor);

        RowDispatcher = Runtime.Register(NewRowDispatcher(
            config,
            commonConfig,
            credentialsProviderFactory,
            yqSharedResources,
            credentialsFactory,
            "Tenant",
            TestActorFactory
            ).release());

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
        return settings;
    }

    void AddSession(const TString& topic, ui64 partitionId, TActorId readActorId) {
        auto event = new NFq::TEvRowDispatcher::TEvStartSession(
            BuildPqTopicSourceSettings(topic),
            partitionId,          // partitionId
            "Token",
            true,       // AddBearerToToken
            Nothing(),  // readOffset,
            0);         // StartingMessageTimestamp;

        Runtime.Send(new IEventHandle(RowDispatcher, readActorId, event));
    }

    void ExpectStartSession(NActors::TActorId actorId) {
        auto eventHolder = Runtime.GrabEdgeEvent<NFq::TEvRowDispatcher::TEvStartSession>(actorId/*, TDuration::Seconds(20)*/);
        UNIT_ASSERT(eventHolder.Get() != nullptr);
    }

    void ExpectStartSessionAck(NActors::TActorId actorId) {
        auto eventHolder = Runtime.GrabEdgeEvent<NFq::TEvRowDispatcher::TEvStartSessionAck>(actorId/*, TDuration::Seconds(20)*/);
        UNIT_ASSERT(eventHolder.Get() != nullptr);
    }

    NActors::TActorId ExpectRegisterTopicSession() {
        auto actorId = TestActorFactory->GetActorId();
        return actorId;
    }

    TActorSystemStub actorSystemStub;
    NActors::TTestActorRuntime Runtime;
    NActors::TActorId RowDispatcher;
    NActors::TActorId EdgeActor;
    NActors::TActorId ReadActorId1;
    NActors::TActorId ReadActorId2;
    TIntrusivePtr<TTestActorFactory> TestActorFactory;
};

Y_UNIT_TEST_SUITE(RowDispatcherTests) {
    Y_UNIT_TEST_F(OneClientStartStop, TFixture) {

        auto ev = std::make_unique<NFq::TEvRowDispatcher::TEvCoordinatorChanged>(EdgeActor);
        Runtime.Send(new IEventHandle(RowDispatcher, EdgeActor, ev.release()));

        AddSession("topic", 0, ReadActorId1);
        auto topicSessionId = ExpectRegisterTopicSession();
        ExpectStartSessionAck(ReadActorId1);
        ExpectStartSession(topicSessionId);

        AddSession("topic", 0, ReadActorId2);
        ExpectStartSessionAck(ReadActorId2);
        ExpectStartSession(topicSessionId);
    }


}

}

