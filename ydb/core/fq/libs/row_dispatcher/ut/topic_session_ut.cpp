#include <ydb/core/fq/libs/ydb/ydb.h>
#include <ydb/core/fq/libs/events/events.h>

#include <ydb/core/fq/libs/row_dispatcher/topic_session.h>
#include <ydb/core/fq/libs/row_dispatcher/events/data_plane.h>

#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/testlib/basics/helpers.h>
#include <ydb/core/testlib/actor_helpers.h>
#include <library/cpp/testing/unittest/registar.h>
#include <ydb/tests/fq/pq_async_io/ut_helpers.h>

namespace {

using namespace NKikimr;
using namespace NFq;
using namespace NYql::NDq;

const ui64 TimeoutBeforeStartSessionSec = 3;

class TFixture : public NUnitTest::TBaseFixture {

public:
    TFixture()
    : Runtime(true) {}

    void SetUp(NUnitTest::TTestContext&) override {
        TAutoPtr<TAppPrepare> app = new TAppPrepare();
        Runtime.Initialize(app->Unwrap());
        Runtime.SetLogPriority(NKikimrServices::YQ_ROW_DISPATCHER, NLog::PRI_DEBUG);

        NYql::ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory;

        ReadActorId1 = Runtime.AllocateEdgeActor();
        ReadActorId2 = Runtime.AllocateEdgeActor();
        RowDispatcherActorId = Runtime.AllocateEdgeActor();
    }

    void Init(const TString& topic) {
        Settings = BuildPqTopicSourceSettings(topic);
        Config.SetTimeoutBeforeStartSessionSec(TimeoutBeforeStartSessionSec);

        TopicSession = Runtime.Register(NewTopicSession(
            Config,
            RowDispatcherActorId,
            0,
            Driver,
            CredentialsProviderFactory
            ).release());
        Runtime.EnableScheduleForActor(TopicSession);

        TDispatchOptions options;
        options.FinalEvents.emplace_back(NActors::TEvents::TSystem::Bootstrap, 1);
        UNIT_ASSERT(Runtime.DispatchEvents(options));
    }

    void TearDown(NUnitTest::TTestContext& /* context */) override {
    }

    void StartSession(TActorId readActorId) {
        auto event = new NFq::TEvRowDispatcher::TEvStartSession(
            Settings,
            PartitionId,
            "Token",
            true,       // AddBearerToToken
            Nothing(),  // readOffset,
            0);         // StartingMessageTimestamp;

        Runtime.Send(new IEventHandle(TopicSession, readActorId, event));
    }

    NYql::NPq::NProto::TDqPqTopicSource BuildPqTopicSourceSettings(TString topic) {
        NYql::NPq::NProto::TDqPqTopicSource settings;
        settings.SetEndpoint(GetDefaultPqEndpoint());
        settings.SetTopicPath(topic);
        settings.SetConsumerName("PqConsumer");
        settings.MutableToken()->SetName("token");
        settings.SetDatabase(GetDefaultPqDatabase());
        settings.AddColumns("dt");
        settings.AddColumns("value");
        settings.AddColumnTypes("UInt64");
        settings.AddColumnTypes("String");
        return settings;
    }

    void StopSession(NActors::TActorId readActorId) {
        auto event = std::make_unique<NFq::TEvRowDispatcher::TEvStopSession>();
        event->Record.MutableSource()->CopyFrom(Settings);
        event->Record.SetPartitionId(PartitionId);
        Runtime.Send(new IEventHandle(TopicSession, readActorId, event.release()));
    }

    void ExpectMessageBatch(NActors::TActorId readActorId, const std::vector<TString>& expected) {
        auto eventHolder = Runtime.GrabEdgeEvent<TEvRowDispatcher::TEvMessageBatch>(RowDispatcherActorId, TDuration::Seconds(10));
        UNIT_ASSERT(eventHolder.Get() != nullptr);
        UNIT_ASSERT(eventHolder->Get()->ReadActorId == readActorId);
        UNIT_ASSERT(expected.size() == eventHolder->Get()->Record.MessagesSize());
        for (size_t i = 0; i < expected.size(); ++i) {
            NFq::NRowDispatcherProto::TEvMessage message = eventHolder->Get()->Record.GetMessages(i);
            std::cerr << "message.GetJson() " << message.GetJson() << std::endl;    
            UNIT_ASSERT(expected[i] == message.GetJson());
        }
    }

    void ExpectNewDataArrived(NActors::TActorId readActorId) {
        auto eventHolder = Runtime.GrabEdgeEvent<TEvRowDispatcher::TEvNewDataArrived>(RowDispatcherActorId, TDuration::Seconds(10));
        UNIT_ASSERT(eventHolder.Get() != nullptr);
        UNIT_ASSERT(eventHolder->Get()->ReadActorId == readActorId);
    }

    TActorSystemStub actorSystemStub;
    NActors::TTestActorRuntime Runtime;
    NActors::TActorId TopicSession;
    NActors::TActorId RowDispatcherActorId;
    NYdb::TDriver Driver = NYdb::TDriver(NYdb::TDriverConfig().SetLog(CreateLogBackend("cerr")));
    std::shared_ptr<NYdb::ICredentialsProviderFactory> CredentialsProviderFactory;
    NYql::NPq::NProto::TDqPqTopicSource Settings;
    NActors::TActorId ReadActorId1;
    NActors::TActorId ReadActorId2;
    ui64 PartitionId = 0;
    NConfig::TRowDispatcherConfig Config;
    const TString TopicName = "ReadFromTopic";
    const TString Json1 = "{\"dt\":100,\"value\":\"value1\"}";
};

Y_UNIT_TEST_SUITE(TopicSessionTests) {
    Y_UNIT_TEST_F(OneSession, TFixture) {
        PQCreateStream(TopicName);
        Init(TopicName);
        StartSession(ReadActorId1);
      //  TDispatchOptions options;
       // Runtime.DispatchEvents(options, TDuration::MilliSeconds(100));
        //Runtime.Send(new IEventHandle(TopicSession, TopicSession, new NTopicSession::TEvPrivate::TEvCreateSession()));

        std::cerr << "---   Sleep   " << std::endl;
     //   Sleep(TDuration::Seconds(4 * TimeoutBeforeStartSessionSec));
        std::cerr << "---   Sleep end  " << std::endl;

        //TDispatchOptions options;
      //  Runtime.DispatchEvents(options, TDuration::MilliSeconds(100));

        const std::vector<TString> data = { Json1 };
        PQWrite(data, TopicName);
        ExpectNewDataArrived(ReadActorId1);
        Runtime.Send(new IEventHandle(TopicSession, ReadActorId1, new TEvRowDispatcher::TEvGetNextBatch()));
        ExpectMessageBatch(ReadActorId1, { Json1 });

        StopSession(ReadActorId1);
    }

    Y_UNIT_TEST_F(SessionError, TFixture) {
    }
    
}

}

