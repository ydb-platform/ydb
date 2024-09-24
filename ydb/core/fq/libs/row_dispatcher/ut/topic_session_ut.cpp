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
const ui64 GrabTimeoutSec = 4 * TimeoutBeforeStartSessionSec;

class TFixture : public NUnitTest::TBaseFixture {

public:
    TFixture()
    : Runtime(true) {}

    void SetUp(NUnitTest::TTestContext&) override {
        TAutoPtr<TAppPrepare> app = new TAppPrepare();
        Runtime.Initialize(app->Unwrap());
        Runtime.SetLogPriority(NKikimrServices::FQ_ROW_DISPATCHER, NLog::PRI_TRACE);
        Runtime.SetDispatchTimeout(TDuration::Seconds(5));

        ReadActorId1 = Runtime.AllocateEdgeActor();
        ReadActorId2 = Runtime.AllocateEdgeActor();
        RowDispatcherActorId = Runtime.AllocateEdgeActor();
    }

    void Init(const TString& topicPath, ui64 maxSessionUsedMemory = std::numeric_limits<ui64>::max()) {
        Config.SetTimeoutBeforeStartSessionSec(TimeoutBeforeStartSessionSec);
        Config.SetMaxSessionUsedMemory(maxSessionUsedMemory);
        Config.SetSendStatusPeriodSec(2);
        Config.SetWithoutConsumer(true);

        TopicSession = Runtime.Register(NewTopicSession(
            topicPath,
            Config,
            RowDispatcherActorId,
            0,
            Driver,
            CredentialsProviderFactory,
            MakeIntrusive<NMonitoring::TDynamicCounters>()
            ).release());
        Runtime.EnableScheduleForActor(TopicSession);

        TDispatchOptions options;
        options.FinalEvents.emplace_back(NActors::TEvents::TSystem::Bootstrap, 1);
        UNIT_ASSERT(Runtime.DispatchEvents(options));
    }

    void TearDown(NUnitTest::TTestContext& /* context */) override {
    }

    void StartSession(TActorId readActorId, const NYql::NPq::NProto::TDqPqTopicSource& source, TMaybe<ui64> readOffset = Nothing()) {
        auto event = new NFq::TEvRowDispatcher::TEvStartSession(
            source,
            PartitionId,
            "Token",
            readOffset, // readOffset,
            0,         // StartingMessageTimestamp;
            "QueryId");
        Runtime.Send(new IEventHandle(TopicSession, readActorId, event));
    }

    NYql::NPq::NProto::TDqPqTopicSource BuildSource(TString topic, bool emptyPredicate = false) {
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
        if (!emptyPredicate) {
            settings.SetPredicate("WHERE true");
        }
        return settings;
    }

    void StopSession(NActors::TActorId readActorId, const NYql::NPq::NProto::TDqPqTopicSource& source) {
        auto event = std::make_unique<NFq::TEvRowDispatcher::TEvStopSession>();
        *event->Record.MutableSource() = source;
        event->Record.SetPartitionId(PartitionId);
        Runtime.Send(new IEventHandle(TopicSession, readActorId, event.release()));
    }

    void ExpectMessageBatch(NActors::TActorId readActorId, const std::vector<TString>& expected) {
        auto eventHolder = Runtime.GrabEdgeEvent<TEvRowDispatcher::TEvMessageBatch>(RowDispatcherActorId, TDuration::Seconds(GrabTimeoutSec));
        UNIT_ASSERT(eventHolder.Get() != nullptr);
        UNIT_ASSERT(eventHolder->Get()->ReadActorId == readActorId);
        UNIT_ASSERT(expected.size() == eventHolder->Get()->Record.MessagesSize());
        for (size_t i = 0; i < expected.size(); ++i) {
            NFq::NRowDispatcherProto::TEvMessage message = eventHolder->Get()->Record.GetMessages(i);
            std::cerr << "message.GetJson() " << message.GetJson() << std::endl;    
            UNIT_ASSERT(expected[i] == message.GetJson());
        }
    }

    void ExpectSessionError(NActors::TActorId readActorId, TString message) {
        auto eventHolder = Runtime.GrabEdgeEvent<TEvRowDispatcher::TEvSessionError>(RowDispatcherActorId, TDuration::Seconds(GrabTimeoutSec));
        UNIT_ASSERT(eventHolder.Get() != nullptr);
        UNIT_ASSERT(eventHolder->Get()->ReadActorId == readActorId);
        UNIT_ASSERT(TString(eventHolder->Get()->Record.GetMessage()).Contains(message));
    }

    void ExpectNewDataArrived(TSet<NActors::TActorId> readActorIds) {
        size_t count = readActorIds.size();
        for (size_t i = 0; i < count; ++i) {
            auto eventHolder = Runtime.GrabEdgeEvent<TEvRowDispatcher::TEvNewDataArrived>(RowDispatcherActorId, TDuration::Seconds(GrabTimeoutSec));
            UNIT_ASSERT(eventHolder.Get() != nullptr);
            UNIT_ASSERT(readActorIds.contains(eventHolder->Get()->ReadActorId));
            readActorIds.erase(eventHolder->Get()->ReadActorId);
        }
    }

    size_t ReadMessages(NActors::TActorId readActorId) {
        Runtime.Send(new IEventHandle(TopicSession, readActorId, new TEvRowDispatcher::TEvGetNextBatch()));
        auto eventHolder = Runtime.GrabEdgeEvent<TEvRowDispatcher::TEvMessageBatch>(RowDispatcherActorId, TDuration::Seconds(GrabTimeoutSec));
        UNIT_ASSERT(eventHolder.Get() != nullptr);
        UNIT_ASSERT(eventHolder->Get()->ReadActorId == readActorId);
        return eventHolder->Get()->Record.MessagesSize();
    }

    TActorSystemStub actorSystemStub;
    NActors::TTestActorRuntime Runtime;
    NActors::TActorId TopicSession;
    NActors::TActorId RowDispatcherActorId;
    NYdb::TDriver Driver = NYdb::TDriver(NYdb::TDriverConfig().SetLog(CreateLogBackend("cerr")));
    std::shared_ptr<NYdb::ICredentialsProviderFactory> CredentialsProviderFactory;
    NActors::TActorId ReadActorId1;
    NActors::TActorId ReadActorId2;
    ui64 PartitionId = 0;
    NConfig::TRowDispatcherConfig Config;

    const TString Json1 = "{\"dt\":100,\"value\":\"value1\"}";
    const TString Json2 = "{\"dt\":200,\"value\":\"value2\"}";
    const TString Json3 = "{\"dt\":300,\"value\":\"value3\"}";
    const TString Json4 = "{\"dt\":400,\"value\":\"value4\"}";
};

Y_UNIT_TEST_SUITE(TopicSessionTests) {
    Y_UNIT_TEST_F(TwoSessionsWithoutOffsets, TFixture) {
        const TString topicName = "topic1";
        PQCreateStream(topicName);
        Init(topicName);
        auto source = BuildSource(topicName);
        StartSession(ReadActorId1, source);
        StartSession(ReadActorId2, source);

        const std::vector<TString> data = { Json1 };
        PQWrite(data, topicName);
        ExpectNewDataArrived({ReadActorId1, ReadActorId2});
        Runtime.Send(new IEventHandle(TopicSession, ReadActorId1, new TEvRowDispatcher::TEvGetNextBatch()));
        Runtime.Send(new IEventHandle(TopicSession, ReadActorId2, new TEvRowDispatcher::TEvGetNextBatch()));
        ExpectMessageBatch(ReadActorId1, { Json1 });
        ExpectMessageBatch(ReadActorId2, { Json1 });

        StopSession(ReadActorId1, source);
        StopSession(ReadActorId2, source);
    }

    Y_UNIT_TEST_F(SessionWithPredicateAndSessionWithoutPredicate, TFixture) {
        const TString topicName = "topic2";
        PQCreateStream(topicName);
        Init(topicName);
        auto source1 = BuildSource(topicName, false);
        auto source2 = BuildSource(topicName, true);
        StartSession(ReadActorId1, source1);
        StartSession(ReadActorId2, source2);

        const std::vector<TString> data = { Json1 };
        PQWrite(data, topicName);
        ExpectNewDataArrived({ReadActorId1, ReadActorId2});
        Runtime.Send(new IEventHandle(TopicSession, ReadActorId1, new TEvRowDispatcher::TEvGetNextBatch()));
        Runtime.Send(new IEventHandle(TopicSession, ReadActorId2, new TEvRowDispatcher::TEvGetNextBatch()));
        ExpectMessageBatch(ReadActorId1, { Json1 });
        ExpectMessageBatch(ReadActorId2, { Json1 });

        StopSession(ReadActorId1, source1);
        StopSession(ReadActorId2, source2);
    }

    Y_UNIT_TEST_F(SecondSessionWithoutOffsetsAfterSessionConnected, TFixture) {
        const TString topicName = "topic3";
        PQCreateStream(topicName);
        Init(topicName);
        auto source = BuildSource(topicName);
        StartSession(ReadActorId1, source);

        const std::vector<TString> data = { Json1 };
        PQWrite(data, topicName);
        ExpectNewDataArrived({ReadActorId1});
        Runtime.Send(new IEventHandle(TopicSession, ReadActorId1, new TEvRowDispatcher::TEvGetNextBatch()));
        ExpectMessageBatch(ReadActorId1, data);

        StartSession(ReadActorId2, source);

        const std::vector<TString> data2 = { Json2 };
        PQWrite(data2, topicName);
        ExpectNewDataArrived({ReadActorId1, ReadActorId2});
        
        Runtime.Send(new IEventHandle(TopicSession, ReadActorId1, new TEvRowDispatcher::TEvGetNextBatch()));
        ExpectMessageBatch(ReadActorId1, data2);
        Runtime.Send(new IEventHandle(TopicSession, ReadActorId2, new TEvRowDispatcher::TEvGetNextBatch()));
        ExpectMessageBatch(ReadActorId2, data2);

        StopSession(ReadActorId1, source);
        StopSession(ReadActorId2, source);
    }

    Y_UNIT_TEST_F(TwoSessionsWithOffsets, TFixture) {
        const TString topicName = "topic4";
        PQCreateStream(topicName);
        Init(topicName);
        auto source = BuildSource(topicName);
        const std::vector<TString> data = { Json1, Json2, Json3};
        PQWrite(data, topicName);

        StartSession(ReadActorId1, source, 1);
        StartSession(ReadActorId2, source, 2);

        ExpectNewDataArrived({ReadActorId1, ReadActorId2});
        Runtime.Send(new IEventHandle(TopicSession, ReadActorId1, new TEvRowDispatcher::TEvGetNextBatch()));
        std::vector<TString> expected1 = { Json2, Json3};
        ExpectMessageBatch(ReadActorId1, expected1);

        Runtime.Send(new IEventHandle(TopicSession, ReadActorId2, new TEvRowDispatcher::TEvGetNextBatch()));
        std::vector<TString> expected2 = { Json3 };
        ExpectMessageBatch(ReadActorId2, expected2);

        const std::vector<TString> data2 = { Json4 };
        PQWrite(data2, topicName);
        ExpectNewDataArrived({ReadActorId1, ReadActorId2});
        Runtime.Send(new IEventHandle(TopicSession, ReadActorId1, new TEvRowDispatcher::TEvGetNextBatch()));
        ExpectMessageBatch(ReadActorId1, data2);

        Runtime.Send(new IEventHandle(TopicSession, ReadActorId2, new TEvRowDispatcher::TEvGetNextBatch()));
        ExpectMessageBatch(ReadActorId2, data2);

        StopSession(ReadActorId1, source);
        StopSession(ReadActorId2, source);
    }

    Y_UNIT_TEST_F(BadDataSessionError, TFixture) {
        const TString topicName = "topic5";
        PQCreateStream(topicName);
        Init(topicName);
        auto source = BuildSource(topicName);
        StartSession(ReadActorId1, source);

        const std::vector<TString> data = { "not json", "noch einmal / nicht json" };
        PQWrite(data, topicName);

        ExpectSessionError(ReadActorId1, "Failed to unwrap empty optional");
        StopSession(ReadActorId1, source);
    }

    Y_UNIT_TEST_F(RestartSessionIfNewClientWithOffset, TFixture) {
        const TString topicName = "topic6";
        PQCreateStream(topicName);
        Init(topicName);
        auto source = BuildSource(topicName);
        StartSession(ReadActorId1, source);

        const std::vector<TString> data = { Json1, Json2 }; // offset 0, 1
        PQWrite(data, topicName);
        ExpectNewDataArrived({ReadActorId1});
        Runtime.Send(new IEventHandle(TopicSession, ReadActorId1, new TEvRowDispatcher::TEvGetNextBatch()));
        ExpectMessageBatch(ReadActorId1, data);

        // Restart topic session.
        StartSession(ReadActorId2, source, 1);
        ExpectNewDataArrived({ReadActorId2});

        PQWrite({ Json3 }, topicName);
        ExpectNewDataArrived({ReadActorId1});

        Runtime.Send(new IEventHandle(TopicSession, ReadActorId1, new TEvRowDispatcher::TEvGetNextBatch()));
        ExpectMessageBatch(ReadActorId1, { Json3 });

        Runtime.Send(new IEventHandle(TopicSession, ReadActorId2, new TEvRowDispatcher::TEvGetNextBatch()));
        ExpectMessageBatch(ReadActorId2, { Json2, Json3 });

        StopSession(ReadActorId1, source);
        StopSession(ReadActorId2, source);
    }

    Y_UNIT_TEST_F(ReadNonExistentTopic, TFixture) {
        const TString topicName = "topic7";
        Init(topicName);
        auto source = BuildSource(topicName);
        StartSession(ReadActorId1, source);
        ExpectSessionError(ReadActorId1, "no path");
        StopSession(ReadActorId1, source);
    }

    Y_UNIT_TEST_F(SlowSession, TFixture) {
        const TString topicName = "topic8";
        PQCreateStream(topicName);
        Init(topicName, 50);
        auto source = BuildSource(topicName);
        StartSession(ReadActorId1, source);
        StartSession(ReadActorId2, source);

        size_t messagesSize = 5;
        for (size_t i = 0; i < messagesSize; ++i) {
            const std::vector<TString> data = { Json1 };
            PQWrite(data, topicName);
        }
        ExpectNewDataArrived({ReadActorId1, ReadActorId2});

        auto readMessages = ReadMessages(ReadActorId1);
        UNIT_ASSERT(readMessages == messagesSize);

        // Reading from yds is stopped.

        for (size_t i = 0; i < messagesSize; ++i) {
            const std::vector<TString> data = { Json1 };
            PQWrite(data, topicName);
        }
        Sleep(TDuration::MilliSeconds(100));
        Runtime.DispatchEvents({}, Runtime.GetCurrentTime() - TDuration::MilliSeconds(1));

        readMessages = ReadMessages(ReadActorId1);
        UNIT_ASSERT(readMessages == 0);

        readMessages = ReadMessages(ReadActorId2);
        UNIT_ASSERT(readMessages == messagesSize);

        Sleep(TDuration::MilliSeconds(100));
        Runtime.DispatchEvents({}, Runtime.GetCurrentTime() - TDuration::MilliSeconds(1));

        readMessages = ReadMessages(ReadActorId1);
        UNIT_ASSERT(readMessages == messagesSize);

        readMessages = ReadMessages(ReadActorId2);
        UNIT_ASSERT(readMessages == messagesSize);

        StopSession(ReadActorId1, source);
        StopSession(ReadActorId2, source);
    }
}

}

