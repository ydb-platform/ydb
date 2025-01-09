#include <ydb/core/fq/libs/ydb/ydb.h>
#include <ydb/core/fq/libs/events/events.h>

#include <ydb/core/fq/libs/row_dispatcher/topic_session.h>
#include <ydb/core/fq/libs/row_dispatcher/events/data_plane.h>
#include <ydb/core/fq/libs/row_dispatcher/format_handler/ut/common/ut_common.h>

#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/testlib/basics/helpers.h>
#include <ydb/core/testlib/actor_helpers.h>
#include <library/cpp/testing/unittest/registar.h>
#include <ydb/tests/fq/pq_async_io/ut_helpers.h>

#include <ydb/library/yql/providers/pq/gateway/native/yql_pq_gateway.h>

#include <yql/essentials/public/purecalc/common/interface.h>
#include <yql/essentials/public/issue/yql_issue_message.h>

#include <library/cpp/threading/future/async.h>
#include <ydb/tests/fq/pq_async_io/mock_pq_gateway.h>


namespace NFq::NRowDispatcher::NTests {

namespace {

using namespace NKikimr;
using namespace NYql::NDq;

const ui64 TimeoutBeforeStartSessionSec = 3;
const ui64 GrabTimeoutSec = 4 * TimeoutBeforeStartSessionSec;


class TFixture : public NTests::TBaseFixture {
public:
    using TBase = NTests::TBaseFixture;

public:
    void SetUp(NUnitTest::TTestContext& ctx) override {
        TBase::SetUp(ctx);

        ReadActorId1 = Runtime.AllocateEdgeActor();
        ReadActorId2 = Runtime.AllocateEdgeActor();
        ReadActorId3 = Runtime.AllocateEdgeActor();
        RowDispatcherActorId = Runtime.AllocateEdgeActor();
    }

    void Init(const TString& topicPath, ui64 maxSessionUsedMemory = std::numeric_limits<ui64>::max(), bool mockTopicSession = false) {
        TopicPath = topicPath;
        MockTopicSession = mockTopicSession;
        Config.SetTimeoutBeforeStartSessionSec(TimeoutBeforeStartSessionSec);
        Config.SetMaxSessionUsedMemory(maxSessionUsedMemory);
        Config.SetSendStatusPeriodSec(2);
        Config.SetWithoutConsumer(false);

        auto credFactory = NKikimr::CreateYdbCredentialsProviderFactory;
        auto yqSharedResources = NFq::TYqSharedResources::Cast(NFq::CreateYqSharedResourcesImpl({}, credFactory, MakeIntrusive<NMonitoring::TDynamicCounters>()));
   
        NYql::TPqGatewayServices pqServices(
            yqSharedResources->UserSpaceYdbDriver,
            nullptr,
            nullptr,
            std::make_shared<NYql::TPqGatewayConfig>(),
            nullptr);

        CompileNotifier = Runtime.AllocateEdgeActor();
        const auto compileServiceActorId = Runtime.Register(CreatePurecalcCompileServiceMock(CompileNotifier));
        if (mockTopicSession) {
            MockPqGateway = CreateMockPqGateway();
        }

        TopicSession = Runtime.Register(NewTopicSession(
            "read_group",
            topicPath,
            GetDefaultPqEndpoint(),
            GetDefaultPqDatabase(),
            Config,
            RowDispatcherActorId,
            compileServiceActorId,
            0,
            Driver,
            CredentialsProviderFactory,
            MakeIntrusive<NMonitoring::TDynamicCounters>(),
            MakeIntrusive<NMonitoring::TDynamicCounters>(),
            !MockTopicSession ? CreatePqNativeGateway(pqServices) : MockPqGateway,
            16000000
            ).release());
        Runtime.EnableScheduleForActor(TopicSession);

        TDispatchOptions options;
        options.FinalEvents.emplace_back(NActors::TEvents::TSystem::Bootstrap, 1);
        UNIT_ASSERT(Runtime.DispatchEvents(options));
    }

    void StartSession(TActorId readActorId, const NYql::NPq::NProto::TDqPqTopicSource& source, TMaybe<ui64> readOffset = Nothing(), bool expectedError = false) {
        auto event = new NFq::TEvRowDispatcher::TEvStartSession(
            source,
            PartitionId,
            "Token",
            readOffset, // readOffset,
            0,         // StartingMessageTimestamp;
            "QueryId");
        Runtime.Send(new IEventHandle(TopicSession, readActorId, event));

        const auto& predicate = source.GetPredicate();
        if (predicate && !expectedError) {
            // Wait predicate compilation
            const auto ping = Runtime.GrabEdgeEvent<NActors::TEvents::TEvPing>(CompileNotifier);
            UNIT_ASSERT_C(ping, "Compilation is not performed for predicate: " << predicate);
        }
        
        //TVector<TMessage> msgs;
        if (MockTopicSession) {
            MockPqGateway->GetEventQueue(TopicPath)->Push(NYdb::NTopic::TReadSessionEvent::TStartPartitionSessionEvent(nullptr, 0, 0), 0);
        }
    }

    NYql::NPq::NProto::TDqPqTopicSource BuildSource(bool emptyPredicate = false, const TString& consumer = DefaultPqConsumer) {
        NYql::NPq::NProto::TDqPqTopicSource settings;
        settings.SetEndpoint(GetDefaultPqEndpoint());
        settings.SetTopicPath(TopicPath);
        settings.SetConsumerName(consumer);
        settings.SetFormat("json_each_row");
        settings.MutableToken()->SetName("token");
        settings.SetDatabase(GetDefaultPqDatabase());
        settings.AddColumns("dt");
        settings.AddColumns("value");
        settings.AddColumnTypes("[DataType; Uint64]");
        settings.AddColumnTypes("[DataType; String]");
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

    void ExpectMessageBatch(NActors::TActorId readActorId, const TBatch& expected) {
        Runtime.Send(new IEventHandle(TopicSession, readActorId, new TEvRowDispatcher::TEvGetNextBatch()));

        auto eventHolder = Runtime.GrabEdgeEvent<TEvRowDispatcher::TEvMessageBatch>(RowDispatcherActorId, TDuration::Seconds(GrabTimeoutSec));
        UNIT_ASSERT(eventHolder.Get() != nullptr);
        UNIT_ASSERT_VALUES_EQUAL(eventHolder->Get()->ReadActorId, readActorId);
        UNIT_ASSERT_VALUES_EQUAL(1, eventHolder->Get()->Record.MessagesSize());

        NFq::NRowDispatcherProto::TEvMessage message = eventHolder->Get()->Record.GetMessages(0);
        UNIT_ASSERT_VALUES_EQUAL(message.OffsetsSize(), expected.Rows.size());
        CheckMessageBatch(eventHolder->Get()->GetPayload(message.GetPayloadId()), expected);
    }

    void ExpectSessionError(NActors::TActorId readActorId, TStatusCode statusCode, TString message = "") {
        auto eventHolder = Runtime.GrabEdgeEvent<TEvRowDispatcher::TEvSessionError>(RowDispatcherActorId, TDuration::Seconds(GrabTimeoutSec));
        UNIT_ASSERT(eventHolder.Get() != nullptr);
        UNIT_ASSERT_VALUES_EQUAL(eventHolder->Get()->ReadActorId, readActorId);

        const auto& record = eventHolder->Get()->Record;
        NYql::TIssues issues;
        NYql::IssuesFromMessage(record.GetIssues(), issues);
        NTests::CheckError(TStatus::Fail(record.GetStatusCode(), std::move(issues)), statusCode, message);
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
        UNIT_ASSERT_VALUES_EQUAL(eventHolder->Get()->ReadActorId, readActorId);

        size_t numberMessages = 0;
        for (const auto& message : eventHolder->Get()->Record.GetMessages()) {
            numberMessages += message.OffsetsSize();
        }

        return numberMessages;
    }

    void ExpectStatisticToReadActor(TSet<NActors::TActorId> readActorIds, ui64 expectedNextMessageOffset) {
        size_t count = readActorIds.size();
        for (size_t i = 0; i < count; ++i) {
            auto eventHolder = Runtime.GrabEdgeEvent<TEvRowDispatcher::TEvStatistics>(RowDispatcherActorId, TDuration::Seconds(GrabTimeoutSec));
            UNIT_ASSERT(eventHolder.Get() != nullptr);
            UNIT_ASSERT(readActorIds.contains(eventHolder->Get()->ReadActorId));
            readActorIds.erase(eventHolder->Get()->ReadActorId);
            UNIT_ASSERT_VALUES_EQUAL(eventHolder->Get()->Record.GetNextMessageOffset(), expectedNextMessageOffset);
        }
    }

    static TRow JsonMessage(ui64 index) {
        return TRow().AddUint64(100 * index).AddString(TStringBuilder() << "value" << index);
    }

    using TMessageInformation = NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessageInformation; 
    using TMessage = NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage; 

    TMessageInformation MakeNextMessageInformation(size_t offset, size_t uncompressedSize, const TString& messageGroupId = "") { 
        auto now = TInstant::Now(); 
        TMessageInformation msgInfo(
            offset,
            "ProducerId",
            0, //SeqNo_,
            now,
            now,
            MakeIntrusive<NYdb::NTopic::TWriteSessionMeta>(),
            MakeIntrusive<NYdb::NTopic::TMessageMeta>(),
            uncompressedSize,
            messageGroupId
        );
        return msgInfo;
    }

    TMessage MakeNextMessage(const TString& msgBuff, size_t offset) {
        TMessage msg(msgBuff, nullptr, MakeNextMessageInformation(offset, msgBuff.size()), CreatePartitionSession());
        return msg;
    }

    void PQWrite2(
        const std::vector<TString>& sequence,
        const TString& topic,
        const TString& endpoint = GetDefaultPqEndpoint()) {
        if (!MockTopicSession) {
            PQWrite(sequence, topic, endpoint);
        } else {
            static size_t MsgOffset_ = 0; // TODO
            TVector<TMessage> msgs;
            size_t size = 0;
            for (const auto& s : sequence) {
                msgs.emplace_back(MakeNextMessage(s, MsgOffset_));
                MsgOffset_++;
                size += s.size();
            }
            if (!msgs.empty()) {
                MockPqGateway->GetEventQueue(topic)->Push(NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent(msgs, {}, CreatePartitionSession()), size);
            }
        }
    }

    void PassAway() {
        Runtime.Send(new IEventHandle(TopicSession, RowDispatcherActorId, new NActors::TEvents::TEvPoisonPill));
    }

public:
    bool MockTopicSession = false;
    TString TopicPath;
    NActors::TActorId TopicSession;
    NActors::TActorId RowDispatcherActorId;
    NActors::TActorId CompileNotifier;
    NYdb::TDriver Driver = NYdb::TDriver(NYdb::TDriverConfig().SetLog(CreateLogBackend("cerr")));
    std::shared_ptr<NYdb::ICredentialsProviderFactory> CredentialsProviderFactory;
    NActors::TActorId ReadActorId1;
    NActors::TActorId ReadActorId2;
    NActors::TActorId ReadActorId3;
    ui64 PartitionId = 0;
    NConfig::TRowDispatcherConfig Config;
    TIntrusivePtr<IMockPqGateway> MockPqGateway;

    const TString Json1 = "{\"dt\":100,\"value\":\"value1\"}";
    const TString Json2 = "{\"dt\":200,\"value\":\"value2\"}";
    const TString Json3 = "{\"dt\":300,\"value\":\"value3\"}";
    const TString Json4 = "{\"dt\":400,\"value\":\"value4\"}";
};

}  // anonymous namespace

Y_UNIT_TEST_SUITE(TopicSessionTests) {

    Y_UNIT_TEST_F(Two222, TFixture) {
        const TString topicName = "fake_topic";
        Init(topicName, 1000, true);
        auto source = BuildSource();
        StartSession(ReadActorId1, source);

        std::vector<TString> data = { Json1 };
        PQWrite2(data, topicName);
        ExpectNewDataArrived({ReadActorId1});
        ExpectMessageBatch(ReadActorId1, { JsonMessage(1) });

        PassAway();
    }

    Y_UNIT_TEST_F(TwoSessionsWithoutOffsets, TFixture) {
        const TString topicName = "topic1";
        PQCreateStream(topicName);
        Init(topicName);
        auto source = BuildSource();
        StartSession(ReadActorId1, source);
        StartSession(ReadActorId2, source);

        std::vector<TString> data = { Json1 };
        PQWrite(data, topicName);
        ExpectNewDataArrived({ReadActorId1, ReadActorId2});
        ExpectMessageBatch(ReadActorId1, { JsonMessage(1) });
        ExpectMessageBatch(ReadActorId2, { JsonMessage(1) });
        ExpectStatisticToReadActor({ReadActorId1, ReadActorId2}, 1);

        data = { Json2 };
        PQWrite(data, topicName);
        ExpectNewDataArrived({ReadActorId1, ReadActorId2});
        ExpectStatisticToReadActor({ReadActorId1, ReadActorId2}, 1);
        ExpectMessageBatch(ReadActorId1, { JsonMessage(2) });
        ExpectMessageBatch(ReadActorId2, { JsonMessage(2) });
        ExpectStatisticToReadActor({ReadActorId1, ReadActorId2}, 2);

        auto source2 = BuildSource(false, "OtherConsumer");
        StartSession(ReadActorId3, source2, Nothing(), true);
        ExpectSessionError(ReadActorId3, EStatusId::PRECONDITION_FAILED, "Use the same consumer");

        StopSession(ReadActorId1, source);
        StopSession(ReadActorId2, source);
    }

    Y_UNIT_TEST_F(TwoSessionWithoutPredicate, TFixture) {
        const TString topicName = "twowithoutpredicate";
        PQCreateStream(topicName);
        Init(topicName);
        auto source1 = BuildSource(true);
        auto source2 = BuildSource(true);
        StartSession(ReadActorId1, source1);
        StartSession(ReadActorId2, source2);

        const std::vector<TString> data = { Json1 };
        PQWrite(data, topicName);
        ExpectNewDataArrived({ReadActorId1, ReadActorId2});
        Runtime.Send(new IEventHandle(TopicSession, ReadActorId1, new TEvRowDispatcher::TEvGetNextBatch()));
        Runtime.Send(new IEventHandle(TopicSession, ReadActorId2, new TEvRowDispatcher::TEvGetNextBatch()));
        ExpectMessageBatch(ReadActorId1, { JsonMessage(1) });
        ExpectMessageBatch(ReadActorId2, { JsonMessage(1) });

        StopSession(ReadActorId1, source1);
        StopSession(ReadActorId2, source2);
    }

    Y_UNIT_TEST_F(SessionWithPredicateAndSessionWithoutPredicate, TFixture) {
        const TString topicName = "topic2";
        PQCreateStream(topicName);
        Init(topicName);
        auto source1 = BuildSource(false);
        auto source2 = BuildSource(true);
        StartSession(ReadActorId1, source1);
        StartSession(ReadActorId2, source2);

        const std::vector<TString> data = { Json1 };
        PQWrite(data, topicName);
        ExpectNewDataArrived({ReadActorId1, ReadActorId2});
        ExpectMessageBatch(ReadActorId1, { JsonMessage(1) });
        ExpectMessageBatch(ReadActorId2, { JsonMessage(1) });

        StopSession(ReadActorId1, source1);
        StopSession(ReadActorId2, source2);
    }

    Y_UNIT_TEST_F(SecondSessionWithoutOffsetsAfterSessionConnected, TFixture) {
        const TString topicName = "topic3";
        PQCreateStream(topicName);
        Init(topicName);
        auto source = BuildSource();
        StartSession(ReadActorId1, source);

        const std::vector<TString> data = { Json1 };
        PQWrite(data, topicName);
        ExpectNewDataArrived({ReadActorId1});
        ExpectMessageBatch(ReadActorId1, { JsonMessage(1) });

        StartSession(ReadActorId2, source);

        const std::vector<TString> data2 = { Json2 };
        PQWrite(data2, topicName);
        ExpectNewDataArrived({ReadActorId1, ReadActorId2});

        ExpectMessageBatch(ReadActorId1, { JsonMessage(2) });
        ExpectMessageBatch(ReadActorId2, { JsonMessage(2) });

        StopSession(ReadActorId1, source);
        StopSession(ReadActorId2, source);
    }

    Y_UNIT_TEST_F(TwoSessionsWithOffsets, TFixture) {
        const TString topicName = "topic4";
        PQCreateStream(topicName);
        Init(topicName);
        auto source = BuildSource();
        const std::vector<TString> data = { Json1, Json2, Json3};
        PQWrite(data, topicName);

        StartSession(ReadActorId1, source, 1);
        StartSession(ReadActorId2, source, 2);

        ExpectNewDataArrived({ReadActorId1, ReadActorId2});
        TBatch expected1 = { JsonMessage(2), JsonMessage(3) };
        ExpectMessageBatch(ReadActorId1, expected1);

        TBatch expected2 = { JsonMessage(3) };
        ExpectMessageBatch(ReadActorId2, expected2);

        const std::vector<TString> data2 = { Json4 };
        PQWrite(data2, topicName);
        ExpectNewDataArrived({ReadActorId1, ReadActorId2});
        ExpectMessageBatch(ReadActorId1, { JsonMessage(4) });
        ExpectMessageBatch(ReadActorId2, { JsonMessage(4) });

        StopSession(ReadActorId1, source);
        StopSession(ReadActorId2, source);
    }

    Y_UNIT_TEST_F(BadDataSessionError, TFixture) {
        const TString topicName = "topic5";
        PQCreateStream(topicName);
        Init(topicName);
        auto source = BuildSource();
        StartSession(ReadActorId1, source);

        const std::vector<TString> data = { "not json", "noch einmal / nicht json" };
        PQWrite(data, topicName);

        ExpectSessionError(ReadActorId1, EStatusId::BAD_REQUEST, "INCORRECT_TYPE: The JSON element does not have the requested type.");
        StopSession(ReadActorId1, source);
    }

    Y_UNIT_TEST_F(WrongFieldType, TFixture) {
        const TString topicName = "wrong_field";
        PQCreateStream(topicName);
        Init(topicName);

        auto source = BuildSource();
        StartSession(ReadActorId1, source);

        source.AddColumns("field1");
        source.AddColumnTypes("[DataType; String]");
        StartSession(ReadActorId2, source);

        PQWrite({ Json1 }, topicName);
        ExpectNewDataArrived({ReadActorId1});
        ExpectMessageBatch(ReadActorId1, { JsonMessage(1) });
        ExpectSessionError(ReadActorId2, EStatusId::PRECONDITION_FAILED, "Failed to parse json messages, found 1 missing values");

        PQWrite({ Json2 }, topicName);
        ExpectNewDataArrived({ReadActorId1});
        ExpectMessageBatch(ReadActorId1, { JsonMessage(2) });
        ExpectSessionError(ReadActorId2, EStatusId::PRECONDITION_FAILED, "Failed to parse json messages, found 1 missing values");

        StopSession(ReadActorId1, source);
        StopSession(ReadActorId2, source);
    }

    Y_UNIT_TEST_F(RestartSessionIfNewClientWithOffset, TFixture) {
        const TString topicName = "topic6";
        PQCreateStream(topicName);
        Init(topicName);
        auto source = BuildSource();
        StartSession(ReadActorId1, source);

        const std::vector<TString> data = { Json1, Json2, Json3 }; // offset 0, 1, 2
        PQWrite(data, topicName);
        ExpectNewDataArrived({ReadActorId1});
        ExpectMessageBatch(ReadActorId1, { JsonMessage(1), JsonMessage(2), JsonMessage(3) });

        // Restart topic session.
        StartSession(ReadActorId2, source, 1);
        ExpectNewDataArrived({ReadActorId2});

        PQWrite({ Json4 }, topicName);
        ExpectNewDataArrived({ReadActorId1});

        ExpectMessageBatch(ReadActorId1, { JsonMessage(4) });
        ExpectMessageBatch(ReadActorId2, { JsonMessage(2), JsonMessage(3), JsonMessage(4) });

        StopSession(ReadActorId1, source);
        StopSession(ReadActorId2, source);
    }

    Y_UNIT_TEST_F(ReadNonExistentTopic, TFixture) {
        const TString topicName = "topic7";
        Init(topicName);
        auto source = BuildSource();
        StartSession(ReadActorId1, source);
        ExpectSessionError(ReadActorId1, EStatusId::SCHEME_ERROR, "no path");
        StopSession(ReadActorId1, source);
    }

    Y_UNIT_TEST_F(SlowSession, TFixture) {
        const TString topicName = "topic8";
        PQCreateStream(topicName);
        Init(topicName, 40);
        auto source = BuildSource();
        StartSession(ReadActorId1, source);
        StartSession(ReadActorId2, source); // slow session

        size_t messagesSize = 5;
        auto writeMessages = [&]() {
            for (size_t i = 0; i < messagesSize; ++i) {
                const std::vector<TString> data = { Json1 };
                PQWrite(data, topicName);
            }
            Sleep(TDuration::MilliSeconds(100));
            Runtime.DispatchEvents({}, Runtime.GetCurrentTime() - TDuration::MilliSeconds(1));
        };

        writeMessages();
        ExpectNewDataArrived({ReadActorId1, ReadActorId2});

        auto readMessages = ReadMessages(ReadActorId1);
        UNIT_ASSERT_VALUES_EQUAL(readMessages, messagesSize);

        // Reading from yds is stopped.
        writeMessages();

        readMessages = ReadMessages(ReadActorId1);
        UNIT_ASSERT_VALUES_EQUAL(readMessages, 0);
        readMessages = ReadMessages(ReadActorId2);
        UNIT_ASSERT_VALUES_EQUAL(readMessages, messagesSize);

        Sleep(TDuration::MilliSeconds(100));
        Runtime.DispatchEvents({}, Runtime.GetCurrentTime() - TDuration::MilliSeconds(1));

        readMessages = ReadMessages(ReadActorId1);
        UNIT_ASSERT_VALUES_EQUAL(readMessages, messagesSize);

        writeMessages();
        StopSession(ReadActorId2, source);      // delete slow client, clear unread buffer
        Sleep(TDuration::MilliSeconds(100));
        Runtime.DispatchEvents({}, Runtime.GetCurrentTime() - TDuration::MilliSeconds(1));

        readMessages = ReadMessages(ReadActorId1);
        UNIT_ASSERT_VALUES_EQUAL(readMessages, messagesSize);
        StopSession(ReadActorId1, source);
    }

    Y_UNIT_TEST_F(TwoSessionsWithDifferentSchemes, TFixture) {
        const TString topicName = "dif_schemes";
        PQCreateStream(topicName);
        Init(topicName);
        auto source1 = BuildSource();
        auto source2 = BuildSource();
        source2.AddColumns("field1");
        source2.AddColumnTypes("[DataType; String]");

        StartSession(ReadActorId1, source1);
        StartSession(ReadActorId2, source2);

        TString json1 = "{\"dt\":100,\"value\":\"value1\", \"field1\":\"field1\"}";
        TString json2 = "{\"dt\":200,\"value\":\"value2\", \"field1\":\"field2\"}";

        PQWrite({ json1, json2 }, topicName);
        ExpectNewDataArrived({ReadActorId1, ReadActorId2});
        ExpectMessageBatch(ReadActorId1, { JsonMessage(1), JsonMessage(2) });
        ExpectMessageBatch(ReadActorId2, { JsonMessage(1).AddString("field1"), JsonMessage(2).AddString("field2") });

        auto source3 = BuildSource();
        source3.AddColumns("field2");
        source3.AddColumnTypes("[DataType; String]");
        auto readActorId3 = Runtime.AllocateEdgeActor();
        StartSession(readActorId3, source3);

        TString json3 = "{\"dt\":300,\"value\":\"value3\", \"field1\":\"value1_field1\", \"field2\":\"value1_field2\"}";
        PQWrite({ json3 }, topicName);
        ExpectNewDataArrived({ReadActorId1, ReadActorId2, readActorId3});
        ExpectMessageBatch(ReadActorId1, { JsonMessage(3) });
        ExpectMessageBatch(ReadActorId2, { JsonMessage(3).AddString("value1_field1") });
        ExpectMessageBatch(readActorId3, { JsonMessage(3).AddString("value1_field2") });

        StopSession(ReadActorId1, source3);
        StopSession(readActorId3, source3);

        TString json4 = "{\"dt\":400,\"value\":\"value4\", \"field1\":\"value2_field1\", \"field2\":\"value2_field2\"}";
        TString json5 = "{\"dt\":500,\"value\":\"value5\", \"field1\":\"value3_field1\", \"field2\":\"value3_field2\"}";
        PQWrite({ json4, json5 }, topicName);
        ExpectNewDataArrived({ReadActorId2});
        ExpectMessageBatch(ReadActorId2, { JsonMessage(4).AddString("value2_field1"), JsonMessage(5).AddString("value3_field1") });

        StopSession(ReadActorId1, source1);
        StopSession(ReadActorId2, source2);
    }

    Y_UNIT_TEST_F(TwoSessionsWithDifferentColumnTypes, TFixture) {
        const TString topicName = "dif_types";
        PQCreateStream(topicName);
        Init(topicName);

        auto source1 = BuildSource();
        source1.AddColumns("field1");
        source1.AddColumnTypes("[OptionalType; [DataType; String]]");
        StartSession(ReadActorId1, source1);

        TString json1 = "{\"dt\":100,\"field1\":\"str\",\"value\":\"value1\"}";
        PQWrite({ json1 }, topicName);
        ExpectNewDataArrived({ReadActorId1});
        ExpectMessageBatch(ReadActorId1, { JsonMessage(1).AddString("str", true) });

        auto source2 = BuildSource();
        source2.AddColumns("field1");
        source2.AddColumnTypes("[DataType; String]");
        StartSession(ReadActorId2, source2, Nothing(), true);
        ExpectSessionError(ReadActorId2, EStatusId::SCHEME_ERROR, "Use the same column type in all queries via RD, current type for column `field1` is [OptionalType; [DataType; String]] (requested type is [DataType; String])");
    }
}

}  // namespace NFq::NRowDispatcher::NTests
