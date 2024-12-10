#include <ydb/core/base/backtrace.h>

#include <ydb/core/fq/libs/ydb/ydb.h>
#include <ydb/core/fq/libs/events/events.h>

#include <ydb/core/fq/libs/row_dispatcher/topic_session.h>
#include <ydb/core/fq/libs/row_dispatcher/events/data_plane.h>
#include <ydb/core/fq/libs/row_dispatcher/purecalc_compilation/compile_service.h>

#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/testlib/basics/helpers.h>
#include <ydb/core/testlib/actor_helpers.h>
#include <library/cpp/testing/unittest/registar.h>
#include <ydb/tests/fq/pq_async_io/ut_helpers.h>

#include <ydb/library/yql/providers/pq/gateway/native/yql_pq_gateway.h>

#include <yql/essentials/public/purecalc/common/interface.h>

namespace {

using namespace NKikimr;
using namespace NFq;
using namespace NYql::NDq;

const ui64 TimeoutBeforeStartSessionSec = 3;
const ui64 GrabTimeoutSec = 4 * TimeoutBeforeStartSessionSec;

class TPurecalcCompileServiceMock : public NActors::TActor<TPurecalcCompileServiceMock> {
    using TBase = NActors::TActor<TPurecalcCompileServiceMock>;

public:
    TPurecalcCompileServiceMock(TActorId owner)
        : TBase(&TPurecalcCompileServiceMock::StateFunc)
        , Owner(owner)
        , ProgramFactory(NYql::NPureCalc::MakeProgramFactory())
    {}

    STRICT_STFUNC(StateFunc,
        hFunc(TEvRowDispatcher::TEvPurecalcCompileRequest, Handle);
    )

    void Handle(TEvRowDispatcher::TEvPurecalcCompileRequest::TPtr& ev) {
        IProgramHolder::TPtr programHolder = std::move(ev->Get()->ProgramHolder);

        try {
            programHolder->CreateProgram(ProgramFactory);
        } catch (const NYql::NPureCalc::TCompileError& e) {
            UNIT_ASSERT_C(false, "Failed to compile purecalc filter: sql: " << e.GetYql() << ", error: " << e.GetIssues());
        }

        Send(ev->Sender, new TEvRowDispatcher::TEvPurecalcCompileResponse(std::move(programHolder)), 0, ev->Cookie);
        Send(Owner, new NActors::TEvents::TEvPing());
    }

private:
    const TActorId Owner;
    const NYql::NPureCalc::IProgramFactoryPtr ProgramFactory;
};

class TFixture : public NUnitTest::TBaseFixture {
public:
    TFixture()
        : Runtime(true)
    {}

    void SetUp(NUnitTest::TTestContext&) override {
        TAutoPtr<TAppPrepare> app = new TAppPrepare();
        Runtime.Initialize(app->Unwrap());
        Runtime.SetLogPriority(NKikimrServices::FQ_ROW_DISPATCHER, NLog::PRI_TRACE);
        Runtime.SetDispatchTimeout(TDuration::Seconds(5));

        NKikimr::EnableYDBBacktraceFormat();

        ReadActorId1 = Runtime.AllocateEdgeActor();
        ReadActorId2 = Runtime.AllocateEdgeActor();
        ReadActorId3 = Runtime.AllocateEdgeActor();
        RowDispatcherActorId = Runtime.AllocateEdgeActor();
    }

    void Init(const TString& topicPath, ui64 maxSessionUsedMemory = std::numeric_limits<ui64>::max()) {
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
        const auto compileServiceActorId = Runtime.Register(new TPurecalcCompileServiceMock(CompileNotifier));

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
            CreatePqNativeGateway(pqServices),
            16000000
            ).release());
        Runtime.EnableScheduleForActor(TopicSession);

        TDispatchOptions options;
        options.FinalEvents.emplace_back(NActors::TEvents::TSystem::Bootstrap, 1);
        UNIT_ASSERT(Runtime.DispatchEvents(options));
    }

    void TearDown(NUnitTest::TTestContext& /* context */) override {
    }

    void StartSession(TActorId readActorId, const NYql::NPq::NProto::TDqPqTopicSource& source, TMaybe<ui64> readOffset = Nothing(), bool expectedError = false) {
        TMap<ui32, ui64> readOffsets;
        if (readOffset) {
            readOffsets[PartitionId] = *readOffset;
        }
        auto event = new NFq::TEvRowDispatcher::TEvStartSession(
            source,
            {PartitionId},
            "Token",
            readOffsets,
            0,         // StartingMessageTimestamp;
            "QueryId");
        Runtime.Send(new IEventHandle(TopicSession, readActorId, event));

        const auto& predicate = source.GetPredicate();
        if (predicate && !expectedError) {
            // Wait predicate compilation
            const auto ping = Runtime.GrabEdgeEvent<NActors::TEvents::TEvPing>(CompileNotifier);
            UNIT_ASSERT_C(ping, "Compilation is not performed for predicate: " << predicate);
        }
    }

    NYql::NPq::NProto::TDqPqTopicSource BuildSource(TString topic, bool emptyPredicate = false, const TString& consumer = DefaultPqConsumer) {
        NYql::NPq::NProto::TDqPqTopicSource settings;
        settings.SetEndpoint(GetDefaultPqEndpoint());
        settings.SetTopicPath(topic);
        settings.SetConsumerName(consumer);
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
        Runtime.Send(new IEventHandle(TopicSession, readActorId, event.release()));
    }

    void ExpectMessageBatch(NActors::TActorId readActorId, const std::vector<TString>& expected) {
        Runtime.Send(new IEventHandle(TopicSession, readActorId, new TEvRowDispatcher::TEvGetNextBatch()));

        auto eventHolder = Runtime.GrabEdgeEvent<TEvRowDispatcher::TEvMessageBatch>(RowDispatcherActorId, TDuration::Seconds(GrabTimeoutSec));
        UNIT_ASSERT(eventHolder.Get() != nullptr);
        UNIT_ASSERT_VALUES_EQUAL(eventHolder->Get()->ReadActorId, readActorId);
        UNIT_ASSERT_VALUES_EQUAL(expected.size(), eventHolder->Get()->Record.MessagesSize());
        for (size_t i = 0; i < expected.size(); ++i) {
            NFq::NRowDispatcherProto::TEvMessage message = eventHolder->Get()->Record.GetMessages(i);
            std::cerr << "message.GetJson() " << message.GetJson() << std::endl;    
            UNIT_ASSERT_VALUES_EQUAL(expected[i], message.GetJson());
        }
    }

    TString ExpectSessionError(NActors::TActorId readActorId, TMaybe<TString> message = Nothing()) {
        auto eventHolder = Runtime.GrabEdgeEvent<TEvRowDispatcher::TEvSessionError>(RowDispatcherActorId, TDuration::Seconds(GrabTimeoutSec));
        UNIT_ASSERT(eventHolder.Get() != nullptr);
        UNIT_ASSERT_VALUES_EQUAL(eventHolder->Get()->ReadActorId, readActorId);
        if (message) {
            UNIT_ASSERT_STRING_CONTAINS(TString(eventHolder->Get()->Record.GetMessage()), *message);
        }
        return eventHolder->Get()->Record.GetMessage();
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
        return eventHolder->Get()->Record.MessagesSize();
    }

    void ExpectStatistics(TMap<NActors::TActorId, ui64> clients) {
        auto check = [&]() -> bool {
            auto eventHolder = Runtime.GrabEdgeEvent<TEvRowDispatcher::TEvSessionStatistic>(RowDispatcherActorId, TDuration::Seconds(GrabTimeoutSec));
            if (clients.size() !=  eventHolder->Get()->Stat.Clients.size()) {
                return false;
            }
            for (const auto& client : eventHolder->Get()->Stat.Clients) {
                if (!clients.contains(client.ReadActorId)) {
                    return false;
                }
                if (clients[client.ReadActorId] !=  client.Offset) {
                    return false;
                }
            }
            return true;
        };
        auto start = TInstant::Now();
        while (TInstant::Now() - start < TDuration::Seconds(5)) {
            if (check()) {
                return;
            }
=======
    void ExpectStatisticToReadActor(TSet<NActors::TActorId> readActorIds, ui64 expectedNextMessageOffset) {
        size_t count = readActorIds.size();
        for (size_t i = 0; i < count; ++i) {
            auto eventHolder = Runtime.GrabEdgeEvent<TEvRowDispatcher::TEvStatistics>(RowDispatcherActorId, TDuration::Seconds(GrabTimeoutSec));
            UNIT_ASSERT(eventHolder.Get() != nullptr);
            UNIT_ASSERT(readActorIds.contains(eventHolder->Get()->ReadActorId));
            readActorIds.erase(eventHolder->Get()->ReadActorId);
            UNIT_ASSERT_VALUES_EQUAL(eventHolder->Get()->Record.GetNextMessageOffset(), expectedNextMessageOffset);
>>>>>>> upstream/main
        }
        UNIT_ASSERT_C(false, "ExpectStatistics timeout");
    }

    NActors::TTestActorRuntime Runtime;
    TActorSystemStub ActorSystemStub;
    NActors::TActorId TopicSession;
    NActors::TActorId RowDispatcherActorId;
    NActors::TActorId CompileNotifier;
    NYdb::TDriver Driver = NYdb::TDriver(NYdb::TDriverConfig().SetLog(CreateLogBackend("cerr")));
    std::shared_ptr<NYdb::ICredentialsProviderFactory> CredentialsProviderFactory;
    NActors::TActorId ReadActorId1;
    NActors::TActorId ReadActorId2;
    NActors::TActorId ReadActorId3;
    ui32 PartitionId = 0;
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
        ExpectStatistics({{ReadActorId1, 0}});
        StartSession(ReadActorId2, source);
        ExpectStatistics({{ReadActorId1, 0}, {ReadActorId2, 0}});

        std::vector<TString> data = { Json1 };
        PQWrite(data, topicName);
        ExpectNewDataArrived({ReadActorId1, ReadActorId2});
        ExpectMessageBatch(ReadActorId1, { Json1 });
        ExpectMessageBatch(ReadActorId2, { Json1 });
        ExpectStatistics({{ReadActorId1, 1}, {ReadActorId2, 1}});

        data = { Json2 };
        PQWrite(data, topicName);
        ExpectNewDataArrived({ReadActorId1, ReadActorId2});
        ExpectStatistics({{ReadActorId1, 1}, {ReadActorId2, 1}});
        ExpectMessageBatch(ReadActorId1, data);
        ExpectMessageBatch(ReadActorId2, data);
        ExpectStatistics({{ReadActorId1, 2}, {ReadActorId2, 2}});

        auto source2 = BuildSource(topicName, false, "OtherConsumer");
        StartSession(ReadActorId3, source2, Nothing(), true);
        ExpectSessionError(ReadActorId3, "Use the same consumer");

        StopSession(ReadActorId1, source);
        StopSession(ReadActorId2, source);
    }

    Y_UNIT_TEST_F(TwoSessionWithoutPredicate, TFixture) {
        const TString topicName = "twowithoutpredicate";
        PQCreateStream(topicName);
        Init(topicName);
        auto source1 = BuildSource(topicName, true);
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
        ExpectMessageBatch(ReadActorId1, data);

        StartSession(ReadActorId2, source);

        const std::vector<TString> data2 = { Json2 };
        PQWrite(data2, topicName);
        ExpectNewDataArrived({ReadActorId1, ReadActorId2});
        
        ExpectMessageBatch(ReadActorId1, data2);
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
        std::vector<TString> expected1 = { Json2, Json3};
        ExpectMessageBatch(ReadActorId1, expected1);

        std::vector<TString> expected2 = { Json3 };
        ExpectMessageBatch(ReadActorId2, expected2);

        const std::vector<TString> data2 = { Json4 };
        PQWrite(data2, topicName);
        ExpectNewDataArrived({ReadActorId1, ReadActorId2});
        ExpectMessageBatch(ReadActorId1, data2);
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

        ExpectSessionError(ReadActorId1, "INCORRECT_TYPE: The JSON element does not have the requested type.");
        StopSession(ReadActorId1, source);
    }

    Y_UNIT_TEST_F(WrongFieldType, TFixture) {
        const TString topicName = "wrong_field";
        PQCreateStream(topicName);
        Init(topicName);
        auto source = BuildSource(topicName);
        StartSession(ReadActorId1, source);

        const std::vector<TString> data = {"{\"dt\":100}"};
        PQWrite(data, topicName);
        auto error = ExpectSessionError(ReadActorId1);
        UNIT_ASSERT_STRING_CONTAINS(error, "Failed to parse json messages, found 1 missing values");
        UNIT_ASSERT_STRING_CONTAINS(error, "the field (value) has been added by query");
        StopSession(ReadActorId1, source);
    }

    Y_UNIT_TEST_F(RestartSessionIfNewClientWithOffset, TFixture) {
        const TString topicName = "topic6";
        PQCreateStream(topicName);
        Init(topicName);
        auto source = BuildSource(topicName);
        StartSession(ReadActorId1, source);

        const std::vector<TString> data = { Json1, Json2, Json3 }; // offset 0, 1, 2
        PQWrite(data, topicName);
        ExpectNewDataArrived({ReadActorId1});
        ExpectMessageBatch(ReadActorId1, data);

        // Restart topic session.
        StartSession(ReadActorId2, source, 1);
        ExpectNewDataArrived({ReadActorId2});

        PQWrite({ Json4 }, topicName);
        ExpectNewDataArrived({ReadActorId1});

        ExpectMessageBatch(ReadActorId1, { Json4 });
        ExpectMessageBatch(ReadActorId2, { Json2, Json3, Json4 });

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
        UNIT_ASSERT(readMessages == messagesSize);

        // Reading from yds is stopped.
        writeMessages();

        readMessages = ReadMessages(ReadActorId1);
        UNIT_ASSERT(readMessages == 0);
        readMessages = ReadMessages(ReadActorId2);
        UNIT_ASSERT(readMessages == messagesSize);

        Sleep(TDuration::MilliSeconds(100));
        Runtime.DispatchEvents({}, Runtime.GetCurrentTime() - TDuration::MilliSeconds(1));

        readMessages = ReadMessages(ReadActorId1);
        UNIT_ASSERT(readMessages == messagesSize);

        writeMessages();
        StopSession(ReadActorId2, source);      // delete slow client, clear unread buffer
        Sleep(TDuration::MilliSeconds(100));
        Runtime.DispatchEvents({}, Runtime.GetCurrentTime() - TDuration::MilliSeconds(1));

        readMessages = ReadMessages(ReadActorId1);
        UNIT_ASSERT(readMessages == messagesSize);
        StopSession(ReadActorId1, source);
    }

     Y_UNIT_TEST_F(TwoSessionsWithDifferentSchemes, TFixture) {
        const TString topicName = "dif_schemes";
        PQCreateStream(topicName);
        Init(topicName);
        auto source1 = BuildSource(topicName);
        auto source2 = BuildSource(topicName);
        source2.AddColumns("field1");
        source2.AddColumnTypes("[DataType; String]");

        StartSession(ReadActorId1, source1);
        StartSession(ReadActorId2, source2);

        TString json1 = "{\"dt\":101,\"value\":\"value1\", \"field1\":\"field1\"}";
        TString json2 = "{\"dt\":102,\"value\":\"value2\", \"field1\":\"field2\"}";

        PQWrite({ json1, json2 }, topicName);
        ExpectNewDataArrived({ReadActorId1, ReadActorId2});
        ExpectMessageBatch(ReadActorId1, { "{\"dt\":101,\"value\":\"value1\"}", "{\"dt\":102,\"value\":\"value2\"}" });
        ExpectMessageBatch(ReadActorId2, { "{\"dt\":101,\"field1\":\"field1\",\"value\":\"value1\"}", "{\"dt\":102,\"field1\":\"field2\",\"value\":\"value2\"}" });

        auto source3 = BuildSource(topicName);
        source3.AddColumns("field2");
        source3.AddColumnTypes("[DataType; String]");
        auto readActorId3 = Runtime.AllocateEdgeActor();
        StartSession(readActorId3, source3);

        TString json3 = "{\"dt\":103,\"value\":\"value3\", \"field1\":\"value1_field1\", \"field2\":\"value1_field2\"}";
        PQWrite({ json3 }, topicName);
        ExpectNewDataArrived({ReadActorId1, ReadActorId2, readActorId3});
        ExpectMessageBatch(ReadActorId1, { "{\"dt\":103,\"value\":\"value3\"}" });
        ExpectMessageBatch(ReadActorId2, { "{\"dt\":103,\"field1\":\"value1_field1\",\"value\":\"value3\"}" });
        ExpectMessageBatch(readActorId3, { "{\"dt\":103,\"field2\":\"value1_field2\",\"value\":\"value3\"}" });

        StopSession(ReadActorId1, source3);
        StopSession(readActorId3, source3);

        TString json4 = "{\"dt\":104,\"value\":\"value4\", \"field1\":\"value2_field1\", \"field2\":\"value2_field2\"}";
        TString json5 = "{\"dt\":105,\"value\":\"value5\", \"field1\":\"value2_field1\", \"field2\":\"value2_field2\"}";
        PQWrite({ json4, json5 }, topicName);
        ExpectNewDataArrived({ReadActorId2});
        ExpectMessageBatch(ReadActorId2, { "{\"dt\":104,\"field1\":\"value2_field1\",\"value\":\"value4\"}", "{\"dt\":105,\"field1\":\"value2_field1\",\"value\":\"value5\"}" });

        StopSession(ReadActorId1, source1);
        StopSession(ReadActorId2, source2);
    }

     Y_UNIT_TEST_F(TwoSessionsWithDifferentColumnTypes, TFixture) {
        const TString topicName = "dif_types";
        PQCreateStream(topicName);
        Init(topicName);

        auto source1 = BuildSource(topicName);
        source1.AddColumns("field1");
        source1.AddColumnTypes("[OptionalType; [DataType; String]]");
        StartSession(ReadActorId1, source1);

        TString json1 = "{\"dt\":101,\"field1\":null,\"value\":\"value1\"}";
        PQWrite({ json1 }, topicName);
        ExpectNewDataArrived({ReadActorId1});
        ExpectMessageBatch(ReadActorId1, { json1 });

        auto source2 = BuildSource(topicName);
        source2.AddColumns("field1");
        source2.AddColumnTypes("[DataType; String]");
        StartSession(ReadActorId2, source2, Nothing(), true);
        ExpectSessionError(ReadActorId2, "Use the same column type in all queries via RD, current type for column `field1` is [OptionalType; [DataType; String]] (requested type is [DataType; String])");
     }
}

}

