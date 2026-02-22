#include <ydb/library/testlib/common/test_with_actor_system.h>
#include <ydb/library/testlib/pq_helpers/mock_pq_gateway.h>
#include <ydb/library/yql/providers/pq/async_io/dq_pq_info_aggregation_actor.h>
#include <ydb/library/yql/providers/pq/gateway/clients/composite/yql_pq_composite_read_session.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/read_session.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/cast.h>

#include <optional>

namespace NYql {

using namespace NActors;
using namespace NTestUtils;
using namespace NYdb::NTopic;
using namespace NYql::NDq;

namespace {

struct TEvCompositeSessionTest {
    enum EEv : ui32 {
        EvBegin = EventSpaceBegin(TEvents::ES_USERSPACE),
        EvCreateSession = EvBegin,
        EvSessionCreated,
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TEvents::ES_USERSPACE), "event space overflow");

    struct TEvCreateSession : public TEventLocal<TEvCreateSession, EvCreateSession> {
        IMockPqGateway* Gateway = nullptr;
        TCompositeTopicReadSessionSettings Settings;
        TActorId ReplyTo;

        TEvCreateSession(IMockPqGateway* gateway, TCompositeTopicReadSessionSettings settings, TActorId replyTo)
            : Gateway(gateway)
            , Settings(std::move(settings))
            , ReplyTo(replyTo)
        {}
    };

    struct TEvSessionCreated : public TEventLocal<TEvSessionCreated, EvSessionCreated> {
        std::shared_ptr<IReadSession> Session;
        ICompositeTopicReadSessionControl::TPtr Control;

        TEvSessionCreated(std::shared_ptr<IReadSession> session, ICompositeTopicReadSessionControl::TPtr control)
            : Session(std::move(session))
            , Control(std::move(control))
        {}
    };
};

class TCompositeSessionCreatorActor : public TActorBootstrapped<TCompositeSessionCreatorActor> {
public:
    void Bootstrap() {
        Become(&TThis::StateFunc);
    }

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvCompositeSessionTest::TEvCreateSession, HandleCreate);
        }
    }

    void HandleCreate(TEvCompositeSessionTest::TEvCreateSession::TPtr& ev) {
        Y_ABORT_UNLESS(ev->Get()->Gateway && ev->Get()->ReplyTo);

        IMockPqGateway* gateway = ev->Get()->Gateway;
        TCompositeTopicReadSessionSettings settings = std::move(ev->Get()->Settings);
        const TActorId replyTo = ev->Get()->ReplyTo;

        NYdb::TDriver driver(NYdb::TDriverConfig().SetEndpoint("localhost:1"));
        auto topicClient = gateway->GetTopicClient(driver, gateway->GetTopicClientSettings());
        auto [session, control] = CreateCompositeTopicReadSession(ActorContext(), *topicClient, settings);

        Send(replyTo, new TEvCompositeSessionTest::TEvSessionCreated(std::move(session), std::move(control)));
        PassAway();
    }
};

class TCompositeClientTestFixture : public TTestWithActorSystemFixture {
    using TBase = TTestWithActorSystemFixture;

public:
    using TBase::TBase;

    void SetUp(NUnitTest::TTestContext& ctx) override {
        Settings.LogSettings.AddLogPriority(NKikimrServices::EServiceKikimr::KQP_COMPUTE, NLog::PRI_TRACE);
        TBase::SetUp(ctx);
        AggregatorActorId = Runtime.Register(CreateDqPqInfoAggregationActor("test_tx"));
    }

protected:
    TCompositeTopicReadSessionSettings MakeSettings(
        const TString& topicPath = "topic",
        std::vector<ui64> partitionIds = {0},
        NActors::TActorId aggregatorActor = {},
        std::optional<TDuration> idleTimeout = std::nullopt,
        std::optional<TDuration> maxPartitionReadSkew = std::nullopt,
        std::optional<ui64> inputIndex = std::nullopt)
    {
        NYdb::NTopic::TReadSessionSettings baseSettings;
        {
            NYdb::NTopic::TTopicReadSettings topic;
            topic.Path(topicPath);
            for (ui64 id : partitionIds) {
                topic.AppendPartitionIds(id);
            }
            baseSettings.AppendTopics(std::move(topic));
        }

        TCompositeTopicReadSessionSettings settings;
        settings.TxId = "test_tx";
        settings.TaskId = 1;
        settings.Cluster = "cluster";
        settings.AmountPartitionsCount = partitionIds.size();
        settings.InputIndex = inputIndex.value_or(0);
        settings.Counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        settings.BaseSettings = std::move(baseSettings);
        settings.IdleTimeout = idleTimeout.value_or(TDuration::Minutes(1));
        settings.MaxPartitionReadSkew = maxPartitionReadSkew.value_or(TDuration::Seconds(10));
        settings.AggregatorActor = aggregatorActor ? aggregatorActor : AggregatorActorId;
        return settings;
    }

    std::pair<std::shared_ptr<IReadSession>, ICompositeTopicReadSessionControl::TPtr> CreateSession(
        IMockPqGateway* gateway,
        const TCompositeTopicReadSessionSettings& settings)
    {
        const TActorId edge = Runtime.AllocateEdgeActor();
        const TActorId creator = Runtime.Register(new TCompositeSessionCreatorActor());
        Runtime.Send(creator, edge, new TEvCompositeSessionTest::TEvCreateSession(gateway, settings, edge));

        auto ev = Runtime.GrabEdgeEvent<TEvCompositeSessionTest::TEvSessionCreated>(edge);
        UNIT_ASSERT(ev);
        UNIT_ASSERT(ev->Get()->Session != nullptr);
        UNIT_ASSERT(ev->Get()->Control != nullptr);
        return {std::move(ev->Get()->Session), std::move(ev->Get()->Control)};
    }

    TActorId AggregatorActorId;
};

} // anonymous namespace

Y_UNIT_TEST_SUITE(TCompositeTopicReadSessionTest) {
    Y_UNIT_TEST_F(SessionCreationAndGetSessionId, TCompositeClientTestFixture) {
        auto gateway = CreateMockPqGateway({.OperationTimeout = TDuration::Seconds(5), .Runtime = &Runtime});
        auto settings = MakeSettings("topic", {0});

        auto [session, control] = CreateSession(gateway.Get(), settings);

        UNIT_ASSERT(!session->GetSessionId().empty());
        UNIT_ASSERT(session->GetSessionId().find("0=") != TString::npos);
    }

    Y_UNIT_TEST_F(GetEventWhenNoDataReturnsNullopt, TCompositeClientTestFixture) {
        auto gateway = CreateMockPqGateway({.OperationTimeout = TDuration::Seconds(5), .Runtime = &Runtime});
        auto settings = MakeSettings("topic", {0});

        auto [session, control] = CreateSession(gateway.Get(), settings);

        auto event = session->GetEvent(
            NYdb::NTopic::TReadSessionGetEventSettings().MaxEventsCount(1).MaxByteSize(4096));
        UNIT_ASSERT(!event.has_value());
    }

    Y_UNIT_TEST_F(GetEventReturnsDataFromMock, TCompositeClientTestFixture) {
        auto gateway = CreateMockPqGateway({.OperationTimeout = TDuration::Seconds(5), .Runtime = &Runtime});
        auto settings = MakeSettings("topic", {0});

        auto [session, control] = CreateSession(gateway.Get(), settings);

        auto mockSession = gateway->ExtractReadSession("topic");
        UNIT_ASSERT(mockSession != nullptr);
        mockSession->AddDataReceivedEvent(0, "msg1");

        auto event = session->GetEvent(
            NYdb::NTopic::TReadSessionGetEventSettings().MaxEventsCount(1).MaxByteSize(4096));
        UNIT_ASSERT(event.has_value());

        const auto* dataEv = std::get_if<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent>(&*event);
        UNIT_ASSERT(dataEv != nullptr);
        UNIT_ASSERT_VALUES_EQUAL(dataEv->GetMessagesCount(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(std::string(dataEv->GetMessages()[0].GetData()), "msg1");
    }

    Y_UNIT_TEST_F(AdvancePartitionTimeAfterGetEvent, TCompositeClientTestFixture) {
        auto gateway = CreateMockPqGateway({.OperationTimeout = TDuration::Seconds(5), .Runtime = &Runtime});
        auto settings = MakeSettings("topic", {0});

        auto [session, control] = CreateSession(gateway.Get(), settings);

        auto mockSession = gateway->ExtractReadSession("topic");
        UNIT_ASSERT(mockSession != nullptr);
        mockSession->AddDataReceivedEvent(0, "msg1");

        auto event = session->GetEvent(
            NYdb::NTopic::TReadSessionGetEventSettings().MaxEventsCount(1).MaxByteSize(4096));
        UNIT_ASSERT(event.has_value());

        const auto* dataEv = std::get_if<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent>(&*event);
        UNIT_ASSERT(dataEv != nullptr);
        UNIT_ASSERT_VALUES_EQUAL(std::string(dataEv->GetMessages()[0].GetData()), "msg1");
        const ui64 partitionId = dataEv->GetPartitionSession()->GetPartitionId();
        const TInstant eventTime = TInstant::MilliSeconds(100);

        control->AdvancePartitionTime(partitionId, eventTime);

        mockSession->AddDataReceivedEvent(1, "msg2");
        session->WaitEvent().Wait(TDuration::Seconds(2));

        auto event2 = session->GetEvent(
            NYdb::NTopic::TReadSessionGetEventSettings().MaxEventsCount(1).MaxByteSize(4096));
        UNIT_ASSERT(event2.has_value());
        const auto* dataEv2 = std::get_if<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent>(&*event2);
        UNIT_ASSERT(dataEv2 != nullptr);
        UNIT_ASSERT_VALUES_EQUAL(std::string(dataEv2->GetMessages()[0].GetData()), "msg2");
    }

    Y_UNIT_TEST_F(GetEventsReturnsBatch, TCompositeClientTestFixture) {
        const TInstant T0 = TInstant::MilliSeconds(100);
        auto gateway = CreateMockPqGateway({.OperationTimeout = TDuration::Seconds(5), .Runtime = &Runtime});
        auto settings = MakeSettings("topic", {0});

        auto [session, control] = CreateSession(gateway.Get(), settings);

        auto mockSession = gateway->ExtractReadSession("topic");
        UNIT_ASSERT(mockSession != nullptr);
        mockSession->AddDataReceivedEvent(0, "a", T0);

        auto event1 = session->GetEvent(
            NYdb::NTopic::TReadSessionGetEventSettings().MaxEventsCount(1).MaxByteSize(4096));
        UNIT_ASSERT(event1.has_value());
        const auto* data1 = std::get_if<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent>(&*event1);
        UNIT_ASSERT(data1 != nullptr);
        UNIT_ASSERT_VALUES_EQUAL(std::string(data1->GetMessages()[0].GetData()), "a");
        const ui64 partitionId = data1->GetPartitionSession()->GetPartitionId();
        control->AdvancePartitionTime(partitionId, T0);

        mockSession->AddDataReceivedEvent({{.Offset = 1, .Data = "b", .MessageTime = T0}, {.Offset = 2, .Data = "c", .MessageTime = T0}});
        session->WaitEvent().Wait(TDuration::Seconds(2));
        auto event2 = session->GetEvent(
            NYdb::NTopic::TReadSessionGetEventSettings().MaxEventsCount(1).MaxByteSize(4096));
        UNIT_ASSERT(event2.has_value());
        const auto* data2 = std::get_if<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent>(&*event2);
        UNIT_ASSERT(data2 != nullptr);
        UNIT_ASSERT_VALUES_EQUAL(data2->GetMessages().size(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(std::string(data2->GetMessages()[0].GetData()), "b");
        UNIT_ASSERT_VALUES_EQUAL(std::string(data2->GetMessages()[1].GetData()), "c");
    }

    Y_UNIT_TEST_F(WaitEventCompletesWhenDataAvailable, TCompositeClientTestFixture) {
        auto gateway = CreateMockPqGateway({.OperationTimeout = TDuration::Seconds(5), .Runtime = &Runtime});
        auto settings = MakeSettings("topic", {0});

        auto [session, control] = CreateSession(gateway.Get(), settings);

        auto mockSession = gateway->ExtractReadSession("topic");
        UNIT_ASSERT(mockSession != nullptr);

        NThreading::TFuture<void> waitFuture = session->WaitEvent();
        UNIT_ASSERT(!waitFuture.HasValue());

        mockSession->AddDataReceivedEvent(0, "wake");

        waitFuture.Wait(TDuration::Seconds(2));
        UNIT_ASSERT(waitFuture.HasValue());
    }

    Y_UNIT_TEST_F(CloseSucceeds, TCompositeClientTestFixture) {
        auto gateway = CreateMockPqGateway({.OperationTimeout = TDuration::Seconds(5), .Runtime = &Runtime});
        auto settings = MakeSettings("topic", {0});

        auto [session, control] = CreateSession(gateway.Get(), settings);

        bool closed = session->Close(TDuration::Zero());
        UNIT_ASSERT(closed);
    }

    Y_UNIT_TEST_F(AdvancePartitionTimeNoOpWhenTimeNotAdvanced, TCompositeClientTestFixture) {
        auto gateway = CreateMockPqGateway({.OperationTimeout = TDuration::Seconds(5), .Runtime = &Runtime});
        auto settings = MakeSettings("topic", {0});

        auto [session, control] = CreateSession(gateway.Get(), settings);

        auto mockSession = gateway->ExtractReadSession("topic");
        UNIT_ASSERT(mockSession != nullptr);
        mockSession->AddDataReceivedEvent(0, "msg1");

        auto event = session->GetEvent(
            NYdb::NTopic::TReadSessionGetEventSettings().MaxEventsCount(1).MaxByteSize(4096));
        UNIT_ASSERT(event.has_value());

        const auto* dataEv = std::get_if<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent>(&*event);
        UNIT_ASSERT(dataEv != nullptr);
        const ui64 partitionId = dataEv->GetPartitionSession()->GetPartitionId();
        const TInstant eventTime = TInstant::MilliSeconds(100);

        control->AdvancePartitionTime(partitionId, eventTime);
        control->AdvancePartitionTime(partitionId, TInstant::MilliSeconds(50));
        control->AdvancePartitionTime(partitionId, eventTime);

        mockSession->AddDataReceivedEvent(1, "msg2");
        session->WaitEvent().Wait(TDuration::Seconds(2));

        auto event2 = session->GetEvent(
            NYdb::NTopic::TReadSessionGetEventSettings().MaxEventsCount(1).MaxByteSize(4096));
        UNIT_ASSERT(event2.has_value());
        const auto* dataEv2 = std::get_if<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent>(&*event2);
        UNIT_ASSERT(dataEv2 != nullptr);
        UNIT_ASSERT_VALUES_EQUAL(std::string(dataEv2->GetMessages()[0].GetData()), "msg2");
    }

    Y_UNIT_TEST_F(GetEventsRespectsMaxEventsCount, TCompositeClientTestFixture) {
        auto gateway = CreateMockPqGateway({.OperationTimeout = TDuration::Seconds(5), .Runtime = &Runtime});
        auto settings = MakeSettings("topic", {0});

        auto [session, control] = CreateSession(gateway.Get(), settings);

        auto mockSession = gateway->ExtractReadSession("topic");
        UNIT_ASSERT(mockSession != nullptr);
        mockSession->AddDataReceivedEvent(0, "a");
        mockSession->AddDataReceivedEvent(1, "b");
        mockSession->AddDataReceivedEvent(2, "c");

        auto events = session->GetEvents(
            NYdb::NTopic::TReadSessionGetEventSettings()
                .MaxEventsCount(1)
                .MaxByteSize(65536));
        UNIT_ASSERT_VALUES_EQUAL(events.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(
            std::string(std::get<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent>(events[0]).GetMessages()[0].GetData()),
            "a");
    }

    Y_UNIT_TEST_F(StartSessionEventThenDataDelivered, TCompositeClientTestFixture) {
        const TInstant T0 = TInstant::MilliSeconds(100);
        auto gateway = CreateMockPqGateway({.OperationTimeout = TDuration::Seconds(5), .Runtime = &Runtime});
        auto settings = MakeSettings("topic", {0});

        auto [session, control] = CreateSession(gateway.Get(), settings);

        auto mockSession = gateway->ExtractReadSession("topic");
        UNIT_ASSERT(mockSession != nullptr);
        mockSession->AddStartSessionEvent();
        mockSession->AddDataReceivedEvent(0, "after_start", T0);

        auto event = session->GetEvent(
            NYdb::NTopic::TReadSessionGetEventSettings().MaxEventsCount(1).MaxByteSize(4096));
        UNIT_ASSERT(event.has_value());
        const auto* startEv = std::get_if<NYdb::NTopic::TReadSessionEvent::TStartPartitionSessionEvent>(&*event);
        UNIT_ASSERT(startEv != nullptr);
        const ui64 partitionId = startEv->GetPartitionSession() ? startEv->GetPartitionSession()->GetPartitionId() : 0u;
        control->AdvancePartitionTime(partitionId, T0);
        session->WaitEvent().Wait(TDuration::Seconds(2));

        auto event2 = session->GetEvent(
            NYdb::NTopic::TReadSessionGetEventSettings().MaxEventsCount(1).MaxByteSize(4096));
        UNIT_ASSERT(event2.has_value());
        const auto* dataEv = std::get_if<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent>(&*event2);
        UNIT_ASSERT(dataEv != nullptr);
        UNIT_ASSERT_VALUES_EQUAL(std::string(dataEv->GetMessages()[0].GetData()), "after_start");
    }

    Y_UNIT_TEST_F(IdleTimeoutPartitionStillReceivesData, TCompositeClientTestFixture) {
        const TDuration shortIdle = TDuration::MilliSeconds(50);
        const TDuration skew = TDuration::Seconds(1);
        const TInstant T0 = TInstant::MilliSeconds(100);
        const TInstant T1 = TInstant::MilliSeconds(900);
        auto gateway = CreateMockPqGateway({.OperationTimeout = TDuration::Seconds(5), .Runtime = &Runtime});
        auto settings = MakeSettings("topic", {0, 1}, {}, shortIdle, skew);

        auto [session, control] = CreateSession(gateway.Get(), settings);
        auto mockP0 = gateway->GetReadSession("topic", 0);
        auto mockP1 = gateway->GetReadSession("topic", 1);
        UNIT_ASSERT(mockP0 != nullptr);
        UNIT_ASSERT(mockP1 != nullptr);

        mockP0->AddDataReceivedEvent(0, "p0_first", T0);
        mockP1->AddDataReceivedEvent(0, "p1_first", T1);
        auto ev0 = session->GetEvent(
            NYdb::NTopic::TReadSessionGetEventSettings().MaxEventsCount(1).MaxByteSize(4096));
        UNIT_ASSERT(ev0.has_value());
        const auto* d0 = std::get_if<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent>(&*ev0);
        UNIT_ASSERT(d0 != nullptr);
        control->AdvancePartitionTime(d0->GetPartitionSession()->GetPartitionId(), d0->GetPartitionSession()->GetPartitionId() == 0 ? T0 : T1);
        auto ev1 = session->GetEvent(
            NYdb::NTopic::TReadSessionGetEventSettings().MaxEventsCount(1).MaxByteSize(4096));
        UNIT_ASSERT(ev1.has_value());
        const auto* d1 = std::get_if<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent>(&*ev1);
        UNIT_ASSERT(d1 != nullptr);
        control->AdvancePartitionTime(d1->GetPartitionSession()->GetPartitionId(), d1->GetPartitionSession()->GetPartitionId() == 0 ? T0 : T1);

        TString state = control->GetInternalState();
        UNIT_ASSERT_C(state.Contains("SuspendedPartitions"), "Partition 1 should be suspended: " << state);

        Sleep(shortIdle + TDuration::MilliSeconds(30));

        mockP0->AddDataReceivedEvent(1, "after_idle");
        mockP1->AddDataReceivedEvent(1, "p1_after_idle_unsuspend", T1);
        session->WaitEvent().Wait(TDuration::Seconds(2));
        auto event1 = session->GetEvent(
            NYdb::NTopic::TReadSessionGetEventSettings().MaxEventsCount(1).MaxByteSize(4096));
        UNIT_ASSERT(event1.has_value());
        const auto* dataEv1 = std::get_if<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent>(&*event1);
        UNIT_ASSERT(dataEv1 != nullptr);
        TString content1(dataEv1->GetMessages()[0].GetData());
        UNIT_ASSERT_C(content1 == "after_idle" || content1 == "p1_after_idle_unsuspend",
            "Expected after_idle or p1_after_idle_unsuspend, got: " << content1);

        session->WaitEvent().Wait(TDuration::Seconds(2));
        auto event2 = session->GetEvent(
            NYdb::NTopic::TReadSessionGetEventSettings().MaxEventsCount(1).MaxByteSize(4096));
        UNIT_ASSERT(event2.has_value());
        const auto* dataEv2 = std::get_if<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent>(&*event2);
        UNIT_ASSERT(dataEv2 != nullptr);
        TString content2(dataEv2->GetMessages()[0].GetData());
        UNIT_ASSERT_C(content2 == "after_idle" || content2 == "p1_after_idle_unsuspend", "Got: " << content2);
        UNIT_ASSERT(content1 != content2);
    }

    Y_UNIT_TEST_F(PartitionBalancingInsideOneSession, TCompositeClientTestFixture) {
        const TDuration skew = TDuration::Seconds(1);
        const TInstant T0 = TInstant::MilliSeconds(100);
        const TInstant T1 = TInstant::MilliSeconds(900);
        auto gateway = CreateMockPqGateway({.OperationTimeout = TDuration::Seconds(5), .Runtime = &Runtime});
        auto settings = MakeSettings("topic", {0, 1}, {}, std::nullopt, skew);

        auto [session, control] = CreateSession(gateway.Get(), settings);

        auto mockP0 = gateway->GetReadSession("topic", 0);
        auto mockP1 = gateway->GetReadSession("topic", 1);
        UNIT_ASSERT(mockP0 != nullptr);
        UNIT_ASSERT(mockP1 != nullptr);

        mockP0->AddDataReceivedEvent(0, "p0_msg", T0);
        auto ev0 = session->GetEvent(
            NYdb::NTopic::TReadSessionGetEventSettings().MaxEventsCount(1).MaxByteSize(4096));
        UNIT_ASSERT(ev0.has_value());
        const auto* data0 = std::get_if<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent>(&*ev0);
        UNIT_ASSERT(data0 != nullptr);
        UNIT_ASSERT_VALUES_EQUAL(std::string(data0->GetMessages()[0].GetData()), "p0_msg");
        control->AdvancePartitionTime(0, T0);

        mockP1->AddDataReceivedEvent(0, "p1_msg", T1);
        auto ev1 = session->GetEvent(
            NYdb::NTopic::TReadSessionGetEventSettings().MaxEventsCount(1).MaxByteSize(4096));
        UNIT_ASSERT(ev1.has_value());
        const auto* data1 = std::get_if<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent>(&*ev1);
        UNIT_ASSERT(data1 != nullptr);
        UNIT_ASSERT_VALUES_EQUAL(std::string(data1->GetMessages()[0].GetData()), "p1_msg");
        control->AdvancePartitionTime(1, T1);

        TString state = control->GetInternalState();
        UNIT_ASSERT_C(state.Contains("SuspendedPartitions"), "Expected suspended partitions: " << state);
        UNIT_ASSERT_C(state.Contains("PartitionId: 1"), "Partition 1 should be suspended (ahead in time): " << state);

        auto evNone = session->GetEvent(
            NYdb::NTopic::TReadSessionGetEventSettings().MaxEventsCount(1).MaxByteSize(4096));
        UNIT_ASSERT_C(!evNone.has_value(), "No event while partition 1 is suspended");

        control->AdvancePartitionTime(0, T1);

        mockP1->AddDataReceivedEvent(1, "after_unsuspend", T1);
        session->WaitEvent().Wait(TDuration::Seconds(2));
        auto ev2 = session->GetEvent(
            NYdb::NTopic::TReadSessionGetEventSettings().MaxEventsCount(1).MaxByteSize(4096));
        UNIT_ASSERT(ev2.has_value());
        const auto* data2 = std::get_if<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent>(&*ev2);
        UNIT_ASSERT(data2 != nullptr);
        UNIT_ASSERT_VALUES_EQUAL(std::string(data2->GetMessages()[0].GetData()), "after_unsuspend");
    }

    Y_UNIT_TEST_F(TwoSessionsSameAggregatorBothWork, TCompositeClientTestFixture) {
        auto gateway = CreateMockPqGateway({.OperationTimeout = TDuration::Seconds(5), .Runtime = &Runtime});

        auto settingsA = MakeSettings("topic_a", {0}, {}, std::nullopt, std::nullopt, 0);
        auto [sessionA, controlA] = CreateSession(gateway.Get(), settingsA);

        auto settingsB = MakeSettings("topic_b", {0}, {}, std::nullopt, std::nullopt, 1);
        auto [sessionB, controlB] = CreateSession(gateway.Get(), settingsB);

        auto mockA = gateway->ExtractReadSession("topic_a");
        auto mockB = gateway->ExtractReadSession("topic_b");
        UNIT_ASSERT(mockA != nullptr);
        UNIT_ASSERT(mockB != nullptr);

        mockA->AddDataReceivedEvent(0, "data_a");
        mockB->AddDataReceivedEvent(0, "data_b");

        auto evA = sessionA->GetEvent(
            NYdb::NTopic::TReadSessionGetEventSettings().MaxEventsCount(1).MaxByteSize(4096));
        auto evB = sessionB->GetEvent(
            NYdb::NTopic::TReadSessionGetEventSettings().MaxEventsCount(1).MaxByteSize(4096));

        UNIT_ASSERT(evA.has_value());
        UNIT_ASSERT(evB.has_value());
        const auto* dataA = std::get_if<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent>(&*evA);
        const auto* dataB = std::get_if<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent>(&*evB);
        UNIT_ASSERT(dataA != nullptr);
        UNIT_ASSERT(dataB != nullptr);
        UNIT_ASSERT_VALUES_EQUAL(std::string(dataA->GetMessages()[0].GetData()), "data_a");
        UNIT_ASSERT_VALUES_EQUAL(std::string(dataB->GetMessages()[0].GetData()), "data_b");
    }

    Y_UNIT_TEST_F(ReconnectSessionRecreationWithSameAggregator, TCompositeClientTestFixture) {
        const TInstant T0 = TInstant::MilliSeconds(100);
        const TInstant T1 = TInstant::MilliSeconds(200);
        auto gateway = CreateMockPqGateway({.OperationTimeout = TDuration::Seconds(5), .Runtime = &Runtime});
        auto settings = MakeSettings("topic", {0, 1});

        auto [session1, control1] = CreateSession(gateway.Get(), settings);
        auto mockP0_1 = gateway->GetReadSession("topic", 0);
        auto mockP1_1 = gateway->GetReadSession("topic", 1);
        UNIT_ASSERT(mockP0_1 != nullptr);
        UNIT_ASSERT(mockP1_1 != nullptr);

        mockP0_1->AddDataReceivedEvent(0, "first_p0", T0);
        mockP1_1->AddDataReceivedEvent(0, "first_p1", T1);
        auto ev0 = session1->GetEvent(
            NYdb::NTopic::TReadSessionGetEventSettings().MaxEventsCount(1).MaxByteSize(4096));
        UNIT_ASSERT(ev0.has_value());
        const auto* d0 = std::get_if<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent>(&*ev0);
        UNIT_ASSERT(d0 != nullptr);
        ui64 pid0 = d0->GetPartitionSession()->GetPartitionId();
        control1->AdvancePartitionTime(pid0, pid0 == 0 ? T0 : T1);
        auto ev1 = session1->GetEvent(
            NYdb::NTopic::TReadSessionGetEventSettings().MaxEventsCount(1).MaxByteSize(4096));
        UNIT_ASSERT(ev1.has_value());
        const auto* d1 = std::get_if<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent>(&*ev1);
        UNIT_ASSERT(d1 != nullptr);
        ui64 pid1 = d1->GetPartitionSession()->GetPartitionId();
        control1->AdvancePartitionTime(pid1, pid1 == 0 ? T0 : T1);
        UNIT_ASSERT(pid0 != pid1);

        UNIT_ASSERT(session1->Close(TDuration::Zero()));

        auto [session2, control2] = CreateSession(gateway.Get(), settings);
        UNIT_ASSERT(!session2->GetSessionId().empty());
        auto mockP0_2 = gateway->GetReadSession("topic", 0);
        auto mockP1_2 = gateway->GetReadSession("topic", 1);
        UNIT_ASSERT(mockP0_2 != nullptr);
        UNIT_ASSERT(mockP1_2 != nullptr);

        mockP0_2->AddDataReceivedEvent(1, "reconnected_p0", T0);
        mockP1_2->AddDataReceivedEvent(1, "reconnected_p1", T1);
        auto ev2a = session2->GetEvent(
            NYdb::NTopic::TReadSessionGetEventSettings().MaxEventsCount(1).MaxByteSize(4096));
        UNIT_ASSERT(ev2a.has_value());
        const auto* d2a = std::get_if<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent>(&*ev2a);
        UNIT_ASSERT(d2a != nullptr);
        TString content2a(d2a->GetMessages()[0].GetData());
        UNIT_ASSERT(content2a == "reconnected_p0" || content2a == "reconnected_p1");
        ui64 pid2a = d2a->GetPartitionSession()->GetPartitionId();
        control2->AdvancePartitionTime(pid2a, pid2a == 0 ? T0 : T1);
        session2->WaitEvent().Wait(TDuration::Seconds(2));
        auto ev2b = session2->GetEvent(
            NYdb::NTopic::TReadSessionGetEventSettings().MaxEventsCount(1).MaxByteSize(4096));
        UNIT_ASSERT(ev2b.has_value());
        const auto* d2b = std::get_if<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent>(&*ev2b);
        UNIT_ASSERT(d2b != nullptr);
        TString content2b(d2b->GetMessages()[0].GetData());
        UNIT_ASSERT(content2b == "reconnected_p0" || content2b == "reconnected_p1");
        UNIT_ASSERT(content2a != content2b);
    }

    Y_UNIT_TEST_F(PartitionBalancingBetweenTwoSessions, TCompositeClientTestFixture) {
        const TDuration skew = TDuration::Seconds(1);
        const TInstant T0 = TInstant::MilliSeconds(100);
        const TInstant T1 = TInstant::MilliSeconds(1200);
        auto gateway = CreateMockPqGateway({.OperationTimeout = TDuration::Seconds(5), .Runtime = &Runtime});

        auto settingsA = MakeSettings("topic", {0}, {}, std::nullopt, skew, 0);
        auto settingsB = MakeSettings("topic", {1}, {}, std::nullopt, skew, 0);
        // Both sessions share one aggregator: use total partition count so AllPartitionsStarted
        // becomes true when both have started and aggregator can propagate read_time.
        settingsA.AmountPartitionsCount = 2;
        settingsB.AmountPartitionsCount = 2;
        auto [sessionA, controlA] = CreateSession(gateway.Get(), settingsA);
        auto [sessionB, controlB] = CreateSession(gateway.Get(), settingsB);

        auto mockP0 = gateway->GetReadSession("topic", 0);
        auto mockP1 = gateway->GetReadSession("topic", 1);
        UNIT_ASSERT(mockP0 != nullptr);
        UNIT_ASSERT(mockP1 != nullptr);

        mockP0->AddDataReceivedEvent(0, "p0_msg", T0);
        auto evA = sessionA->GetEvent(
            NYdb::NTopic::TReadSessionGetEventSettings().MaxEventsCount(1).MaxByteSize(4096));
        UNIT_ASSERT(evA.has_value());
        const auto* dataA = std::get_if<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent>(&*evA);
        UNIT_ASSERT(dataA != nullptr);
        UNIT_ASSERT_VALUES_EQUAL(std::string(dataA->GetMessages()[0].GetData()), "p0_msg");
        controlA->AdvancePartitionTime(0, T0);

        mockP1->AddDataReceivedEvent(0, "p1_msg", T1);
        auto evB = sessionB->GetEvent(
            NYdb::NTopic::TReadSessionGetEventSettings().MaxEventsCount(1).MaxByteSize(4096));
        UNIT_ASSERT(evB.has_value());
        const auto* dataB = std::get_if<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent>(&*evB);
        UNIT_ASSERT(dataB != nullptr);
        UNIT_ASSERT_VALUES_EQUAL(std::string(dataB->GetMessages()[0].GetData()), "p1_msg");
        controlB->AdvancePartitionTime(1, T1);

        TString stateB = controlB->GetInternalState();
        UNIT_ASSERT_C(stateB.Contains("SuspendedPartitions"), "Session B should have suspended partition: " << stateB);

        mockP1->AddDataReceivedEvent(1, "blocked_until_a_advances", T1);
        auto evNone = sessionB->GetEvent(
            NYdb::NTopic::TReadSessionGetEventSettings().MaxEventsCount(1).MaxByteSize(4096));
        UNIT_ASSERT_C(!evNone.has_value(), "Session B should not get event while its partition is suspended (waiting for A)");

        controlA->AdvancePartitionTime(0, T1);
        std::optional<NYdb::NTopic::TReadSessionEvent::TEvent> evB2;
        const auto deadline = TInstant::Now() + TDuration::Seconds(10);
        while (TInstant::Now() < deadline) {
            sessionB->WaitEvent().Wait(TDuration::Seconds(1));
            evB2 = sessionB->GetEvent(
                NYdb::NTopic::TReadSessionGetEventSettings().MaxEventsCount(1).MaxByteSize(4096));
            if (evB2.has_value()) {
                break;
            }
        }
        UNIT_ASSERT_C(evB2.has_value(), "Session B should receive blocked_until_a_advances after A advances (aggregator propagation)");
        const auto* dataB2 = std::get_if<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent>(&*evB2);
        UNIT_ASSERT(dataB2 != nullptr);
        UNIT_ASSERT_VALUES_EQUAL(std::string(dataB2->GetMessages()[0].GetData()), "blocked_until_a_advances");
    }
}

} // namespace NYql
