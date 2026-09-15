#include "dq_channel_test_harness.h"

#include <library/cpp/monlib/service/monservice.h>

// Message sizes at the edges of the windows, the stats a channel exports, jitter on both sides and the
// monitoring page.

// The stats of the producer and the consumer of one channel must agree with each other and with the
// traffic; with the level None the output side collects nothing, the input side still counts
struct TStatsTest : public TSessionTest {

    void Run() override {
        Prepare();
        Init();

        const int messages = 50;
        ProducerSettings = TWorkerSettings{ .MessageCount = messages, .MinMessageSize = 100, .MaxMessageSize = 1000, .StatsLevel = Level };
        ConsumerSettings = ProducerSettings;

        StartChannel(1, true);
        WaitChannel("stats");

        auto producer = FindFinished(1, TEvTestPrivate::ERole::Producer);
        auto consumer = FindFinished(1, TEvTestPrivate::ERole::Consumer);
        UNIT_ASSERT(producer && consumer);

        auto details = TStringBuilder() << "producer push " << producer->PushStats.Chunks << '/' << producer->PushStats.Rows << '/' << producer->PushStats.Bytes
            << ", producer pop " << producer->PopStats.Chunks << '/' << producer->PopStats.Rows << '/' << producer->PopStats.Bytes
            << ", consumer push " << consumer->PushStats.Chunks << '/' << consumer->PushStats.Rows << '/' << consumer->PushStats.Bytes
            << ", consumer pop " << consumer->PopStats.Chunks << '/' << consumer->PopStats.Rows << '/' << consumer->PopStats.Bytes;

        if (Level == TCollectStatsLevel::None) {
            // the output side does not count at all, the local buffer neither; the remote input does
            UNIT_ASSERT_VALUES_EQUAL_C(producer->PushStats.Bytes, 0, details);
            UNIT_ASSERT_VALUES_EQUAL_C(producer->PushStats.Chunks, 0, details);
            if (Local) {
                UNIT_ASSERT_VALUES_EQUAL_C(consumer->PopStats.Bytes, 0, details);
            } else {
                UNIT_ASSERT_VALUES_EQUAL_C(consumer->PopStats.Rows, messages, details);
                UNIT_ASSERT_VALUES_EQUAL_C(consumer->PopStats.Chunks, messages + 1, details);
            }
            return Finish();
        }

        // what the consumer popped: the messages and the finish chunk
        UNIT_ASSERT_VALUES_EQUAL_C(consumer->PopStats.Rows, messages, details);
        UNIT_ASSERT_VALUES_EQUAL_C(consumer->PopStats.Chunks, messages + 1, details);
        UNIT_ASSERT_C(consumer->PopStats.Bytes > messages * 101, details);
        UNIT_ASSERT_VALUES_EQUAL_C(producer->PushStats.Rows, messages, details);
        UNIT_ASSERT_VALUES_EQUAL_C(producer->PushStats.Bytes, consumer->PopStats.Bytes, details);
        if (Local) {
            // one buffer, one set of stats
            UNIT_ASSERT_VALUES_EQUAL_C(producer->PushStats.Chunks, consumer->PopStats.Chunks, details);
            UNIT_ASSERT_VALUES_EQUAL_C(producer->PopStats.Bytes, consumer->PopStats.Bytes, details);
        } else {
            // the output side counts the confirmation of the finish it sends as a chunk of its own, pushed
            // and popped, and the input side counts it as received; it carries no bytes and is never popped
            UNIT_ASSERT_VALUES_EQUAL_C(producer->PushStats.Chunks, messages + 2, details);
            UNIT_ASSERT_VALUES_EQUAL_C(producer->PopStats.Bytes, consumer->PopStats.Bytes, details);
            UNIT_ASSERT_VALUES_EQUAL_C(producer->PopStats.Chunks, messages + 2, details);
            UNIT_ASSERT_VALUES_EQUAL_C(consumer->PushStats.Bytes, consumer->PopStats.Bytes, details);
            UNIT_ASSERT_VALUES_EQUAL_C(consumer->PushStats.Chunks, messages + 2, details);
        }
        Finish();
    }

    void Finish() {
        CheckSensors();
        Destroy();
        CheckQuota();
    }

    TCollectStatsLevel Level = TCollectStatsLevel::Basic;
};

struct TMockMonHttpRequest : NMonitoring::IMonHttpRequest {
    TCgiParameters Params_;

    explicit TMockMonHttpRequest(const TString& params) {
        Params_.Scan(params);
    }

    const TCgiParameters& GetParams() const override { return Params_; }
    IOutputStream& Output() override { Y_ABORT("Not implemented"); }
    HTTP_METHOD GetMethod() const override { Y_ABORT("Not implemented"); }
    TStringBuf GetPath() const override { Y_ABORT("Not implemented"); }
    TStringBuf GetPathInfo() const override { Y_ABORT("Not implemented"); }
    TStringBuf GetUri() const override { Y_ABORT("Not implemented"); }
    const TCgiParameters& GetPostParams() const override { Y_ABORT("Not implemented"); }
    TStringBuf GetPostContent() const override { Y_ABORT("Not implemented"); }
    const THttpHeaders& GetHeaders() const override { Y_ABORT("Not implemented"); }
    TStringBuf GetHeader(TStringBuf) const override { Y_ABORT("Not implemented"); }
    TStringBuf GetCookie(TStringBuf) const override { Y_ABORT("Not implemented"); }
    TString GetRemoteAddr() const override { Y_ABORT("Not implemented"); }
    TString GetServiceTitle() const override { Y_ABORT("Not implemented"); }
    NMonitoring::IMonPage* GetPage() const override { Y_ABORT("Not implemented"); }
    NMonitoring::IMonHttpRequest* MakeChild(NMonitoring::IMonPage*, const TString&) const override { Y_ABORT("Not implemented"); }
};

// The page renders with live sessions and channels of every kind, and its destroy action frees a session
struct TMonPageTest : public TSessionTest {

    TString Render(ui32 nodeIndex, const TString& params) {
        auto control = nodeIndex == NodeIndex0 ? Control0 : Control1;
        TMockMonHttpRequest request(params);
        Runtime->Send(MakeChannelServiceActorID(Runtime->GetNodeId(nodeIndex)), control, new NActors::NMon::TEvHttpInfo(request), nodeIndex, true);
        auto reply = Runtime->GrabEdgeEvent<NActors::NMon::TEvHttpInfoRes>(control, TDuration::Seconds(10));
        TStringStream answer;
        reply->Get()->Output(answer);
        return answer.Str();
    }

    void Run() override {
        Prepare();
        Init();

        // a remote channel each way and a local one, their consumers stalled so that everything is live
        ProducerSettings = TWorkerSettings{ .MessageCount = 20, .MinMessageSize = 100, .MaxMessageSize = 1000 };
        ConsumerSettings = TWorkerSettings{ .MessageCount = 20, .MinMessageSize = 100, .MaxMessageSize = 1000,
            .PauseMessageIndex = 1, .PauseDelayMs = 30000 };
        StartChannel(1, true);
        StartInboundChannel(2, true);
        auto localProducer = Runtime->Register(new TProducerActor(Service0, 3, ProducerSettings, OutputQuotaManager), NodeIndex0);
        auto localConsumer = Runtime->Register(new TConsumerActor(Service0, 3, ConsumerSettings, InputQuotaManager), NodeIndex0);
        StartPair(localProducer, localConsumer, NodeIndex0, NodeIndex0);

        auto peerNodeId = Runtime->GetNodeId(1);
        std::shared_ptr<TNodeState> session;
        UNIT_ASSERT_C(WaitFor([&]() {
            session = FindNodeState(Service0, peerNodeId);
            return session && GetOutputCount(session) == 1 && GetInputPopBytes(session) > 0;
        }, TDuration::Seconds(10)), "the channels did not come up");

        auto page = Render(NodeIndex0, "");
        UNIT_ASSERT_C(page.Contains("Local Buffers"), page);
        UNIT_ASSERT_C(page.Contains("Sessions"), page);
        UNIT_ASSERT_C(page.Contains("Output Descriptors") && page.Contains("Input Descriptors"), page);
        UNIT_ASSERT_C(page.Contains(ToString(peerNodeId)), page);

        auto redirect = Render(NodeIndex0, TStringBuilder() << "node=" << peerNodeId);
        UNIT_ASSERT_C(redirect.Contains("307"), redirect);

        Render(NodeIndex0, TStringBuilder() << "node=" << peerNodeId << "&fail=destroy");
        UNIT_ASSERT_C(WaitFor([&]() { return FindNodeState(Service0, peerNodeId) == nullptr; }, TDuration::Seconds(5)),
            "the destroy action did not free the session");
        UNIT_ASSERT_C(session->Terminating.load(), "the session is not terminating");

        session.reset();
        Destroy();
    }
};

Y_UNIT_TEST_SUITE(Channels20Misc) {

    void LoadTest(int count, bool local, const TWorkerSettings& settings, const TDqChannelLimits& limits = TDqChannelLimits{}) {
        TLoadTest test;

        test.Count = count;
        test.Local = local;
        test.Limits = limits;
        test.ProducerSettings = settings;
        test.ConsumerSettings = settings;

        test.Run();
    }

    // one chunk always passes the window, however large
    Y_UNIT_TEST(BigMessages2n) {
        LoadTest(4, false, TWorkerSettings{ .MessageCount = 5, .MinMessageSize = 4000000, .MaxMessageSize = 4000000 });
        LoadTest(2, false, TWorkerSettings{ .MessageCount = 2, .MinMessageSize = 20000000, .MaxMessageSize = 20000000 });
    }

    Y_UNIT_TEST(BigMessages1n) {
        LoadTest(4, true, TWorkerSettings{ .MessageCount = 5, .MinMessageSize = 4000000, .MaxMessageSize = 4000000 });
        LoadTest(2, true, TWorkerSettings{ .MessageCount = 2, .MinMessageSize = 20000000, .MaxMessageSize = 20000000 });
    }

    // the session window is checked before the bytes are added, so one message is always in flight
    Y_UNIT_TEST(MessageLargerThanSessionWindow2n) {
        TDqChannelLimits limits;
        limits.RemoteSessionInflightBytes = 1_MB;
        LoadTest(2, false, TWorkerSettings{ .MessageCount = 5, .MinMessageSize = 4000000, .MaxMessageSize = 4000000 }, limits);
    }

    Y_UNIT_TEST(ZeroLengthPayload2n) {
        LoadTest(10, false, TWorkerSettings{ .MessageCount = 100, .MinMessageSize = 0, .MaxMessageSize = 0 });
    }

    Y_UNIT_TEST(ZeroLengthPayload1n) {
        LoadTest(10, true, TWorkerSettings{ .MessageCount = 100, .MinMessageSize = 0, .MaxMessageSize = 0 });
    }

    Y_UNIT_TEST(StatsExportBasic2n) {
        TStatsTest test;
        test.Local = false;
        test.Run();
    }

    Y_UNIT_TEST(StatsExportBasic1n) {
        TStatsTest test;
        test.Local = true;
        test.Run();
    }

    Y_UNIT_TEST(StatsExportNone2n) {
        TStatsTest test;
        test.Local = false;
        test.Level = TCollectStatsLevel::None;
        test.Run();
    }

    Y_UNIT_TEST(StatsExportNone1n) {
        TStatsTest test;
        test.Local = true;
        test.Level = TCollectStatsLevel::None;
        test.Run();
    }

    // jitter on every worker of every channel must not cost a single reconciliation
    Y_UNIT_TEST(RandomPausesBothSides2n) {
        LoadTest(100, false, TWorkerSettings{ .MessageCount = 100, .MinMessageSize = 0, .MaxMessageSize = 65536, .RandomPauseMaxMs = 300 });
    }

    Y_UNIT_TEST(RandomPausesBothSides1n) {
        LoadTest(100, true, TWorkerSettings{ .MessageCount = 100, .MinMessageSize = 0, .MaxMessageSize = 65536, .RandomPauseMaxMs = 300 });
    }

    Y_UNIT_TEST(MonPage2n) {
        TMonPageTest test;
        test.Local = false;
        test.Run();
    }
}
