#include <library/cpp/testing/unittest/registar.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_channels.h>
#include <ydb/library/actors/testlib/test_runtime.h>

using namespace NActors;

namespace NYql::NDq {

namespace {
static const bool TESTS_VERBOSE = getenv("TESTS_VERBOSE") != nullptr;
}
struct TMockChannelCallbacks: public TDqComputeActorChannels::ICallbacks
{
    i64 GetInputChannelFreeSpace(ui64 channelId) const override {
        Y_UNUSED(channelId);
        return 100;
    }

    void TakeInputChannelData(TChannelDataOOB&& channelData, bool ack) override  {
        Y_UNUSED(channelData);
        Y_UNUSED(ack);
    }

    void PeerFinished(ui64 channelId) override {
        Y_UNUSED(channelId);
    }

    void ResumeExecution(EResumeSource source) override {
        Y_UNUSED(source);
    }
};

struct TActorSystem: NActors::TTestActorRuntimeBase
{
    TString UnusedComponent = TString("Unused");

    void Start()
    {
        SetDispatchTimeout(TDuration::Seconds(5));
        InitNodes();
        AppendToLogSettings(
                NKikimrServices::EServiceKikimr_MIN,
                NKikimrServices::EServiceKikimr_MAX,
                NKikimrServices::EServiceKikimr_Name<NLog::EComponent>
                );

        if (TESTS_VERBOSE) {
            SetLogPriority(NKikimrServices::KQP_COMPUTE, NActors::NLog::EPriority::PRI_TRACE);
            SetLogPriority(NKikimrServices::DQ_TASK_RUNNER, NActors::NLog::EPriority::PRI_TRACE);
        }
    }
};

struct TChannelsTestFixture: public NUnitTest::TBaseFixture
{
    TActorSystem ActorSystem;
    TDqComputeActorChannels* Channels;
    TDqComputeActorChannels::TInputChannelState* InputChannel;
    TActorId ChannelsId;
    TActorId EdgeActor;
    TMockChannelCallbacks Callbacks;

    void SetUp(NUnitTest::TTestContext& /* context */) override
    {
        ActorSystem.Start();

        EdgeActor = ActorSystem.AllocateEdgeActor();

        NDqProto::TDqTask task;
        auto channels = std::unique_ptr<TDqComputeActorChannels>(new TDqComputeActorChannels(
            /*ParentActorId = */ {}, 
            "TxId", 
            /*Task = */ TDqTaskSettings { &task },
            /*retry = */ true,
            NYql::NDqProto::EDqStatsMode::DQ_STATS_MODE_NONE,
            /*channelBufferSize = */ 1000000,
            /*callbacks = */ &Callbacks,
            /*activityType = */ {}
        ));
        channels->InputChannelsMap.emplace((ui64)0, TDqComputeActorChannels::TInputChannelState {});
        InputChannel = &channels->InCh(0);
        Channels = channels.release();
        ChannelsId = ActorSystem.Register(Channels);
    }

    void RunInActorContext(const std::function<void(void)>& f) {
        bool executed = false;
        auto prev = ActorSystem.SetEventFilter([&](TTestActorRuntimeBase &, TAutoPtr<IEventHandle> &) -> bool {
            if (!executed) {
                executed = true;
                f();
                return true;
            } else {
                return false;
            }
        });
        ActorSystem.Send(
            EdgeActor,
            TActorId{},
            new TEvents::TEvWakeup
        );
        ActorSystem.SetEventFilter(prev);
    }

    void SendAck(i64 freeSpace) {
        Channels->SendChannelDataAck(*InputChannel, freeSpace);
    }

    bool Poll(i64 freeSpace) {
        return Channels->PollChannel(InputChannel->ChannelId, freeSpace);
    }
};

Y_UNIT_TEST_SUITE(TComputeActorTest) {
    Y_UNIT_TEST_F(Empty, TChannelsTestFixture) { }

    Y_UNIT_TEST_F(ReceiveData, TChannelsTestFixture) {
        auto ev = MakeHolder<TEvDqCompute::TEvChannelData>();
        ActorSystem.Send(
            ChannelsId,
            EdgeActor,
            ev.Release()
        );

        UNIT_ASSERT(!InputChannel->Peer.has_value());

        ev = MakeHolder<TEvDqCompute::TEvChannelData>();
        ev->Record.SetSeqNo(1);

        ActorSystem.Send(
            ChannelsId,
            EdgeActor,
            ev.Release()
        );

        UNIT_ASSERT(InputChannel->Peer == EdgeActor);
        UNIT_ASSERT(InputChannel->InFlight.size() == 0);

        RunInActorContext([&]() { SendAck(10); });

        UNIT_ASSERT(InputChannel->InFlight.size() == 1);
    }

    Y_UNIT_TEST_F(PollAckPoll, TChannelsTestFixture) {
        {
            auto ev = MakeHolder<TEvDqCompute::TEvChannelData>();
            ActorSystem.Send(
                ChannelsId,
                EdgeActor,
                ev.Release()
            );
        }

        UNIT_ASSERT(!InputChannel->Peer.has_value());

        {
            auto ev = MakeHolder<TEvDqCompute::TEvChannelData>();
            ev->Record.SetSeqNo(1);

            ActorSystem.Send(
                ChannelsId,
                EdgeActor,
                ev.Release()
            );
        }

        UNIT_ASSERT(InputChannel->Peer == EdgeActor);
        UNIT_ASSERT(InputChannel->InFlight.size() == 0);

        RunInActorContext([&]() {
            auto res = Channels->PollChannel(InputChannel->ChannelId, +10);
            UNIT_ASSERT(res);
        });
        {
            auto ev = ActorSystem.GrabEdgeEvent<TEvDqCompute::TEvChannelDataAck>(EdgeActor);
            UNIT_ASSERT_EQUAL(ev->Get()->Record.GetFreeSpace(), +10);
        }
        RunInActorContext([&]() {
            Channels->SendChannelDataAck(InputChannel->ChannelId, -10);
        });
        {
            auto ev = ActorSystem.GrabEdgeEvent<TEvDqCompute::TEvChannelDataAck>(EdgeActor);
            UNIT_ASSERT_EQUAL(ev->Get()->Record.GetFreeSpace(), -10);
        }
        RunInActorContext([&]() {
            auto res = Channels->PollChannel(InputChannel->ChannelId, +10);
            UNIT_ASSERT(res);
        });
        {
            auto ev = ActorSystem.GrabEdgeEvent<TEvDqCompute::TEvChannelDataAck>(EdgeActor);
            UNIT_ASSERT_EQUAL(ev->Get()->Record.GetFreeSpace(), +10);
        }
    }
}

} //namespace NYql::NDq

