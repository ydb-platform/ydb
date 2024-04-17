#include <library/cpp/testing/unittest/registar.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_channels.h>
#include <ydb/library/actors/testlib/test_runtime.h>

using namespace NActors;

namespace NYql::NDq {

struct TActorSystem: NActors::TTestActorRuntimeBase
{
    TString UnusedComponent = TString("Unused");

    void Start()
    {
        SetDispatchTimeout(TDuration::Seconds(5));
        InitNodes();
        AppendToLogSettings(
            static_cast<NLog::EComponent>(500),
            static_cast<NLog::EComponent>(600),
            [&](int) -> const TString& { return UnusedComponent; }
        );
    }
};

struct TChannelsTestFixture: public NUnitTest::TBaseFixture
{
    TActorSystem ActorSystem;
    TActorId Channels;
    TActorId EdgeActor;

    void SetUp(NUnitTest::TTestContext& /* context */) override
    {
        ActorSystem.Start();

        NDqProto::TDqTask task;
        auto channels = std::unique_ptr<TDqComputeActorChannels>(new TDqComputeActorChannels(
            /*ParentActorId = */ {}, 
            "TxId", 
            /*Task = */ TDqTaskSettings { &task },
            /*retry = */ true,
            NYql::NDqProto::EDqStatsMode::DQ_STATS_MODE_NONE,
            /*channelBufferSize = */ 1000000,
            /*callbacks = */ nullptr,
            /*activityType = */ 0
        ));
        channels->InputChannelsMap.emplace((ui64)0, TDqComputeActorChannels::TInputChannelState {});

        Channels = ActorSystem.Register(channels.release());
        ActorSystem.DispatchEvents(
            {.FinalEvents = {{TEvents::TSystem::Bootstrap}}},
            TDuration::MilliSeconds(10));
    }
};

Y_UNIT_TEST_SUITE(TComputeActorTest) {
    Y_UNIT_TEST_F(Empty, TChannelsTestFixture) { }

    Y_UNIT_TEST_F(ReceiveData, TChannelsTestFixture) {
        auto ev = MakeHolder<TEvDqCompute::TEvChannelData>();
        ActorSystem.Send(
            Channels,
            EdgeActor,
            ev.Release()
        );
    }
}

} //namespace NYql::NDq

