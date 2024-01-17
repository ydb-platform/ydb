#include "selfping_actor.h"

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/library/actors/testlib/test_runtime.h>

namespace NActors {
namespace Tests {

THolder<TTestActorRuntimeBase> CreateRuntime() {
    auto runtime = MakeHolder<TTestActorRuntimeBase>();
    runtime->SetScheduledEventFilter([](auto&&, auto&&, auto&&, auto&&) { return false; });
    runtime->Initialize();
    return runtime;
}

Y_UNIT_TEST_SUITE(TSelfPingTest) {
    Y_UNIT_TEST(Basic)
    {
        auto runtime = CreateRuntime();

        //const TActorId sender = runtime.AllocateEdgeActor();

        NMonitoring::TDynamicCounters::TCounterPtr counter(new NMonitoring::TCounterForPtr());
        NMonitoring::TDynamicCounters::TCounterPtr counter2(new NMonitoring::TCounterForPtr());
        NMonitoring::TDynamicCounters::TCounterPtr counter3(new NMonitoring::TCounterForPtr());
        NMonitoring::TDynamicCounters::TCounterPtr counter4(new NMonitoring::TCounterForPtr());

        auto actor = CreateSelfPingActor(
            TDuration::MilliSeconds(100), // sendInterval (unused in test)
            counter, counter2, counter3, counter4);

        UNIT_ASSERT_VALUES_EQUAL(counter->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(counter2->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(counter3->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(counter4->Val(), 0);

        const TActorId actorId = runtime->Register(actor);
        Y_UNUSED(actorId);

        //runtime.Send(new IEventHandle(actorId, sender, new TEvSelfPing::TEvPing(0.0)));

        // TODO check after events are handled
        //Sleep(TDuration::Seconds(1));
        //UNIT_ASSERT((intmax_t)counter->Val() >= (intmax_t)Delay.MicroSeconds());
    }
}

} // namespace Tests
} // namespace NActors
