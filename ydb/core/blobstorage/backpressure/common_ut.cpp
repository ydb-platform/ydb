#include "common.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/testlib/actors/test_runtime.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NBsQueue {
namespace {

using namespace NActors;

TTestActorRuntime::TEgg MakeTestRuntimeEgg() {
    return {new TAppData(0, 0, 0, 0, {}, nullptr, nullptr, nullptr, nullptr), nullptr, nullptr, {}, {}};
}

class TVirtualTimerActor : public TActor<TVirtualTimerActor> {
public:
    TVirtualTimerActor(TActorId replyTo, double& passed)
        : TActor(&TVirtualTimerActor::StateFunc)
        , ReplyTo(replyTo)
        , Passed(passed)
    {}

private:
    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            case TEvents::THelloWorld::Ping:
                Timer.ConstructInPlace(true);
                Schedule(TDuration::Seconds(5), new TEvents::TEvWakeup);
                break;

            case TEvents::TSystem::Wakeup:
                Passed = Timer->Passed();
                Send(ReplyTo, new TEvents::TEvWakeup);
                PassAway();
                break;
        }
    }

    const TActorId ReplyTo;
    double& Passed;
    TMaybe<TBSQueueTimer> Timer;
};

} // anonymous namespace

Y_UNIT_TEST_SUITE(TBSQueueTimerTest) {
    Y_UNIT_TEST(HighPrecisionTimer) {
        TBSQueueTimer timer(false);
        Sleep(TDuration::MilliSeconds(1));
        const double first = timer.Passed();
        Sleep(TDuration::MilliSeconds(1));
        const double second = timer.Passed();

        UNIT_ASSERT_C(first > 0, first);
        UNIT_ASSERT_C(second >= first, first << " " << second);
        UNIT_ASSERT_VALUES_EQUAL(sizeof(timer), 16);
    }

    Y_UNIT_TEST(ActorSystemTimerUsesVirtualTime) {
        TTestActorRuntime runtime;
        runtime.Initialize(MakeTestRuntimeEgg());
        runtime.SetScheduledEventsSelectorFunc(&TTestActorRuntimeBase::CollapsedTimeScheduledEventsSelector);

        const TActorId edge = runtime.AllocateEdgeActor();
        double passed = 0;
        const TActorId actor = runtime.Register(new TVirtualTimerActor(edge, passed));
        runtime.EnableScheduleForActor(actor);
        runtime.Send(new IEventHandle(actor, edge, new TEvents::TEvPing));

        runtime.GrabEdgeEvent<TEvents::TEvWakeup>(edge, TDuration::Seconds(10));
        UNIT_ASSERT_DOUBLES_EQUAL(passed, 5.0, 1e-5);
    }
}

} // namespace NKikimr::NBsQueue
