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
    explicit TVirtualTimerActor(TActorId replyTo)
        : TActor(&TVirtualTimerActor::StateFunc)
        , ReplyTo(replyTo)
    {}

private:
    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            case TEvents::THelloWorld::Ping:
                Timer.ConstructInPlace(true);
                Schedule(TDuration::Seconds(5), new TEvents::TEvWakeup);
                break;

            case TEvents::TSystem::Wakeup:
                Send(ReplyTo, new TEvents::TEvWakeup(TDuration::Seconds(Timer->Passed()).MicroSeconds()));
                PassAway();
                break;
        }
    }

    const TActorId ReplyTo;
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
        const TActorId actor = runtime.Register(new TVirtualTimerActor(edge));
        runtime.EnableScheduleForActor(actor);
        runtime.Send(new IEventHandle(actor, edge, new TEvents::TEvPing));

        const auto result = runtime.GrabEdgeEvent<TEvents::TEvWakeup>(edge, TDuration::Seconds(10));
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Tag, TDuration::Seconds(5).MicroSeconds());
    }
}

} // namespace NKikimr::NBsQueue
