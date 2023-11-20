#include <library/cpp/testing/unittest/registar.h>

#include "scheduler_actor.h"
#include "misc/test_sync.h"

using namespace NBus;
using namespace NBus::NPrivate;
using namespace NActor;

Y_UNIT_TEST_SUITE(TSchedulerActorTests) {
    struct TMyActor: public TAtomicRefCount<TMyActor>, public TActor<TMyActor>, public TScheduleActor<TMyActor> {
        TTestSync TestSync;

        TMyActor(TExecutor* executor, TScheduler* scheduler)
            : TActor<TMyActor>(executor)
            , TScheduleActor<TMyActor>(scheduler)
            , Iteration(0)
        {
        }

        unsigned Iteration;

        void Act(TDefaultTag) {
            if (!Alarm.FetchTask()) {
                Y_ABORT("must not have no spurious wakeups in test");
            }

            TestSync.WaitForAndIncrement(Iteration++);
            if (Iteration <= 5) {
                ScheduleAt(TInstant::Now() + TDuration::MilliSeconds(Iteration));
            }
        }
    };

    Y_UNIT_TEST(Simple) {
        TExecutor executor(1);
        TScheduler scheduler;

        TIntrusivePtr<TMyActor> actor(new TMyActor(&executor, &scheduler));

        actor->ScheduleAt(TInstant::Now() + TDuration::MilliSeconds(1));

        actor->TestSync.WaitForAndIncrement(6);

        // TODO: stop in destructor
        scheduler.Stop();
    }
}
