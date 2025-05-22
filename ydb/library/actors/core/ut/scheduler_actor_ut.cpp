#include "actor_coroutine.h"
#include "actorsystem.h"
#include "executor_pool_basic.h"
#include "scheduler_actor.h"
#include "scheduler_basic.h"
#include "events.h"
#include "event_local.h"
#include "hfunc.h"
#include <ydb/library/actors/interconnect/poller_actor.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/system/sanitizers.h>

using namespace NActors;

Y_UNIT_TEST_SUITE(SchedulerActor) {
    class TTestActor: public TActorBootstrapped<TTestActor> {
        TManualEvent& DoneEvent;
        TAtomic& EventsProcessed;
        TInstant LastWakeup;
        const TAtomicBase EventsTotalCount;
        const TDuration ScheduleDelta;

    public:
        TTestActor(TManualEvent& doneEvent, TAtomic& eventsProcessed, TAtomicBase eventsTotalCount, ui32 scheduleDeltaMs)
            : DoneEvent(doneEvent)
            , EventsProcessed(eventsProcessed)
            , EventsTotalCount(eventsTotalCount)
            , ScheduleDelta(TDuration::MilliSeconds(scheduleDeltaMs))
        {
        }

        void Bootstrap(const TActorContext& ctx) {
            LastWakeup = ctx.Now();
            Become(&TThis::StateFunc);
            ctx.Schedule(ScheduleDelta, new TEvents::TEvWakeup());
        }

        void Handle(TEvents::TEvWakeup::TPtr& /*ev*/, const TActorContext& ctx) {
            const TInstant now = ctx.Now();
            UNIT_ASSERT(now - LastWakeup >= ScheduleDelta);
            LastWakeup = now;

            if (AtomicIncrement(EventsProcessed) == EventsTotalCount) {
                DoneEvent.Signal();
            } else {
                ctx.Schedule(ScheduleDelta, new TEvents::TEvWakeup());
            }
        }

        STRICT_STFUNC(StateFunc, {HFunc(TEvents::TEvWakeup, Handle)})
    };

    void Test(TAtomicBase eventsTotalCount, ui32 scheduleDeltaMs) {
        THolder<TActorSystemSetup> setup = MakeHolder<TActorSystemSetup>();
        setup->NodeId = 0;
        setup->ExecutorsCount = 1;
        setup->Executors.Reset(new TAutoPtr<IExecutorPool>[setup->ExecutorsCount]);
        for (ui32 i = 0; i < setup->ExecutorsCount; ++i) {
            setup->Executors[i] = new TBasicExecutorPool(i, 5, 10, "basic");
        }
        // create poller actor (whether platform supports it)
        TActorId pollerActorId;
        if (IActor* poller = CreatePollerActor()) {
            pollerActorId = MakePollerActorId();
            setup->LocalServices.emplace_back(pollerActorId, TActorSetupCmd(poller, TMailboxType::ReadAsFilled, 0));
        }
        TActorId schedulerActorId;
        if (IActor* schedulerActor = CreateSchedulerActor(TSchedulerConfig())) {
            schedulerActorId = MakeSchedulerActorId();
            setup->LocalServices.emplace_back(schedulerActorId, TActorSetupCmd(schedulerActor, TMailboxType::ReadAsFilled, 0));
        }
        setup->Scheduler = CreateSchedulerThread(TSchedulerConfig());

        TActorSystem actorSystem(setup);

        actorSystem.Start();

        TManualEvent doneEvent;
        TAtomic eventsProcessed = 0;
        actorSystem.Register(new TTestActor(doneEvent, eventsProcessed, eventsTotalCount, scheduleDeltaMs));
        doneEvent.WaitI();

        UNIT_ASSERT(AtomicGet(eventsProcessed) == eventsTotalCount);

        actorSystem.Stop();
    }

    Y_UNIT_TEST(LongEvents) {
        Test(10, 500);
    }

    Y_UNIT_TEST(MediumEvents) {
        Test(100, 50);
    }

    Y_UNIT_TEST(QuickEvents) {
        Test(1000, 5);
    }
}
