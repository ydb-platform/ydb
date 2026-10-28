#include "actor_coroutine.h"
#include "actorsystem.h"
#include "executor_pool_basic.h"
#include "scheduler_actor.h"
#include "scheduler_basic.h"
#include "events.h"
#include "event_local.h"
#include "hfunc.h"
#include <ydb/library/actors/interconnect/poller/poller_actor.h>
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

#ifdef __linux__
Y_UNIT_TEST_SUITE(SchedulerActorLinux) {
    struct TObservation {
        TManualEvent Done;
        TVector<ui64> Delivered;
        TAtomic Destroyed = 0;
        TAtomic CancelledDestroyed = 0;
        TAtomic PendingDestroyed = 0;
    };

    struct TEvTimer : TEventLocal<TEvTimer, EventSpaceBegin(TEvents::ES_PRIVATE)> {
        const ui64 Id;
        TAtomic& Destroyed;

        TEvTimer(ui64 id, TAtomic& destroyed)
            : Id(id)
            , Destroyed(destroyed)
        {}

        ~TEvTimer() override {
            AtomicIncrement(Destroyed);
        }
    };

    enum class EActorTimers {
        None,
        Single,
        Ordered,
    };

    class TReceiver : public TActorBootstrapped<TReceiver> {
        TObservation& Observation;
        const ui32 Expected;
        const EActorTimers ActorTimers;

    public:
        TReceiver(TObservation& observation, ui32 expected, EActorTimers actorTimers = EActorTimers::None)
            : Observation(observation)
            , Expected(expected)
            , ActorTimers(actorTimers)
        {}

        void Bootstrap() {
            Become(&TThis::StateWork);
            if (ActorTimers == EActorTimers::Single) {
                // Exercise the executor's schedule reader, in addition to the
                // actor system reader used by the other tests.
                Schedule(TDuration::MilliSeconds(10), new TEvTimer(1, Observation.Destroyed));
            } else if (ActorTimers == EActorTimers::Ordered) {
                const auto now = TActivationContext::Monotonic();
                // One executor worker keeps the scheduler from consuming a
                // partial batch if this handler is preempted by the OS.
                Schedule(now + TDuration::MilliSeconds(1500), new TEvTimer(3, Observation.Destroyed));
                Schedule(now + TDuration::MilliSeconds(100), new TEvTimer(2, Observation.Destroyed));
                Schedule(now, new TEvTimer(1, Observation.Destroyed));
            }
        }

        void Handle(TEvTimer::TPtr& ev) {
            Observation.Delivered.push_back(ev->Get()->Id);
            if (Observation.Delivered.size() == Expected) {
                Observation.Done.Signal();
            }
        }

        STRICT_STFUNC(StateWork, hFunc(TEvTimer, Handle))
    };

    class TFixture {
    public:
        THolder<TActorSystem> System;

        TFixture() {
            TSchedulerConfig config;
            config.UseSchedulerActor = true;
            auto setup = MakeHolder<TActorSystemSetup>();
            setup->NodeId = 0;
            setup->ExecutorsCount = 1;
            setup->Executors.Reset(new TAutoPtr<IExecutorPool>[1]);
            setup->Executors[0] = new TBasicExecutorPool(0, 1, 10, "scheduler-test");

            THolder<IActor> poller(CreatePollerActor());
            THolder<IActor> scheduler(CreateSchedulerActor(config));
            UNIT_ASSERT(poller);
            UNIT_ASSERT(scheduler);
            setup->LocalServices.emplace_back(MakePollerActorId(),
                TActorSetupCmd(poller.Release(), TMailboxType::ReadAsFilled, 0));
            setup->LocalServices.emplace_back(MakeSchedulerActorId(),
                TActorSetupCmd(scheduler.Release(), TMailboxType::ReadAsFilled, 0));
            // On Linux this is TMockSchedulerThread: it initializes the clocks
            // but cannot deliver timers. Both factories must use the same config.
            setup->Scheduler = CreateSchedulerThread(config);
            System = MakeHolder<TActorSystem>(setup);
            System->Start();
        }

        ~TFixture() {
            System->Stop();
        }

        void Schedule(TActorId recipient, TMonotonic deadline, ui64 id,
                      TAtomic& destroyed, ISchedulerCookie* cookie = nullptr) {
            System->Schedule(deadline,
                new IEventHandle(recipient, TActorId(), new TEvTimer(id, destroyed)), cookie);
        }

        void WaitAndStop(TObservation& observation) {
            const bool delivered = observation.Done.WaitT(TDuration::Seconds(30));
            // Stop workers before inspecting actor-owned observations, including
            // on failure. Observation must outlive fixture teardown.
            System->Stop();
            UNIT_ASSERT_C(delivered, "Scheduler actor did not deliver timers within 30 seconds");
        }
    };

    Y_UNIT_TEST(DeliversActorTimers) {
        TObservation observation;
        TFixture fixture;
        fixture.System->Register(new TReceiver(observation, 1, EActorTimers::Single));
        fixture.WaitAndStop(observation);
        UNIT_ASSERT_VALUES_EQUAL(observation.Delivered.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(observation.Delivered[0], 1);
        UNIT_ASSERT_VALUES_EQUAL(AtomicGet(observation.Destroyed), 1);
    }

    Y_UNIT_TEST(OrdersDifferentDeadlines) {
        TObservation observation;
        TFixture fixture;
        // Enqueue in reverse deadline order. The late timer crosses the
        // scheduler's ~second buckets; no exact delivery times are asserted.
        fixture.System->Register(new TReceiver(observation, 3, EActorTimers::Ordered));
        fixture.WaitAndStop(observation);
        UNIT_ASSERT_VALUES_EQUAL(observation.Delivered.size(), 3);
        for (ui32 i = 0; i < 3; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(observation.Delivered[i], i + 1);
        }
        UNIT_ASSERT_VALUES_EQUAL(AtomicGet(observation.Destroyed), 3);
    }

    Y_UNIT_TEST(CancelsWithSchedulerCookie) {
        TObservation observation;
        TFixture fixture;
        const auto recipient = fixture.System->Register(new TReceiver(observation, 1));
        TSchedulerCookieHolder cancelled(ISchedulerCookie::Make2Way());
        auto* cookie = cancelled.Get();
        // Win cancellation before publishing: this avoids racing a timer on a
        // slow builder and still exercises the scheduler's cancelled branch.
        UNIT_ASSERT(cancelled.Detach());
        const auto now = fixture.System->Monotonic();
        fixture.Schedule(recipient, now, 99, observation.CancelledDestroyed, cookie);
        TSchedulerCookieHolder delivered(ISchedulerCookie::Make2Way());
        fixture.Schedule(recipient, now + TDuration::MilliSeconds(20), 1,
            observation.Destroyed, delivered.Get());
        fixture.WaitAndStop(observation);
        UNIT_ASSERT_VALUES_EQUAL(observation.Delivered.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(observation.Delivered[0], 1);
        UNIT_ASSERT_VALUES_EQUAL(AtomicGet(observation.CancelledDestroyed), 1);
        UNIT_ASSERT(!delivered.Get()->IsArmed());
        UNIT_ASSERT(!delivered.Detach());
    }

    Y_UNIT_TEST(CancelsAlreadyQueuedTimer) {
        TObservation observation;
        TSchedulerCookieHolder cancelled(ISchedulerCookie::Make2Way());
        TFixture fixture;
        const auto recipient = fixture.System->Register(new TReceiver(observation, 1));
        const auto now = fixture.System->Monotonic();
        fixture.Schedule(recipient, now + TDuration::Hours(24), 99,
            observation.CancelledDestroyed, cancelled.Get());
        // Both entries use the same FIFO reader. Delivery of the marker proves
        // the scheduler consumed the future timer while its cookie was armed.
        fixture.Schedule(recipient, now, 1, observation.Destroyed);
        const bool consumed = observation.Done.WaitT(TDuration::Seconds(30));
        if (!consumed) {
            fixture.System->Stop();
        }
        UNIT_ASSERT(consumed);
        UNIT_ASSERT(cancelled.Get()->IsArmed());
        UNIT_ASSERT_VALUES_EQUAL(AtomicGet(observation.CancelledDestroyed), 0);
        UNIT_ASSERT(cancelled.Detach());
        fixture.System->Stop();
        fixture.System->Cleanup();
        UNIT_ASSERT_VALUES_EQUAL(observation.Delivered.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(observation.Delivered[0], 1);
        UNIT_ASSERT_VALUES_EQUAL(AtomicGet(observation.CancelledDestroyed), 1);
        UNIT_ASSERT_VALUES_EQUAL(AtomicGet(observation.Destroyed), 1);
    }

    Y_UNIT_TEST(StopsWithPendingTimers) {
        TObservation observation;
        TSchedulerCookieHolder pending(ISchedulerCookie::Make2Way());
        {
            TFixture fixture;
            const auto recipient = fixture.System->Register(new TReceiver(observation, 1));
            const auto now = fixture.System->Monotonic();
            fixture.Schedule(recipient, now + TDuration::Hours(24), 99,
                observation.PendingDestroyed, pending.Get());
            // A delivered timer from the same reader proves the scheduler has
            // consumed the earlier pending entry before we stop it.
            fixture.Schedule(recipient, now, 1, observation.Destroyed);
            UNIT_ASSERT(observation.Done.WaitT(TDuration::Seconds(30)));
            UNIT_ASSERT_VALUES_EQUAL(AtomicGet(observation.PendingDestroyed), 0);
            UNIT_ASSERT(pending.Get()->IsArmed());
            fixture.System->Stop();
            // Stop may already clean executor mailboxes. Require reclamation
            // after full teardown, without prescribing its intermediate phase.
            fixture.System->Cleanup();
            UNIT_ASSERT_VALUES_EQUAL(observation.Delivered.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(observation.Delivered[0], 1);
            UNIT_ASSERT_VALUES_EQUAL(AtomicGet(observation.Destroyed), 1);
            UNIT_ASSERT_VALUES_EQUAL(AtomicGet(observation.PendingDestroyed), 1);
            UNIT_ASSERT(!pending.Get()->IsArmed());
        }
        UNIT_ASSERT(!pending.Detach());
    }
}
#endif
