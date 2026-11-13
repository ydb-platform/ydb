#include "test_runtime.h"

#include <library/cpp/testing/unittest/registar.h>

#include <chrono>
#include <future>
#include <thread>

using namespace NActors;

namespace {

struct TEvValue : TEventLocal<TEvValue, EventSpaceBegin(TEvents::ES_PRIVATE)> {
    ui64 Value;
    explicit TEvValue(ui64 value) : Value(value) {}
};

class TReceiver : public TActor<TReceiver> {
public:
    explicit TReceiver(std::function<void(ui64)> receive)
        : TActor(&TThis::StateWork)
        , ReceiveValue(std::move(receive))
    {}

    STFUNC(StateWork) {
        if (ev->GetTypeRewrite() == TEvValue::EventType) {
            ReceiveValue(ev->Get<TEvValue>()->Value);
        }
    }

private:
    std::function<void(ui64)> ReceiveValue;
};

void EnableTimers(TTestActorRuntimeBase& runtime) {
    runtime.SetScheduledEventFilter([](auto&&, auto&&, auto&&, auto&&) { return false; });
}

void CheckRescheduleTimeout(bool fast) {
    TTestActorRuntimeBase runtime;
    runtime.SetFastSimulatedTime(fast);
    runtime.Initialize();
    const auto actor = runtime.Register(new TReceiver([](ui64) { UNIT_FAIL("Rescheduled event was delivered"); }));
    runtime.SetReschedulingDelay(TDuration::MilliSeconds(10));
    runtime.SetDispatchTimeout(TDuration::MilliSeconds(30));
    runtime.SetDispatchedEventsLimit(100);
    ui64 observed = 0;
    runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
        if (ev->GetTypeRewrite() != TEvValue::EventType || ev->GetRecipientRewrite() != actor) {
            return TTestActorRuntimeBase::EEventAction::PROCESS;
        }
        ++observed;
        return TTestActorRuntimeBase::EEventAction::RESCHEDULE;
    });
    runtime.Send(new IEventHandle(actor, {}, new TEvValue(1)), 0, true);
    TDispatchOptions options;
    options.CustomFinalCondition = [] { return false; };
    // No simulated-time deadline: the dispatch budget must still stop the loop
    // before the independent limit of 100 event deliveries.
    UNIT_ASSERT_EXCEPTION(runtime.DispatchEvents(options), TEmptyEventQueueException);
    UNIT_ASSERT_VALUES_EQUAL(observed, 3);
    UNIT_ASSERT_VALUES_EQUAL(runtime.GetCurrentTime(), TInstant::Zero() + TDuration::MilliSeconds(20));
}

} // namespace

Y_UNIT_TEST_SUITE(TestRuntimeFastTime) {
    Y_UNIT_TEST(EnabledByDefaultAndCanBeDisabledPerRuntime) {
        TTestActorRuntimeBase runtime;
        UNIT_ASSERT(runtime.SetFastSimulatedTime(false));
        UNIT_ASSERT(!runtime.SetFastSimulatedTime(true));
    }

    Y_UNIT_TEST(RescheduleExhaustsDispatchBudgetInFastMode) {
        CheckRescheduleTimeout(true);
    }

    Y_UNIT_TEST(RescheduleExhaustsDispatchBudgetInLegacyMode) {
        CheckRescheduleTimeout(false);
    }

    Y_UNIT_TEST(TimerChainPreservesValuesAndModelTime) {
        for (bool fast : {false, true}) {
            TTestActorRuntimeBase runtime;
            runtime.SetFastSimulatedTime(fast);
            EnableTimers(runtime);
            runtime.Initialize();
            runtime.SetDispatchTimeout(TDuration::MilliSeconds(20));
            TVector<ui64> values;
            TVector<ui64> times;
            TActorId actor;
            actor = runtime.Register(new TReceiver([&](ui64 value) {
                values.push_back(value);
                times.push_back(runtime.GetCurrentTime().MilliSeconds());
                if (value < 6) {
                    runtime.Schedule(new IEventHandle(actor, {}, new TEvValue(value + 1)), TDuration::MilliSeconds(5));
                }
            }));
            runtime.EnableScheduleForActor(actor);
            runtime.Send(new IEventHandle(actor, {}, new TEvValue(1)), 0, true);
            TDispatchOptions options;
            options.CustomFinalCondition = [&] { return values.size() == 6; };
            UNIT_ASSERT(runtime.DispatchEvents(options, TDuration::Seconds(1)));
            UNIT_ASSERT_VALUES_EQUAL(values, (TVector<ui64>{1, 2, 3, 4, 5, 6}));
            UNIT_ASSERT_VALUES_EQUAL(times, (TVector<ui64>{0, 5, 10, 15, 20, 25}));
        }
    }

    Y_UNIT_TEST(QuietDoesNotConsumeTimers) {
        TTestActorRuntimeBase runtime;
        EnableTimers(runtime);
        runtime.Initialize();
        const auto actor = runtime.Register(new TReceiver([](ui64) { UNIT_FAIL("Quiet dispatch consumed a timer"); }));
        runtime.Schedule(new IEventHandle(actor, {}, new TEvValue(1)), TDuration::Seconds(1));
        runtime.SetDispatchTimeout(TDuration::MilliSeconds(20));
        TDispatchOptions options;
        options.Quiet = true;
        options.CustomFinalCondition = [] { return false; };
        UNIT_ASSERT_EXCEPTION(runtime.DispatchEvents(options), TEmptyEventQueueException);
        UNIT_ASSERT_VALUES_EQUAL(runtime.GetCurrentTime(), TInstant::Zero());
        UNIT_ASSERT_VALUES_EQUAL(runtime.CaptureScheduledEvents().size(), 1);
    }

    Y_UNIT_TEST(UnselectedTimerStillExhaustsDispatchBudget) {
        for (bool fast : {false, true}) {
            TTestActorRuntimeBase runtime;
            runtime.SetFastSimulatedTime(fast);
            EnableTimers(runtime);
            runtime.Initialize();
            const auto actor = runtime.Register(new TReceiver([](ui64) { UNIT_FAIL("Unselected timer was delivered"); }));
            runtime.EnableScheduleForActor(actor);
            runtime.Schedule(new IEventHandle(actor, {}, new TEvValue(1)), TDuration::Seconds(1));
            runtime.SetScheduledEventsSelectorFunc([](TTestActorRuntimeBase&, TScheduledEventsList&, TEventsList&) {});
            runtime.SetDispatchTimeout(TDuration::MilliSeconds(30));
            TDispatchOptions options;
            options.CustomFinalCondition = [] { return false; };
            UNIT_ASSERT_EXCEPTION(runtime.DispatchEvents(options), TEmptyEventQueueException);
            UNIT_ASSERT_VALUES_EQUAL(runtime.GetCurrentTime(), TInstant::Zero());
            UNIT_ASSERT_VALUES_EQUAL(runtime.CaptureScheduledEvents().size(), 1);
        }
    }

    Y_UNIT_TEST(IdleRuntimeAcceptsLateExternalEvent) {
        TTestActorRuntimeBase runtime;
        runtime.Initialize();
        // Keep an empty edge mailbox alive while waiting for external work.
        // Dispatch exits when its mailbox map becomes empty.
        runtime.AllocateEdgeActor();
        bool delivered = false;
        const auto actor = runtime.Register(new TReceiver([&](ui64) { delivered = true; }));
        auto* system = runtime.GetActorSystem(0);
        std::promise<void> start;
        auto ready = start.get_future();
        auto sender = std::async(std::launch::async, [&] {
            if (ready.wait_for(std::chrono::seconds(2)) != std::future_status::ready) return;
            // Deliberately arrive after several idle waits, with no modelled work.
            std::this_thread::sleep_for(std::chrono::milliseconds(30));
            system->Send(new IEventHandle(actor, {}, new TEvValue(1)));
        });
        bool started = false;
        runtime.SetDispatchTimeout(TDuration::Seconds(2));
        TDispatchOptions options;
        options.CustomFinalCondition = [&] {
            if (!started) {
                started = true;
                start.set_value();
            }
            return delivered;
        };
        UNIT_ASSERT(runtime.DispatchEvents(options));
        sender.get();
        UNIT_ASSERT(delivered);
        UNIT_ASSERT_VALUES_EQUAL(runtime.GetCurrentTime(), TInstant::Zero());
    }

    Y_UNIT_TEST(SynchronizedExternalReplyPrecedesNextTimer) {
        for (bool fast : {false, true}) {
            TTestActorRuntimeBase runtime;
            runtime.SetFastSimulatedTime(fast);
            EnableTimers(runtime);
            runtime.Initialize();
            runtime.SetDispatchTimeout(TDuration::Seconds(2));
            std::promise<void> start;
            auto ready = start.get_future();
            std::promise<void> sent;
            auto sentFuture = sent.get_future();
            TVector<ui64> values;
            TVector<ui64> times;
            TActorId actor;
            actor = runtime.Register(new TReceiver([&](ui64 value) {
                values.push_back(value);
                times.push_back(runtime.GetCurrentTime().MilliSeconds());
                if (value == 1) {
                    runtime.Schedule(new IEventHandle(actor, {}, new TEvValue(2)), TDuration::MilliSeconds(5));
                } else if (value == 2) {
                    start.set_value();
                    // Actor callbacks release the runtime mutex. Establish that
                    // Send has queued the reply before advancing simulated time;
                    // a real-time pause alone cannot guarantee this ordering.
                    UNIT_ASSERT(sentFuture.wait_for(std::chrono::seconds(2)) == std::future_status::ready);
                    sentFuture.get();
                    runtime.Schedule(new IEventHandle(actor, {}, new TEvValue(4)), TDuration::MilliSeconds(5));
                }
            }));
            runtime.EnableScheduleForActor(actor);
            auto* system = runtime.GetActorSystem(0);
            auto sender = std::async(std::launch::async, [&] {
                if (ready.wait_for(std::chrono::seconds(2)) != std::future_status::ready) return false;
                system->Send(new IEventHandle(actor, {}, new TEvValue(3)));
                sent.set_value();
                return true;
            });
            runtime.Send(new IEventHandle(actor, {}, new TEvValue(1)), 0, true);
            TDispatchOptions options;
            options.CustomFinalCondition = [&] { return values.size() == 4; };
            UNIT_ASSERT(runtime.DispatchEvents(options, TDuration::Seconds(1)));
            UNIT_ASSERT(sender.get());
            UNIT_ASSERT_VALUES_EQUAL(values, (TVector<ui64>{1, 2, 3, 4}));
            UNIT_ASSERT_VALUES_EQUAL(times, (TVector<ui64>{0, 5, 5, 10}));
        }
    }
}
