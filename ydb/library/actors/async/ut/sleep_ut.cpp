#include <ydb/library/actors/async/sleep.h>
#include "common.h"
#include <ydb/library/actors/async/yield.h>

namespace NAsyncTest {

    Y_UNIT_TEST_SUITE(Sleep) {

        Y_UNIT_TEST(ZeroDelay) {
            bool finished = false;
            bool finishedNormally = false;
            int resumed = 0;
            int scheduled = 0;
            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto observer = runtime.AddObserver([&](auto& ev) {
                if (ev->GetTypeRewrite() == TEvents::TSystem::ResumeRunnable) {
                    ++resumed;
                }
            });
            runtime.SetScheduledEventFilter([&](auto&, auto& ev, auto, auto&) {
                if (ev->GetTypeRewrite() == TEvents::TSystem::ResumeRunnable) {
                    ++scheduled;
                }
                return false;
            });

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                Y_DEFER { finished = true; };

                co_await AsyncSleepFor(TDuration::Zero());
                co_await AsyncSleepUntil(TMonotonic::Zero());
                co_await AsyncSleepUntil(TInstant::Zero());
                co_await AsyncYield();

                finishedNormally = true;
            });

            // We expect only bootstrap to be handled, all yields must be enqueued
            UNIT_ASSERT(!finished);
            UNIT_ASSERT_VALUES_EQUAL(scheduled, 0);
            UNIT_ASSERT_VALUES_EQUAL(resumed, 0);

            runtime.DispatchEvents();

            UNIT_ASSERT(finished);
            UNIT_ASSERT(finishedNormally);

            UNIT_ASSERT_VALUES_EQUAL(scheduled, 0);
            UNIT_ASSERT_VALUES_EQUAL(resumed, 4);
        }

        Y_UNIT_TEST(InfiniteDelay) {
            bool finished = false;
            bool finishedNormally = false;
            int scheduled = 0;
            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            runtime.SetScheduledEventFilter([&](auto&, auto& ev, auto, auto&) {
                if (ev->GetTypeRewrite() == TEvents::TSystem::ResumeRunnable) {
                    ++scheduled;
                }
                return false;
            });

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                Y_DEFER { finished = true; };

                co_await AsyncSleepFor(TDuration::Max());

                finishedNormally = true;
            });

            // We must wait indefinitely without scheduling anything
            UNIT_ASSERT(!finished);
            UNIT_ASSERT_VALUES_EQUAL(scheduled, 0);

            // Sanity check, no wakeup later
            runtime.SimulateSleep(TDuration::MilliSeconds(10));
            UNIT_ASSERT(!finished);
            UNIT_ASSERT_VALUES_EQUAL(scheduled, 0);

            // This delay must be cancellable
            actor.Poison();
            UNIT_ASSERT(finished);
            UNIT_ASSERT(!finishedNormally);
            UNIT_ASSERT(state.Destroyed);
        }

        Y_UNIT_TEST(PassAwayBeforeYield) {
            bool finished = false;
            bool finishedNormally = false;
            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto* self) -> async<void> {
                Y_DEFER { finished = true; };

                self->PassAway();

                co_await AsyncYield();

                finishedNormally = true;
            });

            // We expect actor to immediately stop
            UNIT_ASSERT(finished);
            UNIT_ASSERT(!finishedNormally);
            UNIT_ASSERT(state.Destroyed);
        }

        Y_UNIT_TEST(PassAwayAfterYield) {
            bool finished = false;
            bool finishedNormally = false;
            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                Y_DEFER { finished = true; };

                co_await AsyncYield();

                finishedNormally = true;
            });

            UNIT_ASSERT(!finished);

            actor.Poison();

            // We expect yield to immediately cancel
            UNIT_ASSERT(finished);
            UNIT_ASSERT(!finishedNormally);
            UNIT_ASSERT(state.Destroyed);
        }

        Y_UNIT_TEST(Delay) {
            bool finished = false;
            bool finishedNormally = false;
            int scheduled = 0;
            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            runtime.SetScheduledEventFilter([&](auto&, auto& ev, auto, auto&) {
                if (ev->GetTypeRewrite() == TEvents::TSystem::ResumeRunnable) {
                    ++scheduled;
                }
                return false;
            });

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                Y_DEFER { finished = true; };

                co_await AsyncSleepFor(TDuration::MilliSeconds(1));

                finishedNormally = true;
            });

            UNIT_ASSERT(!finished);
            UNIT_ASSERT_VALUES_EQUAL(scheduled, 1);

            runtime.WaitFor("sleep to finish", [&]{ return finished; }, TDuration::Seconds(1), /* quiet */ true);

            UNIT_ASSERT(finished);
            UNIT_ASSERT(finishedNormally);
        }

        Y_UNIT_TEST(DelayCancelWhileScheduled) {
            bool finished = false;
            bool finishedNormally = false;
            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                Y_DEFER { finished = true; };

                co_await AsyncSleepFor(TDuration::MilliSeconds(2));

                finishedNormally = true;
            });
            UNIT_ASSERT(!finished);

            runtime.SimulateSleep(TDuration::MilliSeconds(1));
            UNIT_ASSERT(!finished);

            actor.Poison();
            UNIT_ASSERT(finished);
            UNIT_ASSERT(!finishedNormally);
            UNIT_ASSERT(state.Destroyed);

            runtime.SimulateSleep(TDuration::MilliSeconds(10));
        }

        Y_UNIT_TEST(DelayDestroyedBeforeDelivery) {
            bool finished = false;
            bool finishedNormally = false;
            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                Y_DEFER { finished = true; };

                co_await AsyncSleepFor(TDuration::MilliSeconds(2));

                finishedNormally = true;
            });

            UNIT_ASSERT(!finished);

            TEvents::TEvResumeRunnable::TPtr captured;
            auto observer = runtime.AddObserver<TEvents::TEvResumeRunnable>([&](auto& ev) {
                captured = std::move(ev);
            });

            runtime.WaitFor("captured event", [&]{ return !!captured; }, TDuration::Seconds(1), /* quiet */ true);

            // Destroy the event (as if scheduler is shutting down)
            captured.Reset();

            // Destroy mailboxes (as if actor system is shutting down)
            runtime.CleanupNode();

            UNIT_ASSERT(finished);
            UNIT_ASSERT(!finishedNormally);
            UNIT_ASSERT(state.Destroyed);
        }

    } // Y_UNIT_TEST_SUITE(Sleep)

} // namespace NAsyncTest
