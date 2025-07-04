#include <ydb/library/actors/async/sleep.h>
#include "common.h"
#include <ydb/library/actors/async/yield.h>

namespace NAsyncTest {

    Y_UNIT_TEST_SUITE(Sleep) {

        Y_UNIT_TEST(ZeroDelay) {
            TVector<TString> sequence;
            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto observer = runtime.AddObserver([&](auto& ev) {
                if (ev->GetTypeRewrite() == TEvents::TSystem::ResumeRunnable) {
                    sequence.push_back("resume received");
                }
            });

            runtime.SetScheduledEventFilter([&](auto&, auto& ev, auto, auto&) {
                if (ev->GetTypeRewrite() == TEvents::TSystem::ResumeRunnable) {
                    sequence.push_back("resume scheduled");
                }
                return false;
            });

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                sequence.push_back("started");
                Y_DEFER { sequence.push_back("finished"); };

                co_await AsyncSleepFor(TDuration::Zero());
                sequence.push_back("returned from 1");
                co_await AsyncSleepUntil(TMonotonic::Zero());
                sequence.push_back("returned from 2");
                co_await AsyncSleepUntil(TInstant::Zero());
                sequence.push_back("returned from 3");
                co_await AsyncYield();
                sequence.push_back("returned from 4");
            });

            // We expect only bootstrap to be handled, all sleeps must be enqueued
            ASYNC_ASSERT_SEQUENCE(sequence, "started");

            runtime.DispatchEvents();
            ASYNC_ASSERT_SEQUENCE(sequence,
                "resume received", "returned from 1",
                "resume received", "returned from 2",
                "resume received", "returned from 3",
                "resume received", "returned from 4",
                "finished");
        }

        Y_UNIT_TEST(InfiniteDelay) {
            TVector<TString> sequence;
            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            runtime.SetScheduledEventFilter([&](auto&, auto& ev, auto, auto&) {
                if (ev->GetTypeRewrite() == TEvents::TSystem::ResumeRunnable) {
                    sequence.push_back("resume scheduled");
                }
                return false;
            });

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                sequence.push_back("started");
                Y_DEFER { sequence.push_back("finished"); };

                co_await AsyncSleepFor(TDuration::Max());
                sequence.push_back("returned from infinite sleep");
            });

            // We must wait indefinitely without scheduling anything
            ASYNC_ASSERT_SEQUENCE(sequence, "started");

            // Sanity check, no wakeups later
            runtime.SimulateSleep(TDuration::MilliSeconds(10));
            ASYNC_ASSERT_SEQUENCE_EMPTY(sequence);

            // This delay must be cancellable
            actor.Poison();
            ASYNC_ASSERT_SEQUENCE(sequence, "finished");
            UNIT_ASSERT(state.Destroyed);
        }

        Y_UNIT_TEST(PassAwayBeforeYield) {
            TVector<TString> sequence;
            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto* self) -> async<void> {
                sequence.push_back("started");
                Y_DEFER { sequence.push_back("finished"); };

                self->PassAway();

                co_await AsyncYield();
                sequence.push_back("returned from yield");
            });

            // We expect actor to immediately stop
            ASYNC_ASSERT_SEQUENCE(sequence, "started", "finished");
            UNIT_ASSERT(state.Destroyed);
        }

        Y_UNIT_TEST(PassAwayAfterYield) {
            TVector<TString> sequence;
            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                sequence.push_back("started");
                Y_DEFER { sequence.push_back("finished"); };

                co_await AsyncYield();
                sequence.push_back("returned from yield");
            });

            ASYNC_ASSERT_SEQUENCE(sequence, "started");

            actor.Poison();

            // We expect yield to immediately cancel
            ASYNC_ASSERT_SEQUENCE(sequence, "finished");
        }

        Y_UNIT_TEST(Delay) {
            TVector<TString> sequence;
            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto observer = runtime.AddObserver([&](auto& ev) {
                if (ev->GetTypeRewrite() == TEvents::TSystem::ResumeRunnable) {
                    sequence.push_back("resume received");
                }
            });

            runtime.SetScheduledEventFilter([&](auto&, auto& ev, auto, auto&) {
                if (ev->GetTypeRewrite() == TEvents::TSystem::ResumeRunnable) {
                    sequence.push_back("resume scheduled");
                }
                return false;
            });

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                sequence.push_back("started");
                Y_DEFER { sequence.push_back("finished"); };

                co_await AsyncSleepFor(TDuration::MilliSeconds(1));
                sequence.push_back("returned from sleep");
            });

            ASYNC_ASSERT_SEQUENCE(sequence, "started", "resume scheduled");

            runtime.WaitFor(Cdbg, "sleep to finish", [&]{ return !sequence.empty(); }, TDuration::Seconds(1));

            ASYNC_ASSERT_SEQUENCE(sequence, "resume received", "returned from sleep", "finished");
        }

        Y_UNIT_TEST(DelayCancelWhileScheduled) {
            TVector<TString> sequence;
            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                sequence.push_back("started");
                Y_DEFER { sequence.push_back("finished"); };

                co_await AsyncSleepFor(TDuration::MilliSeconds(2));
                sequence.push_back("returned from sleep");
            });
            ASYNC_ASSERT_SEQUENCE(sequence, "started");

            runtime.SimulateSleep(TDuration::MilliSeconds(1));
            ASYNC_ASSERT_SEQUENCE_EMPTY(sequence);

            actor.Poison();
            ASYNC_ASSERT_SEQUENCE(sequence, "finished");
            UNIT_ASSERT(state.Destroyed);

            runtime.SimulateSleep(TDuration::MilliSeconds(10));
            ASYNC_ASSERT_SEQUENCE_EMPTY(sequence);
        }

        Y_UNIT_TEST(DelayDestroyedBeforeDelivery) {
            TVector<TString> sequence;
            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                sequence.push_back("started");
                Y_DEFER { sequence.push_back("finished"); };

                co_await AsyncSleepFor(TDuration::MilliSeconds(2));
                sequence.push_back("returned from sleep");
            });

            ASYNC_ASSERT_SEQUENCE(sequence, "started");

            TEvents::TEvResumeRunnable::TPtr captured;
            auto observer = runtime.AddObserver<TEvents::TEvResumeRunnable>([&](auto& ev) {
                captured = std::move(ev);
                sequence.push_back("resume captured");
            });

            runtime.WaitFor(Cdbg, "captured event", [&]{ return !sequence.empty(); }, TDuration::Seconds(1));
            ASYNC_ASSERT_SEQUENCE(sequence, "resume captured");

            // Destroy the event (as if scheduler is shutting down)
            captured.Reset();

            // Destroy mailboxes (as if actor system is shutting down)
            runtime.CleanupNode();

            ASYNC_ASSERT_SEQUENCE(sequence, "finished");
            UNIT_ASSERT(state.Destroyed);
        }

    } // Y_UNIT_TEST_SUITE(Sleep)

} // namespace NAsyncTest
