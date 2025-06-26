#include "common.h"
#include <ydb/library/actors/async/sleep.h>

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

                co_await AsyncYield();
                co_await AsyncSleepFor(TDuration::Zero());
                co_await AsyncSleepUntil(TMonotonic::Zero());
                co_await AsyncSleepUntil(TInstant::Zero());

                finishedNormally = true;
            });
            Y_UNUSED(actor);

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
            runtime.Poison(actor);
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
            Y_UNUSED(actor);

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

            runtime.Poison(actor);

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
            Y_UNUSED(actor);

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

            runtime.Poison(actor);
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
            Y_UNUSED(actor);

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

        Y_UNIT_TEST(Timeout) {
            bool finished = false;
            bool finishedNormally = false;
            bool finishedWithTimeout = false;
            std::coroutine_handle<> resume, cancel;

            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                Y_DEFER { finished = true; };

                try {
                    co_await WithTimeout(TDuration::MilliSeconds(10), [&]() -> async<void> {
                        co_await TSuspendAwaiter{ &resume, &cancel };
                    });
                } catch (const TAsyncTimeout&) {
                    finishedWithTimeout = true;
                    throw;
                }

                finishedNormally = true;
            });
            Y_UNUSED(actor);

            UNIT_ASSERT(!finished);
            UNIT_ASSERT(resume);
            UNIT_ASSERT(!cancel);

            // Eventually timeout must request awaiter to cancel
            runtime.SimulateSleep(TDuration::MilliSeconds(15));
            UNIT_ASSERT(!finished);
            UNIT_ASSERT(cancel);

            // Confirm cancellation, it must throw a timeout exception
            runtime.ResumeCoroutine(actor, cancel);
            UNIT_ASSERT(finished);
            UNIT_ASSERT(finishedWithTimeout);
            UNIT_ASSERT(!state.Destroyed);
        }

        Y_UNIT_TEST(TimeoutResumeImmediately) {
            bool finished = false;
            bool finishedNormally = false;
            bool finishedWithTimeout = false;

            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                Y_DEFER { finished = true; };

                try {
                    int result = co_await WithTimeout(TDuration::MilliSeconds(10), [&]() -> async<int> {
                        co_return 42;
                    });
                    UNIT_ASSERT_VALUES_EQUAL(result, 42);
                } catch (const TAsyncTimeout&) {
                    finishedWithTimeout = true;
                    throw;
                }

                finishedNormally = true;
            });
            Y_UNUSED(actor);

            UNIT_ASSERT(finished);
            UNIT_ASSERT(!finishedWithTimeout);
            UNIT_ASSERT(finishedNormally);
            UNIT_ASSERT(!state.Destroyed);

            runtime.SimulateSleep(TDuration::MilliSeconds(15));
        }

        Y_UNIT_TEST(TimeoutResumeImmediatelyAfterPassAway) {
            bool finished = false;
            bool finishedNormally = false;
            bool finishedWithTimeout = false;

            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto* self) -> async<void> {
                Y_DEFER { finished = true; };

                self->PassAway();

                try {
                    int result = co_await WithTimeout(TDuration::MilliSeconds(10), [&]() -> async<int> {
                        co_return 42;
                    });
                    UNIT_ASSERT_VALUES_EQUAL(result, 42);
                } catch (const TAsyncTimeout&) {
                    finishedWithTimeout = true;
                    throw;
                }

                finishedNormally = true;
            });
            Y_UNUSED(actor);

            UNIT_ASSERT(finished);
            UNIT_ASSERT(!finishedWithTimeout);
            UNIT_ASSERT(finishedNormally);
            UNIT_ASSERT(state.Destroyed);

            runtime.SimulateSleep(TDuration::MilliSeconds(15));
        }

        Y_UNIT_TEST(TimeoutUnwindOnSuspendAfterPassAway) {
            bool finished = false;
            bool finishedNormally = false;
            bool finishedWithTimeout = false;
            std::coroutine_handle<> resume, cancel;

            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto* self) -> async<void> {
                Y_DEFER { finished = true; };

                self->PassAway();

                try {
                    int result = co_await WithTimeout(TDuration::MilliSeconds(10), [&]() -> async<int> {
                        co_await TSuspendAwaiter{ &resume, &cancel };
                        co_return 42;
                    });
                    UNIT_ASSERT_VALUES_EQUAL(result, 42);
                } catch (const TAsyncTimeout&) {
                    finishedWithTimeout = true;
                    throw;
                }

                finishedNormally = true;
            });
            Y_UNUSED(actor);

            UNIT_ASSERT(finished);
            UNIT_ASSERT(!finishedWithTimeout);
            UNIT_ASSERT(!finishedNormally);
            UNIT_ASSERT(state.Destroyed);
            UNIT_ASSERT(!resume);
            UNIT_ASSERT(!cancel);
        }

        Y_UNIT_TEST(ResumeBeforeTimeout) {
            bool finished = false;
            bool finishedNormally = false;
            bool finishedWithTimeout = false;
            std::coroutine_handle<> resume, cancel;

            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                Y_DEFER { finished = true; };

                try {
                    int result = co_await WithTimeout(TDuration::MilliSeconds(10), [&]() -> async<int> {
                        co_await TSuspendAwaiter{ &resume, &cancel };
                        co_return 42;
                    });
                    UNIT_ASSERT_VALUES_EQUAL(result, 42);
                } catch (const TAsyncTimeout&) {
                    finishedWithTimeout = true;
                    throw;
                }

                finishedNormally = true;
            });
            Y_UNUSED(actor);

            UNIT_ASSERT(!finished);
            UNIT_ASSERT(resume);
            UNIT_ASSERT(!cancel);

            // Wait until some later time, but before 
            runtime.SimulateSleep(TDuration::MilliSeconds(5));
            UNIT_ASSERT(!finished);
            UNIT_ASSERT(!cancel);

            // Resume, it must not throw exceptions
            runtime.ResumeCoroutine(actor, resume);
            UNIT_ASSERT(finished);
            UNIT_ASSERT(!finishedWithTimeout);
            UNIT_ASSERT(finishedNormally);
            UNIT_ASSERT(!state.Destroyed);

            runtime.SimulateSleep(TDuration::MilliSeconds(15));
        }

        Y_UNIT_TEST(ResumeAfterTimeout) {
            bool finished = false;
            bool finishedNormally = false;
            bool finishedWithTimeout = false;
            std::coroutine_handle<> resume, cancel;

            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                Y_DEFER { finished = true; };

                try {
                    int result = co_await WithTimeout(TDuration::MilliSeconds(10), [&]() -> async<int> {
                        co_await TSuspendAwaiter{ &resume, &cancel };
                        co_return 42;
                    });
                    UNIT_ASSERT_VALUES_EQUAL(result, 42);
                } catch (const TAsyncTimeout&) {
                    finishedWithTimeout = true;
                    throw;
                }

                finishedNormally = true;
            });
            Y_UNUSED(actor);

            UNIT_ASSERT(!finished);
            UNIT_ASSERT(resume);
            UNIT_ASSERT(!cancel);

            // Eventually timeout must request awaiter to cancel
            runtime.SimulateSleep(TDuration::MilliSeconds(15));
            UNIT_ASSERT(!finished);
            UNIT_ASSERT(cancel);

            // Resume instead of cancelling, it must not throw exceptions
            runtime.ResumeCoroutine(actor, resume);
            UNIT_ASSERT(finished);
            UNIT_ASSERT(!finishedWithTimeout);
            UNIT_ASSERT(finishedNormally);
            UNIT_ASSERT(!state.Destroyed);
        }

        Y_UNIT_TEST(ResumeAfterTimeoutAndPassAway) {
            bool finished = false;
            bool finishedNormally = false;
            bool finishedWithTimeout = false;
            std::coroutine_handle<> resume, cancel, cancelCopy;

            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                Y_DEFER { finished = true; };

                try {
                    int result = co_await WithTimeout(TDuration::MilliSeconds(10), [&]() -> async<int> {
                        co_await TSuspendAwaiter{ &resume, &cancel };
                        co_return 42;
                    });
                    UNIT_ASSERT_VALUES_EQUAL(result, 42);
                } catch (const TAsyncTimeout&) {
                    finishedWithTimeout = true;
                    throw;
                }

                finishedNormally = true;
            });
            Y_UNUSED(actor);

            UNIT_ASSERT(!finished);
            UNIT_ASSERT(resume);
            UNIT_ASSERT(!cancel);

            // Eventually timeout must request awaiter to cancel
            runtime.SimulateSleep(TDuration::MilliSeconds(15));
            UNIT_ASSERT(!finished);
            UNIT_ASSERT(cancel);
            std::swap(cancel, cancelCopy);

            // After PassAway upstream cancellation must be properly handled
            // Most importantly AwaitCancel must not be called again
            runtime.Poison(actor);
            UNIT_ASSERT(!finished);
            UNIT_ASSERT(!cancel);

            // Resume instead of cancelling, it must not throw exceptions
            runtime.ResumeCoroutine(actor, resume);
            UNIT_ASSERT(finished);
            UNIT_ASSERT(!finishedWithTimeout);
            UNIT_ASSERT(finishedNormally);
            // Actor must be destroyed after PassAway
            UNIT_ASSERT(state.Destroyed);
        }

        Y_UNIT_TEST(CancelAfterTimeoutAndPassAway) {
            bool finished = false;
            bool finishedNormally = false;
            bool finishedWithTimeout = false;
            std::coroutine_handle<> resume, cancel;

            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                Y_DEFER { finished = true; };

                try {
                    int result = co_await WithTimeout(TDuration::MilliSeconds(10), [&]() -> async<int> {
                        co_await TSuspendAwaiter{ &resume, &cancel };
                        co_return 42;
                    });
                    UNIT_ASSERT_VALUES_EQUAL(result, 42);
                } catch (const TAsyncTimeout&) {
                    finishedWithTimeout = true;
                    throw;
                }

                finishedNormally = true;
            });
            Y_UNUSED(actor);

            UNIT_ASSERT(!finished);
            UNIT_ASSERT(resume);
            UNIT_ASSERT(!cancel);

            // Eventually timeout must request awaiter to cancel
            runtime.SimulateSleep(TDuration::MilliSeconds(15));
            UNIT_ASSERT(!finished);
            UNIT_ASSERT(cancel);

            // After PassAway upstream cancellation must be properly handled
            runtime.Poison(actor);
            UNIT_ASSERT(!finished);

            // Confirm cancellation, it must not throw an exception since upstream cancelled
            runtime.ResumeCoroutine(actor, cancel);
            UNIT_ASSERT(finished);
            UNIT_ASSERT(!finishedWithTimeout);
            UNIT_ASSERT(!finishedNormally);
            // Actor must be destroyed after PassAway
            UNIT_ASSERT(state.Destroyed);
        }

        Y_UNIT_TEST(ResumeAfterPassAwayAndTimeout) {
            bool finished = false;
            bool finishedNormally = false;
            bool finishedWithTimeout = false;
            std::coroutine_handle<> resume, cancel, cancelCopy;

            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                Y_DEFER { finished = true; };

                try {
                    int result = co_await WithTimeout(TDuration::MilliSeconds(10), [&]() -> async<int> {
                        co_await TSuspendAwaiter{ &resume, &cancel };
                        co_return 42;
                    });
                    UNIT_ASSERT_VALUES_EQUAL(result, 42);
                } catch (const TAsyncTimeout&) {
                    finishedWithTimeout = true;
                    throw;
                }

                finishedNormally = true;
            });
            Y_UNUSED(actor);

            UNIT_ASSERT(!finished);
            UNIT_ASSERT(resume);
            UNIT_ASSERT(!cancel);

            // After PassAway upstream cancellation must be passed downstream
            runtime.Poison(actor);
            UNIT_ASSERT(!finished);
            UNIT_ASSERT(cancel);
            std::swap(cancel, cancelCopy);

            // Timer must be detached already, but sleep until it is delivered
            // Most importantly AwaitCancel must not be called again
            runtime.SimulateSleep(TDuration::MilliSeconds(15));
            UNIT_ASSERT(!finished);
            UNIT_ASSERT(!cancel);

            // Resume instead of cancelling, it must not throw exceptions
            runtime.ResumeCoroutine(actor, resume);
            UNIT_ASSERT(finished);
            UNIT_ASSERT(!finishedWithTimeout);
            UNIT_ASSERT(finishedNormally);
            // Actor must be destroyed after PassAway
            UNIT_ASSERT(state.Destroyed);
        }

        Y_UNIT_TEST(CancelAfterPassAwayAndTimeout) {
            bool finished = false;
            bool finishedNormally = false;
            bool finishedWithTimeout = false;
            std::coroutine_handle<> resume, cancel;

            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                Y_DEFER { finished = true; };

                try {
                    int result = co_await WithTimeout(TDuration::MilliSeconds(10), [&]() -> async<int> {
                        co_await TSuspendAwaiter{ &resume, &cancel };
                        co_return 42;
                    });
                    UNIT_ASSERT_VALUES_EQUAL(result, 42);
                } catch (const TAsyncTimeout&) {
                    finishedWithTimeout = true;
                    throw;
                }

                finishedNormally = true;
            });
            Y_UNUSED(actor);

            UNIT_ASSERT(!finished);
            UNIT_ASSERT(resume);
            UNIT_ASSERT(!cancel);

            // After PassAway upstream cancellation must be passed downstream
            runtime.Poison(actor);
            UNIT_ASSERT(!finished);
            UNIT_ASSERT(cancel);

            // Timer must be detached already, but sleep until it is delivered
            runtime.SimulateSleep(TDuration::MilliSeconds(15));
            UNIT_ASSERT(!finished);

            // Confirm cancellation, it must not throw exceptions
            runtime.ResumeCoroutine(actor, cancel);
            UNIT_ASSERT(finished);
            UNIT_ASSERT(!finishedWithTimeout);
            UNIT_ASSERT(!finishedNormally);
            // Actor must be destroyed after PassAway
            UNIT_ASSERT(state.Destroyed);
        }

    } // Y_UNIT_TEST_SUITE(Sleep)

} // namespace NAsyncTest
