#include <ydb/library/actors/async/timeout.h>

#include "common.h"

namespace NAsyncTest {

    Y_UNIT_TEST_SUITE(Timeout) {

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

            UNIT_ASSERT(!finished);
            UNIT_ASSERT(resume);
            UNIT_ASSERT(!cancel);

            // Eventually timeout must request awaiter to cancel
            runtime.SimulateSleep(TDuration::MilliSeconds(15));
            UNIT_ASSERT(!finished);
            UNIT_ASSERT(cancel);

            // Confirm cancellation, it must throw a timeout exception
            actor.ResumeCoroutine(cancel);
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

            UNIT_ASSERT(!finished);
            UNIT_ASSERT(resume);
            UNIT_ASSERT(!cancel);

            // Wait until some later time, but before 
            runtime.SimulateSleep(TDuration::MilliSeconds(5));
            UNIT_ASSERT(!finished);
            UNIT_ASSERT(!cancel);

            // Resume, it must not throw exceptions
            actor.ResumeCoroutine(resume);
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

            UNIT_ASSERT(!finished);
            UNIT_ASSERT(resume);
            UNIT_ASSERT(!cancel);

            // Eventually timeout must request awaiter to cancel
            runtime.SimulateSleep(TDuration::MilliSeconds(15));
            UNIT_ASSERT(!finished);
            UNIT_ASSERT(cancel);

            // Resume instead of cancelling, it must not throw exceptions
            actor.ResumeCoroutine(resume);
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
            actor.Poison();
            UNIT_ASSERT(!finished);
            UNIT_ASSERT(!cancel);

            // Resume instead of cancelling, it must not throw exceptions
            actor.ResumeCoroutine(resume);
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

            UNIT_ASSERT(!finished);
            UNIT_ASSERT(resume);
            UNIT_ASSERT(!cancel);

            // Eventually timeout must request awaiter to cancel
            runtime.SimulateSleep(TDuration::MilliSeconds(15));
            UNIT_ASSERT(!finished);
            UNIT_ASSERT(cancel);

            // After PassAway upstream cancellation must be properly handled
            actor.Poison();
            UNIT_ASSERT(!finished);

            // Confirm cancellation, it must not throw an exception since upstream cancelled
            actor.ResumeCoroutine(cancel);
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

            UNIT_ASSERT(!finished);
            UNIT_ASSERT(resume);
            UNIT_ASSERT(!cancel);

            // After PassAway upstream cancellation must be passed downstream
            actor.Poison();
            UNIT_ASSERT(!finished);
            UNIT_ASSERT(cancel);
            std::swap(cancel, cancelCopy);

            // Timer must be detached already, but sleep until it is delivered
            // Most importantly AwaitCancel must not be called again
            runtime.SimulateSleep(TDuration::MilliSeconds(15));
            UNIT_ASSERT(!finished);
            UNIT_ASSERT(!cancel);

            // Resume instead of cancelling, it must not throw exceptions
            actor.ResumeCoroutine(resume);
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

            UNIT_ASSERT(!finished);
            UNIT_ASSERT(resume);
            UNIT_ASSERT(!cancel);

            // After PassAway upstream cancellation must be passed downstream
            actor.Poison();
            UNIT_ASSERT(!finished);
            UNIT_ASSERT(cancel);

            // Timer must be detached already, but sleep until it is delivered
            runtime.SimulateSleep(TDuration::MilliSeconds(15));
            UNIT_ASSERT(!finished);

            // Confirm cancellation, it must not throw exceptions
            actor.ResumeCoroutine(cancel);
            UNIT_ASSERT(finished);
            UNIT_ASSERT(!finishedWithTimeout);
            UNIT_ASSERT(!finishedNormally);
            // Actor must be destroyed after PassAway
            UNIT_ASSERT(state.Destroyed);
        }

    }

} // namespace NAsyncTest
