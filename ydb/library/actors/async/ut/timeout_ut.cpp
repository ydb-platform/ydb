#include <ydb/library/actors/async/timeout.h>

#include "common.h"

namespace NAsyncTest {

    Y_UNIT_TEST_SUITE(Timeout) {

        Y_UNIT_TEST(Timeout) {
            TVector<TString> sequence;
            std::coroutine_handle<> resume, cancel;

            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                sequence.push_back("started");
                Y_DEFER { sequence.push_back("finished"); };

                auto success = co_await WithTimeout(TDuration::MilliSeconds(10), [&]() -> async<void> {
                    sequence.push_back("suspending");
                    co_await TSuspendAwaiter{ &resume, &cancel };
                });

                if (!success) {
                    sequence.push_back("timeout");
                    co_return;
                }

                sequence.push_back("returning");
            });

            ASYNC_ASSERT_SEQUENCE(sequence, "started", "suspending");
            UNIT_ASSERT(resume && !cancel);

            // Eventually timeout must request awaiter to cancel
            runtime.SimulateSleep(TDuration::MilliSeconds(15));
            ASYNC_ASSERT_SEQUENCE_EMPTY(sequence);
            UNIT_ASSERT(cancel);

            // Confirm cancellation, it must throw a timeout exception
            actor.ResumeCoroutine(cancel);
            ASYNC_ASSERT_SEQUENCE(sequence, "timeout", "finished");
            UNIT_ASSERT(!state.Destroyed);
        }

        Y_UNIT_TEST(TimeoutResumeImmediately) {
            TVector<TString> sequence;

            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                sequence.push_back("started");
                Y_DEFER { sequence.push_back("finished"); };

                auto result = co_await WithTimeout(TDuration::MilliSeconds(10), [&]() -> async<int> {
                    co_return 42;
                });

                if (!result) {
                    sequence.push_back("timeout");
                    co_return;
                }

                UNIT_ASSERT_VALUES_EQUAL(*result, 42);
                sequence.push_back("returning");
            });

            ASYNC_ASSERT_SEQUENCE(sequence, "started", "returning", "finished");
            UNIT_ASSERT(!state.Destroyed);

            runtime.SimulateSleep(TDuration::MilliSeconds(15));
            ASYNC_ASSERT_SEQUENCE_EMPTY(sequence);
        }

        Y_UNIT_TEST(TimeoutResumeImmediatelyAfterPassAway) {
            TVector<TString> sequence;

            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto* self) -> async<void> {
                sequence.push_back("started");
                Y_DEFER { sequence.push_back("finished"); };

                self->PassAway();

                auto result = co_await WithTimeout(TDuration::MilliSeconds(10), [&]() -> async<int> {
                    co_return 42;
                });

                if (!result) {
                    sequence.push_back("timeout");
                    co_return;
                }

                UNIT_ASSERT_VALUES_EQUAL(*result, 42);
                sequence.push_back("returning");
            });

            ASYNC_ASSERT_SEQUENCE(sequence, "started", "returning", "finished");
            UNIT_ASSERT(state.Destroyed);

            runtime.SimulateSleep(TDuration::MilliSeconds(15));
            ASYNC_ASSERT_SEQUENCE_EMPTY(sequence);
        }

        Y_UNIT_TEST(TimeoutUnwindOnSuspendAfterPassAway) {
            TVector<TString> sequence;
            std::coroutine_handle<> resume, cancel;

            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto* self) -> async<void> {
                sequence.push_back("started");
                Y_DEFER { sequence.push_back("finished"); };

                self->PassAway();

                auto result = co_await WithTimeout(TDuration::MilliSeconds(10), [&]() -> async<int> {
                    sequence.push_back("suspending");
                    co_await TSuspendAwaiter{ &resume, &cancel };
                    sequence.push_back("resumed");
                    co_return 42;
                });

                if (!result) {
                    sequence.push_back("timeout");
                    co_return;
                }

                UNIT_ASSERT_VALUES_EQUAL(*result, 42);
                sequence.push_back("returning");
            });

            ASYNC_ASSERT_SEQUENCE(sequence, "started", "suspending", "finished");
            UNIT_ASSERT(!resume && !cancel);
            UNIT_ASSERT(state.Destroyed);
        }

        Y_UNIT_TEST(ResumeBeforeTimeout) {
            TVector<TString> sequence;
            std::coroutine_handle<> resume, cancel;

            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                sequence.push_back("started");
                Y_DEFER { sequence.push_back("finished"); };

                auto result = co_await WithTimeout(TDuration::MilliSeconds(10), [&]() -> async<int> {
                    sequence.push_back("suspending");
                    co_await TSuspendAwaiter{ &resume, &cancel };
                    sequence.push_back("resumed");
                    co_return 42;
                });

                if (!result) {
                    sequence.push_back("timeout");
                    co_return;
                }

                UNIT_ASSERT_VALUES_EQUAL(*result, 42);
                sequence.push_back("returning");
            });

            ASYNC_ASSERT_SEQUENCE(sequence, "started", "suspending");
            UNIT_ASSERT(resume && !cancel);

            // Wait until some later time, but before the timeout
            runtime.SimulateSleep(TDuration::MilliSeconds(5));
            ASYNC_ASSERT_SEQUENCE_EMPTY(sequence);
            UNIT_ASSERT(!cancel);

            // Resume, it must not throw exceptions
            actor.ResumeCoroutine(resume);
            ASYNC_ASSERT_SEQUENCE(sequence, "resumed", "returning", "finished");
            UNIT_ASSERT(!state.Destroyed);

            runtime.SimulateSleep(TDuration::MilliSeconds(15));
            ASYNC_ASSERT_SEQUENCE_EMPTY(sequence);
        }

        Y_UNIT_TEST(ResumeAfterTimeout) {
            TVector<TString> sequence;
            std::coroutine_handle<> resume, cancel;

            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                sequence.push_back("started");
                Y_DEFER { sequence.push_back("finished"); };

                auto result = co_await WithTimeout(TDuration::MilliSeconds(10), [&]() -> async<int> {
                    sequence.push_back("suspending");
                    co_await TSuspendAwaiter{ &resume, &cancel };
                    sequence.push_back("resumed");
                    co_return 42;
                });

                if (!result) {
                    sequence.push_back("timeout");
                    co_return;
                }

                UNIT_ASSERT_VALUES_EQUAL(*result, 42);
                sequence.push_back("returning");
            });

            ASYNC_ASSERT_SEQUENCE(sequence, "started", "suspending");
            UNIT_ASSERT(resume && !cancel);

            // Eventually timeout must request awaiter to cancel
            runtime.SimulateSleep(TDuration::MilliSeconds(15));
            ASYNC_ASSERT_SEQUENCE_EMPTY(sequence);
            UNIT_ASSERT(cancel);

            // Resume instead of cancelling, it must not throw exceptions
            actor.ResumeCoroutine(resume);
            ASYNC_ASSERT_SEQUENCE(sequence, "resumed", "returning", "finished");
            UNIT_ASSERT(!state.Destroyed);
        }

        Y_UNIT_TEST(ResumeAfterTimeoutAndPassAway) {
            TVector<TString> sequence;
            std::coroutine_handle<> resume, cancel, cancelCopy;

            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                sequence.push_back("started");
                Y_DEFER { sequence.push_back("finished"); };

                auto result = co_await WithTimeout(TDuration::MilliSeconds(10), [&]() -> async<int> {
                    sequence.push_back("suspending");
                    co_await TSuspendAwaiter{ &resume, &cancel };
                    sequence.push_back("resumed");
                    co_return 42;
                });

                if (!result) {
                    sequence.push_back("timeout");
                    co_return;
                }

                UNIT_ASSERT_VALUES_EQUAL(*result, 42);
                sequence.push_back("returning");
            });

            ASYNC_ASSERT_SEQUENCE(sequence, "started", "suspending");
            UNIT_ASSERT(resume && !cancel);

            // Eventually timeout must request awaiter to cancel
            runtime.SimulateSleep(TDuration::MilliSeconds(15));
            ASYNC_ASSERT_SEQUENCE_EMPTY(sequence);
            UNIT_ASSERT(cancel);
            std::swap(cancel, cancelCopy);

            // After PassAway upstream cancellation must be properly handled
            // Most importantly await_cancel must not be called again
            actor.Poison();
            ASYNC_ASSERT_SEQUENCE_EMPTY(sequence);
            UNIT_ASSERT(!cancel);

            // Resume instead of cancelling, it must not throw exceptions
            actor.ResumeCoroutine(resume);
            ASYNC_ASSERT_SEQUENCE(sequence, "resumed", "returning", "finished");
            // Actor must be destroyed after PassAway
            UNIT_ASSERT(state.Destroyed);
        }

        Y_UNIT_TEST(CancelAfterTimeoutAndPassAway) {
            TVector<TString> sequence;
            std::coroutine_handle<> resume, cancel;

            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                sequence.push_back("started");
                Y_DEFER { sequence.push_back("finished"); };

                auto result = co_await WithTimeout(TDuration::MilliSeconds(10), [&]() -> async<int> {
                    sequence.push_back("suspending");
                    co_await TSuspendAwaiter{ &resume, &cancel };
                    sequence.push_back("resumed");
                    co_return 42;
                });

                if (!result) {
                    sequence.push_back("timeout");
                    co_return;
                }

                UNIT_ASSERT_VALUES_EQUAL(*result, 42);
                sequence.push_back("returning");
            });

            ASYNC_ASSERT_SEQUENCE(sequence, "started", "suspending");
            UNIT_ASSERT(resume && !cancel);

            // Eventually timeout must request awaiter to cancel
            runtime.SimulateSleep(TDuration::MilliSeconds(15));
            ASYNC_ASSERT_SEQUENCE_EMPTY(sequence);
            UNIT_ASSERT(cancel);

            // After PassAway upstream cancellation must be properly handled
            actor.Poison();
            ASYNC_ASSERT_SEQUENCE_EMPTY(sequence);

            // Confirm cancellation, it must not throw an exception since upstream cancelled
            actor.ResumeCoroutine(cancel);
            ASYNC_ASSERT_SEQUENCE(sequence, "finished");
            // Actor must be destroyed after PassAway
            UNIT_ASSERT(state.Destroyed);
        }

        Y_UNIT_TEST(ResumeAfterPassAwayAndTimeout) {
            TVector<TString> sequence;
            std::coroutine_handle<> resume, cancel, cancelCopy;

            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                sequence.push_back("started");
                Y_DEFER { sequence.push_back("finished"); };

                auto result = co_await WithTimeout(TDuration::MilliSeconds(10), [&]() -> async<int> {
                    sequence.push_back("suspending");
                    co_await TSuspendAwaiter{ &resume, &cancel };
                    sequence.push_back("resumed");
                    co_return 42;
                });

                if (!result) {
                    sequence.push_back("timeout");
                    co_return;
                }

                UNIT_ASSERT_VALUES_EQUAL(*result, 42);
                sequence.push_back("returning");
            });

            ASYNC_ASSERT_SEQUENCE(sequence, "started", "suspending");
            UNIT_ASSERT(resume && !cancel);

            // After PassAway upstream cancellation must be passed downstream
            actor.Poison();
            ASYNC_ASSERT_SEQUENCE_EMPTY(sequence);
            UNIT_ASSERT(cancel);
            std::swap(cancel, cancelCopy);

            // Timer must be detached already, but sleep until it is delivered
            // Most importantly await_cancel must not be called again
            runtime.SimulateSleep(TDuration::MilliSeconds(15));
            ASYNC_ASSERT_SEQUENCE_EMPTY(sequence);
            UNIT_ASSERT(!cancel);

            // Resume instead of cancelling, it must not throw exceptions
            actor.ResumeCoroutine(resume);
            ASYNC_ASSERT_SEQUENCE(sequence, "resumed", "returning", "finished");
            // Actor must be destroyed after PassAway
            UNIT_ASSERT(state.Destroyed);
        }

        Y_UNIT_TEST(CancelAfterPassAwayAndTimeout) {
            TVector<TString> sequence;
            std::coroutine_handle<> resume, cancel;

            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                sequence.push_back("started");
                Y_DEFER { sequence.push_back("finished"); };

                auto result = co_await WithTimeout(TDuration::MilliSeconds(10), [&]() -> async<int> {
                    sequence.push_back("suspending");
                    co_await TSuspendAwaiter{ &resume, &cancel };
                    sequence.push_back("resumed");
                    co_return 42;
                });

                if (!result) {
                    sequence.push_back("timeout");
                    co_return;
                }

                UNIT_ASSERT_VALUES_EQUAL(*result, 42);
                sequence.push_back("returning");
            });

            ASYNC_ASSERT_SEQUENCE(sequence, "started", "suspending");
            UNIT_ASSERT(resume && !cancel);

            // After PassAway upstream cancellation must be passed downstream
            actor.Poison();
            ASYNC_ASSERT_SEQUENCE_EMPTY(sequence);
            UNIT_ASSERT(cancel);

            // Timer must be detached already, but sleep until it is delivered
            runtime.SimulateSleep(TDuration::MilliSeconds(15));
            ASYNC_ASSERT_SEQUENCE_EMPTY(sequence);

            // Confirm cancellation, it must not throw exceptions
            actor.ResumeCoroutine(cancel);
            ASYNC_ASSERT_SEQUENCE(sequence, "finished");
            // Actor must be destroyed after PassAway
            UNIT_ASSERT(state.Destroyed);
        }


        // The async<R> overloads take an already created lazy frame
        Y_UNIT_TEST(AsyncOverloadReturnsValue) {
            TVector<TString> sequence;

            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                sequence.push_back("started");
                Y_DEFER { sequence.push_back("finished"); };

                auto op = [&]() -> async<int> {
                    sequence.push_back("op running");
                    co_return 42;
                };

                auto result = co_await WithTimeout(TDuration::MilliSeconds(10), op());
                UNIT_ASSERT(result);
                UNIT_ASSERT_VALUES_EQUAL(*result, 42);

                result = co_await WithDeadline(TActivationContext::Monotonic() + TDuration::MilliSeconds(10), op());
                UNIT_ASSERT(result);
                UNIT_ASSERT_VALUES_EQUAL(*result, 42);

                result = co_await WithDeadline(TActivationContext::Now() + TDuration::MilliSeconds(10), op());
                UNIT_ASSERT(result);
                UNIT_ASSERT_VALUES_EQUAL(*result, 42);

                sequence.push_back("returning");
            });

            ASYNC_ASSERT_SEQUENCE(sequence, "started", "op running", "op running", "op running", "returning", "finished");
            UNIT_ASSERT(!state.Destroyed);
        }

        Y_UNIT_TEST(AsyncOverloadTimesOut) {
            TVector<TString> sequence;
            std::coroutine_handle<> resume, cancel;

            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                sequence.push_back("started");
                Y_DEFER { sequence.push_back("finished"); };

                auto op = [&]() -> async<int> {
                    sequence.push_back("suspending");
                    co_await TSuspendAwaiter{ &resume, &cancel };
                    sequence.push_back("resumed");
                    co_return 42;
                };

                auto result = co_await WithTimeout(TDuration::MilliSeconds(10), op());
                if (!result) {
                    sequence.push_back("timeout");
                    co_return;
                }
                sequence.push_back("returning");
            });

            ASYNC_ASSERT_SEQUENCE(sequence, "started", "suspending");
            UNIT_ASSERT(resume && !cancel);

            runtime.SimulateSleep(TDuration::MilliSeconds(15));
            ASYNC_ASSERT_SEQUENCE_EMPTY(sequence);
            UNIT_ASSERT(cancel);

            actor.ResumeCoroutine(cancel);
            ASYNC_ASSERT_SEQUENCE(sequence, "timeout", "finished");
            UNIT_ASSERT(!state.Destroyed);
        }

    }

} // namespace NAsyncTest
