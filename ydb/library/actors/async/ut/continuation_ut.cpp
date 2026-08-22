#include <ydb/library/actors/async/continuation.h>

#include "common.h"

namespace NAsyncTest {

    Y_UNIT_TEST_SUITE(Continuation) {

        Y_UNIT_TEST(ResumeFromCallback) {
            TVector<TString> sequence;

            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                sequence.push_back("started");
                Y_DEFER { sequence.push_back("finished"); };

                int value = co_await WithAsyncContinuation<int>([&](auto c) {
                    sequence.push_back("before resume");
                    c.Resume(42);
                    sequence.push_back("after resume");
                });
                sequence.push_back("resumed");

                UNIT_ASSERT_VALUES_EQUAL(value, 42);

                sequence.push_back("returning");
            });

            ASYNC_ASSERT_SEQUENCE(sequence,
                "started", "before resume", "after resume",
                "resumed", "returning", "finished");
        }

        Y_UNIT_TEST(Resume) {
            TVector<TString> sequence;
            TAsyncContinuation<int> continuation;

            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                sequence.push_back("started");
                Y_DEFER { sequence.push_back("finished"); };

                int value = co_await WithAsyncContinuation<int>([&](auto c) {
                    sequence.push_back("callback");
                    continuation = std::move(c);
                });
                sequence.push_back("resumed");

                UNIT_ASSERT_VALUES_EQUAL(value, 42);

                sequence.push_back("returning");
            });

            ASYNC_ASSERT_SEQUENCE(sequence, "started", "callback");
            UNIT_ASSERT(continuation);

            actor.RunSync([&]{
                sequence.push_back("before resume");
                continuation.Resume(42);
                sequence.push_back("after resume");
            });

            ASYNC_ASSERT_SEQUENCE(sequence,
                "before resume", "after resume",
                "resumed", "returning", "finished");
            UNIT_ASSERT(!continuation);
        }

        Y_UNIT_TEST(DropFromCallback) {
            TVector<TString> sequence;

            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                sequence.push_back("started");
                Y_DEFER { sequence.push_back("finished"); };

                auto callback = [&](auto c) {
                    sequence.push_back("before drop");
                    std::optional<TAsyncContinuation<int>> continuation(std::move(c));
                    continuation.reset();
                    sequence.push_back("after drop");
                };

                UNIT_ASSERT_EXCEPTION_CONTAINS(co_await WithAsyncContinuation<int>(callback),
                    std::logic_error, "continuation object was destroyed");

                sequence.push_back("returning");
            });

            ASYNC_ASSERT_SEQUENCE(sequence,
                "started", "before drop", "after drop",
                "returning", "finished");
        }

        Y_UNIT_TEST(Drop) {
            TVector<TString> sequence;
            std::optional<TAsyncContinuation<int>> continuation;

            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                sequence.push_back("started");
                Y_DEFER { sequence.push_back("finished"); };

                auto callback = [&](auto c) {
                    sequence.push_back("callback");
                    continuation = std::move(c);
                };

                UNIT_ASSERT_EXCEPTION_CONTAINS(co_await WithAsyncContinuation<int>(callback),
                    std::logic_error, "continuation object was destroyed");

                sequence.push_back("returning");
            });

            ASYNC_ASSERT_SEQUENCE(sequence, "started", "callback");
            UNIT_ASSERT(continuation);

            actor.RunSync([&]{
                sequence.push_back("before drop");
                continuation.reset();
                sequence.push_back("after drop");
            });

            ASYNC_ASSERT_SEQUENCE(sequence,
                "before drop", "after drop",
                "returning", "finished");
        }

        Y_UNIT_TEST(AlreadyCancelled) {
            TVector<TString> sequence;
            TAsyncContinuation<int> continuation;

            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto* self) -> async<void> {
                sequence.push_back("started");
                Y_DEFER { sequence.push_back("finished"); };

                auto callback = [&](auto c) {
                    sequence.push_back("callback");
                    continuation = std::move(c);
                };

                self->PassAway();

                ASYNC_ASSERT_NO_RETURN(co_await WithAsyncContinuation<int>(callback));
            });

            // Note: callback not called
            ASYNC_ASSERT_SEQUENCE(sequence, "started", "finished");
        }

        Y_UNIT_TEST(Cancel) {
            TVector<TString> sequence;
            TAsyncContinuation<int> continuation;

            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                sequence.push_back("started");
                Y_DEFER { sequence.push_back("finished"); };

                auto callback = [&](auto c) {
                    sequence.push_back("callback");
                    continuation = std::move(c);
                };

                ASYNC_ASSERT_NO_RETURN(co_await WithAsyncContinuation<int>(callback));
            });

            ASYNC_ASSERT_SEQUENCE(sequence, "started", "callback");
            UNIT_ASSERT(continuation);

            actor.Poison();
            ASYNC_ASSERT_SEQUENCE(sequence, "finished");
            UNIT_ASSERT(!continuation);
        }

        Y_UNIT_TEST(Throw) {
            TVector<TString> sequence;
            TAsyncContinuation<int> continuation;

            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                sequence.push_back("started");
                Y_DEFER { sequence.push_back("finished"); };

                auto callback = [&](auto c) {
                    sequence.push_back("callback");
                    continuation = std::move(c);
                };

                UNIT_ASSERT_EXCEPTION(co_await WithAsyncContinuation<int>(callback), TTestException);

                sequence.push_back("returning");
            });

            ASYNC_ASSERT_SEQUENCE(sequence, "started", "callback");
            UNIT_ASSERT(continuation);

            actor.RunSync([&]{
                sequence.push_back("before throw");
                continuation.Throw(std::make_exception_ptr(TTestException() << "test exception"));
                sequence.push_back("after throw");
            });

            ASYNC_ASSERT_SEQUENCE(sequence,
                "before throw", "after throw",
                "returning", "finished");
            UNIT_ASSERT(!continuation);
        }

        Y_UNIT_TEST(NoResumeOnShutdown) {
            TVector<TString> sequence;
            TAsyncContinuation<void> continuation;
            std::coroutine_handle<> resume;

            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                sequence.push_back("started");
                Y_DEFER { sequence.push_back("finished"); };

                Y_DEFER {
                    if (continuation) {
                        sequence.push_back("calling Resume");
                        continuation.Resume();
                    }
                };

                co_await TSuspendAwaiterWithoutCancel{ &resume };

                sequence.push_back("resumed");
            });

            actor.RunAsync([&]() -> async<void> {
                sequence.push_back("second started");
                Y_DEFER { sequence.push_back("second finished"); };

                co_await WithAsyncContinuation<void>([&](auto c) {
                    sequence.push_back("second suspended");
                    continuation = std::move(c);
                });

                sequence.push_back("second resumed");
            });

            ASYNC_ASSERT_SEQUENCE(sequence,
                "started", "second started", "second suspended");

            runtime.CleanupNode();

            ASYNC_ASSERT_SEQUENCE(sequence,
                "calling Resume", "finished", "second finished");
        }

    } // Y_UNIT_TEST_SUITE(Continuation)

} // namespace NAsyncTest
