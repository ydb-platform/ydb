#include <ydb/library/actors/async/cancellation.h>

#include "common.h"

namespace NAsyncTest {

    Y_UNIT_TEST_SUITE(Cancellation) {

        Y_UNIT_TEST(CancelWithResult) {
            TVector<TString> sequence;
            std::coroutine_handle<> resume, cancel;
            TAsyncCancellationSource source;

            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                Y_DEFER { sequence.push_back("finished"); };

                int value = co_await source.WithCancellation([&]() -> async<int> {
                    sequence.push_back("callback started");
                    Y_DEFER { sequence.push_back("callback finished"); };
                    co_await TSuspendAwaiter{ &resume, &cancel };
                    sequence.push_back("callback resumed");
                    co_return 42;
                });

                UNIT_ASSERT_VALUES_EQUAL(value, 42);

                sequence.push_back("returning");
            });

            ASYNC_ASSERT_SEQUENCE(sequence, "callback started");
            UNIT_ASSERT(resume && !cancel);

            actor.RunSync([&]{ source.Cancel(); });
            ASYNC_ASSERT_SEQUENCE_EMPTY(sequence);
            UNIT_ASSERT(cancel);

            actor.ResumeCoroutine(resume);
            ASYNC_ASSERT_SEQUENCE(sequence, "callback resumed", "callback finished", "returning", "finished");
        }

        Y_UNIT_TEST(CancelWithException) {
            TVector<TString> sequence;
            std::coroutine_handle<> resume, cancel;
            TAsyncCancellationSource source;

            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                Y_DEFER { sequence.push_back("finished"); };

                auto callback = [&]() -> async<void> {
                    sequence.push_back("callback started");
                    Y_DEFER { sequence.push_back("callback finished"); };
                    co_await TSuspendAwaiter{ &resume, &cancel };
                    sequence.push_back("callback resumed");
                };

                UNIT_ASSERT_EXCEPTION(co_await source.WithCancellation(callback), TAsyncCancellation);

                sequence.push_back("returning");
            });

            ASYNC_ASSERT_SEQUENCE(sequence, "callback started");
            UNIT_ASSERT(resume && !cancel);

            actor.RunSync([&]{ source.Cancel(); });
            ASYNC_ASSERT_SEQUENCE_EMPTY(sequence);
            UNIT_ASSERT(cancel);

            actor.ResumeCoroutine(cancel);
            ASYNC_ASSERT_SEQUENCE(sequence, "callback finished", "returning", "finished");
        }

        Y_UNIT_TEST(AlreadyCancelledSource) {
            TVector<TString> sequence;
            std::coroutine_handle<> resume, cancel;
            TAsyncCancellationSource source;

            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                Y_DEFER { sequence.push_back("finished"); };

                auto callback = [&]() -> async<void> {
                    sequence.push_back("callback started");
                    Y_DEFER { sequence.push_back("callback finished"); };
                    co_await TSuspendAwaiter{ &resume, &cancel };
                    sequence.push_back("callback resumed");
                };

                source.Cancel();

                UNIT_ASSERT_EXCEPTION(co_await source.WithCancellation(callback), TAsyncCancellation);

                sequence.push_back("returning");
            });

            ASYNC_ASSERT_SEQUENCE(sequence, "callback started", "callback finished", "returning", "finished");
            UNIT_ASSERT(!resume && !cancel);
        }

        Y_UNIT_TEST(AlreadyCancelledCaller) {
            TVector<TString> sequence;
            std::coroutine_handle<> resume, cancel;
            TAsyncCancellationSource source;

            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto* self) -> async<void> {
                Y_DEFER { sequence.push_back("finished"); };

                auto callback = [&]() -> async<void> {
                    sequence.push_back("callback started");
                    Y_DEFER { sequence.push_back("callback finished"); };
                    co_await TSuspendAwaiter{ &resume, &cancel };
                    sequence.push_back("callback resumed");
                };

                self->PassAway();

                ASYNC_ASSERT_NO_RETURN(co_await source.WithCancellation(callback));
            });

            ASYNC_ASSERT_SEQUENCE(sequence, "callback started", "callback finished", "finished");
            UNIT_ASSERT(!resume && !cancel);
        }

        Y_UNIT_TEST(CancelSourceThenCaller) {
            TVector<TString> sequence;
            std::coroutine_handle<> resume, cancel, cancelCopy;
            TAsyncCancellationSource source;

            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                Y_DEFER { sequence.push_back("finished"); };

                auto callback = [&]() -> async<void> {
                    sequence.push_back("callback started");
                    Y_DEFER { sequence.push_back("callback finished"); };
                    co_await TSuspendAwaiter{ &resume, &cancel };
                    sequence.push_back("callback resumed");
                };

                ASYNC_ASSERT_NO_RETURN(co_await source.WithCancellation(callback));
            });

            ASYNC_ASSERT_SEQUENCE(sequence, "callback started");
            UNIT_ASSERT(resume && !cancel);

            actor.RunSync([&]{ source.Cancel(); });
            ASYNC_ASSERT_SEQUENCE_EMPTY(sequence);
            UNIT_ASSERT(cancel);

            std::swap(cancel, cancelCopy);

            actor.Poison();
            ASYNC_ASSERT_SEQUENCE_EMPTY(sequence);
            UNIT_ASSERT(!cancel);

            actor.ResumeCoroutine(cancelCopy);
            ASYNC_ASSERT_SEQUENCE(sequence, "callback finished", "finished");
        }

        Y_UNIT_TEST(CancelCallerThenSource) {
            TVector<TString> sequence;
            std::coroutine_handle<> resume, cancel, cancelCopy;
            TAsyncCancellationSource source;

            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                Y_DEFER { sequence.push_back("finished"); };

                auto callback = [&]() -> async<void> {
                    sequence.push_back("callback started");
                    Y_DEFER { sequence.push_back("callback finished"); };
                    co_await TSuspendAwaiter{ &resume, &cancel };
                    sequence.push_back("callback resumed");
                };

                ASYNC_ASSERT_NO_RETURN(co_await source.WithCancellation(callback));
            });

            ASYNC_ASSERT_SEQUENCE(sequence, "callback started");
            UNIT_ASSERT(resume && !cancel);

            actor.Poison();
            ASYNC_ASSERT_SEQUENCE_EMPTY(sequence);
            UNIT_ASSERT(cancel);

            std::swap(cancel, cancelCopy);

            actor.RunSync([&]{ source.Cancel(); });
            ASYNC_ASSERT_SEQUENCE_EMPTY(sequence);
            UNIT_ASSERT(!cancel);

            actor.ResumeCoroutine(cancelCopy);
            ASYNC_ASSERT_SEQUENCE(sequence, "callback finished", "finished");
        }

        Y_UNIT_TEST(SuspendedSourceTearDown) {
            TVector<TString> sequence;
            std::coroutine_handle<> resume, cancel;
            TAsyncCancellationSource source;

            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                Y_DEFER { sequence.push_back("finished"); };

                auto callback = [&]() -> async<void> {
                    sequence.push_back("callback started");
                    Y_DEFER { sequence.push_back("callback finished"); };
                    co_await TSuspendAwaiter{ &resume, &cancel };
                    sequence.push_back("callback resumed");
                };

                ASYNC_ASSERT_NO_RETURN(co_await source.WithCancellation(callback));
            });

            ASYNC_ASSERT_SEQUENCE(sequence, "callback started");
            UNIT_ASSERT(resume && !cancel);

            runtime.CleanupNode();
            ASYNC_ASSERT_SEQUENCE(sequence, "callback finished", "finished");
        }

        Y_UNIT_TEST(CancelledSourceTearDown) {
            TVector<TString> sequence;
            std::coroutine_handle<> resume, cancel;
            TAsyncCancellationSource source;

            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                Y_DEFER { sequence.push_back("finished"); };

                auto callback = [&]() -> async<void> {
                    sequence.push_back("callback started");
                    Y_DEFER { sequence.push_back("callback finished"); };
                    co_await TSuspendAwaiter{ &resume, &cancel };
                    sequence.push_back("callback resumed");
                };

                ASYNC_ASSERT_NO_RETURN(co_await source.WithCancellation(callback));
            });

            ASYNC_ASSERT_SEQUENCE(sequence, "callback started");
            UNIT_ASSERT(resume && !cancel);

            actor.RunSync([&]{
                source.Cancel();
            });
            UNIT_ASSERT(cancel);

            runtime.CleanupNode();
            ASYNC_ASSERT_SEQUENCE(sequence, "callback finished", "finished");
        }

        Y_UNIT_TEST(WithoutCancellation) {
            TVector<TString> sequence;
            std::coroutine_handle<> resume, cancel;

            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                Y_DEFER { sequence.push_back("finished"); };

                auto callback = [&]() -> async<int> {
                    sequence.push_back("callback started");
                    Y_DEFER { sequence.push_back("callback finished"); };
                    co_await TSuspendAwaiter{ &resume, &cancel };
                    sequence.push_back("callback resumed");
                    co_return 42;
                };

                int value = co_await WithoutCancellation(callback);

                UNIT_ASSERT_VALUES_EQUAL(value, 42);

                sequence.push_back("returning");
            });

            ASYNC_ASSERT_SEQUENCE(sequence, "callback started");
            UNIT_ASSERT(resume && !cancel);

            actor.Poison();
            ASYNC_ASSERT_SEQUENCE_EMPTY(sequence);
            UNIT_ASSERT(!cancel);

            actor.ResumeCoroutine(resume);
            ASYNC_ASSERT_SEQUENCE(sequence, "callback resumed", "callback finished", "returning", "finished");
        }

        Y_UNIT_TEST(WithoutCancellationTearDown) {
            TVector<TString> sequence;
            std::coroutine_handle<> resume, cancel;

            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                Y_DEFER { sequence.push_back("finished"); };

                auto callback = [&]() -> async<int> {
                    sequence.push_back("callback started");
                    Y_DEFER { sequence.push_back("callback finished"); };
                    co_await TSuspendAwaiter{ &resume, &cancel };
                    sequence.push_back("callback resumed");
                    co_return 42;
                };

                int value = co_await WithoutCancellation(callback);

                UNIT_ASSERT_VALUES_EQUAL(value, 42);

                sequence.push_back("returning");
            });

            ASYNC_ASSERT_SEQUENCE(sequence, "callback started");
            UNIT_ASSERT(resume && !cancel);

            actor.Poison();
            ASYNC_ASSERT_SEQUENCE_EMPTY(sequence);
            UNIT_ASSERT(!cancel);

            runtime.CleanupNode();
            ASYNC_ASSERT_SEQUENCE(sequence, "callback finished", "finished");
        }

        Y_UNIT_TEST(InterceptCancellationVoidThenResume) {
            TVector<TString> sequence;
            std::coroutine_handle<> resume, cancel;

            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                Y_DEFER { sequence.push_back("finished"); };

                auto callback = [&]() -> async<int> {
                    sequence.push_back("callback started");
                    Y_DEFER { sequence.push_back("callback finished"); };
                    co_await TSuspendAwaiter{ &resume, &cancel };
                    sequence.push_back("callback resumed");
                    co_return 42;
                };

                int value = co_await InterceptCancellation(
                    callback, [&]{ sequence.push_back("on cancel"); });

                UNIT_ASSERT_VALUES_EQUAL(value, 42);

                sequence.push_back("returning");
            });

            ASYNC_ASSERT_SEQUENCE(sequence, "callback started");
            UNIT_ASSERT(resume && !cancel);

            actor.Poison();
            ASYNC_ASSERT_SEQUENCE(sequence, "on cancel");
            UNIT_ASSERT(cancel);

            actor.ResumeCoroutine(resume);
            ASYNC_ASSERT_SEQUENCE(sequence, "callback resumed", "callback finished", "returning", "finished");
        }

        Y_UNIT_TEST(InterceptCancellationVoidThenCancel) {
            TVector<TString> sequence;
            std::coroutine_handle<> resume, cancel;

            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                Y_DEFER { sequence.push_back("finished"); };

                auto callback = [&]() -> async<int> {
                    sequence.push_back("callback started");
                    Y_DEFER { sequence.push_back("callback finished"); };
                    co_await TSuspendAwaiter{ &resume, &cancel };
                    sequence.push_back("callback resumed");
                    co_return 42;
                };

                int value = co_await InterceptCancellation(
                    callback, [&]{ sequence.push_back("on cancel"); });

                UNIT_ASSERT_VALUES_EQUAL(value, 42);

                sequence.push_back("returning");
            });

            ASYNC_ASSERT_SEQUENCE(sequence, "callback started");
            UNIT_ASSERT(resume && !cancel);

            actor.Poison();
            ASYNC_ASSERT_SEQUENCE(sequence, "on cancel");
            UNIT_ASSERT(cancel);

            actor.ResumeCoroutine(cancel);
            ASYNC_ASSERT_SEQUENCE(sequence, "callback finished", "finished");
        }

        Y_UNIT_TEST(InterceptCancellationTrueThenTearDown) {
            TVector<TString> sequence;
            std::coroutine_handle<> resume, cancel;

            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                Y_DEFER { sequence.push_back("finished"); };

                auto callback = [&]() -> async<int> {
                    sequence.push_back("callback started");
                    Y_DEFER { sequence.push_back("callback finished"); };
                    co_await TSuspendAwaiter{ &resume, &cancel };
                    sequence.push_back("callback resumed");
                    co_return 42;
                };

                int value = co_await InterceptCancellation(
                    callback, [&]{ sequence.push_back("on cancel"); return true; });

                UNIT_ASSERT_VALUES_EQUAL(value, 42);

                sequence.push_back("returning");
            });

            ASYNC_ASSERT_SEQUENCE(sequence, "callback started");
            UNIT_ASSERT(resume && !cancel);

            actor.Poison();
            ASYNC_ASSERT_SEQUENCE(sequence, "on cancel");
            UNIT_ASSERT(cancel);

            runtime.CleanupNode();
            ASYNC_ASSERT_SEQUENCE(sequence, "callback finished", "finished");
        }

        Y_UNIT_TEST(InterceptCancellationFalseThenResume) {
            TVector<TString> sequence;
            std::coroutine_handle<> resume, cancel;

            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                Y_DEFER { sequence.push_back("finished"); };

                auto callback = [&]() -> async<int> {
                    sequence.push_back("callback started");
                    Y_DEFER { sequence.push_back("callback finished"); };
                    co_await TSuspendAwaiter{ &resume, &cancel };
                    sequence.push_back("callback resumed");
                    co_return 42;
                };

                int value = co_await InterceptCancellation(
                    callback, [&]{ sequence.push_back("on cancel"); return false; });

                UNIT_ASSERT_VALUES_EQUAL(value, 42);

                sequence.push_back("returning");
            });

            ASYNC_ASSERT_SEQUENCE(sequence, "callback started");
            UNIT_ASSERT(resume && !cancel);

            actor.Poison();
            ASYNC_ASSERT_SEQUENCE(sequence, "on cancel");
            UNIT_ASSERT(!cancel);

            actor.ResumeCoroutine(resume);
            ASYNC_ASSERT_SEQUENCE(sequence, "callback resumed", "callback finished", "returning", "finished");
        }

        Y_UNIT_TEST(InterceptCancellationBoolThrow) {
            TVector<TString> sequence;
            std::coroutine_handle<> resume, cancel;

            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                Y_DEFER { sequence.push_back("finished"); };

                auto callback = [&]() -> async<int> {
                    sequence.push_back("callback started");
                    Y_DEFER { sequence.push_back("callback finished"); };
                    co_await TSuspendAwaiter{ &resume, &cancel };
                    sequence.push_back("callback resumed");
                    co_return 42;
                };

                auto onCancel = [&]() -> bool {
                    sequence.push_back("on cancel");
                    throw std::runtime_error("on cancel exception");
                };

                UNIT_ASSERT_EXCEPTION_CONTAINS(
                    co_await InterceptCancellation(callback, onCancel),
                    std::runtime_error, "on cancel exception");

                sequence.push_back("returning");
            });

            ASYNC_ASSERT_SEQUENCE(sequence, "callback started");
            UNIT_ASSERT(resume && !cancel);

            actor.Poison();
            ASYNC_ASSERT_SEQUENCE(sequence, "on cancel");
            UNIT_ASSERT(cancel);

            actor.ResumeCoroutine(resume);
            ASYNC_ASSERT_SEQUENCE(sequence, "callback resumed", "callback finished", "returning", "finished");
        }

        Y_UNIT_TEST(InterceptCancellationAsyncVoidOnCancelFinishThenResume) {
            TVector<TString> sequence;
            std::coroutine_handle<> resume, cancel;
            std::coroutine_handle<> cresume, ccancel;

            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                Y_DEFER { sequence.push_back("finished"); };

                auto callback = [&]() -> async<int> {
                    sequence.push_back("callback started");
                    Y_DEFER { sequence.push_back("callback finished"); };
                    co_await TSuspendAwaiter{ &resume, &cancel };
                    sequence.push_back("callback resumed");
                    co_return 42;
                };

                auto onCancel = [&]() -> async<void> {
                    sequence.push_back("on cancel started");
                    Y_DEFER { sequence.push_back("on cancel finished"); };
                    co_await TSuspendAwaiter{ &cresume, &ccancel };
                    sequence.push_back("on cancel resumed");
                };

                int value = co_await InterceptCancellation(callback, onCancel);

                UNIT_ASSERT_VALUES_EQUAL(value, 42);

                sequence.push_back("returning");
            });

            ASYNC_ASSERT_SEQUENCE(sequence, "callback started");
            UNIT_ASSERT(resume && !cancel);

            actor.Poison();
            ASYNC_ASSERT_SEQUENCE(sequence, "on cancel started");
            UNIT_ASSERT(cresume && !ccancel);
            UNIT_ASSERT(!cancel); // not cancelled yet

            actor.ResumeCoroutine(cresume);
            ASYNC_ASSERT_SEQUENCE(sequence, "on cancel resumed", "on cancel finished");
            UNIT_ASSERT(cancel); // now cancelled

            actor.ResumeCoroutine(resume);
            ASYNC_ASSERT_SEQUENCE(sequence, "callback resumed", "callback finished", "returning", "finished");
        }

        Y_UNIT_TEST(InterceptCancellationAsyncVoidOnCancelFinishThenUnwind) {
            TVector<TString> sequence;
            std::coroutine_handle<> resume, cancel;
            std::coroutine_handle<> cresume, ccancel;

            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                Y_DEFER { sequence.push_back("finished"); };

                auto callback = [&]() -> async<int> {
                    sequence.push_back("callback started");
                    Y_DEFER { sequence.push_back("callback finished"); };
                    co_await TSuspendAwaiter{ &resume, &cancel };
                    sequence.push_back("callback resumed");
                    co_return 42;
                };

                auto onCancel = [&]() -> async<void> {
                    sequence.push_back("on cancel started");
                    Y_DEFER { sequence.push_back("on cancel finished"); };
                    co_await TSuspendAwaiter{ &cresume, &ccancel };
                    sequence.push_back("on cancel resumed");
                };

                int value = co_await InterceptCancellation(callback, onCancel);

                UNIT_ASSERT_VALUES_EQUAL(value, 42);

                sequence.push_back("returning");
            });

            ASYNC_ASSERT_SEQUENCE(sequence, "callback started");
            UNIT_ASSERT(resume && !cancel);

            actor.Poison();
            ASYNC_ASSERT_SEQUENCE(sequence, "on cancel started");
            UNIT_ASSERT(cresume && !ccancel);
            UNIT_ASSERT(!cancel); // not cancelled yet

            actor.ResumeCoroutine(cresume);
            ASYNC_ASSERT_SEQUENCE(sequence, "on cancel resumed", "on cancel finished");
            UNIT_ASSERT(cancel); // now cancelled

            actor.ResumeCoroutine(cancel);
            ASYNC_ASSERT_SEQUENCE(sequence, "callback finished", "finished");
        }

        Y_UNIT_TEST(InterceptCancellationAsyncVoidResumeBeforeOnCancelFinish) {
            TVector<TString> sequence;
            std::coroutine_handle<> resume, cancel;
            std::coroutine_handle<> cresume, ccancel;

            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                Y_DEFER { sequence.push_back("finished"); };

                auto callback = [&]() -> async<int> {
                    sequence.push_back("callback started");
                    Y_DEFER { sequence.push_back("callback finished"); };
                    co_await TSuspendAwaiter{ &resume, &cancel };
                    sequence.push_back("callback resumed");
                    co_return 42;
                };

                auto onCancel = [&]() -> async<void> {
                    sequence.push_back("on cancel started");
                    Y_DEFER { sequence.push_back("on cancel finished"); };
                    co_await TSuspendAwaiter{ &cresume, &ccancel };
                    sequence.push_back("on cancel resumed");
                };

                int value = co_await InterceptCancellation(callback, onCancel);

                UNIT_ASSERT_VALUES_EQUAL(value, 42);

                sequence.push_back("returning");
            });

            ASYNC_ASSERT_SEQUENCE(sequence, "callback started");
            UNIT_ASSERT(resume && !cancel);

            actor.Poison();
            ASYNC_ASSERT_SEQUENCE(sequence, "on cancel started");
            UNIT_ASSERT(cresume && !ccancel);
            UNIT_ASSERT(!cancel); // not cancelled yet

            actor.ResumeCoroutine(resume);
            ASYNC_ASSERT_SEQUENCE(sequence, "callback resumed", "callback finished");
            UNIT_ASSERT(ccancel); // on cancel now cancelled

            actor.ResumeCoroutine(cresume);
            ASYNC_ASSERT_SEQUENCE(sequence, "on cancel resumed", "on cancel finished", "returning", "finished");
        }

        Y_UNIT_TEST(InterceptCancellationAsyncVoidResumeBeforeOnCancelUnwind) {
            TVector<TString> sequence;
            std::coroutine_handle<> resume, cancel;
            std::coroutine_handle<> cresume, ccancel;

            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                Y_DEFER { sequence.push_back("finished"); };

                auto callback = [&]() -> async<int> {
                    sequence.push_back("callback started");
                    Y_DEFER { sequence.push_back("callback finished"); };
                    co_await TSuspendAwaiter{ &resume, &cancel };
                    sequence.push_back("callback resumed");
                    co_return 42;
                };

                auto onCancel = [&]() -> async<void> {
                    sequence.push_back("on cancel started");
                    Y_DEFER { sequence.push_back("on cancel finished"); };
                    co_await TSuspendAwaiter{ &cresume, &ccancel };
                    sequence.push_back("on cancel resumed");
                };

                int value = co_await InterceptCancellation(callback, onCancel);

                UNIT_ASSERT_VALUES_EQUAL(value, 42);

                sequence.push_back("returning");
            });

            ASYNC_ASSERT_SEQUENCE(sequence, "callback started");
            UNIT_ASSERT(resume && !cancel);

            actor.Poison();
            ASYNC_ASSERT_SEQUENCE(sequence, "on cancel started");
            UNIT_ASSERT(cresume && !ccancel);
            UNIT_ASSERT(!cancel); // not cancelled yet

            actor.ResumeCoroutine(resume);
            ASYNC_ASSERT_SEQUENCE(sequence, "callback resumed", "callback finished");
            UNIT_ASSERT(ccancel); // on cancel now cancelled

            actor.ResumeCoroutine(ccancel);
            ASYNC_ASSERT_SEQUENCE(sequence, "on cancel finished", "returning", "finished");
        }

        Y_UNIT_TEST(InterceptCancellationAsyncVoidSuspendedTearDown) {
            TVector<TString> sequence;
            std::coroutine_handle<> resume, cancel;
            std::coroutine_handle<> cresume, ccancel;

            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                Y_DEFER { sequence.push_back("finished"); };

                auto callback = [&]() -> async<int> {
                    sequence.push_back("callback started");
                    Y_DEFER { sequence.push_back("callback finished"); };
                    co_await TSuspendAwaiter{ &resume, &cancel };
                    sequence.push_back("callback resumed");
                    co_return 42;
                };

                auto onCancel = [&]() -> async<void> {
                    sequence.push_back("on cancel started");
                    Y_DEFER { sequence.push_back("on cancel finished"); };
                    co_await TSuspendAwaiter{ &cresume, &ccancel };
                    sequence.push_back("on cancel resumed");
                };

                int value = co_await InterceptCancellation(callback, onCancel);

                UNIT_ASSERT_VALUES_EQUAL(value, 42);

                sequence.push_back("returning");
            });

            ASYNC_ASSERT_SEQUENCE(sequence, "callback started");
            UNIT_ASSERT(resume && !cancel);

            actor.Poison();
            ASYNC_ASSERT_SEQUENCE(sequence, "on cancel started");
            UNIT_ASSERT(cresume && !ccancel);
            UNIT_ASSERT(!cancel); // not cancelled yet

            // we expect cancellation frame to unwind first on shutdown
            runtime.CleanupNode();
            ASYNC_ASSERT_SEQUENCE(sequence, "on cancel finished", "callback finished", "finished");
        }

        Y_UNIT_TEST(InterceptCancellationAsyncVoidThrowsOnCall) {
            TVector<TString> sequence;
            std::coroutine_handle<> resume, cancel;
            TAsyncCancellationSource source;

            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                Y_DEFER { sequence.push_back("finished"); };

                auto callback = [&]() -> async<int> {
                    sequence.push_back("callback started");
                    Y_DEFER { sequence.push_back("callback finished"); };
                    co_await TSuspendAwaiter{ &resume, &cancel };
                    sequence.push_back("callback resumed");
                    co_return 42;
                };

                auto onCancel = [&]() -> async<void> {
                    sequence.push_back("on cancel");
                    throw std::runtime_error("on cancel exception");
                };

                co_await source.WithCancellation([&]() -> async<void> {
                    UNIT_ASSERT_EXCEPTION_CONTAINS(
                        co_await InterceptCancellation(callback, onCancel),
                        std::runtime_error, "on cancel exception");
                });

                sequence.push_back("returning");
            });

            ASYNC_ASSERT_SEQUENCE(sequence, "callback started");
            UNIT_ASSERT(resume && !cancel);

            actor.RunSync([&]{
                sequence.push_back("before cancel");
                source.Cancel();
                sequence.push_back("after cancel");
                // We must be cancelled already
                ASYNC_ASSERT_SEQUENCE(sequence, "before cancel", "on cancel", "after cancel");
                UNIT_ASSERT(cancel);
            });

            ASYNC_ASSERT_SEQUENCE_EMPTY(sequence);

            actor.ResumeCoroutine(resume);
            ASYNC_ASSERT_SEQUENCE(sequence, "callback resumed", "callback finished", "returning", "finished");
        }

        Y_UNIT_TEST(InterceptCancellationAsyncVoidThrowsOnStart) {
            TVector<TString> sequence;
            std::coroutine_handle<> resume, cancel;
            TAsyncCancellationSource source;

            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                Y_DEFER { sequence.push_back("finished"); };

                auto callback = [&]() -> async<int> {
                    sequence.push_back("callback started");
                    Y_DEFER { sequence.push_back("callback finished"); };
                    co_await TSuspendAwaiter{ &resume, &cancel };
                    sequence.push_back("callback resumed");
                    co_return 42;
                };

                auto onCancelReal = [&]() -> async<void> {
                    sequence.push_back("on cancel started");
                    throw std::runtime_error("on cancel exception");
                    co_return;
                };

                auto onCancel = [&]() -> async<void> {
                    sequence.push_back("on cancel");
                    return onCancelReal();
                };

                co_await source.WithCancellation([&]() -> async<void> {
                    UNIT_ASSERT_EXCEPTION_CONTAINS(
                        co_await InterceptCancellation(callback, onCancel),
                        std::runtime_error, "on cancel exception");
                });

                sequence.push_back("returning");
            });

            ASYNC_ASSERT_SEQUENCE(sequence, "callback started");
            UNIT_ASSERT(resume && !cancel);

            actor.RunSync([&]{
                sequence.push_back("before cancel");
                source.Cancel();
                sequence.push_back("after cancel");

                // We must not be cancelled yet (on cancel start scheduled)
                ASYNC_ASSERT_SEQUENCE(sequence, "before cancel", "on cancel", "after cancel");
                UNIT_ASSERT(!cancel);
            });

            // Cancel handler should start and cancel callback to rethrow exceptions
            ASYNC_ASSERT_SEQUENCE(sequence, "on cancel started");
            UNIT_ASSERT(cancel); // must be cancelled already

            actor.ResumeCoroutine(resume);
            ASYNC_ASSERT_SEQUENCE(sequence, "callback resumed", "callback finished", "returning", "finished");
        }

        Y_UNIT_TEST(InterceptCancellationAsyncVoidThrowsResumeBeforeStart) {
            TVector<TString> sequence;
            std::coroutine_handle<> resume, cancel;
            TAsyncCancellationSource source;

            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                Y_DEFER { sequence.push_back("finished"); };

                auto callback = [&]() -> async<int> {
                    sequence.push_back("callback started");
                    Y_DEFER { sequence.push_back("callback finished"); };
                    co_await TSuspendAwaiter{ &resume, &cancel };
                    sequence.push_back("callback resumed");
                    co_return 42;
                };

                auto onCancelReal = [&]() -> async<void> {
                    sequence.push_back("on cancel started");
                    throw std::runtime_error("on cancel exception");
                    co_return;
                };

                auto onCancel = [&]() -> async<void> {
                    sequence.push_back("on cancel");
                    return onCancelReal();
                };

                co_await source.WithCancellation([&]() -> async<void> {
                    UNIT_ASSERT_EXCEPTION_CONTAINS(
                        co_await InterceptCancellation(callback, onCancel),
                        std::runtime_error, "on cancel exception");
                });

                sequence.push_back("returning");
            });

            ASYNC_ASSERT_SEQUENCE(sequence, "callback started");
            UNIT_ASSERT(resume && !cancel);

            actor.RunSync([&]{
                sequence.push_back("before cancel");
                source.Cancel();
                sequence.push_back("after cancel");
                // We must not be cancelled yet (on cancel start scheduled)
                ASYNC_ASSERT_SEQUENCE(sequence, "before cancel", "on cancel", "after cancel");
                UNIT_ASSERT(!cancel);
                // Resume before the scheduled startup is processed
                // We must stop just after callback finishes
                resume.resume();
                ASYNC_ASSERT_SEQUENCE(sequence, "callback resumed", "callback finished");
            });

            // We must continue and rethrow the exception
            ASYNC_ASSERT_SEQUENCE(sequence, "on cancel started", "returning", "finished");
        }

        Y_UNIT_TEST(InterceptCancellationAsyncTrue) {
            TVector<TString> sequence;
            std::coroutine_handle<> resume, cancel;
            TAsyncCancellationSource source;

            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                Y_DEFER { sequence.push_back("finished"); };

                auto callback = [&]() -> async<int> {
                    sequence.push_back("callback started");
                    Y_DEFER { sequence.push_back("callback finished"); };
                    co_await TSuspendAwaiter{ &resume, &cancel };
                    sequence.push_back("callback resumed");
                    co_return 42;
                };

                auto onCancel = [&]() -> async<bool> {
                    sequence.push_back("on cancel started");
                    Y_DEFER { sequence.push_back("on cancel finished"); };
                    co_return true; // should cancel (like void)
                };

                co_await source.WithCancellation([&]() -> async<void> {
                    int value = co_await InterceptCancellation(callback, onCancel);

                    UNIT_ASSERT_VALUES_EQUAL(value, 42);
                });

                sequence.push_back("returning");
            });

            ASYNC_ASSERT_SEQUENCE(sequence, "callback started");
            UNIT_ASSERT(resume && !cancel);

            actor.RunSync([&]{
                sequence.push_back("before cancel");
                source.Cancel();
                sequence.push_back("after cancel");
                // We must not be cancelled yet (on cancel start scheduled)
                ASYNC_ASSERT_SEQUENCE(sequence, "before cancel", "after cancel");
                UNIT_ASSERT(!cancel);
            });

            // Cancel handler should start and we asked to cancel
            ASYNC_ASSERT_SEQUENCE(sequence, "on cancel started", "on cancel finished");
            UNIT_ASSERT(cancel);

            actor.ResumeCoroutine(resume);
            ASYNC_ASSERT_SEQUENCE(sequence, "callback resumed", "callback finished", "returning", "finished");
        }

        Y_UNIT_TEST(InterceptCancellationAsyncFalse) {
            TVector<TString> sequence;
            std::coroutine_handle<> resume, cancel;
            TAsyncCancellationSource source;

            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                Y_DEFER { sequence.push_back("finished"); };

                auto callback = [&]() -> async<int> {
                    sequence.push_back("callback started");
                    Y_DEFER { sequence.push_back("callback finished"); };
                    co_await TSuspendAwaiter{ &resume, &cancel };
                    sequence.push_back("callback resumed");
                    co_return 42;
                };

                auto onCancel = [&]() -> async<bool> {
                    sequence.push_back("on cancel started");
                    Y_DEFER { sequence.push_back("on cancel finished"); };
                    co_return false; // should not cancel
                };

                co_await source.WithCancellation([&]() -> async<void> {
                    int value = co_await InterceptCancellation(callback, onCancel);

                    UNIT_ASSERT_VALUES_EQUAL(value, 42);
                });

                sequence.push_back("returning");
            });

            ASYNC_ASSERT_SEQUENCE(sequence, "callback started");
            UNIT_ASSERT(resume && !cancel);

            actor.RunSync([&]{
                sequence.push_back("before cancel");
                source.Cancel();
                sequence.push_back("after cancel");

                // We must not be cancelled yet (on cancel start scheduled)
                ASYNC_ASSERT_SEQUENCE(sequence, "before cancel", "after cancel");
                UNIT_ASSERT(!cancel);
            });

            // Cancel handler should start, but we asked not to cancel
            ASYNC_ASSERT_SEQUENCE(sequence, "on cancel started", "on cancel finished");
            UNIT_ASSERT(!cancel);

            actor.ResumeCoroutine(resume);
            ASYNC_ASSERT_SEQUENCE(sequence, "callback resumed", "callback finished", "returning", "finished");
        }

    } // Y_UNIT_TEST_SUITE(Cancellation)

} // namespace NAsyncTest
