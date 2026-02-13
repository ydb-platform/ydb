#include <ydb/library/actors/async/event.h>
#include "common.h"
#include <ydb/library/actors/async/cancellation.h>

namespace NAsyncTest {

    Y_UNIT_TEST_SUITE(Event) {

        Y_UNIT_TEST(NotifyOne) {
            TVector<TString> sequence;
            TAsyncEvent event;
            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                sequence.push_back("started");
                Y_DEFER { sequence.push_back("finished"); };

                bool resumed = co_await event.Wait();
                sequence.push_back(resumed ? "resumed" : "detached");
            });

            ASYNC_ASSERT_SEQUENCE(sequence, "started");

            UNIT_ASSERT(event.HasAwaiters());
            UNIT_ASSERT_VALUES_EQUAL(event.AwaitersCount(), 1u);

            actor.RunAsync([&]() -> async<void> {
                sequence.push_back("second started");
                Y_DEFER { sequence.push_back("second finished"); };

                bool resumed = co_await event.Wait();
                sequence.push_back(resumed ? "second resumed" : "second detached");
            });

            ASYNC_ASSERT_SEQUENCE(sequence, "second started");
            UNIT_ASSERT_VALUES_EQUAL(event.AwaitersCount(), 2u);

            actor.RunSync([&]{
                sequence.push_back("calling NotifyOne");
                event.NotifyOne();
                sequence.push_back("after NotifyOne");
            });

            ASYNC_ASSERT_SEQUENCE(sequence,
                "calling NotifyOne",
                "after NotifyOne",
                "resumed",
                "finished");
            UNIT_ASSERT_VALUES_EQUAL(event.AwaitersCount(), 1u);

            actor.Poison();
            ASYNC_ASSERT_SEQUENCE(sequence, "second finished");

            UNIT_ASSERT(!event.HasAwaiters());
            UNIT_ASSERT_VALUES_EQUAL(event.AwaitersCount(), 0u);
        }

        Y_UNIT_TEST(NotifyAll) {
            TVector<TString> sequence;
            TAsyncEvent event;
            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                sequence.push_back("started");
                Y_DEFER { sequence.push_back("finished"); };

                bool resumed = co_await event.Wait();
                sequence.push_back(resumed ? "resumed" : "detached");
            });

            ASYNC_ASSERT_SEQUENCE(sequence, "started");

            UNIT_ASSERT(event.HasAwaiters());
            UNIT_ASSERT_VALUES_EQUAL(event.AwaitersCount(), 1u);

            actor.RunAsync([&]() -> async<void> {
                sequence.push_back("second started");
                Y_DEFER { sequence.push_back("second finished"); };

                bool resumed = co_await event.Wait();
                sequence.push_back(resumed ? "second resumed" : "second detached");
            });

            ASYNC_ASSERT_SEQUENCE(sequence, "second started");
            UNIT_ASSERT_VALUES_EQUAL(event.AwaitersCount(), 2u);

            actor.RunSync([&]{
                sequence.push_back("calling NotifyAll");
                event.NotifyAll();
                sequence.push_back("after NotifyAll");
                UNIT_ASSERT(!event.HasAwaiters());
            });

            ASYNC_ASSERT_SEQUENCE(sequence,
                "calling NotifyAll",
                "after NotifyAll",
                "resumed",
                "finished",
                "second resumed",
                "second finished");

            UNIT_ASSERT(!event.HasAwaiters());
            UNIT_ASSERT_VALUES_EQUAL(event.AwaitersCount(), 0u);
        }

        Y_UNIT_TEST(AlreadyCancelled) {
            TVector<TString> sequence;
            TAsyncEvent event;
            TAsyncCancellationScope scope;
            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                sequence.push_back("started");
                Y_DEFER { sequence.push_back("finished"); };

                bool success = co_await scope.Wrap([&]() -> async<void> {
                    Y_DEFER { sequence.push_back("scope.Wrap finished"); };
                    scope.Cancel();
                    sequence.push_back("calling Wait");
                    bool resumed = co_await event.Wait();
                    sequence.push_back(resumed ? "resumed" : "detached");
                });

                if (!success) {
                    sequence.push_back("was cancelled");
                }

                sequence.push_back("returning");
            });

            ASYNC_ASSERT_SEQUENCE(sequence,
                "started",
                "calling Wait",
                "scope.Wrap finished",
                "was cancelled",
                "returning",
                "finished");

            UNIT_ASSERT(!event.HasAwaiters());
            UNIT_ASSERT_VALUES_EQUAL(event.AwaitersCount(), 0u);
        }

        Y_UNIT_TEST(Cancel) {
            TVector<TString> sequence;
            TAsyncEvent event;
            TAsyncCancellationScope scope;
            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                sequence.push_back("started");
                Y_DEFER { sequence.push_back("finished"); };

                bool success = co_await scope.Wrap([&]() -> async<void> {
                    Y_DEFER { sequence.push_back("scope.Wrap finished"); };
                    sequence.push_back("calling Wait");
                    bool resumed = co_await event.Wait();
                    sequence.push_back(resumed ? "resumed" : "detached");
                });

                if (!success) {
                    sequence.push_back("was cancelled");
                }

                sequence.push_back("returning");
            });

            ASYNC_ASSERT_SEQUENCE(sequence,
                "started",
                "calling Wait");

            UNIT_ASSERT(event.HasAwaiters());
            UNIT_ASSERT_VALUES_EQUAL(event.AwaitersCount(), 1u);

            actor.RunSync([&]{
                UNIT_ASSERT(event.HasAwaiters());
                sequence.push_back("calling Cancel");
                scope.Cancel();
                sequence.push_back("after Cancel");
                UNIT_ASSERT(!event.HasAwaiters());
            });

            ASYNC_ASSERT_SEQUENCE(sequence,
                "calling Cancel",
                "after Cancel",
                "scope.Wrap finished",
                "was cancelled",
                "returning",
                "finished");

            UNIT_ASSERT(!event.HasAwaiters());
            UNIT_ASSERT_VALUES_EQUAL(event.AwaitersCount(), 0u);
        }

        Y_UNIT_TEST(CancelAfterNotify) {
            TVector<TString> sequence;
            TAsyncEvent event;
            TAsyncCancellationScope scope;
            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                sequence.push_back("started");
                Y_DEFER { sequence.push_back("finished"); };

                bool success = co_await scope.Wrap([&]() -> async<void> {
                    Y_DEFER { sequence.push_back("scope.Wrap finished"); };
                    sequence.push_back("calling Wait");
                    bool resumed = co_await event.Wait();
                    sequence.push_back(resumed ? "resumed" : "detached");
                });

                if (!success) {
                    sequence.push_back("was cancelled");
                }

                sequence.push_back("returning");
            });

            ASYNC_ASSERT_SEQUENCE(sequence,
                "started",
                "calling Wait");

            UNIT_ASSERT(event.HasAwaiters());
            UNIT_ASSERT_VALUES_EQUAL(event.AwaitersCount(), 1u);

            actor.RunSync([&]{
                UNIT_ASSERT(event.HasAwaiters());
                sequence.push_back("calling NotifyOne");
                event.NotifyOne();
                UNIT_ASSERT(!event.HasAwaiters());
                sequence.push_back("calling Cancel");
                scope.Cancel();
                sequence.push_back("after Cancel");
            });

            ASYNC_ASSERT_SEQUENCE(sequence,
                "calling NotifyOne",
                "calling Cancel",
                "after Cancel",
                "resumed",
                "scope.Wrap finished",
                "returning",
                "finished");

            UNIT_ASSERT(!event.HasAwaiters());
            UNIT_ASSERT_VALUES_EQUAL(event.AwaitersCount(), 0u);
        }

        Y_UNIT_TEST(Detach) {
            TVector<TString> sequence;
            std::unique_ptr<TAsyncEvent> event;
            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            event = std::make_unique<TAsyncEvent>();

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                sequence.push_back("started");
                Y_DEFER { sequence.push_back("finished"); };

                sequence.push_back("calling Wait");
                bool resumed = co_await event->Wait();
                sequence.push_back(resumed ? "resumed" : "detached");
            });

            ASYNC_ASSERT_SEQUENCE(sequence,
                "started",
                "calling Wait");

            UNIT_ASSERT(event->HasAwaiters());
            UNIT_ASSERT_VALUES_EQUAL(event->AwaitersCount(), 1u);

            actor.RunSync([&]{
                sequence.push_back("calling reset");
                event.reset();
                sequence.push_back("after reset");
            });

            ASYNC_ASSERT_SEQUENCE(sequence,
                "calling reset",
                "after reset",
                "detached",
                "finished");
        }

        Y_UNIT_TEST(CancelAfterDetach) {
            TVector<TString> sequence;
            std::unique_ptr<TAsyncEvent> event;
            TAsyncCancellationScope scope;
            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            event = std::make_unique<TAsyncEvent>();

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                sequence.push_back("started");
                Y_DEFER { sequence.push_back("finished"); };

                bool success = co_await scope.Wrap([&]() -> async<void> {
                    Y_DEFER { sequence.push_back("scope.Wrap finished"); };
                    sequence.push_back("calling Wait");
                    bool resumed = co_await event->Wait();
                    sequence.push_back(resumed ? "resumed" : "detached");
                });

                if (!success) {
                    sequence.push_back("was cancelled");
                }

                sequence.push_back("returning");
            });

            ASYNC_ASSERT_SEQUENCE(sequence,
                "started",
                "calling Wait");

            UNIT_ASSERT(event->HasAwaiters());
            UNIT_ASSERT_VALUES_EQUAL(event->AwaitersCount(), 1u);

            actor.RunSync([&]{
                sequence.push_back("calling reset");
                event.reset();
                scope.Cancel();
                sequence.push_back("after reset");
            });

            ASYNC_ASSERT_SEQUENCE(sequence,
                "calling reset",
                "after reset",
                "detached",
                "scope.Wrap finished",
                "returning",
                "finished");
        }

        Y_UNIT_TEST(RemoveOnShutdown) {
            TVector<TString> sequence;
            TAsyncEvent event;
            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                sequence.push_back("started");
                Y_DEFER { sequence.push_back("finished"); };

                bool resumed = co_await event.Wait();
                sequence.push_back(resumed ? "resumed" : "detached");
            });

            ASYNC_ASSERT_SEQUENCE(sequence, "started");

            UNIT_ASSERT(event.HasAwaiters());
            UNIT_ASSERT_VALUES_EQUAL(event.AwaitersCount(), 1u);

            runtime.CleanupNode();
            ASYNC_ASSERT_SEQUENCE(sequence,
                "finished");

            UNIT_ASSERT(!event.HasAwaiters());
            UNIT_ASSERT_VALUES_EQUAL(event.AwaitersCount(), 0u);
        }

        Y_UNIT_TEST(NoResumeOnShutdown) {
            TVector<TString> sequence;
            TAsyncEvent event;
            int quota = 1;
            std::coroutine_handle<> resume;
            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto acquireQuota = [&]() -> async<void> {
                if (quota > 0) {
                    quota--;
                    co_return;
                }
                co_await event.Wait();
            };

            auto releaseQuota = [&]() {
                if (event.HasAwaiters()) {
                    event.NotifyOne();
                } else {
                    quota++;
                }
            };

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                sequence.push_back("started");
                Y_DEFER { sequence.push_back("finished"); };

                co_await acquireQuota();
                Y_DEFER { releaseQuota(); };

                sequence.push_back("suspending");
                co_await TSuspendAwaiterWithoutCancel{ &resume };

                sequence.push_back("resumed");
            });

            actor.RunAsync([&]() -> async<void> {
                sequence.push_back("second started");
                Y_DEFER { sequence.push_back("second finished"); };

                co_await acquireQuota();
                Y_DEFER { releaseQuota(); };

                sequence.push_back("second resumed");
            });

            ASYNC_ASSERT_SEQUENCE(sequence,
                "started", "suspending", "second started");

            runtime.CleanupNode();
            ASYNC_ASSERT_SEQUENCE(sequence,
                "finished", "second finished");

            UNIT_ASSERT(!event.HasAwaiters());
            UNIT_ASSERT_VALUES_EQUAL(event.AwaitersCount(), 0u);
        }

        Y_UNIT_TEST(CallbackOnSuspend) {
            TVector<TString> sequence;
            TAsyncEvent event;
            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                sequence.push_back("started");
                Y_DEFER { sequence.push_back("finished"); };

                bool resumed = co_await event.Wait([&]{
                    sequence.push_back("suspended");
                });
                sequence.push_back(resumed ? "resumed" : "detached");
            });

            ASYNC_ASSERT_SEQUENCE(sequence,
                "started",
                "suspended");

            UNIT_ASSERT(event.HasAwaiters());
            UNIT_ASSERT_VALUES_EQUAL(event.AwaitersCount(), 1u);
        }

        Y_UNIT_TEST(ExceptionOnSuspend) {
            TVector<TString> sequence;
            TAsyncEvent event;
            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                sequence.push_back("started");
                Y_DEFER { sequence.push_back("finished"); };

                auto callback = [&]{
                    sequence.push_back(TStringBuilder() << "have " << event.AwaitersCount() << " awaiters");
                    throw TTestException();
                };

                UNIT_ASSERT_EXCEPTION(co_await event.Wait(callback), TTestException);

                sequence.push_back("returning");
            });

            ASYNC_ASSERT_SEQUENCE(sequence,
                "started",
                "have 1 awaiters",
                "returning",
                "finished");

            UNIT_ASSERT(!event.HasAwaiters());
            UNIT_ASSERT_VALUES_EQUAL(event.AwaitersCount(), 0u);
        }

    } // Y_UNIT_TEST_SUITE(Event)

} // namespace NAsyncTest
