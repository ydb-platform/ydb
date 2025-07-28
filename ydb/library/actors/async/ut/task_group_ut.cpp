#include <ydb/library/actors/async/task_group.h>

#include "common.h"
#include <ydb/library/actors/async/timeout.h>
#include <ydb/library/actors/async/yield.h>

namespace NAsyncTest {

    Y_UNIT_TEST_SUITE(TaskGroup) {

        Y_UNIT_TEST(ImmediateReturn) {
            TVector<TString> sequence;
            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                sequence.push_back("started");
                Y_DEFER { sequence.push_back("finished"); };

                int value = co_await WithTaskGroup([&](auto& g) -> async<int> {
                    sequence.push_back("group started");
                    Y_DEFER { sequence.push_back("group finished"); };
                    UNIT_ASSERT_VALUES_EQUAL(g.Ready(), false);
                    UNIT_ASSERT_VALUES_EQUAL(g.Running(), 0u);
                    sequence.push_back("group returning");
                    co_return 42;
                });

                UNIT_ASSERT_VALUES_EQUAL(value, 42);

                sequence.push_back("returning");
            });

            ASYNC_ASSERT_SEQUENCE(sequence,
                "started", "group started",
                "group returning", "group finished",
                "returning", "finished");
        }

        Y_UNIT_TEST(TaskDiscardedBeforeStart) {
            TVector<TString> sequence;
            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                sequence.push_back("started");
                Y_DEFER { sequence.push_back("finished"); };

                int value = co_await WithTaskGroup([&](auto& g) -> async<int> {
                    sequence.push_back("group started");
                    Y_DEFER { sequence.push_back("group finished"); };
                    size_t index = g.Add([&]() -> async<void> {
                        sequence.push_back("task started");
                        co_return;
                    });
                    UNIT_ASSERT_VALUES_EQUAL(index, 0u);
                    UNIT_ASSERT_VALUES_EQUAL(g.Running(), 1u);
                    sequence.push_back("group returning");
                    co_return 42;
                });

                UNIT_ASSERT_VALUES_EQUAL(value, 42);

                sequence.push_back("returning");
            });

            // Note: task doesn't even start
            ASYNC_ASSERT_SEQUENCE(sequence,
                "started", "group started",
                "group returning", "group finished",
                "returning", "finished");
        }

        Y_UNIT_TEST(TaskStartNotRecursive) {
            TVector<TString> sequence;
            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                sequence.push_back("started");
                Y_DEFER { sequence.push_back("finished"); };

                int value = co_await WithTaskGroup<int>([&](auto& g) -> async<int> {
                    sequence.push_back("group started");
                    Y_DEFER { sequence.push_back("group finished"); };
                    size_t index = g.Add([&]() -> async<int> {
                        sequence.push_back("task started");
                        co_return 42;
                    });
                    UNIT_ASSERT_VALUES_EQUAL(index, 0u);
                    UNIT_ASSERT_VALUES_EQUAL(g.Ready(), false);
                    UNIT_ASSERT_VALUES_EQUAL(g.Running(), 1u);
                    sequence.push_back("group waiting");
                    int value = co_await g.Next();
                    UNIT_ASSERT_VALUES_EQUAL(g.Running(), 0u);
                    sequence.push_back("group returning");
                    co_return value;
                });

                UNIT_ASSERT_VALUES_EQUAL(value, 42);

                sequence.push_back("returning");
            });

            // Note: task starts after group starts waiting
            ASYNC_ASSERT_SEQUENCE(sequence,
                "started", "group started",
                "group waiting", "task started",
                "group returning", "group finished",
                "returning", "finished");
        }

        Y_UNIT_TEST(TaskImmediateReturnBeforeNextTask) {
            TVector<TString> sequence;
            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                sequence.push_back("started");
                Y_DEFER { sequence.push_back("finished"); };

                int value = co_await WithTaskGroup<int>([&](auto& g) -> async<int> {
                    sequence.push_back("group started");
                    Y_DEFER { sequence.push_back("group finished"); };
                    size_t index = g.Add([&]() -> async<int> {
                        sequence.push_back("task 1 started");
                        co_return 42;
                    });
                    UNIT_ASSERT_VALUES_EQUAL(index, 0u);
                    size_t index2 = g.Add([&]() -> async<int> {
                        sequence.push_back("task 2 started");
                        co_return 43;
                    });
                    UNIT_ASSERT_VALUES_EQUAL(index2, 1u);
                    UNIT_ASSERT_VALUES_EQUAL(g.Ready(), false);
                    UNIT_ASSERT_VALUES_EQUAL(g.Running(), 2u);
                    sequence.push_back("group waiting");
                    int value = co_await g.Next();
                    sequence.push_back("group resumed");
                    UNIT_ASSERT_VALUES_EQUAL(g.Running(), 1u);
                    sequence.push_back("group returning");
                    co_return value;
                });

                UNIT_ASSERT_VALUES_EQUAL(value, 42);

                sequence.push_back("returning");
            });

            ASYNC_ASSERT_SEQUENCE(sequence,
                "started", "group started",
                "group waiting", "task 1 started", "group resumed",
                "group returning", "group finished",
                "returning", "finished");
        }

        Y_UNIT_TEST(TaskThrowsImmediately) {
            TVector<TString> sequence;
            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                sequence.push_back("started");
                Y_DEFER { sequence.push_back("finished"); };

                int value = co_await WithTaskGroup<int>([&](auto& g) -> async<int> {
                    sequence.push_back("group started");
                    Y_DEFER { sequence.push_back("group finished"); };
                    size_t index = g.Add([&]() -> async<int> {
                        // Note: not a coroutine and throws immediately
                        sequence.push_back("task callback throwing");
                        throw TTestException() << "throws immediately";
                    });
                    UNIT_ASSERT_VALUES_EQUAL(index, 0u);
                    UNIT_ASSERT_VALUES_EQUAL(g.Ready(), false);
                    UNIT_ASSERT_VALUES_EQUAL(g.Running(), 1u);
                    sequence.push_back("group waiting");
                    UNIT_ASSERT_EXCEPTION_CONTAINS(co_await g.Next(), TTestException, "throws immediately");
                    sequence.push_back("group returning");
                    co_return 42;
                });

                UNIT_ASSERT_VALUES_EQUAL(value, 42);

                sequence.push_back("returning");
            });

            ASYNC_ASSERT_SEQUENCE(sequence,
                "started", "group started",
                "group waiting", "task callback throwing",
                "group returning", "group finished",
                "returning", "finished");
        }

        Y_UNIT_TEST(TaskThrowsFromCoroutine) {
            TVector<TString> sequence;
            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                sequence.push_back("started");
                Y_DEFER { sequence.push_back("finished"); };

                int value = co_await WithTaskGroup<int>([&](auto& g) -> async<int> {
                    sequence.push_back("group started");
                    Y_DEFER { sequence.push_back("group finished"); };
                    size_t index = g.Add([&]() -> async<int> {
                        sequence.push_back("task throwing");
                        throw TTestException() << "throws immediately";
                        co_return 42;
                    });
                    UNIT_ASSERT_VALUES_EQUAL(index, 0u);
                    UNIT_ASSERT_VALUES_EQUAL(g.Ready(), false);
                    UNIT_ASSERT_VALUES_EQUAL(g.Running(), 1u);
                    sequence.push_back("group waiting");
                    UNIT_ASSERT_EXCEPTION_CONTAINS(co_await g.Next(), TTestException, "throws immediately");
                    sequence.push_back("group returning");
                    co_return 42;
                });

                UNIT_ASSERT_VALUES_EQUAL(value, 42);

                sequence.push_back("returning");
            });

            ASYNC_ASSERT_SEQUENCE(sequence,
                "started", "group started",
                "group waiting", "task throwing",
                "group returning", "group finished",
                "returning", "finished");
        }

        Y_UNIT_TEST(TaskAwaitedBeforeReturn) {
            TVector<TString> sequence;
            std::coroutine_handle<> tresume, tcancel;
            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                sequence.push_back("started");
                Y_DEFER { sequence.push_back("finished"); };

                int value = co_await WithTaskGroup([&](auto& g) -> async<int> {
                    sequence.push_back("group started");
                    Y_DEFER { sequence.push_back("group finished"); };
                    g.Add([&]() -> async<void> {
                        Y_DEFER { sequence.push_back("task finished"); };
                        sequence.push_back("task suspending");
                        co_await TSuspendAwaiter{ &tresume, &tcancel };
                        sequence.push_back("task resumed");
                    });
                    UNIT_ASSERT(!tresume);
                    sequence.push_back("group yielding");
                    co_await AsyncYield();
                    sequence.push_back("group resumed");
                    UNIT_ASSERT(tresume && !tcancel);
                    sequence.push_back("group returning");
                    co_return 42;
                });

                UNIT_ASSERT_VALUES_EQUAL(value, 42);

                sequence.push_back("returning");
            });

            // Note: AsyncYield pending on the mailbox, but task must have started
            ASYNC_ASSERT_SEQUENCE(sequence,
                "started", "group started",
                "group yielding", "task suspending");
            UNIT_ASSERT(tresume && !tcancel);

            actor.Step();
            ASYNC_ASSERT_SEQUENCE(sequence,
                "group resumed", "group returning", "group finished");
            UNIT_ASSERT(tcancel);

            actor.ResumeCoroutine(tresume);
            ASYNC_ASSERT_SEQUENCE(sequence,
                "task resumed", "task finished",
                "returning", "finished");
        }

        Y_UNIT_TEST(TaskNotCancelledUntilGroupReturns) {
            TVector<TString> sequence;
            std::coroutine_handle<> gresume, gcancel;
            std::coroutine_handle<> tresume, tcancel;
            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                sequence.push_back("started");
                Y_DEFER { sequence.push_back("finished"); };

                int value = co_await WithTaskGroup([&](auto& g) -> async<int> {
                    sequence.push_back("group started");
                    Y_DEFER { sequence.push_back("group finished"); };
                    g.Add([&]() -> async<void> {
                        Y_DEFER { sequence.push_back("task finished"); };
                        sequence.push_back("task suspending");
                        co_await TSuspendAwaiter{ &tresume, &tcancel };
                        sequence.push_back("task resumed");
                    });
                    sequence.push_back("group suspending");
                    co_await TSuspendAwaiter{ &gresume, &gcancel };
                    sequence.push_back("group returning");
                    co_return 42;
                });

                UNIT_ASSERT_VALUES_EQUAL(value, 42);

                sequence.push_back("returning");
            });

            ASYNC_ASSERT_SEQUENCE(sequence,
                "started", "group started",
                "group suspending", "task suspending");
            UNIT_ASSERT(gresume && !gcancel);
            UNIT_ASSERT(tresume && !tcancel);

            actor.Poison();
            ASYNC_ASSERT_SEQUENCE_EMPTY(sequence);
            UNIT_ASSERT(gcancel);
            UNIT_ASSERT(!tcancel);

            actor.ResumeCoroutine(gresume);
            ASYNC_ASSERT_SEQUENCE(sequence,
                "group returning", "group finished");
            UNIT_ASSERT(tcancel);

            actor.ResumeCoroutine(tresume);
            ASYNC_ASSERT_SEQUENCE(sequence,
                "task resumed", "task finished",
                "returning", "finished");
            UNIT_ASSERT(state.Destroyed);
        }

        Y_UNIT_TEST(TaskCancelAfterGroupCancel) {
            TVector<TString> sequence;
            std::coroutine_handle<> gresume, gcancel;
            std::coroutine_handle<> tresume, tcancel;
            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                sequence.push_back("started");
                Y_DEFER { sequence.push_back("finished"); };

                int value = co_await WithTaskGroup([&](auto& g) -> async<int> {
                    sequence.push_back("group started");
                    Y_DEFER { sequence.push_back("group finished"); };
                    g.Add([&]() -> async<void> {
                        Y_DEFER { sequence.push_back("task finished"); };
                        sequence.push_back("task suspending");
                        co_await TSuspendAwaiter{ &tresume, &tcancel };
                        sequence.push_back("task resumed");
                    });
                    sequence.push_back("group suspending");
                    co_await TSuspendAwaiter{ &gresume, &gcancel };
                    sequence.push_back("group returning");
                    co_return 42;
                });

                UNIT_ASSERT_VALUES_EQUAL(value, 42);

                sequence.push_back("returning");
            });

            ASYNC_ASSERT_SEQUENCE(sequence,
                "started", "group started",
                "group suspending", "task suspending");
            UNIT_ASSERT(gresume && !gcancel);
            UNIT_ASSERT(tresume && !tcancel);

            actor.Poison();
            ASYNC_ASSERT_SEQUENCE_EMPTY(sequence);
            UNIT_ASSERT(gcancel);
            UNIT_ASSERT(!tcancel);

            actor.ResumeCoroutine(gcancel);
            ASYNC_ASSERT_SEQUENCE(sequence,
                "group finished");
            UNIT_ASSERT(tcancel);

            actor.ResumeCoroutine(tcancel);
            ASYNC_ASSERT_SEQUENCE(sequence,
                "task finished", "finished");
            UNIT_ASSERT(state.Destroyed);
        }

        Y_UNIT_TEST(TaskAwaitCancelThenRetry) {
            TVector<TString> sequence;
            std::coroutine_handle<> tresume, tcancel;
            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                sequence.push_back("started");
                Y_DEFER { sequence.push_back("finished"); };

                int value = co_await WithTaskGroup<int>([&](auto& g) -> async<int> {
                    sequence.push_back("group started");
                    Y_DEFER { sequence.push_back("group finished"); };
                    g.Add([&]() -> async<int> {
                        Y_DEFER { sequence.push_back("task finished"); };
                        sequence.push_back("task suspending");
                        co_await TSuspendAwaiter{ &tresume, &tcancel };
                        sequence.push_back("task resumed");
                        co_return 42;
                    });
                    auto callNext = [&]() -> async<int> {
                        co_return co_await g.Next();
                    };
                    sequence.push_back("group g.Next() with timeout 1ms");
                    auto result1 = co_await WithTimeout(TDuration::MilliSeconds(1), callNext);
                    UNIT_ASSERT(!result1);
                    sequence.push_back("group g.Next() with timeout 10ms");
                    auto result2 = co_await WithTimeout(TDuration::MilliSeconds(10), callNext);
                    UNIT_ASSERT(result2);
                    sequence.push_back("group returning");
                    co_return *result2;
                });

                UNIT_ASSERT_VALUES_EQUAL(value, 42);

                sequence.push_back("returning");
            });

            ASYNC_ASSERT_SEQUENCE(sequence,
                "started", "group started",
                "group g.Next() with timeout 1ms",
                "task suspending");
            UNIT_ASSERT(tresume && !tcancel);

            runtime.SimulateSleep(TDuration::MilliSeconds(2));
            ASYNC_ASSERT_SEQUENCE(sequence,
                "group g.Next() with timeout 10ms");

            actor.ResumeCoroutine(tresume);
            ASYNC_ASSERT_SEQUENCE(sequence,
                "task resumed", "task finished",
                "group returning", "group finished",
                "returning", "finished");
        }

        /**
         * This is a testing infra test
         *
         * It validates we can actually step one event at a time with actor.Step()
         */
        Y_UNIT_TEST(TestSingleStepTesting) {
            TVector<TString> sequence;
            std::coroutine_handle<> resume1, cancel1;
            std::coroutine_handle<> resume2, cancel2;
            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                sequence.push_back("started");
                Y_DEFER { sequence.push_back("finished"); };

                co_await WithTaskGroup([&](auto& g) -> async<void> {
                    sequence.push_back("group started");
                    Y_DEFER { sequence.push_back("group finished"); };
                    g.Add([&]() -> async<void> {
                        sequence.push_back("task 1 started");
                        co_await AsyncYield();
                        sequence.push_back("task 1 suspending");
                        co_await TSuspendAwaiter{ &resume1, &cancel1 };
                        sequence.push_back("task 1 resumed");
                    });
                    g.Add([&]() -> async<void> {
                        sequence.push_back("task 2 started");
                        co_await AsyncYield();
                        sequence.push_back("task 2 suspending");
                        co_await TSuspendAwaiter{ &resume2, &cancel2 };
                        sequence.push_back("task 2 resumed");
                    });
                    while (g) {
                        sequence.push_back("group waiting");
                        co_await g.Next();
                        sequence.push_back("group resumed");
                    }
                    sequence.push_back("group returning");
                });

                sequence.push_back("returning");
            });

            ASYNC_ASSERT_SEQUENCE(sequence,
                "started", "group started", "group waiting",
                "task 1 started", "task 2 started");
            UNIT_ASSERT(!resume1 && !resume2);

            actor.Step();
            ASYNC_ASSERT_SEQUENCE(sequence,
                "task 1 suspending");
            UNIT_ASSERT(resume1 && !cancel1);

            actor.Step();
            ASYNC_ASSERT_SEQUENCE(sequence,
                "task 2 suspending");
            UNIT_ASSERT(resume2 && !cancel2);

            actor.ResumeCoroutine(resume1);
            ASYNC_ASSERT_SEQUENCE(sequence,
                "task 1 resumed", "group resumed", "group waiting");

            actor.ResumeCoroutine(resume2);
            ASYNC_ASSERT_SEQUENCE(sequence,
                "task 2 resumed", "group resumed",
                "group returning", "group finished",
                "returning", "finished");
        }

    } // Y_UNIT_TEST_SUITE(TaskGroup)

} // namespace NAsyncTest
