#include <ydb/library/actors/async/task_group.h>

#include "common.h"
#include <ydb/library/actors/async/timeout.h>
#include <ydb/library/actors/async/yield.h>

namespace NAsyncTest {

    Y_UNIT_TEST_SUITE(TaskGroup) {

        Y_UNIT_TEST(ImmediateReturn) {
            bool finished = false;
            bool finishedNormally = false;
            bool finishedInnerNormally = false;
            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                Y_DEFER { finished = true; };

                int value = co_await WithTaskGroup([&](auto& g) -> async<int> {
                    UNIT_ASSERT_VALUES_EQUAL(g.Ready(), false);
                    UNIT_ASSERT_VALUES_EQUAL(g.Running(), 0u);
                    finishedInnerNormally = true;
                    co_return 42;
                });

                UNIT_ASSERT_VALUES_EQUAL(value, 42);

                finishedNormally = true;
            });

            UNIT_ASSERT(finishedInnerNormally);
            UNIT_ASSERT(finishedNormally);
            UNIT_ASSERT(finished);
        }

        Y_UNIT_TEST(TaskDiscardedBeforeStart) {
            bool finished = false;
            bool finishedNormally = false;
            bool finishedInnerNormally = false;
            bool innerTaskStarted = false;
            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                Y_DEFER { finished = true; };

                int value = co_await WithTaskGroup([&](auto& g) -> async<int> {
                    size_t index = g.Add([&]() -> async<void> {
                        innerTaskStarted = false;
                        co_return;
                    });
                    UNIT_ASSERT_VALUES_EQUAL(index, 0u);
                    UNIT_ASSERT_VALUES_EQUAL(g.Running(), 1u);
                    finishedInnerNormally = true;
                    co_return 42;
                });

                UNIT_ASSERT_VALUES_EQUAL(value, 42);

                finishedNormally = true;
            });

            UNIT_ASSERT(!innerTaskStarted);
            UNIT_ASSERT(finishedInnerNormally);
            UNIT_ASSERT(finishedNormally);
            UNIT_ASSERT(finished);
        }

        Y_UNIT_TEST(TaskStartNotRecursive) {
            bool finished = false;
            bool finishedNormally = false;
            bool finishedInnerNormally = false;
            bool innerTaskStarted = false;
            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                Y_DEFER { finished = true; };

                int value = co_await WithTaskGroup<int>([&](auto& g) -> async<int> {
                    size_t index = g.Add([&]() -> async<int> {
                        innerTaskStarted = true;
                        co_return 42;
                    });
                    UNIT_ASSERT_VALUES_EQUAL(index, 0u);
                    UNIT_ASSERT_VALUES_EQUAL(g.Ready(), false);
                    UNIT_ASSERT_VALUES_EQUAL(g.Running(), 1u);
                    UNIT_ASSERT_VALUES_EQUAL(innerTaskStarted, false);
                    int value = co_await g.Next();
                    UNIT_ASSERT_VALUES_EQUAL(g.Running(), 0u);
                    finishedInnerNormally = true;
                    co_return value;
                });

                UNIT_ASSERT_VALUES_EQUAL(value, 42);

                finishedNormally = true;
            });

            UNIT_ASSERT(finishedInnerNormally);
            UNIT_ASSERT(finishedNormally);
            UNIT_ASSERT(finished);
        }

        Y_UNIT_TEST(TaskImmediateReturnBeforeNextTask) {
            bool finished = false;
            bool finishedNormally = false;
            bool finishedInnerNormally = false;
            bool innerTaskStarted = false;
            bool secondTaskStarted = false;
            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                Y_DEFER { finished = true; };

                int value = co_await WithTaskGroup<int>([&](auto& g) -> async<int> {
                    size_t index = g.Add([&]() -> async<int> {
                        innerTaskStarted = true;
                        co_return 42;
                    });
                    UNIT_ASSERT_VALUES_EQUAL(index, 0u);
                    size_t index2 = g.Add([&]() -> async<int> {
                        secondTaskStarted = true;
                        co_return 43;
                    });
                    UNIT_ASSERT_VALUES_EQUAL(index2, 1u);
                    UNIT_ASSERT_VALUES_EQUAL(g.Ready(), false);
                    UNIT_ASSERT_VALUES_EQUAL(g.Running(), 2u);
                    UNIT_ASSERT_VALUES_EQUAL(innerTaskStarted, false);
                    UNIT_ASSERT_VALUES_EQUAL(secondTaskStarted, false);
                    int value = co_await g.Next();
                    UNIT_ASSERT_VALUES_EQUAL(secondTaskStarted, false);
                    UNIT_ASSERT_VALUES_EQUAL(g.Running(), 1u);
                    finishedInnerNormally = true;
                    co_return value;
                });

                UNIT_ASSERT_VALUES_EQUAL(value, 42);

                finishedNormally = true;
            });

            UNIT_ASSERT_VALUES_EQUAL(secondTaskStarted, false);
            UNIT_ASSERT(finishedInnerNormally);
            UNIT_ASSERT(finishedNormally);
            UNIT_ASSERT(finished);
        }

        Y_UNIT_TEST(TaskThrowsImmediately) {
            bool finished = false;
            bool finishedNormally = false;
            bool finishedInnerNormally = false;
            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                Y_DEFER { finished = true; };

                int value = co_await WithTaskGroup<int>([&](auto& g) -> async<int> {
                    size_t index = g.Add([&]() -> async<int> {
                        // Note: not a coroutine and throws immediately
                        throw TTestException() << "throws immediately";
                    });
                    UNIT_ASSERT_VALUES_EQUAL(index, 0u);
                    UNIT_ASSERT_VALUES_EQUAL(g.Ready(), false);
                    UNIT_ASSERT_VALUES_EQUAL(g.Running(), 1u);
                    UNIT_ASSERT_EXCEPTION_CONTAINS(co_await g.Next(), TTestException, "throws immediately");
                    finishedInnerNormally = true;
                    co_return 42;
                });

                UNIT_ASSERT_VALUES_EQUAL(value, 42);

                finishedNormally = true;
            });

            UNIT_ASSERT(finishedInnerNormally);
            UNIT_ASSERT(finishedNormally);
            UNIT_ASSERT(finished);
        }

        Y_UNIT_TEST(TaskThrowsFromCoroutine) {
            bool finished = false;
            bool finishedNormally = false;
            bool finishedInnerNormally = false;
            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                Y_DEFER { finished = true; };

                int value = co_await WithTaskGroup<int>([&](auto& g) -> async<int> {
                    size_t index = g.Add([&]() -> async<int> {
                        // Note: not a coroutine and throws immediately
                        throw TTestException() << "throws immediately";
                        co_return 42;
                    });
                    UNIT_ASSERT_VALUES_EQUAL(index, 0u);
                    UNIT_ASSERT_VALUES_EQUAL(g.Ready(), false);
                    UNIT_ASSERT_VALUES_EQUAL(g.Running(), 1u);
                    UNIT_ASSERT_EXCEPTION_CONTAINS(co_await g.Next(), TTestException, "throws immediately");
                    finishedInnerNormally = true;
                    co_return 42;
                });

                UNIT_ASSERT_VALUES_EQUAL(value, 42);

                finishedNormally = true;
            });

            UNIT_ASSERT(finishedInnerNormally);
            UNIT_ASSERT(finishedNormally);
            UNIT_ASSERT(finished);
        }

        Y_UNIT_TEST(TaskAwaitedBeforeReturn) {
            bool finished = false;
            bool finishedNormally = false;
            bool groupFinishedNormally = false;
            bool taskFinishedNormally = false;
            std::coroutine_handle<> resume, cancel;
            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                Y_DEFER { finished = true; };

                int value = co_await WithTaskGroup([&](auto& g) -> async<int> {
                    g.Add([&]() -> async<void> {
                        co_await TSuspendAwaiter{ &resume, &cancel };
                        taskFinishedNormally = true;
                    });
                    UNIT_ASSERT(!resume);
                    co_await AsyncYield();
                    UNIT_ASSERT(resume);
                    UNIT_ASSERT(!cancel);
                    groupFinishedNormally = true;
                    co_return 42;
                });

                UNIT_ASSERT_VALUES_EQUAL(value, 42);

                finishedNormally = true;
            });

            // Note: AsyncYield pending on the mailbox, but task must have started
            UNIT_ASSERT(!groupFinishedNormally);
            UNIT_ASSERT(resume);
            UNIT_ASSERT(!cancel);
            actor.Step();

            UNIT_ASSERT(resume);
            UNIT_ASSERT(cancel);
            UNIT_ASSERT(groupFinishedNormally);
            UNIT_ASSERT(!taskFinishedNormally);
            UNIT_ASSERT(!finishedNormally);
            UNIT_ASSERT(!finished);

            actor.ResumeCoroutine(resume);

            UNIT_ASSERT(taskFinishedNormally);
            UNIT_ASSERT(finishedNormally);
            UNIT_ASSERT(finished);
        }

        Y_UNIT_TEST(TaskNotCancelledUntilGroupReturns) {
            bool finished = false;
            bool finishedNormally = false;
            bool groupFinishedNormally = false;
            bool taskFinishedNormally = false;
            std::coroutine_handle<> tresume, tcancel;
            std::coroutine_handle<> gresume, gcancel;
            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                Y_DEFER { finished = true; };

                int value = co_await WithTaskGroup([&](auto& g) -> async<int> {
                    g.Add([&]() -> async<void> {
                        co_await TSuspendAwaiter{ &tresume, &tcancel };
                        taskFinishedNormally = true;
                    });
                    co_await TSuspendAwaiter{ &gresume, &gcancel };
                    groupFinishedNormally = true;
                    co_return 42;
                });

                UNIT_ASSERT_VALUES_EQUAL(value, 42);

                finishedNormally = true;
            });

            UNIT_ASSERT(tresume);
            UNIT_ASSERT(!tcancel);
            UNIT_ASSERT(gresume);
            UNIT_ASSERT(!gcancel);

            actor.Poison();
            UNIT_ASSERT(gcancel);
            UNIT_ASSERT(!tcancel);

            actor.ResumeCoroutine(gresume);
            UNIT_ASSERT(groupFinishedNormally);
            UNIT_ASSERT(!finished);

            actor.ResumeCoroutine(tresume);
            UNIT_ASSERT(taskFinishedNormally);
            UNIT_ASSERT(finishedNormally);
            UNIT_ASSERT(finished);
            UNIT_ASSERT(state.Destroyed);
        }

        Y_UNIT_TEST(TaskCancelAfterGroupCancel) {
            bool finished = false;
            bool finishedNormally = false;
            bool groupFinished = false;
            bool groupFinishedNormally = false;
            bool taskFinished = false;
            bool taskFinishedNormally = false;
            std::coroutine_handle<> tresume, tcancel;
            std::coroutine_handle<> gresume, gcancel;
            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                Y_DEFER { finished = true; };

                int value = co_await WithTaskGroup([&](auto& g) -> async<int> {
                    Y_DEFER { groupFinished = true; };
                    g.Add([&]() -> async<void> {
                        Y_DEFER { taskFinished = true; };
                        co_await TSuspendAwaiter{ &tresume, &tcancel };
                        taskFinishedNormally = true;
                    });
                    co_await TSuspendAwaiter{ &gresume, &gcancel };
                    groupFinishedNormally = true;
                    co_return 42;
                });

                UNIT_ASSERT_VALUES_EQUAL(value, 42);

                finishedNormally = true;
            });

            UNIT_ASSERT(tresume);
            UNIT_ASSERT(!tcancel);
            UNIT_ASSERT(gresume);
            UNIT_ASSERT(!gcancel);

            actor.Poison();
            UNIT_ASSERT(gcancel);
            UNIT_ASSERT(!tcancel);

            actor.ResumeCoroutine(gcancel);
            UNIT_ASSERT(groupFinished);
            UNIT_ASSERT(!groupFinishedNormally);
            UNIT_ASSERT(!finished);

            actor.ResumeCoroutine(tcancel);
            UNIT_ASSERT(taskFinished);
            UNIT_ASSERT(!taskFinishedNormally);
            UNIT_ASSERT(finished);
            UNIT_ASSERT(!finishedNormally);
            UNIT_ASSERT(state.Destroyed);
        }

        Y_UNIT_TEST(TaskAwaitCancelThenRetry) {
            bool finished = false;
            bool finishedNormally = false;
            bool groupFinished = false;
            bool groupFinishedNormally = false;
            bool taskFinished = false;
            bool taskFinishedNormally = false;
            int groupValue = 0;
            int groupState = 0;
            std::coroutine_handle<> tresume, tcancel;
            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                Y_DEFER { finished = true; };

                int value = co_await WithTaskGroup<int>([&](auto& g) -> async<int> {
                    Y_DEFER { groupFinished = true; };
                    g.Add([&]() -> async<int> {
                        Y_DEFER { taskFinished = true; };
                        co_await TSuspendAwaiter{ &tresume, &tcancel };
                        taskFinishedNormally = true;
                        co_return 42;
                    });
                    groupState = 1;
                    try {
                        co_await WithTimeout(TDuration::MilliSeconds(1), [&]() -> async<void> {
                            groupValue = co_await g.Next();
                        });
                    } catch (const TAsyncTimeout&) {}
                    groupState = 2;
                    int value = co_await WithTimeout(TDuration::MilliSeconds(10), [&]() -> async<int> {
                        groupValue = co_await g.Next();
                        co_return groupValue;
                    });
                    groupState = 3;
                    groupFinishedNormally = true;
                    co_return value;
                });

                UNIT_ASSERT_VALUES_EQUAL(value, 42);

                finishedNormally = true;
            });

            UNIT_ASSERT(tresume);
            UNIT_ASSERT(!tcancel);
            UNIT_ASSERT_VALUES_EQUAL(groupState, 1);

            runtime.SimulateSleep(TDuration::MilliSeconds(2));
            UNIT_ASSERT_VALUES_EQUAL(groupState, 2);

            actor.ResumeCoroutine(tresume);
            UNIT_ASSERT(taskFinishedNormally);
            UNIT_ASSERT(groupFinishedNormally);
            UNIT_ASSERT(finishedNormally);
        }

        /**
         * This is a testing infra test
         *
         * It validates we can actually step one event at a time with actor.Step()
         */
        Y_UNIT_TEST(TestSingleStepTesting) {
            bool finished = false;
            bool finishedNormally = false;
            bool groupFinished = false;
            bool groupFinishedNormally = false;
            bool startedTask1 = false;
            bool startedTask2 = false;
            int finishedTasks = 0;
            std::coroutine_handle<> resume1, cancel1;
            std::coroutine_handle<> resume2, cancel2;
            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                Y_DEFER { finished = true; };

                co_await WithTaskGroup([&](auto& g) -> async<void> {
                    Y_DEFER { groupFinished = true; };
                    g.Add([&]() -> async<void> {
                        startedTask1 = true;
                        co_await AsyncYield();
                        co_await TSuspendAwaiter{ &resume1, &cancel1 };
                    });
                    g.Add([&]() -> async<void> {
                        startedTask2 = true;
                        co_await AsyncYield();
                        co_await TSuspendAwaiter{ &resume2, &cancel2 };
                    });
                    while (g) {
                        co_await g.Next();
                        finishedTasks++;
                    }
                    groupFinishedNormally = true;
                });

                finishedNormally = true;
            });

            UNIT_ASSERT(startedTask1);
            UNIT_ASSERT(startedTask2);
            UNIT_ASSERT(!resume1);
            UNIT_ASSERT(!resume2);

            actor.Step();
            UNIT_ASSERT(resume1);
            UNIT_ASSERT(!resume2);

            actor.Step();
            UNIT_ASSERT(resume2);

            actor.ResumeCoroutine(resume1);
            UNIT_ASSERT_VALUES_EQUAL(finishedTasks, 1);

            actor.ResumeCoroutine(resume2);
            UNIT_ASSERT_VALUES_EQUAL(finishedTasks, 2);

            UNIT_ASSERT(groupFinishedNormally);
            UNIT_ASSERT(finishedNormally);
        }

    } // Y_UNIT_TEST_SUITE(TaskGroup)

} // namespace NAsyncTest
