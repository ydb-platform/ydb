#include "common.h"
#include <ydb/library/actors/async/task_group.h>

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
            Y_UNUSED(actor);

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
            Y_UNUSED(actor);

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
            Y_UNUSED(actor);

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
            Y_UNUSED(actor);

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
            Y_UNUSED(actor);

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
            Y_UNUSED(actor);

            UNIT_ASSERT(finishedInnerNormally);
            UNIT_ASSERT(finishedNormally);
            UNIT_ASSERT(finished);
        }

    } // Y_UNIT_TEST_SUITE(TaskGroup)

} // namespace NAsyncTest
