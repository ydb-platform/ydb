#include "task.h"
#include "task_group.h"
#include "await_callback.h"
#include <library/cpp/testing/unittest/registar.h>

using namespace NActors;

Y_UNIT_TEST_SUITE(Task) {

    TTask<void> SimpleReturnVoid() {
        co_return;
    }

    TTask<int> SimpleReturn42() {
        co_return 42;
    }

    Y_UNIT_TEST(SimpleVoidCoroutine) {
        bool finished = false;
        AwaitThenCallback(SimpleReturnVoid(), [&]() {
            finished = true;
        });
        UNIT_ASSERT(finished);
    }

    Y_UNIT_TEST(SimpleIntCoroutine) {
        std::optional<int> result;
        AwaitThenCallback(SimpleReturn42(), [&](int value) {
            result = value;
        });
        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL(*result, 42);
    }

    Y_UNIT_TEST(DoneAndWhenDone) {
        auto task = SimpleReturn42();
        UNIT_ASSERT(task);
        UNIT_ASSERT(!task.Done());

        bool whenDoneFinished = false;
        AwaitThenCallback(task.WhenDone(), [&]() {
            whenDoneFinished = true;
        });
        UNIT_ASSERT(whenDoneFinished);
        UNIT_ASSERT(task.Done());

        // WhenDone can be used even when task is already done
        whenDoneFinished = false;
        AwaitThenCallback(task.WhenDone(), [&]() {
            whenDoneFinished = true;
        });
        UNIT_ASSERT(whenDoneFinished);

        std::optional<int> result;
        AwaitThenCallback(std::move(task), [&](int value) {
            result = value;
        });
        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL(*result, 42);
        UNIT_ASSERT(!task);
    }

    Y_UNIT_TEST(ManualStart) {
        auto task = SimpleReturn42();
        UNIT_ASSERT(task && !task.Done());
        task.Start();
        UNIT_ASSERT(task.Done());
        UNIT_ASSERT_VALUES_EQUAL(task.ExtractResult(), 42);
    }

    template<class TCallback>
    TTask<int> CallTwice(TCallback&& callback) {
        int a = co_await callback();
        int b = co_await callback();
        co_return a + b;
    }

    Y_UNIT_TEST(NestedAwait) {
        auto task = CallTwice([]{
            return SimpleReturn42();
        });
        std::optional<int> result;
        AwaitThenCallback(std::move(task), [&](int value) {
            result = value;
        });
        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL(*result, 84);
    }

    struct TPauseState {
        std::coroutine_handle<> Next;
        int NextResult;

        auto Wait() {
            struct TAwaiter {
                TPauseState* State;

                bool await_ready() const noexcept { return false; }
                int await_resume() const noexcept {
                    return State->NextResult;
                }
                void await_suspend(std::coroutine_handle<> c) {
                    State->Next = c;
                }
            };
            return TAwaiter{ this };
        }

        explicit operator bool() const {
            return bool(Next);
        }

        void Resume(int result) {
            Y_VERIFY(Next && !Next.done());
            NextResult = result;
            std::exchange(Next, {}).resume();
        }
    };

    Y_UNIT_TEST(PausedAwait) {
        TPauseState state;
        auto callback = [&]{
            return state.Wait();
        };
        auto task = CallTwice(callback);
        std::optional<int> result;
        AwaitThenCallback(std::move(task), [&](int value) {
            result = value;
        });
        UNIT_ASSERT(!result);
        UNIT_ASSERT(state);
        state.Resume(11);
        UNIT_ASSERT(!result);
        UNIT_ASSERT(state);
        state.Resume(22);
        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL(*result, 33);
    }

    Y_UNIT_TEST(ManuallyStartThenWhenDone) {
        TPauseState state;
        auto next = [&]{
            return state.Wait();
        };

        auto task = [](auto next) -> TTask<int> {
            int value = co_await next();
            co_return value * 2;
        }(next);

        UNIT_ASSERT(task && !task.Done());
        task.Start();
        UNIT_ASSERT(!task.Done() && state);
        bool finished = false;
        AwaitThenCallback(task.WhenDone(), [&]{
            finished = true;
        });
        UNIT_ASSERT(!finished && !task.Done());
        state.Resume(11);
        UNIT_ASSERT(finished && task.Done());
        UNIT_ASSERT_VALUES_EQUAL(task.ExtractResult(), 22);
    }

    Y_UNIT_TEST(ManuallyStartThenAwait) {
        TPauseState state;
        auto next = [&]{
            return state.Wait();
        };

        auto task = [](auto next) -> TTask<int> {
            int value = co_await next();
            co_return value * 2;
        }(next);

        UNIT_ASSERT(task && !task.Done());
        task.Start();
        UNIT_ASSERT(!task.Done() && state);

        auto awaitTask = [](auto task) -> TTask<int> {
            int value = co_await std::move(task);
            co_return value * 3;
        }(std::move(task));
        UNIT_ASSERT(awaitTask && !awaitTask.Done());
        std::optional<int> result;
        AwaitThenCallback(std::move(awaitTask), [&](int value) {
            result = value;
        });
        UNIT_ASSERT(!result);
        state.Resume(11);
        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL(*result, 66);
    }

    Y_UNIT_TEST(GroupWithTwoSubTasks) {
        TPauseState state1;
        TPauseState state2;

        std::vector<int> results;
        auto task = [](auto& state1, auto& state2, auto& results) -> TTask<int> {
            TTaskGroup<int> group;
            group.AddTask(state1.Wait());
            group.AddTask(state2.Wait());
            int a = co_await group;
            results.push_back(a);
            int b = co_await group;
            results.push_back(b);
            co_return a + b;
        }(state1, state2, results);

        std::optional<int> result;
        AwaitThenCallback(std::move(task), [&](int value) {
            result = value;
        });

        // We must be waiting for both states
        UNIT_ASSERT(state1);
        UNIT_ASSERT(state2);
        state2.Resume(22);
        UNIT_ASSERT_VALUES_EQUAL(results.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(results.at(0), 22);
        UNIT_ASSERT(!result);
        state1.Resume(11);
        UNIT_ASSERT_VALUES_EQUAL(results.size(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(results.at(1), 11);
        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL(*result, 33);
    }

    Y_UNIT_TEST(GroupWithTwoSubTasksDetached) {
        TPauseState state1;
        TPauseState state2;

        std::vector<int> results;
        auto task = [](auto& state1, auto& state2, auto& results) -> TTask<int> {
            TTaskGroup<int> group;
            group.AddTask(state1.Wait());
            group.AddTask(state2.Wait());
            int a = co_await group;
            results.push_back(a);
            co_return a;
        }(state1, state2, results);

        std::optional<int> result;
        AwaitThenCallback(std::move(task), [&](int value) {
            result = value;
        });

        // We must be waiting for both states
        UNIT_ASSERT(state1);
        UNIT_ASSERT(state2);
        state2.Resume(22);
        UNIT_ASSERT_VALUES_EQUAL(results.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(results.at(0), 22);
        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL(*result, 22);

        // We must resume the first state (otherwise memory leaks), but result is ignored
        state1.Resume(11);
    }

} // Y_UNIT_TEST_SUITE(Task)
