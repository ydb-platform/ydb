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

    Y_UNIT_TEST(SimpleVoidWhenDone) {
        std::optional<TTaskResult<void>> result;
        AwaitThenCallback(SimpleReturnVoid().WhenDone(), [&](auto value) {
            result = std::move(value);
        });
        UNIT_ASSERT(result);
        result->Value();
    }

    Y_UNIT_TEST(SimpleIntWhenDone) {
        std::optional<TTaskResult<int>> result;
        AwaitThenCallback(SimpleReturn42().WhenDone(), [&](auto value) {
            result = std::move(value);
        });
        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL(result->Value(), 42);
    }

    template<class TCallback>
    TTask<int> CallTwice(TCallback callback) {
        int a = co_await callback();
        int b = co_await callback();
        co_return a + b;
    }

    Y_UNIT_TEST(NestedAwait) {
        auto task = CallTwice([]{
            return SimpleReturn42();
        });
        UNIT_ASSERT(task);
        std::optional<int> result;
        AwaitThenCallback(std::move(task), [&](int value) {
            result = value;
        });
        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL(*result, 84);
    }

    template<class T>
    struct TPauseState {
        std::coroutine_handle<> Next;
        std::optional<T> NextResult;

        ~TPauseState() {
            while (Next) {
                NextResult.reset();
                std::exchange(Next, {}).resume();
            }
        }

        struct TAwaiter {
            TPauseState* State;

            bool await_ready() const noexcept { return false; }
            void await_suspend(std::coroutine_handle<> c) const noexcept {
                State->Next = c;
            }
            T await_resume() const {
                if (!State->NextResult) {
                    throw TTaskCancelled();
                } else {
                    T result = std::move(*State->NextResult);
                    State->NextResult.reset();
                    return result;
                }
            }
        };

        auto Wait() {
            return TAwaiter{ this };
        }

        explicit operator bool() const {
            return bool(Next);
        }

        void Resume(T result) {
            Y_ABORT_UNLESS(Next && !Next.done());
            NextResult = result;
            std::exchange(Next, {}).resume();
        }

        void Cancel() {
            Y_ABORT_UNLESS(Next && !Next.done());
            NextResult.reset();
            std::exchange(Next, {}).resume();
        }
    };

    Y_UNIT_TEST(PauseResume) {
        TPauseState<int> state;
        auto task = CallTwice([&]{
            return state.Wait();
        });
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

    Y_UNIT_TEST(PauseCancel) {
        TPauseState<int> state;
        auto task = CallTwice([&]{
            return state.Wait();
        });
        std::optional<int> result;
        AwaitThenCallback(std::move(task).WhenDone(), [&](TTaskResult<int>&& value) {
            try {
                result = value.Value();
            } catch (TTaskCancelled&) {
                // nothing
            }
        });
        UNIT_ASSERT(!result);
        UNIT_ASSERT(state);
        state.Resume(11);
        UNIT_ASSERT(!result);
        UNIT_ASSERT(state);
        state.Cancel();
        UNIT_ASSERT(!result);
    }

    Y_UNIT_TEST(GroupWithTwoSubTasks) {
        TPauseState<int> state1;
        TPauseState<int> state2;

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
        TPauseState<int> state1;
        TPauseState<int> state2;

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
    }

    Y_UNIT_TEST(GroupWithTwoSubTasksOneCancelled) {
        TPauseState<int> state1;
        TPauseState<int> state2;
        std::vector<int> results;
        auto task = [](auto& state1, auto& state2, auto& results) -> TTask<void> {
            TTaskGroup<int> group;
            group.AddTask(state1.Wait());
            group.AddTask(state2.Wait());
            for (int i = 0; i < 2; ++i) {
                try {
                    results.push_back(co_await group);
                } catch (TTaskCancelled&) {
                    results.push_back(-1);
                }
            }
        }(state1, state2, results);

        bool finished = false;
        AwaitThenCallback(std::move(task), [&]() {
            finished = true;
        });

        UNIT_ASSERT(state1);
        UNIT_ASSERT(state2);
        state2.Cancel();
        UNIT_ASSERT_VALUES_EQUAL(results.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(results.at(0), -1);
        UNIT_ASSERT(!finished);
        state1.Resume(11);
        UNIT_ASSERT_VALUES_EQUAL(results.size(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(results.at(1), 11);
        UNIT_ASSERT(finished);
    }

} // Y_UNIT_TEST_SUITE(Task)
