#include "test_helpers.h"


#include <ydb/library/workload/tpcc/task_queue.h>
#include <ydb/library/workload/tpcc/terminal.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NYdb::NTPCC;
using namespace NYdb::NTPCC::NTest;

namespace std {
    template<typename T, typename... Args>
    struct coroutine_traits<NYdb::NTPCC::TTask<T>, Args...> {
        using promise_type = typename NYdb::NTPCC::TTask<T>::TPromiseType;
    };

    template<typename... Args>
    struct coroutine_traits<NYdb::NTPCC::TTask<void>, Args...> {
        using promise_type = typename NYdb::NTPCC::TTask<void>::TPromiseType;
    };
} // namespace std

Y_UNIT_TEST_SUITE(TTaskTest) {

    TTestTask MakeInnerTask(int& flag) {
        flag = 1;
        co_return TTestResult{};
    }

    TTerminalTask MakeOuterTask(int& outerState, int& innerState) {
        outerState = 1;
        co_await MakeInnerTask(innerState);
        outerState = 2;
        co_return;
    }

    Y_UNIT_TEST(ShouldAwaitInnerAndResumeOuter) {
        int outerState = 0;
        int innerState = 0;

        auto task = MakeOuterTask(outerState, innerState);
        task.Handle.resume(); // explicitly resume the outer task

        UNIT_ASSERT_VALUES_EQUAL(outerState, 2); // outer reached end
        UNIT_ASSERT_VALUES_EQUAL(innerState, 1); // inner executed
        task.await_resume(); // should not throw
    }

    TTestTask MakeInnerTaskWithException() {
        throw std::runtime_error("Inner failure");
        co_return TTestResult{};
    }

    TTerminalTask MakeOuterThatAwaitsInnerException() {
        co_await MakeInnerTaskWithException();
        co_return;
    }

    Y_UNIT_TEST(ShouldPropagateExceptionFromInnerToOuter) {
        auto task = MakeOuterThatAwaitsInnerException();
        task.Handle.resume(); // start outer (which starts inner)

        UNIT_ASSERT_EXCEPTION_CONTAINS(task.await_resume(), std::runtime_error, "Inner failure");
    }

    TTerminalTask MakeOuterThrows() {
        throw std::runtime_error("Outer failure");
        co_return;
    }

    Y_UNIT_TEST(ShouldPropagateExceptionFromOuter) {
        auto task = MakeOuterThrows();
        task.Handle.resume(); // start outer (throws immediately)

        UNIT_ASSERT_EXCEPTION_CONTAINS(task.await_resume(), std::runtime_error, "Outer failure");
    }

    TTerminalTask MakeDeeplyNested(int& counter) {
        ++counter;
        co_await TTask<TTestResult>{[]() -> TTask<TTestResult> {
            co_return TTestResult{};
        }()};
        ++counter;
        co_return;
    }

    Y_UNIT_TEST(ShouldSupportMultipleNestedTasks) {
        int counter = 0;
        auto task = MakeDeeplyNested(counter);
        task.Handle.resume();

        UNIT_ASSERT_VALUES_EQUAL(counter, 2);
        task.await_resume(); // no exception
    }

    TTerminalTask MakeVoidTask(bool& flag) {
        flag = true;
        co_return;
    }

    Y_UNIT_TEST(ShouldSupportVoidTask) {
        bool ok = false;
        auto task = MakeVoidTask(ok);
        task.Handle.resume();

        UNIT_ASSERT(ok);
        task.await_resume(); // must complete
    }
}
