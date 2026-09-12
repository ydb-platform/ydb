#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/library/runtime/runtime.h>

#include <library/cpp/testing/unittest/registar.h>


#include <chrono>
#include <future>
#include <thread>

using namespace NYdb;

Y_UNIT_TEST_SUITE(RuntimeTests) {
    Y_UNIT_TEST(ScheduledTaskRunsAfterDeadlineOnAnotherThread) {
        auto done = std::make_shared<std::promise<std::pair<bool, bool>>>();
        auto completed = done->get_future();
        const auto submittingThread = std::this_thread::get_id();
        const auto deadline = TDeadline::AfterDuration(std::chrono::milliseconds(20));
        GetRuntime().Schedule(deadline, [done, deadline, submittingThread] {
            done->set_value({
                deadline <= TDeadline::Now(),
                std::this_thread::get_id() != submittingThread});
        });
        UNIT_ASSERT(completed.wait_for(std::chrono::seconds(5)) == std::future_status::ready);
        const auto [afterDeadline, otherThread] = completed.get();
        UNIT_ASSERT(afterDeadline);
        UNIT_ASSERT(otherThread);
    }

}
