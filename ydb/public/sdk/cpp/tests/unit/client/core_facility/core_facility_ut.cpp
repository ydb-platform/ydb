#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/core_facility/core_facility.h>
#include <ydb/public/sdk/cpp/src/client/types/core_facility/simple_core_facility.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/yexception.h>

#include <atomic>
#include <chrono>
#include <future>
#include <thread>

using namespace NYdb;

Y_UNIT_TEST_SUITE(CoreFacilityTest) {
    Y_UNIT_TEST(UsesSharedRuntimeWithoutInlineExecution) {
        auto facility = CreateSimpleCoreFacility();
        UNIT_ASSERT(facility == CreateSimpleCoreFacility());
        auto completed = std::make_shared<std::promise<std::thread::id>>();
        auto future = completed->get_future();
        facility->PostToResponseQueue([completed] {
            completed->set_value(std::this_thread::get_id());
        });
        facility.reset();
        UNIT_ASSERT(future.wait_for(std::chrono::seconds(10)) == std::future_status::ready);
        UNIT_ASSERT(future.get() != std::this_thread::get_id());
    }

    Y_UNIT_TEST(PeriodicTaskRepeatsAfterExceptionAndStopsOnFalse) {
        struct TState {
            std::promise<unsigned> Completed;
            unsigned Attempts = 0;

            ~TState() {
                Completed.set_value(Attempts);
            }
        };
        auto facility = std::make_unique<TSimpleCoreFacility>();
        auto state = std::make_shared<TState>();
        auto future = state->Completed.get_future();
        facility->AddPeriodicTask([state = std::move(state)](NYdb::NIssue::TIssues&&, EStatus status) {
            UNIT_ASSERT(status == EStatus::SUCCESS);
            if (++state->Attempts == 1) {
                ythrow yexception() << "Retry periodic callback";
            }
            return state->Attempts < 3;
        }, TDeadline::Duration::zero());
        facility.reset();
        UNIT_ASSERT(future.wait_for(std::chrono::seconds(10)) == std::future_status::ready);
        UNIT_ASSERT_VALUES_EQUAL(future.get(), 3u);
    }

    Y_UNIT_TEST(ThrowingTaskDoesNotStopTheSharedRuntime) {
        auto facility = CreateSimpleCoreFacility();
        auto completed = std::make_shared<std::promise<void>>();
        auto future = completed->get_future();
        facility->PostToResponseQueue([] {
            ythrow yexception() << "Failed background callback";
        });
        facility->PostToResponseQueue([completed] {
            completed->set_value();
        });
        UNIT_ASSERT(future.wait_for(std::chrono::seconds(10)) == std::future_status::ready);
    }
}
