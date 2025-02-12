#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/future/async.h>
#include <util/stream/output.h>
#include <util/string/cast.h>
#include <util/system/mutex.h>
#include <util/thread/pool.h>
#include <yt/yql/providers/yt/fmr/job_factory/impl/yql_yt_job_factory_impl.h>
#include <yt/yql/providers/yt/fmr/coordinator/interface/yql_yt_coordinator.h>

namespace NYql {

Y_UNIT_TEST_SUITE(FmrFactoryTests) {
    Y_UNIT_TEST(GetSuccessfulTaskResult) {
        auto operationResults = std::make_shared<TString>("no_result_yet");
        auto func = [&] (TTask::TPtr /*task*/, std::shared_ptr<std::atomic<bool>> cancelFlag) {
            while (! cancelFlag->load()) {
                Sleep(TDuration::Seconds(1));
                *operationResults = "operation_result";
                return ETaskStatus::Completed;
            }
            return ETaskStatus::Aborted;
        };
        TFmrJobFactorySettings settings{.NumThreads =3, .Function=func};
        auto factory = MakeFmrJobFactory(settings);
        auto cancelFlag = std::make_shared<std::atomic<bool>>(false);

        auto futureTaskStatus = factory->StartJob(nullptr, cancelFlag);
        auto taskResult = futureTaskStatus.GetValueSync();
        ETaskStatus taskStatus = taskResult->TaskStatus;

        UNIT_ASSERT_VALUES_EQUAL(taskStatus, ETaskStatus::Completed);
        UNIT_ASSERT_NO_DIFF(*operationResults, "operation_result");
    }
    Y_UNIT_TEST(CancelTask) {
        auto operationResults = std::make_shared<TString>("no_result_yet");
        auto func = [&] (TTask::TPtr /*task*/, std::shared_ptr<std::atomic<bool>> cancelFlag) {
            int numIterations = 0;
            *operationResults = "computing_result";
            while (! cancelFlag->load()) {
                Sleep(TDuration::Seconds(1));
                ++numIterations;
                if (numIterations == 100) {
                    *operationResults = "operation_result";
                    return ETaskStatus::Completed;
                }
            }
            return ETaskStatus::Aborted;
        };
        TFmrJobFactorySettings settings{.NumThreads =3, .Function=func};

        auto factory = MakeFmrJobFactory(settings);
        auto cancelFlag = std::make_shared<std::atomic<bool>>(false);
        auto futureTaskStatus = factory->StartJob(
            nullptr, cancelFlag);
        Sleep(TDuration::Seconds(2));
        cancelFlag->store(true);
        auto taskResult = futureTaskStatus.GetValueSync();
        ETaskStatus taskStatus = taskResult->TaskStatus;
        UNIT_ASSERT_VALUES_EQUAL(taskStatus, ETaskStatus::Aborted);
        UNIT_ASSERT_NO_DIFF(*operationResults, "computing_result");
    }
}

} // namspace NYql
