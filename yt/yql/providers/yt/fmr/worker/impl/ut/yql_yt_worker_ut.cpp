#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/future/async.h>
#include <util/stream/output.h>
#include <util/string/cast.h>
#include <util/system/mutex.h>
#include <util/thread/pool.h>
#include <yt/yql/providers/yt/fmr/worker/impl/yql_yt_worker_impl.h>
#include <yt/yql/providers/yt/fmr/coordinator/impl/yql_yt_coordinator_impl.h>
#include <yt/yql/providers/yt/fmr/job_factory/impl/yql_yt_job_factory_impl.h>

namespace NYql::NFmr {

TDownloadTaskParams downloadTaskParams{
    .Input = TYtTableRef{"Path","Cluster","TransactionId"},
    .Output = TFmrTableRef{"TableId"}
};

TStartOperationRequest CreateOperationRequest(ETaskType taskType = ETaskType::Download, TTaskParams taskParams = downloadTaskParams) {
    return TStartOperationRequest{.TaskType = taskType, .TaskParams = taskParams, .IdempotencyKey = "IdempotencyKey"};
}

Y_UNIT_TEST_SUITE(FmrWorkerTests) {
    Y_UNIT_TEST(GetSuccessfulOperationResult) {
        auto coordinator = MakeFmrCoordinator();
        auto operationResults = std::make_shared<TString>("no_result_yet");
        auto func = [&] (TTask::TPtr /*task*/, std::shared_ptr<std::atomic<bool>> cancelFlag) {
            while (!cancelFlag->load()) {
                *operationResults = "operation_result";
                return ETaskStatus::Completed;
            }
            return ETaskStatus::Aborted;
        };
        TFmrJobFactorySettings settings{.NumThreads = 3, .Function = func};

        auto factory = MakeFmrJobFactory(settings);
        TFmrWorkerSettings workerSettings{.WorkerId = 1, .RandomProvider = CreateDeterministicRandomProvider(1)};
        auto worker = MakeFmrWorker(coordinator, factory, workerSettings);
        worker->Start();
        coordinator->StartOperation(CreateOperationRequest()).GetValueSync();
        Sleep(TDuration::Seconds(2));
        worker->Stop();
        UNIT_ASSERT_NO_DIFF(*operationResults, "operation_result");
    }

    Y_UNIT_TEST(CancelOperation) {
        auto coordinator = MakeFmrCoordinator();
        auto operationResults = std::make_shared<TString>("no_result_yet");
        auto func = [&] (TTask::TPtr /*task*/, std::shared_ptr<std::atomic<bool>> cancelFlag) {
            int numIterations = 0;
            while (!cancelFlag->load()) {
                Sleep(TDuration::Seconds(1));
                ++numIterations;
                if (numIterations == 100) {
                    *operationResults = "operation_result";
                    return ETaskStatus::Completed;
                }
            }
            *operationResults = "operation_cancelled";
            return ETaskStatus::Aborted;
        };
        TFmrJobFactorySettings settings{.NumThreads =3, .Function=func};
        auto factory = MakeFmrJobFactory(settings);
        TFmrWorkerSettings workerSettings{.WorkerId = 1, .RandomProvider = CreateDeterministicRandomProvider(1)};
        auto worker = MakeFmrWorker(coordinator, factory, workerSettings);
        worker->Start();
        auto operationId = coordinator->StartOperation(CreateOperationRequest()).GetValueSync().OperationId;
        Sleep(TDuration::Seconds(2));
        coordinator->DeleteOperation({operationId}).GetValueSync();
        Sleep(TDuration::Seconds(5));
        worker->Stop();
        UNIT_ASSERT_NO_DIFF(*operationResults, "operation_cancelled");
    }
    Y_UNIT_TEST(CreateSeveralWorkers) {
        TFmrCoordinatorSettings coordinatorSettings{};
        coordinatorSettings.WorkersNum = 2;
        coordinatorSettings.RandomProvider = CreateDeterministicRandomProvider(3);
        auto coordinator = MakeFmrCoordinator(coordinatorSettings);
        std::shared_ptr<std::atomic<ui32>> operationResult = std::make_shared<std::atomic<ui32>>(0);
        auto func = [&] (TTask::TPtr /*task*/, std::shared_ptr<std::atomic<bool>> cancelFlag) {
            while (!cancelFlag->load()) {
                Sleep(TDuration::Seconds(1));
                (*operationResult)++;
                return ETaskStatus::Completed;
            }
            return ETaskStatus::Aborted;
        };
        TFmrJobFactorySettings settings{.NumThreads =3, .Function=func};
        auto factory = MakeFmrJobFactory(settings);
        TFmrWorkerSettings firstWorkerSettings{.WorkerId = 1, .RandomProvider = CreateDeterministicRandomProvider(1)};
        TFmrWorkerSettings secondWorkerSettings{.WorkerId = 2, .RandomProvider = CreateDeterministicRandomProvider(2)};
        auto firstWorker = MakeFmrWorker(coordinator, factory, firstWorkerSettings);
        auto secondWorker = MakeFmrWorker(coordinator, factory, secondWorkerSettings);
        firstWorker->Start();
        secondWorker->Start();
        coordinator->StartOperation(CreateOperationRequest()).GetValueSync();
        Sleep(TDuration::Seconds(3));
        UNIT_ASSERT_VALUES_EQUAL(operationResult->load(), 1);
    }
}

} // namespace NYql::NFmr
