#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/future/async.h>
#include <util/stream/output.h>
#include <util/string/cast.h>
#include <util/system/mutex.h>
#include <util/thread/pool.h>
#include <yt/yql/providers/yt/fmr/worker/impl/yql_yt_worker_impl.h>
#include <yt/yql/providers/yt/fmr/coordinator/impl/yql_yt_coordinator_impl.h>
#include <yt/yql/providers/yt/fmr/coordinator/yt_coordinator_service//file/yql_yt_file_coordinator_service.h>
#include <yt/yql/providers/yt/fmr/job_factory/impl/yql_yt_job_factory_impl.h>

namespace NYql::NFmr {

TDownloadOperationParams downloadOperationParams{
    .Input = TYtTableRef{"Path","Cluster", "FilePath"},
    .Output = TFmrTableRef{{"Cluster", "Path"}}
};

TStartOperationRequest CreateOperationRequest(ETaskType taskType = ETaskType::Download, TOperationParams operationParams = downloadOperationParams) {
    return TStartOperationRequest{
        .TaskType = taskType,
        .OperationParams = operationParams,
        .IdempotencyKey = "IdempotencyKey",
        .ClusterConnections = {{TFmrTableId("Cluster", "Path"), TClusterConnection{.TransactionId = "transaction_id", .YtServerName = "hahn.yt.yandex.net", .Token = "token"}}}
    };
}

Y_UNIT_TEST_SUITE(FmrWorkerTests) {
    Y_UNIT_TEST(GetSuccessfulOperationResult) {
        auto coordinator = MakeFmrCoordinator(TFmrCoordinatorSettings(), MakeFileYtCoordinatorService());
        auto operationResults = std::make_shared<TString>("no_result_yet");
        auto func = [&] (TTask::TPtr /*task*/, std::shared_ptr<std::atomic<bool>> cancelFlag) {
            while (!cancelFlag->load()) {
                *operationResults = "operation_result";
                return TJobResult{.TaskStatus = ETaskStatus::Completed, .Stats = TStatistics()};
            }
            return TJobResult{.TaskStatus = ETaskStatus::Failed, .Stats = TStatistics()};
        };
        TFmrJobFactorySettings settings{.NumThreads = 3, .Function = func};

        auto factory = MakeFmrJobFactory(settings);
        TFmrWorkerSettings workerSettings{.WorkerId = 0, .RandomProvider = CreateDeterministicRandomProvider(1)};
        auto worker = MakeFmrWorker(coordinator, factory, workerSettings);
        worker->Start();
        coordinator->StartOperation(CreateOperationRequest()).GetValueSync();
        Sleep(TDuration::Seconds(2));
        worker->Stop();
        UNIT_ASSERT_NO_DIFF(*operationResults, "operation_result");
    }

    Y_UNIT_TEST(CancelOperation) {
        auto coordinator = MakeFmrCoordinator(TFmrCoordinatorSettings(), MakeFileYtCoordinatorService());
        auto operationResults = std::make_shared<TString>("no_result_yet");
        auto func = [&] (TTask::TPtr /*task*/, std::shared_ptr<std::atomic<bool>> cancelFlag) {
            int numIterations = 0;
            while (!cancelFlag->load()) {
                Sleep(TDuration::Seconds(1));
                ++numIterations;
                if (numIterations == 100) {
                    *operationResults = "operation_result";
                    return TJobResult{.TaskStatus = ETaskStatus::Completed, .Stats = TStatistics()};
                }
            }
            *operationResults = "operation_cancelled";
            return TJobResult{.TaskStatus = ETaskStatus::Failed, .Stats = TStatistics()};
        };
        TFmrJobFactorySettings settings{.NumThreads =3, .Function=func};
        auto factory = MakeFmrJobFactory(settings);
        TFmrWorkerSettings workerSettings{.WorkerId = 0, .RandomProvider = CreateDeterministicRandomProvider(1)};
        auto worker = MakeFmrWorker(coordinator, factory, workerSettings);
        worker->Start();
        auto operationId = coordinator->StartOperation(CreateOperationRequest()).GetValueSync().OperationId;
        Sleep(TDuration::Seconds(3));
        coordinator->DeleteOperation({operationId}).GetValueSync();
        Sleep(TDuration::Seconds(5));
        worker->Stop();
        UNIT_ASSERT_NO_DIFF(*operationResults, "operation_cancelled");
    }
    Y_UNIT_TEST(CreateSeveralWorkers) {
        TFmrCoordinatorSettings coordinatorSettings{};
        coordinatorSettings.WorkersNum = 2;
        coordinatorSettings.RandomProvider = CreateDeterministicRandomProvider(3);
        auto coordinator = MakeFmrCoordinator(coordinatorSettings, MakeFileYtCoordinatorService());
        std::shared_ptr<std::atomic<ui32>> operationResult = std::make_shared<std::atomic<ui32>>(0);
        auto func = [&] (TTask::TPtr /*task*/, std::shared_ptr<std::atomic<bool>> cancelFlag) {
            while (!cancelFlag->load()) {
                Sleep(TDuration::Seconds(1));
                (*operationResult)++;
                return TJobResult{.TaskStatus = ETaskStatus::Completed, .Stats = TStatistics()};
            }
            return TJobResult{.TaskStatus = ETaskStatus::Failed, .Stats = TStatistics()};
        };
        TFmrJobFactorySettings settings{.NumThreads =3, .Function=func};
        auto factory = MakeFmrJobFactory(settings);
        TFmrWorkerSettings firstWorkerSettings{.WorkerId = 0, .RandomProvider = CreateDeterministicRandomProvider(1)};
        TFmrWorkerSettings secondWorkerSettings{.WorkerId = 1, .RandomProvider = CreateDeterministicRandomProvider(2)};
        auto firstWorker = MakeFmrWorker(coordinator, factory, firstWorkerSettings);
        auto secondWorker = MakeFmrWorker(coordinator, factory, secondWorkerSettings);
        firstWorker->Start();
        secondWorker->Start();
        coordinator->StartOperation(CreateOperationRequest()).GetValueSync();
        Sleep(TDuration::Seconds(3));
        UNIT_ASSERT_VALUES_EQUAL(operationResult->load(), 1);
    }
    Y_UNIT_TEST(SimpleWorkerFailover) {
        TFmrCoordinatorSettings coordinatorSettings{};
        coordinatorSettings.WorkersNum = 2;
        coordinatorSettings.RandomProvider = CreateDeterministicRandomProvider(3);
        auto coordinator = MakeFmrCoordinator(coordinatorSettings, MakeFileYtCoordinatorService());
        std::shared_ptr<std::atomic<ui32>> operationResult = std::make_shared<std::atomic<ui32>>(0);
        // TODO - maybe improve test
        ui64 numIterations = 5;
        auto func = [&] (TTask::TPtr /*task*/, std::shared_ptr<std::atomic<bool>> cancelFlag) {
            ui64 pos = 0;
            while (!cancelFlag->load()) {
                if (pos == numIterations) {
                    return TJobResult{.TaskStatus = ETaskStatus::Completed, .Stats = TStatistics()};
                }
                (*operationResult)++;
                Sleep(TDuration::Seconds(1));
                ++pos;
            }
            *operationResult = 0; // reset counter in case of cancel
            return TJobResult{.TaskStatus = ETaskStatus::Failed, .Stats = TStatistics()};
        };
        TFmrJobFactorySettings settings{.NumThreads = 3, .Function=func};
        TFmrWorkerSettings firstWorkerSettings{.WorkerId = 0, .RandomProvider = CreateDeterministicRandomProvider(1)};
        TFmrWorkerSettings secondWorkerSettings{.WorkerId = 1, .RandomProvider = CreateDeterministicRandomProvider(2)};
        auto firstWorker = MakeFmrWorker(coordinator,  MakeFmrJobFactory(settings), firstWorkerSettings);
        auto secondWorker = MakeFmrWorker(coordinator,  MakeFmrJobFactory(settings), secondWorkerSettings);
        firstWorker->Start();
        coordinator->StartOperation(CreateOperationRequest()).GetValueSync();
        Sleep(TDuration::Seconds(2));
        firstWorker->Stop();
        secondWorker->Start();
        Sleep(TDuration::Seconds(10));
        UNIT_ASSERT_VALUES_EQUAL(operationResult->load(), numIterations);
    }
}

} // namespace NYql::NFmr
