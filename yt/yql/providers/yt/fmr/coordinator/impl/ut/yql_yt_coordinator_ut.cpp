#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/future/async.h>
#include <util/stream/output.h>
#include <util/string/cast.h>
#include <util/system/mutex.h>
#include <util/thread/pool.h>
#include <yt/yql/providers/yt/fmr/coordinator/impl/yql_yt_coordinator_impl.h>
#include <yt/yql/providers/yt/fmr/job_factory/impl/yql_yt_job_factory_impl.h>
#include <yt/yql/providers/yt/fmr/worker/impl/yql_yt_worker_impl.h>

namespace NYql::NFmr {

class TFmrWorkerProxy: public IFmrWorker {
public:
    TFmrWorkerProxy(IFmrCoordinator::TPtr coordinator, IFmrJobFactory::TPtr jobFactory, const TFmrWorkerSettings& settings):
        Coordinator_(coordinator), JobFactory_(jobFactory), WorkerSettings_(settings), WorkerCreationNum_(1)
    {
        Worker_ = MakeFmrWorker(Coordinator_, JobFactory_, WorkerSettings_);
    }

    void ResetWorker() {
        ++WorkerCreationNum_;
        WorkerSettings_.RandomProvider = CreateDeterministicRandomProvider(WorkerCreationNum_);

        Stop();
        Worker_ = MakeFmrWorker(Coordinator_, JobFactory_, WorkerSettings_);
        Start();
    }

    void Start() {
        Worker_->Start();
    }

    void Stop() {
        Worker_->Stop();
    }

private:
    IFmrCoordinator::TPtr Coordinator_;
    IFmrJobFactory::TPtr JobFactory_;
    IFmrWorker::TPtr Worker_;
    TFmrWorkerSettings WorkerSettings_;
    int WorkerCreationNum_;
};


TDownloadTaskParams downloadTaskParams{
    .Input = TYtTableRef{"Path","Cluster","TransactionId"},
    .Output = TFmrTableRef{"TableId"}
};

TStartOperationRequest CreateOperationRequest(ETaskType taskType = ETaskType::Download, TTaskParams taskParams = downloadTaskParams) {
    return TStartOperationRequest{.TaskType = taskType, .TaskParams = taskParams, .SessionId = "SessionId", .IdempotencyKey = "IdempotencyKey"};
}

std::vector<TStartOperationRequest> CreateSeveralOperationRequests(
    ETaskType taskType = ETaskType::Download, TTaskParams taskParams = downloadTaskParams, int numRequests = 10)
{
    std::vector<TStartOperationRequest> startOperationRequests(numRequests);
    for (int i = 0; i < numRequests; ++i) {
        startOperationRequests[i] = TStartOperationRequest{
            .TaskType = taskType, .TaskParams = taskParams, .IdempotencyKey = "IdempotencyKey_" + ToString(i)
        };
    }
    return startOperationRequests;
}

auto defaultTaskFunction = [] (TTask::TPtr /*task*/, std::shared_ptr<std::atomic<bool>> cancelFlag) {
    while (!cancelFlag->load()) {
        Sleep(TDuration::Seconds(4));
        return ETaskStatus::Completed;
    }
    return ETaskStatus::Failed;
};

Y_UNIT_TEST_SUITE(FmrCoordinatorTests) {
    Y_UNIT_TEST(StartOperation) {
        auto coordinator = MakeFmrCoordinator();
        auto startOperationResponse = coordinator->StartOperation(CreateOperationRequest()).GetValueSync();
        auto status = startOperationResponse.Status;
        UNIT_ASSERT_VALUES_EQUAL(status, EOperationStatus::Accepted);
    }
    Y_UNIT_TEST(RetryAcceptedOperation) {
        auto coordinator = MakeFmrCoordinator();
        auto downloadRequest = CreateOperationRequest();
        auto firstResponse = coordinator->StartOperation(downloadRequest).GetValueSync();
        auto firstOperationId = firstResponse.OperationId;
        auto sameRequest = coordinator->StartOperation(downloadRequest);
        auto secondResponse = sameRequest.GetValueSync();
        auto secondOperationId = secondResponse.OperationId;
        auto status = secondResponse.Status;
        UNIT_ASSERT_VALUES_EQUAL(status, EOperationStatus::Accepted);
        UNIT_ASSERT_NO_DIFF(firstOperationId, secondOperationId);
    }

    Y_UNIT_TEST(DeleteNonexistentOperation) {
        auto coordinator = MakeFmrCoordinator();
        auto deleteOperationResponse = coordinator->DeleteOperation({"delete_operation_id"}).GetValueSync();
        EOperationStatus status = deleteOperationResponse.Status;
        UNIT_ASSERT_VALUES_EQUAL(status, EOperationStatus::NotFound);
    }
    Y_UNIT_TEST(DeleteOperationBeforeSendToWorker) {
        auto coordinator = MakeFmrCoordinator();
        auto startOperationResponse = coordinator->StartOperation(CreateOperationRequest()).GetValueSync();
        TString operationId = startOperationResponse.OperationId;
        auto deleteOperationResponse = coordinator->DeleteOperation({operationId}).GetValueSync();
        EOperationStatus status = deleteOperationResponse.Status;
        UNIT_ASSERT_VALUES_EQUAL(status, EOperationStatus::Aborted);
    }
    Y_UNIT_TEST(GetNonexistentOperation) {
        auto coordinator = MakeFmrCoordinator();
        auto getOperationResponse = coordinator->GetOperation({"get_operation_id"}).GetValueSync();
        EOperationStatus status = getOperationResponse.Status;
        UNIT_ASSERT_VALUES_EQUAL(status, EOperationStatus::NotFound);
    }
    Y_UNIT_TEST(GetAcceptedOperationStatus) {
        auto coordinator = MakeFmrCoordinator();
        auto startOperationResponse = coordinator->StartOperation(CreateOperationRequest()).GetValueSync();
        TString operationId = startOperationResponse.OperationId;
        auto getOperationResponse = coordinator->GetOperation({operationId}).GetValueSync();
        EOperationStatus status = getOperationResponse.Status;
        UNIT_ASSERT_VALUES_EQUAL(status, EOperationStatus::Accepted);
    }
    Y_UNIT_TEST(GetRunningOperationStatus) {
        auto coordinator = MakeFmrCoordinator();
        auto startOperationResponse = coordinator->StartOperation(CreateOperationRequest()).GetValueSync();
        TString operationId = startOperationResponse.OperationId;

        TFmrJobFactorySettings settings{.NumThreads = 3, .Function = defaultTaskFunction};
        auto factory = MakeFmrJobFactory(settings);
        TFmrWorkerSettings workerSettings{.WorkerId = 0, .RandomProvider = CreateDeterministicRandomProvider(1)};
        auto worker = MakeFmrWorker(coordinator, factory, workerSettings);
        worker->Start();
        Sleep(TDuration::Seconds(1));
        auto getOperationResponse = coordinator->GetOperation({operationId}).GetValueSync();
        EOperationStatus status = getOperationResponse.Status;
        UNIT_ASSERT_VALUES_EQUAL(status, EOperationStatus::InProgress);
    }
    Y_UNIT_TEST(GetCompletedOperationStatuses) {
        auto coordinator = MakeFmrCoordinator();
        auto startOperationRequests = CreateSeveralOperationRequests();
        std::vector<TString> operationIds;
        for (auto& request: startOperationRequests) {
            auto startOperationResponse = coordinator->StartOperation(request).GetValueSync();
            operationIds.emplace_back(startOperationResponse.OperationId);
        }
        TFmrJobFactorySettings settings{.NumThreads = 10, .Function = defaultTaskFunction};
        auto factory = MakeFmrJobFactory(settings);
        TFmrWorkerSettings workerSettings{.WorkerId = 0, .RandomProvider = CreateDeterministicRandomProvider(1)};
        auto worker = MakeFmrWorker(coordinator, factory, workerSettings);
        worker->Start();
        Sleep(TDuration::Seconds(6));
        for (auto& operationId: operationIds) {
            auto getOperationResponse = coordinator->GetOperation({operationId}).GetValueSync();
            EOperationStatus status = getOperationResponse.Status;
            UNIT_ASSERT_VALUES_EQUAL(status, EOperationStatus::Completed);
        }
    }
    Y_UNIT_TEST(GetCompletedAndFailedOperationStatuses) {
        auto coordinator = MakeFmrCoordinator();
        auto downloadOperationRequests = CreateSeveralOperationRequests();
        std::vector<TString> downloadOperationIds;
        for (auto& request: downloadOperationRequests) {
            auto startOperationResponse = coordinator->StartOperation(request).GetValueSync();
            downloadOperationIds.emplace_back(startOperationResponse.OperationId);
        }
        auto uploadOperationRequest = CreateOperationRequest(ETaskType::Upload, TUploadTaskParams{});
        auto uploadOperationResponse = coordinator->StartOperation(uploadOperationRequest).GetValueSync();
        auto uploadOperationId = uploadOperationResponse.OperationId;

        auto func = [&] (TTask::TPtr task, std::shared_ptr<std::atomic<bool>> cancelFlag) {
            while (! cancelFlag->load()) {
                Sleep(TDuration::Seconds(1));
                ETaskStatus taskStatus = std::visit([] (auto&& taskParams) {
                    using T = std::decay_t<decltype(taskParams)>;
                    if constexpr (std::is_same_v<T, TUploadTaskParams>) {
                        return ETaskStatus::Failed;
                    }
                    return ETaskStatus::Completed;
                }, task->TaskParams);
                if (taskStatus == ETaskStatus::Failed) {
                    return taskStatus;
                }
                Sleep(TDuration::Seconds(1));
                return ETaskStatus::Completed;
            }
            return ETaskStatus::Failed;
        };

        TFmrJobFactorySettings settings{.NumThreads = 10, .Function = func};
        auto factory = MakeFmrJobFactory(settings);
        TFmrWorkerSettings workerSettings{.WorkerId = 0, .RandomProvider = CreateDeterministicRandomProvider(1)};
        auto worker = MakeFmrWorker(coordinator, factory, workerSettings);
        worker->Start();
        Sleep(TDuration::Seconds(5));

        for (auto& operationId: downloadOperationIds) {
            auto getDownloadOperationResponse = coordinator->GetOperation({operationId}).GetValueSync();
            EOperationStatus status = getDownloadOperationResponse.Status;
            UNIT_ASSERT_VALUES_EQUAL(status, EOperationStatus::Completed);
        }
        auto getUploadOperationResponse = coordinator->GetOperation({uploadOperationId}).GetValueSync();
        EOperationStatus uploadStatus = getUploadOperationResponse.Status;
        UNIT_ASSERT_VALUES_EQUAL(uploadStatus, EOperationStatus::Failed);
    }
    Y_UNIT_TEST(RetryRunningOperation) {
        auto coordinator = MakeFmrCoordinator();
        auto downloadRequest = CreateOperationRequest();
        auto startOperationResponse = coordinator->StartOperation(downloadRequest).GetValueSync();
        TString firstOperationId = startOperationResponse.OperationId;

        TFmrJobFactorySettings settings{.NumThreads = 3, .Function = defaultTaskFunction};
        auto factory = MakeFmrJobFactory(settings);
        TFmrWorkerSettings workerSettings{.WorkerId = 0, .RandomProvider = CreateDeterministicRandomProvider(1)};
        auto worker = MakeFmrWorker(coordinator, factory, workerSettings);
        worker->Start();

        Sleep(TDuration::Seconds(1));
        auto secondStartOperationResponse = coordinator->StartOperation(downloadRequest).GetValueSync();
        EOperationStatus status = secondStartOperationResponse.Status;
        TString secondOperationId = secondStartOperationResponse.OperationId;
        UNIT_ASSERT_NO_DIFF(firstOperationId, secondOperationId);
        UNIT_ASSERT_VALUES_EQUAL(status, EOperationStatus::InProgress);
    }
    Y_UNIT_TEST(RetryRunningOperationAfterIdempotencyKeyClear) {
        TFmrCoordinatorSettings coordinatorSettings{
            .WorkersNum = 1, .RandomProvider = CreateDeterministicRandomProvider(2), .IdempotencyKeyStoreTime = TDuration::Seconds(1)};
        auto coordinator = MakeFmrCoordinator(coordinatorSettings);

        TFmrJobFactorySettings settings{.NumThreads = 3, .Function = defaultTaskFunction};
        auto factory = MakeFmrJobFactory(settings);
        TFmrWorkerSettings workerSettings{.WorkerId = 0, .RandomProvider = CreateDeterministicRandomProvider(1)};
        auto worker = MakeFmrWorker(coordinator, factory, workerSettings);
        worker->Start();

        auto downloadRequest = CreateOperationRequest();
        auto startOperationResponse = coordinator->StartOperation(downloadRequest).GetValueSync();
        TString firstOperationId = startOperationResponse.OperationId;

        Sleep(TDuration::Seconds(2));
        auto secondStartOperationResponse = coordinator->StartOperation(downloadRequest).GetValueSync();
        EOperationStatus secondOperationStatus = secondStartOperationResponse.Status;
        TString secondOperationId = secondStartOperationResponse.OperationId;
        auto getFirstOperationResponse = coordinator->GetOperation({firstOperationId}).GetValueSync();
        EOperationStatus firstOperationStatus = getFirstOperationResponse.Status;

        UNIT_ASSERT_VALUES_UNEQUAL(firstOperationId, secondOperationId);
        UNIT_ASSERT_VALUES_EQUAL(firstOperationStatus, EOperationStatus::InProgress);
        UNIT_ASSERT_VALUES_EQUAL(secondOperationStatus, EOperationStatus::Accepted);
    }
    Y_UNIT_TEST(CancelTasksAfterVolatileIdReload) {
        auto coordinator = MakeFmrCoordinator();
        auto func = [&] (TTask::TPtr /*task*/, std::shared_ptr<std::atomic<bool>> cancelFlag) {
            int numIterations = 0;
            while (!cancelFlag->load()) {
                Sleep(TDuration::Seconds(1));
                ++numIterations;
                if (numIterations == 100) {
                    return ETaskStatus::Completed;
                }
            }
            return ETaskStatus::Failed;
        };
        TFmrJobFactorySettings settings{.NumThreads =3, .Function=func};
        auto factory = MakeFmrJobFactory(settings);
        TFmrWorkerSettings workerSettings{.WorkerId = 0, .RandomProvider = CreateDeterministicRandomProvider(1)};
        TFmrWorkerProxy workerProxy(coordinator, factory, workerSettings);

        workerProxy.Start();
        auto operationId = coordinator->StartOperation(CreateOperationRequest()).GetValueSync().OperationId;
        Sleep(TDuration::Seconds(2));
        workerProxy.ResetWorker();
        Sleep(TDuration::Seconds(5));
        workerProxy.Stop();
        auto getOperationResult = coordinator->GetOperation({operationId}).GetValueSync();
        auto getOperationStatus = getOperationResult.Status;
        UNIT_ASSERT_VALUES_EQUAL(getOperationStatus, EOperationStatus::Failed);
        auto error = getOperationResult.ErrorMessages[0];
        UNIT_ASSERT_VALUES_EQUAL(error.Component, EFmrComponent::Coordinator);
        UNIT_ASSERT_NO_DIFF(error.ErrorMessage, "Max retries limit exceeded");
        UNIT_ASSERT_NO_DIFF(*error.OperationId, operationId);
    }
    Y_UNIT_TEST(HandleJobErrors) {
        auto coordinator = MakeFmrCoordinator();
        auto startOperationResponse = coordinator->StartOperation(CreateOperationRequest()).GetValueSync();
        TString operationId = startOperationResponse.OperationId;

        auto func = [&] (TTask::TPtr /*task*/, std::shared_ptr<std::atomic<bool>> cancelFlag) {
            while (! cancelFlag->load()) {
                Sleep(TDuration::Seconds(2));
                throw std::runtime_error{"Function crashed"};
            }
            return ETaskStatus::Failed;
        };

        TFmrJobFactorySettings settings{.NumThreads = 3, .Function = func};
        auto factory = MakeFmrJobFactory(settings);
        TFmrWorkerSettings workerSettings{.WorkerId = 0, .RandomProvider = CreateDeterministicRandomProvider(1)};
        auto worker = MakeFmrWorker(coordinator, factory, workerSettings);
        worker->Start();
        Sleep(TDuration::Seconds(4));
        auto getOperationResponse = coordinator->GetOperation({operationId}).GetValueSync();

        EOperationStatus status = getOperationResponse.Status;
        std::vector<TFmrError> errorMessages = getOperationResponse.ErrorMessages;
        UNIT_ASSERT_VALUES_EQUAL(status, EOperationStatus::Failed);
        UNIT_ASSERT(errorMessages.size() == 1);
        auto& error = errorMessages[0];
        UNIT_ASSERT_NO_DIFF(error.ErrorMessage, "Function crashed");
        UNIT_ASSERT_VALUES_EQUAL(error.Component, EFmrComponent::Job);
    }
}

} // namespace NYql::NFmr
