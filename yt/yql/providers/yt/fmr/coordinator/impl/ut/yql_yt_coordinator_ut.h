#pragma once

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/threading/future/async.h>
#include <library/cpp/time_provider/time_provider.h>

#include <util/system/tempfile.h>
#include <yt/yql/providers/yt/fmr/coordinator/impl/yql_yt_coordinator_impl.h>
#include <yt/yql/providers/yt/fmr/job_factory/impl/yql_yt_job_factory_impl.h>
#include <yt/yql/providers/yt/fmr/job_preparer/impl/yql_yt_job_preparer_impl.h>
#include <yt/yql/providers/yt/fmr/test_tools/table_data_service/yql_yt_table_data_service_helpers.h>
#include <yt/yql/providers/yt/fmr/test_tools/mock_time_provider/yql_yt_mock_time_provider.h>
#include <yt/yql/providers/yt/fmr/worker/impl/yql_yt_worker_impl.h>
#include <yt/yql/providers/yt/fmr/coordinator/yt_coordinator_service/file/yql_yt_file_coordinator_service.h>

#include <yql/essentials/core/file_storage/proto/file_storage.pb.h>

namespace NYql::NFmr {

struct TFmrTestSetup {

    TFmrTestSetup(ui64 fileStorageNumThreads = 3) {
        TFileStorageConfig fsConfig;
        fsConfig.SetThreads(fileStorageNumThreads);
        FileStorage = WithAsync(CreateFileStorage(fsConfig, {}));
        PortManager = MakeHolder<TPortManager>();
    }

    TStartOperationRequest CreateOperationRequest(
        ETaskType taskType = ETaskType::Download,
        TOperationParams operationParams = TFmrTestSetup::DownloadOperationParams)
    {
        return TStartOperationRequest{
            .TaskType = taskType,
            .OperationParams = operationParams,
            .SessionId = "test-session-id",
            .IdempotencyKey = "IdempotencyKey",
            .ClusterConnections = {
                {TFmrTableId("Cluster", "Path"),
                    TClusterConnection{.TransactionId = "transaction_id",
                    .YtServerName = "hahn.yt.yandex.net",
                    .Token = "token"
                }}
            }
        };
    }

    std::vector<TStartOperationRequest> CreateSeveralOperationRequests(
        ETaskType taskType = ETaskType::Download,
        TOperationParams operationParams = TFmrTestSetup::DownloadOperationParams,
        int numRequests = 10)
    {
        std::vector<TStartOperationRequest> startOperationRequests(numRequests);
        for (int i = 0; i < numRequests; ++i) {
            startOperationRequests[i] = TStartOperationRequest{
                .TaskType = taskType,
                .OperationParams = operationParams,
                .SessionId = "test-session-id",
                .IdempotencyKey = "IdempotencyKey_" + ToString(i),
                .ClusterConnections = {
                    {TFmrTableId("Cluster", "Path"),
                        TClusterConnection{.TransactionId = "transaction_id",
                        .YtServerName = "hahn.yt.yandex.net",
                        .Token = "token"}}
                }
            };
        }
        return startOperationRequests;
    }

    IFmrCoordinator::TPtr GetFmrCoordinator(
        const TFmrCoordinatorSettings& settings = TFmrCoordinatorSettings(),
        const TString& sessionId = TFmrTestSetup::SessionId)
    {
        auto coordinator = MakeFmrCoordinator(settings, MakeFileYtCoordinatorService());
        coordinator->OpenSession({.SessionId = sessionId}).GetValueSync();
        return coordinator;
    }

    IFmrWorker::TPtr GetFmrWorker(
        IFmrCoordinator::TPtr coordinator,
        ui64 numThreads = 3,
        const std::function<TJobResult(TTask::TPtr, std::shared_ptr<std::atomic<bool>>)>& taskFunction = TFmrTestSetup::DefaultTaskFunction,
        const TFmrWorkerSettings& workerSettings = TFmrTestSetup::WorkerSettings,
        bool startWorker = true)
    {

        TFmrJobFactorySettings settings{.NumThreads = numThreads, .Function = taskFunction};
        auto factory = MakeFmrJobFactory(settings);
        SetupTableDataServiceDiscovery(TableDataServiceDiscoveryFile, PortManager->GetPort());
        auto jobPreparer = NFmr::MakeFmrJobPreparer(FileStorage, TableDataServiceDiscoveryFile.GetName());
        auto worker = MakeFmrWorker(coordinator, factory, jobPreparer, workerSettings);
        if (startWorker) {
            worker->Start();
        }
        return worker;
    }


public:
    static inline TDownloadOperationParams DownloadOperationParams = TDownloadOperationParams{
        .Input = TYtTableRef{"Cluster", "Path", "File_path"},
        .Output = TFmrTableRef{{"TestCluster", "TestPath"}}
    };

    static inline TFmrWorkerSettings WorkerSettings = TFmrWorkerSettings{
        .WorkerId = 0,
        .RandomProvider = CreateDeterministicRandomProvider(1)
    };

    static constexpr auto SessionId = "test-session-id";

    static constexpr auto DefaultTaskFunction =  [] (TTask::TPtr /*task*/, std::shared_ptr<std::atomic<bool>> cancelFlag) {
        while (!cancelFlag->load()) {
            Sleep(TDuration::Seconds(4));
            return TJobResult{.TaskStatus = ETaskStatus::Completed, .Stats = TStatistics()};
        }
        return TJobResult{.TaskStatus = ETaskStatus::Failed, .Stats = TStatistics()};
    };

    TFileStoragePtr FileStorage;
    THolder<TPortManager> PortManager;
    TTempFileHandle TableDataServiceDiscoveryFile;
};

} // namespace NYql::NFmr
