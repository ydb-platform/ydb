#include <library/cpp/testing/unittest/registar.h>
#include <yt/yql/providers/yt/fmr/coordinator/impl/yql_yt_coordinator_impl.h>
#include <yt/yql/providers/yt/fmr/coordinator/yt_coordinator_service/file/yql_yt_file_coordinator_service.h>
#include <yt/yql/providers/yt/fmr/worker/impl/yql_yt_worker_impl.h>
#include <yt/yql/providers/yt/fmr/job_factory/impl/yql_yt_job_factory_impl.h>
#include <yt/yql/providers/yt/fmr/request_options/proto_helpers/yql_yt_request_proto_helpers.h>

namespace NYql::NFmr {

Y_UNIT_TEST_SUITE(BoundaryKeysEndToEndTests) {
    Y_UNIT_TEST(WorkerSendsBoundaryKeysToCoordinator) {
        auto coordinator = MakeFmrCoordinator(TFmrCoordinatorSettings(), MakeFileYtCoordinatorService());
        coordinator->OpenSession({.SessionId = "test-session-id"}).GetValueSync();

        auto jobFunc = [](TTask::TPtr task, std::shared_ptr<std::atomic<bool>> /*cancelFlag*/) {
            auto* downloadTaskParams = std::get_if<TDownloadTaskParams>(&task->TaskParams);
            YQL_ENSURE(downloadTaskParams, "Expected Download task");

            TFmrTableOutputRef outputRef = downloadTaskParams->Output;

            TTableChunkStats tableChunkStats;
            tableChunkStats.PartId = outputRef.PartId;

            TChunkStats chunkStats;
            chunkStats.Rows = 1000;
            chunkStats.DataWeight = 50000;
            chunkStats.SortedChunkStats.IsSorted = true;

            NYT::TNode FirstRowKeys = NYT::TNode::CreateMap();
            FirstRowKeys["key"] = 100;
            FirstRowKeys["subkey"] = "aaa";

            chunkStats.SortedChunkStats.FirstRowKeys = FirstRowKeys;

            tableChunkStats.PartIdChunkStats.push_back(chunkStats);

            TStatistics stats;
            stats.OutputTables[outputRef] = tableChunkStats;

            return TJobResult{
                .TaskStatus = ETaskStatus::Completed,
                .Stats = stats
            };
        };

        TFmrJobFactorySettings settings{.NumThreads = 1, .Function = jobFunc};
        auto factory = MakeFmrJobFactory(settings);

        TFmrWorkerSettings workerSettings{
            .WorkerId = 0,
            .RandomProvider = CreateDeterministicRandomProvider(1)
        };
        auto worker = MakeFmrWorker(coordinator, factory, workerSettings);
        worker->Start();

        // Запускаем операцию
        TDownloadOperationParams operationParams{
            .Input = TYtTableRef("cluster", "//path", "file"),
            .Output = TFmrTableRef{{"cluster", "//path"}}
        };

        TStartOperationRequest request{
            .TaskType = ETaskType::Download,
            .OperationParams = operationParams,
            .SessionId = "test-session-id",
            .IdempotencyKey = "test-key",
            .ClusterConnections = {{
                TFmrTableId("cluster", "//path"),
                TClusterConnection{
                    .TransactionId = "txn",
                    .YtServerName = "server",
                    .Token = "token"
                }
            }}
        };

        auto operationId = coordinator->StartOperation(request).GetValueSync().OperationId;

        // Активно ждём завершения операции
        TGetOperationRequest getRequest{.OperationId = operationId};
        TGetOperationResponse operationResponse;

        for (int i = 0; i < 100; ++i) {
            operationResponse = coordinator->GetOperation(getRequest).GetValueSync();
            if (operationResponse.Status == EOperationStatus::Completed ||
                operationResponse.Status == EOperationStatus::Failed) {
                break;
            }
            Sleep(TDuration::MilliSeconds(100));
        }

        UNIT_ASSERT_VALUES_EQUAL(operationResponse.Status, EOperationStatus::Completed);

        UNIT_ASSERT_VALUES_EQUAL(operationResponse.OutputTablesStats.size(), 1);
        auto& outputTableStats = operationResponse.OutputTablesStats[0];
        UNIT_ASSERT_VALUES_EQUAL(outputTableStats.Rows, 1000);
        UNIT_ASSERT_VALUES_EQUAL(outputTableStats.DataWeight, 50000);
        UNIT_ASSERT_VALUES_EQUAL(outputTableStats.Chunks, 1);

        worker->Stop();
    }
}

} // namespace NYql::NFmr

