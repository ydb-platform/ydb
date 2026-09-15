#include <library/cpp/testing/unittest/registar.h>
#include "yql_yt_coordinator_ut.h"

namespace NYql::NFmr {

Y_UNIT_TEST_SUITE(BoundaryKeysEndToEndTests) {
    Y_UNIT_TEST(WorkerSendsBoundaryKeysToCoordinator) {
        TFmrTestSetup setup;
        auto coordinator = setup.GetFmrCoordinator();

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

        auto worker = setup.GetFmrWorker(coordinator, 3, jobFunc);
        TString operationId = coordinator->StartOperation(setup.CreateOperationRequest()).GetValueSync().OperationId;

        Sleep(TDuration::Seconds(3));
        auto getOperationResponse = coordinator->GetOperation({operationId}).GetValueSync();

        UNIT_ASSERT_VALUES_EQUAL(getOperationResponse.Status, EOperationStatus::Completed);
        UNIT_ASSERT_VALUES_EQUAL(getOperationResponse.OutputTablesStats.size(), 1);

        auto& outputTableStats = getOperationResponse.OutputTablesStats[0];
        UNIT_ASSERT_VALUES_EQUAL(outputTableStats.Rows, 1000);
        UNIT_ASSERT_VALUES_EQUAL(outputTableStats.DataWeight, 50000);
        UNIT_ASSERT_VALUES_EQUAL(outputTableStats.Chunks, 1);

        worker->Stop();
    }
}

} // namespace NYql::NFmr

