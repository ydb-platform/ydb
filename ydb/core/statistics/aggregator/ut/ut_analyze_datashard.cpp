#include <ydb/core/statistics/ut_common/ut_common.h>

#include <ydb/library/actors/testlib/test_runtime.h>

#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/statistics/events.h>
#include <ydb/core/statistics/service/service.h>

namespace NKikimr {
namespace NStat {

namespace {

void PrepareTable(TTestEnv& env, const TString& tableName) {
    CreateUniformTable(env, "Database", tableName);
    InsertDataIntoTable(env, "Database", tableName, 1000);
}

void ValidateCountMinSketch(TTestActorRuntime& runtime, const TPathId& pathId) {
    std::vector<TCountMinSketchProbes> expected = {
        {
            .Tag = 1, // Key column
            .Probes = std::nullopt,
        },
        {
            .Tag = 2, // Value column
            .Probes = { { {"1", 100}, {"2", 100}, {"10", 0} } }
        }
    };

    CheckCountMinSketch(runtime, pathId, expected);
}

} // namespace

Y_UNIT_TEST_SUITE(AnalyzeDatashard) {

    Y_UNIT_TEST(AnalyzeOneTable) {
        TTestEnv env(1, 1);

        auto& runtime = *env.GetServer().GetRuntime();

        CreateDatabase(env, "Database");
        PrepareTable(env, "Table");

        ui64 saTabletId;
        auto pathId = ResolvePathId(runtime, "/Root/Database/Table", nullptr, &saTabletId);

        Analyze(runtime, saTabletId, {{pathId}});

        ValidateCountMinSketch(runtime, pathId);
    }

    Y_UNIT_TEST(AnalyzeTwoTables) {
        TTestEnv env(1, 1);

        auto& runtime = *env.GetServer().GetRuntime();

        CreateDatabase(env, "Database");
        PrepareTable(env, "Table1");
        PrepareTable(env, "Table2");

        ui64 saTabletId1;
        auto pathId1 = ResolvePathId(runtime, "/Root/Database/Table1", nullptr, &saTabletId1);
        auto pathId2 = ResolvePathId(runtime, "/Root/Database/Table2");

        Analyze(runtime, saTabletId1, {pathId1, pathId2});

        ValidateCountMinSketch(runtime, pathId1);
        ValidateCountMinSketch(runtime, pathId2);
    }

    Y_UNIT_TEST(DropTableNavigateError) {
        TTestEnv env(1, 1);

        auto& runtime = *env.GetServer().GetRuntime();

        CreateDatabase(env, "Database");
        PrepareTable(env, "Table");

        ui64 saTabletId = 0;
        auto pathId = ResolvePathId(runtime, "/Root/Database/Table", nullptr, &saTabletId);

        DropTable(env, "Database", "Table");

        auto result = Analyze(
            runtime, saTabletId, {pathId},
            "operationId", {}, NKikimrStat::TEvAnalyzeResponse::STATUS_ERROR);

        NYql::TIssues issues;
        NYql::IssuesFromMessage(result.GetIssues(), issues);
        UNIT_ASSERT_C(issues.ToString().Contains("Could not find table"), issues.ToString());

        std::vector<TCountMinSketchProbes> expected = {
            { .Tag = 1, .Probes = std::nullopt },
            { .Tag = 2, .Probes = std::nullopt },
        };
        CheckCountMinSketch(runtime, pathId, expected);
    }

    Y_UNIT_TEST(TrickyTableAndColumnNames) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");
        ExecuteYqlScript(env, R"(
            CREATE TABLE `Root/Database/test\\Test\`test`(
                key Uint32,
                `val-Val` Uint32,
                PRIMARY KEY (key)
            )
        )");

        ui64 saTabletId = 0;
        auto pathId = ResolvePathId(runtime, R"(/Root/Database/test\Test`test)", nullptr, &saTabletId);
        // Check that ANALYZE is successful
        Analyze(runtime, saTabletId, {pathId}, "operationId");
    }

    Y_UNIT_TEST(DeleteForceTraversalUsesCorrectKey) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");
        PrepareTable(env, "Table1");
        PrepareTable(env, "Table2");

        ui64 saTabletId = 0;
        auto pathId1 = ResolvePathId(runtime, "/Root/Database/Table1", nullptr, &saTabletId);
        auto pathId2 = ResolvePathId(runtime, "/Root/Database/Table2");

        auto sender1 = runtime.AllocateEdgeActor();
        auto sender2 = runtime.AllocateEdgeActor();
        auto sender3 = runtime.AllocateEdgeActor();

        TBlockEvents<TEvStatistics::TEvSaveStatisticsQueryResponse> block(runtime);

        auto req1 = MakeAnalyzeRequest({pathId1}, "op1");
        runtime.SendToPipe(saTabletId, sender1, req1.release());
        runtime.WaitFor("TEvSaveStatisticsQueryResponse", [&]{ return block.size() > 0; });

        // Re-send from different sender triggers delete of the queued operation
        auto req2 = MakeAnalyzeRequest({pathId2}, "op2");
        runtime.SendToPipe(saTabletId, sender2, req2.release());
        auto req3 = MakeAnalyzeRequest({pathId2}, "op2");
        runtime.SendToPipe(saTabletId, sender3, req3.release());

        runtime.SimulateSleep(TDuration::MilliSeconds(10));
        RebootTablet(runtime, saTabletId, sender1);

        block.Unblock();
        block.Stop();

        runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeResponse>(sender3);

        // op1 must still be enqueued after reboot
        AnalyzeStatus(runtime, sender1, saTabletId, "op1",
            NKikimrStat::TEvAnalyzeStatusResponse::STATUS_ENQUEUED);
    }

    Y_UNIT_TEST(AnalyzeRebootSa) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();

        CreateDatabase(env, "Database");
        PrepareTable(env, "Table");

        ui64 saTabletId = 0;
        auto pathId = ResolvePathId(runtime, "/Root/Database/Table", nullptr, &saTabletId);
        auto sender = runtime.AllocateEdgeActor();
        const TString operationId = "operationId";

        // Block near end of analyze to ensure ForceTraversalOperationId is committed to DB.
        TBlockEvents<TEvStatistics::TEvSaveStatisticsQueryResponse> block(runtime);

        auto analyzeRequest = MakeAnalyzeRequest({pathId}, operationId);
        runtime.SendToPipe(saTabletId, sender, analyzeRequest.release());

        runtime.WaitFor("TEvSaveStatisticsQueryResponse", [&]{ return block.size() > 0; });

        // Reboot the SA tablet while the analyze is in flight.
        RebootTablet(runtime, saTabletId, sender);

        // The blocked response now targets the old (dead) actor and will be dropped on unblock.
        block.Unblock();
        block.Stop();

        // After restart, the operation must appear as IN_PROGRESS, not ENQUEUED.
        // Without the fix ForceTraversalOperationId is empty after restart, so the check
        // `ForceTraversalOperationId == operationId` fails and STATUS_ENQUEUED is returned.
        AnalyzeStatus(runtime, sender, saTabletId, operationId,
            NKikimrStat::TEvAnalyzeStatusResponse::STATUS_IN_PROGRESS);

        // Re-send the same request to trigger RequestingActorReattached recovery path.
        auto analyzeRequest2 = MakeAnalyzeRequest({pathId}, operationId);
        runtime.SendToPipe(saTabletId, sender, analyzeRequest2.release());
        runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeResponse>(sender);

        ValidateCountMinSketch(runtime, pathId);
    }
}

} // NStat
} // NKikimr
