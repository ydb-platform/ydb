#include <ydb/core/statistics/ut_common/ut_common.h>

#include <ydb/library/actors/testlib/test_runtime.h>

#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <ydb/core/testlib/actors/block_events.h>

namespace NKikimr {
namespace NStat {

namespace {

TTableInfo PrepareDatabaseAndTable(TTestEnv& env) {
    CreateDatabase(env, "Database");
    return PrepareColumnTable(env, "Database", "Table", 1);
}

} // namespace

Y_UNIT_TEST_SUITE(AnalyzeColumnshard) {
    Y_UNIT_TEST(AnalyzeShard) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        const auto tableInfo = PrepareDatabaseAndTable(env);

        AnalyzeShard(runtime, tableInfo.ShardIds[0], tableInfo.PathId);
    }

    Y_UNIT_TEST(Analyze) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        const auto tableInfo = PrepareDatabaseAndTable(env);

        Analyze(runtime, tableInfo.SaTabletId, {tableInfo.PathId});
    }

    Y_UNIT_TEST(AnalyzeEmptyTable) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");
        const auto tableInfo = CreateColumnTable(env, "Database", "Table", 4);

        Analyze(runtime, tableInfo.SaTabletId, {tableInfo.PathId});
    }

    Y_UNIT_TEST(AnalyzeServerless) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Shared", 1, true);
        CreateServerlessDatabase(env, "Database", "/Root/Shared");
        auto tableInfo = PrepareColumnTable(env, "Database", "Table", 1);

        Analyze(runtime, tableInfo.SaTabletId, {tableInfo.PathId}, "operationId", "/Root/Database");
    }

    Y_UNIT_TEST(AnalyzeAnalyzeOneColumnTableSpecificColumns) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        const auto tableInfo = PrepareDatabaseAndTable(env);

        Analyze(runtime, tableInfo.SaTabletId, {{tableInfo.PathId, {1, 2}}});
    }

    Y_UNIT_TEST(AnalyzeTwoColumnTables) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();

        CreateDatabase(env, "Database");
        const auto table1 = PrepareColumnTable(env, "Database", "Table1", 1);
        const auto table2 = PrepareColumnTable(env, "Database", "Table2", 1);

        Analyze(runtime, table1.SaTabletId, {table1.PathId, table2.PathId});
    }

    Y_UNIT_TEST(AnalyzeStatus) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        TBlockEvents<TEvStatistics::TEvSaveStatisticsQueryResponse> block(runtime);
        const auto tableInfo = PrepareDatabaseAndTable(env);

        const TString operationId = "operationId";
        AnalyzeStatus(runtime, sender, tableInfo.SaTabletId, operationId, NKikimrStat::TEvAnalyzeStatusResponse::STATUS_NO_OPERATION);

        auto analyzeRequest = MakeAnalyzeRequest({{tableInfo.PathId, {1, 2}}}, operationId);
        runtime.SendToPipe(tableInfo.SaTabletId, sender, analyzeRequest.release());

        runtime.WaitFor("TEvSaveStatisticsQueryResponse", [&]{ return block.size(); });

        AnalyzeStatus(runtime, sender, tableInfo.SaTabletId, operationId, NKikimrStat::TEvAnalyzeStatusResponse::STATUS_IN_PROGRESS);

        // Check EvRemoteHttpInfo
        {
            auto httpRequest = std::make_unique<NActors::NMon::TEvRemoteHttpInfo>("/app?");
            runtime.SendToPipe(tableInfo.SaTabletId, sender, httpRequest.release(), 0, {});
            auto httpResponse = runtime.GrabEdgeEventRethrow<NActors::NMon::TEvRemoteHttpInfoRes>(sender);
            TString body = httpResponse->Get()->Html;
            Cerr << body << Endl;
            UNIT_ASSERT(body.size() > 500);
            UNIT_ASSERT(body.Contains("ForceTraversals: 1"));
        }

        block.Unblock();
        block.Stop();

        auto analyzeResonse = runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeResponse>(sender);
        UNIT_ASSERT_VALUES_EQUAL(analyzeResonse->Get()->Record.GetOperationId(), operationId);

        AnalyzeStatus(runtime, sender, tableInfo.SaTabletId, operationId, NKikimrStat::TEvAnalyzeStatusResponse::STATUS_NO_OPERATION);
    }    

    Y_UNIT_TEST(AnalyzeSameOperationId) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        const auto tableInfo = PrepareDatabaseAndTable(env);
        auto sender = runtime.AllocateEdgeActor();
        const TString operationId = "operationId";

        TBlockEvents<TEvStatistics::TEvSaveStatisticsQueryResponse> block(runtime);

        auto tabletPipe = runtime.ConnectToPipe(tableInfo.SaTabletId, sender, 0, {});

        auto analyzeRequest1 = MakeAnalyzeRequest({tableInfo.PathId}, operationId);
        runtime.SendToPipe(tabletPipe, sender, analyzeRequest1.release());

        runtime.WaitFor("TEvSaveStatisticsQueryResponse", [&]{ return block.size(); });

        auto analyzeRequest2 = MakeAnalyzeRequest({tableInfo.PathId}, operationId);
        runtime.SendToPipe(tabletPipe, sender, analyzeRequest2.release());

        block.Unblock();
        block.Stop();
        
        auto response1 = runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeResponse>(sender);
        UNIT_ASSERT(response1);
        UNIT_ASSERT_VALUES_EQUAL(response1->Get()->Record.GetOperationId(), operationId);

        auto response2 = runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeResponse>(sender, TDuration::Seconds(5));
        UNIT_ASSERT(!response2);
    }

    Y_UNIT_TEST(AnalyzeMultiOperationId) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        const auto tableInfo = PrepareDatabaseAndTable(env);
        auto sender = runtime.AllocateEdgeActor();

        auto GetOperationId = [] (size_t i) { return TStringBuilder() << "operationId" << i; };

        TBlockEvents<TEvStatistics::TEvSaveStatisticsQueryResponse> block(runtime);

        const size_t numEvents = 10;

        auto tabletPipe = runtime.ConnectToPipe(tableInfo.SaTabletId, sender, 0, {});

        for (size_t i = 0; i < numEvents; ++i) {
            auto analyzeRequest = MakeAnalyzeRequest({tableInfo.PathId}, GetOperationId(i));
            runtime.SendToPipe(tabletPipe, sender, analyzeRequest.release());
        }

        block.Unblock();
        block.Stop();

        for (size_t i = 0; i < numEvents; ++i) {
            auto response = runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeResponse>(sender);
            UNIT_ASSERT(response);
            UNIT_ASSERT_VALUES_EQUAL(response->Get()->Record.GetOperationId(), GetOperationId(i));
        }        
    }    

    Y_UNIT_TEST(AnalyzeRebootSa) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        const auto tableInfo = PrepareDatabaseAndTable(env);
        auto sender = runtime.AllocateEdgeActor();

        size_t finalResultsCount = 0;
        auto observer = runtime.AddObserver<TEvStatistics::TEvAnalyzeActorResult>([&](auto& ev) {
            if (ev->Get()->Final) {
                ++finalResultsCount;
            }
        });

        TBlockEvents<TEvDataShard::TEvKqpScan> block(runtime);

        auto analyzeRequest1 = MakeAnalyzeRequest({tableInfo.PathId});
        runtime.SendToPipe(tableInfo.SaTabletId, sender, analyzeRequest1.release());

        runtime.WaitFor("TEvKqpScan", [&]{ return !block.empty(); });
        RebootTablet(runtime, tableInfo.SaTabletId, sender);
        block.Unblock();
        block.Stop();

        // Make sure that new operations can be performed
        auto analyzeRequest2 = MakeAnalyzeRequest({tableInfo.PathId}, "operationId2");
        auto sender2 = runtime.AllocateEdgeActor();
        runtime.SendToPipe(tableInfo.SaTabletId, sender2, analyzeRequest2.release());
        runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeResponse>(sender2);

        // Make sure that the old operation is performed after the reattach request
        auto analyzeRequest3 = MakeAnalyzeRequest({tableInfo.PathId}, "operationId");
        runtime.SendToPipe(tableInfo.SaTabletId, sender, analyzeRequest3.release());
        runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeResponse>(sender);

        // Check that AnalyzeActor on the initial tablet instance got cancelled and
        // only 2 AnalyzeActors successfully finished.
        UNIT_ASSERT_VALUES_EQUAL(finalResultsCount, 2);
    }


    Y_UNIT_TEST(AnalyzeRebootColumnShard) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        const auto tableInfo = PrepareDatabaseAndTable(env);
        auto sender = runtime.AllocateEdgeActor();

        TBlockEvents<TEvDataShard::TEvKqpScan> block(runtime);

        auto analyzeRequest = MakeAnalyzeRequest({tableInfo.PathId});
        runtime.SendToPipe(tableInfo.SaTabletId, sender, analyzeRequest.release());

        runtime.WaitFor("TEvKqpScan", [&]{ return !block.empty(); });
        RebootTablet(runtime, tableInfo.ShardIds[0], sender);
        block.Unblock();
        block.Stop();

        runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeResponse>(sender);
    }

    Y_UNIT_TEST(AnalyzeDeadline) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        const auto tableInfo = PrepareDatabaseAndTable(env);
        auto sender = runtime.AllocateEdgeActor();

        TBlockEvents<TEvStatistics::TEvSaveStatisticsQueryResponse> block(runtime);

        auto analyzeRequest = MakeAnalyzeRequest({tableInfo.PathId});
        runtime.SendToPipe(tableInfo.SaTabletId, sender, analyzeRequest.release());

        runtime.WaitFor("TEvSaveStatisticsQueryResponse", [&]{ return block.size(); });
        runtime.AdvanceCurrentTime(TDuration::Days(2));

        auto analyzeResponse = runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeResponse>(sender);
        const auto& record = analyzeResponse->Get()->Record;
        UNIT_ASSERT_VALUES_EQUAL(record.GetOperationId(), "operationId");
        UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), NKikimrStat::TEvAnalyzeResponse::STATUS_ERROR);
        UNIT_ASSERT(!record.GetIssues().empty());
    }    

    Y_UNIT_TEST(AnalyzeCancel) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        const auto tableInfo = PrepareDatabaseAndTable(env);
        auto sender = runtime.AllocateEdgeActor();

        size_t finalResultsCount = 0;
        auto observer = runtime.AddObserver<TEvStatistics::TEvAnalyzeActorResult>([&](auto& ev) {
            if (ev->Get()->Final) {
                ++finalResultsCount;
            }
        });

        TBlockEvents<TEvDataShard::TEvKqpScan> block(runtime);

        auto analyzeRequest = MakeAnalyzeRequest({tableInfo.PathId});
        auto operationId = analyzeRequest->Record.GetOperationId();
        runtime.SendToPipe(tableInfo.SaTabletId, sender, analyzeRequest.release());

        runtime.WaitFor("TEvKqpScan", [&]{ return !block.empty(); });

        auto cancelRequest = MakeHolder<TEvStatistics::TEvAnalyzeCancel>();
        cancelRequest->Record.SetOperationId(operationId);
        runtime.SendToPipe(tableInfo.SaTabletId, sender, cancelRequest.Release());

        auto analyzeResponse = runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeResponse>(sender);
        const auto& record = analyzeResponse->Get()->Record;
        UNIT_ASSERT_VALUES_EQUAL(record.GetOperationId(), "operationId");
        UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), NKikimrStat::TEvAnalyzeResponse::STATUS_CANCELLED);
        block.Unblock();
        block.Stop();

        // Do another ANALYZE
        auto analyzeRequest2 = MakeAnalyzeRequest({tableInfo.PathId}, "operationId2");
        runtime.SendToPipe(tableInfo.SaTabletId, sender, analyzeRequest2.release());
        runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeResponse>(sender);

        // Make sure that only 1 AnalyzeActor successfully finished.
        UNIT_ASSERT_VALUES_EQUAL(finalResultsCount, 1);
    }
}

} // NStat
} // NKikimr
