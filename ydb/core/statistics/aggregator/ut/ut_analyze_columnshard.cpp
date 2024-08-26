#include <ydb/core/statistics/ut_common/ut_common.h>

#include <ydb/library/actors/testlib/test_runtime.h>

#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <thread>

namespace NKikimr {
namespace NStat {



Y_UNIT_TEST_SUITE(AnalyzeColumnshard) {
    Y_UNIT_TEST(AnalyzeOneColumnTable) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        auto tableInfo = CreateDatabaseColumnTables(env, 1, 1)[0];

        AnalyzeTable(runtime, tableInfo.ShardIds[0], tableInfo.PathId);

        Analyze(runtime, tableInfo.SaTabletId, {tableInfo.PathId});
    }

    Y_UNIT_TEST(AnalyzeAnalyzeOneColumnTableSpecificColumns) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        auto tableInfo = CreateDatabaseColumnTables(env, 1, 1)[0];

        Analyze(runtime, tableInfo.SaTabletId, {{tableInfo.PathId, {1, 2}}});
    }

    Y_UNIT_TEST(AnalyzeTwoColumnTables) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        auto tableInfos = CreateDatabaseColumnTables(env, 2, 1);

        Analyze(runtime, tableInfos[0].SaTabletId, {tableInfos[0].PathId, tableInfos[1].PathId});
    }

    Y_UNIT_TEST(AnalyzeStatus) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        auto schemeShardStatsBlocker = runtime.AddObserver<TEvStatistics::TEvSchemeShardStats>([&](auto& ev) {
            ev.Reset();
        });

        auto tableInfo = CreateDatabaseColumnTables(env, 1, 1)[0];

        const TString operationId = "operationId";

        AnalyzeStatus(runtime, sender, tableInfo.SaTabletId, operationId, NKikimrStat::TEvAnalyzeStatusResponse::STATUS_NO_OPERATION);

        auto analyzeRequest = MakeAnalyzeRequest({{tableInfo.PathId, {1, 2}}}, operationId);
        runtime.SendToPipe(tableInfo.SaTabletId, sender, analyzeRequest.release());

        AnalyzeStatus(runtime, sender, tableInfo.SaTabletId, operationId, NKikimrStat::TEvAnalyzeStatusResponse::STATUS_ENQUEUED);

        schemeShardStatsBlocker.Remove();

        auto analyzeResonse = runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeResponse>(sender);
        UNIT_ASSERT_VALUES_EQUAL(analyzeResonse->Get()->Record.GetOperationId(), operationId);

        AnalyzeStatus(runtime, sender, tableInfo.SaTabletId, operationId, NKikimrStat::TEvAnalyzeStatusResponse::STATUS_NO_OPERATION);
    }    

    Y_UNIT_TEST(AnalyzeSameOperationId) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        auto tableInfo = CreateDatabaseColumnTables(env, 1, 1)[0];
        auto sender = runtime.AllocateEdgeActor();
        const TString operationId = "operationId";

        auto analyzeRequest1 = MakeAnalyzeRequest({tableInfo.PathId}, operationId);
        runtime.SendToPipe(tableInfo.SaTabletId, sender, analyzeRequest1.release());

        auto analyzeRequest2 = MakeAnalyzeRequest({tableInfo.PathId}, operationId);
        runtime.SendToPipe(tableInfo.SaTabletId, sender, analyzeRequest2.release());

        auto response1 = runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeResponse>(sender);
        UNIT_ASSERT(response1);
        UNIT_ASSERT_VALUES_EQUAL(response1->Get()->Record.GetOperationId(), operationId);

        auto response2 = runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeResponse>(sender, TDuration::Seconds(5));
        UNIT_ASSERT(!response2);
    }

    Y_UNIT_TEST(AnalyzeRebootSaBeforeAnalyzeTableResponse) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        auto tableInfo = CreateDatabaseColumnTables(env, 1, 1)[0];
        auto sender = runtime.AllocateEdgeActor();

        bool eventSeen = false;
        auto observer = runtime.AddObserver<TEvStatistics::TEvAnalyzeTableResponse>([&](auto&) {
            eventSeen = true;
        });

        auto analyzeRequest1 = MakeAnalyzeRequest({tableInfo.PathId});
        runtime.SendToPipe(tableInfo.SaTabletId, sender, analyzeRequest1.release());

        runtime.WaitFor("TEvAnalyzeTableResponse", [&]{ return eventSeen; });
        RebootTablet(runtime, tableInfo.SaTabletId, sender);

        auto analyzeRequest2 = MakeAnalyzeRequest({tableInfo.PathId});
        runtime.SendToPipe(tableInfo.SaTabletId, sender, analyzeRequest2.release());

        runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeResponse>(sender);
    }

    Y_UNIT_TEST(AnalyzeRebootSaBeforeResolve) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        auto tableInfo = CreateDatabaseColumnTables(env, 1, 1)[0];
        auto sender = runtime.AllocateEdgeActor();

        int observerCount = 0;
        auto observer = runtime.AddObserver<TEvTxProxySchemeCache::TEvResolveKeySetResult>([&](auto&){
            observerCount++;
        });

        auto analyzeRequest1 = MakeAnalyzeRequest({tableInfo.PathId});
        runtime.SendToPipe(tableInfo.SaTabletId, sender, analyzeRequest1.release());

        runtime.WaitFor("TEvResolveKeySetResult", [&]{ return observerCount == 3; });
        RebootTablet(runtime, tableInfo.SaTabletId, sender);

        auto analyzeRequest2 = MakeAnalyzeRequest({tableInfo.PathId});
        runtime.SendToPipe(tableInfo.SaTabletId, sender, analyzeRequest2.release());

        runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeResponse>(sender);
    }

    Y_UNIT_TEST(AnalyzeRebootSaBeforeReqDistribution) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        auto tableInfo = CreateDatabaseColumnTables(env, 1, 1)[0];
        auto sender = runtime.AllocateEdgeActor();

        bool eventSeen = false;
        auto observer = runtime.AddObserver<TEvHive::TEvRequestTabletDistribution>([&](auto&) {
            eventSeen = true;
        });

        auto analyzeRequest1 = MakeAnalyzeRequest({tableInfo.PathId});
        runtime.SendToPipe(tableInfo.SaTabletId, sender, analyzeRequest1.release());

        runtime.WaitFor("TEvRequestTabletDistribution", [&]{ return eventSeen; });
        RebootTablet(runtime, tableInfo.SaTabletId, sender);

        auto analyzeRequest2 = MakeAnalyzeRequest({tableInfo.PathId});
        runtime.SendToPipe(tableInfo.SaTabletId, sender, analyzeRequest2.release());

        runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeResponse>(sender);
    }

    Y_UNIT_TEST(AnalyzeRebootSaBeforeAggregate) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        auto tableInfo = CreateDatabaseColumnTables(env, 1, 1)[0];
        auto sender = runtime.AllocateEdgeActor();

        bool eventSeen = false;
        auto observer = runtime.AddObserver<TEvStatistics::TEvAggregateStatistics>([&](auto&){
            eventSeen = true;
        });

        auto analyzeRequest1 = MakeAnalyzeRequest({tableInfo.PathId});
        runtime.SendToPipe(tableInfo.SaTabletId, sender, analyzeRequest1.release());

        runtime.WaitFor("TEvAggregateStatistics", [&]{ return eventSeen; });
        RebootTablet(runtime, tableInfo.SaTabletId, sender);

        auto analyzeRequest2 = MakeAnalyzeRequest({tableInfo.PathId});
        runtime.SendToPipe(tableInfo.SaTabletId, sender, analyzeRequest2.release());

        runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeResponse>(sender);
    }

    Y_UNIT_TEST(AnalyzeRebootSaBeforeSave) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        auto tableInfo = CreateDatabaseColumnTables(env, 1, 1)[0];
        auto sender = runtime.AllocateEdgeActor();

        bool eventSeen = false;
        auto observer = runtime.AddObserver<TEvStatistics::TEvAggregateStatisticsResponse>([&](auto&){
            eventSeen = true;
        });

        auto analyzeRequest1 = MakeAnalyzeRequest({tableInfo.PathId});
        runtime.SendToPipe(tableInfo.SaTabletId, sender, analyzeRequest1.release());

        runtime.WaitFor("TEvAggregateStatisticsResponse", [&]{ return eventSeen; });
        RebootTablet(runtime, tableInfo.SaTabletId, sender);

        auto analyzeRequest2 = MakeAnalyzeRequest({tableInfo.PathId});
        runtime.SendToPipe(tableInfo.SaTabletId, sender, analyzeRequest2.release());

        runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeResponse>(sender);
    }

    //
    Y_UNIT_TEST(AnalyzeRebootSaInAggregate) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        auto tableInfo = CreateDatabaseColumnTables(env, 1, 10)[0];
        auto sender = runtime.AllocateEdgeActor();
        
        int observerCount = 0;
        auto observer = runtime.AddObserver<TEvStatistics::TEvStatisticsRequest>([&](auto&) {
            observerCount++;
        });

        auto analyzeRequest1 = MakeAnalyzeRequest({tableInfo.PathId});
        runtime.SendToPipe(tableInfo.SaTabletId, sender, analyzeRequest1.release());

        runtime.WaitFor("5th TEvStatisticsRequest", [&]{ return observerCount == 5; });
        RebootTablet(runtime, tableInfo.SaTabletId, sender);

        auto analyzeRequest2 = MakeAnalyzeRequest({tableInfo.PathId});
        runtime.SendToPipe(tableInfo.SaTabletId, sender, analyzeRequest2.release());

        runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeResponse>(sender);
    }    

}

} // NStat
} // NKikimr
