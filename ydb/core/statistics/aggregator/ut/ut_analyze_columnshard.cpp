#include <ydb/core/statistics/ut_common/ut_common.h>

#include <ydb/library/actors/testlib/test_runtime.h>

#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <ydb/core/testlib/actors/block_events.h>

namespace NKikimr {
namespace NStat {

Y_UNIT_TEST_SUITE(AnalyzeColumnshard) {
    Y_UNIT_TEST(AnalyzeTable) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        auto tableInfo = CreateDatabaseColumnTables(env, 1, 1)[0];

        AnalyzeTable(runtime, tableInfo.ShardIds[0], tableInfo.PathId);
    }

    Y_UNIT_TEST(Analyze) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        auto tableInfo = CreateDatabaseColumnTables(env, 1, 1)[0];

        Analyze(runtime, tableInfo.SaTabletId, {tableInfo.PathId});
    }

    Y_UNIT_TEST(AnalyzeServerless) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        auto tableInfo = CreateServerlessDatabaseColumnTables(env, 1, 1)[0];

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

        TBlockEvents<TEvStatistics::TEvAnalyzeTableResponse> block(runtime);

        auto tableInfo = CreateDatabaseColumnTables(env, 1, 1)[0];

        const TString operationId = "operationId";

        AnalyzeStatus(runtime, sender, tableInfo.SaTabletId, operationId, NKikimrStat::TEvAnalyzeStatusResponse::STATUS_NO_OPERATION);

        auto analyzeRequest = MakeAnalyzeRequest({{tableInfo.PathId, {1, 2}}}, operationId);
        runtime.SendToPipe(tableInfo.SaTabletId, sender, analyzeRequest.release());

        AnalyzeStatus(runtime, sender, tableInfo.SaTabletId, operationId, NKikimrStat::TEvAnalyzeStatusResponse::STATUS_ENQUEUED);

        // Check EvRemoteHttpInfo
        {
            auto httpRequest = std::make_unique<NActors::NMon::TEvRemoteHttpInfo>("/app?");
            runtime.SendToPipe(tableInfo.SaTabletId, sender, httpRequest.release(), 0, {});
            auto httpResponse = runtime.GrabEdgeEventRethrow<NActors::NMon::TEvRemoteHttpInfoRes>(sender);
            TString body = httpResponse->Get()->Html;
            Cerr << body << Endl;
            UNIT_ASSERT(body.Size() > 500);
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
        auto tableInfo = CreateDatabaseColumnTables(env, 1, 1)[0];
        auto sender = runtime.AllocateEdgeActor();
        const TString operationId = "operationId";

        TBlockEvents<TEvStatistics::TEvAnalyzeTableResponse> block(runtime);

        auto tabletPipe = runtime.ConnectToPipe(tableInfo.SaTabletId, sender, 0, {});

        auto analyzeRequest1 = MakeAnalyzeRequest({tableInfo.PathId}, operationId);
        runtime.SendToPipe(tabletPipe, sender, analyzeRequest1.release());

        runtime.WaitFor("TEvAnalyzeTableResponse", [&]{ return block.size(); });

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
        auto tableInfo = CreateDatabaseColumnTables(env, 1, 1)[0];
        auto sender = runtime.AllocateEdgeActor();

        auto GetOperationId = [] (size_t i) { return TStringBuilder() << "operationId" << i; };

        TBlockEvents<TEvStatistics::TEvAnalyzeTableResponse> block(runtime);

        const size_t numEvents = 10;

        auto tabletPipe = runtime.ConnectToPipe(tableInfo.SaTabletId, sender, 0, {});

        for (size_t i = 0; i < numEvents; ++i) {
            auto analyzeRequest = MakeAnalyzeRequest({tableInfo.PathId}, GetOperationId(i));
            runtime.SendToPipe(tabletPipe, sender, analyzeRequest.release());
        }

        runtime.WaitFor("TEvAnalyzeTableResponse", [&]{ return block.size() == numEvents; });

        block.Unblock();
        block.Stop();

        for (size_t i = 0; i < numEvents; ++i) {
            auto response = runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeResponse>(sender);
            UNIT_ASSERT(response);
            UNIT_ASSERT_VALUES_EQUAL(response->Get()->Record.GetOperationId(), GetOperationId(i));
        }        
    }    

    Y_UNIT_TEST(AnalyzeRebootSaBeforeAnalyzeTableResponse) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        auto tableInfo = CreateDatabaseColumnTables(env, 1, 1)[0];
        auto sender = runtime.AllocateEdgeActor();

        bool eventSeen = false;
        auto observer = runtime.AddObserver<TEvStatistics::TEvAnalyzeTableResponse>([&](auto& ev) {
            eventSeen = true;
            ev.Reset();
        });

        auto analyzeRequest1 = MakeAnalyzeRequest({tableInfo.PathId});
        runtime.SendToPipe(tableInfo.SaTabletId, sender, analyzeRequest1.release());

        runtime.WaitFor("TEvAnalyzeTableResponse", [&]{ return eventSeen; });
        observer.Remove();
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

        TBlockEvents<TEvTxProxySchemeCache::TEvResolveKeySetResult> block(runtime);

        auto analyzeRequest1 = MakeAnalyzeRequest({tableInfo.PathId});
        runtime.SendToPipe(tableInfo.SaTabletId, sender, analyzeRequest1.release());
        
        runtime.WaitFor("1st TEvResolveKeySetResult", [&]{ return block.size() >= 1; });
        block.Unblock(1);
        runtime.WaitFor("2nd TEvResolveKeySetResult", [&]{ return block.size() >= 1; });
        block.Unblock(1);
        runtime.WaitFor("3rd TEvResolveKeySetResult", [&]{ return block.size() >= 1; });
        
        RebootTablet(runtime, tableInfo.SaTabletId, sender);
        
        block.Unblock();
        block.Stop();

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
        auto observer = runtime.AddObserver<TEvHive::TEvRequestTabletDistribution>([&](auto& ev) {
            eventSeen = true;
            ev.Reset();            
        });

        auto analyzeRequest1 = MakeAnalyzeRequest({tableInfo.PathId});
        runtime.SendToPipe(tableInfo.SaTabletId, sender, analyzeRequest1.release());

        runtime.WaitFor("TEvRequestTabletDistribution", [&]{ return eventSeen; });
        observer.Remove();
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
        auto observer = runtime.AddObserver<TEvStatistics::TEvAggregateStatistics>([&](auto& ev){
            eventSeen = true;
            ev.Reset();
        });

        auto analyzeRequest1 = MakeAnalyzeRequest({tableInfo.PathId});
        runtime.SendToPipe(tableInfo.SaTabletId, sender, analyzeRequest1.release());

        runtime.WaitFor("TEvAggregateStatistics", [&]{ return eventSeen; });
        observer.Remove();
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
        auto observer = runtime.AddObserver<TEvStatistics::TEvAggregateStatisticsResponse>([&](auto& ev){
            eventSeen = true;
            ev.Reset();
        });

        auto analyzeRequest1 = MakeAnalyzeRequest({tableInfo.PathId});
        runtime.SendToPipe(tableInfo.SaTabletId, sender, analyzeRequest1.release());

        runtime.WaitFor("TEvAggregateStatisticsResponse", [&]{ return eventSeen; });
        observer.Remove();        
        RebootTablet(runtime, tableInfo.SaTabletId, sender);

        auto analyzeRequest2 = MakeAnalyzeRequest({tableInfo.PathId});
        runtime.SendToPipe(tableInfo.SaTabletId, sender, analyzeRequest2.release());

        runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeResponse>(sender);
    }

    Y_UNIT_TEST(AnalyzeRebootSaInAggregate) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        auto tableInfo = CreateDatabaseColumnTables(env, 1, 10)[0];
        auto sender = runtime.AllocateEdgeActor();
        
        int observerCount = 0;
        auto observer = runtime.AddObserver<TEvStatistics::TEvStatisticsRequest>([&](auto& ev) {
            if (++observerCount >= 5) {
                ev.Reset();
            }
        });

        auto analyzeRequest1 = MakeAnalyzeRequest({tableInfo.PathId});
        runtime.SendToPipe(tableInfo.SaTabletId, sender, analyzeRequest1.release());

        runtime.WaitFor("5th TEvStatisticsRequest", [&]{ return observerCount >= 5; });
        observer.Remove();
        RebootTablet(runtime, tableInfo.SaTabletId, sender);

        auto analyzeRequest2 = MakeAnalyzeRequest({tableInfo.PathId});
        runtime.SendToPipe(tableInfo.SaTabletId, sender, analyzeRequest2.release());

        runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeResponse>(sender);
    }    

    Y_UNIT_TEST(AnalyzeRebootColumnShard) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        auto tableInfo = CreateDatabaseColumnTables(env, 1, 1)[0];
        auto sender = runtime.AllocateEdgeActor();

        TBlockEvents<TEvStatistics::TEvAnalyzeTableResponse> block(runtime);

        auto analyzeRequest = MakeAnalyzeRequest({tableInfo.PathId});
        runtime.SendToPipe(tableInfo.SaTabletId, sender, analyzeRequest.release());

        runtime.WaitFor("TEvAnalyzeTableResponse", [&]{ return block.size(); });
        block.Stop();
        RebootTablet(runtime, tableInfo.ShardIds[0], sender);

        runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeResponse>(sender);
    }

    Y_UNIT_TEST(AnalyzeDeadline) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        auto tableInfo = CreateDatabaseColumnTables(env, 1, 1)[0];
        auto sender = runtime.AllocateEdgeActor();

        TBlockEvents<TEvStatistics::TEvAnalyzeTableResponse> block(runtime);

        auto analyzeRequest = MakeAnalyzeRequest({tableInfo.PathId});
        runtime.SendToPipe(tableInfo.SaTabletId, sender, analyzeRequest.release());

        runtime.WaitFor("TEvAnalyzeTableResponse", [&]{ return block.size(); });
        runtime.AdvanceCurrentTime(TDuration::Days(2));

        auto analyzeResponse = runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeResponse>(sender);
        const auto& record = analyzeResponse->Get()->Record;
        UNIT_ASSERT_VALUES_EQUAL(record.GetOperationId(), "operationId");
        UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), NKikimrStat::TEvAnalyzeResponse::STATUS_ERROR);
    }    
}

} // NStat
} // NKikimr
