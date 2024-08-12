#include <ydb/core/statistics/ut_common/ut_common.h>

#include <ydb/library/actors/testlib/test_runtime.h>

#include <thread>

namespace NKikimr {
namespace NStat {

struct TTableInfo {
    std::vector<ui64> ShardIds;
    ui64 SaTabletId; 
    TPathId DomainKey;
    TPathId PathId;
};

std::vector<TTableInfo> CreateDatabaseTables(TTestEnv& env, ui8 tableCount, ui8 shardCount) {
    auto init = [&] () {
        CreateDatabase(env, "Database");
        for (ui8 tableId = 1; tableId <= tableCount; tableId++) {
            CreateColumnStoreTable(env, "Database", Sprintf("Table%u", tableId), shardCount);
        }
    };
    std::thread initThread(init);

    auto& runtime = *env.GetServer().GetRuntime();
    auto sender = runtime.AllocateEdgeActor();

    runtime.SimulateSleep(TDuration::Seconds(10));
    initThread.join();

    std::vector<TTableInfo> ret;
    for (ui8 tableId = 1; tableId <= tableCount; tableId++) {
        TTableInfo tableInfo;
        const TString path = Sprintf("/Root/Database/Table%u", tableId);
        tableInfo.ShardIds = GetColumnTableShards(runtime, sender, path);
        tableInfo.PathId = ResolvePathId(runtime, path, &tableInfo.DomainKey, &tableInfo.SaTabletId);
        ret.emplace_back(tableInfo);
    }
    return ret;
}    

Y_UNIT_TEST_SUITE(AnalyzeColumnshard) {
    Y_UNIT_TEST(AnalyzeOneColumnTable) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        auto tableInfo = CreateDatabaseTables(env, 1, 1)[0];

        AnalyzeTable(runtime, tableInfo.PathId, tableInfo.ShardIds[0]);

        Analyze(runtime, {tableInfo.PathId}, tableInfo.SaTabletId);
    }

    Y_UNIT_TEST(AnalyzeAnalyzeOneColumnTableSpecificColumns) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        auto tableInfo = CreateDatabaseTables(env, 1, 1)[0];

        Analyze(runtime, {{tableInfo.PathId, {1, 2}}}, tableInfo.SaTabletId);
    }

    Y_UNIT_TEST(AnalyzeTwoColumnTables) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        auto tableInfos = CreateDatabaseTables(env, 2, 1);

        Analyze(runtime, {tableInfos[0].PathId, tableInfos[1].PathId}, tableInfos[0].SaTabletId);
    }

    Y_UNIT_TEST(AnalyzeSameCookie) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        auto tableInfo = CreateDatabaseTables(env, 1, 1)[0];

        auto sender = runtime.AllocateEdgeActor();
        const TString cookie = "cookie";

        auto analyzeRequest1 = MakeAnalyzeRequest(cookie, {tableInfo.PathId});
        runtime.SendToPipe(tableInfo.SaTabletId, sender, analyzeRequest1.release());

        auto analyzeRequest2 = MakeAnalyzeRequest(cookie, {tableInfo.PathId});
        runtime.SendToPipe(tableInfo.SaTabletId, sender, analyzeRequest2.release());

        auto response1 = runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeResponse>(sender);
        UNIT_ASSERT(response1);
        UNIT_ASSERT_VALUES_EQUAL(response1->Get()->Record.GetCookie(), cookie);

        auto response2 = runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeResponse>(sender, TDuration::Seconds(5));
        UNIT_ASSERT(!response2);
    }

}

} // NStat
} // NKikimr
