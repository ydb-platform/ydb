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

    Y_UNIT_TEST(AnalyzeTwoColumnTables) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        auto tableInfos = CreateDatabaseTables(env, 2, 1);

        Analyze(runtime, {tableInfos[0].PathId, tableInfos[1].PathId}, tableInfos[0].SaTabletId);
    }
}

} // NStat
} // NKikimr
