#include <ydb/core/statistics/ut_common/ut_common.h>

#include <ydb/library/actors/testlib/test_runtime.h>

#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/statistics/events.h>
#include <ydb/core/statistics/service/service.h>

namespace NKikimr {
namespace NStat {

namespace {

TTestEnv CreateTestEnv() {
    return TTestEnv(1, 1, false, [](Tests::TServerSettings& settings) {
        settings.AppConfig->MutableStatisticsConfig()
            ->SetEnableBackgroundColumnStatsCollection(true);
    });
}

void ValidateCountMinAbsence(TTestActorRuntime& runtime, TPathId pathId) {
    std::vector<TCountMinSketchProbes> expected = {
        { .Tag = 1, .Probes = std::nullopt },
        { .Tag = 2, .Probes = std::nullopt },
    };
    CheckCountMinSketch(runtime, pathId, expected);
}

}

Y_UNIT_TEST_SUITE(TraverseDatashard) {

    Y_UNIT_TEST(TraverseOneTable) {
        TTestEnv env = CreateTestEnv();
        auto& runtime = *env.GetServer().GetRuntime();

        CreateDatabase(env, "Database");
        PrepareUniformTable(env, "Database", "Table");

        auto pathId = ResolvePathId(runtime, "/Root/Database/Table");
        ValidateCountMinAbsence(runtime, pathId);
    }

    Y_UNIT_TEST(TraverseTwoTables) {
        TTestEnv env = CreateTestEnv();
        auto& runtime = *env.GetServer().GetRuntime();

        CreateDatabase(env, "Database");
        PrepareUniformTable(env, "Database", "Table1");
        PrepareUniformTable(env, "Database", "Table2");

        auto pathId1 = ResolvePathId(runtime, "/Root/Database/Table1");
        auto pathId2 = ResolvePathId(runtime, "/Root/Database/Table2");
        ValidateCountMinAbsence(runtime, pathId1);
        ValidateCountMinAbsence(runtime, pathId2);
    }    

    Y_UNIT_TEST(TraverseOneTableServerless) {
        TTestEnv env = CreateTestEnv();
        auto& runtime = *env.GetServer().GetRuntime();

        CreateDatabase(env, "Shared", 1, true);
        CreateServerlessDatabase(env, "Serverless", "/Root/Shared");
        PrepareUniformTable(env, "Serverless", "Table");

        auto pathId = ResolvePathId(runtime, "/Root/Serverless/Table");
        ValidateCountMinAbsence(runtime, pathId);
    }

    Y_UNIT_TEST(TraverseTwoTablesServerless) {
        TTestEnv env = CreateTestEnv();
        auto& runtime = *env.GetServer().GetRuntime();

        CreateDatabase(env, "Shared", 1, true);
        CreateServerlessDatabase(env, "Serverless", "/Root/Shared");
        PrepareUniformTable(env, "Serverless", "Table1");
        PrepareUniformTable(env, "Serverless", "Table2");

        auto pathId1 = ResolvePathId(runtime, "/Root/Serverless/Table1");
        auto pathId2 = ResolvePathId(runtime, "/Root/Serverless/Table2");
        ValidateCountMinAbsence(runtime, pathId1);
        ValidateCountMinAbsence(runtime, pathId2);
    }

    Y_UNIT_TEST(TraverseTwoTablesTwoServerlessDbs) {
        TTestEnv env = CreateTestEnv();
        auto& runtime = *env.GetServer().GetRuntime();

        CreateDatabase(env, "Shared", 1, true);
        CreateServerlessDatabase(env, "Serverless1", "/Root/Shared");
        CreateServerlessDatabase(env, "Serverless2", "/Root/Shared");
        PrepareUniformTable(env, "Serverless1", "Table1");
        PrepareUniformTable(env, "Serverless2", "Table2");

        auto pathId1 = ResolvePathId(runtime, "/Root/Serverless1/Table1");
        auto pathId2 = ResolvePathId(runtime, "/Root/Serverless2/Table2");
        ValidateCountMinAbsence(runtime, pathId1);
        ValidateCountMinAbsence(runtime, pathId2);
    }

}

} // NStat
} // NKikimr
