#include <ydb/core/statistics/ut_common/ut_common.h>

#include <ydb/library/actors/testlib/test_runtime.h>

#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/statistics/events.h>
#include <ydb/core/statistics/service/service.h>

namespace NKikimr {
namespace NStat {

namespace {

void PrepareTable(TTestEnv& env, const TString& tableName) {
    CreateUniformTable(env, "Database", tableName);
    InsertDataIntoTable(env, "Database", tableName, RowsWithFewDistinctValues(1000));
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

        Analyze(
            runtime, saTabletId, {pathId},
            "operationId", {}, NKikimrStat::TEvAnalyzeResponse::STATUS_ERROR);

        std::vector<TCountMinSketchProbes> expected = {
            { .Tag = 1, .Probes = std::nullopt },
            { .Tag = 2, .Probes = std::nullopt },
        };
        CheckCountMinSketch(runtime, pathId, expected);
    }
}

} // NStat
} // NKikimr
