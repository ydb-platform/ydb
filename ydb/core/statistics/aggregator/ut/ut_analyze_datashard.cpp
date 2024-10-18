#include <ydb/core/statistics/ut_common/ut_common.h>

#include <ydb/library/actors/testlib/test_runtime.h>

#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/statistics/events.h>
#include <ydb/core/statistics/service/service.h>

namespace NKikimr {
namespace NStat {

Y_UNIT_TEST_SUITE(AnalyzeDatashard) {

    Y_UNIT_TEST(AnalyzeOneTable) {
        TTestEnv env(1, 1);

        auto& runtime = *env.GetServer().GetRuntime();

        CreateDatabase(env, "Database");
        CreateUniformTable(env, "Database", "Table");

        ui64 saTabletId;
        auto pathId = ResolvePathId(runtime, "/Root/Database/Table", nullptr, &saTabletId);

        Analyze(runtime, saTabletId, {{pathId}});

        ValidateCountMinDatashardAbsense(runtime, pathId);
    }

    Y_UNIT_TEST(AnalyzeTwoTables) {
        TTestEnv env(1, 1);

        auto& runtime = *env.GetServer().GetRuntime();

        CreateDatabase(env, "Database");
        CreateUniformTable(env, "Database", "Table1");
        CreateUniformTable(env, "Database", "Table2");

        ui64 saTabletId1;
        auto pathId1 = ResolvePathId(runtime, "/Root/Database/Table1", nullptr, &saTabletId1);
        auto pathId2 = ResolvePathId(runtime, "/Root/Database/Table2");

        Analyze(runtime, saTabletId1, {pathId1, pathId2});

        ValidateCountMinDatashardAbsense(runtime, pathId1);
        ValidateCountMinDatashardAbsense(runtime, pathId2);
    }

    Y_UNIT_TEST(DropTableNavigateError) {
        TTestEnv env(1, 1);

        auto& runtime = *env.GetServer().GetRuntime();

        CreateDatabase(env, "Database");
        CreateUniformTable(env, "Database", "Table");

        ui64 saTabletId = 0;
        auto pathId = ResolvePathId(runtime, "/Root/Database/Table", nullptr, &saTabletId);

        DropTable(env, "Database", "Table");

        Analyze(runtime, saTabletId, {pathId});

        ValidateCountMinDatashardAbsense(runtime, pathId);
    }
}

} // NStat
} // NKikimr
