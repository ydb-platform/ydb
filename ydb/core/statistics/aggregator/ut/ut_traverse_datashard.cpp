#include <ydb/core/statistics/ut_common/ut_common.h>

#include <ydb/library/actors/testlib/test_runtime.h>

#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/statistics/events.h>
#include <ydb/core/statistics/service/service.h>

namespace NKikimr {
namespace NStat {

Y_UNIT_TEST_SUITE(TraverseDatashard) {

    Y_UNIT_TEST(TraverseOneTable) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();

        CreateDatabase(env, "Database");
        CreateUniformTable(env, "Database", "Table");

        auto pathId = ResolvePathId(runtime, "/Root/Database/Table");
        ValidateCountMinDatashardAbsense(runtime, pathId);
    }

    Y_UNIT_TEST(TraverseTwoTables) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();

        CreateDatabase(env, "Database");
        CreateUniformTable(env, "Database", "Table1");
        CreateUniformTable(env, "Database", "Table2");

        auto pathId1 = ResolvePathId(runtime, "/Root/Database/Table1");
        auto pathId2 = ResolvePathId(runtime, "/Root/Database/Table2");
        ValidateCountMinDatashardAbsense(runtime, pathId1);
        ValidateCountMinDatashardAbsense(runtime, pathId2);
    }    

    Y_UNIT_TEST(TraverseOneTableServerless) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();

        CreateDatabase(env, "Shared", 1, true);
        CreateServerlessDatabase(env, "Serverless", "/Root/Shared");
        CreateUniformTable(env, "Serverless", "Table");

        auto pathId = ResolvePathId(runtime, "/Root/Serverless/Table");
        ValidateCountMinDatashardAbsense(runtime, pathId);
    }

    Y_UNIT_TEST(TraverseTwoTablesServerless) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();

        CreateDatabase(env, "Shared", 1, true);
        CreateServerlessDatabase(env, "Serverless", "/Root/Shared");
        CreateUniformTable(env, "Serverless", "Table1");
        CreateUniformTable(env, "Serverless", "Table2");

        auto pathId1 = ResolvePathId(runtime, "/Root/Serverless/Table1");
        auto pathId2 = ResolvePathId(runtime, "/Root/Serverless/Table2");
        ValidateCountMinDatashardAbsense(runtime, pathId1);
        ValidateCountMinDatashardAbsense(runtime, pathId2);
    }

    Y_UNIT_TEST(TraverseTwoTablesTwoServerlessDbs) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();

        CreateDatabase(env, "Shared", 1, true);
        CreateServerlessDatabase(env, "Serverless1", "/Root/Shared");
        CreateServerlessDatabase(env, "Serverless2", "/Root/Shared");
        CreateUniformTable(env, "Serverless1", "Table1");
        CreateUniformTable(env, "Serverless2", "Table2");

        auto pathId1 = ResolvePathId(runtime, "/Root/Serverless1/Table1");
        auto pathId2 = ResolvePathId(runtime, "/Root/Serverless2/Table2");
        ValidateCountMinDatashardAbsense(runtime, pathId1);
        ValidateCountMinDatashardAbsense(runtime, pathId2);
    }

}

} // NStat
} // NKikimr
