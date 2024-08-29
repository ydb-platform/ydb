#include <ydb/core/statistics/ut_common/ut_common.h>

#include <ydb/library/actors/testlib/test_runtime.h>

#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/statistics/events.h>
#include <ydb/core/statistics/service/service.h>

#include <thread>

namespace NKikimr {
namespace NStat {

namespace {


} // namespace

Y_UNIT_TEST_SUITE(TraverseDatashard) {

    Y_UNIT_TEST(TraverseOneTable) {
        TTestEnv env(1, 1);
        auto init = [&] () {
            CreateDatabase(env, "Database");
            CreateUniformTable(env, "Database", "Table");
        };
        std::thread initThread(init);

        auto& runtime = *env.GetServer().GetRuntime();
        runtime.SimulateSleep(TDuration::Seconds(5));
        initThread.join();

        runtime.SimulateSleep(TDuration::Seconds(60));

        auto pathId = ResolvePathId(runtime, "/Root/Database/Table");
        ValidateCountMinDatashard(runtime, pathId);
    }

    Y_UNIT_TEST(TraverseTwoTables) {
        TTestEnv env(1, 1);
        auto init = [&] () {
            CreateDatabase(env, "Database");
            CreateUniformTable(env, "Database", "Table1");
            CreateUniformTable(env, "Database", "Table2");
        };
        std::thread initThread(init);

        auto& runtime = *env.GetServer().GetRuntime();
        runtime.SimulateSleep(TDuration::Seconds(5));
        initThread.join();

        runtime.SimulateSleep(TDuration::Seconds(60));

        auto pathId1 = ResolvePathId(runtime, "/Root/Database/Table1");
        auto pathId2 = ResolvePathId(runtime, "/Root/Database/Table2");
        ValidateCountMinDatashard(runtime, pathId1);
        ValidateCountMinDatashard(runtime, pathId2);
    }    

    Y_UNIT_TEST(TraverseOneTableServerless) {
        TTestEnv env(1, 1);

        auto init = [&] () {
            CreateDatabase(env, "Shared");
        };
        std::thread initThread(init);

        auto& runtime = *env.GetServer().GetRuntime();
        runtime.SimulateSleep(TDuration::Seconds(5));
        initThread.join();

        TPathId domainKey;
        ResolvePathId(runtime, "/Root/Shared", &domainKey);

        auto init2 = [&] () {
            CreateServerlessDatabase(env, "Serverless", domainKey);
            CreateUniformTable(env, "Serverless", "Table");
        };
        std::thread init2Thread(init2);

        runtime.SimulateSleep(TDuration::Seconds(5));
        init2Thread.join();

        runtime.SimulateSleep(TDuration::Seconds(60));

        auto pathId = ResolvePathId(runtime, "/Root/Serverless/Table");
        ValidateCountMinDatashard(runtime, pathId);
    }

    Y_UNIT_TEST(TraverseTwoTablesServerless) {
        TTestEnv env(1, 1);

        auto init = [&] () {
            CreateDatabase(env, "Shared");
        };
        std::thread initThread(init);

        auto& runtime = *env.GetServer().GetRuntime();
        runtime.SimulateSleep(TDuration::Seconds(5));
        initThread.join();

        TPathId domainKey;
        ResolvePathId(runtime, "/Root/Shared", &domainKey);

        auto init2 = [&] () {
            CreateServerlessDatabase(env, "Serverless", domainKey);
            CreateUniformTable(env, "Serverless", "Table1");
            CreateUniformTable(env, "Serverless", "Table2");
        };
        std::thread init2Thread(init2);

        runtime.SimulateSleep(TDuration::Seconds(5));
        init2Thread.join();

        runtime.SimulateSleep(TDuration::Seconds(60));

        auto pathId1 = ResolvePathId(runtime, "/Root/Serverless/Table1");
        auto pathId2 = ResolvePathId(runtime, "/Root/Serverless/Table2");
        ValidateCountMinDatashard(runtime, pathId1);
        ValidateCountMinDatashard(runtime, pathId2);
    }

    Y_UNIT_TEST(TraverseTwoTablesTwoServerlessDbs) {
        TTestEnv env(1, 1);

        auto init = [&] () {
            CreateDatabase(env, "Shared");
        };
        std::thread initThread(init);

        auto& runtime = *env.GetServer().GetRuntime();
        runtime.SimulateSleep(TDuration::Seconds(5));
        initThread.join();

        TPathId domainKey;
        ResolvePathId(runtime, "/Root/Shared", &domainKey);

        auto init2 = [&] () {
            CreateServerlessDatabase(env, "Serverless1", domainKey);
            CreateServerlessDatabase(env, "Serverless2", domainKey);
            CreateUniformTable(env, "Serverless1", "Table1");
            CreateUniformTable(env, "Serverless2", "Table2");
        };
        std::thread init2Thread(init2);

        runtime.SimulateSleep(TDuration::Seconds(5));
        init2Thread.join();

        runtime.SimulateSleep(TDuration::Seconds(60));

        auto pathId1 = ResolvePathId(runtime, "/Root/Serverless1/Table1");
        auto pathId2 = ResolvePathId(runtime, "/Root/Serverless2/Table2");
        ValidateCountMinDatashard(runtime, pathId1);
        ValidateCountMinDatashard(runtime, pathId2);
    }

}

} // NStat
} // NKikimr
