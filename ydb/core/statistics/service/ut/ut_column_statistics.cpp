#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/statistics/ut_common/ut_common.h>
#include <ydb/core/testlib/test_client.h>
#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/base/tablet_resolver.h>

#include <ydb/core/statistics/events.h>
#include <ydb/core/statistics/service/service.h>
#include <ydb/core/protos/statistics.pb.h>

#include <type_traits>

namespace NKikimr {
namespace NStat {

namespace {

TTableInfo PrepareTable(TTestEnv& env, const TString& databaseName, const TString& tableName) {
    auto tableInfo = CreateColumnTable(env, databaseName, tableName, 1);
    InsertDataIntoTable(env, databaseName, tableName, RowsWithFewDistinctValues(1000));
    return tableInfo;
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

Y_UNIT_TEST_SUITE(ColumnStatistics) {
    Y_UNIT_TEST(CountMinSketchStatistics) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();

        CreateDatabase(env, "Database");
        const auto tableInfo = PrepareTable(env, "Database", "Table1");
        Analyze(runtime, tableInfo.SaTabletId, {tableInfo.PathId});

        ValidateCountMinSketch(runtime, tableInfo.PathId);
    }

    Y_UNIT_TEST(CountMinSketchServerlessStatistics) {
        TTestEnv env(1, 3);
        auto& runtime = *env.GetServer().GetRuntime();

        CreateDatabase(env, "Shared", 1, true);
        CreateServerlessDatabase(env, "Serverless1", "/Root/Shared", 1);
        CreateServerlessDatabase(env, "Serverless2", "/Root/Shared", 1);

        const auto table1 = PrepareTable(env, "Serverless1", "Table1");
        const auto table2 = PrepareTable(env, "Serverless2", "Table2");

        Analyze(runtime, table1.SaTabletId, {table1.PathId}, "opId1", "/Root/Serverless1");
        Analyze(runtime, table2.SaTabletId, {table2.PathId}, "opId1", "/Root/Serverless2");

        ValidateCountMinSketch(runtime, table1.PathId);
        ValidateCountMinSketch(runtime, table2.PathId);
    }
}

} // NSysView
} // NKikimr
