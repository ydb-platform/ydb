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

const std::vector<TColumnDesc>& GetColumns() {
    static const std::vector<TColumnDesc> ret {
        {
            .Name = "LowCardinalityString",
            .TypeId = NScheme::NTypeIds::String,
            .AddValue = [](ui64 key, Ydb::Value& row) {
                row.add_items()->set_bytes_value(ToString(key % 10));
            },
        },
    };

    return ret;
}

ui16 GetTag(const std::string_view& columnName) {
    if (columnName == "Key") {
        return 1;
    }

    const auto& columns = GetColumns();
    for (size_t i = 0; i < columns.size(); ++i) {
        if (columns[i].Name == columnName) {
            return i + 2; // Key column is 1, value columns go after.
        }
    }
    UNIT_ASSERT_C(false, "unknown column " << columnName);
    Y_UNREACHABLE();
}

TTableInfo PrepareTable(TTestEnv& env, const TString& databaseName, const TString& tableName) {
    auto info = CreateColumnTable(env, databaseName, tableName, 4, GetColumns());
    InsertDataIntoTable(env, databaseName, tableName, ColumnTableRowsNumber, GetColumns());
    return info;
}

void ValidateCountMinSketch(TTestActorRuntime& runtime, const TPathId& pathId) {
    std::vector<TCountMinSketchProbes> expected = {
        {
            .Tag = GetTag("Key"),
            .Probes = std::nullopt,
        },
        {
            .Tag = GetTag("LowCardinalityString"),
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

    Y_UNIT_TEST(SimpleColumnStatistics) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();

        CreateDatabase(env, "Database");
        const auto tableInfo = PrepareTable(env, "Database", "Table1");
        Analyze(runtime, tableInfo.SaTabletId, {tableInfo.PathId});

        auto responses = GetStatistics(runtime, tableInfo.PathId, EStatType::SIMPLE_COLUMN, {
            GetTag("Key"),
            GetTag("LowCardinalityString"),
        });
        UNIT_ASSERT_VALUES_EQUAL(responses.size(), 2);
        for (const auto& resp : responses) {
            UNIT_ASSERT(resp.Success);
            UNIT_ASSERT(resp.SimpleColumn.Data);
            UNIT_ASSERT_VALUES_EQUAL(resp.SimpleColumn.Data->GetCount(), 1000);
        }

        {
            // Key column
            const auto& data = *responses[0].SimpleColumn.Data;
            UNIT_ASSERT_VALUES_EQUAL(data.GetCountDistinct(), 1000);
        }

        {
            // LowCardinalityString column
            const auto& data = *responses[1].SimpleColumn.Data;
            UNIT_ASSERT_VALUES_EQUAL(data.GetCountDistinct(), 10);
        }
    }
}

} // NSysView
} // NKikimr
