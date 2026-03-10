#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/statistics/ut_common/ut_common.h>
#include <ydb/core/testlib/test_client.h>
#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/base/tablet_resolver.h>

#include <ydb/core/statistics/events.h>
#include <ydb/core/statistics/service/service.h>
#include <ydb/core/protos/statistics.pb.h>

#include <type_traits>

namespace NKikimr::NStat {

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
        {
            .Name = "LowCardinalityInt",
            .TypeId = NScheme::NTypeIds::Int16,
            .AddValue = [](ui64 key, Ydb::Value& row) {
                row.add_items()->set_int32_value(key % 10);
            },
        },
        {
            .Name = "Float",
            .TypeId = NScheme::NTypeIds::Float,
            .AddValue = [](ui64 key, Ydb::Value& row) {
                row.add_items()->set_float_value(key / 10);
            },
        },
        {
            .Name = "Date",
            .TypeId = NScheme::NTypeIds::Date,
            .AddValue = [](ui64 key, Ydb::Value& row) {
                ui32 startDate = 10000; // 1997-05-19
                row.add_items()->set_uint32_value(startDate + key / 10);
            },
        },
        {
            .Name = "NearNumericLimits",
            .TypeId = NScheme::NTypeIds::Int64,
            .AddValue = [](ui64 key, Ydb::Value& row) {
                Y_ASSERT(key >= 0);
                i64 val = (key % 2 == 0
                    ? std::numeric_limits<i64>::min() + key / 10
                    : std::numeric_limits<i64>::max() - key / 10);
                row.add_items()->set_int64_value(val);
            },
        },
    };

    return ret;
}

ui16 GetTag(const std::string_view& columnName, const std::vector<TColumnDesc>& columns = GetColumns()) {
    if (columnName == "Key") {
        return 1;
    }

    for (size_t i = 0; i < columns.size(); ++i) {
        if (columns[i].Name == columnName) {
            return i + 2; // Key column is 1, value columns go after.
        }
    }
    UNIT_ASSERT_C(false, "unknown column " << columnName);
    Y_UNREACHABLE();
}

TTableInfo PrepareTable(
        TTestEnv& env, const TString& databaseName, const TString& tableName,
        const std::vector<TColumnDesc>& columns = GetColumns()) {
    auto info = CreateColumnTable(env, databaseName, tableName, 4, columns);
    InsertDataIntoTable(env, databaseName, tableName, ColumnTableRowsNumber, columns);
    return info;
}

std::vector<TResponse> PrepareAndGetStatistics(
        const std::vector<TColumnDesc>& columnsDesc,
        EStatType type,
        const std::vector<std::string>& columns) {
    TTestEnv env(1, 1);
    auto& runtime = *env.GetServer().GetRuntime();

    CreateDatabase(env, "Database");
    const auto tableInfo = PrepareTable(env, "Database", "Table1", columnsDesc);
    Analyze(runtime, tableInfo.SaTabletId, {tableInfo.PathId});

    std::vector<std::optional<ui32>> columnTags;
    for (const auto& col : columns) {
        columnTags.push_back(GetTag(col, columnsDesc));
    }
    auto responses = GetStatistics(runtime, tableInfo.PathId, type, columnTags);
    UNIT_ASSERT_VALUES_EQUAL(responses.size(), columns.size());
    return responses;
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
        auto responses = PrepareAndGetStatistics(
            GetColumns(), EStatType::SIMPLE_COLUMN, { "Key", "LowCardinalityString" });
        for (const auto& resp : responses) {
            UNIT_ASSERT(resp.Success);
            UNIT_ASSERT(resp.SimpleColumn.Data);
            UNIT_ASSERT_VALUES_EQUAL(resp.SimpleColumn.Data->GetCount(), 1000);
        }

        {
            // Key column
            const auto& data = *responses.at(0).SimpleColumn.Data;
            UNIT_ASSERT_VALUES_EQUAL(data.GetCountDistinct(), 1000);
            UNIT_ASSERT_VALUES_EQUAL(data.GetTypeId(), NScheme::NTypeIds::Uint64);
            UNIT_ASSERT(!data.HasTypeInfo());
            UNIT_ASSERT_VALUES_EQUAL(data.GetMin().uint64_value(), 0);
            UNIT_ASSERT_VALUES_EQUAL(data.GetMax().uint64_value(), 999);
        }

        {
            // LowCardinalityString column
            const auto& data = *responses.at(1).SimpleColumn.Data;
            UNIT_ASSERT_VALUES_EQUAL(data.GetCountDistinct(), 10);
            UNIT_ASSERT_VALUES_EQUAL(data.GetTypeId(), NScheme::NTypeIds::String);
            UNIT_ASSERT(!data.HasTypeInfo());
            UNIT_ASSERT(!data.HasMin());
            UNIT_ASSERT(!data.HasMax());
        }
    }

    Y_UNIT_TEST(EqWidthHistogram) {
        auto responses = PrepareAndGetStatistics(
            GetColumns(), EStatType::EQ_WIDTH_HISTOGRAM, {
                "Key",
                "LowCardinalityString",
                "LowCardinalityInt",
                "Float",
                "Date",
                "NearNumericLimits",
            });

        UNIT_ASSERT(!responses.at(0).Success);
        UNIT_ASSERT(!responses.at(1).Success);

        {
            // LowCardinalityInt column
            const auto& resp = responses.at(2);
            UNIT_ASSERT(resp.Success);
            const auto& histogram = resp.EqWidthHistogram.Data;
            UNIT_ASSERT(histogram);
            UNIT_ASSERT(histogram->GetType() == EHistogramValueType::Int16);
            auto estimator = TEqWidthHistogramEstimator(histogram);
            UNIT_ASSERT_VALUES_EQUAL(estimator.EstimateLess<i16>(5), 500);
        }

        {
            // Float column
            const auto& resp = responses.at(3);
            UNIT_ASSERT(resp.Success);
            const auto& histogram = resp.EqWidthHistogram.Data;
            UNIT_ASSERT(histogram);
            UNIT_ASSERT(histogram->GetType() == EHistogramValueType::Float);
            auto estimator = TEqWidthHistogramEstimator(histogram);
            const i64 expected = 500;
            const i64 actual = estimator.EstimateLess<float>(50.0);
            UNIT_ASSERT_C(
                std::abs(expected - actual) < 50,
                "Expected: " << expected << ", actual: " << actual);
        }

        {
            // Date column
            const auto& resp = responses.at(4);
            UNIT_ASSERT(resp.Success);
            const auto& histogram = resp.EqWidthHistogram.Data;
            UNIT_ASSERT(histogram);
            UNIT_ASSERT(histogram->GetType() == EHistogramValueType::Uint16);
            auto estimator = TEqWidthHistogramEstimator(histogram);
            const i64 expected = 500;
            const i64 actual = estimator.EstimateLess<ui16>(10000 + 50);
            UNIT_ASSERT_C(
                std::abs(expected - actual) < 50,
                "Expected: " << expected << ", actual: " << actual);
        }

        {
            // NearNumericLimits column
            const auto& resp = responses.at(5);
            UNIT_ASSERT(resp.Success);
            const auto& histogram = resp.EqWidthHistogram.Data;
            UNIT_ASSERT(histogram);
            UNIT_ASSERT(histogram->GetType() == EHistogramValueType::Int64);
            auto estimator = TEqWidthHistogramEstimator(histogram);
            UNIT_ASSERT_VALUES_EQUAL(estimator.EstimateLess<i64>(0), 500);
        }
    }

    Y_UNIT_TEST(EqWidthHistogramSmallParamTypes) {
        // Regression test for a bug involving serializing small min/max values
        // as literals of incorrect type.
        const std::vector<TColumnDesc> columns {
            {
                .Name = "Value",
                .TypeId = NScheme::NTypeIds::Int64,
                .AddValue = [](ui64 key, Ydb::Value& row) {
                    row.add_items()->set_int64_value(key % 2 == 0 ? 1 : -1);
                },
            },
        };

        auto responses = PrepareAndGetStatistics(
            columns, EStatType::EQ_WIDTH_HISTOGRAM, {"Value"});

        const auto& resp = responses.at(0);
        UNIT_ASSERT(resp.Success);
        const auto& histogram = resp.EqWidthHistogram.Data;
        UNIT_ASSERT(histogram);
        UNIT_ASSERT(histogram->GetType() == EHistogramValueType::Int64);
        auto estimator = TEqWidthHistogramEstimator(histogram);
        UNIT_ASSERT_VALUES_EQUAL(estimator.EstimateLess<i64>(0), 500);
    }

    Y_UNIT_TEST(ManyColumns) {
        std::vector<TColumnDesc> columns;
        for (size_t i = 0; i < 100; ++i) {
            columns.push_back({
                .Name = std::format("V_{}", i),
                .TypeId = NScheme::NTypeIds::Int64,
                .AddValue = [](ui64 key, Ydb::Value& row) {
                    row.add_items()->set_int64_value(key / 10);
                },
            });
        }

        TTestEnv env(1, 3);
        auto& runtime = *env.GetServer().GetRuntime();

        CreateDatabase(env, "Database", 3);
        const auto tableInfo = PrepareTable(env, "Database", "Table1", columns);
        Analyze(runtime, tableInfo.SaTabletId, {tableInfo.PathId});

        auto responses = GetStatistics(
            runtime, tableInfo.PathId, EStatType::COUNT_MIN_SKETCH, {GetTag("V_99", columns)});
        UNIT_ASSERT_VALUES_EQUAL(responses.size(), 1);
        const auto& resp = responses.at(0);
        UNIT_ASSERT(resp.Success);
        UNIT_ASSERT(resp.CountMinSketch.CountMin);
        UNIT_ASSERT_VALUES_EQUAL(
            resp.CountMinSketch.CountMin->GetElementCount(), ColumnTableRowsNumber);
    }
}

} // NKikimr::NStat
