#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include "helpers/kqp_analyzed_benchmarks.h"
#include <yql/essentials/core/minsketch/count_min_sketch.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/testing/common/env.h>

#include <util/stream/file.h>

namespace NKikimr::NKqp {
Y_UNIT_TEST_SUITE(KqpRboStatistics) {
    Y_UNIT_TEST(RejectStatisticsScaleMismatch) {
        // Validation runs before the fixture starts actors or invokes the CLI.
        UNIT_ASSERT_EXCEPTION_CONTAINS(LoadTpchAndAnalyze(1.0), yexception, "require scale 0.1");
        UNIT_ASSERT_EXCEPTION_CONTAINS(LoadTpcdsAndAnalyze(0.01, true), yexception, "require scale 0.1");
    }

    Y_UNIT_TEST(ExplainTpchFilterAfterAnalyze) {
        // Generate a small, self-contained dataset with real row and column
        // statistics, pinning only base counts and sizes from the JSON snapshot.
        // Keep the owning fixture alive while using its session.
        constexpr double scale = 0.1;
        auto benchmark = LoadTpchAndAnalyze(scale);
        auto& session = benchmark->GetSession();

        // Establish ground truth independently of both the optimizer and CMS.
        // Only these counting queries execute; the query under test is explained.
        const auto countRows = [&](const TString& query) {
            const auto result = session.ExecuteQuery(query,
                NYdb::NQuery::TTxControl::BeginTx(NYdb::NQuery::TTxSettings::SerializableRW()).CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            NYdb::TResultSetParser parser(result.GetResultSet(0));
            UNIT_ASSERT(parser.TryNextRow());
            return parser.ColumnParser(0).GetUint64();
        };
        const auto actualRows = countRows("SELECT COUNT(*) FROM `tpch/customer`;");
        const auto actualFilteredRows = countRows(R"(
            SELECT COUNT(*) FROM `tpch/customer` WHERE c_mktsegment = 'HOUSEHOLD';
        )");
        UNIT_ASSERT_VALUES_EQUAL(actualRows, 15000); // TPCH customer at scale 0.1.
        UNIT_ASSERT_GT(actualFilteredRows, 0);
        UNIT_ASSERT_LT(actualFilteredRows, actualRows);

        auto& runtime = benchmark->GetRuntime();
        const auto customerId = NStat::ResolvePathId(runtime, benchmark->GetTablePath("customer"));
        UNIT_ASSERT_VALUES_EQUAL(benchmark->GetRowCount("customer"), actualRows);
        // Check that ANALYZE collected the complete customer population and all
        // five market segments, rather than relying on fallback selectivities.
        const auto columnStats = NStat::GetStatistics(runtime, customerId,
            NStat::EStatType::SIMPLE_COLUMN, {5}); // c_mktsegment in the CLI TPCH schema.
        UNIT_ASSERT_VALUES_EQUAL(columnStats.size(), 1);
        UNIT_ASSERT(columnStats[0].Success && columnStats[0].SimpleColumn.Data);
        UNIT_ASSERT_VALUES_EQUAL(columnStats[0].SimpleColumn.Data->GetCount(), actualRows);
        UNIT_ASSERT_VALUES_EQUAL(columnStats[0].SimpleColumn.Data->GetCountDistinct(), 5);

        // Probe the persisted sketch directly. The expected frequency comes
        // from the data, so this also detects a missing or stale column sketch.
        const auto sketches = NStat::GetStatistics(runtime, customerId,
            NStat::EStatType::COUNT_MIN_SKETCH, {5});
        UNIT_ASSERT_VALUES_EQUAL(sketches.size(), 1);
        UNIT_ASSERT(sketches[0].Success && sketches[0].CountMinSketch.CountMin);
        const TString segment = "HOUSEHOLD";
        UNIT_ASSERT_VALUES_EQUAL(
            sketches[0].CountMinSketch.CountMin->Probe(segment.data(), segment.size()), actualFilteredRows);

        const auto result = session.ExecuteQuery(R"(
            PRAGMA YqlSelect = 'force';
            SELECT c_custkey, c_name
            FROM `tpch/customer`
            WHERE c_mktsegment = 'HOUSEHOLD';
        )", NYdb::NQuery::TTxControl::NoTx(),
            NYdb::NQuery::TExecuteQuerySettings().ExecMode(NYdb::NQuery::EExecMode::Explain)).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT(result.GetStats() && result.GetStats()->GetPlan());
        const TString plan(*result.GetStats()->GetPlan());
        // Retain the complete Explain JSON as a test artifact for investigating
        // estimation failures, including details absent from SimplifiedPlan.
        GetOutputPath().MkDirs();
        const auto outputPath = GetOutputPath().Child("tpch_filter.explain.json");
        TOFStream output(outputPath);
        output << plan << Endl;
        output.Finish();
        Cerr << "Actual rows: " << actualRows << ", HOUSEHOLD rows: " << actualFilteredRows << Endl
            << "Explain JSON: " << outputPath << Endl << plan << Endl;

        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(plan, &json, true));
        const auto& simplified = json.GetMapSafe().at("SimplifiedPlan");
        const auto filter = FindPlanNodeByKv(simplified, "Name", "Filter");
        const auto read = FindPlanNodeByKv(simplified, "Table", "customer");
        UNIT_ASSERT_C(filter.IsDefined(), plan);
        UNIT_ASSERT_C(read.IsDefined(), plan);
        // Verify both base cardinality and filter selectivity. At scale 0.1 the
        // actual HOUSEHOLD count is 3009, distinguishable from the uniform 3000
        // estimate obtained using only the five distinct segment values.
        UNIT_ASSERT_VALUES_EQUAL_C(read.GetMapSafe().at("E-Rows").GetDoubleSafe(), actualRows, plan);
        UNIT_ASSERT_DOUBLES_EQUAL_C(filter.GetMapSafe().at("E-Rows").GetDoubleSafe(), actualFilteredRows, 0.01, plan);
        // The scan projects a subset of customer's eight columns. Its size
        // must use the captured byte_size even while selectivity uses the CMS.
        const auto readColumns = read.GetMapSafe().at("ReadColumns").GetArraySafe().size();
        UNIT_ASSERT_DOUBLES_EQUAL_C(read.GetMapSafe().at("E-Size").GetDoubleSafe(),
            benchmark->GetByteSize("customer") * (static_cast<double>(readColumns) / 8), 0.01, plan);
    }
}

} // namespace NKikimr::NKqp
