#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/ut/rbo/kqp_rbo_yql_workload_helpers.h>
#include <ydb/core/kqp/ut/rbo/statistics/helpers/kqp_analyzed_benchmarks.h>

#include <library/cpp/testing/common/env.h>

#include <util/string/split.h>

#include <set>
#include <vector>

namespace NKikimr::NKqp {
namespace {

void CompareResults(const std::vector<NYdb::TResultSet>& expected,
                    const std::vector<NYdb::TResultSet>& actual, const TString& context) {
    UNIT_ASSERT_VALUES_EQUAL_C(actual.size(), expected.size(), context);
    for (size_t index = 0; index < actual.size(); ++index) {
        const TString resultContext = context + ": result set " + ToString(index);
        UNIT_ASSERT_C(actual[index].GetColumnsMeta() == expected[index].GetColumnsMeta(),
            resultContext << ": column names or types differ");
        CompareYson(NYdb::FormatResultSetYson(expected[index]), NYdb::FormatResultSetYson(actual[index]), resultContext);
    }
}

void RunCorrectnessBenchmark(const TString& name, NYqlTests::EBenchType type, ui32 queryCount,
                            bool columnStore, const std::set<ui32>& disabled = {}) {
    std::set<ui32> selected;
    const TString selection = GetTestParam("CORRECTNESS_QUERIES");
    if (!selection.empty()) {
        TVector<TString> ids;
        Split(selection, ",", ids);
        for (const auto& value : ids) {
            ui32 id = 0;
            UNIT_ASSERT_C(TryFromString(value, id) && id >= 1 && id <= queryCount,
                "Invalid " << name << " query ID: " << value);
            selected.insert(id);
        }
    }

    const TString profile = name + "_sf0.1" + (columnStore ? "_column" : "_row");
    // Both optimizers use the same data and column statistics. Keep only the
    // current query's results, and disable fallback in the shared fixture.
    TAnalyzedBenchmark database(name, 0.1, columnStore);
    TString errors;
    for (ui32 id = 1; id <= queryCount; ++id) {
        if (!selected.empty() ? !selected.contains(id) : disabled.contains(id)) {
            continue;
        }
        const TString context = profile + "/q" + ToString(id);
        try {
            const auto execute = [&](bool enableNewRbo) {
                const TString optimizer = enableNewRbo ? "NewRBO" : "old optimizer";
                const TString executionContext = context + " (" + optimizer + ")";
                Cerr << "Executing " << executionContext << Endl;
                database.SetEnableNewRbo(enableNewRbo);
                const TString query = R"(
                    $to_decimal = ($x) -> { return cast($x as Decimal(12, 2)); };
                    $to_decimal_max_precision = ($x) -> { return cast($x as Decimal(35, 2)); };
                    $round = ($x,$y) -> {return $x;};
                )" + NYqlTests::ReadBenchmarkQuery(type, id, database.GetTablesPath());
                auto result = database.GetSession().ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(),
                    NYdb::NQuery::TExecuteQuerySettings().ClientTimeout(TDuration::Minutes(3)))
                    .GetValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), executionContext << ": " << result.GetIssues().ToString());
                const auto& sets = result.GetResultSets();
                UNIT_ASSERT_C(!sets.empty(), executionContext << ": query returned no result sets");
                for (const auto& set : sets) {
                    UNIT_ASSERT_C(!set.Truncated(), executionContext << ": cannot compare a truncated result set");
                }
                return result;
            };
            const auto expected = execute(false);
            const auto actual = execute(true);
            CompareResults(expected.GetResultSets(), actual.GetResultSets(), context);
        } catch (const std::exception& error) {
            errors += context + ": " + error.what() + "\n";
            Cerr << context << ": " << error.what() << Endl;
        }
    }
    UNIT_ASSERT_C(errors.empty(), errors);
}

} // namespace

Y_UNIT_TEST_SUITE(KqpCorrectnessAnalyzedStatistics) {
    Y_UNIT_TEST_TWIN(TPCH, ColumnStore) {
        RunCorrectnessBenchmark("tpch", NYqlTests::TPCH, 22, ColumnStore);
    }

    Y_UNIT_TEST_TWIN(TPCDS, ColumnStore) {
        // Match the analyzed join-order suite's unsupported queries.
        RunCorrectnessBenchmark("tpcds", NYqlTests::TPCDS, 99, ColumnStore, {17, 44});
    }
}

} // namespace NKikimr::NKqp
