#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/ut/rbo/kqp_rbo_yql_workload_helpers.h>
#include <ydb/core/kqp/ut/rbo/statistics/helpers/kqp_analyzed_benchmarks.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/testing/common/env.h>

#include <util/stream/file.h>
#include <util/string/split.h>

#include <cmath>
#include <set>
#include <vector>

namespace NKikimr::NKqp {
namespace {

void CheckAnalyzePlan(const TString& plan, const TString& context) {
    const auto path = GetOutputPath().Child(context + ".analyze.json");
    path.Parent().MkDirs();
    TOFStream output(path);
    output << plan << Endl;
    output.Finish();

    NJson::TJsonValue json;
    NJson::ReadJsonTree(plan, &json, true);
    UNIT_ASSERT_C(json.Has("SimplifiedPlan"), context << ": missing simplified plan");
    UNIT_ASSERT_C(!plan.Contains("\"RuntimeStatsType\"") && !plan.Contains("\"OperatorId\"") && !plan.Contains("\"UseStageStats\""),
        context << ": internal operator metadata leaked into published plan");
    size_t joins = 0;
    size_t missingRows = 0;
    size_t missingSizes = 0;
    NJson::TJsonValue audit;
    audit["Query"] = context;
    audit["MissingRows"] = NJson::TJsonValue(NJson::JSON_ARRAY);
    audit["MissingSize"] = NJson::TJsonValue(NJson::JSON_ARRAY);
    const auto visit = [&](const auto& self, const NJson::TJsonValue& node) -> void {
        if (node.Has("Operators")) {
            for (const auto& op : node["Operators"].GetArraySafe()) {
                // Use structured fields: Map expressions can contain "Join".
                // BlockHash cross joins do have counters and must be checked.
                if (!op.Has("JoinKind") || (op["JoinKind"] == "Cross" &&
                    op.Has("JoinAlgo") && op["JoinAlgo"] != "BlockHash")) {
                    continue;
                }
                ++joins;
                const bool numericRows = op.Has("A-Rows") && (op["A-Rows"].IsInteger() ||
                    op["A-Rows"].IsUInteger() || op["A-Rows"].IsDouble());
                const bool validRows = numericRows && std::isfinite(op["A-Rows"].GetDoubleSafe()) &&
                    op["A-Rows"].GetDoubleSafe() >= 0;
                NJson::TJsonValue location;
                location["PlanNodeId"] = node["PlanNodeId"];
                location["Name"] = op["Name"];
                if (!validRows) {
                    ++missingRows;
                    audit["MissingRows"].AppendValue(location);
                }
                if (!op.Has("A-Size")) {
                    ++missingSizes;
                    audit["MissingSize"].AppendValue(location);
                }
                if (!validRows || !op.Has("A-Size")) {
                    Cerr << context << ": node " << node["PlanNodeId"].GetIntegerSafe()
                         << " " << op["Name"].GetStringSafe()
                         << ", missing/invalid A-Rows=" << !validRows
                         << ", missing A-Size=" << !op.Has("A-Size") << Endl;
                }
            }
        }
        if (node.Has("Plans")) {
            for (const auto& child : node["Plans"].GetArraySafe()) {
                self(self, child);
            }
        }
    };
    visit(visit, json["SimplifiedPlan"]);
    audit["Joins"] = joins;
    const auto auditPath = GetOutputPath().Child(context + ".join-stats.json");
    TOFStream auditOutput(auditPath);
    auditOutput << NJson::WriteJson(audit, true) << Endl;
    auditOutput.Finish();
    Cerr << "Analyze JSON: " << path << ", joins: " << joins
         << ", missing/invalid A-Rows: " << missingRows
         << ", missing A-Size: " << missingSizes << Endl;
    UNIT_ASSERT_VALUES_EQUAL_C(missingRows, 0,
        context << ": joins without valid A-Rows: " << NJson::WriteJson(audit["MissingRows"])
        << "; plan: " << path);
}

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

void RunAnalyzedBenchmark(const TString& name, NYqlTests::EBenchType type, ui32 queryCount,
                            bool columnStore, bool compareWithOldOptimizer, const std::set<ui32>& disabled = {}) {
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
    // Keep only the current query's results. Correctness mode compares both
    // optimizers; join-statistics mode validates NewRBO independently.
    TAnalyzedBenchmark database(name, 0.1, columnStore);
    TString errors;
    NJson::TJsonValue outcomes(NJson::JSON_ARRAY);
    for (ui32 id = 1; id <= queryCount; ++id) {
        if (!selected.empty() ? !selected.contains(id) : disabled.contains(id)) {
            continue;
        }
        const TString context = profile + "/q" + ToString(id);
        NJson::TJsonValue outcome;
        outcome["Query"] = id;
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
                    NYdb::NQuery::TExecuteQuerySettings().ClientTimeout(TDuration::Minutes(3))
                        .StatsMode(enableNewRbo ? NYdb::NQuery::EStatsMode::Full : NYdb::NQuery::EStatsMode::None))
                    .GetValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), executionContext << ": " << result.GetIssues().ToString());
                const auto& sets = result.GetResultSets();
                UNIT_ASSERT_C(!sets.empty(), executionContext << ": query returned no result sets");
                if (compareWithOldOptimizer) {
                    for (const auto& set : sets) {
                        UNIT_ASSERT_C(!set.Truncated(), executionContext << ": cannot compare a truncated result set");
                    }
                }
                if (enableNewRbo) {
                    UNIT_ASSERT_C(result.GetStats() && result.GetStats()->GetPlan(), executionContext << ": missing analyze plan");
                    CheckAnalyzePlan(TString(*result.GetStats()->GetPlan()), context);
                }
                return result;
            };
            if (compareWithOldOptimizer) {
                const auto expected = execute(false);
                const auto actual = execute(true);
                CompareResults(expected.GetResultSets(), actual.GetResultSets(), context);
            } else {
                execute(true);
            }
            outcome["Success"] = true;
        } catch (const std::exception& error) {
            outcome["Success"] = false;
            outcome["Error"] = error.what();
            errors += context + ": " + error.what() + "\n";
            Cerr << context << ": " << error.what() << Endl;
        }
        outcomes.AppendValue(std::move(outcome));
    }
    const auto outcomesPath = GetOutputPath().Child(profile + "/validation.json");
    outcomesPath.Parent().MkDirs();
    TOFStream outcomesOutput(outcomesPath);
    outcomesOutput << NJson::WriteJson(outcomes, true) << Endl;
    outcomesOutput.Finish();
    UNIT_ASSERT_C(errors.empty(), errors);
}

} // namespace

Y_UNIT_TEST_SUITE(KqpCorrectnessAnalyzedStatistics) {
    Y_UNIT_TEST_TWIN(TPCH, ColumnStore) {
        RunAnalyzedBenchmark("tpch", NYqlTests::TPCH, 22, ColumnStore, true);
    }

    Y_UNIT_TEST_TWIN(TPCDS, ColumnStore) {
        // Match the analyzed join-order suite's unsupported queries.
        RunAnalyzedBenchmark("tpcds", NYqlTests::TPCDS, 99, ColumnStore, true, {17, 44});
    }
}

Y_UNIT_TEST_SUITE(KqpJoinStatisticsAnalyzed) {
    Y_UNIT_TEST_TWIN(TPCH, ColumnStore) {
        RunAnalyzedBenchmark("tpch", NYqlTests::TPCH, 22, ColumnStore, false);
    }

    Y_UNIT_TEST_TWIN(TPCDS, ColumnStore) {
        // Exercise every query, including 17 and 44 excluded by correctness tests.
        RunAnalyzedBenchmark("tpcds", NYqlTests::TPCDS, 99, ColumnStore, false);
    }
}

} // namespace NKikimr::NKqp
