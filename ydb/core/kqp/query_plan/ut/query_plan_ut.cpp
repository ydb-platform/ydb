#include <ydb/core/kqp/query_plan/query_plan.h>
#include <ydb/core/protos/kqp_stats.pb.h>
#include <ydb/library/testlib/helpers.h>
#include <ydb/library/yql/dq/actors/protos/dq_stats.pb.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp {
namespace {

NJson::TJsonValue ParsePlan(const TString& text) {
    NJson::TJsonValue plan;
    UNIT_ASSERT(NJson::ReadJsonTree(text, &plan, true));
    return plan;
}

NJson::TJsonValue MakeRboPlan(const TString& stageGuid, i64 operatorId = 7) {
    auto plan = ParsePlan(R"({
        "SimplifiedPlan": {
            "Node Type": "TableFullScan",
            "Operators": [{"Name": "TableFullScan", "Table": "T"}]
        },
        "Plans": [{
            "Node Type": "TableFullScan",
            "Operators": [{"Name": "TableFullScan"}]
        }]
    })");
    plan["Plans"][0]["StageGuid"] = stageGuid;
    plan["Plans"][0]["Operators"][0]["OperatorId"] = operatorId;
    plan["SimplifiedPlan"]["Operators"][0]["OperatorId"] = operatorId;
    return plan;
}

const TString LegacyTxPlan = R"({
    "Node Type": "Phase",
    "Plans": [{
        "Node Type": "TableFullScan", "PlanNodeId": 1, "StageGuid": "legacy",
        "Operators": [{"Name": "TableFullScan", "Table": "LegacyTable", "Inputs": []}],
        "Stats": {"Tasks": 2, "Table": [{"Path": "LegacyTable", "ReadRows": {"Sum": 3}}]}
    }]
})";

} // namespace

Y_UNIT_TEST_SUITE(KqpQueryPlanSerialization) {
    Y_UNIT_TEST(RboExplainAndAnalyzePreserveTableSummary) {
        auto txPlan = MakeRboPlan("stage");
        const auto tables = ParsePlan(R"json([
            {"name":"/Root/T", "reads":[{"type":"Scan", "scan_by":["key (1, +inf)"],
                "columns":["key", "value"], "limit":"5", "reverse":true}],
                "writes":[{"type":"MultiUpsert", "columns":["key", "value"]}]},
            {"name":"/Root/T/index/indexImplTable", "writes":[{"type":"MultiUpsert"}]}
        ])json");
        txPlan["tables"] = tables;
        UNIT_ASSERT_VALUES_EQUAL(ParsePlan(SerializeRBOExplainPlan(txPlan))["tables"], tables);

        NKqpProto::TKqpStatsQuery queryStats;
        const TString tx = NJson::WriteJson(txPlan, false);
        // Each transaction carries the same whole-query summary; do not duplicate it.
        UNIT_ASSERT_VALUES_EQUAL(ParsePlan(SerializeRBOAnalyzePlan({tx, tx}, queryStats))["tables"], tables);
    }

    Y_UNIT_TEST(RboPlansIncludeEmptyTableSummary) {
        auto txPlan = MakeRboPlan("stage");
        const NJson::TJsonValue tables(NJson::JSON_ARRAY);
        UNIT_ASSERT_VALUES_EQUAL(ParsePlan(SerializeRBOExplainPlan(txPlan))["tables"], tables);
        txPlan["tables"] = tables;
        NKqpProto::TKqpStatsQuery queryStats;
        UNIT_ASSERT_VALUES_EQUAL(ParsePlan(SerializeRBOAnalyzePlan({NJson::WriteJson(txPlan, false)}, queryStats))["tables"], tables);
    }

    Y_UNIT_TEST(RboAnalyzePopulatesEverySharedJoinOccurrence) {
        const TString txPlan = R"({
            "SimplifiedPlan": {
                "Operators": [{"OperatorId": 1, "Name": "Aggregate"}],
                "Plans": [
                    {"Operators": [{"OperatorId": 7, "Name": "Join"}]},
                    {"Operators": [{"OperatorId": 7, "Name": "Join"}]}
                ]
            },
            "Plans": [{
                "StageGuid": "stage",
                "Operators": [
                    {"OperatorId": 1, "Name": "Aggregate"},
                    {"OperatorId": 7, "Name": "Join", "RuntimeStatsType": "Join"}
                ],
                "Stats": {
                    "OutputRows": {"Sum": 3}, "CpuTimeUs": {"Max": 7000},
                    "Operator": [{"Type": "Join", "Id": "0", "Rows": {"Sum": 17}, "Bytes": {"Sum": 170}}]
                }
            }]
        })";
        NKqpProto::TKqpStatsQuery queryStats;
        const auto plan = ParsePlan(SerializeRBOAnalyzePlan({txPlan}, queryStats));
        const auto& simplified = plan["SimplifiedPlan"];
        for (const auto& branch : simplified["Plans"].GetArraySafe()) {
            const auto& join = branch["Operators"][0];
            UNIT_ASSERT_VALUES_EQUAL(join["A-Rows"].GetDoubleSafe(), 17);
            UNIT_ASSERT_VALUES_EQUAL(join["A-Size"].GetDoubleSafe(), 170);
            UNIT_ASSERT(!join.Has("A-SelfCpu"));
            UNIT_ASSERT(!join.Has("A-Cpu"));
        }
        const auto& aggregate = simplified["Operators"][0];
        UNIT_ASSERT_VALUES_EQUAL(aggregate["A-Rows"].GetDoubleSafe(), 3);
        UNIT_ASSERT_VALUES_EQUAL(aggregate["A-Cpu"].GetDoubleSafe(), 7);
    }

    Y_UNIT_TEST_TWIN(RboAnalyzePreservesZeroJoinRows, RuntimeCounter) {
        const TString txPlan = R"({
            "SimplifiedPlan": {"Operators": [{"OperatorId": 7, "Name": "Join"}]},
            "Plans": [{
                "StageGuid": "stage", "PlanNodeId": 1,
                "Operators": [{"OperatorId": 7, "Name": "Join", "RuntimeStatsType": "Join"}]
            }]
        })";
        NYql::NDqProto::TDqExecutionStats execution;
        auto* stage = execution.AddStages();
        stage->SetStageGuid("stage");
        stage->SetStageId(1);
        stage->MutableOutputRows()->SetSum(RuntimeCounter ? 17 : 0);
        if (RuntimeCounter) {
            // The zero operator counter takes precedence over stage output.
            (*stage->MutableOperatorJoin())["0"].MutableRows()->SetSum(0);
        }
        NKqpProto::TKqpStatsQuery queryStats;
        const auto plan = ParsePlan(SerializeRBOAnalyzePlan(
            {AddExecStatsToTxPlan(txPlan, execution, true)}, queryStats));
        const auto& join = plan["SimplifiedPlan"]["Operators"][0];
        UNIT_ASSERT_VALUES_EQUAL(join["A-Rows"].GetDoubleSafe(), 0);
        UNIT_ASSERT(!join.Has("A-Size"));

        // Do not invent a zero when neither source supplied a row metric.
        stage->ClearOutputRows();
        stage->ClearOperatorJoin();
        const auto missing = ParsePlan(SerializeRBOAnalyzePlan(
            {AddExecStatsToTxPlan(txPlan, execution, true)}, queryStats));
        UNIT_ASSERT(!missing["SimplifiedPlan"]["Operators"][0].Has("A-Rows"));
    }

    Y_UNIT_TEST(OptimizerStatsArePreserved) {
        NJson::TJsonValue optimizerStats(NJson::JSON_MAP);
        optimizerStats["JoinsCount"] = ui64(3);
        optimizerStats["CBOTreesOptimized"] = ui64(2);

        const auto plan = ParsePlan(SerializeTxPlans({}, "", "", optimizerStats));
        UNIT_ASSERT_VALUES_EQUAL(plan["meta"]["type"].GetStringSafe(), "query");
        UNIT_ASSERT(plan["SimplifiedPlan"]["OptimizerStats"] == optimizerStats);

        const auto planWithoutStats = ParsePlan(SerializeTxPlans({}));
        UNIT_ASSERT(!planWithoutStats["SimplifiedPlan"].Has("OptimizerStats"));
    }

    Y_UNIT_TEST(LegacyAnalyzeIncludesQueryAndPoolStats) {
        NKqpProto::TKqpStatsQuery stats;
        stats.SetWorkerCpuTimeUs(123);
        stats.SetDurationUs(456);
        stats.SetQueuedTimeUs(78);
        stats.MutableCompilation()->SetFromCache(true);
        stats.MutableCompilation()->SetDurationUs(90);

        const auto plan = ParsePlan(SerializeAnalyzePlan(stats, "pool"));
        const auto& queryStats = plan["Plan"]["Stats"];
        UNIT_ASSERT_VALUES_EQUAL(queryStats["ProcessCpuTimeUs"].GetIntegerRobust(), 123);
        UNIT_ASSERT_VALUES_EQUAL(queryStats["TotalDurationUs"].GetIntegerRobust(), 456);
        UNIT_ASSERT_VALUES_EQUAL(queryStats["QueuedTimeUs"].GetIntegerRobust(), 78);
        UNIT_ASSERT_VALUES_EQUAL(queryStats["ResourcePoolId"].GetStringSafe(), "pool");
        UNIT_ASSERT(queryStats["Compilation"]["FromCache"].GetBooleanSafe());
        UNIT_ASSERT_VALUES_EQUAL(queryStats["Compilation"]["DurationUs"].GetIntegerRobust(), 90);
    }

    Y_UNIT_TEST(RboAnalyzeQueryStatsMatchLegacy) {
        for (const bool compiled : {false, true}) {
            for (const TString& pool : {TString(), TString("pool")}) {
                NKqpProto::TKqpStatsQuery stats;
                stats.SetWorkerCpuTimeUs(123);
                stats.SetDurationUs(456);
                stats.SetQueuedTimeUs(78);
                if (compiled) {
                    stats.MutableCompilation()->SetFromCache(true);
                    stats.MutableCompilation()->SetDurationUs(90);
                    stats.MutableCompilation()->SetCpuTimeUs(45);
                }
                const auto legacy = ParsePlan(SerializeAnalyzePlan(stats, pool));
                stats.AddExecutions()->AddTxPlansWithStats(NJson::WriteJson(MakeRboPlan("stage"), false));
                const auto rbo = ParsePlan(SerializeAnalyzePlan(stats, pool));
                UNIT_ASSERT_VALUES_EQUAL(rbo["Plan"]["Stats"], legacy["Plan"]["Stats"]);
                const auto& queryStats = rbo["Plan"]["Stats"];
                UNIT_ASSERT_VALUES_EQUAL(queryStats.Has("Compilation"), compiled);
                UNIT_ASSERT_VALUES_EQUAL(queryStats.Has("ResourcePoolId"), !pool.empty());
                UNIT_ASSERT_VALUES_EQUAL(queryStats.Has("QueuedTimeUs"), !pool.empty());
                if (compiled) {
                    UNIT_ASSERT_VALUES_EQUAL(queryStats["Compilation"]["CpuTimeUs"].GetIntegerSafe(), 45);
                }
            }
        }
    }

    Y_UNIT_TEST(RboExplainIncludesOnlyAvailableQueryStats) {
        const auto txPlan = MakeRboPlan("stage");
        UNIT_ASSERT(!ParsePlan(SerializeRBOExplainPlan(txPlan))["Plan"].Has("Stats"));
        NJson::TJsonValue queryStats;
        queryStats["ResourcePoolId"] = "pool";
        const auto plan = ParsePlan(SerializeRBOExplainPlan(txPlan, queryStats));
        UNIT_ASSERT_VALUES_EQUAL(plan["Plan"]["Stats"], queryStats);
        UNIT_ASSERT(!plan["Plan"]["Plans"][0].Has("Stats"));
    }

    Y_UNIT_TEST(RboAnalyzeIncludesOperatorStats) {
        NKqpProto::TKqpStatsQuery stats;
        stats.AddExecutions()->AddTxPlansWithStats(R"({
            "SimplifiedPlan": {
                "Node Type": "Stage",
                "Operators": [{"OperatorId": 7, "Name": "TableFullScan", "Table": "T"}]
            },
            "Plans": [{
                "Node Type": "Stage",
                "StageGuid": "stage",
                "Operators": [{"OperatorId": 7, "Name": "TableFullScan"}],
                "Stats": {
                    "Tasks": 1,
                    "CpuTimeUs": {"Max": 2000},
                    "Table": [{"Path": "T", "ReadRows": {"Sum": 3}, "ReadBytes": {"Sum": 120}}]
                }
            }]
        })");

        stats.SetWorkerCpuTimeUs(123);
        stats.SetDurationUs(456);
        stats.SetQueuedTimeUs(78);
        const auto plan = ParsePlan(SerializeAnalyzePlan(stats, "pool"));
        UNIT_ASSERT_VALUES_EQUAL(plan["Plan"]["Stats"]["ProcessCpuTimeUs"].GetIntegerSafe(), 123);
        UNIT_ASSERT_VALUES_EQUAL(plan["Plan"]["Stats"]["TotalDurationUs"].GetIntegerSafe(), 456);
        UNIT_ASSERT_VALUES_EQUAL(plan["Plan"]["Stats"]["ResourcePoolId"].GetStringSafe(), "pool");
        UNIT_ASSERT_VALUES_EQUAL(plan["Plan"]["Plans"][0]["Stats"]["Tasks"].GetIntegerSafe(), 1);
        UNIT_ASSERT_VALUES_EQUAL(plan["Plan"]["Plans"][0]["Stats"]["CpuTimeUs"]["Max"].GetIntegerSafe(), 2000);
        const auto& op = plan["SimplifiedPlan"]["Operators"][0];
        UNIT_ASSERT_VALUES_EQUAL(op["A-Rows"].GetDoubleSafe(), 3.0);
        UNIT_ASSERT_VALUES_EQUAL(op["A-Size"].GetDoubleSafe(), 120.0);
        UNIT_ASSERT_VALUES_EQUAL(op["A-Cpu"].GetDoubleSafe(), 2.0);
        UNIT_ASSERT(!op.Has("OperatorId"));
        UNIT_ASSERT(!plan["Plan"]["Plans"][0]["Operators"][0].Has("OperatorId"));
    }

    Y_UNIT_TEST(AnalyzeUsesLegacyTransactionPlanAfterFallback) {
        NKqpProto::TKqpStatsQuery stats;
        stats.AddExecutions()->AddTxPlansWithStats(""); // A schema/commit-only part.
        stats.AddExecutions()->AddTxPlansWithStats(LegacyTxPlan);
        stats.AddExecutions()->AddTxPlansWithStats("");

        const auto plan = ParsePlan(SerializeAnalyzePlan(stats));
        UNIT_ASSERT_VALUES_EQUAL(plan["Plan"]["Plans"].GetArraySafe().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(plan["Plan"]["Plans"][0]["Stats"]["Tasks"].GetIntegerSafe(), 2);
        const auto& node = plan["SimplifiedPlan"]["Plans"][0]["Operators"][0];
        UNIT_ASSERT_VALUES_EQUAL(node["Table"].GetStringSafe(), "LegacyTable");
        UNIT_ASSERT_VALUES_EQUAL(node["A-Rows"].GetDoubleSafe(), 3);
    }

    Y_UNIT_TEST(AnalyzeMergesTransactionsOfTheSameRboQuery) {
        auto first = MakeRboPlan("first");
        const auto secondStage = MakeRboPlan("second", 8);
        first["Plans"][0]["Plans"].AppendValue(secondStage["Plans"][0]);
        first["SimplifiedPlan"]["Plans"].AppendValue(secondStage["SimplifiedPlan"]);
        auto second = first;
        first["Plans"][0]["Stats"]["Tasks"] = 2;
        second["Plans"][0]["Plans"][0]["Stats"]["Tasks"] = 3;

        NKqpProto::TKqpStatsQuery stats;
        stats.AddExecutions()->AddTxPlansWithStats("");
        stats.AddExecutions()->AddTxPlansWithStats(first.GetStringRobust());
        stats.AddExecutions()->AddTxPlansWithStats("");
        stats.AddExecutions()->AddTxPlansWithStats(second.GetStringRobust());
        const auto plan = ParsePlan(SerializeAnalyzePlan(stats));
        UNIT_ASSERT_VALUES_EQUAL(plan["Plan"]["Plans"].GetArraySafe().size(), 1);
        const auto& stage = plan["Plan"]["Plans"][0];
        UNIT_ASSERT_VALUES_EQUAL(stage["Stats"]["Tasks"].GetIntegerSafe(), 2);
        UNIT_ASSERT_VALUES_EQUAL(stage["Plans"][0]["Stats"]["Tasks"].GetIntegerSafe(), 3);
    }

    Y_UNIT_TEST_TWIN(ScriptPreservesSerializedSimplifiedPlans, NewRbo) {
        NKqpProto::TKqpStatsQuery stats;
        auto rbo = MakeRboPlan("rbo");
        rbo["SimplifiedPlan"]["OptimizerStats"]["CBOTreesOptimized"] = 1;
        stats.AddExecutions()->AddTxPlansWithStats(rbo.GetStringRobust());
        const auto queryPlan = NewRbo ? SerializeAnalyzePlan(stats) : SerializeTxPlans({LegacyTxPlan});
        const auto script = ParsePlan(SerializeScriptPlan({queryPlan}));
        const auto& queries = script["queries"].GetArraySafe();
        UNIT_ASSERT_VALUES_EQUAL(queries.size(), 1);
        UNIT_ASSERT(queries[0]["SimplifiedPlan"] == ParsePlan(queryPlan)["SimplifiedPlan"]);
    }

    Y_UNIT_TEST(ScriptBuildsMissingLegacySimplifiedPlan) {
        auto legacyPlan = ParsePlan(SerializeTxPlans({LegacyTxPlan}));
        const auto expected = legacyPlan["SimplifiedPlan"];
        legacyPlan.EraseValue("SimplifiedPlan");
        const auto script = ParsePlan(SerializeScriptPlan({legacyPlan.GetStringRobust()}));
        UNIT_ASSERT(script["queries"][0]["SimplifiedPlan"] == expected);
    }
}

} // namespace NKikimr::NKqp
