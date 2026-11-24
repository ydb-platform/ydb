#include <ydb/core/kqp/query_plan/query_plan.h>
#include <ydb/core/protos/kqp_stats.pb.h>
#include <ydb/library/testlib/helpers.h>
#include <ydb/library/yql/dq/actors/protos/dq_stats.pb.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp {
namespace {

NJson::TJsonValue ParsePlan(const TString& text) {
    NJson::TJsonValue plan;
    UNIT_ASSERT(NJson::ReadJsonTree(text, &plan, true));
    return plan;
}

} // namespace

Y_UNIT_TEST_SUITE(KqpQueryPlanSerialization) {
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

        const auto plan = ParsePlan(SerializeAnalyzePlan(stats, false, "pool"));
        const auto& queryStats = plan["Plan"]["Stats"];
        UNIT_ASSERT_VALUES_EQUAL(queryStats["ProcessCpuTimeUs"].GetIntegerRobust(), 123);
        UNIT_ASSERT_VALUES_EQUAL(queryStats["TotalDurationUs"].GetIntegerRobust(), 456);
        UNIT_ASSERT_VALUES_EQUAL(queryStats["QueuedTimeUs"].GetIntegerRobust(), 78);
        UNIT_ASSERT_VALUES_EQUAL(queryStats["ResourcePoolId"].GetStringSafe(), "pool");
        UNIT_ASSERT(queryStats["Compilation"]["FromCache"].GetBooleanSafe());
        UNIT_ASSERT_VALUES_EQUAL(queryStats["Compilation"]["DurationUs"].GetIntegerRobust(), 90);
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

        const auto plan = ParsePlan(SerializeAnalyzePlan(stats, true));
        const auto& op = plan["SimplifiedPlan"]["Operators"][0];
        UNIT_ASSERT_VALUES_EQUAL(op["A-Rows"].GetDoubleSafe(), 3.0);
        UNIT_ASSERT_VALUES_EQUAL(op["A-Size"].GetDoubleSafe(), 120.0);
        UNIT_ASSERT_VALUES_EQUAL(op["A-Cpu"].GetDoubleSafe(), 2.0);
        UNIT_ASSERT(!op.Has("OperatorId"));
        UNIT_ASSERT(!plan["Plan"]["Plans"][0]["Operators"][0].Has("OperatorId"));
    }
}

} // namespace NKikimr::NKqp
