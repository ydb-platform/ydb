#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/kqp/compute_actor/kqp_task_graph.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp {
namespace {

TVector<TString> ExecuteWorkload(bool enabled, bool observeOnly, ui32 nodes, bool useWorkerPool = true) {
    TKikimrSettings settings;
    settings.SetNodeCount(nodes);
    auto* config = settings.AppConfig.MutableTableServiceConfig();
    config->SetDqChannelVersion(2);
    auto* scheduling = config->MutableDqChannelConfig();
    scheduling->SetEnableTaskScheduling(enabled);
    scheduling->SetTaskSchedulerObserveOnly(observeOnly);
    scheduling->SetTaskSchedulerUseWorkerPool(useWorkerPool);
    scheduling->SetTaskSchedulerShards(2);
    scheduling->SetTaskSchedulerMaxInFlightPerShard(1);
    scheduling->SetTaskSchedulerMaxRowsPerRun(2);

    TKikimrRunner kikimr(settings);
    auto db = kikimr.GetQueryClient();
    TVector<TString> results;
    for (const TString& query : {TString(R"(
            SELECT Key, SUM(Key) AS Total FROM `/Root/EightShard`
            GROUP BY Key ORDER BY Key;
        )"),
                                 TString(R"(
            $input = SELECT Key, Data FROM `/Root/EightShard`;
            SELECT a.Key, COUNT(*) AS Matches
            FROM $input AS a INNER JOIN $input AS b ON a.Key = b.Key
            WHERE b.Key >= 100
            GROUP BY a.Key ORDER BY a.Key;
        )"),
                                 TString(R"(
            SELECT Key FROM `/Root/EightShard`
            WHERE Length(Text) > 100000 ORDER BY Key;
        )")}) {
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx(),
                                      NYdb::NQuery::TExecuteQuerySettings().ClientTimeout(TDuration::Seconds(30)))
                          .ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        results.push_back(FormatResultSetYson(result.GetResultSet(0)));
    }
    if (enabled) {
        ui64 runs = 0;
        ui64 outputRows = 0;
        ui64 budgetYields = 0;
        ui64 workerRuns = 0;
        for (ui32 node = 0; node < nodes; ++node) {
            TKqpCounters counters(kikimr.GetTestServer().GetRuntime()->GetAppData(node).Counters);
            auto group = counters.GetChannelCounters()->GetSubgroup("component", "TaskSchedulerPoC");
            runs += group->GetCounter("Runs", true)->Val();
            outputRows += group->GetCounter("OutputRows", true)->Val();
            budgetYields += group->GetCounter("BudgetYields", true)->Val();
            workerRuns += counters.GetChannelCounters()->GetSubgroup("component", "TaskSchedulerWorkers")
                ->GetCounter("WorkerRuns", true)->Val();
        }
        UNIT_ASSERT_C(runs > 0, "The workload did not execute instrumented tasks");
        UNIT_ASSERT_C(outputRows > 0, "No output progress recorded");
        if (!observeOnly) {
            UNIT_ASSERT_C(budgetYields > 0, "The row budget was not exercised");
            if (useWorkerPool) {
                UNIT_ASSERT_C(workerRuns > 0, "The workload did not execute on anonymous workers");
            }
        }
    }
    return results;
}

} // namespace

Y_UNIT_TEST_SUITE(KqpTaskSchedulingPoc) {
    Y_UNIT_TEST(SchedulingGraphSerializationPreservesRemoteAndResultEdges) {
        const NYql::NDq::TQueryTaskGraph graph(
            {{1, 10, 1, 0, 1, true}, {2, 20, 2, 1, 1}},
            {{10, 1, 2, 0, 0, 3}, {11, 2, 0, 0, 0, 1}});
        NKikimrKqp::TTaskSchedulingGraph message;
        SerializeSchedulingGraph(graph, message);
        auto restored = DeserializeSchedulingGraph(message);
        UNIT_ASSERT_VALUES_EQUAL(restored->Tasks.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(restored->Channels.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(restored->FindTask(2)->PlannedNodeId, 2);
        UNIT_ASSERT_VALUES_EQUAL(restored->Channels[0].RoutingKind, 3);
        UNIT_ASSERT(restored->GetInputs(2) == TVector<ui64>({10}));
        UNIT_ASSERT(restored->IsResultTask(2));
        UNIT_ASSERT(!restored->IsResultTask(1));
    }

    Y_UNIT_TEST(ProducesSameResultsAsLegacyAndObservationMode) {
        const auto baseline = ExecuteWorkload(false, false, 1);
        UNIT_ASSERT_VALUES_EQUAL(ExecuteWorkload(true, true, 1), baseline);
        UNIT_ASSERT_VALUES_EQUAL(ExecuteWorkload(true, false, 1, false), baseline);
        UNIT_ASSERT_VALUES_EQUAL(ExecuteWorkload(true, false, 1), baseline);
    }

    Y_UNIT_TEST(WorksAcrossTwoNodes) {
        const auto baseline = ExecuteWorkload(false, false, 2);
        UNIT_ASSERT_VALUES_EQUAL(ExecuteWorkload(true, false, 2), baseline);
    }
}

} // namespace NKikimr::NKqp
