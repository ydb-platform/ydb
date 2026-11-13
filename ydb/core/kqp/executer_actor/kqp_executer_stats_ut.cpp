#include <ydb/core/kqp/executer_actor/kqp_executer_stats.h>
#include <ydb/core/kqp/query_data/kqp_prepared_query.h>
#include <ydb/core/testlib/actor_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp {

namespace {

NYql::NDqProto::TDqTaskStats TaskStats(ui64 taskId) {
    NYql::NDqProto::TDqTaskStats stats;
    stats.SetTaskId(taskId);
    return stats;
}

void Report(TStageExecutionStats& stage, ui32 nodeId, ui64 taskId, NYql::NDqProto::EComputeState state) {
    stage.UpdateStats(nodeId, TaskStats(taskId), state, 0, 0, 0);
}

TStageExecutionStats PreparedStage(ui32 taskCount) {
    TStageExecutionStats stage;
    for (ui64 taskId = 1; taskId <= taskCount; ++taskId) {
        stage.Task2Index.emplace(taskId, stage.Task2Index.size());
    }
    stage.TaskCount = (taskCount + 3) & ~3;
    stage.Resize(stage.TaskCount);
    return stage;
}

} // namespace

Y_UNIT_TEST_SUITE(KqpExecuterStats) {
    Y_UNIT_TEST(ColumnShardReportsReplaceTaskContribution) {
        auto stage = PreparedStage(2);
        auto first = TaskStats(1);
        NKqpProto::TKqpTaskExtraStats extra;
        auto* report = extra.AddColumnShardStats();
        report->MutableIdentity()->SetTabletId(42);
        report->MutableIdentity()->SetScanAttemptId("first-attempt");
        auto* operation = report->AddOperations();
        operation->SetKey("Filter");
        operation->SetName("Filter");
        operation->SetCallsCount(1);
        operation->MutableStatistics()->MutableOutputRows()->SetValue(0);
        first.MutableExtra()->PackFrom(extra);
        stage.UpdateStats(1, first, NYql::NDqProto::COMPUTE_STATE_EXECUTING, 0, 0, 0);
        stage.UpdateStats(1, first, NYql::NDqProto::COMPUTE_STATE_FINISHED, 0, 0, 0);
        UNIT_ASSERT_VALUES_EQUAL(stage.ColumnShardStats.at(1).size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(stage.ColumnShardStats.at(1).Get(0).GetOperations(0).GetCallsCount(), 1);
        UNIT_ASSERT(stage.ColumnShardStats.at(1).Get(0).GetOperations(0).GetStatistics().GetOutputRows().HasValue());

        extra.MutableColumnShardStats(0)->MutableOperations(0)->SetCallsCount(3);
        first.MutableExtra()->PackFrom(extra);
        stage.UpdateStats(1, first, NYql::NDqProto::COMPUTE_STATE_FINISHED, 0, 0, 0);
        UNIT_ASSERT_VALUES_EQUAL(stage.ColumnShardStats.at(1).size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(stage.ColumnShardStats.at(1).Get(0).GetOperations(0).GetCallsCount(), 3);
        // A later task update without a CS payload must not erase the final scan report.
        first.ClearExtra();
        stage.UpdateStats(1, first, NYql::NDqProto::COMPUTE_STATE_FINISHED, 0, 0, 0);
        UNIT_ASSERT_VALUES_EQUAL(stage.ColumnShardStats.at(1).Get(0).GetOperations(0).GetCallsCount(), 3);

        auto second = TaskStats(2);
        extra.MutableColumnShardStats(0)->MutableIdentity()->SetTabletId(43);
        extra.MutableColumnShardStats(0)->MutableIdentity()->SetScanAttemptId("second-attempt");
        second.MutableExtra()->PackFrom(extra);
        stage.UpdateStats(2, second, NYql::NDqProto::COMPUTE_STATE_FINISHED, 0, 0, 0);
        UNIT_ASSERT_VALUES_EQUAL(stage.ColumnShardStats.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(stage.ColumnShardStats.at(1).Get(0).GetIdentity().GetTabletId(), 42);
        UNIT_ASSERT_VALUES_EQUAL(stage.ColumnShardStats.at(2).Get(0).GetIdentity().GetTabletId(), 43);
    }

    Y_UNIT_TEST(ColumnShardReportsExportedUnderTheirStage) {
        TActorSystemStub actorSystem;
        NKqpProto::TKqpPhyTx physicalTx;
        physicalTx.AddStages()->SetStageGuid("first-stage");
        physicalTx.AddStages()->SetStageGuid("second-stage");
        const auto body = std::make_shared<TKqpPhyTxHolder>(nullptr, &physicalTx, nullptr, nullptr);
        const IKqpGateway::TPhysicalTxData tx(body, nullptr);
        const TVector<IKqpGateway::TPhysicalTxData> transactions;
        const NKikimrConfig::TTableServiceConfig::TAggregationConfig aggregation;
        TKqpTasksGraph graph("/Root", transactions, nullptr, {}, aggregation, nullptr, {}, nullptr, false);
        for (ui32 index = 0; index < 2; ++index) {
            const NYql::NDq::TStageId stageId(0, index);
            graph.AddStageInfo(TStageInfo(stageId, 0, 0, TStageInfoMeta(tx)));
            graph.AddTask(graph.GetStageInfo(stageId));
        }

        for (bool withNodeStats : {false, true}) {
            NYql::NDqProto::TDqExecutionStats result;
            TQueryExecutionStats stats(Ydb::Table::QueryStatsCollection::STATS_COLLECTION_PROFILE, &graph, &result, 0);
            stats.Prepare();
            if (withNodeStats) {
                stats.AddNodeShardsCount(0, 11, 5);
            }
            for (const auto& task : graph.GetTasks()) {
                auto taskStats = TaskStats(task.Id);
                NKqpProto::TKqpTaskExtraStats extra;
                for (ui64 tabletId : {42u, 43u}) {
                    auto* report = extra.AddColumnShardStats();
                    report->MutableIdentity()->SetTabletId(tabletId + task.StageId.StageId * 100);
                    report->MutableIdentity()->SetScanAttemptId(ToString(tabletId));
                    auto* operation = report->AddOperations();
                    operation->SetName("Fetching");
                    operation->SetCallsCount(3);
                    operation->MutableUnfinished()->SetCallsCount(1);
                }
                taskStats.MutableExtra()->PackFrom(extra);
                auto& stage = stats.StageStats.at(task.StageId);
                stage.UpdateStats(1, taskStats, NYql::NDqProto::COMPUTE_STATE_FINISHED, 0, 0, 0);
                stage.UpdateStats(1, taskStats, NYql::NDqProto::COMPUTE_STATE_FINISHED, 0, 0, 0);
            }
            stats.ExportExecStats(result);
            UNIT_ASSERT_VALUES_EQUAL(result.StagesSize(), 2);
            for (const auto& stage : result.GetStages()) {
                NKqpProto::TKqpStageExtraStats extra;
                UNIT_ASSERT(stage.GetExtra().UnpackTo(&extra));
                const bool hasNodeStats = stage.GetStageId() == 0 && withNodeStats;
                UNIT_ASSERT_VALUES_EQUAL(extra.NodeStatsSize(), hasNodeStats ? 1 : 0);
                if (hasNodeStats) {
                    UNIT_ASSERT_VALUES_EQUAL(extra.GetNodeStats(0).GetNodeId(), 11);
                    UNIT_ASSERT_VALUES_EQUAL(extra.GetNodeStats(0).GetShardsCount(), 5);
                }
                UNIT_ASSERT_VALUES_EQUAL(extra.ColumnShardStatsSize(), 2);
                for (ui32 index = 0; index < 2; ++index) {
                    const auto& report = extra.GetColumnShardStats(index);
                    UNIT_ASSERT_VALUES_EQUAL(report.GetIdentity().GetTabletId(), 42 + index + stage.GetStageId() * 100);
                    UNIT_ASSERT_VALUES_EQUAL(report.GetOperations(0).GetCallsCount(), 3);
                    UNIT_ASSERT_VALUES_EQUAL(report.GetOperations(0).GetUnfinished().GetCallsCount(), 1);
                }
            }
        }
    }

    Y_UNIT_TEST(StageNodeCountedOnFirstReport) {
        auto stage = PreparedStage(3);
        Report(stage, 7, 1, NYql::NDqProto::COMPUTE_STATE_EXECUTING);
        Report(stage, 7, 2, NYql::NDqProto::COMPUTE_STATE_EXECUTING);
        Report(stage, 3, 3, NYql::NDqProto::COMPUTE_STATE_EXECUTING);
        // Repeated reports do not count the task again.
        Report(stage, 7, 1, NYql::NDqProto::COMPUTE_STATE_EXECUTING);

        UNIT_ASSERT_VALUES_EQUAL(stage.Nodes.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(stage.Nodes.at(7).Tasks, 2);
        UNIT_ASSERT_VALUES_EQUAL(stage.Nodes.at(7).Finished, 0);
        UNIT_ASSERT_VALUES_EQUAL(stage.Nodes.at(3).Tasks, 1);
        UNIT_ASSERT_VALUES_EQUAL(stage.Nodes.at(3).Finished, 0);
        UNIT_ASSERT_VALUES_EQUAL(stage.FinishedCount, 0);
    }

    Y_UNIT_TEST(StageNodeFinishedCountedOnce) {
        auto stage = PreparedStage(2);
        Report(stage, 7, 1, NYql::NDqProto::COMPUTE_STATE_EXECUTING);
        Report(stage, 7, 1, NYql::NDqProto::COMPUTE_STATE_FINISHED);
        Report(stage, 7, 1, NYql::NDqProto::COMPUTE_STATE_FINISHED);
        // The first and only report of a task may already be the final one.
        Report(stage, 3, 2, NYql::NDqProto::COMPUTE_STATE_FINISHED);

        UNIT_ASSERT_VALUES_EQUAL(stage.FinishedCount, 2);
        UNIT_ASSERT_VALUES_EQUAL(stage.Nodes.at(7).Tasks, 1);
        UNIT_ASSERT_VALUES_EQUAL(stage.Nodes.at(7).Finished, 1);
        UNIT_ASSERT_VALUES_EQUAL(stage.Nodes.at(3).Tasks, 1);
        UNIT_ASSERT_VALUES_EQUAL(stage.Nodes.at(3).Finished, 1);
    }

    Y_UNIT_TEST(StageNodeFailureIsNotFinished) {
        auto stage = PreparedStage(1);
        Report(stage, 7, 1, NYql::NDqProto::COMPUTE_STATE_FAILURE);

        UNIT_ASSERT_VALUES_EQUAL(stage.FinishedCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(stage.Nodes.at(7).Tasks, 1);
        UNIT_ASSERT_VALUES_EQUAL(stage.Nodes.at(7).Finished, 0);
    }

    Y_UNIT_TEST(StageNodeUnknownUntilReported) {
        auto stage = PreparedStage(2);
        // Node 0 means "unknown": the task stays outside Nodes and the first real node wins later.
        Report(stage, 0, 1, NYql::NDqProto::COMPUTE_STATE_EXECUTING);
        UNIT_ASSERT(stage.Nodes.empty());

        Report(stage, 7, 1, NYql::NDqProto::COMPUTE_STATE_EXECUTING);
        Report(stage, 3, 1, NYql::NDqProto::COMPUTE_STATE_FINISHED);
        UNIT_ASSERT_VALUES_EQUAL(stage.Nodes.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(stage.Nodes.at(7).Tasks, 1);
        UNIT_ASSERT_VALUES_EQUAL(stage.Nodes.at(7).Finished, 1);

        // A task never heard from is not attributed to any node.
        UNIT_ASSERT_VALUES_EQUAL(stage.Task2Index.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(stage.TaskNodeId[stage.Task2Index.at(2)], 0);
    }

    Y_UNIT_TEST(StageNodeAttributedAfterFinish) {
        auto stage = PreparedStage(1);
        Report(stage, 0, 1, NYql::NDqProto::COMPUTE_STATE_FINISHED);
        UNIT_ASSERT_VALUES_EQUAL(stage.FinishedCount, 1);
        UNIT_ASSERT(stage.Nodes.empty());

        // Late attribution (export time) of an already finished task counts it as finished on that node.
        stage.SetTaskNode(stage.Task2Index.at(1), 5);
        UNIT_ASSERT_VALUES_EQUAL(stage.Nodes.at(5).Tasks, 1);
        UNIT_ASSERT_VALUES_EQUAL(stage.Nodes.at(5).Finished, 1);

        // Once attributed, the node does not change.
        stage.SetTaskNode(stage.Task2Index.at(1), 6);
        UNIT_ASSERT_VALUES_EQUAL(stage.Nodes.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(stage.TaskNodeId[stage.Task2Index.at(1)], 5);
    }

    Y_UNIT_TEST(StageNodeGrowsWithTasks) {
        auto stage = PreparedStage(4);
        UNIT_ASSERT_VALUES_EQUAL(stage.TaskCount, 4);

        // A task unknown to the prepared graph grows all per-task vectors.
        Report(stage, 7, 5, NYql::NDqProto::COMPUTE_STATE_FINISHED);
        UNIT_ASSERT_VALUES_EQUAL(stage.TaskCount, 8);
        UNIT_ASSERT_VALUES_EQUAL(stage.TaskNodeId.size(), 8);
        UNIT_ASSERT_VALUES_EQUAL(stage.TaskNodeId[stage.Task2Index.at(5)], 7);
        UNIT_ASSERT_VALUES_EQUAL(stage.Nodes.at(7).Tasks, 1);
        UNIT_ASSERT_VALUES_EQUAL(stage.Nodes.at(7).Finished, 1);
    }

    Y_UNIT_TEST(NodeStateMemQueryAllocated) {
        TNodeExecutionStats node;
        node.SetHistorySampleCount(32);

        NYql::NDqProto::TEvNodeState state;
        state.SetLocalInflightBytes(1_MB);
        state.SetMemQueryAllocated(3_MB);
        node.UpdateStats(state);

        UNIT_ASSERT_VALUES_EQUAL(node.GlobalMemoryUsage.Value.LocalInflightBytes, 1_MB);
        UNIT_ASSERT_VALUES_EQUAL(node.GlobalMemoryUsage.Value.MemQueryAllocated, 3_MB);
        UNIT_ASSERT(!node.GlobalMemoryUsage.History.empty());
        UNIT_ASSERT_VALUES_EQUAL(node.GlobalMemoryUsage.History.back().second.MemQueryAllocated, 3_MB);
    }
}

} // namespace NKikimr::NKqp
