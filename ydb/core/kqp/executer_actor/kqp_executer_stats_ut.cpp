#include <ydb/core/kqp/executer_actor/kqp_executer_stats.h>

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
}

} // namespace NKikimr::NKqp
