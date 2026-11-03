#include <ydb/core/kqp/executer_actor/kqp_executer_stats.h>
#include <ydb/core/kqp/compute_actor/kqp_scan_events.h>

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

Y_UNIT_TEST_SUITE(TColumnShardV2Transport) {
    Y_UNIT_TEST(PreservesSemanticContextAcrossWireAndEmptyFinalReport) {
        NKqpProto::TColumnShardScanStats first;
        first.SetStatsScanId("v2-attempt");
        first.SetTabletId(42);
        first.SetReaderKind("PLAIN");
        first.SetPendingOperations(2);
        first.SetStatsGroupId("group-1");
        first.SetParticipatingShards(2);
        first.AddParticipatingTablets(42);
        first.AddParticipatingTablets(43);
        auto* step = first.AddSteps();
        step->SetStepId(7);
        step->SetStepKind(NKqpProto::TColumnShardStepStats::Decode);
        step->SetMetaStep("PKFilter");
        step->SetCanonicalPath("Merge");
        step->SetOwnedExecutionDuration(0);

        NKqpProto::TColumnShardScanStats remote;
        UNIT_ASSERT(remote.ParseFromString(first.SerializeAsString()));
        TColumnShardScanStatsCollection collection;
        collection.Merge(remote);
        collection.Merge(remote);

        remote.ClearReaderKind();
        remote.ClearTabletId();
        remote.ClearStatsGroupId();
        remote.ClearParticipatingShards();
        remote.ClearParticipatingTablets();
        remote.ClearSteps();
        remote.SetFinished(true);
        remote.SetPendingOperations(0);
        collection.Merge(remote);
        collection.Merge(first);

        google::protobuf::RepeatedPtrField<NKqpProto::TColumnShardScanStats> reports;
        collection.Export(&reports);
        UNIT_ASSERT_VALUES_EQUAL(reports.size(), 1);
        const auto& report = reports.Get(0);
        UNIT_ASSERT_VALUES_EQUAL(report.GetReaderKind(), "PLAIN");
        UNIT_ASSERT_VALUES_EQUAL(report.GetTabletId(), 42);
        UNIT_ASSERT_VALUES_EQUAL(report.GetStatsGroupId(), "group-1");
        UNIT_ASSERT_VALUES_EQUAL(report.GetParticipatingShards(), 2);
        UNIT_ASSERT_VALUES_EQUAL(report.ParticipatingTabletsSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(report.GetParticipatingTablets(0), 42);
        UNIT_ASSERT_VALUES_EQUAL(report.GetParticipatingTablets(1), 43);
        UNIT_ASSERT(report.GetFinished());
        UNIT_ASSERT_VALUES_EQUAL(report.GetPendingOperations(), 0);
        UNIT_ASSERT_VALUES_EQUAL(report.StepsSize(), 1);
        const auto& restored = report.GetSteps(0);
        UNIT_ASSERT_VALUES_EQUAL(restored.GetMetaStep(), "PKFilter");
        UNIT_ASSERT_VALUES_EQUAL(restored.GetCanonicalPath(), "Merge");
        UNIT_ASSERT(restored.HasOwnedExecutionDuration());
        UNIT_ASSERT_VALUES_EQUAL(restored.GetOwnedExecutionDuration(), 0);
    }

    Y_UNIT_TEST(ExpectedRosterRemainsSeparateFromRuntimeAttempts) {
        NKqpProto::TColumnShardScanStats expected;
        expected.SetStatsScanId("group:expected");
        expected.SetStatsGroupId("group");
        expected.SetParticipatingShards(16);
        expected.SetExpectedContextOnly(true);
        expected.SetFinished(true);
        for (ui64 tabletId = 1; tabletId <= 16; ++tabletId) {
            expected.AddParticipatingTablets(tabletId);
        }
        TColumnShardScanStatsCollection collection;
        collection.Merge(expected);
        collection.Merge(expected);

        NKqpProto::TColumnShardScanStats runtime;
        runtime.SetStatsScanId("group:attempt");
        runtime.SetStatsGroupId("group");
        runtime.SetTabletId(1);
        runtime.SetReaderKind("TRIVIAL");
        runtime.SetFinished(true);
        collection.Merge(runtime);

        google::protobuf::RepeatedPtrField<NKqpProto::TColumnShardScanStats> reports;
        collection.Export(&reports);
        UNIT_ASSERT_VALUES_EQUAL(reports.size(), 2);
        ui32 expectedCount = 0;
        ui32 runtimeCount = 0;
        for (const auto& report : reports) {
            if (report.GetExpectedContextOnly()) {
                ++expectedCount;
                UNIT_ASSERT_VALUES_EQUAL(report.GetTabletId(), 0);
                UNIT_ASSERT_VALUES_EQUAL(report.ParticipatingTabletsSize(), 16);
                UNIT_ASSERT_VALUES_EQUAL(report.StepsSize(), 0);
            } else {
                ++runtimeCount;
                UNIT_ASSERT_VALUES_EQUAL(report.GetTabletId(), 1);
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(expectedCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(runtimeCount, 1);
    }
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

    Y_UNIT_TEST(ColumnShardStatsSnapshotsReplaceRecords) {
        auto stage = PreparedStage(1);
        auto task = TaskStats(1);
        NKqpProto::TKqpTaskExtraStats extra;
        auto* scan = extra.MutableScanTaskExtraStats()->AddColumnShardScans();
        scan->SetStatsScanId("scan:attempt1");
        scan->SetTabletId(42);
        auto* step = scan->AddSteps();
        step->SetStepId(1);
        step->SetStepKind(NKqpProto::TColumnShardStepStats::Filter);
        step->SetOutcome(NKqpProto::TColumnShardStepStats::completed);
        step->SetReadBytes(0);
        step->MutableDetails()->MutableFilter()->SetInputRows(100);
        step->MutableDetails()->MutableFilter()->SetOutputRows(20);
        task.MutableExtra()->PackFrom(extra);
        stage.UpdateStats(7, task, NYql::NDqProto::COMPUTE_STATE_EXECUTING, 0, 0, 0);
        stage.UpdateStats(7, task, NYql::NDqProto::COMPUTE_STATE_EXECUTING, 0, 0, 0);

        scan->SetFinished(true);
        scan->SetTruncated(true);
        scan->MutableCacheStats()->SetReturnedBytes(20);
        scan->AddSteps()->SetStepId(2);
        task.MutableExtra()->PackFrom(extra);
        stage.UpdateStats(7, task, NYql::NDqProto::COMPUTE_STATE_FINISHED, 0, 0, 0);

        google::protobuf::RepeatedPtrField<NKqpProto::TColumnShardScanStats> reports;
        stage.ColumnShardScans.Export(&reports);
        UNIT_ASSERT_VALUES_EQUAL(reports.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(reports.Get(0).StepsSize(), 2);
        UNIT_ASSERT(reports.Get(0).GetFinished());
        UNIT_ASSERT(reports.Get(0).GetTruncated());
        UNIT_ASSERT_VALUES_EQUAL(reports.Get(0).GetCacheStats().GetReturnedBytes(), 20);
        UNIT_ASSERT(reports.Get(0).GetSteps(0).HasReadBytes());
        UNIT_ASSERT(!reports.Get(0).GetSteps(0).HasIntegralWaitDuration());
        UNIT_ASSERT(stage.ComputeActors.empty());

        const auto first = reports.Get(0).SerializeAsString();
        stage.ColumnShardScans.Export(&reports);
        UNIT_ASSERT_VALUES_EQUAL(reports.Get(0).SerializeAsString(), first);
        // An older snapshot cannot reopen the scan or duplicate an operation.
        scan->SetFinished(false);
        scan->ClearSteps();
        scan->MutableCacheStats()->SetReturnedBytes(1);
        task.MutableExtra()->PackFrom(extra);
        stage.UpdateStats(7, task, NYql::NDqProto::COMPUTE_STATE_EXECUTING, 0, 0, 0);
        stage.ColumnShardScans.Export(&reports);
        UNIT_ASSERT_VALUES_EQUAL(reports.Get(0).SerializeAsString(), first);

        scan->SetStatsScanId("scan:attempt2");
        task.MutableExtra()->PackFrom(extra);
        stage.UpdateStats(7, task, NYql::NDqProto::COMPUTE_STATE_EXECUTING, 0, 0, 0);
        stage.ColumnShardScans.Export(&reports);
        UNIT_ASSERT_VALUES_EQUAL(reports.size(), 2);
    }

    Y_UNIT_TEST(ColumnShardStatsRemoteFinalEmptyBatch) {
        TEvKqpCompute::TEvScanData event(7, 2);
        event.Finished = true;
        event.ColumnShardStats = std::make_unique<NKqpProto::TColumnShardScanStats>();
        auto& scan = *event.ColumnShardStats;
        scan.SetStatsScanId("scan:attempt2");
        scan.SetTabletId(42);
        scan.SetFinished(true);
        auto* step = scan.AddSteps();
        step->SetStepId(1);
        step->SetStepKind(NKqpProto::TColumnShardStepStats::Decode);
        step->SetIntegralExecutionDuration(0);
        step->SetOutcome(NKqpProto::TColumnShardStepStats::failed);
        step->MutableDetails()->MutableDecode()->SetEncodedInputBytes(17);

        NActors::TAllocChunkSerializer serializer;
        UNIT_ASSERT(event.SerializeToArcadiaStream(&serializer));
        auto serialized = serializer.Release(event.CreateSerializationInfo(false));
        THolder<TEvKqpCompute::TEvScanData> restored(TEvKqpCompute::TEvScanData::Load(serialized.Get()));
        UNIT_ASSERT(restored->Finished);
        UNIT_ASSERT(restored->IsEmpty());
        UNIT_ASSERT(restored->ColumnShardStats);
        UNIT_ASSERT_VALUES_EQUAL(restored->ColumnShardStats->SerializeAsString(), scan.SerializeAsString());

        NScanPrivate::TEvScanExchange::TEvSendData metadata(std::move(restored->ColumnShardStats));
        UNIT_ASSERT(metadata.IsStatsOnly());
        UNIT_ASSERT_VALUES_EQUAL(metadata.GetRowsCount(), 0);
        UNIT_ASSERT_VALUES_EQUAL(metadata.GetColumnShardStats()->GetStatsScanId(), "scan:attempt2");
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
