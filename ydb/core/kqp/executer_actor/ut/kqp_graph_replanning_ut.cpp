#include <ydb/core/kqp/executer_actor/kqp_graph_replanning.h>
#include <ydb/library/yql/providers/pq/common/pq_partitions.h>
#include <ydb/library/yql/providers/pq/proto/dq_io.pb.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp {

Y_UNIT_TEST_SUITE(TStreamingGraphReplanning) {
    Y_UNIT_TEST(ComparatorIgnoresPlacementAndMapInsertionOrder) {
        NKikimrKqp::TQueryPhysicalGraph old;
        auto& task = *old.AddTasks()->MutableDqTask();
        task.SetId(1);
        (*task.MutableTaskParams())["a"] = "1";
        (*task.MutableTaskParams())["b"] = "2";
        (*task.MutableTaskParams())["runtime_actor"] = "previousActor";
        (*old.MutablePreparedQuery()->MutablePhysicalQuery()->AddTransactions()->AddStages()
            ->MutableStageControlPlaneActors())["runtime_actor"].SetType("actor");
        task.MutableExecuter()->MutableActorId()->SetRawX1(1);
        task.MutableExecuter()->MutableActorId()->SetRawX2(0);
        auto* channel = task.AddOutputs()->AddChannels();
        channel->SetId(1);
        channel->SetDstTaskId(2);
        auto next = old;
        auto* newTask = next.MutableTasks(0)->MutableDqTask();
        newTask->MutableExecuter()->MutableActorId()->SetRawX1(42);
        newTask->MutableTaskParams()->clear();
        (*newTask->MutableTaskParams())["b"] = "2";
        (*newTask->MutableTaskParams())["a"] = "1";
        (*newTask->MutableTaskParams())["current_execution_generation"] = "99";
        (*newTask->MutableTaskParams())["runtime_actor"] = "nextActor";
        (*newTask->MutableTaskParams())["fq.job_id"] = "newTrace";
        newTask->MutableOutputs(0)->MutableChannels(0)->SetId(99);
        next.SetZeroCheckpointSaved(true);
        UNIT_ASSERT(MaterializedGraphsEqual(old, next));
        newTask->MutableOutputs(0)->MutableChannels(0)->SetDstTaskId(3);
        UNIT_ASSERT(!MaterializedGraphsEqual(old, next));
    }

    Y_UNIT_TEST(ComparatorPreservesOrderedRoutingAndSourceAssignments) {
        NKikimrKqp::TQueryPhysicalGraph old;
        auto& task = *old.AddTasks()->MutableDqTask();
        task.SetId(1);
        task.AddReadRanges("range");
        auto& output = *task.AddOutputs();
        output.MutableHashPartition()->SetPartitionsCount(2);
        output.AddChannels()->SetDstTaskId(2);
        output.AddChannels()->SetDstTaskId(3);
        auto next = old;
        next.MutableTasks(0)->MutableDqTask()->MutableOutputs(0)->MutableChannels()->SwapElements(0, 1);
        UNIT_ASSERT(!MaterializedGraphsEqual(old, next));
        next = old;
        next.MutableTasks(0)->MutableDqTask()->SetReadRanges(0, "other");
        UNIT_ASSERT(!MaterializedGraphsEqual(old, next));
        next = old;
        next.MutableTasks(0)->MutableDqTask()->SetId(100);
        UNIT_ASSERT(!MaterializedGraphsEqual(old, next));
        next = old;
        next.MutableTasks(0)->MutableDqTask()->MutableMeta()->set_type_url("table_scan");
        next.MutableTasks(0)->MutableDqTask()->MutableMeta()->set_value("changed_read_ranges");
        UNIT_ASSERT(!MaterializedGraphsEqual(old, next));
    }

    Y_UNIT_TEST(ComparatorIgnoresTableSinkActorsButPreservesTableIdentity) {
        NKikimrKqp::TQueryPhysicalGraph old;
        auto& task = *old.AddTasks()->MutableDqTask();
        task.SetId(1);
        auto* sink = task.AddOutputs()->MutableSink();
        sink->SetType("KqpTableSink");
        NKikimrKqp::TKqpTableSinkSettings settings;
        settings.MutableTable()->SetPath("/Root/state");
        settings.MutableBufferActorId()->SetRawX1(1);
        settings.MutableBufferActorId()->SetRawX2(0);
        settings.SetLockTxId(10);
        settings.SetLockNodeId(2);
        sink->MutableSettings()->PackFrom(settings);
        auto next = old;
        settings.MutableBufferActorId()->SetRawX1(42);
        settings.SetLockTxId(99);
        settings.SetLockNodeId(3);
        settings.SetQuerySpanId(100);
        next.MutableTasks(0)->MutableDqTask()->MutableOutputs(0)->MutableSink()->MutableSettings()->PackFrom(settings);
        UNIT_ASSERT(MaterializedGraphsEqual(old, next));
        settings.MutableTable()->SetPath("/Root/other");
        next.MutableTasks(0)->MutableDqTask()->MutableOutputs(0)->MutableSink()->MutableSettings()->PackFrom(settings);
        UNIT_ASSERT(!MaterializedGraphsEqual(old, next));
    }

    Y_UNIT_TEST(CurrentPartitionSnapshotHonorsReaderLimitAndFederation) {
        NKqpProto::TKqpExternalSource source;
        source.SetType("PqSource");
        NYql::NPq::NProto::TDqPqTopicSource settings;
        settings.AddFederatedClusters()->SetName("east");
        settings.AddFederatedClusters()->SetName("west");
        source.MutableSettings()->PackFrom(settings);
        RefreshPqSourcePartitions(source, {{"east", 10}, {"west", 20}}, 20, 3);
        UNIT_ASSERT_VALUES_EQUAL(source.PartitionedTaskParamsSize(), 3);
        UNIT_ASSERT(source.GetSettings().UnpackTo(&settings));
        UNIT_ASSERT_VALUES_EQUAL(settings.GetFederatedClusters(0).GetPartitionsCount(), 10);
        UNIT_ASSERT_VALUES_EQUAL(settings.GetFederatedClusters(1).GetPartitionsCount(), 20);
        TVector<TString> ranges(source.GetPartitionedTaskParams().begin(), source.GetPartitionedTaskParams().end());
        const auto partitions = NYql::NDq::GetPartitionsToRead(NYql::NDq::ExtractReadTaskParams({}, ranges), {});
        UNIT_ASSERT_VALUES_EQUAL(partitions.size(), 20);
        RefreshPqSourcePartitions(source, {{"east", 10}, {"west", 20}}, 20, 0);
        UNIT_ASSERT_VALUES_EQUAL(source.PartitionedTaskParamsSize(), 4);
    }

    Y_UNIT_TEST(PartitionPruningKeepsSparseIdsAfterTopicGrowth) {
        NKqpProto::TKqpExternalSource source;
        NYql::NPq::NProto::TDqPqTopicSource settings;
        settings.SetUsedPartitionPredicate(true);
        source.MutableSettings()->PackFrom(settings);
        NYql::NPq::NProto::TDqReadTaskParams params;
        for (const ui64 id : {7, 19, 51}) {
            auto& range = *params.AddPartitioningParams();
            range.SetTopicPartitionsCount(100);
            range.SetEachTopicPartitionGroupId(id);
            range.SetDqPartitionsCount(100);
        }
        source.AddPartitionedTaskParams(params.SerializeAsString());
        RefreshPqSourcePartitions(source, {}, 200, 2);
        UNIT_ASSERT_VALUES_EQUAL(source.PartitionedTaskParamsSize(), 2);
        THashSet<ui64> selected;
        for (const auto& raw : source.GetPartitionedTaskParams()) {
            UNIT_ASSERT(params.ParseFromString(raw));
            for (const auto& range : params.GetPartitioningParams()) {
                UNIT_ASSERT_VALUES_EQUAL(range.GetTopicPartitionsCount(), 200);
                UNIT_ASSERT_VALUES_EQUAL(range.GetDqPartitionsCount(), 200);
                UNIT_ASSERT(selected.insert(range.GetEachTopicPartitionGroupId()).second);
            }
        }
        UNIT_ASSERT(selected == THashSet<ui64>({7, 19, 51}));
    }
}

} // namespace NKikimr::NKqp
