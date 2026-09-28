#include <ydb/core/kqp/federated_query/actors/streaming_query_nodes_manager.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/fq/libs/checkpointing/events/events.h>
#include <ydb/core/mind/tenant_node_enumeration.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/testlib/basics/appdata.h>

#include <ydb/library/actors/testlib/test_runtime.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/yql/dq/common/dq_common.h>
#include <ydb/library/yql/providers/pq/proto/dq_task_params.pb.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp {

using namespace NActors;

namespace {

void InjectLookupResult(TTestActorRuntime& runtime, TActorId target, TVector<ui32> nodeIds) {
    runtime.Send(new IEventHandle(
        target,
        TActorId{},
        new NKikimr::TEvTenantNodeEnumerator::TEvLookupResult("/Root/test", std::move(nodeIds))));
}

void InjectLookupFailure(TTestActorRuntime& runtime, TActorId target) {
    runtime.Send(new IEventHandle(
        target,
        TActorId{},
        new NKikimr::TEvTenantNodeEnumerator::TEvLookupResult("/Root/test", /* success */ false)));
}

void InjectReadyState(TTestActorRuntime& runtime, TActorId target, const TVector<ui32>& nodeIds) {
    auto event = MakeHolder<NFq::TEvCheckpointCoordinator::TEvReadyState>();
    for (ui64 taskId = 0; taskId < nodeIds.size(); ++taskId) {
        event->Tasks.push_back({
            .Id = taskId,
            .ActorId = TActorId(nodeIds[taskId], "compute"),
        });
    }
    runtime.Send(new IEventHandle(target, TActorId{}, event.Release()));
}

google::protobuf::RepeatedPtrField<NYql::NDqProto::TDqTask> MakeTopicSourceTasks(ui64 taskCount, ui64 topicPartitionsCount) {
    google::protobuf::RepeatedPtrField<NYql::NDqProto::TDqTask> tasks;
    for (ui64 taskId = 0; taskId < taskCount; ++taskId) {
        auto* task = tasks.Add();
        task->SetId(taskId);
        task->AddInputs()->MutableSource()->SetType(TString(NYql::NDq::PqSource));

        NYql::NPq::NProto::TDqReadTaskParams readTaskParams;
        auto* partitioningParams = readTaskParams.AddPartitioningParams();
        partitioningParams->SetTopicPartitionsCount(topicPartitionsCount);
        partitioningParams->SetEachTopicPartitionGroupId(taskId);
        partitioningParams->SetDqPartitionsCount(taskCount);
        task->AddReadRanges(readTaskParams.SerializeAsString());
    }
    return tasks;
}

TActorId CreateManager(
    TTestActorRuntime& runtime,
    TActorId edgeActor,
    ui64 taskCount,
    ui64 partitionsCount,
    ui64 maxTasksPerStage = 0)
{
    runtime.SetLogPriority(NKikimrServices::KQP_EXECUTER, NLog::PRI_TRACE);
    const auto manager = runtime.Register(CreateStreamingQueryNodesManager(
        edgeActor,
        "/Root/test",
        "query",
        MakeTopicSourceTasks(taskCount, partitionsCount),
        TDuration::Seconds(1),
        TDuration::Zero(),
        maxTasksPerStage));
    runtime.EnableScheduleForActor(manager, true);

    // Registration queues Bootstrap automatically. Finish processing it before
    // sending ReadyState, without waiting for an event from the idle manager.
    TDispatchOptions options;
    options.OnlyMailboxes.emplace_back(manager);
    options.FinalEvents.emplace_back([manager](IEventHandle& event) {
        return event.GetRecipientRewrite() == manager
            && event.GetTypeRewrite() == TEvents::TSystem::Bootstrap;
    });
    runtime.DispatchEvents(options);
    return manager;
}

void WaitForCheck(TTestActorRuntime& runtime, TActorId manager) {
    // Only dispatch the manager: its own timer starts the lookup, while the
    // lookup actor is left pending so the test can supply a controlled result.
    TDispatchOptions options;
    options.OnlyMailboxes.emplace_back(manager);
    options.FinalEvents.emplace_back([manager](IEventHandle& event) {
        return event.GetRecipientRewrite() == manager
            && event.GetTypeRewrite() == TEvents::TEvWakeup::EventType
            && event.Get<TEvents::TEvWakeup>()->Tag == 1;
    });
    runtime.DispatchEvents(options);
}

void CompleteCheck(TTestActorRuntime& runtime, TActorId manager, TVector<ui32> tenantNodes) {
    WaitForCheck(runtime, manager);
    InjectLookupResult(runtime, manager, std::move(tenantNodes));
}

ui32 TakeAbortCount(TTestActorRuntime& runtime, TActorId edgeActor) {
    // InjectLookupResult uses synchronous runtime.Send, so any abort is already
    // queued in the edge mailbox. No timeout or additional timer is needed.
    auto events = runtime.CaptureMailboxEvents(edgeActor.Hint(), edgeActor.NodeId());
    ui32 count = 0;
    for (const auto& event : events) {
        UNIT_ASSERT_VALUES_EQUAL(event->GetTypeRewrite(), TEvKqp::TEvAbortExecution::EventType);
        ++count;
    }
    return count;
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(TStreamingQueryNodesManagerTest) {

Y_UNIT_TEST(AbortWhenNotAllNodesRunQueryAndNodesDoNotExceedExpectedTasks) {
    TTestActorRuntime runtime(1, false);
    runtime.Initialize(NKikimr::TAppPrepare().Unwrap());
    const TActorId edgeActor = runtime.AllocateEdgeActor();

    // 10 partitions require 2 tasks. With 2 tenant nodes, both must run query tasks.
    const TActorId manager = CreateManager(runtime, edgeActor, 2, 10);
    InjectReadyState(runtime, manager, {1, 1});
    CompleteCheck(runtime, manager, {1, 2});

    UNIT_ASSERT_VALUES_EQUAL(TakeAbortCount(runtime, edgeActor), 1);
}

Y_UNIT_TEST(NoAbortWhenAllNodesRunQueryAndNodesDoNotExceedExpectedTasks) {
    TTestActorRuntime runtime(1, false);
    runtime.Initialize(NKikimr::TAppPrepare().Unwrap());
    const TActorId edgeActor = runtime.AllocateEdgeActor();

    // 10 partitions require 2 tasks. With 2 tenant nodes, both run query tasks.
    const TActorId manager = CreateManager(runtime, edgeActor, 2, 10);
    InjectReadyState(runtime, manager, {1, 2});
    CompleteCheck(runtime, manager, {1, 2});

    UNIT_ASSERT_VALUES_EQUAL(TakeAbortCount(runtime, edgeActor), 0);
}

Y_UNIT_TEST(AbortWhenNodesExceedExpectedTasksAndQueryUsesTooFewNodes) {
    TTestActorRuntime runtime(1, false);
    runtime.Initialize(NKikimr::TAppPrepare().Unwrap());
    const TActorId edgeActor = runtime.AllocateEdgeActor();

    // 10 partitions require 2 tasks. With 4 tenant nodes, the query needs 2 nodes.
    const TActorId manager = CreateManager(runtime, edgeActor, 2, 10);
    InjectReadyState(runtime, manager, {1, 1});
    CompleteCheck(runtime, manager, {1, 2, 3, 4});

    UNIT_ASSERT_VALUES_EQUAL(TakeAbortCount(runtime, edgeActor), 1);
}

Y_UNIT_TEST(NoAbortWhenNodesExceedExpectedTasksAndQueryUsesExpectedNodes) {
    TTestActorRuntime runtime(1, false);
    runtime.Initialize(NKikimr::TAppPrepare().Unwrap());
    const TActorId edgeActor = runtime.AllocateEdgeActor();

    // 10 partitions require 2 tasks. With 4 tenant nodes, two query nodes suffice.
    const TActorId manager = CreateManager(runtime, edgeActor, 2, 10);
    InjectReadyState(runtime, manager, {1, 2});
    CompleteCheck(runtime, manager, {1, 2, 3, 4});

    UNIT_ASSERT_VALUES_EQUAL(TakeAbortCount(runtime, edgeActor), 0);
}

Y_UNIT_TEST(NoAbortWhenMaxTasksPerStageLimitsTopicReaderTasks) {
    TTestActorRuntime runtime(1, false);
    runtime.Initialize(NKikimr::TAppPrepare().Unwrap());
    const TActorId edgeActor = runtime.AllocateEdgeActor();

    // 100 partitions would require 20 tasks, but MaxTasksPerStage limits the query to one task.
    const TActorId manager = CreateManager(runtime, edgeActor, 1, 100, 1);
    InjectReadyState(runtime, manager, {1});
    CompleteCheck(runtime, manager, {1, 2, 3, 4});

    UNIT_ASSERT_VALUES_EQUAL(TakeAbortCount(runtime, edgeActor), 0);
}

Y_UNIT_TEST(FailedLookupDoesNotAbort) {
    TTestActorRuntime runtime(1, false);
    runtime.Initialize(NKikimr::TAppPrepare().Unwrap());
    const TActorId edgeActor = runtime.AllocateEdgeActor();
    const TActorId manager = CreateManager(runtime, edgeActor, 2, 10);
    InjectReadyState(runtime, manager, {1, 1});

    WaitForCheck(runtime, manager);
    InjectLookupFailure(runtime, manager);

    UNIT_ASSERT_VALUES_EQUAL(TakeAbortCount(runtime, edgeActor), 0);
}

Y_UNIT_TEST(AbortIsSentOnlyOnce) {
    TTestActorRuntime runtime(1, false);
    runtime.Initialize(NKikimr::TAppPrepare().Unwrap());
    const TActorId edgeActor = runtime.AllocateEdgeActor();
    const TActorId manager = CreateManager(runtime, edgeActor, 2, 10);
    InjectReadyState(runtime, manager, {1, 1});

    CompleteCheck(runtime, manager, {1, 2, 3, 4});
    CompleteCheck(runtime, manager, {1, 2, 3, 4});

    UNIT_ASSERT_VALUES_EQUAL(TakeAbortCount(runtime, edgeActor), 1);
}

Y_UNIT_TEST(ReadyStateIgnoresNonTopicTasks) {
    TTestActorRuntime runtime(1, false);
    runtime.Initialize(NKikimr::TAppPrepare().Unwrap());
    const TActorId edgeActor = runtime.AllocateEdgeActor();
    const TActorId manager = CreateManager(runtime, edgeActor, 2, 10);

    // Only tasks 0 and 1 read topics; task 2 must not contribute a query node.
    InjectReadyState(runtime, manager, {1, 1, 2});
    CompleteCheck(runtime, manager, {1, 2});

    UNIT_ASSERT_VALUES_EQUAL(TakeAbortCount(runtime, edgeActor), 1);
}

} // Y_UNIT_TEST_SUITE

} // namespace NKikimr::NKqp
