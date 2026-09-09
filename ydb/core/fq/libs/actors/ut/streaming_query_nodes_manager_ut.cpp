#include <ydb/core/fq/libs/actors/streaming_query_nodes_manager.h>
#include <ydb/core/mind/tenant_node_enumeration.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/testlib/basics/appdata.h>

#include <ydb/library/actors/testlib/test_runtime.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/yql/dq/common/dq_common.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>
#include <ydb/library/yql/providers/pq/proto/dq_task_params.pb.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NFq {

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

void InjectTaskStates(TTestActorRuntime& runtime, TActorId target, const TVector<ui32>& nodeIds) {
    for (ui64 taskId = 0; taskId < nodeIds.size(); ++taskId) {
        auto state = MakeHolder<NYql::NDq::TEvDqCompute::TEvState>();
        state->Record.SetTaskId(taskId);
        state->Record.SetState(NYql::NDqProto::COMPUTE_STATE_EXECUTING);
        runtime.Send(new IEventHandle(target, TActorId(nodeIds[taskId], "compute"), state.Release()));
    }
}

NProto::TGraphParams MakeTopicSourceGraph(ui64 taskCount, ui64 topicPartitionsCount) {
    NProto::TGraphParams graphParams;
    for (ui64 taskId = 0; taskId < taskCount; ++taskId) {
        auto* task = graphParams.AddTasks();
        task->SetId(taskId);
        task->AddInputs()->MutableSource()->SetType(TString(NYql::NDq::PqSource));

        NYql::NPq::NProto::TDqReadTaskParams readTaskParams;
        auto* partitioningParams = readTaskParams.AddPartitioningParams();
        partitioningParams->SetTopicPartitionsCount(topicPartitionsCount);
        partitioningParams->SetEachTopicPartitionGroupId(taskId);
        partitioningParams->SetDqPartitionsCount(taskCount);
        task->AddReadRanges(readTaskParams.SerializeAsString());
    }
    return graphParams;
}

TActorId CreateManager(TTestActorRuntime& runtime, TActorId edgeActor, ui64 taskCount, ui64 partitionsCount) {
    runtime.SetLogPriority(NKikimrServices::KQP_EXECUTER, NLog::PRI_TRACE);
    const auto manager = runtime.Register(CreateStreamingQueryNodesManager(
        edgeActor,
        "/Root/test",
        "query",
        MakeTopicSourceGraph(taskCount, partitionsCount),
        TDuration::Seconds(1),
        TDuration::Zero()));
    runtime.EnableScheduleForActor(manager, true);
    runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(50));
    return manager;
}

void TriggerCheck(TTestActorRuntime& runtime, TActorId manager, TActorId edgeActor, TVector<ui32> tenantNodes) {
    runtime.Send(new IEventHandle(manager, edgeActor, new TEvents::TEvWakeup(/* tag */ 1)));
    runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(50));
    InjectLookupResult(runtime, manager, std::move(tenantNodes));
    runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(50));
}

TEvStreamingQueryNodesManager::TEvAbortQuery* GrabAbort(TTestActorRuntime& runtime, TAutoPtr<IEventHandle>& handle) {
    return runtime.GrabEdgeEventRethrow<TEvStreamingQueryNodesManager::TEvAbortQuery>(
        handle,
        TDuration::MilliSeconds(100));
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(TStreamingQueryNodesManagerTest) {

Y_UNIT_TEST(AbortWhenNotAllNodesRunQueryAndNodesDoNotExceedExpectedTasks) {
    TTestActorRuntime runtime(1, false);
    runtime.Initialize(NKikimr::TAppPrepare().Unwrap());
    const TActorId edgeActor = runtime.AllocateEdgeActor();

    // 10 partitions require 2 tasks. With 2 tenant nodes, both must run query tasks.
    const TActorId manager = CreateManager(runtime, edgeActor, 2, 10);
    InjectTaskStates(runtime, manager, {1, 1});
    TriggerCheck(runtime, manager, edgeActor, {1, 2});

    TAutoPtr<IEventHandle> handle;
    UNIT_ASSERT(GrabAbort(runtime, handle));
}

Y_UNIT_TEST(NoAbortWhenAllNodesRunQueryAndNodesDoNotExceedExpectedTasks) {
    TTestActorRuntime runtime(1, false);
    runtime.Initialize(NKikimr::TAppPrepare().Unwrap());
    const TActorId edgeActor = runtime.AllocateEdgeActor();

    // 10 partitions require 2 tasks. With 2 tenant nodes, both run query tasks.
    const TActorId manager = CreateManager(runtime, edgeActor, 2, 10);
    InjectTaskStates(runtime, manager, {1, 2});
    TriggerCheck(runtime, manager, edgeActor, {1, 2});

    TAutoPtr<IEventHandle> handle;
    UNIT_ASSERT(!GrabAbort(runtime, handle));
}

Y_UNIT_TEST(AbortWhenNodesExceedExpectedTasksAndQueryUsesTooFewNodes) {
    TTestActorRuntime runtime(1, false);
    runtime.Initialize(NKikimr::TAppPrepare().Unwrap());
    const TActorId edgeActor = runtime.AllocateEdgeActor();

    // 10 partitions require 2 tasks. With 4 tenant nodes, the query needs 2 nodes.
    const TActorId manager = CreateManager(runtime, edgeActor, 2, 10);
    InjectTaskStates(runtime, manager, {1, 1});
    TriggerCheck(runtime, manager, edgeActor, {1, 2, 3, 4});

    TAutoPtr<IEventHandle> handle;
    UNIT_ASSERT(GrabAbort(runtime, handle));
}

Y_UNIT_TEST(NoAbortWhenNodesExceedExpectedTasksAndQueryUsesExpectedNodes) {
    TTestActorRuntime runtime(1, false);
    runtime.Initialize(NKikimr::TAppPrepare().Unwrap());
    const TActorId edgeActor = runtime.AllocateEdgeActor();

    // 10 partitions require 2 tasks. With 4 tenant nodes, two query nodes suffice.
    const TActorId manager = CreateManager(runtime, edgeActor, 2, 10);
    InjectTaskStates(runtime, manager, {1, 2});
    TriggerCheck(runtime, manager, edgeActor, {1, 2, 3, 4});

    TAutoPtr<IEventHandle> handle;
    UNIT_ASSERT(!GrabAbort(runtime, handle));
}

Y_UNIT_TEST(FailedLookupDoesNotAbort) {
    TTestActorRuntime runtime(1, false);
    runtime.Initialize(NKikimr::TAppPrepare().Unwrap());
    const TActorId edgeActor = runtime.AllocateEdgeActor();
    const TActorId manager = CreateManager(runtime, edgeActor, 2, 10);

    runtime.Send(new IEventHandle(manager, edgeActor, new TEvents::TEvWakeup(/* tag */ 1)));
    runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(50));
    InjectLookupFailure(runtime, manager);
    runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(50));

    TAutoPtr<IEventHandle> handle;
    UNIT_ASSERT(!GrabAbort(runtime, handle));
}

Y_UNIT_TEST(AbortIsSentOnlyOnce) {
    TTestActorRuntime runtime(1, false);
    runtime.Initialize(NKikimr::TAppPrepare().Unwrap());
    const TActorId edgeActor = runtime.AllocateEdgeActor();
    const TActorId manager = CreateManager(runtime, edgeActor, 2, 10);
    InjectTaskStates(runtime, manager, {1, 1});

    TriggerCheck(runtime, manager, edgeActor, {1, 2, 3, 4});
    TriggerCheck(runtime, manager, edgeActor, {1, 2, 3, 4});

    ui32 abortCount = 0;
    TAutoPtr<IEventHandle> handle;
    while (GrabAbort(runtime, handle)) {
        ++abortCount;
    }
    UNIT_ASSERT_VALUES_EQUAL(abortCount, 1);
}

} // Y_UNIT_TEST_SUITE

} // namespace NFq
