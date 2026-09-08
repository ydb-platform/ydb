#include <ydb/core/fq/libs/actors/streaming_query_nodes_manager.h>
#include <ydb/core/mind/tenant_node_enumeration.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/testlib/basics/appdata.h>

#include <ydb/library/actors/testlib/test_runtime.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/yql/dq/common/dq_common.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NFq {

using namespace NActors;

namespace {

// Helper: inject a successful TEvLookupResult with the given node ids.
void InjectLookupResult(
    TTestActorRuntime& runtime,
    TActorId target,
    TVector<ui32> nodeIds)
{
    TVector<ui32> nodes = std::move(nodeIds);
    runtime.Send(
        new IEventHandle(target, TActorId{},
            new NKikimr::TEvTenantNodeEnumerator::TEvLookupResult(
                "/Root/test", std::move(nodes))));
}

// Helper: inject a failed TEvLookupResult.
void InjectLookupFailure(TTestActorRuntime& runtime, TActorId target) {
    runtime.Send(
        new IEventHandle(target, TActorId{},
            new NKikimr::TEvTenantNodeEnumerator::TEvLookupResult(
                "/Root/test", /* success */ false)));
}

// Helper: inject compute states from tasks placed on the supplied nodes.
void InjectTaskStates(
    TTestActorRuntime& runtime,
    TActorId target,
    const TVector<ui32>& nodeIds,
    ui64 firstTaskId = 0)
{
    for (ui64 offset = 0; offset < nodeIds.size(); ++offset) {
        auto state = MakeHolder<NYql::NDq::TEvDqCompute::TEvState>();
        state->Record.SetTaskId(firstTaskId + offset);
        state->Record.SetState(NYql::NDqProto::COMPUTE_STATE_EXECUTING);
        runtime.Send(new IEventHandle(
            target, TActorId(nodeIds[offset], "compute"), state.Release()));
    }
}

NProto::TGraphParams MakeTopicSourceGraph(ui64 taskCount) {
    NProto::TGraphParams graphParams;
    for (ui64 taskId = 0; taskId < taskCount; ++taskId) {
        auto* task = graphParams.AddTasks();
        task->SetId(taskId);
        task->AddInputs()->MutableSource()->SetType(TString(NYql::NDq::PqSource));
    }
    return graphParams;
}

NProto::TGraphParams MakeGraphWithTopicAndNonTopicTasks(ui64 topicTaskCount, ui64 nonTopicTaskCount) {
    auto graphParams = MakeTopicSourceGraph(topicTaskCount);
    for (ui64 taskId = topicTaskCount; taskId < topicTaskCount + nonTopicTaskCount; ++taskId) {
        graphParams.AddTasks()->SetId(taskId);
    }
    return graphParams;
}

} // anonymous namespace

// ============================================================================
Y_UNIT_TEST_SUITE(TStreamingQueryNodesManagerTest) {

// ---------------------------------------------------------------------------
// 1. When nodesWithQuery / total >= 0.5 – no abort.
// ---------------------------------------------------------------------------
Y_UNIT_TEST(NoAbortWhenRatioSufficient) {
    TTestActorRuntime runtime(1, false);
    runtime.Initialize(NKikimr::TAppPrepare().Unwrap());

    TActorId edgeActor = runtime.AllocateEdgeActor();

    // 4 tasks on 4 nodes total → ratio = 4/4 = 1.0 → OK
    TActorId manager = runtime.Register(
        CreateStreamingQueryNodesManager(
            edgeActor,
            "/Root/test",
            "query-1",
            MakeTopicSourceGraph(4),
            TDuration::Hours(1), // use large period so wakeup doesn't auto-fire
            TDuration::Zero()
        ));

    runtime.EnableScheduleForActor(manager, true);
    runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(50));

    // Manually simulate a Wakeup then immediately inject a good lookup result.
    runtime.Send(new IEventHandle(manager, edgeActor,
        new TEvents::TEvWakeup(/* tag */ 1)));

    // Let actors process the wakeup (it will try to launch a child lookup actor,
    // which we won't respond to – just inject the result directly).
    runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(50));

    InjectTaskStates(runtime, manager, {1, 2, 3, 4});
    // Now inject: 4 tenant nodes → nodesWithQuery = 4, ratio = 1.0
    InjectLookupResult(runtime, manager, {1, 2, 3, 4});
    runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(50));

    // No abort should have been sent.
    TAutoPtr<IEventHandle> handle;
    auto* ev = runtime.GrabEdgeEventRethrow<TEvStreamingQueryNodesManager::TEvAbortQuery>(
        handle, TDuration::MilliSeconds(100));
    UNIT_ASSERT_C(ev == nullptr, "Unexpected abort: " << (ev ? ev->Reason : ""));
}

// ---------------------------------------------------------------------------
// 2. When nodesWithQuery / total < 0.5 – must abort.
// ---------------------------------------------------------------------------
Y_UNIT_TEST(AbortWhenRatioBelowThreshold) {
    TTestActorRuntime runtime(1, false);
    runtime.Initialize(NKikimr::TAppPrepare().Unwrap());

    TActorId edgeActor = runtime.AllocateEdgeActor();

    // 1 task running on 10 tenant nodes → ratio = 1/10 = 0.1 < 0.5 → abort.
    TActorId manager = runtime.Register(
        CreateStreamingQueryNodesManager(
            edgeActor,
            "/Root/test",
            "query-2",
            MakeTopicSourceGraph(1),
            TDuration::Hours(1), TDuration::Zero()));

    runtime.EnableScheduleForActor(manager, true);
    runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(50));

    // Trigger check cycle.
    runtime.Send(new IEventHandle(manager, edgeActor,
        new TEvents::TEvWakeup(/* tag */ 1)));
    runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(50));

    InjectTaskStates(runtime, manager, {1});
    // Inject: 10 tenant nodes, only 1 node hosting our query.
    InjectLookupResult(runtime, manager, {1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
    runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(50));

    TAutoPtr<IEventHandle> handle;
    auto* ev = runtime.GrabEdgeEventRethrow<TEvStreamingQueryNodesManager::TEvAbortQuery>(
        handle, TDuration::Seconds(2));
    UNIT_ASSERT_C(ev != nullptr, "Expected TEvAbortQuery but got none");
    UNIT_ASSERT(!ev->Reason.empty());
}

// ---------------------------------------------------------------------------
// 3. Compute states identify the nodes hosting query tasks.
// ---------------------------------------------------------------------------
Y_UNIT_TEST(ComputeStatesDetermineQueryNodes) {
    TTestActorRuntime runtime(1, false);
    runtime.Initialize(NKikimr::TAppPrepare().Unwrap());

    TActorId edgeActor = runtime.AllocateEdgeActor();

    // 10 tasks, but only 2 distinct nodes running them → ratio = 2/10 = 0.2 < 0.5
    TActorId manager = runtime.Register(
        CreateStreamingQueryNodesManager(
            edgeActor,
            "/Root/test",
            "query-3",
            MakeTopicSourceGraph(10),
            TDuration::Hours(1), TDuration::Zero()));

    runtime.EnableScheduleForActor(manager, true);
    runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(50));

    // Ten tasks report states from nodes 1 and 2 only.
    InjectTaskStates(runtime, manager, {1, 1, 2, 2, 1, 2, 1, 2, 1, 2});
    runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(50));

    // Trigger check cycle.
    runtime.Send(new IEventHandle(manager, edgeActor,
        new TEvents::TEvWakeup(/* tag */ 1)));
    runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(50));

    // Inject 10 tenant nodes total.
    InjectLookupResult(runtime, manager, {1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
    runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(50));

    TAutoPtr<IEventHandle> handle;
    auto* ev = runtime.GrabEdgeEventRethrow<TEvStreamingQueryNodesManager::TEvAbortQuery>(
        handle, TDuration::Seconds(2));
    UNIT_ASSERT_C(ev != nullptr, "Expected TEvAbortQuery but got none");
}

// ---------------------------------------------------------------------------
// 4. Abort is sent only once even on multiple check cycles.
// ---------------------------------------------------------------------------
Y_UNIT_TEST(AbortSentOnlyOnce) {
    TTestActorRuntime runtime(1, false);
    runtime.Initialize(NKikimr::TAppPrepare().Unwrap());

    TActorId edgeActor = runtime.AllocateEdgeActor();

    TActorId manager = runtime.Register(
        CreateStreamingQueryNodesManager(
            edgeActor,
            "/Root/test",
            "query-4",
            MakeTopicSourceGraph(1),
            TDuration::Hours(1), TDuration::Zero()));

    runtime.EnableScheduleForActor(manager, true);
    runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(50));

    auto triggerBadCheck = [&]() {
        InjectTaskStates(runtime, manager, {1});
        runtime.Send(new IEventHandle(manager, edgeActor,
            new TEvents::TEvWakeup(/* tag */ 1)));
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(50));
        // 1 task, 10 nodes total → abort
        InjectLookupResult(runtime, manager, {1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(50));
    };

    triggerBadCheck(); // first bad check → abort sent
    triggerBadCheck(); // second bad check → should be suppressed

    // Collect all abort events within a short window.
    int abortCount = 0;
    while (true) {
        TAutoPtr<IEventHandle> handle;
        auto* ev = runtime.GrabEdgeEventRethrow<TEvStreamingQueryNodesManager::TEvAbortQuery>(
            handle, TDuration::MilliSeconds(100));
        if (!ev) {
            break;
        }
        ++abortCount;
    }
    UNIT_ASSERT_VALUES_EQUAL_C(abortCount, 1, "Expected exactly one TEvAbortQuery");
}

// ---------------------------------------------------------------------------
// 5. A failed lookup result does not trigger an abort.
// ---------------------------------------------------------------------------
Y_UNIT_TEST(FailedLookupDoesNotAbort) {
    TTestActorRuntime runtime(1, false);
    runtime.Initialize(NKikimr::TAppPrepare().Unwrap());

    TActorId edgeActor = runtime.AllocateEdgeActor();

    TActorId manager = runtime.Register(
        CreateStreamingQueryNodesManager(
            edgeActor,
            "/Root/test",
            "query-5",
            MakeTopicSourceGraph(1),
            TDuration::Hours(1), TDuration::Zero()));

    runtime.EnableScheduleForActor(manager, true);
    runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(50));

    runtime.Send(new IEventHandle(manager, edgeActor,
        new TEvents::TEvWakeup(/* tag */ 1)));
    runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(50));

    InjectLookupFailure(runtime, manager);
    runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(50));

    TAutoPtr<IEventHandle> handle;
    auto* ev = runtime.GrabEdgeEventRethrow<TEvStreamingQueryNodesManager::TEvAbortQuery>(
        handle, TDuration::MilliSeconds(100));
    UNIT_ASSERT_C(ev == nullptr, "Abort should not be sent after a failed lookup");
}

// ---------------------------------------------------------------------------
// 6. Exactly-half ratio (boundary): nodesWithQuery * 2 == totalNodes → no abort.
// ---------------------------------------------------------------------------
Y_UNIT_TEST(NoAbortAtExactlyHalf) {
    TTestActorRuntime runtime(1, false);
    runtime.Initialize(NKikimr::TAppPrepare().Unwrap());

    TActorId edgeActor = runtime.AllocateEdgeActor();

    // 5 tasks on 5 nodes, 10 total nodes.
    // 5 * 2 == 10 → NOT less than 10 → no abort
    TActorId manager = runtime.Register(
        CreateStreamingQueryNodesManager(
            edgeActor,
            "/Root/test",
            "query-6",
            MakeTopicSourceGraph(5),
            TDuration::Hours(1), TDuration::Zero()));

    runtime.EnableScheduleForActor(manager, true);
    runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(50));

    runtime.Send(new IEventHandle(manager, edgeActor,
        new TEvents::TEvWakeup(/* tag */ 1)));
    runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(50));

    InjectTaskStates(runtime, manager, {1, 2, 3, 4, 5});
    InjectLookupResult(runtime, manager, {1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
    runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(50));

    TAutoPtr<IEventHandle> handle;
    auto* ev = runtime.GrabEdgeEventRethrow<TEvStreamingQueryNodesManager::TEvAbortQuery>(
        handle, TDuration::MilliSeconds(100));
    UNIT_ASSERT_C(ev == nullptr, "Should not abort at exactly 50% ratio");
}

// ---------------------------------------------------------------------------
// 7. Task count > 2 * nodesWithQuery while ratio >= 0.5 → no abort (just warn).
// ---------------------------------------------------------------------------
Y_UNIT_TEST(NoAbortWhenManyTasksOnFewNodesButRatioOk) {
    TTestActorRuntime runtime(1, false);
    runtime.Initialize(NKikimr::TAppPrepare().Unwrap());

    TActorId edgeActor = runtime.AllocateEdgeActor();

    // 100 tasks on four nodes, with four tenant nodes total.
    // ratio = 4/4 = 1.0 → no abort
    // taskCount (100) > 2 * nodesWithQuery (8) → just a warning, not an abort
    TActorId manager = runtime.Register(
        CreateStreamingQueryNodesManager(
            edgeActor,
            "/Root/test",
            "query-7",
            MakeTopicSourceGraph(100),
            TDuration::Hours(1), TDuration::Zero()));

    runtime.EnableScheduleForActor(manager, true);
    runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(50));

    runtime.Send(new IEventHandle(manager, edgeActor,
        new TEvents::TEvWakeup(/* tag */ 1)));
    runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(50));

    InjectTaskStates(runtime, manager, {1, 2, 3, 4});
    InjectLookupResult(runtime, manager, {1, 2, 3, 4});
    runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(50));

    TAutoPtr<IEventHandle> handle;
    auto* ev = runtime.GrabEdgeEventRethrow<TEvStreamingQueryNodesManager::TEvAbortQuery>(
        handle, TDuration::MilliSeconds(100));
    UNIT_ASSERT_C(ev == nullptr, "Should not abort: ratio is ok even if tasks > 2*nodes");
}

// ---------------------------------------------------------------------------
// 8. Non-topic tasks do not participate in the node coverage check.
// ---------------------------------------------------------------------------
Y_UNIT_TEST(NonTopicTasksAreIgnored) {
    TTestActorRuntime runtime(1, false);
    runtime.Initialize(NKikimr::TAppPrepare().Unwrap());

    TActorId edgeActor = runtime.AllocateEdgeActor();
    TActorId manager = runtime.Register(
        CreateStreamingQueryNodesManager(
            edgeActor,
            "/Root/test",
            "query-8",
            MakeGraphWithTopicAndNonTopicTasks(5, 100),
            TDuration::Hours(1), TDuration::Zero()));

    runtime.EnableScheduleForActor(manager, true);
    runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(50));

    // Topic readers cover half of the tenant nodes. Non-topic tasks are all on
    // one node and must not change the result.
    InjectTaskStates(runtime, manager, {1, 2, 3, 4, 5});
    InjectTaskStates(runtime, manager, TVector<ui32>(100, 1), 5);
    runtime.Send(new IEventHandle(manager, edgeActor, new TEvents::TEvWakeup(/* tag */ 1)));
    runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(50));
    InjectLookupResult(runtime, manager, {1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
    runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(50));

    TAutoPtr<IEventHandle> handle;
    auto* ev = runtime.GrabEdgeEventRethrow<TEvStreamingQueryNodesManager::TEvAbortQuery>(
        handle, TDuration::MilliSeconds(100));
    UNIT_ASSERT_C(ev == nullptr, "Non-topic tasks must be ignored");
}

} // Y_UNIT_TEST_SUITE

} // namespace NFq
