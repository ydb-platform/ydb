#include <ydb/library/yql/dq/task_scheduler/task_scheduler.h>
#include <ydb/library/actors/testlib/test_runtime.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/hash_set.h>

#include <atomic>
#include <thread>

using namespace NYql::NDq;

namespace {

struct TTestRuntime : NActors::TTestActorRuntimeBase {
    TTestRuntime() : NActors::TTestActorRuntimeBase(1, true) { InitNodes(); }
    ~TTestRuntime() override { CleanupNodes(); }
};

struct TManualTask {
    ui32 Enqueues = 0;
    ui32 Releases = 0;
    std::shared_ptr<TTaskSchedulingHandle> Task =
        std::make_shared<TTaskSchedulingHandle>(NActors::TActorId{}, NActors::TActorId{}, [this](auto) { ++Enqueues; }, [this] { ++Releases; });

    ui64 Start() {
        auto generation = Task->TryDispatch();
        UNIT_ASSERT(generation);
        UNIT_ASSERT(Task->Start(*generation));
        return *generation;
    }
};

} // namespace

Y_UNIT_TEST_SUITE(DqTaskScheduler) {
    Y_UNIT_TEST(UninstrumentedChildSpillStillWakesAnInputWait) {
        for (bool observedInput : {false, true}) {
            for (bool completesBeforeParking : {false, true}) {
                TManualTask t;
                t.Task->RequestExecution();
                t.Start();
                const auto scope = t.Task->BeginInputWait();
                if (observedInput) {
                    t.Task->Await(InputDependency(1), t.Task->Observe(InputDependency(1)));
                }
                if (completesBeforeParking) {
                    t.Task->Signal(SpillingDependency);
                }
                t.Task->ReportInputWait(scope, 20, ETaskInputSide::Right);
                t.Task->Complete({.RunnerInvoked = true});
                UNIT_ASSERT_VALUES_EQUAL(t.Enqueues, completesBeforeParking ? 2 : 1);
                if (!completesBeforeParking) {
                    t.Task->Signal(SpillingDependency);
                }
                UNIT_ASSERT_VALUES_EQUAL(t.Enqueues, 2);
                t.Task->Cancel();
            }
        }
    }

    Y_UNIT_TEST(UninstrumentedSpillAfterChannelProbeIsNotLost) {
        TManualTask t;
        t.Task->RequestExecution();
        t.Start();
        t.Task->Await(InputDependency(1), t.Task->Observe(InputDependency(1)));
        t.Task->Signal(SpillingDependency);
        t.Task->Complete({.RunnerInvoked = true});
        UNIT_ASSERT_VALUES_EQUAL(t.Enqueues, 2);
        t.Task->Cancel();
    }

    Y_UNIT_TEST(DispatcherKnowsWholeGraphAndActualLocalTasks) {
        TTestRuntime runtime;
        auto scheduler = CreateDqTaskScheduler(runtime.SingleSys(), 0, MakeIntrusive<NMonitoring::TDynamicCounters>(),
                                               {.Shards = 2, .MaxInFlightPerShard = 1, .MaxRowsPerRun = 16});
        auto graph = std::make_shared<const TQueryTaskGraph>(
            TVector<TTaskGraphNode>{{1, 10, 99, 0, 1, true}, {2, 20, 99, 2, 1},
                                   {3, 10, 99, 0, 1, true}, {4, 30, 99, 1, 1}},
            TVector<TTaskGraphChannel>{{1, 1, 2, 0, 0}, {2, 3, 2, 0, 1}, {3, 2, 4, 0, 0}, {4, 4, 0, 0, 0}});
        const auto query = runtime.AllocateEdgeActor();
        const auto actor = runtime.AllocateEdgeActor();
        const auto reply = runtime.AllocateEdgeActor();
        auto registration = scheduler->RegisterGraph(query, graph, {2, 4});
        auto task = scheduler->Register(actor, query, 2);
        task->RequestExecution();
        auto permit = runtime.GrabEdgeEvent<TEvTaskRunPermit>(actor, TDuration::Seconds(5));
        UNIT_ASSERT(permit);
        UNIT_ASSERT(task->Start(permit->Get()->Generation));
        const auto scope = task->BeginInputWait();
        task->Await(InputDependency(2), task->Observe(InputDependency(2)));
        task->ReportInputWait(scope, 700, ETaskInputSide::Right);
        task->Complete({.InputRows = 3, .RunnerInvoked = true});

        scheduler->DescribeQuery(query, reply);
        auto description = runtime.GrabEdgeEvent<TEvQueryGraph>(reply, TDuration::Seconds(5));
        UNIT_ASSERT(description);
        const auto& snapshot = description->Get()->Snapshot;
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Graph->Tasks.size(), 4);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Graph->Channels.size(), 4);
        UNIT_ASSERT(snapshot.LocalTaskIds == TVector<ui64>({2, 4}));
        UNIT_ASSERT_VALUES_EQUAL(snapshot.NodeId, actor.NodeId());
        UNIT_ASSERT_VALUES_EQUAL(snapshot.ManagedTasks.size(), 1);
        const auto& state = snapshot.ManagedTasks.front();
        UNIT_ASSERT_VALUES_EQUAL(state.TaskId, 2);
        UNIT_ASSERT(state.State == ETaskSchedulingState::Blocked);
        UNIT_ASSERT(state.StopReason == ETaskStopReason::Waiting);
        UNIT_ASSERT_VALUES_EQUAL(state.Progress.InputRows, 3);
        UNIT_ASSERT_VALUES_EQUAL(state.Waits.size(), 1);
        UNIT_ASSERT(state.Waits.front().InputSide == ETaskInputSide::Right);
        UNIT_ASSERT(state.Waits.front().Origin == ETaskWaitOrigin::Operator);
        UNIT_ASSERT(snapshot.Graph->GetInputs(2) == TVector<ui64>({1, 2}));
        UNIT_ASSERT(snapshot.Graph->IsResultTask(4));
        UNIT_ASSERT(!snapshot.Graph->IsResultTask(2));
        task->Cancel();
    }

    Y_UNIT_TEST(GraphRegistrationIsReleasedWithoutStartingTasks) {
        TTestRuntime runtime;
        auto scheduler = CreateDqTaskScheduler(runtime.SingleSys(), 0, MakeIntrusive<NMonitoring::TDynamicCounters>(), {});
        const auto query = runtime.AllocateEdgeActor();
        auto graph = std::make_shared<const TQueryTaskGraph>(TVector<TTaskGraphNode>{{1, 1, 0, 0, 0}},
                                                          TVector<TTaskGraphChannel>{});
        auto registration = scheduler->RegisterGraph(query, graph, {1});
        scheduler->DescribeQuery(query, query);
        auto description = runtime.GrabEdgeEvent<TEvQueryGraph>(query, TDuration::Seconds(5));
        UNIT_ASSERT(description && description->Get()->Snapshot.Graph);
        registration.reset();
        scheduler->DescribeQuery(query, query);
        description = runtime.GrabEdgeEvent<TEvQueryGraph>(query, TDuration::Seconds(5));
        UNIT_ASSERT(description && !description->Get()->Snapshot.Graph);
    }

    Y_UNIT_TEST(ChildSpillReasonSurvivesOuterRightInputScope) {
        TManualTask t;
        t.Task->RequestExecution();
        t.Start();
        const auto input = t.Task->BeginInputWait();
        const auto spill = t.Task->NewAsyncDependency();
        TTaskWaitDescription wait;
        wait.Reason = ETaskWaitReason::SpillRead;
        wait.Origin = ETaskWaitOrigin::Operator;
        wait.Precision = ETaskWaitPrecision::Exact;
        wait.OperatorId = 10;
        wait.Condition = TTaskWaitCondition::Any({spill});
        t.Task->ReportWait(std::move(wait));
        t.Task->ReportInputWait(input, 20, ETaskInputSide::Right);
        t.Task->Complete({.RunnerInvoked = true});
        const auto snapshot = t.Task->GetSnapshot();
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Waits.size(), 1);
        UNIT_ASSERT(snapshot.Waits[0].Reason == ETaskWaitReason::SpillRead);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Waits[0].OperatorId, 10);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Waits[0].InputPath.size(), 1);
        UNIT_ASSERT(snapshot.Waits[0].InputPath[0].Side == ETaskInputSide::Right);
        t.Task->Signal(InputDependency(1));
        UNIT_ASSERT_VALUES_EQUAL(t.Enqueues, 1);
        t.Task->Signal(spill.Id);
        UNIT_ASSERT_VALUES_EQUAL(t.Enqueues, 2);
        t.Task->Cancel();
    }

    Y_UNIT_TEST(CoalescesRequestsAndRejectsDuplicatePermits) {
        TManualTask t;
        t.Task->RequestExecution();
        t.Task->RequestExecution();
        UNIT_ASSERT_VALUES_EQUAL(t.Enqueues, 1);
        auto generation = t.Start();
        UNIT_ASSERT(!t.Task->Start(generation));
        UNIT_ASSERT(!t.Task->TryDispatch());
        t.Task->Complete({});
        UNIT_ASSERT_VALUES_EQUAL(t.Releases, 1);
        UNIT_ASSERT(!t.Task->TryDispatch());
    }

    Y_UNIT_TEST(UnrelatedChannelDoesNotWakeBlockedTask) {
        TManualTask t;
        t.Task->RequestExecution();
        t.Start();
        auto version = t.Task->Observe(InputDependency(2));
        t.Task->Await(InputDependency(2), version);
        t.Task->Complete({});

        t.Task->Signal(InputDependency(1));
        UNIT_ASSERT_VALUES_EQUAL(t.Enqueues, 1);
        t.Task->Signal(InputDependency(2));
        UNIT_ASSERT_VALUES_EQUAL(t.Enqueues, 2);
        t.Start();
        t.Task->Complete({});
    }

    Y_UNIT_TEST(NotificationBetweenEmptyProbeAndParkingIsNotLost) {
        TManualTask t;
        t.Task->RequestExecution();
        t.Start();
        auto version = t.Task->Observe(InputDependency(1));
        // Producer runs after the consumer observed an empty channel but before
        // it publishes its wait condition.
        t.Task->Signal(InputDependency(1));
        t.Task->Await(InputDependency(1), version);
        t.Task->Complete({});
        UNIT_ASSERT_VALUES_EQUAL(t.Enqueues, 2);
        t.Start();
        t.Task->Cancel();
        UNIT_ASSERT_VALUES_EQUAL(t.Releases, 2);
    }

    Y_UNIT_TEST(ReadySetYieldWithoutARepeatedProbeStillWakes) {
        TManualTask t;
        t.Task->RequestExecution();
        t.Start();
        t.Task->Await(InputDependency(1), 0);
        t.Task->Complete({});
        t.Task->Signal(InputDependency(1));
        t.Start();
        // The ready set can skip all empty channels on the next Fetch.
        t.Task->BeginProbe();
        t.Task->Complete({});
        t.Task->Signal(InputDependency(1));
        UNIT_ASSERT_VALUES_EQUAL(t.Enqueues, 3);
        t.Start();
        t.Task->Cancel();
    }

    Y_UNIT_TEST(ReadySetNotificationDuringRunIsNotLost) {
        TManualTask t;
        t.Task->RequestExecution();
        t.Start();
        t.Task->BeginProbe();
        t.Task->Signal(InputDependency(1));
        t.Task->Complete({});
        UNIT_ASSERT_VALUES_EQUAL(t.Enqueues, 2);
        t.Start();
        t.Task->Cancel();
    }

    Y_UNIT_TEST(WaitForBothInputs) {
        TManualTask t;
        t.Task->RequestExecution();
        t.Start();
        auto wait = TTaskWaitCondition::All({{InputDependency(1), 0}, {InputDependency(2), 0}});
        t.Task->Complete({}, wait);
        t.Task->Signal(InputDependency(1));
        UNIT_ASSERT_VALUES_EQUAL(t.Enqueues, 1);
        t.Task->Signal(InputDependency(2));
        UNIT_ASSERT_VALUES_EQUAL(t.Enqueues, 2);
        t.Start();
        t.Task->Cancel();
    }

    Y_UNIT_TEST(FutureCompletesDuringExecution) {
        TManualTask t;
        t.Task->RequestExecution();
        t.Start();
        t.Task->AwaitCallback();
        t.Task->RequestExecution();
        t.Task->Complete({});
        UNIT_ASSERT_VALUES_EQUAL(t.Enqueues, 2);
        t.Start();
        t.Task->Cancel();
    }

    Y_UNIT_TEST(BudgetRequeuesWithoutExternalNotification) {
        TManualTask t;
        t.Task->RequestExecution();
        t.Start();
        t.Task->Complete({.OutputRows = 4096, .RunnerInvoked = true, .BudgetExhausted = true});
        UNIT_ASSERT_VALUES_EQUAL(t.Enqueues, 2);
        UNIT_ASSERT(t.Task->GetPriority() == ETaskPriority::Producer);
        t.Start();
        t.Task->Complete({.InputRows = 4096, .RunnerInvoked = true});
        UNIT_ASSERT(t.Task->GetPriority() == ETaskPriority::Drain);
    }

    Y_UNIT_TEST(RunnerFinishStillAllowsOutputFinishNotifications) {
        TManualTask t;
        t.Task->RequestExecution();
        t.Start();
        t.Task->Await(InputDependency(1), 0);
        t.Task->Complete({.RunnerInvoked = true, .Finished = true});
        t.Task->Signal(OutputDependency(1));
        UNIT_ASSERT_VALUES_EQUAL(t.Enqueues, 2);
        t.Start();
        t.Task->Cancel();
    }

    Y_UNIT_TEST(CancellationReleasesPermitExactlyOnce) {
        TManualTask t;
        t.Task->RequestExecution();
        auto generation = t.Task->TryDispatch();
        UNIT_ASSERT(generation);
        t.Task->Cancel();
        t.Task->Cancel();
        UNIT_ASSERT(!t.Task->Start(*generation));
        t.Task->Complete({});
        UNIT_ASSERT_VALUES_EQUAL(t.Releases, 1);
    }

    Y_UNIT_TEST(ConcurrentNotificationsCreateOneReadyEntry) {
        std::atomic<ui32> enqueues = 0;
        auto task = std::make_shared<TTaskSchedulingHandle>(NActors::TActorId{}, NActors::TActorId{}, [&](auto) { ++enqueues; }, [] {});
        task->RequestExecution();
        UNIT_ASSERT(task->Start(*task->TryDispatch()));
        task->Await(InputDependency(1), 0);
        task->Complete({});
        std::vector<std::thread> threads;
        for (ui32 i = 0; i < 8; ++i) {
            threads.emplace_back([&] {
                for (ui32 j = 0; j < 1000; ++j) {
                    task->Signal(InputDependency(1));
                }
            });
        }
        for (auto& thread : threads) {
            thread.join();
        }
        UNIT_ASSERT_VALUES_EQUAL(enqueues.load(), 2);
        task->Cancel();
    }

    Y_UNIT_TEST(QueryTasksShareDispatcherAndQueriesUseMultipleShards) {
        TTestRuntime runtime;
        auto scheduler = CreateDqTaskScheduler(runtime.SingleSys(), 0, MakeIntrusive<NMonitoring::TDynamicCounters>(),
                                               {.Shards = 4, .MaxInFlightPerShard = 1, .MaxRowsPerRun = 16});
        const auto query = runtime.AllocateEdgeActor();
        NActors::TActorId dispatcher;
        for (ui32 i = 0; i < 32; ++i) {
            const auto actor = runtime.AllocateEdgeActor();
            auto task = scheduler->Register(actor, query);
            UNIT_ASSERT(task->Query == query);
            task->RequestExecution();
            auto permit = runtime.GrabEdgeEvent<TEvTaskRunPermit>(actor, TDuration::Seconds(5));
            UNIT_ASSERT(permit);
            if (i == 0) {
                dispatcher = permit->Sender;
            }
            UNIT_ASSERT_C(permit->Sender == dispatcher, "One query was split across dispatchers");
            UNIT_ASSERT(task->Start(permit->Get()->Generation));
            task->Complete({});
            task->Cancel();
        }

        THashSet<NActors::TActorId> dispatchers;
        for (ui32 i = 0; i < 32; ++i) {
            const auto actor = runtime.AllocateEdgeActor();
            auto task = scheduler->Register(actor, actor);
            task->RequestExecution();
            auto permit = runtime.GrabEdgeEvent<TEvTaskRunPermit>(actor, TDuration::Seconds(5));
            UNIT_ASSERT(permit);
            dispatchers.insert(permit->Sender);
            UNIT_ASSERT(task->Start(permit->Get()->Generation));
            task->Complete({});
            task->Cancel();
        }
        UNIT_ASSERT_C(dispatchers.size() > 1, "Independent queries all used one dispatcher");
    }

    Y_UNIT_TEST(ProducerGetsTurnsUnderContinuousDrainLoad) {
        TTestRuntime runtime;
        auto scheduler = CreateDqTaskScheduler(runtime.SingleSys(), 0, MakeIntrusive<NMonitoring::TDynamicCounters>(),
                                               {.Shards = 1, .MaxInFlightPerShard = 1, .MaxRowsPerRun = 16});
        auto edge = runtime.AllocateEdgeActor();
        auto drain = scheduler->Register(edge, edge);
        auto producer = scheduler->Register(edge, edge);
        drain->RequestExecution();
        producer->RequestExecution();
        ui32 drainRuns = 0;
        ui32 producerRuns = 0;
        for (ui32 i = 0; i < 30; ++i) {
            auto permit = runtime.GrabEdgeEvent<TEvTaskRunPermit>(edge, TDuration::Seconds(5));
            UNIT_ASSERT(permit);
            auto task = permit->Get()->Task;
            UNIT_ASSERT(task->Start(permit->Get()->Generation));
            const bool consuming = task == drain;
            drainRuns += consuming;
            producerRuns += !consuming;
            task->Complete({.InputRows = consuming ? 1u : 0u, .OutputRows = 1, .RunnerInvoked = true, .BudgetExhausted = true});
        }
        UNIT_ASSERT_C(producerRuns >= 4, producerRuns);
        UNIT_ASSERT_C(drainRuns > producerRuns, drainRuns << " / " << producerRuns);
        drain->Cancel();
        producer->Cancel();
    }

    Y_UNIT_TEST(DispatchesThroughActorMailboxesAndBoundsOutstandingPermits) {
        TTestRuntime runtime;
        auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        auto scheduler =
            CreateDqTaskScheduler(runtime.SingleSys(), 0, counters, {.Shards = 1, .MaxInFlightPerShard = 1, .MaxRowsPerRun = 16});
        auto edge = runtime.AllocateEdgeActor();
        auto first = scheduler->Register(edge, edge);
        auto second = scheduler->Register(edge, edge);
        first->RequestExecution();
        auto permit = runtime.GrabEdgeEvent<TEvTaskRunPermit>(edge, TDuration::Seconds(5));
        UNIT_ASSERT(permit);
        UNIT_ASSERT(permit->Get()->Task == first);
        UNIT_ASSERT(first->Start(permit->Get()->Generation));

        second->RequestExecution();
        // Releasing the first permit is what makes dispatch of the next task
        // possible.
        first->Complete({.InputRows = 7, .CpuTimeUs = 11, .RunnerInvoked = true});
        permit = runtime.GrabEdgeEvent<TEvTaskRunPermit>(edge, TDuration::Seconds(5));
        UNIT_ASSERT(permit);
        UNIT_ASSERT(permit->Get()->Task == second);
        UNIT_ASSERT(second->Start(permit->Get()->Generation));
        second->Complete({.CpuTimeUs = 3, .RunnerInvoked = true});

        auto group = counters->GetSubgroup("component", "TaskSchedulerPoC");
        UNIT_ASSERT_VALUES_EQUAL(group->GetCounter("Runs", true)->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(group->GetCounter("InputRows", true)->Val(), 7);
        UNIT_ASSERT_VALUES_EQUAL(group->GetCounter("NoObservedProgressRuns", true)->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(group->GetCounter("NoObservedProgressCpuUs", true)->Val(), 3);
        first->Cancel();
        second->Cancel();
    }
}
