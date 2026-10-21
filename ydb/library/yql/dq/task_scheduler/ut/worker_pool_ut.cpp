#include <library/cpp/json/json_reader.h>
#include <ydb/library/yql/dq/task_scheduler/worker_pool.h>
#include <ydb/library/actors/testlib/test_runtime.h>
#include <library/cpp/testing/unittest/registar.h>

using namespace NYql::NDq;
using namespace NActors;

namespace {
TActorId Id(ui64 id) { return TActorId(1, 0, id, 0); }

struct TEvExecuted : TEventLocal<TEvExecuted, EventSpaceBegin(TEvents::ES_PRIVATE) + 1701> {
    TEvExecuted(TActorId worker, TActorId query, TTaskSliceBudget budget)
        : Worker(worker), Query(query), Budget(budget) {}
    TActorId Worker, Query;
    TTaskSliceBudget Budget;
};

class TExecution : public IDqTaskExecution {
public:
    std::shared_ptr<const std::atomic<bool>> GetAvailability() const override { return Available; }
    bool IsAvailable() const override { return Available->load(); }
    bool TryAcquire() override {
        bool expected = true;
        return Available->compare_exchange_strong(expected, false);
    }
    void Release() override {
        Available->store(true);
        if (OnAvailable) { OnAvailable(); }
    }
    TTaskSliceResult RunSlice(const TTaskSliceBudget& budget) override {
        Budget = budget;
        Worker = TActivationContext::AsActorContext().SelfID;
        return {.Progress = {.OutputRows = 1, .RunnerInvoked = true, .Finished = true}};
    }
    void OnSliceFinished(TTaskSliceResult result) override {
        Task->Complete(result.Progress);
        Task->Cancel(true);
        System->Send(Edge, new TEvExecuted(Worker, Task->Query, Budget));
    }
    std::shared_ptr<std::atomic<bool>> Available = std::make_shared<std::atomic<bool>>(true);
    std::shared_ptr<TTaskSchedulingHandle> Task;
    std::function<void()> OnAvailable;
    TActorSystem* System = nullptr;
    TActorId Edge, Worker;
    TTaskSliceBudget Budget;
};

struct TQueues {
    TVector<TActorId> Wakeups;
    std::shared_ptr<TTaskWorkPool> Pool = std::make_shared<TTaskWorkPool>(2,
        [this](auto worker) { Wakeups.push_back(worker); });
    TVector<std::shared_ptr<TExecution>> Executions;
    ~TQueues() {
        for (const auto& execution : Executions) {
            execution->Task->Cancel();
        }
    }
    std::shared_ptr<TExecution> Add(TActorId query, ui64 taskId, ui64 actor) {
        auto task = std::make_shared<TTaskSchedulingHandle>(Id(actor), query,
            [pool = Pool](auto task) { pool->Enqueue(std::move(task)); }, [] {}, nullptr, false, taskId,
            [pool = Pool](auto state) { pool->Report(0, std::move(state)); });
        Pool->RegisterTask(task);
        auto execution = std::make_shared<TExecution>();
        execution->Task = task;
        execution->OnAvailable = [pool = Pool] { pool->NotifyAvailable(); };
        Pool->BindExecution(query, task->Actor, execution);
        Executions.push_back(execution);
        task->RequestExecution();
        return execution;
    }
    void Run(TTaskWorkPool::TClaim claim, bool again, ui64 cpuUs = 10) {
        UNIT_ASSERT(claim.Execution->TryAcquire());
        const auto generation = claim.Task->TryDispatch();
        UNIT_ASSERT(generation && claim.Task->Start(*generation));
        claim.Task->Complete({.OutputRows = 1, .RunnerInvoked = true, .BudgetExhausted = again});
        claim.Execution->Release();
        Pool->Finish(claim, cpuUs);
    }
};

struct TTestRuntime : TTestActorRuntimeBase {
    TTestRuntime() : TTestActorRuntimeBase(1, true) { InitNodes(); }
    ~TTestRuntime() override { CleanupNodes(); }
};
} // namespace

Y_UNIT_TEST_SUITE(DqTaskWorkerPool) {
    Y_UNIT_TEST(NearestReadyTaskWinsAcrossRemoteGraphEdges) {
        TQueues t;
        auto graph = std::make_shared<const TQueryTaskGraph>(
            TVector<TTaskGraphNode>{{1, 1, 1, 0, 1, true}, {2, 2, 99, 1, 1}, {3, 3, 1, 1, 1}},
            TVector<TTaskGraphChannel>{{1, 1, 2}, {2, 2, 3}, {3, 3, 0}});
        UNIT_ASSERT_VALUES_EQUAL(*graph->GetResultDistance(1), 2);
        UNIT_ASSERT_VALUES_EQUAL(*graph->GetResultDistance(3), 0);
        t.Pool->RegisterGraph(Id(100), 0, graph, {1, 3});
        t.Add(Id(100), 1, 1);
        t.Add(Id(100), 3, 3);
        auto first = t.Pool->Take({}, 100);
        UNIT_ASSERT(first && first->Task->TaskId == 3);
        t.Run(std::move(*first), false);
        auto next = t.Pool->Take({}, 100);
        UNIT_ASSERT(next && next->Task->TaskId == 1);
        t.Run(std::move(*next), false);
    }

    Y_UNIT_TEST(QueryCpuServiceAndInFlightReservationsAffectSelection) {
        TQueues t;
        t.Add(Id(100), 1, 1);
        t.Add(Id(100), 2, 2);
        t.Add(Id(200), 1, 3);
        auto first = t.Pool->Take({}, 100);
        UNIT_ASSERT(first && first->Task->Query == Id(100));
        auto concurrent = t.Pool->Take({}, 100);
        UNIT_ASSERT(concurrent && concurrent->Task->Query == Id(200));
        t.Run(std::move(*first), true, 100);
        t.Run(std::move(*concurrent), true, 10);
        auto next = t.Pool->Take({}, 100);
        UNIT_ASSERT(next && next->Task->Query == Id(200));
        t.Run(std::move(*next), false);
    }

    Y_UNIT_TEST(KnownReadyWorkPrecedesACloserConservativeCandidate) {
        TQueues t;
        auto graph = std::make_shared<const TQueryTaskGraph>(
            TVector<TTaskGraphNode>{{1, 1, 1, 0, 1, true}, {2, 2, 1, 1, 1}},
            TVector<TTaskGraphChannel>{{1, 1, 2}, {2, 2, 0}});
        t.Pool->RegisterGraph(Id(100), 0, graph, {1, 2});
        t.Add(Id(100), 1, 1);
        auto sink = t.Add(Id(100), 2, 2);
        auto first = t.Pool->Take({}, 100);
        UNIT_ASSERT(first && first->Task == sink->Task);
        UNIT_ASSERT(first->Execution->TryAcquire());
        const auto generation = first->Task->TryDispatch();
        UNIT_ASSERT(generation && first->Task->Start(*generation));
        first->Task->Await(InputDependency(1), first->Task->Observe(InputDependency(1)));
        first->Task->Complete({.RunnerInvoked = true});
        first->Execution->Release();
        t.Pool->Finish(*first, 10);
        sink->Task->Signal(InputDependency(1));
        auto next = t.Pool->Take({}, 100);
        UNIT_ASSERT(next && next->Task->TaskId == 1);
        t.Run(std::move(*next), false);
    }

    Y_UNIT_TEST(OneTaskCannotBeClaimedTwiceAndBusyEndpointsDoNotSpin) {
        TQueues t;
        auto execution = t.Add(Id(100), 1, 1);
        UNIT_ASSERT(execution->TryAcquire());
        UNIT_ASSERT(!t.Pool->Take(Id(900), 100));
        UNIT_ASSERT(t.Wakeups.empty());
        execution->Release();
        UNIT_ASSERT(t.Wakeups == TVector<TActorId>({Id(900)}));
        auto claim = t.Pool->Take({}, 100);
        UNIT_ASSERT(claim);
        UNIT_ASSERT(!t.Pool->Take({}, 100));
        t.Run(std::move(*claim), false);
    }

    Y_UNIT_TEST(QueryWeightsDistributeCpuService) {
        TQueues t;
        t.Add(Id(100), 1, 1);
        t.Add(Id(200), 1, 2);
        t.Pool->SetQueryWeight(Id(100), 2);
        ui32 first = 0, second = 0;
        for (ui32 i = 0; i < 60; ++i) {
            auto claim = t.Pool->Take({}, 100);
            UNIT_ASSERT(claim);
            (claim->Task->Query == Id(100) ? first : second)++;
            t.Run(std::move(*claim), true, 10);
        }
        UNIT_ASSERT_VALUES_EQUAL(first, 40);
        UNIT_ASSERT_VALUES_EQUAL(second, 20);
    }

    Y_UNIT_TEST(CancelledReadyTaskDoesNotWakeIdleWorkers) {
        TQueues t;
        auto execution = t.Add(Id(100), 1, 1);
        execution->Task->Cancel();
        UNIT_ASSERT(!t.Pool->Take(Id(900), 100));
        t.Pool->NotifyAvailable();
        UNIT_ASSERT(t.Wakeups.empty());
    }

    Y_UNIT_TEST(QuotaDenialParksTheWholeQueryUntilItsRetryDeadline) {
        TQueues t;
        t.Add(Id(100), 1, 1);
        t.Add(Id(100), 2, 2);
        t.Add(Id(200), 1, 3);
        const auto deadline = TMonotonic::Now() + TDuration::Hours(1);
        t.Pool->DeferQuery(Id(100), deadline);
        auto other = t.Pool->Take({}, 100);
        UNIT_ASSERT(other && other->Task->Query == Id(200));
        t.Run(std::move(*other), false);
        UNIT_ASSERT(!t.Pool->Take(Id(900), 100));
        t.Pool->ResumeQuery(Id(100), deadline - TDuration::MicroSeconds(1));
        UNIT_ASSERT(t.Wakeups.empty());
        t.Pool->ResumeQuery(Id(100), deadline);
        UNIT_ASSERT(t.Wakeups == TVector<TActorId>({Id(900)}));
        auto next = t.Pool->Take({}, 100);
        UNIT_ASSERT(next && next->Task->Query == Id(100));
        t.Run(std::move(*next), false);
    }

    Y_UNIT_TEST(UpstreamReadyTaskIsNotStarvedByContinuousResultWork) {
        TQueues t;
        auto graph = std::make_shared<const TQueryTaskGraph>(
            TVector<TTaskGraphNode>{{1, 1, 1, 0, 1, true}, {2, 2, 1, 1, 1}},
            TVector<TTaskGraphChannel>{{1, 1, 2}, {2, 2, 0}});
        t.Pool->RegisterGraph(Id(100), 0, graph, {1, 2});
        t.Add(Id(100), 1, 1);
        t.Add(Id(100), 2, 2);
        bool upstreamRan = false;
        for (ui32 i = 0; i < 65; ++i) {
            auto claim = t.Pool->Take({}, 100);
            UNIT_ASSERT(claim);
            upstreamRan |= claim->Task->TaskId == 1;
            t.Run(std::move(*claim), true);
        }
        UNIT_ASSERT(upstreamRan);
    }

    Y_UNIT_TEST(AReusableWorkerExecutesDifferentQueriesWithoutTaskActorPermits) {
        TTestRuntime runtime;
        auto scheduler = CreateDqTaskScheduler(runtime.SingleSys(), 0, MakeIntrusive<NMonitoring::TDynamicCounters>(),
            {.Trace = {.Enabled = true}, .Shards = 1, .MaxInFlightPerShard = 1, .UseWorkerPool = true, .WorkerSliceCpuUs = 100});
        const auto edge = runtime.AllocateEdgeActor();
        TVector<std::shared_ptr<TExecution>> executions;
        for (ui32 i = 0; i < 2; ++i) {
            auto execution = std::make_shared<TExecution>();
            const auto endpoint = runtime.AllocateEdgeActor();
            execution->Task = scheduler->Register(endpoint, endpoint, 1);
            execution->System = runtime.SingleSys();
            execution->Edge = edge;
            execution->OnAvailable = [scheduler] { scheduler->NotifyExecutionAvailable(); };
            scheduler->BindExecution(endpoint, endpoint, execution);
            executions.push_back(execution);
        }
        TActorId worker;
        for (auto execution : executions) {
            execution->Task->RequestExecution();
            auto done = runtime.GrabEdgeEvent<TEvExecuted>(edge, TDuration::Seconds(5));
            UNIT_ASSERT(done);
            UNIT_ASSERT(done->Get()->Worker != execution->Task->Actor);
            UNIT_ASSERT_VALUES_EQUAL(done->Get()->Budget.CpuUs, 100);
            UNIT_ASSERT(done->Get()->Budget.Deadline);
            if (worker) {
                UNIT_ASSERT(worker == done->Get()->Worker);
            }
            worker = done->Get()->Worker;
            UNIT_ASSERT(done->Get()->Query == execution->Task->Query);
            NJson::TJsonValue capture;
            UNIT_ASSERT(NJson::ReadJsonTree(scheduler->ExportTrace(ToString(execution->Task->Query)), &capture, true));
            bool selected = false, ran = false;
            for (const auto& event : capture["traceEvents"].GetArraySafe()) {
                if (event["name"].GetString() == "selected") {
                    selected = true;
                    UNIT_ASSERT_VALUES_EQUAL(event["args"]["worker"].GetString(), ToString(worker));
                    UNIT_ASSERT_VALUES_EQUAL(event["args"]["reserved_cpu_us"].GetUInteger(), 100);
                } else if (event["name"].GetString() == "worker_slice") {
                    ran = true;
                    UNIT_ASSERT(event["args"]["runner_invoked"].GetBoolean());
                    UNIT_ASSERT_VALUES_EQUAL(event["ph"].GetString(), "X");
                }
            }
            UNIT_ASSERT(selected && ran);
        }
    }
}
