#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/kqp/query_data/kqp_predictor.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/tx/conveyor_composite/service/service.h>
#include <ydb/core/tx/conveyor_composite/service/workers_pool.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>

#include <library/cpp/testing/unittest/registar.h>

#include <algorithm>
#include <numeric>
#include <set>

namespace NKikimr::NConveyorComposite {

namespace {

using TLinkConfig = std::pair<ESpecialTaskCategory, double>;

void AddPool(NKikimrConfig::TCompositeConveyorConfig& config, const std::optional<TString>& name,
    const std::vector<TLinkConfig>& links, const std::optional<double> workersCount = 1,
    const std::optional<double> fraction = std::nullopt, const std::optional<ui64> maxBatchSize = std::nullopt) {
    auto* pool = config.AddWorkerPools();
    if (name) {
        pool->SetName(*name);
    }
    if (workersCount) {
        pool->SetWorkersCount(*workersCount);
    }
    if (fraction) {
        pool->SetDefaultFractionOfThreadsCount(*fraction);
    }
    if (maxBatchSize) {
        pool->SetMaxBatchSize(*maxBatchSize);
    }
    for (const auto& [category, weight] : links) {
        auto* link = pool->AddLinks();
        link->SetCategory(::ToString(category));
        link->SetWeight(weight);
    }
}

NKikimrConfig::TCompositeConveyorConfig BuildTopologyConfig(const std::vector<std::vector<TLinkConfig>>& pools,
    const std::vector<double>& workersCounts = {}, const std::optional<ui64> maxBatchSize = std::nullopt) {
    NKikimrConfig::TCompositeConveyorConfig result;
    result.SetEnabled(true);
    for (ui64 poolIdx = 0; poolIdx < pools.size(); ++poolIdx) {
        AddPool(result, "pool-" + ::ToString(poolIdx + 1), pools[poolIdx],
            poolIdx < workersCounts.size() ? workersCounts[poolIdx] : 1, std::nullopt, maxBatchSize);
    }
    return result;
}

NKikimrConfig::TCompositeConveyorConfig BuildSinglePoolConfig(const double workersCount) {
    std::vector<TLinkConfig> links;
    for (const auto category : GetEnumAllValues<ESpecialTaskCategory>()) {
        links.emplace_back(category, 1);
    }
    return BuildTopologyConfig({links}, {workersCount});
}

constexpr ui64 InitialSubscriptionId = 42;
constexpr ui64 InitialNotificationCookie = 0x1234;

class TCounterTask: public NConveyor::ITask {
private:
    TAtomicCounter& Counter;
    const TDuration ExecutionDuration;

    void DoExecute(const std::shared_ptr<ITask>& /*taskPtr*/) override {
        const auto start = TMonotonic::Now();
        while (TMonotonic::Now() - start < ExecutionDuration) {
        }
        Counter.Inc();
    }

public:
    explicit TCounterTask(TAtomicCounter& counter, const TDuration executionDuration = TDuration::Zero())
        : Counter(counter)
        , ExecutionDuration(executionDuration) {
    }

    TString GetTaskClassIdentifier() const override {
        return "RUNTIME_UPDATE_MATRIX";
    }
};

class TFakeConfigsDispatcher: public NActors::TActorBootstrapped<TFakeConfigsDispatcher> {
private:
    const NActors::TActorId Sink;

    void Handle(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest::TPtr& ev, const NActors::TActorContext& ctx) {
        ctx.Send(Sink, new NActors::TEvents::TEvWakeup(ev->Get()->ConfigItemKinds.front()));
        ctx.Send(ev->Sender, new NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse());

        auto notification = MakeHolder<NConsole::TEvConsole::TEvConfigNotificationRequest>();
        notification->Record.SetSubscriptionId(InitialSubscriptionId);
        notification->Record.AddItemKinds(ev->Get()->ConfigItemKinds.front());
        ctx.Send(ev->Sender, notification.Release(), 0, InitialNotificationCookie);
    }

    void Handle(NConsole::TEvConsole::TEvConfigNotificationResponse::TPtr& ev, const NActors::TActorContext& ctx) {
        ctx.Send(ev->Forward(Sink));
    }

public:
    explicit TFakeConfigsDispatcher(const NActors::TActorId& sink)
        : Sink(sink) {
    }

    void Bootstrap() {
        Become(&TFakeConfigsDispatcher::StateWork);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest, Handle);
            HFunc(NConsole::TEvConsole::TEvConfigNotificationResponse, Handle);
        }
    }
};

class TRuntimeFixture {
private:
    ui64 NextUpdateId = 100;

public:
    NActors::TTestActorRuntime Runtime;
    NActors::TActorId Sink;
    NActors::TActorId Distributor;
    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;

    explicit TRuntimeFixture(const NKikimrConfig::TCompositeConveyorConfig& proto) {
        Runtime.Initialize(NKikimr::TAppPrepare().Unwrap());
        Sink = Runtime.AllocateEdgeActor();
        const auto dispatcher = Runtime.Register(new TFakeConfigsDispatcher(Sink));
        Runtime.RegisterService(NConsole::MakeConfigsDispatcherID(Runtime.GetNodeId(0)), dispatcher);

        auto config = NConfig::TConfig::BuildFromProto(proto);
        UNIT_ASSERT_C(!config.IsFail(), config.GetErrorMessage());
        Counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
        Distributor = Runtime.Register(CreateService(config.DetachResult(), Counters));
        Runtime.EnableScheduleForActor(Distributor, true);

        const auto subscription = Runtime.GrabEdgeEvent<NActors::TEvents::TEvWakeup>(Sink);
        UNIT_ASSERT_VALUES_EQUAL(
            subscription->Get()->Tag, (ui32)NKikimrConsole::TConfigItem::CompositeConveyorConfigItem);
        WaitForUpdate(InitialSubscriptionId, InitialNotificationCookie);
    }

    std::pair<ui64, ui64> SendUpdate(const NKikimrConfig::TCompositeConveyorConfig& config) {
        const ui64 id = NextUpdateId++;
        const ui64 cookie = 1000 + id;
        auto update = MakeHolder<NConsole::TEvConsole::TEvConfigNotificationRequest>();
        update->Record.SetSubscriptionId(id);
        update->Record.AddItemKinds((ui32)NKikimrConsole::TConfigItem::CompositeConveyorConfigItem);
        update->Record.MutableConfig()->MutableCompositeConveyorConfig()->CopyFrom(config);
        Runtime.Send(new NActors::IEventHandle(Distributor, Sink, update.Release(), 0, cookie));
        return {id, cookie};
    }

    void WaitForUpdate(const ui64 id, const ui64 cookie) {
        const auto response = Runtime.GrabEdgeEvent<NConsole::TEvConsole::TEvConfigNotificationResponse>(Sink);
        UNIT_ASSERT_VALUES_EQUAL(response->Get()->Record.GetSubscriptionId(), id);
        UNIT_ASSERT_VALUES_EQUAL(response->Cookie, cookie);
    }

    void Update(const NKikimrConfig::TCompositeConveyorConfig& config) {
        const auto [id, cookie] = SendUpdate(config);
        WaitForUpdate(id, cookie);
    }

    void RegisterProcess(const ESpecialTaskCategory category, const TString& scopeId, const ui64 processId) {
        Runtime.Send(Distributor, Sink,
            new TEvExecution::TEvRegisterProcess(TCPULimitsConfig(1000), category, scopeId, processId));
    }

    void Submit(TAtomicCounter& counter, const ESpecialTaskCategory category, const ui64 processId = 0,
        const TDuration executionDuration = TDuration::Zero()) {
        Runtime.Send(Distributor, Sink,
            new TEvExecution::TEvNewTask(std::make_shared<TCounterTask>(counter, executionDuration), category, processId));
    }

    ui64 Run(const ESpecialTaskCategory category) {
        TAtomicCounter counter;
        std::optional<ui64> workersPoolId;
        auto resultObserver = Runtime.AddObserver<TEvInternal::TEvTaskProcessedResult>([&](auto& ev) {
            for (const auto& result : ev->Get()->GetResults()) {
                if (result.GetCategory() == category) {
                    workersPoolId = ev->Get()->GetWorkersPoolId();
                }
            }
        });
        auto scheduleObserver = Runtime.AddObserver<TEvInternal::TEvNewTask>([&](auto& ev) {
            Runtime.EnableScheduleForActor(ev->Recipient, true);
        });

        Submit(counter, category);
        for (ui32 attempt = 0; attempt < 100 && (!workersPoolId || counter.Val() != 1); ++attempt) {
            Runtime.SimulateSleep(TDuration::MilliSeconds(1));
        }
        UNIT_ASSERT_VALUES_EQUAL(counter.Val(), 1);
        UNIT_ASSERT(workersPoolId);
        return *workersPoolId;
    }
};

void RunHeldTasks(TRuntimeFixture& fixture, std::vector<TAutoPtr<NActors::IEventHandle>>& heldTasks,
    TAtomicCounter& counter, const i64 expectedCount) {
    for (auto& task : heldTasks) {
        fixture.Runtime.EnableScheduleForActor(task->Recipient, true);
        fixture.Runtime.Send(task.Release(), 0, true);
    }
    for (ui32 attempt = 0; attempt < 1000 && counter.Val() != expectedCount; ++attempt) {
        fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
    }
    UNIT_ASSERT_VALUES_EQUAL(counter.Val(), expectedCount);
}

ui64 GetWeightCounter(const TRuntimeFixture& fixture, const TString& poolName, const ESpecialTaskCategory category) {
    return fixture.Counters->GetSubgroup("module_id", "COMPOSITE_CONVEYOR")
        ->GetSubgroup("pool_name", poolName)
        ->GetSubgroup("wp_category", ::ToString(category))
        ->GetCounter("Value/Weight")
        ->Val();
}

std::pair<ui32, ui32> RunWeightedPhase(TRuntimeFixture& fixture, const ESpecialTaskCategory blockerCategory) {
    TAtomicCounter counter;
    TAutoPtr<NActors::IEventHandle> heldTask;
    auto previousObserver = fixture.Runtime.SetObserverFunc([&](TAutoPtr<NActors::IEventHandle>& ev) {
        if (ev->GetTypeRewrite() == TEvInternal::TEvNewTask::EventType && !heldTask) {
            heldTask = ev.Release();
            return NActors::TTestActorRuntime::EEventAction::DROP;
        }
        return NActors::TTestActorRuntime::EEventAction::PROCESS;
    });
    fixture.Submit(counter, blockerCategory);
    fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
    UNIT_ASSERT(heldTask);

    for (ui32 i = 0; i < 50; ++i) {
        fixture.Submit(counter, ESpecialTaskCategory::Scan);
        fixture.Submit(counter, ESpecialTaskCategory::Insert);
    }
    fixture.Runtime.SetObserverFunc(previousObserver);

    std::vector<ESpecialTaskCategory> completed;
    auto resultObserver = fixture.Runtime.AddObserver<TEvInternal::TEvTaskProcessedResult>([&](auto& ev) {
        for (const auto& result : ev->Get()->GetResults()) {
            completed.emplace_back(result.GetCategory());
        }
    });
    fixture.Runtime.Send(heldTask.Release(), 0, true);
    for (ui32 attempt = 0; attempt < 1000 && counter.Val() != 101; ++attempt) {
        fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
    }
    UNIT_ASSERT_VALUES_EQUAL(counter.Val(), 101);
    UNIT_ASSERT_VALUES_EQUAL(completed.size(), 101);

    ui32 scanTasks = 0;
    ui32 insertTasks = 0;
    for (ui32 i = 1; i < Min<ui32>(completed.size(), 76); ++i) {
        scanTasks += completed[i] == ESpecialTaskCategory::Scan;
        insertTasks += completed[i] == ESpecialTaskCategory::Insert;
    }
    return {scanTasks, insertTasks};
}

std::vector<ui64> RunMaxBatchUpdatePhase(TRuntimeFixture& fixture,
    const NKikimrConfig::TCompositeConveyorConfig& config, const ui64 queuedTasksCount) {
    TAtomicCounter counter;
    TAutoPtr<NActors::IEventHandle> heldTask;
    auto previousObserver = fixture.Runtime.SetObserverFunc([&](TAutoPtr<NActors::IEventHandle>& ev) {
        if (ev->GetTypeRewrite() == TEvInternal::TEvNewTask::EventType && !heldTask) {
            heldTask = ev.Release();
            return NActors::TTestActorRuntime::EEventAction::DROP;
        }
        return NActors::TTestActorRuntime::EEventAction::PROCESS;
    });
    fixture.Submit(counter, ESpecialTaskCategory::Scan);
    fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
    UNIT_ASSERT(heldTask);

    for (ui64 i = 0; i < queuedTasksCount; ++i) {
        fixture.Submit(counter, ESpecialTaskCategory::Scan);
    }
    fixture.Update(config);
    fixture.Runtime.SetObserverFunc(previousObserver);

    std::vector<ui64> batchSizes;
    auto resultObserver = fixture.Runtime.AddObserver<TEvInternal::TEvTaskProcessedResult>([&](auto& ev) {
        if (ev->Get()->GetWorkersPoolId() == 1) {
            batchSizes.emplace_back(ev->Get()->GetResults().size());
        }
    });
    fixture.Runtime.Send(heldTask.Release(), 0, true);
    const i64 expectedTasksCount = queuedTasksCount + 1;
    for (ui32 attempt = 0; attempt < 1000 && counter.Val() != expectedTasksCount; ++attempt) {
        fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
    }
    UNIT_ASSERT_VALUES_EQUAL(counter.Val(), expectedTasksCount);
    return batchSizes;
}

Y_UNIT_TEST_SUITE(TCompositeConveyorRuntimeUpdate) {

    Y_UNIT_TEST(CpuIdleReconcileMatrix) {
        // use grow/shrink loops without a retained-limit update.
        TRuntimeFixture fixture(BuildSinglePoolConfig(2));
        ui32 limitUpdates = 0;
        ui32 stoppedWorkers = 0;
        auto limitObserver = fixture.Runtime.AddObserver<TEvInternal::TEvWorkerCPULimitUpdated>([&](auto&) {
            ++limitUpdates;
        });
        auto stopObserver = fixture.Runtime.AddObserver<TEvInternal::TEvWorkerStopped>([&](auto&) {
            ++stoppedWorkers;
        });

        fixture.Update(BuildSinglePoolConfig(3));
        fixture.Update(BuildSinglePoolConfig(5));
        UNIT_ASSERT_VALUES_EQUAL(limitUpdates, 0);
        UNIT_ASSERT_VALUES_EQUAL(stoppedWorkers, 0);

        TAtomicCounter counter;
        std::vector<TAutoPtr<NActors::IEventHandle>> heldTasks;
        auto previousObserver = fixture.Runtime.SetObserverFunc([&](TAutoPtr<NActors::IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvInternal::TEvNewTask::EventType) {
                heldTasks.emplace_back(ev.Release());
                return NActors::TTestActorRuntime::EEventAction::DROP;
            }
            return NActors::TTestActorRuntime::EEventAction::PROCESS;
        });
        for (ui32 i = 0; i < 5; ++i) {
            fixture.Submit(counter, ESpecialTaskCategory::Scan);
        }
        fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL(heldTasks.size(), 5);
        fixture.Runtime.SetObserverFunc(previousObserver);
        RunHeldTasks(fixture, heldTasks, counter, 5);

        fixture.Update(BuildSinglePoolConfig(4));
        fixture.Update(BuildSinglePoolConfig(2));
        UNIT_ASSERT_VALUES_EQUAL(limitUpdates, 0);
        UNIT_ASSERT_VALUES_EQUAL(stoppedWorkers, 3);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Run(ESpecialTaskCategory::Scan), 1);
    }

    Y_UNIT_TEST(CpuEpsilonAndRepresentationUpdates) {
        // below Eps is ignored, above the boundary updates the actor.
        TRuntimeFixture fixture(BuildSinglePoolConfig(2.4));
        ui32 limitUpdates = 0;
        auto observer = fixture.Runtime.AddObserver<TEvInternal::TEvWorkerCPULimitUpdated>([&](auto&) {
            ++limitUpdates;
        });

        fixture.Update(BuildSinglePoolConfig(2.4 + TWorkersPool::Eps / 2));
        UNIT_ASSERT_VALUES_EQUAL(limitUpdates, 0);
        fixture.Update(BuildSinglePoolConfig(2.4 + TWorkersPool::Eps * 2));
        UNIT_ASSERT_VALUES_EQUAL(limitUpdates, 1);

        // switching representation with the same resolved limits is a runtime no-op.
        auto fractionConfig = BuildSinglePoolConfig(2.4 + TWorkersPool::Eps * 2);
        auto* pool = fractionConfig.MutableWorkerPools(0);
        pool->ClearWorkersCount();
        pool->SetDefaultFractionOfThreadsCount(
            (2.4 + TWorkersPool::Eps * 2) / NKqp::TStagePredictor::GetPossibleMaxLimitThreads());
        fixture.Update(fractionConfig);
        UNIT_ASSERT_VALUES_EQUAL(limitUpdates, 1);
    }

    Y_UNIT_TEST(CpuFractionalReconcileMatrix) {
        // retained fractional limits combine with grow and shrink.
        TRuntimeFixture fixture(BuildSinglePoolConfig(2.4));
        std::vector<ui64> limitUpdates;
        std::vector<ui64> stoppedWorkers;
        auto limitObserver = fixture.Runtime.AddObserver<TEvInternal::TEvWorkerCPULimitUpdated>([&](auto& ev) {
            limitUpdates.emplace_back(ev->Get()->WorkerIdx);
        });
        auto stopObserver = fixture.Runtime.AddObserver<TEvInternal::TEvWorkerStopped>([&](auto& ev) {
            stoppedWorkers.emplace_back(ev->Get()->WorkerIdx);
        });

        fixture.Update(BuildSinglePoolConfig(2.8));
        fixture.Update(BuildSinglePoolConfig(3.8));
        fixture.Update(BuildSinglePoolConfig(1.4));
        UNIT_ASSERT_VALUES_EQUAL(limitUpdates, std::vector<ui64>({2, 2, 1}));
        Sort(stoppedWorkers);
        UNIT_ASSERT_VALUES_EQUAL(stoppedWorkers, std::vector<ui64>({2, 3}));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Run(ESpecialTaskCategory::Scan), 1);
    }

    Y_UNIT_TEST(DefaultPoolCPUIsIndependent) {
        // updating an explicit pool must not update or stop synthetic default workers.
        auto initial = BuildTopologyConfig({{{ESpecialTaskCategory::Scan, 1}}}, {1});
        TRuntimeFixture fixture(initial);
        ui64 defaultPoolEvents = 0;
        auto limitObserver = fixture.Runtime.AddObserver<TEvInternal::TEvWorkerCPULimitUpdated>([&](auto& ev) {
            defaultPoolEvents += ev->Get()->WorkersPoolId == 0;
        });
        auto stopObserver = fixture.Runtime.AddObserver<TEvInternal::TEvWorkerStopped>([&](auto& ev) {
            defaultPoolEvents += ev->Get()->WorkersPoolId == 0;
        });

        fixture.Update(BuildTopologyConfig({{{ESpecialTaskCategory::Scan, 1}}}, {2}));
        UNIT_ASSERT_VALUES_EQUAL(defaultPoolEvents, 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Run(ESpecialTaskCategory::Scan), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Run(ESpecialTaskCategory::Compaction), 0);
    }

    Y_UNIT_TEST(WeightsUpdateInPlace) {
        auto initial = BuildTopologyConfig(
            {{{ESpecialTaskCategory::Scan, 1}, {ESpecialTaskCategory::Insert, 100}}}, {1});
        TRuntimeFixture fixture(initial);
        UNIT_ASSERT_VALUES_EQUAL(GetWeightCounter(fixture, "pool-1", ESpecialTaskCategory::Scan), 1);
        UNIT_ASSERT_VALUES_EQUAL(GetWeightCounter(fixture, "pool-1", ESpecialTaskCategory::Insert), 100);
        const auto [scanBefore, insertBefore] = RunWeightedPhase(fixture, ESpecialTaskCategory::Scan);
        UNIT_ASSERT(scanBefore > insertBefore);

        auto candidate = BuildTopologyConfig(
            {{{ESpecialTaskCategory::Scan, 100}, {ESpecialTaskCategory::Insert, 1}}}, {1});
        fixture.Update(candidate);
        UNIT_ASSERT_VALUES_EQUAL(GetWeightCounter(fixture, "pool-1", ESpecialTaskCategory::Scan), 100);
        UNIT_ASSERT_VALUES_EQUAL(GetWeightCounter(fixture, "pool-1", ESpecialTaskCategory::Insert), 1);
        const auto [scanAfter, insertAfter] = RunWeightedPhase(fixture, ESpecialTaskCategory::Insert);
        UNIT_ASSERT(insertAfter > scanAfter);
    }

    Y_UNIT_TEST(RemovedLinkResetsWeightCounter) {
        auto initial = BuildTopologyConfig(
            {{{ESpecialTaskCategory::Scan, 7}, {ESpecialTaskCategory::Insert, 1}}});
        TRuntimeFixture fixture(initial);
        UNIT_ASSERT_VALUES_EQUAL(GetWeightCounter(fixture, "pool-1", ESpecialTaskCategory::Scan), 7);

        auto withoutScan = BuildTopologyConfig({{{ESpecialTaskCategory::Insert, 1}}});
        fixture.Update(withoutScan);
        UNIT_ASSERT_VALUES_EQUAL(GetWeightCounter(fixture, "pool-1", ESpecialTaskCategory::Scan), 0);
    }

    Y_UNIT_TEST(RuntimeValidationIsAtomic) {
        auto initial = BuildTopologyConfig(
            {{{ESpecialTaskCategory::Scan, 1}, {ESpecialTaskCategory::Normalizer, 1}},
                {{ESpecialTaskCategory::Insert, 1}}},
            {1, 1});
        TRuntimeFixture fixture(initial);

        // Enabled is immutable.
        auto candidate = initial;
        candidate.SetEnabled(false);
        fixture.Update(candidate);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Run(ESpecialTaskCategory::Scan), 1);

        // a parse failure is atomic and does not block the next valid revision.
        candidate = initial;
        candidate.MutableWorkerPools(0)->MutableLinks(0)->SetWeight(0);
        candidate.MutableWorkerPools(1)->AddLinks()->SetCategory(::ToString(ESpecialTaskCategory::Scan));
        fixture.Update(candidate);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Run(ESpecialTaskCategory::Scan), 1);

        candidate = initial;
        candidate.MutableWorkerPools(0)->ClearLinks();
        auto* retainedLink = candidate.MutableWorkerPools(0)->AddLinks();
        retainedLink->SetCategory(::ToString(ESpecialTaskCategory::Normalizer));
        retainedLink->SetWeight(1);
        auto* movedLink = candidate.MutableWorkerPools(1)->AddLinks();
        movedLink->SetCategory(::ToString(ESpecialTaskCategory::Scan));
        movedLink->SetWeight(1);
        fixture.Update(candidate);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Run(ESpecialTaskCategory::Scan), 2);
    }

    Y_UNIT_TEST(PoolReorderKeepsRuntimeIdentity) {
        auto initial = BuildTopologyConfig(
            {{{ESpecialTaskCategory::Scan, 1}, {ESpecialTaskCategory::Normalizer, 1}},
                {{ESpecialTaskCategory::Insert, 1}}});
        TRuntimeFixture fixture(initial);

        auto candidate = initial;
        candidate.MutableWorkerPools()->SwapElements(0, 1);
        auto* pool1 = candidate.MutableWorkerPools(1);
        pool1->ClearLinks();
        auto* normalizerLink = pool1->AddLinks();
        normalizerLink->SetCategory(::ToString(ESpecialTaskCategory::Normalizer));
        normalizerLink->SetWeight(1);
        auto* scanLink = candidate.MutableWorkerPools(0)->AddLinks();
        scanLink->SetCategory(::ToString(ESpecialTaskCategory::Scan));
        scanLink->SetWeight(1);

        fixture.Update(candidate);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Run(ESpecialTaskCategory::Normalizer), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Run(ESpecialTaskCategory::Insert), 2);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Run(ESpecialTaskCategory::Scan), 2);
    }

    Y_UNIT_TEST(RemovedPoolFinishesAssignedTaskBeforeSlotIsReleased) {
        auto initial = BuildTopologyConfig(
            {{{ESpecialTaskCategory::Scan, 1}}, {{ESpecialTaskCategory::Insert, 1}}});
        TRuntimeFixture fixture(initial);

        TAtomicCounter oldCounter;
        TAutoPtr<NActors::IEventHandle> heldTask;
        TAutoPtr<NActors::IEventHandle> heldRetire;
        bool holdTask = true;
        bool holdRetire = true;
        auto previousObserver = fixture.Runtime.SetObserverFunc([&](TAutoPtr<NActors::IEventHandle>& ev) {
            if (holdTask && ev->GetTypeRewrite() == TEvInternal::TEvNewTask::EventType) {
                holdTask = false;
                heldTask = ev.Release();
                return NActors::TTestActorRuntime::EEventAction::DROP;
            }
            if (holdRetire && ev->GetTypeRewrite() == TEvInternal::TEvRetireWorker::EventType) {
                holdRetire = false;
                heldRetire = ev.Release();
                return NActors::TTestActorRuntime::EEventAction::DROP;
            }
            return NActors::TTestActorRuntime::EEventAction::PROCESS;
        });
        fixture.Submit(oldCounter, ESpecialTaskCategory::Insert);
        fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
        UNIT_ASSERT(heldTask);

        auto candidate = BuildTopologyConfig({{{ESpecialTaskCategory::Scan, 1}}});
        ui32 responses = 0;
        auto responseObserver = fixture.Runtime.AddObserver<NConsole::TEvConsole::TEvConfigNotificationResponse>([&](auto&) {
            ++responses;
        });
        const auto [id, cookie] = fixture.SendUpdate(candidate);
        fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
        UNIT_ASSERT(heldRetire);
        UNIT_ASSERT_VALUES_EQUAL(responses, 0);

        UNIT_ASSERT_VALUES_EQUAL(fixture.Run(ESpecialTaskCategory::Insert), 0);

        std::optional<ui64> oldResultPool;
        auto resultObserver = fixture.Runtime.AddObserver<TEvInternal::TEvTaskProcessedResult>([&](auto& ev) {
            for (const auto& result : ev->Get()->GetResults()) {
                if (result.GetCategory() == ESpecialTaskCategory::Insert) {
                    oldResultPool = ev->Get()->GetWorkersPoolId();
                }
            }
        });
        fixture.Runtime.EnableScheduleForActor(heldTask->Recipient, true);
        fixture.Runtime.Send(heldTask.Release(), 0, true);
        for (ui32 attempt = 0; attempt < 100 && oldCounter.Val() != 1; ++attempt) {
            fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
        }
        UNIT_ASSERT_VALUES_EQUAL(oldCounter.Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(oldResultPool, 2);
        UNIT_ASSERT_VALUES_EQUAL(responses, 0);

        fixture.Runtime.SetObserverFunc(previousObserver);
        fixture.Runtime.Send(heldRetire.Release(), 0, true);
        fixture.WaitForUpdate(id, cookie);
        UNIT_ASSERT(responses > 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Run(ESpecialTaskCategory::Insert), 0);
    }

    Y_UNIT_TEST(AddedPoolReusesEmptySlotBeforeAppending) {
        NKikimrConfig::TCompositeConveyorConfig initial;
        initial.SetEnabled(true);
        AddPool(initial, "pool-1", {{ESpecialTaskCategory::Scan, 1}});
        AddPool(initial, "pool-2", {{ESpecialTaskCategory::Insert, 1}});
        TRuntimeFixture fixture(initial);

        NKikimrConfig::TCompositeConveyorConfig withoutPool1;
        withoutPool1.SetEnabled(true);
        AddPool(withoutPool1, "pool-2", {{ESpecialTaskCategory::Insert, 1}});
        fixture.Update(withoutPool1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Run(ESpecialTaskCategory::Scan), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Run(ESpecialTaskCategory::Insert), 2);

        auto withReusedSlot = withoutPool1;
        AddPool(withReusedSlot, "pool-3", {{ESpecialTaskCategory::Scan, 1}});
        fixture.Update(withReusedSlot);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Run(ESpecialTaskCategory::Scan), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Run(ESpecialTaskCategory::Insert), 2);

        auto withAppendedPool = withReusedSlot;
        AddPool(withAppendedPool, "pool-4", {{ESpecialTaskCategory::Normalizer, 1}});
        fixture.Update(withAppendedPool);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Run(ESpecialTaskCategory::Normalizer), 3);
    }

    Y_UNIT_TEST(DerivedPoolNameChangeRecreatesPool) {
        // Changing links changes an implicit name, so this is remove plus add rather than an in-place rename.
        NKikimrConfig::TCompositeConveyorConfig initial;
        initial.SetEnabled(true);
        AddPool(initial, std::nullopt,
            {{ESpecialTaskCategory::Scan, 1}, {ESpecialTaskCategory::Normalizer, 1}});
        TRuntimeFixture fixture(initial);

        auto candidate = initial;
        candidate.MutableWorkerPools(0)->MutableLinks()->RemoveLast();
        fixture.Update(candidate);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Run(ESpecialTaskCategory::Scan), 2);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Run(ESpecialTaskCategory::Normalizer), 0);
    }

    Y_UNIT_TEST(TopologyRoutingMatrix) {
        auto config = BuildTopologyConfig(
            {{{ESpecialTaskCategory::Normalizer, 1}}, {{ESpecialTaskCategory::Insert, 1}}});
        TRuntimeFixture fixture(config);

        // start on default, then move to the first explicit pool.
        UNIT_ASSERT_VALUES_EQUAL(fixture.Run(ESpecialTaskCategory::Scan), 0);
        config = BuildTopologyConfig(
            {{{ESpecialTaskCategory::Normalizer, 1}, {ESpecialTaskCategory::Scan, 1}},
                {{ESpecialTaskCategory::Insert, 1}}});
        fixture.Update(config);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Run(ESpecialTaskCategory::Scan), 1);

        // With two free explicit pools, either eligible pool may drain first.
        config = BuildTopologyConfig(
            {{{ESpecialTaskCategory::Normalizer, 1}, {ESpecialTaskCategory::Scan, 1}},
                {{ESpecialTaskCategory::Insert, 1}, {ESpecialTaskCategory::Scan, 1}}});
        fixture.Update(config);
        const auto selectedPool = fixture.Run(ESpecialTaskCategory::Scan);
        UNIT_ASSERT(selectedPool == 1 || selectedPool == 2);

        // a busy first pool lets the second pool take the same category.
        TAtomicCounter blocker;
        std::vector<TAutoPtr<NActors::IEventHandle>> heldTasks;
        auto previousObserver = fixture.Runtime.SetObserverFunc([&](TAutoPtr<NActors::IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvInternal::TEvNewTask::EventType) {
                heldTasks.emplace_back(ev.Release());
                return NActors::TTestActorRuntime::EEventAction::DROP;
            }
            return NActors::TTestActorRuntime::EEventAction::PROCESS;
        });
        fixture.Submit(blocker, ESpecialTaskCategory::Normalizer);
        fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL(heldTasks.size(), 1);
        fixture.Runtime.SetObserverFunc(previousObserver);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Run(ESpecialTaskCategory::Scan), 2);
        RunHeldTasks(fixture, heldTasks, blocker, 1);

        // removing one of several explicit links does not add a default link.
        config = BuildTopologyConfig(
            {{{ESpecialTaskCategory::Normalizer, 1}, {ESpecialTaskCategory::Scan, 1}},
                {{ESpecialTaskCategory::Insert, 1}}});
        fixture.Update(config);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Run(ESpecialTaskCategory::Scan), 1);

        // reordering retained links keeps routes and workers intact.
        config = BuildTopologyConfig(
            {{{ESpecialTaskCategory::Scan, 1}, {ESpecialTaskCategory::Normalizer, 1}},
                {{ESpecialTaskCategory::Insert, 1}}});
        fixture.Update(config);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Run(ESpecialTaskCategory::Scan), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Run(ESpecialTaskCategory::Normalizer), 1);

        // when all categories are explicit, the always-created default pool remains empty.
        std::vector<TLinkConfig> allCategories;
        for (const auto category : GetEnumAllValues<ESpecialTaskCategory>()) {
            allCategories.emplace_back(category, 1);
        }
        config = BuildTopologyConfig({allCategories, {{ESpecialTaskCategory::Insert, 1}}});
        fixture.Update(config);
        for (const auto category : GetEnumAllValues<ESpecialTaskCategory>()) {
            UNIT_ASSERT(fixture.Run(category) != 0);
        }

        // removing the final explicit route sends an already queued task to default.
        heldTasks.clear();
        previousObserver = fixture.Runtime.SetObserverFunc([&](TAutoPtr<NActors::IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvInternal::TEvNewTask::EventType) {
                heldTasks.emplace_back(ev.Release());
                return NActors::TTestActorRuntime::EEventAction::DROP;
            }
            return NActors::TTestActorRuntime::EEventAction::PROCESS;
        });
        TAtomicCounter activeTask;
        fixture.Submit(activeTask, ESpecialTaskCategory::Normalizer);
        fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL(heldTasks.size(), 1);
        fixture.Runtime.SetObserverFunc(previousObserver);

        TAtomicCounter queuedTask;
        std::optional<ui64> queuedTaskPool;
        auto resultObserver = fixture.Runtime.AddObserver<TEvInternal::TEvTaskProcessedResult>([&](auto& ev) {
            for (const auto& result : ev->Get()->GetResults()) {
                if (result.GetCategory() == ESpecialTaskCategory::Scan) {
                    queuedTaskPool = ev->Get()->GetWorkersPoolId();
                }
            }
        });
        fixture.Submit(queuedTask, ESpecialTaskCategory::Scan);
        auto linksWithoutScan = allCategories;
        linksWithoutScan.erase(std::remove_if(linksWithoutScan.begin(), linksWithoutScan.end(), [](const auto& link) {
            return link.first == ESpecialTaskCategory::Scan;
        }));
        fixture.Update(BuildTopologyConfig({linksWithoutScan, {{ESpecialTaskCategory::Insert, 1}}}));
        for (ui32 attempt = 0; attempt < 100 && queuedTask.Val() != 1; ++attempt) {
            fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
        }
        UNIT_ASSERT_VALUES_EQUAL(queuedTask.Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(queuedTaskPool, 0);
        RunHeldTasks(fixture, heldTasks, activeTask, 1);
    }

    Y_UNIT_TEST(MaxBatchSizeUpdateControlsNextBatch) {
        // BATCH-001: increasing and decreasing the limit affects the next batch.
        auto config = BuildTopologyConfig({{{ESpecialTaskCategory::Scan, 1}}}, {1}, 2);
        TRuntimeFixture fixture(config);

        config = BuildTopologyConfig({{{ESpecialTaskCategory::Scan, 1}}}, {1}, 5);
        const auto increasedBatchSizes = RunMaxBatchUpdatePhase(fixture, config, 5);
        UNIT_ASSERT(std::find(increasedBatchSizes.begin(), increasedBatchSizes.end(), 5) != increasedBatchSizes.end());

        config = BuildTopologyConfig({{{ESpecialTaskCategory::Scan, 1}}}, {1}, 2);
        const auto decreasedBatchSizes = RunMaxBatchUpdatePhase(fixture, config, 5);
        UNIT_ASSERT_VALUES_EQUAL(std::accumulate(decreasedBatchSizes.begin(), decreasedBatchSizes.end(), ui64(0)), 6);
        for (const auto batchSize : decreasedBatchSizes) {
            UNIT_ASSERT(batchSize <= 2);
        }
    }

    Y_UNIT_TEST(MixedBatchKeepsRemovedCompletionContexts) {
        // one old batch may need completion contexts for several categories.
        auto initial = BuildTopologyConfig(
            {{{ESpecialTaskCategory::Scan, 1}, {ESpecialTaskCategory::Insert, 1}}});
        TRuntimeFixture fixture(initial);

        TAtomicCounter blocker;
        TAutoPtr<NActors::IEventHandle> heldBlocker;
        bool holdNextTask = true;
        auto previousObserver = fixture.Runtime.SetObserverFunc([&](TAutoPtr<NActors::IEventHandle>& ev) {
            if (holdNextTask && ev->GetTypeRewrite() == TEvInternal::TEvNewTask::EventType) {
                holdNextTask = false;
                heldBlocker = ev.Release();
                return NActors::TTestActorRuntime::EEventAction::DROP;
            }
            return NActors::TTestActorRuntime::EEventAction::PROCESS;
        });
        fixture.Submit(blocker, ESpecialTaskCategory::Scan, 0, TDuration::MilliSeconds(1));
        fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
        UNIT_ASSERT(heldBlocker);
        fixture.Runtime.SetObserverFunc(previousObserver);

        TAtomicCounter batchCounter;
        fixture.RegisterProcess(ESpecialTaskCategory::Insert, "INSERT_SCOPE", 1);
        fixture.Submit(batchCounter, ESpecialTaskCategory::Scan);
        fixture.Submit(batchCounter, ESpecialTaskCategory::Insert, 1);
        TAutoPtr<NActors::IEventHandle> heldBatch;
        bool releasedBlockerObserved = false;
        previousObserver = fixture.Runtime.SetObserverFunc([&](TAutoPtr<NActors::IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvInternal::TEvNewTask::EventType) {
                if (!releasedBlockerObserved) {
                    releasedBlockerObserved = true;
                    return NActors::TTestActorRuntime::EEventAction::PROCESS;
                }
                heldBatch = ev.Release();
                return NActors::TTestActorRuntime::EEventAction::DROP;
            }
            return NActors::TTestActorRuntime::EEventAction::PROCESS;
        });
        fixture.Runtime.Send(heldBlocker.Release(), 0, true);
        for (ui32 attempt = 0; attempt < 100 && !heldBatch; ++attempt) {
            fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
        }
        UNIT_ASSERT(heldBatch);

        auto withoutScan = BuildTopologyConfig({{{ESpecialTaskCategory::Insert, 1}}});
        fixture.Update(withoutScan);
        fixture.Runtime.SetObserverFunc(previousObserver);

        std::set<ESpecialTaskCategory> completedCategories;
        std::optional<ui64> completedPool;
        auto resultObserver = fixture.Runtime.AddObserver<TEvInternal::TEvTaskProcessedResult>([&](auto& ev) {
            if (ev->Get()->GetResults().size() == 2) {
                completedPool = ev->Get()->GetWorkersPoolId();
                for (const auto& result : ev->Get()->GetResults()) {
                    completedCategories.emplace(result.GetCategory());
                }
            }
        });
        fixture.Runtime.Send(heldBatch.Release(), 0, true);
        for (ui32 attempt = 0; attempt < 100 && batchCounter.Val() != 2; ++attempt) {
            fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
        }
        UNIT_ASSERT_VALUES_EQUAL(batchCounter.Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(completedPool, 1);
        UNIT_ASSERT(completedCategories.contains(ESpecialTaskCategory::Scan));
        UNIT_ASSERT(completedCategories.contains(ESpecialTaskCategory::Insert));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Run(ESpecialTaskCategory::Scan), 0);
    }

    Y_UNIT_TEST(SeveralTopologyRevisionsKeepOldBatches) {
        // ACKed route moves do not invalidate already assigned batches.
        auto initial = BuildTopologyConfig(
            {{{ESpecialTaskCategory::Scan, 1}, {ESpecialTaskCategory::Normalizer, 1}},
                {{ESpecialTaskCategory::Scan, 1}, {ESpecialTaskCategory::Insert, 1}}});
        TRuntimeFixture fixture(initial);
        TAtomicCounter counter;
        std::vector<TAutoPtr<NActors::IEventHandle>> heldTasks;
        auto previousObserver = fixture.Runtime.SetObserverFunc([&](TAutoPtr<NActors::IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvInternal::TEvNewTask::EventType) {
                heldTasks.emplace_back(ev.Release());
                return NActors::TTestActorRuntime::EEventAction::DROP;
            }
            return NActors::TTestActorRuntime::EEventAction::PROCESS;
        });
        fixture.Submit(counter, ESpecialTaskCategory::Scan);
        fixture.Submit(counter, ESpecialTaskCategory::Scan);
        fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL(heldTasks.size(), 2);

        fixture.Update(BuildTopologyConfig(
            {{{ESpecialTaskCategory::Normalizer, 1}},
                {{ESpecialTaskCategory::Scan, 1}, {ESpecialTaskCategory::Insert, 1}}}));
        fixture.Update(BuildTopologyConfig(
            {{{ESpecialTaskCategory::Scan, 1}, {ESpecialTaskCategory::Normalizer, 1}},
                {{ESpecialTaskCategory::Insert, 1}}}));
        fixture.Runtime.SetObserverFunc(previousObserver);

        std::set<ui64> oldPools;
        auto resultObserver = fixture.Runtime.AddObserver<TEvInternal::TEvTaskProcessedResult>([&](auto& ev) {
            for (const auto& result : ev->Get()->GetResults()) {
                if (result.GetCategory() == ESpecialTaskCategory::Scan) {
                    oldPools.emplace(ev->Get()->GetWorkersPoolId());
                }
            }
        });
        RunHeldTasks(fixture, heldTasks, counter, 2);
        UNIT_ASSERT_VALUES_EQUAL(oldPools.size(), 2);
        UNIT_ASSERT(oldPools.contains(1));
        UNIT_ASSERT(oldPools.contains(2));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Run(ESpecialTaskCategory::Scan), 1);
    }

    Y_UNIT_TEST(BusyWorkerLimitUpdateIsNonBlocking) {
        // an assigned task finishes after an in-place limit update without worker replacement.
        TRuntimeFixture fixture(BuildSinglePoolConfig(1));
        TAtomicCounter counter;
        std::vector<TAutoPtr<NActors::IEventHandle>> heldTasks;
        auto previousObserver = fixture.Runtime.SetObserverFunc([&](TAutoPtr<NActors::IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvInternal::TEvNewTask::EventType) {
                heldTasks.emplace_back(ev.Release());
                return NActors::TTestActorRuntime::EEventAction::DROP;
            }
            return NActors::TTestActorRuntime::EEventAction::PROCESS;
        });
        fixture.Submit(counter, ESpecialTaskCategory::Scan);
        fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL(heldTasks.size(), 1);
        fixture.Update(BuildSinglePoolConfig(0.8));
        fixture.Runtime.SetObserverFunc(previousObserver);
        RunHeldTasks(fixture, heldTasks, counter, 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Run(ESpecialTaskCategory::Scan), 1);
    }

    Y_UNIT_TEST(BusyShrinkWaitsForTaskAndStop) {
        // shrink ACK waits until an assigned batch finishes and its worker stops.
        TRuntimeFixture fixture(BuildSinglePoolConfig(2.4));
        TAtomicCounter counter;
        TAutoPtr<NActors::IEventHandle> heldTask;
        TAutoPtr<NActors::IEventHandle> heldRetire;
        bool blockEvents = true;
        auto previousObserver = fixture.Runtime.SetObserverFunc([&](TAutoPtr<NActors::IEventHandle>& ev) {
            if (blockEvents && ev->GetTypeRewrite() == TEvInternal::TEvNewTask::EventType) {
                heldTask = ev.Release();
                return NActors::TTestActorRuntime::EEventAction::DROP;
            }
            if (blockEvents && ev->GetTypeRewrite() == TEvInternal::TEvRetireWorker::EventType) {
                heldRetire = ev.Release();
                return NActors::TTestActorRuntime::EEventAction::DROP;
            }
            return NActors::TTestActorRuntime::EEventAction::PROCESS;
        });
        fixture.Submit(counter, ESpecialTaskCategory::Scan);
        fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
        UNIT_ASSERT(heldTask);

        ui32 responses = 0;
        auto responseObserver = fixture.Runtime.AddObserver<NConsole::TEvConsole::TEvConfigNotificationResponse>([&](auto&) {
            ++responses;
        });
        const auto [id, cookie] = fixture.SendUpdate(BuildSinglePoolConfig(1.4));
        fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
        UNIT_ASSERT(heldRetire);
        UNIT_ASSERT_VALUES_EQUAL(responses, 0);

        blockEvents = false;
        fixture.Runtime.EnableScheduleForActor(heldTask->Recipient, true);
        fixture.Runtime.Send(heldTask.Release(), 0, true);
        for (ui32 attempt = 0; attempt < 100 && counter.Val() != 1; ++attempt) {
            fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
        }
        UNIT_ASSERT_VALUES_EQUAL(counter.Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(responses, 0);

        fixture.Runtime.Send(heldRetire.Release(), 0, true);
        fixture.WaitForUpdate(id, cookie);
        UNIT_ASSERT(responses > 0);
        fixture.Runtime.SetObserverFunc(previousObserver);
    }

    Y_UNIT_TEST(NewConfigSupersedesInProgressUpdate) {
        TRuntimeFixture fixture(BuildSinglePoolConfig(2.4));
        TAtomicCounter counter;
        TAutoPtr<NActors::IEventHandle> heldTask;
        TAutoPtr<NActors::IEventHandle> heldRetire;
        bool blockEvents = true;
        auto previousObserver = fixture.Runtime.SetObserverFunc([&](TAutoPtr<NActors::IEventHandle>& ev) {
            if (blockEvents && ev->GetTypeRewrite() == TEvInternal::TEvNewTask::EventType) {
                heldTask = ev.Release();
                return NActors::TTestActorRuntime::EEventAction::DROP;
            }
            if (blockEvents && ev->GetTypeRewrite() == TEvInternal::TEvRetireWorker::EventType) {
                heldRetire = ev.Release();
                return NActors::TTestActorRuntime::EEventAction::DROP;
            }
            return NActors::TTestActorRuntime::EEventAction::PROCESS;
        });
        fixture.Submit(counter, ESpecialTaskCategory::Scan);
        fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
        UNIT_ASSERT(heldTask);

        std::vector<ui64> responses;
        auto responseObserver = fixture.Runtime.AddObserver<NConsole::TEvConsole::TEvConfigNotificationResponse>([&](auto& ev) {
            responses.emplace_back(ev->Get()->Record.GetSubscriptionId());
        });
        const auto [firstId, firstCookie] = fixture.SendUpdate(BuildSinglePoolConfig(1.4));
        fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
        UNIT_ASSERT(heldRetire);
        UNIT_ASSERT(responses.empty());

        auto latestConfig = BuildTopologyConfig({{{ESpecialTaskCategory::Insert, 1}}}, {1.4});
        const auto [latestId, latestCookie] = fixture.SendUpdate(latestConfig);
        fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
        UNIT_ASSERT(responses.empty());

        blockEvents = false;
        fixture.Runtime.EnableScheduleForActor(heldTask->Recipient, true);
        fixture.Runtime.Send(heldTask.Release(), 0, true);
        for (ui32 attempt = 0; attempt < 100 && counter.Val() != 1; ++attempt) {
            fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
        }
        UNIT_ASSERT_VALUES_EQUAL(counter.Val(), 1);
        UNIT_ASSERT(responses.empty());

        fixture.Runtime.Send(heldRetire.Release(), 0, true);
        fixture.WaitForUpdate(latestId, latestCookie);
        UNIT_ASSERT_VALUES_EQUAL(responses, std::vector<ui64>({latestId}));
        UNIT_ASSERT(std::find(responses.begin(), responses.end(), firstId) == responses.end());
        Y_UNUSED(firstCookie);

        fixture.Runtime.SetObserverFunc(previousObserver);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Run(ESpecialTaskCategory::Scan), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Run(ESpecialTaskCategory::Insert), 1);
    }

    Y_UNIT_TEST(ThrottledLimitAndTopologyUpdate) {
        // topology is active while the limit ACK and old throttled result are delayed.
        auto initial = BuildTopologyConfig(
            {{{ESpecialTaskCategory::Scan, 1}, {ESpecialTaskCategory::Normalizer, 1}},
                {{ESpecialTaskCategory::Insert, 1}}},
            {0.2, 1});
        TRuntimeFixture fixture(initial);
        TAtomicCounter counter;
        NActors::TActorId workerId;
        TAutoPtr<NActors::IEventHandle> heldWakeup;
        TAutoPtr<NActors::IEventHandle> heldLimitAck;
        auto previousObserver = fixture.Runtime.SetObserverFunc([&](TAutoPtr<NActors::IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvInternal::TEvNewTask::EventType) {
                workerId = ev->Recipient;
                fixture.Runtime.EnableScheduleForActor(workerId, true);
            } else if (workerId && ev->Recipient == workerId &&
                       ev->GetTypeRewrite() == NActors::TEvents::TEvWakeup::EventType) {
                heldWakeup = ev.Release();
                return NActors::TTestActorRuntime::EEventAction::DROP;
            } else if (ev->GetTypeRewrite() == TEvInternal::TEvWorkerCPULimitUpdated::EventType) {
                heldLimitAck = ev.Release();
                return NActors::TTestActorRuntime::EEventAction::DROP;
            }
            return NActors::TTestActorRuntime::EEventAction::PROCESS;
        });
        fixture.Submit(counter, ESpecialTaskCategory::Scan);
        for (ui32 attempt = 0; attempt < 100 && !heldWakeup; ++attempt) {
            fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
        }
        UNIT_ASSERT(heldWakeup);

        auto candidate = BuildTopologyConfig(
            {{{ESpecialTaskCategory::Normalizer, 1}},
                {{ESpecialTaskCategory::Insert, 1}, {ESpecialTaskCategory::Scan, 1}}},
            {0.8, 1});
        const auto [id, cookie] = fixture.SendUpdate(candidate);
        for (ui32 attempt = 0; attempt < 100 && !heldLimitAck; ++attempt) {
            fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
        }
        UNIT_ASSERT(heldLimitAck);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Run(ESpecialTaskCategory::Scan), 2);
        fixture.Runtime.SetObserverFunc(previousObserver);

        std::optional<ui64> oldResultPool;
        auto resultObserver = fixture.Runtime.AddObserver<TEvInternal::TEvTaskProcessedResult>([&](auto& ev) {
            for (const auto& result : ev->Get()->GetResults()) {
                if (result.GetCategory() == ESpecialTaskCategory::Scan) {
                    oldResultPool = ev->Get()->GetWorkersPoolId();
                }
            }
        });
        fixture.Runtime.Send(heldWakeup.Release(), 0, true);
        fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL(oldResultPool, 1);
        fixture.Runtime.Send(heldLimitAck.Release(), 0, true);
        fixture.WaitForUpdate(id, cookie);
    }

    Y_UNIT_TEST(ThrottledRetireAppliesTopologyBeforeAck) {
        auto initial = BuildTopologyConfig(
            {{{ESpecialTaskCategory::Scan, 1}, {ESpecialTaskCategory::Normalizer, 1}},
                {{ESpecialTaskCategory::Insert, 1}}},
            {1.2, 1});
        TRuntimeFixture fixture(initial);
        TAtomicCounter oldCounter;
        NActors::TActorId workerId;
        TAutoPtr<NActors::IEventHandle> heldWakeup;
        auto previousObserver = fixture.Runtime.SetObserverFunc([&](TAutoPtr<NActors::IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvInternal::TEvNewTask::EventType && !workerId) {
                workerId = ev->Recipient;
                fixture.Runtime.EnableScheduleForActor(workerId, true);
            } else if (workerId && ev->Recipient == workerId &&
                       ev->GetTypeRewrite() == NActors::TEvents::TEvWakeup::EventType) {
                heldWakeup = ev.Release();
                return NActors::TTestActorRuntime::EEventAction::DROP;
            }
            return NActors::TTestActorRuntime::EEventAction::PROCESS;
        });
        fixture.Submit(oldCounter, ESpecialTaskCategory::Scan);
        for (ui32 attempt = 0; attempt < 100 && !heldWakeup; ++attempt) {
            fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
        }
        UNIT_ASSERT(heldWakeup);

        auto candidate = BuildTopologyConfig(
            {{{ESpecialTaskCategory::Normalizer, 1}},
                {{ESpecialTaskCategory::Insert, 1}, {ESpecialTaskCategory::Scan, 1}}},
            {1, 1});
        ui32 responses = 0;
        auto responseObserver = fixture.Runtime.AddObserver<NConsole::TEvConsole::TEvConfigNotificationResponse>([&](auto&) {
            ++responses;
        });
        const auto [id, cookie] = fixture.SendUpdate(candidate);
        fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL(responses, 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Run(ESpecialTaskCategory::Scan), 2);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Run(ESpecialTaskCategory::Normalizer), 1);

        fixture.Runtime.SetObserverFunc(previousObserver);
        fixture.Runtime.Send(heldWakeup.Release(), 0, true);
        fixture.WaitForUpdate(id, cookie);
        UNIT_ASSERT(responses > 0);
    }

    Y_UNIT_TEST(MultiPoolCPUAndTopologyUpdate) {
        // grow A, shrink B and move a category in one snapshot.
        auto initial = BuildTopologyConfig(
            {{{ESpecialTaskCategory::Scan, 1}},
                {{ESpecialTaskCategory::Insert, 1}, {ESpecialTaskCategory::Normalizer, 1}}},
            {1, 2});
        TRuntimeFixture fixture(initial);
        ui32 stoppedInSecondPool = 0;
        auto stopObserver = fixture.Runtime.AddObserver<TEvInternal::TEvWorkerStopped>([&](auto& ev) {
            stoppedInSecondPool += ev->Get()->WorkersPoolId == 2;
        });
        auto candidate = BuildTopologyConfig(
            {{{ESpecialTaskCategory::Normalizer, 1}},
                {{ESpecialTaskCategory::Insert, 1}, {ESpecialTaskCategory::Scan, 1}}},
            {2, 1});
        fixture.Update(candidate);
        UNIT_ASSERT_VALUES_EQUAL(stoppedInSecondPool, 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Run(ESpecialTaskCategory::Normalizer), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Run(ESpecialTaskCategory::Scan), 2);
    }

}

}   // namespace

}   // namespace NKikimr::NConveyorComposite
