#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/kqp/query_data/kqp_predictor.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/tx/conveyor_composite/service/service.h>
#include <ydb/core/tx/conveyor_composite/service/workers_pool.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/defs.h>
#include <ydb/library/actors/core/events.h>

#include <library/cpp/testing/unittest/registar.h>

#include <algorithm>
#include <limits>
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
    bool Executed = false;

    void DoExecute(const std::shared_ptr<ITask>& /*taskPtr*/) override {
        UNIT_ASSERT_C(!Executed, "a task was executed more than once");
        Executed = true;
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
    std::vector<ui64>& Responses;

    void Handle(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest::TPtr& ev, const NActors::TActorContext& ctx) {
        ctx.Send(Sink, new NActors::TEvents::TEvWakeup(ev->Get()->ConfigItemKinds.front()));
        ctx.Send(ev->Sender, new NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse());

        auto notification = MakeHolder<NConsole::TEvConsole::TEvConfigNotificationRequest>();
        notification->Record.SetSubscriptionId(InitialSubscriptionId);
        notification->Record.AddItemKinds(ev->Get()->ConfigItemKinds.front());
        ctx.Send(ev->Sender, notification.Release(), 0, InitialNotificationCookie);
    }

    void Handle(NConsole::TEvConsole::TEvConfigNotificationResponse::TPtr& ev, const NActors::TActorContext& ctx) {
        Responses.emplace_back(ev->Get()->Record.GetSubscriptionId());
        ctx.Send(ev->Forward(Sink));
    }

public:
    TFakeConfigsDispatcher(const NActors::TActorId& sink, std::vector<ui64>& responses)
        : Sink(sink)
        , Responses(responses) {
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
    std::vector<ui64> Responses;
    NActors::TTestActorRuntime Runtime;
    NActors::TActorId Sink;
    NActors::TActorId Dispatcher;
    NActors::TActorId Distributor;
    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;

    explicit TRuntimeFixture(const NKikimrConfig::TCompositeConveyorConfig& proto) {
        Runtime.Initialize(NKikimr::TAppPrepare().Unwrap());
        Sink = Runtime.AllocateEdgeActor();
        Dispatcher = Runtime.Register(new TFakeConfigsDispatcher(Sink, Responses));
        Runtime.RegisterService(NConsole::MakeConfigsDispatcherID(Runtime.GetNodeId(0)), Dispatcher);

        auto config = NConfig::TConfig::BuildFromProto(proto);
        UNIT_ASSERT_C(!config.IsFail(), config.GetErrorMessage());
        Counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
        Distributor = Runtime.Register(CreateService(config.DetachResult(), Counters));
        Runtime.EnableScheduleForActor(Distributor, true);

        const auto subscription = Runtime.GrabEdgeEvent<NActors::TEvents::TEvWakeup>(Sink);
        UNIT_ASSERT_VALUES_EQUAL(
            subscription->Get()->Tag, (ui32)NKikimrConsole::TConfigItem::CompositeConveyorConfigItem);
        WaitForUpdate(InitialSubscriptionId, InitialNotificationCookie);
        Responses.clear();
    }

    std::pair<ui64, ui64> SendUpdate(const NKikimrConfig::TCompositeConveyorConfig& config) {
        const ui64 id = NextUpdateId++;
        const ui64 cookie = 1000 + id;
        auto update = MakeHolder<NConsole::TEvConsole::TEvConfigNotificationRequest>();
        update->Record.SetSubscriptionId(id);
        auto* itemId = update->Record.MutableConfigId()->AddItemIds();
        itemId->SetId(id);
        itemId->SetGeneration(cookie);
        update->Record.AddItemKinds((ui32)NKikimrConsole::TConfigItem::CompositeConveyorConfigItem);
        update->Record.MutableConfig()->MutableCompositeConveyorConfig()->CopyFrom(config);
        Runtime.Send(new NActors::IEventHandle(Distributor, Dispatcher, update.Release(), 0, cookie));
        return {id, cookie};
    }

    void WaitForUpdate(const ui64 id, const ui64 cookie) {
        const auto response = Runtime.GrabEdgeEvent<NConsole::TEvConsole::TEvConfigNotificationResponse>(Sink);
        UNIT_ASSERT_VALUES_EQUAL(response->Get()->Record.GetSubscriptionId(), id);
        UNIT_ASSERT_VALUES_EQUAL(response->Cookie, cookie);
        UNIT_ASSERT(response->Flags & NActors::IEventHandle::FlagTrackDelivery);
        if (id != InitialSubscriptionId) {
            const auto& configId = response->Get()->Record.GetConfigId();
            UNIT_ASSERT_VALUES_EQUAL(configId.ItemIdsSize(), 1);
            UNIT_ASSERT_VALUES_EQUAL(configId.GetItemIds(0).GetId(), id);
            UNIT_ASSERT_VALUES_EQUAL(configId.GetItemIds(0).GetGeneration(), cookie);
        }
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

TAutoPtr<NActors::IEventHandle> HoldTask(TRuntimeFixture& fixture, TAtomicCounter& counter,
    const ESpecialTaskCategory category, const ui64 processId = 0) {
    TAutoPtr<NActors::IEventHandle> heldTask;
    auto previousObserver = fixture.Runtime.SetObserverFunc([&](TAutoPtr<NActors::IEventHandle>& ev) {
        if (!heldTask && ev->GetTypeRewrite() == TEvInternal::TEvNewTask::EventType) {
            heldTask = ev.Release();
            return NActors::TTestActorRuntime::EEventAction::DROP;
        }
        return NActors::TTestActorRuntime::EEventAction::PROCESS;
    });
    fixture.Submit(counter, category, processId);
    fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
    fixture.Runtime.SetObserverFunc(previousObserver);
    UNIT_ASSERT(heldTask);
    UNIT_ASSERT_VALUES_EQUAL(counter.Val(), 0);
    return heldTask;
}

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

ui64 GetQueueSizeLimitCounter(const TRuntimeFixture& fixture, const ESpecialTaskCategory category) {
    return fixture.Counters->GetSubgroup("module_id", "COMPOSITE_CONVEYOR")
        ->GetSubgroup("category", ::ToString(category))
        ->GetCounter("Value/WaitingQueueSizeLimit")
        ->Val();
}

ui64 GetWorkersCountLimitCounter(const TRuntimeFixture& fixture, const TString& poolName) {
    return fixture.Counters->GetSubgroup("module_id", "COMPOSITE_CONVEYOR")
        ->GetSubgroup("pool_name", poolName)
        ->GetCounter("Value/WorkersCountLimit")
        ->Val();
}

ui64 GetBadConfigNotificationsCounter(const TRuntimeFixture& fixture) {
    auto counter = fixture.Counters->GetSubgroup("module_id", "COMPOSITE_CONVEYOR")
        ->FindCounter("Deriviative/BadConfigNotifications");
    UNIT_ASSERT(counter);
    return counter->Val();
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

        fixture.Update(BuildSinglePoolConfig(3));
        fixture.Update(BuildSinglePoolConfig(5));

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
        UNIT_ASSERT_VALUES_EQUAL(fixture.Run(ESpecialTaskCategory::Scan), 1);
    }

    Y_UNIT_TEST(CpuEpsilonAndRepresentationUpdates) {
        // below Eps is ignored, above the boundary updates the actor.
        TRuntimeFixture fixture(BuildSinglePoolConfig(2.4));
        std::vector<double> taskLimits;
        auto observer = fixture.Runtime.AddObserver<TEvInternal::TEvNewTask>([&](auto& ev) {
            taskLimits.emplace_back(ev->Get()->GetCPULimit());
        });

        fixture.Update(BuildSinglePoolConfig(2.4 + TWorkersPool::Eps / 2));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Run(ESpecialTaskCategory::Scan), 1);
        UNIT_ASSERT(std::abs(taskLimits.back() - 0.4) < TWorkersPool::Eps);
        fixture.Update(BuildSinglePoolConfig(2.4 + TWorkersPool::Eps * 2));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Run(ESpecialTaskCategory::Scan), 1);
        UNIT_ASSERT(std::abs(taskLimits.back() - (0.4 + TWorkersPool::Eps * 2)) < TWorkersPool::Eps);

        // switching representation with the same resolved limits is a runtime no-op.
        auto fractionConfig = BuildSinglePoolConfig(2.4 + TWorkersPool::Eps * 2);
        auto* pool = fractionConfig.MutableWorkerPools(0);
        pool->ClearWorkersCount();
        pool->SetDefaultFractionOfThreadsCount(
            (2.4 + TWorkersPool::Eps * 2) / NKqp::TStagePredictor::GetPossibleMaxLimitThreads());
        fixture.Update(fractionConfig);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Run(ESpecialTaskCategory::Scan), 1);
        UNIT_ASSERT(std::abs(taskLimits.back() - (0.4 + TWorkersPool::Eps * 2)) < TWorkersPool::Eps);
    }

    Y_UNIT_TEST(CpuFractionalReconcileMatrix) {
        // retained fractional limits combine with grow and shrink.
        TRuntimeFixture fixture(BuildSinglePoolConfig(2.4));
        std::vector<double> taskLimits;
        ui32 stoppedWorkers = 0;
        auto taskObserver = fixture.Runtime.AddObserver<TEvInternal::TEvNewTask>([&](auto& ev) {
            taskLimits.emplace_back(ev->Get()->GetCPULimit());
        });
        auto stopObserver = fixture.Runtime.AddObserver<NActors::TEvents::TEvPoisonPill>([&](auto&) {
            ++stoppedWorkers;
        });

        fixture.Update(BuildSinglePoolConfig(2.8));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Run(ESpecialTaskCategory::Scan), 1);
        UNIT_ASSERT(std::abs(taskLimits.back() - 0.8) < TWorkersPool::Eps);
        fixture.Update(BuildSinglePoolConfig(3.8));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Run(ESpecialTaskCategory::Scan), 1);
        UNIT_ASSERT(std::abs(taskLimits.back() - 0.8) < TWorkersPool::Eps);
        fixture.Update(BuildSinglePoolConfig(1.4));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Run(ESpecialTaskCategory::Scan), 1);
        UNIT_ASSERT(std::abs(taskLimits.back() - 0.4) < TWorkersPool::Eps);
        UNIT_ASSERT_VALUES_EQUAL(stoppedWorkers, 2);
    }

    Y_UNIT_TEST(DefaultPoolCPUIsIndependent) {
        // updating an explicit pool must not update or stop synthetic default workers.
        auto initial = BuildTopologyConfig({{{ESpecialTaskCategory::Scan, 1}}}, {1});
        TRuntimeFixture fixture(initial);

        fixture.Update(BuildTopologyConfig({{{ESpecialTaskCategory::Scan, 1}}}, {2}));
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

    Y_UNIT_TEST(QueueSizeLimitUpdatesMonitoringOnly) {
        auto initial = BuildSinglePoolConfig(1);
        auto* category = initial.AddCategories();
        category->SetName(::ToString(ESpecialTaskCategory::Scan));
        category->SetQueueSizeLimit(10);
        TRuntimeFixture fixture(initial);
        UNIT_ASSERT_VALUES_EQUAL(GetQueueSizeLimitCounter(fixture, ESpecialTaskCategory::Scan), 10);

        auto candidate = initial;
        candidate.MutableCategories(0)->SetQueueSizeLimit(1);
        fixture.Update(candidate);
        UNIT_ASSERT_VALUES_EQUAL(GetQueueSizeLimitCounter(fixture, ESpecialTaskCategory::Scan), 1);

        for (ui32 i = 0; i < 3; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(fixture.Run(ESpecialTaskCategory::Scan), 1);
        }

        candidate.ClearCategories();
        fixture.Update(candidate);
        UNIT_ASSERT_VALUES_EQUAL(GetQueueSizeLimitCounter(fixture, ESpecialTaskCategory::Scan), 256 * 1024);
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

    Y_UNIT_TEST(ValidConfigUpdateAppliesAtomically) {
        auto initial = BuildTopologyConfig(
            {{{ESpecialTaskCategory::Scan, 1}, {ESpecialTaskCategory::Normalizer, 1}},
                {{ESpecialTaskCategory::Insert, 1}}},
            {1, 1});
        TRuntimeFixture fixture(initial);

        auto candidate = initial;
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
        bool holdTask = true;
        auto previousObserver = fixture.Runtime.SetObserverFunc([&](TAutoPtr<NActors::IEventHandle>& ev) {
            if (holdTask && ev->GetTypeRewrite() == TEvInternal::TEvNewTask::EventType) {
                holdTask = false;
                heldTask = ev.Release();
                return NActors::TTestActorRuntime::EEventAction::DROP;
            }
            return NActors::TTestActorRuntime::EEventAction::PROCESS;
        });
        fixture.Submit(oldCounter, ESpecialTaskCategory::Insert);
        fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
        UNIT_ASSERT(heldTask);

        auto candidate = BuildTopologyConfig({{{ESpecialTaskCategory::Scan, 1}}});
        const auto& responses = fixture.Responses;
        ui32 poisonEvents = 0;
        auto poisonObserver = fixture.Runtime.AddObserver<NActors::TEvents::TEvPoisonPill>([&](auto&) {
            ++poisonEvents;
        });
        const auto [id, cookie] = fixture.SendUpdate(candidate);
        fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL(responses.size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(poisonEvents, 0);

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
        fixture.WaitForUpdate(id, cookie);
        UNIT_ASSERT(!responses.empty());
        UNIT_ASSERT_VALUES_EQUAL(poisonEvents, 1);
        fixture.Runtime.SetObserverFunc(previousObserver);
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
        UNIT_ASSERT_VALUES_EQUAL(fixture.Run(ESpecialTaskCategory::Scan), 1);
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
        auto taskObserver = fixture.Runtime.AddObserver<TEvInternal::TEvNewTask>([&](auto& ev) {
            fixture.Runtime.EnableScheduleForActor(ev->Recipient, true);
        });
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
        for (ui32 attempt = 0; attempt < 100 && (queuedTask.Val() != 1 || !queuedTaskPool); ++attempt) {
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

    Y_UNIT_TEST(MixedBatchDelaysApplyUntilResultsProcessed) {
        // Keep the original links until all results of the mixed batch have been processed.
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
        const auto& responses = fixture.Responses;
        const auto [id, cookie] = fixture.SendUpdate(withoutScan);
        fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL(responses.size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(GetWeightCounter(fixture, "pool-1", ESpecialTaskCategory::Scan), 1);
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
        fixture.WaitForUpdate(id, cookie);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Run(ESpecialTaskCategory::Scan), 0);
    }

    Y_UNIT_TEST(LatestTopologySupersedesPrepare) {
        // Replacing the target restores the first link and waits only for the second one.
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

        const auto& responses = fixture.Responses;
        fixture.SendUpdate(BuildTopologyConfig(
            {{{ESpecialTaskCategory::Normalizer, 1}},
                {{ESpecialTaskCategory::Scan, 1}, {ESpecialTaskCategory::Insert, 1}}}));
        fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
        UNIT_ASSERT(responses.empty());
        const auto [id, cookie] = fixture.SendUpdate(BuildTopologyConfig(
            {{{ESpecialTaskCategory::Scan, 1}, {ESpecialTaskCategory::Normalizer, 1}},
                {{ESpecialTaskCategory::Insert, 1}}}));
        fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
        UNIT_ASSERT(responses.empty());
        fixture.Runtime.SetObserverFunc(previousObserver);

        std::set<ui64> oldPools;
        auto resultObserver = fixture.Runtime.AddObserver<TEvInternal::TEvTaskProcessedResult>([&](auto& ev) {
            for (const auto& result : ev->Get()->GetResults()) {
                if (result.GetCategory() == ESpecialTaskCategory::Scan) {
                    oldPools.emplace(ev->Get()->GetWorkersPoolId());
                }
            }
        });
        // The second pool's result is sufficient; the first batch can remain assigned across apply.
        fixture.Runtime.EnableScheduleForActor(heldTasks[1]->Recipient, true);
        fixture.Runtime.Send(heldTasks[1].Release(), 0, true);
        fixture.WaitForUpdate(id, cookie);
        UNIT_ASSERT_VALUES_EQUAL(responses.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(responses.front(), id);
        UNIT_ASSERT_VALUES_EQUAL(counter.Val(), 1);
        heldTasks.resize(1);
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

    Y_UNIT_TEST(BusyShrinkWaitsForTaskResult) {
        // shrink ACK waits until an assigned batch finishes; actor stop itself needs no acknowledgement.
        TRuntimeFixture fixture(BuildSinglePoolConfig(2.4));
        TAtomicCounter counter;
        TAutoPtr<NActors::IEventHandle> heldTask;
        bool blockEvents = true;
        auto previousObserver = fixture.Runtime.SetObserverFunc([&](TAutoPtr<NActors::IEventHandle>& ev) {
            if (blockEvents && ev->GetTypeRewrite() == TEvInternal::TEvNewTask::EventType) {
                heldTask = ev.Release();
                return NActors::TTestActorRuntime::EEventAction::DROP;
            }
            return NActors::TTestActorRuntime::EEventAction::PROCESS;
        });
        fixture.Submit(counter, ESpecialTaskCategory::Scan);
        fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
        UNIT_ASSERT(heldTask);

        const auto& responses = fixture.Responses;
        const auto [id, cookie] = fixture.SendUpdate(BuildSinglePoolConfig(1.4));
        fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL(responses.size(), 0);

        blockEvents = false;
        fixture.Runtime.EnableScheduleForActor(heldTask->Recipient, true);
        fixture.Runtime.Send(heldTask.Release(), 0, true);
        for (ui32 attempt = 0; attempt < 100 && counter.Val() != 1; ++attempt) {
            fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
        }
        UNIT_ASSERT_VALUES_EQUAL(counter.Val(), 1);
        fixture.WaitForUpdate(id, cookie);
        UNIT_ASSERT(!responses.empty());
        fixture.Runtime.SetObserverFunc(previousObserver);
    }

    Y_UNIT_TEST(LatestConfigSupersedesInProgressShrinkAndGrows) {
        TRuntimeFixture fixture(BuildSinglePoolConfig(2.4));
        TAtomicCounter counter;
        TAutoPtr<NActors::IEventHandle> heldTask;
        bool blockEvents = true;
        auto previousObserver = fixture.Runtime.SetObserverFunc([&](TAutoPtr<NActors::IEventHandle>& ev) {
            if (blockEvents && ev->GetTypeRewrite() == TEvInternal::TEvNewTask::EventType) {
                heldTask = ev.Release();
                return NActors::TTestActorRuntime::EEventAction::DROP;
            }
            return NActors::TTestActorRuntime::EEventAction::PROCESS;
        });
        fixture.Submit(counter, ESpecialTaskCategory::Scan);
        fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
        UNIT_ASSERT(heldTask);

        const auto& responses = fixture.Responses;
        const auto [firstId, firstCookie] = fixture.SendUpdate(BuildSinglePoolConfig(1.4));
        fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
        UNIT_ASSERT(responses.empty());

        auto intermediateConfig = BuildTopologyConfig({{{ESpecialTaskCategory::Insert, 1}}}, {1.4});
        const auto [intermediateId, intermediateCookie] = fixture.SendUpdate(intermediateConfig);
        fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
        UNIT_ASSERT(responses.empty());

        auto latestConfig = BuildSinglePoolConfig(3.4);
        ui32 poisonEvents = 0;
        auto poisonObserver = fixture.Runtime.AddObserver<NActors::TEvents::TEvPoisonPill>([&](auto&) {
            ++poisonEvents;
        });
        const auto [latestId, latestCookie] = fixture.SendUpdate(latestConfig);
        fixture.WaitForUpdate(latestId, latestCookie);
        UNIT_ASSERT_VALUES_EQUAL(counter.Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(poisonEvents, 0);
        UNIT_ASSERT_VALUES_EQUAL(GetWorkersCountLimitCounter(fixture, "pool-1"), 4);

        blockEvents = false;
        fixture.Runtime.EnableScheduleForActor(heldTask->Recipient, true);
        fixture.Runtime.Send(heldTask.Release(), 0, true);
        for (ui32 attempt = 0; attempt < 100 && counter.Val() != 1; ++attempt) {
            fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
        }
        UNIT_ASSERT_VALUES_EQUAL(counter.Val(), 1);
        UNIT_ASSERT(std::find(responses.begin(), responses.end(), latestId) != responses.end());
        UNIT_ASSERT(std::find(responses.begin(), responses.end(), firstId) == responses.end());
        UNIT_ASSERT(std::find(responses.begin(), responses.end(), intermediateId) == responses.end());
        Y_UNUSED(firstCookie);
        Y_UNUSED(intermediateCookie);

        fixture.Runtime.SetObserverFunc(previousObserver);
        UNIT_ASSERT_VALUES_EQUAL(GetWorkersCountLimitCounter(fixture, "pool-1"), 4);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Run(ESpecialTaskCategory::Scan), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Run(ESpecialTaskCategory::Insert), 1);
    }

    Y_UNIT_TEST(ThrottledLimitAndTopologyUpdate) {
        // A topology change waits for the throttled result before publishing the new CPU limit.
        auto initial = BuildTopologyConfig(
            {{{ESpecialTaskCategory::Scan, 1}, {ESpecialTaskCategory::Normalizer, 1}},
                {{ESpecialTaskCategory::Insert, 1}}},
            {0.2, 1});
        TRuntimeFixture fixture(initial);
        TAtomicCounter counter;
        NActors::TActorId workerId;
        TAutoPtr<NActors::IEventHandle> heldWakeup;
        auto previousObserver = fixture.Runtime.SetObserverFunc([&](TAutoPtr<NActors::IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvInternal::TEvNewTask::EventType) {
                workerId = ev->Recipient;
                fixture.Runtime.EnableScheduleForActor(workerId, true);
            } else if (workerId && ev->Recipient == workerId &&
                       ev->GetTypeRewrite() == NActors::TEvents::TEvWakeup::EventType) {
                heldWakeup = ev.Release();
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
        fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL(GetWeightCounter(fixture, "pool-1", ESpecialTaskCategory::Scan), 1);
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
        fixture.WaitForUpdate(id, cookie);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Run(ESpecialTaskCategory::Scan), 2);

        std::optional<double> nextTaskLimit;
        auto taskObserver = fixture.Runtime.AddObserver<TEvInternal::TEvNewTask>([&](auto& ev) {
            nextTaskLimit = ev->Get()->GetCPULimit();
        });
        UNIT_ASSERT_VALUES_EQUAL(fixture.Run(ESpecialTaskCategory::Normalizer), 1);
        UNIT_ASSERT(nextTaskLimit);
        UNIT_ASSERT(std::abs(*nextTaskLimit - 0.8) < TWorkersPool::Eps);
    }

    Y_UNIT_TEST(ThrottledRetireKeepsTopologyUntilApply) {
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
        const auto& responses = fixture.Responses;
        const auto [id, cookie] = fixture.SendUpdate(candidate);
        fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL(responses.size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(GetWeightCounter(fixture, "pool-1", ESpecialTaskCategory::Scan), 1);
        UNIT_ASSERT_VALUES_EQUAL(GetWorkersCountLimitCounter(fixture, "pool-1"), 2);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Run(ESpecialTaskCategory::Normalizer), 1);

        fixture.Runtime.SetObserverFunc(previousObserver);
        fixture.Runtime.Send(heldWakeup.Release(), 0, true);
        fixture.WaitForUpdate(id, cookie);
        UNIT_ASSERT(!responses.empty());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Run(ESpecialTaskCategory::Scan), 2);
    }

    Y_UNIT_TEST(MultiPoolCPUAndTopologyUpdate) {
        // grow A, shrink B and move a category in one snapshot.
        auto initial = BuildTopologyConfig(
            {{{ESpecialTaskCategory::Scan, 1}},
                {{ESpecialTaskCategory::Insert, 1}, {ESpecialTaskCategory::Normalizer, 1}}},
            {1, 2});
        TRuntimeFixture fixture(initial);
        ui32 stoppedInSecondPool = 0;
        auto stopObserver = fixture.Runtime.AddObserver<NActors::TEvents::TEvPoisonPill>([&](auto&) {
            ++stoppedInSecondPool;
        });
        auto candidate = BuildTopologyConfig(
            {{{ESpecialTaskCategory::Normalizer, 1}},
                {{ESpecialTaskCategory::Insert, 1}, {ESpecialTaskCategory::Scan, 1}}},
            {2, 1});
        fixture.Update(candidate);
        fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL(stoppedInSecondPool, 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Run(ESpecialTaskCategory::Normalizer), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Run(ESpecialTaskCategory::Scan), 2);
    }

    Y_UNIT_TEST(PolicyChangesWaitForPrepareWhileOtherLinksRun) {
        auto initial = BuildTopologyConfig(
            {{{ESpecialTaskCategory::Scan, 7}, {ESpecialTaskCategory::Normalizer, 11}},
                {{ESpecialTaskCategory::Insert, 1}}}, {2, 1}, 2);
        TRuntimeFixture fixture(initial);
        TAtomicCounter oldTask;
        auto held = HoldTask(fixture, oldTask, ESpecialTaskCategory::Scan);

        auto target = BuildTopologyConfig(
            {{{ESpecialTaskCategory::Normalizer, 3}},
                {{ESpecialTaskCategory::Insert, 1}, {ESpecialTaskCategory::Scan, 1}}}, {0.8, 1}, 5);
        auto* category = target.AddCategories();
        category->SetName(::ToString(ESpecialTaskCategory::Normalizer));
        category->SetQueueSizeLimit(10);

        const auto& responses = fixture.Responses;
        ui32 poisonEvents = 0;
        auto poisonObserver = fixture.Runtime.AddObserver<NActors::TEvents::TEvPoisonPill>([&](auto&) {
            ++poisonEvents;
        });
        const auto [id, cookie] = fixture.SendUpdate(target);
        fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL(responses.size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(poisonEvents, 0);
        UNIT_ASSERT_VALUES_EQUAL(GetWorkersCountLimitCounter(fixture, "pool-1"), 2);
        UNIT_ASSERT_VALUES_EQUAL(GetWeightCounter(fixture, "pool-1", ESpecialTaskCategory::Scan), 7);
        UNIT_ASSERT_VALUES_EQUAL(GetWeightCounter(fixture, "pool-1", ESpecialTaskCategory::Normalizer), 11);
        UNIT_ASSERT_VALUES_EQUAL(GetQueueSizeLimitCounter(fixture, ESpecialTaskCategory::Normalizer), 256 * 1024);

        TAtomicCounter queuedTask;
        fixture.RegisterProcess(ESpecialTaskCategory::Scan, "queued", 101);
        fixture.Submit(queuedTask, ESpecialTaskCategory::Scan, 101);
        fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL(queuedTask.Val(), 0);

        // Every unrelated completion retries prepare without releasing blocked workers or links.
        std::optional<double> taskLimit;
        auto taskObserver = fixture.Runtime.AddObserver<TEvInternal::TEvNewTask>([&](auto& ev) {
            taskLimit = ev->Get()->GetCPULimit();
        });
        for (ui32 i = 0; i < 3; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(fixture.Run(ESpecialTaskCategory::Normalizer), 1);
            UNIT_ASSERT(taskLimit);
            UNIT_ASSERT_VALUES_EQUAL(*taskLimit, 1);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Run(ESpecialTaskCategory::Insert), 2);
        }
        UNIT_ASSERT_VALUES_EQUAL(responses.size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(poisonEvents, 0);
        UNIT_ASSERT_VALUES_EQUAL(queuedTask.Val(), 0);

        std::optional<ui64> queuedPool;
        auto resultObserver = fixture.Runtime.AddObserver<TEvInternal::TEvTaskProcessedResult>([&](auto& ev) {
            for (const auto& result : ev->Get()->GetResults()) {
                if (result.GetProcessId() == 101) {
                    queuedPool = ev->Get()->GetWorkersPoolId();
                }
            }
        });
        fixture.Runtime.Send(held.Release(), 0, true);
        fixture.WaitForUpdate(id, cookie);
        fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(10));
        UNIT_ASSERT_VALUES_EQUAL(oldTask.Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(queuedTask.Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(queuedPool, 2);
        UNIT_ASSERT_VALUES_EQUAL(responses.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(poisonEvents, 1);
        UNIT_ASSERT_VALUES_EQUAL(GetWorkersCountLimitCounter(fixture, "pool-1"), 1);
        UNIT_ASSERT_VALUES_EQUAL(GetWeightCounter(fixture, "pool-1", ESpecialTaskCategory::Scan), 0);
        UNIT_ASSERT_VALUES_EQUAL(GetWeightCounter(fixture, "pool-1", ESpecialTaskCategory::Normalizer), 3);
        UNIT_ASSERT_VALUES_EQUAL(GetQueueSizeLimitCounter(fixture, ESpecialTaskCategory::Normalizer), 10);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Run(ESpecialTaskCategory::Normalizer), 1);
        UNIT_ASSERT(taskLimit && std::abs(*taskLimit - 0.8) < TWorkersPool::Eps);
    }

    Y_UNIT_TEST(SupersededPrepareRestoresIdleWorker) {
        TRuntimeFixture fixture(BuildSinglePoolConfig(3));
        TAtomicCounter oldTask;
        auto held = HoldTask(fixture, oldTask, ESpecialTaskCategory::Scan);
        std::optional<ui64> workerIdx;
        auto resultObserver = fixture.Runtime.AddObserver<TEvInternal::TEvTaskProcessedResult>([&](auto& ev) {
            if (ev->Get()->GetWorkersPoolId() == 1) {
                workerIdx = ev->Get()->GetWorkerIdx();
            }
        });
        ui32 poisonEvents = 0;
        auto poisonObserver = fixture.Runtime.AddObserver<NActors::TEvents::TEvPoisonPill>([&](auto&) {
            ++poisonEvents;
        });
        fixture.SendUpdate(BuildSinglePoolConfig(1));
        fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Run(ESpecialTaskCategory::Scan), 1);
        UNIT_ASSERT_VALUES_EQUAL(workerIdx, 0);

        const auto [id, cookie] = fixture.SendUpdate(BuildSinglePoolConfig(2));
        fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
        for (ui32 i = 0; i < 3; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(fixture.Run(ESpecialTaskCategory::Scan), 1);
            UNIT_ASSERT_VALUES_EQUAL(workerIdx, 1);
        }
        UNIT_ASSERT_VALUES_EQUAL(poisonEvents, 0);
        UNIT_ASSERT_VALUES_EQUAL(GetWorkersCountLimitCounter(fixture, "pool-1"), 3);
        fixture.Runtime.Send(held.Release(), 0, true);
        fixture.WaitForUpdate(id, cookie);
        fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL(oldTask.Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(poisonEvents, 1);
        UNIT_ASSERT_VALUES_EQUAL(GetWorkersCountLimitCounter(fixture, "pool-1"), 2);
    }

    Y_UNIT_TEST(SupersededPrepareRestoresBusyWorkerAfterResult) {
        TRuntimeFixture fixture(BuildSinglePoolConfig(4));
        TAtomicCounter removedTask;
        TAtomicCounter restoredTask;
        auto removed = HoldTask(fixture, removedTask, ESpecialTaskCategory::Scan);
        auto restored = HoldTask(fixture, restoredTask, ESpecialTaskCategory::Scan);
        fixture.SendUpdate(BuildSinglePoolConfig(2));
        fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
        const auto [id, cookie] = fixture.SendUpdate(BuildSinglePoolConfig(3));
        fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
        fixture.Runtime.Send(restored.Release(), 0, true);
        fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL(restoredTask.Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(removedTask.Val(), 0);

        std::optional<ui64> workerIdx;
        auto resultObserver = fixture.Runtime.AddObserver<TEvInternal::TEvTaskProcessedResult>([&](auto& ev) {
            workerIdx = ev->Get()->GetWorkerIdx();
        });
        UNIT_ASSERT_VALUES_EQUAL(fixture.Run(ESpecialTaskCategory::Scan), 1);
        UNIT_ASSERT_VALUES_EQUAL(workerIdx, 2);
        UNIT_ASSERT_VALUES_EQUAL(GetWorkersCountLimitCounter(fixture, "pool-1"), 4);
        fixture.Runtime.Send(removed.Release(), 0, true);
        fixture.WaitForUpdate(id, cookie);
        UNIT_ASSERT_VALUES_EQUAL(removedTask.Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(GetWorkersCountLimitCounter(fixture, "pool-1"), 3);
    }

    Y_UNIT_TEST(ConfigEqualToAppliedCancelsPrepare) {
        const auto initial = BuildSinglePoolConfig(2);
        TRuntimeFixture fixture(initial);
        TAtomicCounter oldTask;
        auto held = HoldTask(fixture, oldTask, ESpecialTaskCategory::Scan);
        const auto workerId = held->Recipient;
        const auto& responses = fixture.Responses;
        ui32 poisonEvents = 0;
        auto poisonObserver = fixture.Runtime.AddObserver<NActors::TEvents::TEvPoisonPill>([&](auto&) {
            ++poisonEvents;
        });

        auto removeScan = BuildTopologyConfig({{{ESpecialTaskCategory::Insert, 1}}}, {1});
        fixture.SendUpdate(removeScan);
        fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
        UNIT_ASSERT(responses.empty());
        TAtomicCounter queuedTask;
        fixture.Submit(queuedTask, ESpecialTaskCategory::Scan);
        fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL(queuedTask.Val(), 0);

        const auto [id, cookie] = fixture.SendUpdate(initial);
        fixture.WaitForUpdate(id, cookie);
        fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL(queuedTask.Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(oldTask.Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(poisonEvents, 0);
        fixture.Runtime.Send(held.Release(), 0, true);
        fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL(oldTask.Val(), 1);

        NActors::TActorId executingWorker;
        auto taskObserver = fixture.Runtime.AddObserver<TEvInternal::TEvNewTask>([&](auto& ev) {
            executingWorker = ev->Recipient;
        });
        UNIT_ASSERT_VALUES_EQUAL(fixture.Run(ESpecialTaskCategory::Scan), 1);
        UNIT_ASSERT_VALUES_EQUAL(executingWorker, workerId);
        UNIT_ASSERT_VALUES_EQUAL(responses.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(responses.front(), id);
        UNIT_ASSERT_VALUES_EQUAL(poisonEvents, 0);
    }

    Y_UNIT_TEST(InvalidUpdatePreservesAppliedConfig) {
        auto initial = BuildTopologyConfig(
            {{{ESpecialTaskCategory::Scan, 3}, {ESpecialTaskCategory::Insert, 5}}}, {2});
        auto* initialCategory = initial.AddCategories();
        initialCategory->SetName(::ToString(ESpecialTaskCategory::Insert));
        initialCategory->SetQueueSizeLimit(23);
        TRuntimeFixture fixture(initial);
        UNIT_ASSERT_VALUES_EQUAL(GetBadConfigNotificationsCounter(fixture), 0);

        auto target = BuildTopologyConfig(
            {{{ESpecialTaskCategory::Insert, 7}, {ESpecialTaskCategory::Normalizer, 11}}}, {1});
        auto* targetCategory = target.AddCategories();
        targetCategory->SetName(::ToString(ESpecialTaskCategory::Insert));
        targetCategory->SetQueueSizeLimit(17);

        auto invalidWeight = target;
        invalidWeight.MutableWorkerPools(0)->MutableLinks(0)->SetWeight(0);
        auto invalidWorkersCount = target;
        invalidWorkersCount.MutableWorkerPools(0)->SetWorkersCount(NActors::MaxWorkers + 1);
        auto invalidCategory = target;
        invalidCategory.MutableWorkerPools(0)->MutableLinks(0)->SetCategory("unknown-category");

        ui64 rejectedCount = 0;
        ui32 poisonEvents = 0;
        auto poisonObserver = fixture.Runtime.AddObserver<NActors::TEvents::TEvPoisonPill>([&](auto&) {
            ++poisonEvents;
        });
        for (const auto& invalid : {invalidWeight, invalidWorkersCount, invalidCategory, invalidWeight}) {
            fixture.Update(invalid);
            ++rejectedCount;
            UNIT_ASSERT_VALUES_EQUAL(GetBadConfigNotificationsCounter(fixture), rejectedCount);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Responses.size(), rejectedCount);
            UNIT_ASSERT_VALUES_EQUAL(GetWorkersCountLimitCounter(fixture, "pool-1"), 2);
            UNIT_ASSERT_VALUES_EQUAL(GetWeightCounter(fixture, "pool-1", ESpecialTaskCategory::Scan), 3);
            UNIT_ASSERT_VALUES_EQUAL(GetWeightCounter(fixture, "pool-1", ESpecialTaskCategory::Insert), 5);
            UNIT_ASSERT_VALUES_EQUAL(GetQueueSizeLimitCounter(fixture, ESpecialTaskCategory::Insert), 23);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Run(ESpecialTaskCategory::Scan), 1);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Run(ESpecialTaskCategory::Insert), 1);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Run(ESpecialTaskCategory::Normalizer), 0);
            UNIT_ASSERT_VALUES_EQUAL(poisonEvents, 0);
        }

        fixture.Update(target);
        UNIT_ASSERT_VALUES_EQUAL(GetBadConfigNotificationsCounter(fixture), rejectedCount);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Responses.size(), rejectedCount + 1);
        UNIT_ASSERT_VALUES_EQUAL(GetWorkersCountLimitCounter(fixture, "pool-1"), 1);
        UNIT_ASSERT_VALUES_EQUAL(GetWeightCounter(fixture, "pool-1", ESpecialTaskCategory::Insert), 7);
        UNIT_ASSERT_VALUES_EQUAL(GetQueueSizeLimitCounter(fixture, ESpecialTaskCategory::Insert), 17);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Run(ESpecialTaskCategory::Scan), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Run(ESpecialTaskCategory::Normalizer), 1);
    }

    Y_UNIT_TEST(InvalidUpdatePreservesPreparedTarget) {
        const auto initial = BuildTopologyConfig(
            {{{ESpecialTaskCategory::Scan, 1}, {ESpecialTaskCategory::Insert, 1}}});
        TRuntimeFixture fixture(initial);
        UNIT_ASSERT_VALUES_EQUAL(GetBadConfigNotificationsCounter(fixture), 0);
        TAtomicCounter oldTask;
        auto held = HoldTask(fixture, oldTask, ESpecialTaskCategory::Scan);
        const auto target = BuildTopologyConfig({{{ESpecialTaskCategory::Insert, 1}}});
        const auto [id, cookie] = fixture.SendUpdate(target);
        fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
        auto invalid = initial;
        invalid.MutableWorkerPools(0)->MutableLinks(0)->SetWeight(0);
        fixture.Update(invalid);
        UNIT_ASSERT_VALUES_EQUAL(GetBadConfigNotificationsCounter(fixture), 1);

        auto excessive = initial;
        excessive.MutableWorkerPools(0)->SetWorkersCount(NActors::MaxWorkers + 1);
        fixture.Update(excessive);
        UNIT_ASSERT_VALUES_EQUAL(GetBadConfigNotificationsCounter(fixture), 2);

        auto unsupported = initial;
        unsupported.SetEnabled(false);
        fixture.Update(unsupported);
        UNIT_ASSERT_VALUES_EQUAL(GetBadConfigNotificationsCounter(fixture), 3);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Responses.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(GetWorkersCountLimitCounter(fixture, "pool-1"), 1);

        TAtomicCounter queuedTask;
        fixture.Submit(queuedTask, ESpecialTaskCategory::Scan);
        fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL(queuedTask.Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(GetWeightCounter(fixture, "pool-1", ESpecialTaskCategory::Scan), 1);
        fixture.Runtime.Send(held.Release(), 0, true);
        fixture.WaitForUpdate(id, cookie);
        fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(10));
        UNIT_ASSERT_VALUES_EQUAL(oldTask.Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(queuedTask.Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Run(ESpecialTaskCategory::Scan), 0);
        UNIT_ASSERT_VALUES_EQUAL(GetBadConfigNotificationsCounter(fixture), 3);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Responses.size(), 4);
    }

    Y_UNIT_TEST(EnabledChangeRejectsEntireUpdate) {
        for (const bool enabled : {true, false}) {
            auto initial = BuildTopologyConfig(
                {{{ESpecialTaskCategory::Scan, 1}, {ESpecialTaskCategory::Insert, 1}}}, {2});
            initial.SetEnabled(enabled);
            TRuntimeFixture fixture(initial);
            UNIT_ASSERT_VALUES_EQUAL(TServiceOperator::IsEnabled(), enabled);
            UNIT_ASSERT_VALUES_EQUAL(GetBadConfigNotificationsCounter(fixture), 0);
            const auto initialQueueLimit = GetQueueSizeLimitCounter(fixture, ESpecialTaskCategory::Insert);
            TAtomicCounter oldTask;
            auto held = HoldTask(fixture, oldTask, ESpecialTaskCategory::Scan);

            auto target = BuildTopologyConfig(
                {{{ESpecialTaskCategory::Insert, 3}, {ESpecialTaskCategory::Normalizer, 7}}}, {1});
            target.SetEnabled(!enabled);
            auto* category = target.AddCategories();
            category->SetName(::ToString(ESpecialTaskCategory::Insert));
            category->SetQueueSizeLimit(17);
            const auto [id, cookie] = fixture.SendUpdate(target);
            fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
            UNIT_ASSERT_VALUES_EQUAL(fixture.Responses.size(), 1);
            fixture.WaitForUpdate(id, cookie);
            UNIT_ASSERT_VALUES_EQUAL(GetBadConfigNotificationsCounter(fixture), 1);
            UNIT_ASSERT_VALUES_EQUAL(oldTask.Val(), 0);
            UNIT_ASSERT_VALUES_EQUAL(TServiceOperator::IsEnabled(), enabled);
            UNIT_ASSERT_VALUES_EQUAL(GetWorkersCountLimitCounter(fixture, "pool-1"), 2);
            UNIT_ASSERT_VALUES_EQUAL(GetWeightCounter(fixture, "pool-1", ESpecialTaskCategory::Insert), 1);
            UNIT_ASSERT_VALUES_EQUAL(GetQueueSizeLimitCounter(fixture, ESpecialTaskCategory::Insert), initialQueueLimit);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Run(ESpecialTaskCategory::Scan), 1);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Run(ESpecialTaskCategory::Normalizer), 0);

            fixture.Runtime.Send(held.Release(), 0, true);
            fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
            UNIT_ASSERT_VALUES_EQUAL(oldTask.Val(), 1);
            UNIT_ASSERT_VALUES_EQUAL(GetWorkersCountLimitCounter(fixture, "pool-1"), 2);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Run(ESpecialTaskCategory::Scan), 1);

            target.SetEnabled(enabled);
            fixture.Update(target);
            UNIT_ASSERT_VALUES_EQUAL(GetBadConfigNotificationsCounter(fixture), 1);
            UNIT_ASSERT_VALUES_EQUAL(TServiceOperator::IsEnabled(), enabled);
            UNIT_ASSERT_VALUES_EQUAL(GetWorkersCountLimitCounter(fixture, "pool-1"), 1);
            UNIT_ASSERT_VALUES_EQUAL(GetWeightCounter(fixture, "pool-1", ESpecialTaskCategory::Insert), 3);
            UNIT_ASSERT_VALUES_EQUAL(GetQueueSizeLimitCounter(fixture, ESpecialTaskCategory::Insert), 17);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Run(ESpecialTaskCategory::Scan), 0);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Run(ESpecialTaskCategory::Normalizer), 1);

            target.MutableWorkerPools(0)->MutableLinks(0)->SetWeight(4);
            fixture.Update(target);
            UNIT_ASSERT_VALUES_EQUAL(GetBadConfigNotificationsCounter(fixture), 1);
            UNIT_ASSERT_VALUES_EQUAL(TServiceOperator::IsEnabled(), enabled);
            UNIT_ASSERT_VALUES_EQUAL(GetWeightCounter(fixture, "pool-1", ESpecialTaskCategory::Insert), 4);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Run(ESpecialTaskCategory::Insert), 1);
        }
    }

    Y_UNIT_TEST(SupersededPreparePreservesPoolIdentity) {
        auto initial = BuildTopologyConfig({{{ESpecialTaskCategory::Scan, 1}}});
        TRuntimeFixture fixture(initial);
        TAtomicCounter oldTask;
        auto held = HoldTask(fixture, oldTask, ESpecialTaskCategory::Scan);
        const auto oldWorkerId = held->Recipient;
        ui32 poisonEvents = 0;
        auto poisonObserver = fixture.Runtime.AddObserver<NActors::TEvents::TEvPoisonPill>([&](auto&) {
            ++poisonEvents;
        });
        auto removed = initial;
        removed.ClearWorkerPools();
        fixture.SendUpdate(removed);
        fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));

        auto restored = initial;
        restored.MutableWorkerPools(0)->MutableLinks(0)->SetWeight(2);
        fixture.Update(restored);
        UNIT_ASSERT_VALUES_EQUAL(oldTask.Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(GetWeightCounter(fixture, "pool-1", ESpecialTaskCategory::Scan), 2);
        fixture.Runtime.Send(held.Release(), 0, true);
        fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL(oldTask.Val(), 1);

        NActors::TActorId newWorkerId;
        auto taskObserver = fixture.Runtime.AddObserver<TEvInternal::TEvNewTask>([&](auto& ev) {
            newWorkerId = ev->Recipient;
        });
        UNIT_ASSERT_VALUES_EQUAL(fixture.Run(ESpecialTaskCategory::Scan), 1);
        UNIT_ASSERT_VALUES_EQUAL(newWorkerId, oldWorkerId);
        UNIT_ASSERT_VALUES_EQUAL(poisonEvents, 0);
    }

    Y_UNIT_TEST(AddedPoolWaitsForDefaultLink) {
        NKikimrConfig::TCompositeConveyorConfig initial;
        TRuntimeFixture fixture(initial);
        TAtomicCounter oldTask;
        auto held = HoldTask(fixture, oldTask, ESpecialTaskCategory::Scan);
        fixture.Runtime.EnableScheduleForActor(held->Recipient, true);
        TAtomicCounter queuedTask;
        fixture.RegisterProcess(ESpecialTaskCategory::Scan, "queued-default", 101);

        const auto& responses = fixture.Responses;
        const auto [id, cookie] = fixture.SendUpdate(BuildTopologyConfig({{{ESpecialTaskCategory::Scan, 1}}}));
        fixture.Submit(queuedTask, ESpecialTaskCategory::Scan, 101);
        fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL(responses.size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(queuedTask.Val(), 0);

        std::optional<ui64> oldPool;
        std::optional<ui64> queuedPool;
        auto resultObserver = fixture.Runtime.AddObserver<TEvInternal::TEvTaskProcessedResult>([&](auto& ev) {
            for (const auto& result : ev->Get()->GetResults()) {
                if (result.GetCategory() == ESpecialTaskCategory::Scan) {
                    (result.GetProcessId() == 101 ? queuedPool : oldPool) = ev->Get()->GetWorkersPoolId();
                }
            }
        });
        fixture.Runtime.Send(held.Release(), 0, true);
        fixture.WaitForUpdate(id, cookie);
        fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(10));
        UNIT_ASSERT_VALUES_EQUAL(oldTask.Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(queuedTask.Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(oldPool, 0);
        UNIT_ASSERT_VALUES_EQUAL(queuedPool, 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Run(ESpecialTaskCategory::Scan), 1);
    }

}

}   // namespace

}   // namespace NKikimr::NConveyorComposite
