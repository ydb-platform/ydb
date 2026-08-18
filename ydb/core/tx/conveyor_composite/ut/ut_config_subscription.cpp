#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/tx/conveyor_composite/service/service.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NConveyorComposite {

namespace {

constexpr ui64 NotificationCookie = 0x1234;
constexpr ui64 SubscriptionId = 42;

class TCounterTask: public NConveyor::ITask {
private:
    TAtomicCounter& Counter;

    void DoExecute(const std::shared_ptr<ITask>& /*taskPtr*/) override {
        Counter.Inc();
    }

public:
    explicit TCounterTask(TAtomicCounter& counter)
        : Counter(counter) {
    }

    TString GetTaskClassIdentifier() const override {
        return "COUNTER";
    }
};

NKikimrConfig::TCompositeConveyorConfig BuildConfig(const double workersCount) {
    NKikimrConfig::TCompositeConveyorConfig result;
    result.SetEnabled(true);
    auto* pool = result.AddWorkerPools();
    pool->SetName("test");
    pool->SetWorkersCount(workersCount);
    for (const auto category : GetEnumAllValues<ESpecialTaskCategory>()) {
        result.AddCategories()->SetName(::ToString(category));
        auto* link = pool->AddLinks();
        link->SetCategory(::ToString(category));
        link->SetWeight(1);
    }
    return result;
}

void SendConfigUpdate(NActors::TTestActorRuntime& runtime, const NActors::TActorId& distributor, const NActors::TActorId& sender,
    const NKikimrConfig::TCompositeConveyorConfig& config, const ui64 subscriptionId, const ui64 cookie) {
    auto update = MakeHolder<NConsole::TEvConsole::TEvConfigNotificationRequest>();
    update->Record.SetSubscriptionId(subscriptionId);
    update->Record.AddItemKinds((ui32)NKikimrConsole::TConfigItem::CompositeConveyorConfigItem);
    update->Record.MutableConfig()->MutableCompositeConveyorConfig()->CopyFrom(config);
    runtime.Send(new NActors::IEventHandle(distributor, sender, update.Release(), 0, cookie));
}

class TFakeConfigsDispatcher: public NActors::TActorBootstrapped<TFakeConfigsDispatcher> {
private:
    const NActors::TActorId Sink;

    void Handle(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest::TPtr& ev, const NActors::TActorContext& ctx) {
        const auto& kinds = ev->Get()->ConfigItemKinds;
        const bool trackDelivery = ev->Flags & NActors::IEventHandle::FlagTrackDelivery;
        UNIT_ASSERT_VALUES_EQUAL(kinds.size(), 1);

        const ui64 observation = kinds.front() | (ui64(trackDelivery) << 32);
        ctx.Send(Sink, new NActors::TEvents::TEvWakeup(observation));
        ctx.Send(ev->Sender, new NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse());

        auto notification = MakeHolder<NConsole::TEvConsole::TEvConfigNotificationRequest>();
        notification->Record.SetSubscriptionId(SubscriptionId);
        notification->Record.AddItemKinds(kinds.front());
        notification->Record.MutableConfig()->MutableCompositeConveyorConfig()->SetEnabled(false);
        ctx.Send(ev->Sender, notification.Release(), 0, NotificationCookie);
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

void CheckSubscriptionRequest(NActors::TTestActorRuntime& runtime, const NActors::TActorId& sink) {
    const auto observed = runtime.GrabEdgeEvent<NActors::TEvents::TEvWakeup>(sink);
    const ui64 kind = observed->Get()->Tag & 0xffffffff;
    const bool trackDelivery = observed->Get()->Tag >> 32;

    UNIT_ASSERT_VALUES_EQUAL(kind, (ui32)NKikimrConsole::TConfigItem::CompositeConveyorConfigItem);
    UNIT_ASSERT(trackDelivery);
}

void CheckNotificationResponse(
    NActors::TTestActorRuntime& runtime, const NActors::TActorId& sink, const ui64 subscriptionId, const ui64 cookie) {
    const auto response = runtime.GrabEdgeEvent<NConsole::TEvConsole::TEvConfigNotificationResponse>(sink);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->Record.GetSubscriptionId(), subscriptionId);
    UNIT_ASSERT_VALUES_EQUAL(response->Cookie, cookie);
    UNIT_ASSERT(response->Flags & NActors::IEventHandle::FlagTrackDelivery);
}

}   // namespace

Y_UNIT_TEST_SUITE(TCompositeConveyorConfigSubscription) {
    Y_UNIT_TEST(DistributorSubscribesAcknowledgesAndRetries) {
        NActors::TTestActorRuntime runtime;
        runtime.Initialize(NKikimr::TAppPrepare().Unwrap());

        const auto sink = runtime.AllocateEdgeActor();
        const auto dispatcher = runtime.Register(new TFakeConfigsDispatcher(sink));
        runtime.RegisterService(NConsole::MakeConfigsDispatcherID(runtime.GetNodeId(0)), dispatcher);

        NKikimrConfig::TCompositeConveyorConfig protoConfig;
        protoConfig.SetEnabled(true);
        auto config = NConfig::TConfig::BuildFromProto(protoConfig).DetachResult();
        const auto distributor = runtime.Register(CreateService(config, protoConfig, MakeIntrusive<::NMonitoring::TDynamicCounters>()));
        runtime.EnableScheduleForActor(distributor, true);

        CheckSubscriptionRequest(runtime, sink);
        CheckNotificationResponse(runtime, sink, SubscriptionId, NotificationCookie);

        constexpr ui64 updateSubscriptionId = 43;
        constexpr ui64 updateCookie = 0x5678;
        auto update = MakeHolder<NConsole::TEvConsole::TEvConfigNotificationRequest>();
        update->Record.SetSubscriptionId(updateSubscriptionId);
        update->Record.AddItemKinds((ui32)NKikimrConsole::TConfigItem::CompositeConveyorConfigItem);
        update->Record.MutableConfig()->MutableCompositeConveyorConfig()->SetEnabled(true);
        runtime.Send(new NActors::IEventHandle(distributor, sink, update.Release(), 0, updateCookie));
        CheckNotificationResponse(runtime, sink, updateSubscriptionId, updateCookie);

        runtime.Send(new NActors::IEventHandle(distributor, sink,
            new NActors::TEvents::TEvUndelivered(NConsole::TEvConfigsDispatcher::EvSetConfigSubscriptionRequest,
                NActors::TEvents::TEvUndelivered::ReasonActorUnknown)));
        runtime.SimulateSleep(TDuration::Seconds(2));

        CheckSubscriptionRequest(runtime, sink);
        CheckNotificationResponse(runtime, sink, SubscriptionId, NotificationCookie);
    }

    Y_UNIT_TEST(WorkersCountReconcileUsesGrowShrinkAndInPlaceLimitUpdate) {
        NActors::TTestActorRuntime runtime;
        runtime.Initialize(NKikimr::TAppPrepare().Unwrap());

        const auto sink = runtime.AllocateEdgeActor();
        const auto dispatcher = runtime.Register(new TFakeConfigsDispatcher(sink));
        runtime.RegisterService(NConsole::MakeConfigsDispatcherID(runtime.GetNodeId(0)), dispatcher);

        auto initialProto = BuildConfig(2.4);
        auto initialConfig = NConfig::TConfig::BuildFromProto(initialProto).DetachResult();

        TVector<ui64> updatedWorkers;
        TVector<ui64> stoppedWorkers;
        auto updatedObserver = runtime.AddObserver<TEvInternal::TEvWorkerCPULimitUpdated>([&](auto& ev) {
            updatedWorkers.emplace_back(ev->Get()->WorkerIdx);
        });
        auto stoppedObserver = runtime.AddObserver<TEvInternal::TEvWorkerStopped>([&](auto& ev) {
            stoppedWorkers.emplace_back(ev->Get()->WorkerIdx);
        });

        const auto distributor = runtime.Register(CreateService(initialConfig, initialProto, MakeIntrusive<::NMonitoring::TDynamicCounters>()));
        CheckSubscriptionRequest(runtime, sink);
        CheckNotificationResponse(runtime, sink, SubscriptionId, NotificationCookie);

        SendConfigUpdate(runtime, distributor, sink, BuildConfig(2.8), 100, 1000);
        CheckNotificationResponse(runtime, sink, 100, 1000);
        UNIT_ASSERT_VALUES_EQUAL(updatedWorkers.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(updatedWorkers.back(), 2);
        UNIT_ASSERT(stoppedWorkers.empty());

        SendConfigUpdate(runtime, distributor, sink, BuildConfig(3.8), 101, 1001);
        CheckNotificationResponse(runtime, sink, 101, 1001);
        UNIT_ASSERT_VALUES_EQUAL(updatedWorkers.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(updatedWorkers.back(), 2);
        UNIT_ASSERT(stoppedWorkers.empty());

        SendConfigUpdate(runtime, distributor, sink, BuildConfig(1.4), 102, 1002);
        CheckNotificationResponse(runtime, sink, 102, 1002);
        UNIT_ASSERT_VALUES_EQUAL(updatedWorkers.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(updatedWorkers.back(), 1);
        UNIT_ASSERT_VALUES_EQUAL(stoppedWorkers.size(), 2);
        Sort(stoppedWorkers);
        UNIT_ASSERT_VALUES_EQUAL(stoppedWorkers[0], 2);
        UNIT_ASSERT_VALUES_EQUAL(stoppedWorkers[1], 3);

        TAtomicCounter counter;
        runtime.Send(distributor, sink,
            new TEvExecution::TEvNewTask(std::make_shared<TCounterTask>(counter), ESpecialTaskCategory::Scan, 0));
        runtime.SimulateSleep(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL(counter.Val(), 1);
    }

    Y_UNIT_TEST(ShrinkWaitsForAnAlreadyAssignedBatch) {
        NActors::TTestActorRuntime runtime;
        runtime.Initialize(NKikimr::TAppPrepare().Unwrap());

        const auto sink = runtime.AllocateEdgeActor();
        const auto dispatcher = runtime.Register(new TFakeConfigsDispatcher(sink));
        runtime.RegisterService(NConsole::MakeConfigsDispatcherID(runtime.GetNodeId(0)), dispatcher);

        auto initialProto = BuildConfig(2.4);
        auto initialConfig = NConfig::TConfig::BuildFromProto(initialProto).DetachResult();
        const auto distributor = runtime.Register(CreateService(initialConfig, initialProto, MakeIntrusive<::NMonitoring::TDynamicCounters>()));
        CheckSubscriptionRequest(runtime, sink);
        CheckNotificationResponse(runtime, sink, SubscriptionId, NotificationCookie);

        bool blockTask = true;
        bool blockRetire = true;
        ui32 processedResults = 0;
        TAutoPtr<NActors::IEventHandle> capturedTask;
        TAutoPtr<NActors::IEventHandle> capturedRetire;
        auto previousObserver = runtime.SetObserverFunc([&](TAutoPtr<NActors::IEventHandle>& ev) {
            if (blockTask && ev->GetTypeRewrite() == TEvInternal::TEvNewTask::EventType) {
                capturedTask = ev.Release();
                return NActors::TTestActorRuntime::EEventAction::DROP;
            }
            if (blockRetire && ev->GetTypeRewrite() == TEvInternal::TEvRetireWorker::EventType) {
                capturedRetire = ev.Release();
                return NActors::TTestActorRuntime::EEventAction::DROP;
            }
            if (ev->GetTypeRewrite() == TEvInternal::TEvTaskProcessedResult::EventType) {
                ++processedResults;
            }
            return NActors::TTestActorRuntime::EEventAction::PROCESS;
        });

        TAtomicCounter counter;
        runtime.Send(distributor, sink,
            new TEvExecution::TEvNewTask(std::make_shared<TCounterTask>(counter), ESpecialTaskCategory::Scan, 0));
        runtime.SimulateSleep(TDuration::MilliSeconds(1));
        UNIT_ASSERT(capturedTask);
        UNIT_ASSERT_VALUES_EQUAL(counter.Val(), 0);

        SendConfigUpdate(runtime, distributor, sink, BuildConfig(1.4), 200, 2000);
        runtime.SimulateSleep(TDuration::MilliSeconds(1));
        UNIT_ASSERT(capturedRetire);

        blockTask = false;
        runtime.EnableScheduleForActor(capturedTask->Recipient, true);
        runtime.Send(capturedTask.Release(), 0, true);
        runtime.SimulateSleep(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(processedResults, 1);
        UNIT_ASSERT_VALUES_EQUAL(counter.Val(), 1);

        blockRetire = false;
        runtime.Send(capturedRetire.Release(), 0, true);
        CheckNotificationResponse(runtime, sink, 200, 2000);
        runtime.SetObserverFunc(previousObserver);
    }
}

}   // namespace NKikimr::NConveyorComposite
