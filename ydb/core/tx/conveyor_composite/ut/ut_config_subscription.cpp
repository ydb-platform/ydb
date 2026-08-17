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
        const auto distributor = runtime.Register(CreateService(config, MakeIntrusive<::NMonitoring::TDynamicCounters>()));
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
}

}   // namespace NKikimr::NConveyorComposite
