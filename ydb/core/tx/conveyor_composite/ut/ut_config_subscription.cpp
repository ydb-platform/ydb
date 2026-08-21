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
        UNIT_ASSERT_VALUES_EQUAL(kinds.size(), 1);
        const ui64 observation = kinds.front() | (ui64(ev->Flags & NActors::IEventHandle::FlagTrackDelivery) << 32);
        ctx.Send(Sink, new NActors::TEvents::TEvWakeup(observation));
        ctx.Send(ev->Sender, new NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse());

        auto notification = MakeHolder<NConsole::TEvConsole::TEvConfigNotificationRequest>();
        notification->Record.SetSubscriptionId(SubscriptionId);
        notification->Record.AddItemKinds(kinds.front());
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

struct TSubscriptionFixture {
    NActors::TTestActorRuntime Runtime;
    NActors::TActorId Sink;
    NActors::TActorId Distributor;

    TSubscriptionFixture() {
        Runtime.Initialize(NKikimr::TAppPrepare().Unwrap());
        Sink = Runtime.AllocateEdgeActor();
        const auto dispatcher = Runtime.Register(new TFakeConfigsDispatcher(Sink));
        Runtime.RegisterService(NConsole::MakeConfigsDispatcherID(Runtime.GetNodeId(0)), dispatcher);

        NKikimrConfig::TCompositeConveyorConfig proto;
        proto.SetEnabled(true);
        auto config = NConfig::TConfig::BuildFromProto(proto).DetachResult();
        Distributor = Runtime.Register(CreateService(config, MakeIntrusive<::NMonitoring::TDynamicCounters>()));
        Runtime.EnableScheduleForActor(Distributor, true);
    }

    void CheckSubscription() {
        const auto observed = Runtime.GrabEdgeEvent<NActors::TEvents::TEvWakeup>(Sink);
        UNIT_ASSERT_VALUES_EQUAL(observed->Get()->Tag & 0xffffffff,
            (ui32)NKikimrConsole::TConfigItem::CompositeConveyorConfigItem);
        UNIT_ASSERT(observed->Get()->Tag >> 32);
    }

    void CheckResponse(const ui64 subscriptionId, const ui64 cookie) {
        const auto response = Runtime.GrabEdgeEvent<NConsole::TEvConsole::TEvConfigNotificationResponse>(Sink);
        UNIT_ASSERT_VALUES_EQUAL(response->Get()->Record.GetSubscriptionId(), subscriptionId);
        UNIT_ASSERT_VALUES_EQUAL(response->Cookie, cookie);
        UNIT_ASSERT(response->Flags & NActors::IEventHandle::FlagTrackDelivery);
    }
};

Y_UNIT_TEST_SUITE(TCompositeConveyorConfigSubscription) {
    Y_UNIT_TEST(SubscribesAcknowledgesAndRetriesRequest) {
        TSubscriptionFixture fixture;
        fixture.CheckSubscription();
        fixture.CheckResponse(SubscriptionId, NotificationCookie);

        constexpr ui64 updateSubscriptionId = 43;
        constexpr ui64 updateCookie = 0x5678;
        auto update = MakeHolder<NConsole::TEvConsole::TEvConfigNotificationRequest>();
        update->Record.SetSubscriptionId(updateSubscriptionId);
        update->Record.AddItemKinds((ui32)NKikimrConsole::TConfigItem::CompositeConveyorConfigItem);
        update->Record.MutableConfig()->MutableCompositeConveyorConfig()->SetEnabled(true);
        fixture.Runtime.Send(
            new NActors::IEventHandle(fixture.Distributor, fixture.Sink, update.Release(), 0, updateCookie));
        fixture.CheckResponse(updateSubscriptionId, updateCookie);

        fixture.Runtime.Send(new NActors::IEventHandle(fixture.Distributor, fixture.Sink,
            new NActors::TEvents::TEvUndelivered(NConsole::TEvConfigsDispatcher::EvSetConfigSubscriptionRequest,
                NActors::TEvents::TEvUndelivered::ReasonActorUnknown)));
        fixture.Runtime.SimulateSleep(TDuration::Seconds(2));
        fixture.CheckSubscription();
        fixture.CheckResponse(SubscriptionId, NotificationCookie);
    }

    Y_UNIT_TEST(RetriesAfterNotificationResponseIsUndelivered) {
        TSubscriptionFixture fixture;
        fixture.CheckSubscription();
        fixture.CheckResponse(SubscriptionId, NotificationCookie);

        fixture.Runtime.Send(new NActors::IEventHandle(fixture.Distributor, fixture.Sink,
            new NActors::TEvents::TEvUndelivered(NConsole::TEvConsole::EvConfigNotificationResponse,
                NActors::TEvents::TEvUndelivered::ReasonActorUnknown)));
        fixture.Runtime.SimulateSleep(TDuration::Seconds(2));
        fixture.CheckSubscription();
        fixture.CheckResponse(SubscriptionId, NotificationCookie);
    }
}

}   // namespace

}   // namespace NKikimr::NConveyorComposite
