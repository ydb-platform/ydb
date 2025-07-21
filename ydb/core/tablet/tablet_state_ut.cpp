#include <ydb/core/testlib/tablet_helpers.h>

#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/interconnect/interconnect_impl.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

Y_UNIT_TEST_SUITE(TabletState) {

    class TSimpleTablet
        : public TActor<TSimpleTablet>
        , public NTabletFlatExecutor::TTabletExecutedFlat
    {
    public:
        TSimpleTablet(const TActorId& tablet, TTabletStorageInfo* info)
            : TActor(&TThis::StateInit)
            , TTabletExecutedFlat(info, tablet, nullptr)
        {}

    private:
        void DefaultSignalTabletActive(const TActorContext&) override {
            // must be empty
        }

        void OnActivateExecutor(const TActorContext& ctx) override {
            Become(&TThis::StateWork);
            SignalTabletActive(ctx);
        }

        void OnDetach(const TActorContext& ctx) override {
            return Die(ctx);
        }

        void OnTabletDead(TEvTablet::TEvTabletDead::TPtr&, const TActorContext& ctx) override {
            return Die(ctx);
        }

        STFUNC(StateInit) {
            StateInitImpl(ev, SelfId());
        }

        STFUNC(StateWork) {
            switch (ev->GetTypeRewrite()) {
            default:
                HandleDefaultEvents(ev, SelfId());
            }
        }
    };

    Y_UNIT_TEST(NormalLifecycle) {
        TTestBasicRuntime runtime(1);
        SetupTabletServices(runtime);

        auto sender = runtime.AllocateEdgeActor();
        ui64 tabletId = TTestTxConfig::TxTablet0;

        auto tablet1 = StartTestTablet(runtime,
            CreateTestTabletInfo(tabletId, TTabletTypes::Dummy),
            [](const TActorId& tablet, TTabletStorageInfo* info) {
                return new TSimpleTablet(tablet, info);
            });

        runtime.Send(new IEventHandle(tablet1, sender, new TEvTablet::TEvTabletStateSubscribe(tabletId, /* seqNo */ 1), /* flags */ 0, /* cookie */ 123), 0, true);

        // We should receive a booting state immediately after subscription
        {
            auto ev = runtime.GrabEdgeEvent<TEvTablet::TEvTabletStateUpdate>(sender);
            auto* msg = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetTabletId(), tabletId);
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetSeqNo(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetState(), NKikimrTabletBase::TEvTabletStateUpdate::StateBooting);
            UNIT_ASSERT_VALUES_EQUAL(msg->GetUserActorId(), TActorId());
            UNIT_ASSERT_VALUES_EQUAL(ev->Cookie, 123u);
        }

        // Next we should receive an active state with the user tablet
        {
            auto ev = runtime.GrabEdgeEvent<TEvTablet::TEvTabletStateUpdate>(sender);
            auto* msg = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetTabletId(), tabletId);
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetSeqNo(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetState(), NKikimrTabletBase::TEvTabletStateUpdate::StateActive);
            UNIT_ASSERT_VALUES_UNEQUAL(msg->GetUserActorId(), TActorId());
            UNIT_ASSERT_VALUES_EQUAL(ev->Cookie, 123u);
        }

        // Start a new tablet instance, which will cause the first instance to shutdown
        auto tablet2 = StartTestTablet(runtime,
            CreateTestTabletInfo(tabletId, TTabletTypes::Dummy),
            [](const TActorId& tablet, TTabletStorageInfo* info) {
                return new TSimpleTablet(tablet, info);
            });
        Y_UNUSED(tablet2);

        // We should receive a stopping notification (which implies new pipes will not connect)
        {
            auto ev = runtime.GrabEdgeEvent<TEvTablet::TEvTabletStateUpdate>(sender);
            auto* msg = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetTabletId(), tabletId);
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetSeqNo(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetState(), NKikimrTabletBase::TEvTabletStateUpdate::StateStopping);
            UNIT_ASSERT_VALUES_UNEQUAL(msg->GetUserActorId(), TActorId());
            UNIT_ASSERT_VALUES_EQUAL(ev->Cookie, 123u);
        }

        // Eventually the tablet dies and we should receive another notification
        {
            auto ev = runtime.GrabEdgeEvent<TEvTablet::TEvTabletStateUpdate>(sender);
            auto* msg = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetTabletId(), tabletId);
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetSeqNo(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetState(), NKikimrTabletBase::TEvTabletStateUpdate::StateDead);
            UNIT_ASSERT_VALUES_UNEQUAL(msg->GetUserActorId(), TActorId());
            UNIT_ASSERT_VALUES_EQUAL(ev->Cookie, 123u);
        }
    }

    Y_UNIT_TEST(SeqNoSubscriptionReplace) {
        TTestBasicRuntime runtime(1);
        SetupTabletServices(runtime);

        auto sender = runtime.AllocateEdgeActor();
        ui64 tabletId = TTestTxConfig::TxTablet0;

        auto tablet1 = StartTestTablet(runtime,
            CreateTestTabletInfo(tabletId, TTabletTypes::Dummy),
            [](const TActorId& tablet, TTabletStorageInfo* info) {
                return new TSimpleTablet(tablet, info);
            });

        runtime.Send(new IEventHandle(tablet1, sender, new TEvTablet::TEvTabletStateSubscribe(tabletId, /* seqNo */ 1), /* flags */ 0, /* cookie */ 123), 0, true);
        runtime.Send(new IEventHandle(tablet1, sender, new TEvTablet::TEvTabletStateSubscribe(tabletId, /* seqNo */ 2), /* flags */ 0, /* cookie */ 234), 0, true);

        // We should receive a booting state immediately after subscription
        {
            auto ev = runtime.GrabEdgeEvent<TEvTablet::TEvTabletStateUpdate>(sender);
            auto* msg = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetTabletId(), tabletId);
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetSeqNo(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetState(), NKikimrTabletBase::TEvTabletStateUpdate::StateBooting);
            UNIT_ASSERT_VALUES_EQUAL(msg->GetUserActorId(), TActorId());
            UNIT_ASSERT_VALUES_EQUAL(ev->Cookie, 123u);
        }

        // Same for the second SeqNo (which replaces subscription)
        {
            auto ev = runtime.GrabEdgeEvent<TEvTablet::TEvTabletStateUpdate>(sender);
            auto* msg = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetTabletId(), tabletId);
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetSeqNo(), 2u);
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetState(), NKikimrTabletBase::TEvTabletStateUpdate::StateBooting);
            UNIT_ASSERT_VALUES_EQUAL(msg->GetUserActorId(), TActorId());
            UNIT_ASSERT_VALUES_EQUAL(ev->Cookie, 234u);
        }

        // Next we should receive an active state with the user tablet for the second SeqNo
        {
            auto ev = runtime.GrabEdgeEvent<TEvTablet::TEvTabletStateUpdate>(sender);
            auto* msg = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetTabletId(), tabletId);
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetSeqNo(), 2u);
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetState(), NKikimrTabletBase::TEvTabletStateUpdate::StateActive);
            UNIT_ASSERT_VALUES_UNEQUAL(msg->GetUserActorId(), TActorId());
            UNIT_ASSERT_VALUES_EQUAL(ev->Cookie, 234u);
        }
    }

    Y_UNIT_TEST(SeqNoSubscribeOutOfOrder) {
        TTestBasicRuntime runtime(1);
        SetupTabletServices(runtime);

        auto sender = runtime.AllocateEdgeActor();
        ui64 tabletId = TTestTxConfig::TxTablet0;

        auto tablet1 = StartTestTablet(runtime,
            CreateTestTabletInfo(tabletId, TTabletTypes::Dummy),
            [](const TActorId& tablet, TTabletStorageInfo* info) {
                return new TSimpleTablet(tablet, info);
            });

        runtime.Send(new IEventHandle(tablet1, sender, new TEvTablet::TEvTabletStateSubscribe(tabletId, /* seqNo */ 2), /* flags */ 0, /* cookie */ 234), 0, true);
        runtime.Send(new IEventHandle(tablet1, sender, new TEvTablet::TEvTabletStateSubscribe(tabletId, /* seqNo */ 1), /* flags */ 0, /* cookie */ 123), 0, true);

        // We should receive reply for the second SeqNo, the first out-of-order one should be ignored
        {
            auto ev = runtime.GrabEdgeEvent<TEvTablet::TEvTabletStateUpdate>(sender);
            auto* msg = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetTabletId(), tabletId);
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetSeqNo(), 2u);
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetState(), NKikimrTabletBase::TEvTabletStateUpdate::StateBooting);
            UNIT_ASSERT_VALUES_EQUAL(msg->GetUserActorId(), TActorId());
            UNIT_ASSERT_VALUES_EQUAL(ev->Cookie, 234u);
        }

        // Next we should receive an active state with the user tablet for the second SeqNo
        {
            auto ev = runtime.GrabEdgeEvent<TEvTablet::TEvTabletStateUpdate>(sender);
            auto* msg = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetTabletId(), tabletId);
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetSeqNo(), 2u);
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetState(), NKikimrTabletBase::TEvTabletStateUpdate::StateActive);
            UNIT_ASSERT_VALUES_UNEQUAL(msg->GetUserActorId(), TActorId());
            UNIT_ASSERT_VALUES_EQUAL(ev->Cookie, 234u);
        }
    }

    Y_UNIT_TEST(ImplicitUnsubscribeOnDisconnect) {
        TTestBasicRuntime runtime(2);
        SetupTabletServices(runtime);

        auto sender = runtime.AllocateEdgeActor();
        ui64 tabletId = TTestTxConfig::TxTablet0;

        auto tablet1 = StartTestTablet(runtime,
            CreateTestTabletInfo(tabletId, TTabletTypes::Dummy),
            [](const TActorId& tablet, TTabletStorageInfo* info) {
                return new TSimpleTablet(tablet, info);
            }, /* nodeIdx */ 1);

        runtime.Send(new IEventHandle(tablet1, sender, new TEvTablet::TEvTabletStateSubscribe(tabletId, /* seqNo */ 2), /* flags */ IEventHandle::FlagSubscribeOnSession, /* cookie */ 234), 0, true);

        // Connect notification for FlagSubscribeOnSession
        {
            auto ev = runtime.GrabEdgeEvent<TEvInterconnect::TEvNodeConnected>(sender);
        }

        // We should receive reply for the second SeqNo, the first out-of-order one should be ignored
        {
            auto ev = runtime.GrabEdgeEvent<TEvTablet::TEvTabletStateUpdate>(sender);
            auto* msg = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetTabletId(), tabletId);
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetSeqNo(), 2u);
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetState(), NKikimrTabletBase::TEvTabletStateUpdate::StateBooting);
            UNIT_ASSERT_VALUES_EQUAL(msg->GetUserActorId(), TActorId());
            UNIT_ASSERT_VALUES_EQUAL(ev->Cookie, 234u);
        }

        // Next we should receive an active state with the user tablet for the second SeqNo
        {
            auto ev = runtime.GrabEdgeEvent<TEvTablet::TEvTabletStateUpdate>(sender);
            auto* msg = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetTabletId(), tabletId);
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetSeqNo(), 2u);
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetState(), NKikimrTabletBase::TEvTabletStateUpdate::StateActive);
            UNIT_ASSERT_VALUES_UNEQUAL(msg->GetUserActorId(), TActorId());
            UNIT_ASSERT_VALUES_EQUAL(ev->Cookie, 234u);
        }

        // Disconnect nodes
        runtime.Send(new IEventHandle(runtime.GetInterconnectProxy(0, 1), TActorId(), new TEvInterconnect::TEvDisconnect()), 0, true);

        // Disconnect notification for FlagSubscribeOnSession
        {
            auto ev = runtime.GrabEdgeEvent<TEvInterconnect::TEvNodeDisconnected>(sender);
        }

        // There should be no more events in the queue
        {
            auto ev = runtime.GrabEdgeEvent<IEventHandle>(sender, TDuration::MilliSeconds(1));
            UNIT_ASSERT_C(!ev, "Unexpected event received");
        }

        // Start a new tablet instance, which will cause the first instance to shutdown
        auto tablet2 = StartTestTablet(runtime,
            CreateTestTabletInfo(tabletId, TTabletTypes::Dummy),
            [](const TActorId& tablet, TTabletStorageInfo* info) {
                return new TSimpleTablet(tablet, info);
            }, /* nodeIdx */ 1);
        Y_UNUSED(tablet2);

        // There should be no events for the subscription
        {
            auto ev = runtime.GrabEdgeEvent<IEventHandle>(sender, TDuration::Seconds(1));
            UNIT_ASSERT_C(!ev, "Unexpected envet received");
        }
    }

} // Y_UNIT_TEST_SUITE(TabletState)

} // namespace NKikimr
