#include <ydb/core/base/statestorage.h>
#include <ydb/core/base/tablet_resolver.h>
#include <ydb/core/tablet/tablet_setup.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/testlib/tablet_helpers.h>

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

    Y_UNIT_TEST(ResolverDiscoversNewFollowerBeforeStateStorage) {
        TTestBasicRuntime runtime(1, false);
        SetupTabletServices(runtime);

        const auto sender = runtime.AllocateEdgeActor();
        const ui64 tabletId = TTestTxConfig::TxTablet0;
        const ui32 followerId = 1;
        auto createTablet = [](const TActorId& tablet, TTabletStorageInfo* info) {
            return new TSimpleTablet(tablet, info);
        };
        const auto leader = StartTestTablet(runtime, CreateTestTabletInfo(tabletId, TTabletTypes::Dummy), createTablet);

        runtime.Send(new IEventHandle(leader, sender, new TEvTablet::TEvTabletStateSubscribe(tabletId, 1)), 0, true);
        while (runtime.GrabEdgeEventRethrow<TEvTablet::TEvTabletStateUpdate>(sender)->Get()->Record.GetState()
                != NKikimrTabletBase::TEvTabletStateUpdate::StateActive) {
        }

        auto resolve = [&](TEvTabletResolver::TEvForward::TResolveFlags flags) {
            runtime.Send(new IEventHandle(MakeTabletResolverID(), sender,
                new TEvTabletResolver::TEvForward(tabletId, nullptr, flags)), 0, true);
            return runtime.GrabEdgeEventRethrow<TEvTabletResolver::TEvForwardResult>(sender);
        };

        TEvTabletResolver::TEvForward::TResolveFlags flags;
        flags.SetAllowFollower(true);
        const auto initial = resolve(flags);
        UNIT_ASSERT_VALUES_EQUAL(initial->Get()->Status, NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(initial->Get()->Tablet, leader);

        const auto resolver = runtime.GetLocalServiceId(MakeTabletResolverID());
        const auto stateStorageProxy = runtime.GetLocalServiceId(MakeStateStorageProxyID());
        size_t resolverLookups = 0;
        auto countLookups = runtime.AddObserver<TEvStateStorage::TEvLookup>([&](auto& ev) {
            if (ev->Sender == resolver && ev->GetRecipientRewrite() == stateStorageProxy &&
                    ev->Get()->TabletID == tabletId) {
                ++resolverLookups;
            }
        });

        auto setup = MakeIntrusive<TTabletSetupInfo>(createTablet, TMailboxType::Simple, 0u, TMailboxType::Simple, 0u);
        auto startFollower = [&](ui32 id) {
            const auto actor = runtime.Register(CreateTabletFollower(sender,
                CreateTestTabletInfo(tabletId, TTabletTypes::Dummy), setup.Get(), id));
            TDispatchOptions options;
            options.FinalEvents.emplace_back([&](IEventHandle& ev) {
                if (ev.GetTypeRewrite() != TEvTablet::TEvTabletStateUpdate::EventType ||
                        ev.Sender != leader || ev.GetRecipientRewrite() != resolver) {
                    return false;
                }
                const auto& record = ev.Get<TEvTablet::TEvTabletStateUpdate>()->Record;
                return record.ReadyFollowersSize() == 1 && record.GetReadyFollowers(0).GetFollowerId() == id;
            });
            UNIT_ASSERT(runtime.DispatchEvents(options, TDuration::Seconds(30)));
            return actor;
        };

        const auto follower = startFollower(followerId);
        const auto restored = runtime.GrabEdgeEventRethrow<TEvTablet::TEvRestored>(sender);
        const auto preferred = resolve(flags);
        UNIT_ASSERT_VALUES_EQUAL(preferred->Get()->Status, NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(preferred->Get()->Tablet, follower);
        UNIT_ASSERT_VALUES_EQUAL(preferred->Get()->TabletActor, restored->Get()->UserTabletActor);
        UNIT_ASSERT_VALUES_EQUAL(resolverLookups, 0u);

        // A lookup for a different follower can race with registration of the new follower.
        auto staleLookup = runtime.AddObserver<TEvStateStorage::TEvInfo>([&](auto& ev) {
            if (ev->GetRecipientRewrite() == resolver && ev->Get()->TabletID == tabletId) {
                ev->Get()->Followers.clear();
            }
        });
        flags.SetFollowerId(followerId + 1);
        const auto missingFollower = resolve(flags);
        UNIT_ASSERT_UNEQUAL(missingFollower->Get()->Status, NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(resolverLookups, 1u);

        flags.SetFollowerId(followerId);
        const auto explicitFollower = resolve(flags);
        UNIT_ASSERT_VALUES_EQUAL(explicitFollower->Get()->Status, NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(explicitFollower->Get()->Tablet, follower);
        UNIT_ASSERT_VALUES_EQUAL(resolverLookups, 1u);

        runtime.Send(new IEventHandle(follower, sender, new TEvents::TEvPoisonPill()), 0, true);
        runtime.GrabEdgeEventRethrow<TEvTablet::TEvTabletDead>(sender);
        runtime.AdvanceCurrentTime(TDuration::Minutes(6));

        // Leader-only traffic keeps the cache hot without refreshing followers.
        TEvTabletResolver::TEvForward::TResolveFlags leaderOnly;
        const auto stillLeader = resolve(leaderOnly);
        UNIT_ASSERT_VALUES_EQUAL(stillLeader->Get()->Tablet, leader);
        UNIT_ASSERT_VALUES_EQUAL(resolverLookups, 1u);

        const ui32 nextFollowerId = followerId + 1;
        const auto nextFollower = startFollower(nextFollowerId);
        runtime.GrabEdgeEventRethrow<TEvTablet::TEvRestored>(sender);

        // A new announcement must discard expired, unconfirmed actor incarnations.
        // Ask for the old ID first, before a follower lookup could clean it up.
        const auto expiredFollower = resolve(flags);
        UNIT_ASSERT_UNEQUAL(expiredFollower->Get()->Status, NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(resolverLookups, 2u);

        flags.SetFollowerId(nextFollowerId);
        const auto currentFollower = resolve(flags);
        UNIT_ASSERT_VALUES_EQUAL(currentFollower->Get()->Status, NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(currentFollower->Get()->Tablet, nextFollower);
        UNIT_ASSERT_VALUES_EQUAL(resolverLookups, 2u);
    }

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

        // We should receive a terminating notification (which implies new pipes will not connect)
        {
            auto ev = runtime.GrabEdgeEvent<TEvTablet::TEvTabletStateUpdate>(sender);
            auto* msg = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetTabletId(), tabletId);
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetSeqNo(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetState(), NKikimrTabletBase::TEvTabletStateUpdate::StateTerminating);
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

        // We should receive a booting state with SeqNo=1 immediately after subscription
        {
            auto ev = runtime.GrabEdgeEvent<TEvTablet::TEvTabletStateUpdate>(sender);
            auto* msg = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetTabletId(), tabletId);
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetSeqNo(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetState(), NKikimrTabletBase::TEvTabletStateUpdate::StateBooting);
            UNIT_ASSERT_VALUES_EQUAL(msg->GetUserActorId(), TActorId());
            UNIT_ASSERT_VALUES_EQUAL(ev->Cookie, 123u);
        }

        // Same for SeqNo=2 (which replaces subscription)
        {
            auto ev = runtime.GrabEdgeEvent<TEvTablet::TEvTabletStateUpdate>(sender);
            auto* msg = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetTabletId(), tabletId);
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetSeqNo(), 2u);
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetState(), NKikimrTabletBase::TEvTabletStateUpdate::StateBooting);
            UNIT_ASSERT_VALUES_EQUAL(msg->GetUserActorId(), TActorId());
            UNIT_ASSERT_VALUES_EQUAL(ev->Cookie, 234u);
        }

        // Next we should receive an active state with the user tablet for SeqNo=2
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

        // We should receive a reply for SeqNo=2, an out-of-order SeqNo=1 should be ignored
        {
            auto ev = runtime.GrabEdgeEvent<TEvTablet::TEvTabletStateUpdate>(sender);
            auto* msg = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetTabletId(), tabletId);
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetSeqNo(), 2u);
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetState(), NKikimrTabletBase::TEvTabletStateUpdate::StateBooting);
            UNIT_ASSERT_VALUES_EQUAL(msg->GetUserActorId(), TActorId());
            UNIT_ASSERT_VALUES_EQUAL(ev->Cookie, 234u);
        }

        // Next we should receive an active state with the user tablet for SeqNo=2
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

    Y_UNIT_TEST(ExplicitUnsubscribe) {
        TTestBasicRuntime runtime(1);
        SetupTabletServices(runtime);

        auto sender1 = runtime.AllocateEdgeActor();
        auto sender2 = runtime.AllocateEdgeActor();
        ui64 tabletId = TTestTxConfig::TxTablet0;

        auto tablet1 = StartTestTablet(runtime,
            CreateTestTabletInfo(tabletId, TTabletTypes::Dummy),
            [](const TActorId& tablet, TTabletStorageInfo* info) {
                return new TSimpleTablet(tablet, info);
            });

        runtime.Send(new IEventHandle(tablet1, sender1, new TEvTablet::TEvTabletStateSubscribe(tabletId, /* seqNo */ 1), /* flags */ 0, /* cookie */ 123), 0, true);
        runtime.Send(new IEventHandle(tablet1, sender2, new TEvTablet::TEvTabletStateSubscribe(tabletId, /* seqNo */ 2), /* flags */ 0, /* cookie */ 234), 0, true);

        // We should receive a reply for SeqNo=1
        {
            auto ev = runtime.GrabEdgeEvent<TEvTablet::TEvTabletStateUpdate>(sender1);
            auto* msg = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetTabletId(), tabletId);
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetSeqNo(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetState(), NKikimrTabletBase::TEvTabletStateUpdate::StateBooting);
            UNIT_ASSERT_VALUES_EQUAL(msg->GetUserActorId(), TActorId());
            UNIT_ASSERT_VALUES_EQUAL(ev->Cookie, 123u);
        }

        // We should receive a reply for SeqNo=2
        {
            auto ev = runtime.GrabEdgeEvent<TEvTablet::TEvTabletStateUpdate>(sender2);
            auto* msg = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetTabletId(), tabletId);
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetSeqNo(), 2u);
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetState(), NKikimrTabletBase::TEvTabletStateUpdate::StateBooting);
            UNIT_ASSERT_VALUES_EQUAL(msg->GetUserActorId(), TActorId());
            UNIT_ASSERT_VALUES_EQUAL(ev->Cookie, 234u);
        }

        // Same for the active state for SeqNo=1
        {
            auto ev = runtime.GrabEdgeEvent<TEvTablet::TEvTabletStateUpdate>(sender1);
            auto* msg = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetTabletId(), tabletId);
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetSeqNo(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetState(), NKikimrTabletBase::TEvTabletStateUpdate::StateActive);
            UNIT_ASSERT_VALUES_UNEQUAL(msg->GetUserActorId(), TActorId());
            UNIT_ASSERT_VALUES_EQUAL(ev->Cookie, 123u);
        }

        // Same for the active state for SeqNo=2
        {
            auto ev = runtime.GrabEdgeEvent<TEvTablet::TEvTabletStateUpdate>(sender2);
            auto* msg = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetTabletId(), tabletId);
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetSeqNo(), 2u);
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetState(), NKikimrTabletBase::TEvTabletStateUpdate::StateActive);
            UNIT_ASSERT_VALUES_UNEQUAL(msg->GetUserActorId(), TActorId());
            UNIT_ASSERT_VALUES_EQUAL(ev->Cookie, 234u);
        }

        // Unsubscribe for the first actor
        runtime.Send(new IEventHandle(tablet1, sender1, new TEvTablet::TEvTabletStateUnsubscribe(tabletId, /* seqNo */ 1), /* flags */ 0, /* cookie */ 123), 0, true);

        // Start a new tablet instance, which will cause the first instance to shutdown
        auto tablet2 = StartTestTablet(runtime,
            CreateTestTabletInfo(tabletId, TTabletTypes::Dummy),
            [](const TActorId& tablet, TTabletStorageInfo* info) {
                return new TSimpleTablet(tablet, info);
            });
        Y_UNUSED(tablet2);

        // We should receive a terminating notification for SeqNo=2
        {
            auto ev = runtime.GrabEdgeEvent<TEvTablet::TEvTabletStateUpdate>(sender2);
            auto* msg = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetTabletId(), tabletId);
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetSeqNo(), 2u);
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetState(), NKikimrTabletBase::TEvTabletStateUpdate::StateTerminating);
            UNIT_ASSERT_VALUES_UNEQUAL(msg->GetUserActorId(), TActorId());
            UNIT_ASSERT_VALUES_EQUAL(ev->Cookie, 234u);
        }

        // There should be no events for SeqNo=1
        {
            auto ev = runtime.GrabEdgeEvent<IEventHandle>(sender1, TDuration::Seconds(1));
            UNIT_ASSERT_C(!ev, "Unexpected event received by sender1");
        }
    }

    Y_UNIT_TEST(ImplicitUnsubscribeOnDisconnect) {
        TTestBasicRuntime runtime(2);
        SetupTabletServices(runtime);

        auto sender1 = runtime.AllocateEdgeActor(0);
        auto sender2 = runtime.AllocateEdgeActor(1);
        ui64 tabletId = TTestTxConfig::TxTablet0;

        auto tablet1 = StartTestTablet(runtime,
            CreateTestTabletInfo(tabletId, TTabletTypes::Dummy),
            [](const TActorId& tablet, TTabletStorageInfo* info) {
                return new TSimpleTablet(tablet, info);
            }, /* nodeIdx */ 1);

        runtime.Send(new IEventHandle(tablet1, sender1, new TEvTablet::TEvTabletStateSubscribe(tabletId, /* seqNo */ 1), /* flags */ IEventHandle::FlagSubscribeOnSession, /* cookie */ 123), 0, true);
        runtime.Send(new IEventHandle(tablet1, sender2, new TEvTablet::TEvTabletStateSubscribe(tabletId, /* seqNo */ 2), /* flags */ IEventHandle::FlagSubscribeOnSession, /* cookie */ 234), 1, true);

        // Connect notification for FlagSubscribeOnSession
        {
            auto ev = runtime.GrabEdgeEvent<TEvInterconnect::TEvNodeConnected>(sender1);
        }

        // We should receive a reply for SeqNo=1
        {
            auto ev = runtime.GrabEdgeEvent<TEvTablet::TEvTabletStateUpdate>(sender1);
            auto* msg = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetTabletId(), tabletId);
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetSeqNo(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetState(), NKikimrTabletBase::TEvTabletStateUpdate::StateBooting);
            UNIT_ASSERT_VALUES_EQUAL(msg->GetUserActorId(), TActorId());
            UNIT_ASSERT_VALUES_EQUAL(ev->Cookie, 123u);
        }

        // We should receive a reply for SeqNo=2
        {
            auto ev = runtime.GrabEdgeEvent<TEvTablet::TEvTabletStateUpdate>(sender2);
            auto* msg = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetTabletId(), tabletId);
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetSeqNo(), 2u);
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetState(), NKikimrTabletBase::TEvTabletStateUpdate::StateBooting);
            UNIT_ASSERT_VALUES_EQUAL(msg->GetUserActorId(), TActorId());
            UNIT_ASSERT_VALUES_EQUAL(ev->Cookie, 234u);
        }

        // Same for the active state for SeqNo=1
        {
            auto ev = runtime.GrabEdgeEvent<TEvTablet::TEvTabletStateUpdate>(sender1);
            auto* msg = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetTabletId(), tabletId);
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetSeqNo(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetState(), NKikimrTabletBase::TEvTabletStateUpdate::StateActive);
            UNIT_ASSERT_VALUES_UNEQUAL(msg->GetUserActorId(), TActorId());
            UNIT_ASSERT_VALUES_EQUAL(ev->Cookie, 123u);
        }

        // Same for the active state for SeqNo=2
        {
            auto ev = runtime.GrabEdgeEvent<TEvTablet::TEvTabletStateUpdate>(sender2);
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
            auto ev = runtime.GrabEdgeEvent<TEvInterconnect::TEvNodeDisconnected>(sender1);
        }

        // There should be no more events in the queue
        {
            auto ev = runtime.GrabEdgeEvent<IEventHandle>(sender1, TDuration::MilliSeconds(1));
            UNIT_ASSERT_C(!ev, "Unexpected event received");
        }

        // There should be no more events in the queue
        {
            auto ev = runtime.GrabEdgeEvent<IEventHandle>(sender2, TDuration::MilliSeconds(1));
            UNIT_ASSERT_C(!ev, "Unexpected event received");
        }

        // Start a new tablet instance, which will cause the first instance to shutdown
        auto tablet2 = StartTestTablet(runtime,
            CreateTestTabletInfo(tabletId, TTabletTypes::Dummy),
            [](const TActorId& tablet, TTabletStorageInfo* info) {
                return new TSimpleTablet(tablet, info);
            }, /* nodeIdx */ 1);
        Y_UNUSED(tablet2);

        // We should receive a terminating notification for SeqNo=2
        {
            auto ev = runtime.GrabEdgeEvent<TEvTablet::TEvTabletStateUpdate>(sender2);
            auto* msg = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetTabletId(), tabletId);
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetSeqNo(), 2u);
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetState(), NKikimrTabletBase::TEvTabletStateUpdate::StateTerminating);
            UNIT_ASSERT_VALUES_UNEQUAL(msg->GetUserActorId(), TActorId());
            UNIT_ASSERT_VALUES_EQUAL(ev->Cookie, 234u);
        }

        // There should be no events for SeqNo=1
        {
            auto ev = runtime.GrabEdgeEvent<IEventHandle>(sender1, TDuration::Seconds(1));
            UNIT_ASSERT_C(!ev, "Unexpected event received by sender1");
        }
    }

} // Y_UNIT_TEST_SUITE(TabletState)

} // namespace NKikimr
