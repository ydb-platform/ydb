#include <ydb/core/base/statestorage.h>
#include <ydb/core/base/tablet_resolver.h>
#include <ydb/core/base/tabletid.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

Y_UNIT_TEST_SUITE(TTabletResolver) {

    Y_UNIT_TEST(TabletResolvePriority) {
        TEvTabletResolver::TEvForward::TResolveFlags flags;
        flags.SetAllowFollower(true); // this actually sets PreferFollower
        flags.SetPreferLocal(true);
        auto localNodeLeader = flags.GetTabletPriority(true, true, false);
        auto localNodeFollower = flags.GetTabletPriority(true, true, true);
        auto localDcLeader = flags.GetTabletPriority(false, true, false);
        auto localDcFollower = flags.GetTabletPriority(false, true, true);
        auto remoteDcLeader = flags.GetTabletPriority(false, false, false);
        auto remoteDcFollower = flags.GetTabletPriority(false, false, true);

        // Since PreferLocal is set we must prefer local node follower
        UNIT_ASSERT_GT(localNodeFollower, localDcFollower);
        UNIT_ASSERT_GT(localNodeFollower, remoteDcFollower);
        UNIT_ASSERT_GT(localDcFollower, remoteDcFollower);

        // Since PreferFollower is set we must prefer local dc follower over
        // any leader, even when that leader is on local node
        UNIT_ASSERT_GT(localNodeFollower, remoteDcLeader);
        UNIT_ASSERT_GT(localDcFollower, remoteDcLeader);
        UNIT_ASSERT_GT(localNodeFollower, localDcLeader);
        UNIT_ASSERT_GT(localDcFollower, localDcLeader);
        UNIT_ASSERT_GT(localNodeFollower, localNodeLeader);
        UNIT_ASSERT_GT(localDcFollower, localNodeLeader);

        // We still expect local dc leader to be preferred to remote dc follower
        UNIT_ASSERT_GT(localNodeLeader, remoteDcFollower);
        UNIT_ASSERT_GT(localDcLeader, remoteDcFollower);
    }

    class TRegisterTabletInfo : public TActorBootstrapped<TRegisterTabletInfo> {
    public:
        TRegisterTabletInfo(const TActorId& edge, ui64 tabletId, ui32 gen, TActorId leader, TActorId leaderTablet)
            : Edge(edge)
            , TabletId(tabletId)
            , Generation(gen)
            , Leader(leader)
            , LeaderTablet(leaderTablet)
        { }

        void Bootstrap() {
            ProxyId = MakeStateStorageProxyID();
            Send(ProxyId, new TEvStateStorage::TEvLookup(TabletId, 0, TEvStateStorage::TProxyOptions(TEvStateStorage::TProxyOptions::SigSync)));
            Become(&TThis::StateLookup);
        }

        STFUNC(StateLookup) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvStateStorage::TEvInfo, HandleLookup);
            }
        }

        void HandleLookup(TEvStateStorage::TEvInfo::TPtr& ev) {
            auto* msg = ev->Get();
            Y_ABORT_UNLESS(msg->SignatureSz);
            SignatureSz = msg->SignatureSz;
            Signature.Reset(msg->Signature.Release());

            Send(ProxyId, new TEvStateStorage::TEvUpdate(TabletId, 0, Leader, LeaderTablet, Generation, 0, Signature.Get(), SignatureSz, TEvStateStorage::TProxyOptions::SigSync));
            Become(&TThis::StateUpdate);
        }

        STFUNC(StateUpdate) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvStateStorage::TEvInfo, HandleUpdate);
            }
        }

        void HandleUpdate(TEvStateStorage::TEvInfo::TPtr& ev) {
            Send(Edge, ev->Release().Release());
            PassAway();
        }

    private:
        const TActorId Edge;
        const ui64 TabletId;
        const ui32 Generation;
        const TActorId Leader;
        const TActorId LeaderTablet;
        TActorId ProxyId;

        ui32 SignatureSz = 0;
        TArrayHolder<ui64> Signature;
    };

    void DoRegisterTabletInfo(TTestBasicRuntime& runtime, ui64 tabletId, ui32 gen, TActorId leader, TActorId leaderTablet) {
        const TActorId edge = runtime.AllocateEdgeActor();
        runtime.Register(new TRegisterTabletInfo(edge, tabletId, gen, leader, leaderTablet));
        auto ev = runtime.GrabEdgeEventRethrow<TEvStateStorage::TEvInfo>(edge);
        auto* msg = ev->Get();
        UNIT_ASSERT_VALUES_EQUAL(msg->Status, NKikimrProto::OK);
    }

    THolder<TEvTabletResolver::TEvForwardResult> DoResolveTablet(TTestBasicRuntime& runtime, ui64 tabletId) {
        const TActorId edge = runtime.AllocateEdgeActor();
        runtime.Send(new IEventHandle(MakeTabletResolverID(), edge, new TEvTabletResolver::TEvForward(tabletId, nullptr)));
        auto ev = runtime.GrabEdgeEventRethrow<TEvTabletResolver::TEvForwardResult>(edge);
        return std::move(ev->Release());
    }

    void DoSendNodeProblem(TTestBasicRuntime& runtime, ui32 nodeId, ui64 problemEpoch) {
        runtime.Send(new IEventHandle(MakeTabletResolverID(), TActorId(), new TEvTabletResolver::TEvNodeProblem(nodeId, problemEpoch)));
    }

    Y_UNIT_TEST(NodeProblem) {
        TTestBasicRuntime runtime(/* nodes */ 3);
        SetupTabletServices(runtime);

        runtime.SetLogPriority(NKikimrServices::TABLET_RESOLVER, NActors::NLog::PRI_DEBUG);

        TActorId firstLeader = runtime.AllocateEdgeActor(0);
        TActorId firstLeaderTablet = runtime.AllocateEdgeActor(0);
        DoRegisterTabletInfo(runtime, 123, 1, firstLeader, firstLeaderTablet);
        TActorId secondLeader = runtime.AllocateEdgeActor(0);
        TActorId secondLeaderTablet = runtime.AllocateEdgeActor(0);
        DoRegisterTabletInfo(runtime, 234, 1, secondLeader, secondLeaderTablet);

        {
            auto msg = DoResolveTablet(runtime, 123);
            UNIT_ASSERT_VALUES_EQUAL(msg->Status, NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(msg->Tablet, firstLeader);
            UNIT_ASSERT_VALUES_EQUAL(msg->TabletActor, firstLeaderTablet);
        }

        {
            auto msg = DoResolveTablet(runtime, 234);
            UNIT_ASSERT_VALUES_EQUAL(msg->Status, NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(msg->Tablet, secondLeader);
            UNIT_ASSERT_VALUES_EQUAL(msg->TabletActor, secondLeaderTablet);
        }

        // Move both tablets to node 2
        TActorId firstLeaderNode2 = runtime.AllocateEdgeActor(1);
        TActorId firstLeaderTabletNode2 = runtime.AllocateEdgeActor(1);
        DoRegisterTabletInfo(runtime, 123, 2, firstLeaderNode2, firstLeaderTabletNode2);
        TActorId secondLeaderNode2 = runtime.AllocateEdgeActor(1);
        TActorId secondLeaderTabletNode2 = runtime.AllocateEdgeActor(1);
        DoRegisterTabletInfo(runtime, 234, 2, secondLeaderNode2, secondLeaderTabletNode2);

        // We expect tablet resolver to return old cached results
        ui64 problemEpoch;
        {
            auto msg = DoResolveTablet(runtime, 123);
            UNIT_ASSERT_VALUES_EQUAL(msg->Status, NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(msg->Tablet, firstLeader);
            UNIT_ASSERT_VALUES_EQUAL(msg->TabletActor, firstLeaderTablet);
            problemEpoch = msg->CacheEpoch;
        }

        {
            auto msg = DoResolveTablet(runtime, 234);
            UNIT_ASSERT_VALUES_EQUAL(msg->Status, NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(msg->Tablet, secondLeader);
            UNIT_ASSERT_VALUES_EQUAL(msg->TabletActor, secondLeaderTablet);
        }

        // Let's send a node problem signal, it should invalidate both tablets
        DoSendNodeProblem(runtime, firstLeader.NodeId(), problemEpoch);

        // Check both tablets resolve to a new node
        ui64 nextProblemEpoch1;
        {
            auto msg = DoResolveTablet(runtime, 123);
            UNIT_ASSERT_VALUES_EQUAL(msg->Status, NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(msg->Tablet, firstLeaderNode2);
            UNIT_ASSERT_VALUES_EQUAL(msg->TabletActor, firstLeaderTabletNode2);
            UNIT_ASSERT(msg->CacheEpoch > problemEpoch);
            nextProblemEpoch1 = msg->CacheEpoch;
        }

        ui64 nextProblemEpoch2;
        {
            auto msg = DoResolveTablet(runtime, 234);
            UNIT_ASSERT_VALUES_EQUAL(msg->Status, NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(msg->Tablet, secondLeaderNode2);
            UNIT_ASSERT_VALUES_EQUAL(msg->TabletActor, secondLeaderTabletNode2);
            UNIT_ASSERT(msg->CacheEpoch > problemEpoch);
            nextProblemEpoch2 = msg->CacheEpoch;
        }

        // Move both tablets to node 3
        TActorId firstLeaderNode3 = runtime.AllocateEdgeActor(2);
        TActorId firstLeaderTabletNode3 = runtime.AllocateEdgeActor(2);
        DoRegisterTabletInfo(runtime, 123, 3, firstLeaderNode3, firstLeaderTabletNode3);
        TActorId secondLeaderNode3 = runtime.AllocateEdgeActor(2);
        TActorId secondLeaderTabletNode3 = runtime.AllocateEdgeActor(2);
        DoRegisterTabletInfo(runtime, 234, 3, secondLeaderNode3, secondLeaderTabletNode3);

        // Send an outdated node problem signal, it should not cause an invalidation
        DoSendNodeProblem(runtime, firstLeaderNode2.NodeId(), problemEpoch);

        // Check both tablets still resolve to node 2
        {
            auto msg = DoResolveTablet(runtime, 123);
            UNIT_ASSERT_VALUES_EQUAL(msg->Status, NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(msg->Tablet, firstLeaderNode2);
            UNIT_ASSERT_VALUES_EQUAL(msg->TabletActor, firstLeaderTabletNode2);
        }

        {
            auto msg = DoResolveTablet(runtime, 234);
            UNIT_ASSERT_VALUES_EQUAL(msg->Status, NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(msg->Tablet, secondLeaderNode2);
            UNIT_ASSERT_VALUES_EQUAL(msg->TabletActor, secondLeaderTabletNode2);
        }

        // Send a proper node problem signal, it should invalidate the first tablet
        DoSendNodeProblem(runtime, firstLeaderNode2.NodeId(), nextProblemEpoch1);

        // Check the first tablet resolves to node 3
        {
            auto msg = DoResolveTablet(runtime, 123);
            UNIT_ASSERT_VALUES_EQUAL(msg->Status, NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(msg->Tablet, firstLeaderNode3);
            UNIT_ASSERT_VALUES_EQUAL(msg->TabletActor, firstLeaderTabletNode3);
        }

        {
            auto msg = DoResolveTablet(runtime, 234);
            UNIT_ASSERT_VALUES_EQUAL(msg->Status, NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(msg->Tablet, secondLeaderNode2);
            UNIT_ASSERT_VALUES_EQUAL(msg->TabletActor, secondLeaderTabletNode2);
        }

        // Send a proper node problem signal, it should invalidate the second tablet too
        DoSendNodeProblem(runtime, firstLeaderNode2.NodeId(), nextProblemEpoch2);

        // Check the second tablet resolves to node 3 too
        {
            auto msg = DoResolveTablet(runtime, 123);
            UNIT_ASSERT_VALUES_EQUAL(msg->Status, NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(msg->Tablet, firstLeaderNode3);
            UNIT_ASSERT_VALUES_EQUAL(msg->TabletActor, firstLeaderTabletNode3);
        }

        {
            auto msg = DoResolveTablet(runtime, 234);
            UNIT_ASSERT_VALUES_EQUAL(msg->Status, NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(msg->Tablet, secondLeaderNode3);
            UNIT_ASSERT_VALUES_EQUAL(msg->TabletActor, secondLeaderTabletNode3);
        }
    }

}

} // namespace NKikimr
