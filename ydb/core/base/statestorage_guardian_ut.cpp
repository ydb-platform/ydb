#include "statestorage_impl.h"
#include "tablet.h"

#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/helpers.h>
#include <ydb/core/testlib/basics/runtime.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/algorithm.h>
#include <util/string/join.h>

namespace NKikimr {

Y_UNIT_TEST_SUITE(TStateStorageGuardian) {

    // The replicas already hold the record of the leader, with a step of its own, and the guardian of the tablet is
    // made then, as one made in place of a gone guardian is. The replicas send follower changes only to the guardian
    // they have registered, so the new guardian must register itself, keeping the step of the record.
    Y_UNIT_TEST(GuardianRegistersOverMatchingRecord) {
        TTestBasicRuntime runtime;
        SetupStateStorage(runtime, 0);
        runtime.Initialize(TAppPrepare().Unwrap());

        constexpr ui64 tabletId = 72075186224037888ull;
        constexpr ui32 generation = 3;
        constexpr ui32 step = 5;
        const TActorId edge = runtime.AllocateEdgeActor();
        const TActorId leader = runtime.AllocateEdgeActor();
        const TActorId tabletLeader = runtime.AllocateEdgeActor();

        runtime.Send(new IEventHandle(MakeStateStorageProxyID(), edge, new TEvStateStorage::TEvResolveReplicas(tabletId)));
        auto resolved = runtime.GrabEdgeEvent<TEvStateStorage::TEvResolveReplicasList>(edge);
        const auto replicas = resolved->Get()->GetPlainReplicas();
        UNIT_ASSERT(!replicas.empty());

        // the record of the leader in every replica
        for (const TActorId& replica : replicas) {
            runtime.Send(new IEventHandle(replica, edge, new TEvStateStorage::TEvReplicaLookup(tabletId, 1, 0, 0)));
            auto info = runtime.GrabEdgeEvent<TEvStateStorage::TEvReplicaInfo>(edge);
            auto update = MakeHolder<TEvStateStorage::TEvReplicaUpdate>();
            update->Record.SetTabletID(tabletId);
            update->Record.SetCookie(2);
            ActorIdToProto(leader, update->Record.MutableProposedLeader());
            ActorIdToProto(tabletLeader, update->Record.MutableProposedLeaderTablet());
            update->Record.SetProposedGeneration(generation);
            update->Record.SetProposedStep(step);
            update->Record.SetSignature(info->Get()->Record.GetSignature());
            runtime.Send(new IEventHandle(replica, edge, update.Release()));
            auto updated = runtime.GrabEdgeEvent<TEvStateStorage::TEvReplicaInfo>(edge);
            UNIT_ASSERT_VALUES_EQUAL(updated->Get()->Record.GetCurrentStep(), step);
        }

        THashSet<TActorId> registeredAt; // replicas the guardian registered at, with the step of the record
        auto updates = runtime.AddObserver<TEvStateStorage::TEvReplicaUpdate>([&](auto& ev) {
            const auto& record = ev->Get()->Record;
            if (record.GetIsGuardian() && record.GetTabletID() == tabletId) {
                UNIT_ASSERT_VALUES_EQUAL(record.GetProposedStep(), step);
                registeredAt.insert(ev->Recipient);
            }
        });
        runtime.Register(CreateStateStorageTabletGuardian(tabletId, leader, tabletLeader, generation));
        runtime.WaitFor("the guardian registered at every replica", [&] {
            return registeredAt.size() == replicas.size();
        }, TDuration::Seconds(10));
    }

    // The guardian has registered at the replicas. A follower registers at the first replica, the guardian resolves
    // the same replicas again and keeps its children, then another follower registers at the second replica. The
    // follower list for the leader must keep the first follower.
    Y_UNIT_TEST(ResolveKeepsFollowersOfKeptChildren) {
        TTestBasicRuntime runtime;
        SetupStateStorage(runtime, 0);
        runtime.Initialize(TAppPrepare().Unwrap());

        constexpr ui64 tabletId = 72075186224037888ull;
        const TActorId edge = runtime.AllocateEdgeActor();
        const TActorId leader = runtime.AllocateEdgeActor();
        const TActorId tabletLeader = runtime.AllocateEdgeActor();

        runtime.Send(new IEventHandle(MakeStateStorageProxyID(), edge, new TEvStateStorage::TEvResolveReplicas(tabletId)));
        auto resolved = runtime.GrabEdgeEvent<TEvStateStorage::TEvResolveReplicasList>(edge);
        const auto replicas = resolved->Get()->GetPlainReplicas();
        UNIT_ASSERT_GE(replicas.size(), 2u);

        THashSet<TActorId> registeredAt;
        auto updates = runtime.AddObserver<TEvStateStorage::TEvReplicaUpdate>([&](auto& ev) {
            if (ev->Get()->Record.GetIsGuardian() && ev->Get()->Record.GetTabletID() == tabletId) {
                registeredAt.insert(ev->Recipient);
            }
        });
        TVector<TActorId> followerList; // the latest list the leader got
        auto refreshes = runtime.AddObserver<TEvTablet::TEvFollowerListRefresh>([&](auto& ev) {
            if (ev->Recipient == leader) {
                followerList = ev->Get()->FollowerList;
            }
        });
        const TActorId guardian = runtime.Register(CreateStateStorageTabletGuardian(tabletId, leader, tabletLeader, 1));
        runtime.WaitFor("the guardian registered at every replica", [&] {
            return registeredAt.size() == replicas.size();
        }, TDuration::Seconds(10));
        runtime.SimulateSleep(TDuration::MilliSeconds(100));

        auto registerFollower = [&](const TActorId& replica) {
            const TActorId follower = runtime.AllocateEdgeActor();
            runtime.Send(new IEventHandle(replica, runtime.AllocateEdgeActor(), new TEvStateStorage::TEvReplicaRegFollower(
                tabletId, 1, follower, runtime.AllocateEdgeActor(), false, 0, 0)));
            runtime.SimulateSleep(TDuration::MilliSeconds(100));
            return follower;
        };
        const TActorId x = registerFollower(replicas[0]);
        UNIT_ASSERT_C(Find(followerList, x) != followerList.end(), JoinSeq(", ", followerList));

        // the same replicas, so the guardian keeps its children
        runtime.Send(new IEventHandle(guardian, MakeStateStorageProxyID(), resolved->Release().Release()));
        runtime.SimulateSleep(TDuration::MilliSeconds(10));

        const TActorId y = registerFollower(replicas[1]);
        UNIT_ASSERT_C(Find(followerList, y) != followerList.end(), JoinSeq(", ", followerList));
        UNIT_ASSERT_C(Find(followerList, x) != followerList.end(),
            "the follower of the kept child is lost: " << JoinSeq(", ", followerList));
    }

    // A replica is gone, so the child of the follower guardian for it reports the replica missing. The replica gets
    // back and registers the follower, which it acknowledges with a ping; the child must report the replica present.
    Y_UNIT_TEST(FollowerGuardianPingClearsMissingReplica) {
        TTestBasicRuntime runtime;
        SetupStateStorage(runtime, 0);
        runtime.Initialize(TAppPrepare().Unwrap());
        // the children of the guardian retry on wakeups, which the test runtime drops by default
        runtime.SetScheduledEventFilter([](auto&&, auto&&, auto&&, auto&&) { return false; });

        constexpr ui64 tabletId = 72075186224037888ull;
        const TActorId edge = runtime.AllocateEdgeActor();
        runtime.Send(new IEventHandle(MakeStateStorageProxyID(), edge, new TEvStateStorage::TEvResolveReplicas(tabletId)));
        auto resolved = runtime.GrabEdgeEvent<TEvStateStorage::TEvResolveReplicasList>(edge);
        const auto replicas = resolved->Get()->GetPlainReplicas();
        UNIT_ASSERT(!replicas.empty());
        const TActorId replica = replicas.front();
        runtime.Send(new IEventHandle(replica, edge, new TEvents::TEvPoisonPill()));

        // the private event a child of the guardian tells its parent whether its replica is missing with
        const ui32 replicaMissingType = EventSpaceBegin(TKikimrEvents::ES_PRIVATE) + 1;
        // a report is sent only when the state changes, so the first one says missing and the next one present
        ui32 reports = 0;
        bool acknowledge = false;
        auto registrations = runtime.AddObserver<TEvStateStorage::TEvReplicaRegFollower>([&](auto& ev) {
            if (ev->Recipient == replica && std::exchange(acknowledge, false)) {
                // the replica is back and registers the follower; this ping comes before the undelivery of the
                // registration the test runtime makes, as the replica actor is still gone for it
                runtime.Send(new IEventHandle(ev->Sender, replica, new TEvTablet::TEvPing(tabletId, 0)));
            }
        });
        TActorId guardian;
        auto missingReports = runtime.AddObserver([&](IEventHandle::TPtr& ev) {
            // other actors have private events of the same type, so only the ones to the guardian count
            if (ev->GetTypeRewrite() == replicaMissingType && guardian && ev->Recipient == guardian) {
                ++reports;
            }
        });

        const TActorId follower = runtime.AllocateEdgeActor();
        guardian = runtime.Register(CreateStateStorageFollowerGuardian(tabletId, 1, follower));
        runtime.WaitFor("the replica reported missing", [&] { return reports == 1; }, TDuration::Seconds(10));

        acknowledge = true; // at the next registration, made after a pause
        runtime.WaitFor("the replica reported present", [&] { return reports == 2; }, TDuration::Seconds(10));
    }

}

} // NKikimr
