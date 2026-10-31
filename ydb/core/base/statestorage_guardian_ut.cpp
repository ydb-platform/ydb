#include "statestorage_impl.h"
#include "tablet.h"

#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/helpers.h>
#include <ydb/core/testlib/basics/runtime.h>

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/library/actors/core/mon.h>

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

    // A follower registers at the replica and unregisters then. The replica must tell the guardian of the leader,
    // which keeps the list of followers, not the guardian of the follower going away.
    Y_UNIT_TEST(FollowerUnregisterNotifiesLeaderGuardian) {
        TTestBasicRuntime runtime;
        SetupStateStorage(runtime, 0);
        runtime.Initialize(TAppPrepare().Unwrap());

        constexpr ui64 tabletId = 72075186224037888ull;
        const TActorId edge = runtime.AllocateEdgeActor();
        runtime.Send(new IEventHandle(MakeStateStorageProxyID(), edge, new TEvStateStorage::TEvResolveReplicas(tabletId)));
        auto resolved = runtime.GrabEdgeEvent<TEvStateStorage::TEvResolveReplicasList>(edge);
        const TActorId replica = resolved->Get()->GetPlainReplicas().front();

        // the guardian of the leader registers at the replica
        const TActorId leaderGuardian = runtime.AllocateEdgeActor();
        runtime.Send(new IEventHandle(replica, leaderGuardian, new TEvStateStorage::TEvReplicaLookup(tabletId, 1, 0, 0)));
        auto info = runtime.GrabEdgeEvent<TEvStateStorage::TEvReplicaInfo>(leaderGuardian);
        auto update = MakeHolder<TEvStateStorage::TEvReplicaUpdate>();
        update->Record.SetTabletID(tabletId);
        update->Record.SetCookie(2);
        ActorIdToProto(runtime.AllocateEdgeActor(), update->Record.MutableProposedLeader());
        ActorIdToProto(runtime.AllocateEdgeActor(), update->Record.MutableProposedLeaderTablet());
        update->Record.SetProposedGeneration(1);
        update->Record.SetSignature(info->Get()->Record.GetSignature());
        update->Record.SetIsGuardian(true);
        runtime.Send(new IEventHandle(replica, leaderGuardian, update.Release()));
        runtime.GrabEdgeEvent<TEvStateStorage::TEvReplicaInfo>(leaderGuardian);

        // a follower registers, and the guardian of the leader learns of it
        const TActorId followerGuardian = runtime.AllocateEdgeActor();
        const TActorId follower = runtime.AllocateEdgeActor();
        runtime.Send(new IEventHandle(replica, followerGuardian, new TEvStateStorage::TEvReplicaRegFollower(tabletId, 1,
            follower, runtime.AllocateEdgeActor(), false, 0, 0)));
        auto registered = runtime.GrabEdgeEvent<TEvStateStorage::TEvReplicaInfo>(leaderGuardian);
        UNIT_ASSERT_VALUES_EQUAL(registered->Get()->Record.FollowerInfoSize(), 1);

        // the follower unregisters; the guardian of the leader must learn the list without it
        runtime.Send(new IEventHandle(replica, followerGuardian, new TEvStateStorage::TEvReplicaUnregFollower(tabletId, 1,
            follower, 0, 0)));
        auto unregistered = runtime.GrabEdgeEvent<TEvStateStorage::TEvReplicaInfo>(leaderGuardian, TDuration::Seconds(5));
        UNIT_ASSERT_C(unregistered, "the guardian of the leader didn't learn the follower is gone");
        UNIT_ASSERT_VALUES_EQUAL(unregistered->Get()->Record.FollowerInfoSize(), 0);
    }

    // Two ring groups of three replicas each, and two replicas of the second one are gone. The delete reaches four
    // replicas of six, more than half, but only one of three in the second group, which still has the record on the
    // majority of its replicas when they get back. The delete must not report success.
    Y_UNIT_TEST(DeleteNeedsMajorityOfEachRingGroup) {
        TTestBasicRuntime runtime(6);
        SetupCustomStateStorage(runtime, 3, 3, 1, 2);
        runtime.Initialize(TAppPrepare().Unwrap());

        constexpr ui64 tabletId = 72075186224037888ull;
        const TActorId edge = runtime.AllocateEdgeActor();
        runtime.Send(new IEventHandle(MakeStateStorageProxyID(), edge, new TEvStateStorage::TEvResolveReplicas(tabletId)));
        auto resolved = runtime.GrabEdgeEvent<TEvStateStorage::TEvResolveReplicasList>(edge);
        const auto& groups = resolved->Get()->ReplicaGroups;
        UNIT_ASSERT_VALUES_EQUAL(groups.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(groups[1].Replicas.size(), 3);
        for (ui32 i : {1, 2}) {
            runtime.Send(new IEventHandle(groups[1].Replicas[i], edge, new TEvents::TEvPoisonPill()));
        }
        runtime.SimulateSleep(TDuration::MilliSeconds(10));

        runtime.Send(new IEventHandle(MakeStateStorageProxyID(), edge, new TEvStateStorage::TEvDelete(tabletId)));
        auto result = runtime.GrabEdgeEvent<TEvStateStorage::TEvDeleteResult>(edge, TDuration::Seconds(70));
        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_UNEQUAL_C(result->Get()->Status, NKikimrProto::OK,
            "the delete reached one replica of three in the second ring group");
    }

    // The same, but one replica of the first ring group is of an older version and replies without the cookie telling
    // its group. That reply must not make the delete count all replies together as one group.
    Y_UNIT_TEST(DeleteReplyWithoutCookieKeepsQuorumOfEachRingGroup) {
        TTestBasicRuntime runtime(6);
        SetupCustomStateStorage(runtime, 3, 3, 1, 2);
        runtime.Initialize(TAppPrepare().Unwrap());

        constexpr ui64 tabletId = 72075186224037888ull;
        const TActorId edge = runtime.AllocateEdgeActor();
        runtime.Send(new IEventHandle(MakeStateStorageProxyID(), edge, new TEvStateStorage::TEvResolveReplicas(tabletId)));
        auto resolved = runtime.GrabEdgeEvent<TEvStateStorage::TEvResolveReplicasList>(edge);
        const auto& groups = resolved->Get()->ReplicaGroups;
        UNIT_ASSERT_VALUES_EQUAL(groups.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(groups[1].Replicas.size(), 3);
        for (ui32 i : {1, 2}) {
            runtime.Send(new IEventHandle(groups[1].Replicas[i], edge, new TEvents::TEvPoisonPill()));
        }
        runtime.SimulateSleep(TDuration::MilliSeconds(10));

        bool cleared = false;
        auto legacyReply = runtime.AddObserver([&](IEventHandle::TPtr& ev) {
            if (!cleared && ev->GetTypeRewrite() == TEvStateStorage::EvReplicaInfo && ev->Cookie == 1) {
                cleared = true;
                ev = new IEventHandle(ev->Recipient, ev->Sender, ev->ReleaseBase().Release(), ev->Flags, 0);
            }
        });
        runtime.Send(new IEventHandle(MakeStateStorageProxyID(), edge, new TEvStateStorage::TEvDelete(tabletId)));
        auto result = runtime.GrabEdgeEvent<TEvStateStorage::TEvDeleteResult>(edge, TDuration::Seconds(70));
        UNIT_ASSERT(result);
        UNIT_ASSERT(cleared);
        UNIT_ASSERT_VALUES_UNEQUAL_C(result->Get()->Status, NKikimrProto::OK,
            "the delete reached one replica of three in the second ring group");
    }

    // One ring group of three replicas. The first replica deletes the record, the second one answers with a newer
    // cluster state, so the replicas the delete went to may be outdated. Two replies of three are a majority, but the
    // delete must not report success.
    Y_UNIT_TEST(DeleteFailsOnConfigMismatch) {
        TTestBasicRuntime runtime(3);
        SetupCustomStateStorage(runtime, 3, 3, 1);
        runtime.Initialize(TAppPrepare().Unwrap());

        constexpr ui64 tabletId = 72075186224037888ull;
        const TActorId edge = runtime.AllocateEdgeActor();
        ui32 replies = 0;
        auto rewrite = runtime.AddObserver<TEvStateStorage::TEvReplicaInfo>([&](auto& ev) {
            if (ev->Get()->Record.GetTabletID() == tabletId && ++replies == 2) {
                ev->Get()->Record.SetClusterStateGeneration(Max<ui64>());
            }
        });
        runtime.Send(new IEventHandle(MakeStateStorageProxyID(), edge, new TEvStateStorage::TEvDelete(tabletId)));
        auto result = runtime.GrabEdgeEvent<TEvStateStorage::TEvDeleteResult>(edge, TDuration::Seconds(70));
        UNIT_ASSERT(result);
        UNIT_ASSERT_GE(replies, 2u);
        UNIT_ASSERT_VALUES_UNEQUAL_C(result->Get()->Status, NKikimrProto::OK,
            "the delete succeeded after a replica showed a newer configuration");
    }

    // A follower is registered at a replica; the monitoring page of the tablet must list it
    Y_UNIT_TEST(MonitoringShowsFollowers) {
        TTestBasicRuntime runtime;
        SetupStateStorage(runtime, 0);
        runtime.Initialize(TAppPrepare().Unwrap());

        constexpr ui64 tabletId = 72075186224037888ull;
        const TActorId edge = runtime.AllocateEdgeActor();
        runtime.Send(new IEventHandle(MakeStateStorageProxyID(), edge, new TEvStateStorage::TEvResolveReplicas(tabletId)));
        auto resolved = runtime.GrabEdgeEvent<TEvStateStorage::TEvResolveReplicasList>(edge);
        const TActorId replica = resolved->Get()->GetPlainReplicas().front();

        // the record of the leader, then a follower
        runtime.Send(new IEventHandle(replica, edge, new TEvStateStorage::TEvReplicaLookup(tabletId, 1, 0, 0)));
        auto info = runtime.GrabEdgeEvent<TEvStateStorage::TEvReplicaInfo>(edge);
        auto update = MakeHolder<TEvStateStorage::TEvReplicaUpdate>();
        update->Record.SetTabletID(tabletId);
        update->Record.SetCookie(2);
        ActorIdToProto(runtime.AllocateEdgeActor(), update->Record.MutableProposedLeader());
        ActorIdToProto(runtime.AllocateEdgeActor(), update->Record.MutableProposedLeaderTablet());
        update->Record.SetProposedGeneration(1);
        update->Record.SetSignature(info->Get()->Record.GetSignature());
        runtime.Send(new IEventHandle(replica, edge, update.Release()));
        runtime.GrabEdgeEvent<TEvStateStorage::TEvReplicaInfo>(edge);
        const TActorId follower = runtime.AllocateEdgeActor();
        runtime.Send(new IEventHandle(replica, runtime.AllocateEdgeActor(), new TEvStateStorage::TEvReplicaRegFollower(
            tabletId, 1, follower, runtime.AllocateEdgeActor(), false, 0, 0)));
        runtime.SimulateSleep(TDuration::MilliSeconds(10));

        runtime.Register(CreateStateStorageMonitoringActor(tabletId, edge, ""));
        auto page = runtime.GrabEdgeEvent<NMon::TEvHttpInfoRes>(edge, TDuration::Seconds(10));
        UNIT_ASSERT(page);
        TStringStream html;
        page->Get()->Output(html);
        UNIT_ASSERT_C(html.Str().Contains(follower.ToString()), html.Str());
    }

    // A replica is gone from the start. Its child must report it missing only after three seconds of failures, not
    // at the first one.
    Y_UNIT_TEST(MissingReplicaReportedAfterGracePeriod) {
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
        runtime.Send(new IEventHandle(replicas.front(), edge, new TEvents::TEvPoisonPill()));

        const ui32 replicaMissingType = EventSpaceBegin(TKikimrEvents::ES_PRIVATE) + 1;
        TActorId guardian;
        TInstant reportedAt;
        auto missingReports = runtime.AddObserver([&](IEventHandle::TPtr& ev) {
            if (ev->GetTypeRewrite() == replicaMissingType && guardian && ev->Recipient == guardian && !reportedAt) {
                reportedAt = runtime.GetCurrentTime();
            }
        });

        const TActorId follower = runtime.AllocateEdgeActor();
        const TInstant startedAt = runtime.GetCurrentTime();
        guardian = runtime.Register(CreateStateStorageFollowerGuardian(tabletId, 1, follower));
        runtime.WaitFor("the replica reported missing", [&] { return bool(reportedAt); }, TDuration::Seconds(10));
        UNIT_ASSERT_GE_C(reportedAt - startedAt, TDuration::Seconds(3), "reported missing after " << reportedAt - startedAt);
    }

    // The replica fails, gets back within the grace period, and fails again much later. The second failure starts a
    // grace period of its own, so the child must not report the replica missing at once.
    Y_UNIT_TEST(GracePeriodStartsAgainAfterRecovery) {
        TTestBasicRuntime runtime;
        SetupStateStorage(runtime, 0);
        runtime.Initialize(TAppPrepare().Unwrap());

        constexpr ui64 tabletId = 72075186224037888ull;
        const TActorId edge = runtime.AllocateEdgeActor();
        runtime.Send(new IEventHandle(MakeStateStorageProxyID(), edge, new TEvStateStorage::TEvResolveReplicas(tabletId)));
        auto resolved = runtime.GrabEdgeEvent<TEvStateStorage::TEvResolveReplicasList>(edge);
        const auto replicas = resolved->Get()->GetPlainReplicas();
        UNIT_ASSERT(!replicas.empty());
        const TActorId replica = replicas.front();

        // the child of the replica and the cookie of its latest request; the first lookup is lost, as by a failure
        TActorId child;
        ui64 lastCookie = 0;
        bool dropLookup = true;
        auto lookups = runtime.AddObserver<TEvStateStorage::TEvReplicaLookup>([&](auto& ev) {
            if (ev->Recipient == replica) {
                child = ev->Sender;
                lastCookie = ev->Cookie;
                if (std::exchange(dropLookup, false)) {
                    ev.Reset();
                }
            }
        });
        auto updates = runtime.AddObserver<TEvStateStorage::TEvReplicaUpdate>([&](auto& ev) {
            if (ev->Recipient == replica && ev->Sender == child) {
                lastCookie = ev->Cookie;
            }
        });
        const ui32 replicaMissingType = EventSpaceBegin(TKikimrEvents::ES_PRIVATE) + 1;
        TActorId guardian;
        ui32 missingReports = 0;
        auto reports = runtime.AddObserver([&](IEventHandle::TPtr& ev) {
            if (ev->GetTypeRewrite() == replicaMissingType && guardian && ev->Recipient == guardian) {
                ++missingReports;
            }
        });
        auto fail = [&] {
            runtime.Send(new IEventHandle(child, replica, new TEvents::TEvUndelivered(TEvStateStorage::EvReplicaLookup,
                TEvents::TEvUndelivered::ReasonActorUnknown), 0, lastCookie));
        };

        guardian = runtime.Register(CreateStateStorageTabletGuardian(tabletId, runtime.AllocateEdgeActor(),
            runtime.AllocateEdgeActor(), 1));
        runtime.WaitFor("the lookup of the child", [&] { return bool(child); }, TDuration::Seconds(10));
        fail(); // the first failure; the next lookup reaches the replica within the grace period
        runtime.SimulateSleep(TDuration::Seconds(10));
        UNIT_ASSERT_VALUES_EQUAL(missingReports, 0);

        fail(); // the second failure, long after the first one
        runtime.SimulateSleep(TDuration::MilliSeconds(100));
        UNIT_ASSERT_VALUES_EQUAL_C(missingReports, 0, "the second failure was reported at once");
    }

}

} // NKikimr
