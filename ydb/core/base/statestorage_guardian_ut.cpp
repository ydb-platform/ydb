#include "statestorage_impl.h"
#include "tablet.h"

#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/helpers.h>
#include <ydb/core/testlib/basics/runtime.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/algorithm.h>
#include <util/string/join.h>

namespace NKikimr {
namespace {

enum class ERegistrationFailure {
    None,
    Undelivered,
    StaleStep,
    StaleSignature,
};

void CheckGuardianRegistration(ERegistrationFailure failure) {
    TTestBasicRuntime runtime;
    SetupStateStorage(runtime, 0);
    runtime.Initialize(TAppPrepare().Unwrap());

    constexpr ui64 tabletId = 72075186224037888ull;
    constexpr ui32 generation = 3;
    constexpr ui32 step = 5;
    const TActorId edge = runtime.AllocateEdgeActor();
    const TActorId leader = runtime.AllocateEdgeActor();
    const TActorId tabletLeader = runtime.AllocateEdgeActor();
    const TActorId oldGuardian = runtime.AllocateEdgeActor();
    const TActorId followerGuardian = runtime.AllocateEdgeActor();
    const TActorId follower = runtime.AllocateEdgeActor();
    const TActorId followerTablet = runtime.AllocateEdgeActor();

    runtime.Send(new IEventHandle(MakeStateStorageProxyID(), edge, new TEvStateStorage::TEvResolveReplicas(tabletId)));
    auto resolved = runtime.GrabEdgeEvent<TEvStateStorage::TEvResolveReplicasList>(edge);
    const auto replicas = resolved->Get()->GetPlainReplicas();
    UNIT_ASSERT(!replicas.empty());

    THashMap<TActorId, TActorId> replicaServices;
    // Keep both the leader record and the address of the previous guardian.
    for (const TActorId& replica : replicas) {
        runtime.Send(new IEventHandle(replica, edge, new TEvStateStorage::TEvReplicaLookup(tabletId, 1, 0, 0)));
        auto info = runtime.GrabEdgeEvent<TEvStateStorage::TEvReplicaInfo>(edge);
        replicaServices[info->Sender] = replica;
        auto update = MakeHolder<TEvStateStorage::TEvReplicaUpdate>();
        update->Record.SetTabletID(tabletId);
        update->Record.SetCookie(2);
        ActorIdToProto(leader, update->Record.MutableProposedLeader());
        ActorIdToProto(tabletLeader, update->Record.MutableProposedLeaderTablet());
        update->Record.SetProposedGeneration(generation);
        update->Record.SetProposedStep(step);
        update->Record.SetSignature(info->Get()->Record.GetSignature());
        update->Record.SetIsGuardian(true);
        runtime.Send(new IEventHandle(replica, oldGuardian, update.Release()));
        auto updated = runtime.GrabEdgeEvent<TEvStateStorage::TEvReplicaInfo>(oldGuardian);
        UNIT_ASSERT_VALUES_EQUAL(updated->Get()->Record.GetCurrentStep(), step);
    }

    THashMap<TActorId, TActorId> guardians;
    THashMap<TActorId, ui32> attempts;
    THashSet<TActorId> snapshots;
    THashSet<TActorId> notified;
    TActorId tabletGuardian;
    bool injected = false;
    bool followerListRefreshed = false;

    auto lookups = runtime.AddObserver<TEvStateStorage::TEvReplicaLookup>([&](auto& ev) {
        if (ev->Get()->Record.GetTabletID() == tabletId) {
            guardians[ev->Recipient] = ev->Sender;
        }
    });
    auto infos = runtime.AddObserver<TEvStateStorage::TEvReplicaInfo>([&](auto& ev) {
        auto& record = ev->Get()->Record;
        if (record.GetTabletID() != tabletId) {
            return;
        }
        const auto service = replicaServices.find(ev->Sender);
        const auto it = service == replicaServices.end() ? guardians.end() : guardians.find(service->second);
        if (it != guardians.end() && ev->Recipient == it->second) {
            // Return an old lookup snapshot while the real replica already has
            // a newer step or signature. Its registration check stays unchanged.
            if (!injected && service->second == replicas.front() && record.GetCookie() == 1) {
                if (failure == ERegistrationFailure::StaleStep) {
                    record.SetCurrentStep(step - 1);
                    injected = true;
                } else if (failure == ERegistrationFailure::StaleSignature) {
                    record.SetSignature(record.GetSignature() ^ 1);
                    injected = true;
                }
            }
            if (record.FollowerInfoSize()) {
                UNIT_ASSERT_VALUES_EQUAL(ActorIdFromProto(record.GetFollowerInfo(0).GetFollower()), follower);
                notified.insert(service->second);
            }
        }
        if (ev->Recipient == tabletGuardian) {
            snapshots.insert(ev->Sender);
        }
    });
    auto updates = runtime.AddObserver<TEvStateStorage::TEvReplicaUpdate>([&](auto& ev) {
        const auto& record = ev->Get()->Record;
        if (!record.GetIsGuardian() || record.GetTabletID() != tabletId) {
            return;
        }
        const ui32 attempt = ++attempts[ev->Recipient];
        const ui32 expectedStep = failure == ERegistrationFailure::StaleStep
            && ev->Recipient == replicas.front() && attempt == 1 ? step - 1 : step;
        UNIT_ASSERT_VALUES_EQUAL(record.GetProposedStep(), expectedStep);
        if (failure == ERegistrationFailure::Undelivered && !injected && ev->Recipient == replicas.front()) {
            injected = true;
            runtime.EnableScheduleForActor(ev->Sender);
            runtime.Send(new IEventHandle(ev->Sender, ev->Recipient,
                new TEvents::TEvUndelivered(TEvStateStorage::TEvReplicaUpdate::EventType,
                    TEvents::TEvUndelivered::Disconnected), 0, ev->Cookie));
            ev.Reset();
        }
    });
    auto refreshes = runtime.AddObserver<TEvTablet::TEvFollowerListRefresh>([&](auto& ev) {
        if (ev->Recipient == leader && Find(ev->Get()->FollowerList, follower) != ev->Get()->FollowerList.end()) {
            followerListRefreshed = true;
        }
    });

    tabletGuardian = runtime.Register(CreateStateStorageTabletGuardian(tabletId, leader, tabletLeader, generation));
    runtime.WaitFor("the guardian received the replica snapshots", [&] {
        return snapshots.size() == replicas.size();
    }, TDuration::Seconds(10));
    if (failure != ERegistrationFailure::None) {
        UNIT_ASSERT(injected);
        UNIT_ASSERT_C(attempts[replicas.front()] >= 2, "the failed registration was not retried");
    }

    for (const TActorId& replica : replicas) {
        runtime.Send(new IEventHandle(replica, followerGuardian,
            new TEvStateStorage::TEvReplicaRegFollower(tabletId, 1, follower, followerTablet, false, 0, 0)));
    }
    runtime.WaitFor("every replica notified the new guardian and the leader learned about the follower", [&] {
        return notified.size() == replicas.size() && followerListRefreshed;
    }, TDuration::Seconds(10));
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(TStateStorageGuardian) {
    Y_UNIT_TEST(GuardianRegistersOverMatchingRecord) {
        CheckGuardianRegistration(ERegistrationFailure::None);
    }

    Y_UNIT_TEST(GuardianRetriesUndeliveredRegistration) {
        CheckGuardianRegistration(ERegistrationFailure::Undelivered);
    }

    Y_UNIT_TEST(GuardianRetriesRegistrationWithStaleStep) {
        CheckGuardianRegistration(ERegistrationFailure::StaleStep);
    }

    Y_UNIT_TEST(GuardianRetriesRegistrationWithStaleSignature) {
        CheckGuardianRegistration(ERegistrationFailure::StaleSignature);
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
        runtime.Send(new IEventHandle(guardian, MakeStateStorageProxyID(), resolved->Release().Release()), 0, true);
        runtime.SimulateSleep(TDuration::MilliSeconds(10));

        const TActorId y = registerFollower(replicas[1]);
        UNIT_ASSERT_C(Find(followerList, y) != followerList.end(), JoinSeq(", ", followerList));
        UNIT_ASSERT_C(Find(followerList, x) != followerList.end(),
            "the follower of the kept child is lost: " << JoinSeq(", ", followerList));
    }

    // The guardian has registered at the replicas, and a follower registers at the first replica only. The guardian
    // resolves the replicas again without the first one, so the child that reported the follower goes away. The
    // leader must get the list without that follower at once.
    Y_UNIT_TEST(ResolveDropsFollowersOfRemovedChildren) {
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
        UNIT_ASSERT_VALUES_EQUAL(resolved->Get()->ReplicaGroups.size(), 1);

        THashSet<TActorId> registeredAt;
        auto updates = runtime.AddObserver<TEvStateStorage::TEvReplicaUpdate>([&](auto& ev) {
            if (ev->Get()->Record.GetIsGuardian() && ev->Get()->Record.GetTabletID() == tabletId) {
                registeredAt.insert(ev->Recipient);
            }
        });
        TVector<TActorId> followerList; // the latest list the leader got
        ui32 refreshes = 0;
        auto refreshObserver = runtime.AddObserver<TEvTablet::TEvFollowerListRefresh>([&](auto& ev) {
            if (ev->Recipient == leader) {
                followerList = ev->Get()->FollowerList;
                ++refreshes;
            }
        });
        const TActorId guardian = runtime.Register(CreateStateStorageTabletGuardian(tabletId, leader, tabletLeader, 1));
        runtime.WaitFor("the guardian registered at every replica", [&] {
            return registeredAt.size() == replicas.size();
        }, TDuration::Seconds(10));
        runtime.SimulateSleep(TDuration::MilliSeconds(100));

        const TActorId follower = runtime.AllocateEdgeActor();
        runtime.Send(new IEventHandle(replicas[0], runtime.AllocateEdgeActor(), new TEvStateStorage::TEvReplicaRegFollower(
            tabletId, 1, follower, runtime.AllocateEdgeActor(), false, 0, 0)));
        runtime.SimulateSleep(TDuration::MilliSeconds(100));
        UNIT_ASSERT_C(Find(followerList, follower) != followerList.end(), JoinSeq(", ", followerList));

        // the same replicas but the first one, so its child goes away along with the follower it reported
        auto withoutFirst = resolved->Release();
        auto& groupReplicas = withoutFirst->ReplicaGroups[0].Replicas;
        groupReplicas.erase(groupReplicas.begin());
        const ui32 refreshesBefore = refreshes;
        runtime.Send(new IEventHandle(guardian, MakeStateStorageProxyID(), withoutFirst.Release()), 0, true);
        runtime.WaitFor("the leader gets the list again", [&] { return refreshes > refreshesBefore; },
            TDuration::Seconds(1));

        UNIT_ASSERT_C(refreshes > refreshesBefore, "the leader didn't get the list again");
        UNIT_ASSERT_C(followerList.empty(), "the follower of the removed child is kept: " << JoinSeq(", ", followerList));
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

    // One ring group of four rings with two replicas each, and three rings are selected for the tablet, one replica in
    // each. Two of the three selected replicas are gone. The other replicas don't hold the record of the tablet and
    // reply ALREADY, which must not make up for the selected ones: the record is still on two of them when they get
    // back, so the delete must not report success.
    Y_UNIT_TEST(DeleteQuorumCountsSelectedReplicasOnly) {
        TTestBasicRuntime runtime(8);
        SetupCustomStateStorage(runtime, 3, 4, 2, 1);
        runtime.Initialize(TAppPrepare().Unwrap());

        constexpr ui64 tabletId = 72075186224037888ull;
        const TActorId edge = runtime.AllocateEdgeActor();
        runtime.Send(new IEventHandle(MakeStateStorageProxyID(), edge, new TEvStateStorage::TEvResolveReplicas(tabletId)));
        auto resolved = runtime.GrabEdgeEvent<TEvStateStorage::TEvResolveReplicasList>(edge);
        const auto& groups = resolved->Get()->ReplicaGroups;
        UNIT_ASSERT_VALUES_EQUAL(groups.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(groups[0].Replicas.size(), 3); // the selected ones
        for (ui32 i : {1, 2}) {
            runtime.Send(new IEventHandle(groups[0].Replicas[i], edge, new TEvents::TEvPoisonPill()));
        }
        runtime.SimulateSleep(TDuration::MilliSeconds(10));

        ui32 replies = 0;
        auto counter = runtime.AddObserver<TEvStateStorage::TEvReplicaInfo>([&](auto& ev) {
            if (ev->Get()->Record.GetTabletID() == tabletId) {
                ++replies;
            }
        });
        runtime.Send(new IEventHandle(MakeStateStorageProxyID(), edge, new TEvStateStorage::TEvDelete(tabletId)));
        auto result = runtime.GrabEdgeEvent<TEvStateStorage::TEvDeleteResult>(edge, TDuration::Seconds(70));
        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL(replies, 6); // all the replicas but the two selected ones that are gone
        UNIT_ASSERT_VALUES_UNEQUAL_C(result->Get()->Status, NKikimrProto::OK,
            "the delete reached one of the three replicas selected for the tablet");
    }
}

} // NKikimr
