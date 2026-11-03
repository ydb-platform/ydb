#include "statestorage_impl.h"
#include "tablet.h"

#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/helpers.h>
#include <ydb/core/testlib/basics/runtime.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/algorithm.h>

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
}

} // NKikimr
