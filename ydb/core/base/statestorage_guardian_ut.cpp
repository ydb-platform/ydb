#include "statestorage_impl.h"

#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/helpers.h>
#include <ydb/core/testlib/basics/runtime.h>

#include <library/cpp/testing/unittest/registar.h>

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

}

} // NKikimr
