#include "blobstorage_groupinfo.h"
#include "blobstorage_groupinfo_iter.h"
#include "blobstorage_groupinfo_sets.h"
#include "blobstorage_groupinfo_partlayout.h"
#include <ydb/core/blobstorage/vdisk/ingress/blobstorage_ingress.h>
#include <ydb/core/protos/bridge.pb.h>
#include <library/cpp/testing/unittest/registar.h>

#include <bit>

namespace NKikimr {

Y_UNIT_TEST_SUITE(TBlobStorageGroupInfoTest) {

    Y_UNIT_TEST(ParseEncryptedGroupWithoutKeyReportsError) {
        NKikimrBlobStorage::TGroupInfo group;
        group.SetGroupID(0);
        group.SetGroupGeneration(1);
        group.SetErasureSpecies(TBlobStorageGroupType::ErasureNone);
        group.AddRings()->AddFailDomains()->AddVDiskLocations();
        group.SetEncryptionMode(TBlobStorageGroupInfo::EEM_ENC_V1);
        group.SetLifeCyclePhase(TBlobStorageGroupInfo::ELCP_IN_USE);
        TStringStream error;
        const auto info = TBlobStorageGroupInfo::Parse(group, nullptr, &error);
        UNIT_ASSERT(info);
        UNIT_ASSERT_VALUES_EQUAL(info->GetLifeCyclePhase(), TBlobStorageGroupInfo::ELCP_KEY_NOT_LOADED);
        UNIT_ASSERT(!error.Str().empty());
    }

    Y_UNIT_TEST(Mirror3dcStateContract) {
        using T = NKikimrBlobStorage::TMirror3dcGroupState;
        using TInfo = TBlobStorageGroupInfo;
        using TState = TInfo::TMirror3dcState;
        const TInfo::TTopology topology(TBlobStorageGroupType::ErasureMirror3dc, 3, 3, 1, true);
        const TInfo::TTopology other(TBlobStorageGroupType::ErasureNone, 1, 1, 1, true);
        const TInfo::TTopology empty(TBlobStorageGroupType::ErasureMirror3dc, 0, 0, 0);
        T proto;
        UNIT_ASSERT(!TState::Parse(proto, topology));
        proto.SetMode(T::NORMAL);
        UNIT_ASSERT(TState::Parse(proto, topology));
        UNIT_ASSERT(!TState::Parse(proto, other));
        UNIT_ASSERT(!TState::Parse(proto, empty));
        proto.SetFailedFailRealmIdx(0);
        UNIT_ASSERT(!TState::Parse(proto, topology));
        proto.SetMode(T::DC_FAILURE);
        UNIT_ASSERT(TState::Parse(proto, topology));
        proto.SetFailedFailRealmIdx(3);
        UNIT_ASSERT(!TState::Parse(proto, topology));
        proto.SetFailedFailRealmIdx(0);
        proto.SetMode(T::SOLO_DC_INV_1);
        UNIT_ASSERT(!TState::Parse(proto, topology));
        proto.SetSurvivingFailRealmIdx(0);
        UNIT_ASSERT(!TState::Parse(proto, topology));
        proto.SetSurvivingFailRealmIdx(2);
        UNIT_ASSERT(TState::Parse(proto, topology));
        for (auto mode : {T::SOLO_DC_DISASTER, T::SOLO_DC_RESURECTION_PENDING}) {
            proto.SetMode(mode);
            UNIT_ASSERT(TState::Parse(proto, topology));
            proto.ClearSurvivingFailRealmIdx();
            UNIT_ASSERT(!TState::Parse(proto, topology));
            proto.SetSurvivingFailRealmIdx(2);
        }

        TState normal;
        TState pending{T::DC_FAILURE_PENDING, 0, {}};
        TState pendingInv1{T::DC_FAILURE_PENDING_INV_1, 0, {}};
        TState pendingInv2{T::DC_FAILURE_PENDING_INV_2, 0, {}};
        TState failed{T::DC_FAILURE, 0, {}};
        TState inv1{T::DC_FAILURE_INV_1, 0, {}};
        TState inv2{T::DC_FAILURE_INV_2, 0, {}};
        TState solo1{T::SOLO_DC_INV_1, 0, 2};
        TState solo2{T::SOLO_DC_INV_2, 0, 2};
        UNIT_ASSERT(!normal.CanTransitionTo(failed));
        UNIT_ASSERT(normal.CanTransitionTo(pending));
        UNIT_ASSERT(!normal.CanTransitionTo(pendingInv1));
        UNIT_ASSERT(!normal.CanTransitionTo(pendingInv2));
        UNIT_ASSERT(pending.CanTransitionTo(pendingInv1));
        UNIT_ASSERT(pendingInv1.CanTransitionTo(pendingInv2));
        UNIT_ASSERT(!pending.CanTransitionTo(pendingInv2));
        UNIT_ASSERT(!pendingInv1.CanTransitionTo(pending));
        UNIT_ASSERT(!pendingInv2.CanTransitionTo(pendingInv1));
        UNIT_ASSERT(pending.CanTransitionTo(failed));
        UNIT_ASSERT(!pending.CanTransitionTo(inv1));
        UNIT_ASSERT(pendingInv1.CanTransitionTo(inv1));
        UNIT_ASSERT(!pendingInv1.CanTransitionTo(failed));
        UNIT_ASSERT(pendingInv2.CanTransitionTo(inv2));
        UNIT_ASSERT(!pendingInv2.CanTransitionTo(inv1));
        UNIT_ASSERT(pending.GetFailRealmStates() == pendingInv1.GetFailRealmStates());
        UNIT_ASSERT(pending.GetFailRealmStates() == pendingInv2.GetFailRealmStates());
        UNIT_ASSERT(pending.GetFailRealmStates()[0].State == TInfo::EFailRealmState::Leaving);
        UNIT_ASSERT(!pending.GetWritePolicy().CheckWriteOk({1, 1}));
        UNIT_ASSERT(pending.GetWritePolicy().CheckWriteOk({2, 2}));
        UNIT_ASSERT(pending.GetWritePolicy().CheckWriteDurability({1, 1}));
        UNIT_ASSERT(!pendingInv1.GetWritePolicy().CheckWriteDurability({1, 1}));
        UNIT_ASSERT(pendingInv1.GetWritePolicy() == pendingInv2.GetWritePolicy());
        UNIT_ASSERT(failed.CanTransitionTo(inv1));
        UNIT_ASSERT(!failed.CanTransitionTo(inv2));
        UNIT_ASSERT(inv1.CanTransitionTo(inv2));
        UNIT_ASSERT(!inv2.CanTransitionTo(inv1));
        UNIT_ASSERT(inv1.CanTransitionTo(solo1));
        UNIT_ASSERT(inv2.CanTransitionTo(solo2));
        UNIT_ASSERT(!solo1.CanTransitionTo(normal));
        UNIT_ASSERT(!solo2.CanTransitionTo(inv2));
        TState leavingForSolo1{T::SOLO_DC_INV_1_PENDING, 0, 2};
        TState leavingForSolo2{T::SOLO_DC_INV_2_PENDING, 0, 2};
        TState preparingRecovery1{T::SOLO_DC_RECOVERY_PENDING_INV_1, 0, 2};
        TState preparingRecovery2{T::SOLO_DC_RECOVERY_PENDING_INV_2, 0, 2};
        TState soloRecovery1{T::SOLO_DC_RECOVERY_INV_1, 0, 2};
        TState disaster{T::SOLO_DC_DISASTER, 0, 2};
        TState resurection{T::SOLO_DC_RESURECTION, 0, 2};
        TState resurectionPending{T::SOLO_DC_RESURECTION_PENDING, 0, 2};
        UNIT_ASSERT(disaster.CanTransitionTo(resurectionPending));
        UNIT_ASSERT(!disaster.CanTransitionTo(resurection));
        UNIT_ASSERT(resurectionPending.CanTransitionTo(resurection));
        UNIT_ASSERT(!resurectionPending.CanTransitionTo(preparingRecovery1));
        UNIT_ASSERT(resurectionPending.GetFailRealmStates() == resurection.GetFailRealmStates());
        UNIT_ASSERT(resurectionPending.GetWritePolicy() == solo1.GetWritePolicy());
        UNIT_ASSERT(disaster.GetFailRealmStates() == solo1.GetFailRealmStates());
        UNIT_ASSERT(disaster.GetWritePolicy() == solo1.GetWritePolicy());
        UNIT_ASSERT(!disaster.CanTransitionTo(leavingForSolo1));
        UNIT_ASSERT(resurection.CanTransitionTo(leavingForSolo1));
        UNIT_ASSERT(resurection.CanTransitionTo(preparingRecovery1));
        UNIT_ASSERT(!resurection.CanTransitionTo(preparingRecovery2));
        UNIT_ASSERT(!resurection.CanTransitionTo(solo1));
        UNIT_ASSERT(!resurection.CanTransitionTo({T::SOLO_DC_INV_1_PENDING, 0, 1}));
        UNIT_ASSERT(resurection.GetFailRealmStates() == leavingForSolo1.GetFailRealmStates());
        UNIT_ASSERT(resurection.GetWritePolicy() == solo1.GetWritePolicy());
        UNIT_ASSERT(solo1.CanTransitionTo(solo2));
        UNIT_ASSERT(!solo2.CanTransitionTo(solo1));
        UNIT_ASSERT(inv1.CanTransitionTo(leavingForSolo1));
        UNIT_ASSERT(inv2.CanTransitionTo(leavingForSolo2));
        UNIT_ASSERT(leavingForSolo1.CanTransitionTo(solo1));
        UNIT_ASSERT(leavingForSolo2.CanTransitionTo(solo2));
        UNIT_ASSERT(!solo1.CanTransitionTo(leavingForSolo2));
        for (const auto& state : {leavingForSolo1, solo1}) {
            UNIT_ASSERT(state.CanTransitionTo(preparingRecovery1));
            UNIT_ASSERT(!state.CanTransitionTo(preparingRecovery2));
            UNIT_ASSERT(!state.CanTransitionTo(soloRecovery1));
        }
        for (const auto& state : {leavingForSolo2, solo2}) {
            UNIT_ASSERT(state.CanTransitionTo(preparingRecovery2));
            UNIT_ASSERT(!state.CanTransitionTo(preparingRecovery1));
            UNIT_ASSERT(!state.CanTransitionTo(soloRecovery1));
        }
        for (const auto& state : {preparingRecovery1, preparingRecovery2}) {
            UNIT_ASSERT(!state.CanTransitionTo(inv1));
            UNIT_ASSERT(!state.CanTransitionTo({state.Mode, 0, 1}));
            UNIT_ASSERT(state.GetFailRealmStates()[1].State == TInfo::EFailRealmState::Joining);
            UNIT_ASSERT(state.GetWritePolicy().CheckWriteOk({2, 2}));
            UNIT_ASSERT(!state.GetWritePolicy().CheckWriteOk({2}));
            UNIT_ASSERT(state.GetWritePolicy().CheckWriteDurability({2, 2}));
        }
        for (const auto& state : {preparingRecovery1, preparingRecovery2, soloRecovery1}) {
            UNIT_ASSERT(state.GetWritePolicy().CheckWriteDurability({1, 1}));
            UNIT_ASSERT(!state.GetWritePolicy().CheckWriteOk({1, 1}));
        }
        UNIT_ASSERT(preparingRecovery1.CanTransitionTo(soloRecovery1));
        UNIT_ASSERT(preparingRecovery2.CanTransitionTo(soloRecovery1));
        UNIT_ASSERT(soloRecovery1.CanTransitionTo(inv1));
        UNIT_ASSERT(!soloRecovery1.CanTransitionTo(inv2));
        UNIT_ASSERT(!soloRecovery1.CanTransitionTo(solo2));
        for (const auto& state : {leavingForSolo1, leavingForSolo2, solo1, solo2}) {
            UNIT_ASSERT(state.GetWritePolicy().CheckWriteOk({2}));
            UNIT_ASSERT(state.GetWritePolicy().CheckWriteDurability({2}));
            UNIT_ASSERT(!state.GetWritePolicy().CheckWriteOk({1}));
        }
        UNIT_ASSERT(leavingForSolo1.GetFailRealmStates() == leavingForSolo2.GetFailRealmStates());
        UNIT_ASSERT(leavingForSolo1.GetFailRealmStates()[1].State == TInfo::EFailRealmState::Leaving);
        UNIT_ASSERT(solo1.GetFailRealmStates() == solo2.GetFailRealmStates());
        UNIT_ASSERT(!solo2.CanTransitionTo({T::SOLO_DC_INV_2, 0, 1}));
        UNIT_ASSERT(!inv1.CanTransitionTo({T::DC_FAILURE_INV_2, 1, {}}));
        UNIT_ASSERT(!failed.GetWritePolicy().CheckWriteOk({1, 1}));
        UNIT_ASSERT(failed.GetWritePolicy().CheckWriteOk({2, 2}));
        UNIT_ASSERT(!inv1.GetWritePolicy().CheckWriteOk({1, 1}));
        UNIT_ASSERT(inv1.GetWritePolicy().CheckWriteOk({2, 2}));
        UNIT_ASSERT(normal.GetWritePolicy().CheckWriteOk({2, 2}));
        UNIT_ASSERT(!normal.GetWritePolicy().CheckWriteDurability({2, 2}));
        TState recoveryPending{T::DC_RECOVERY_PENDING, 0, {}};
        TState recoveryPendingInv1{T::DC_RECOVERY_PENDING_INV_1, 0, {}};
        TState recovery{T::DC_RECOVERY, 0, {}};
        for (const auto& state : {pending, failed}) {
            UNIT_ASSERT(state.CanTransitionTo(recoveryPending));
            UNIT_ASSERT(!state.CanTransitionTo(recoveryPendingInv1));
            UNIT_ASSERT(!state.CanTransitionTo(recovery));
        }
        for (const auto& state : {pendingInv1, inv1}) {
            UNIT_ASSERT(state.CanTransitionTo(recoveryPendingInv1));
            UNIT_ASSERT(!state.CanTransitionTo(recoveryPending));
            UNIT_ASSERT(!state.CanTransitionTo(recovery));
        }
        for (const auto& state : {pendingInv2, inv2}) {
            UNIT_ASSERT(state.CanTransitionTo(recovery));
            UNIT_ASSERT(!state.CanTransitionTo(recoveryPending));
            UNIT_ASSERT(!state.CanTransitionTo(recoveryPendingInv1));
        }
        for (const auto& state : {recoveryPending, recoveryPendingInv1}) {
            UNIT_ASSERT(state.CanTransitionTo(recovery));
            UNIT_ASSERT(state.CanTransitionTo(state.Mode == T::DC_RECOVERY_PENDING ? pending : pendingInv1));
            UNIT_ASSERT(!state.CanTransitionTo(normal));
            UNIT_ASSERT(state.GetFailRealmStates()[0].State == TInfo::EFailRealmState::Joining);
            UNIT_ASSERT(state.GetWritePolicy().CheckWriteOk({2, 2}));
            UNIT_ASSERT_VALUES_EQUAL(state.GetWritePolicy().CheckWriteOk({1, 1, 1}),
                state.Mode == T::DC_RECOVERY_PENDING_INV_1);
            UNIT_ASSERT(state.GetWritePolicy().CheckWriteDurability({1, 1, 1}));
        }
        UNIT_ASSERT(!recoveryPendingInv1.CanTransitionTo(recoveryPending));
        UNIT_ASSERT(!recoveryPending.CanTransitionTo(recoveryPendingInv1));
        UNIT_ASSERT(recovery.CanTransitionTo(normal));
        UNIT_ASSERT(!recovery.CanTransitionTo(pending));
        UNIT_ASSERT(recovery.CanTransitionTo(recoveryPendingInv1));
        UNIT_ASSERT(!recovery.CanTransitionTo(pendingInv2));
        UNIT_ASSERT(!recovery.CanTransitionTo(inv2));
        UNIT_ASSERT(recovery.CanTransitionTo(preparingRecovery1));
        UNIT_ASSERT(recoveryPendingInv1.CanTransitionTo(preparingRecovery1));
        UNIT_ASSERT(recoveryPending.CanTransitionTo(disaster));
        UNIT_ASSERT(failed.CanTransitionTo(disaster));
        UNIT_ASSERT(preparingRecovery1.CanTransitionTo(leavingForSolo1));
        UNIT_ASSERT(preparingRecovery1.CanTransitionTo(solo1));
        UNIT_ASSERT(preparingRecovery2.CanTransitionTo(leavingForSolo2));
        UNIT_ASSERT(preparingRecovery2.CanTransitionTo(solo2));
        UNIT_ASSERT(!failed.CanTransitionTo(normal));
        UNIT_ASSERT(leavingForSolo1.CanTransitionTo(leavingForSolo2));
        UNIT_ASSERT(!leavingForSolo2.CanTransitionTo(leavingForSolo1));
        UNIT_ASSERT(recoveryPending.GetWritePolicy().WriteDurability == normal.GetWritePolicy().WriteDurability);
        const TInfo::TWritePolicy::TLayouts recoveryDurability = {{1, 1, 1}};
        UNIT_ASSERT(recovery.GetWritePolicy().WriteDurability == recoveryDurability);
        UNIT_ASSERT(!inv2.GetWritePolicy().CheckWriteOk({1, 2}));
        UNIT_ASSERT(inv2.GetWritePolicy().CheckWriteOk({2, 2}));
        UNIT_ASSERT(!inv2.GetWritePolicy().CheckWriteDurability({1, 2}));
        UNIT_ASSERT(solo1.GetWritePolicy().CheckWriteOk({2}));
        UNIT_ASSERT(!solo2.GetWritePolicy().CheckWriteOk({1}));

        const TVector<TState> path = {
            normal, pending, failed, inv1,
            leavingForSolo1, solo1, solo2,
            preparingRecovery2, soloRecovery1, inv1, inv2,
            recovery, normal,
        };
        for (size_t i = 1; i < path.size(); ++i) {
            UNIT_ASSERT_C(path[i - 1].CanTransitionTo(path[i]), i);
        }
        for (int mode = T::NORMAL; mode <= T::SOLO_DC_RESURECTION_PENDING; ++mode) {
            if (!T::EMode_IsValid(mode)) {
                continue;
            }
            TState state;
            state.Mode = static_cast<T::EMode>(mode);
            if (mode != T::NORMAL) {
                state.FailedFailRealmIdx = 0;
            }
            if (state.RequiresSurvivingRealm()) {
                state.SurvivingFailRealmIdx = 2;
            }
            UNIT_ASSERT(TState::Parse(state.ToProto(), topology) == state);
            UNIT_ASSERT(TInfo::TWritePolicy::Parse(state.GetWritePolicy().ToProto(), topology) == state.GetWritePolicy());
            NKikimrBlobStorage::TGroupInfo group;
            group.SetGroupID(0);
            group.SetGroupGeneration(1);
            group.SetErasureSpecies(TBlobStorageGroupType::ErasureMirror3dc);
            for (ui32 r = 0; r < 3; ++r) {
                auto* ring = group.AddRings();
                for (ui32 d = 0; d < 3; ++d) {
                    ring->AddFailDomains()->AddVDiskLocations();
                }
            }
            group.MutableMirror3dcState()->CopyFrom(state.ToProto());
            group.MutableConfirmedWrites()->CopyFrom(state.GetConfirmedWrites().ToProto());
            group.MutableWritePolicy()->CopyFrom(state.GetWritePolicy().ToProto());
            for (const auto& realm : state.GetFailRealmStates()) {
                auto* item = group.MutableFailRealmStates()->AddFailRealm();
                item->SetFailRealmIdx(realm.FailRealmIdx);
                item->SetState(static_cast<NKikimrBlobStorage::TFailRealmStates::EState>(realm.State));
            }
            const auto info = TInfo::Parse(group, nullptr, nullptr);
            UNIT_ASSERT_C(info, mode);
            UNIT_ASSERT(info->Mirror3dcState == state);
            auto wrongConfirmed = group;
            const TInfo::TConfirmedWrites zero{{TVector<ui32>{}}};
            const TInfo::TConfirmedWrites one{{{1}}};
            wrongConfirmed.MutableConfirmedWrites()->CopyFrom(
                (state.GetConfirmedWrites() == zero ? one : zero).ToProto());
            UNIT_ASSERT(!TInfo::Parse(wrongConfirmed, nullptr, nullptr));
            TInfo copy(info, TVDiskID(0, 2, 0, 0, 0), TActorId());
            UNIT_ASSERT(copy.Mirror3dcState == state);
            UNIT_ASSERT(copy.ConfirmedWrites == info->ConfirmedWrites);
            UNIT_ASSERT(!copy.HasStoragePolicyChanged(*info));
            auto missingConfirmed = group;
            missingConfirmed.ClearConfirmedWrites();
            UNIT_ASSERT(!TInfo::Parse(missingConfirmed, nullptr, nullptr));
            group.ClearWritePolicy();
            UNIT_ASSERT(!TInfo::Parse(group, nullptr, nullptr));
        }
        UNIT_ASSERT(!TInfo::TWritePolicy::Parse(normal.GetWritePolicy().ToProto(), empty));
    }

    Y_UNIT_TEST(ConfirmedWrites) {
        using TInfo = TBlobStorageGroupInfo;
        using TConfirmed = TInfo::TConfirmedWrites;
        using TState = TInfo::TMirror3dcState;
        using T = NKikimrBlobStorage::TMirror3dcGroupState;
        const TInfo::TTopology topology(TBlobStorageGroupType::ErasureMirror3dc, 3, 3, 1, true);
        const auto minimize = [](TInfo::TWritePolicy::TLayouts layouts) { return TConfirmed::Minimize(std::move(layouts)); };
        const auto normal = minimize({{1, 1, 1}, {2, 2}});
        UNIT_ASSERT(minimize({{2, 2}, {1, 1, 1}, {2, 2, 2}, {2, 2}}) == normal);
        UNIT_ASSERT(minimize({{2}, {1, 1, 1}, {2, 2, 2}}) == minimize({{2}, {1, 1, 1}}));
        UNIT_ASSERT(minimize({{2, 2, 2}, {2, 2, 2}}) == minimize({{2, 2, 2}}));
        UNIT_ASSERT(minimize({{1, 2}, {2, 1}, {2, 2}}) == minimize({{2, 1}}));
        UNIT_ASSERT(TConfirmed::Parse(normal.ToProto(), topology) == normal);
        UNIT_ASSERT(!TConfirmed::Parse({}, topology));
        UNIT_ASSERT(!TConfirmed::Parse(minimize({{4}}).ToProto(), topology));
        UNIT_ASSERT(!TConfirmed::Parse(minimize({{1, 1, 1, 1}}).ToProto(), topology));
        UNIT_ASSERT(!TConfirmed::Parse(minimize({{0}}).ToProto(), topology));
        const TInfo::TTopology empty(TBlobStorageGroupType::ErasureMirror3dc, 0, 0, 0);
        UNIT_ASSERT(!TConfirmed::Parse(normal.ToProto(), empty));
        UNIT_ASSERT(normal.Covers({{2, 2, 2}, {1, 1, 1}}));
        UNIT_ASSERT(!normal.Covers({{2}}));

        const auto failure = minimize({{1, 1}});
        const auto inv2 = minimize({{2, 2}});
        const auto solo = minimize({{2}});
        const auto one = minimize({{1}});
        const auto zero = minimize({TVector<ui32>{}});
        UNIT_ASSERT(zero.Layouts.size() == 1 && zero.Layouts[0].empty());
        UNIT_ASSERT(TConfirmed::Parse(zero.ToProto(), topology) == zero);
        UNIT_ASSERT(minimize({{}, {1}, {2, 2}}) == zero);
        UNIT_ASSERT(zero.Covers({{}, {1}, {2, 2}}));
        UNIT_ASSERT(!one.Covers({{}}));
        UNIT_ASSERT(!TInfo::TWritePolicy::Parse(TInfo::TWritePolicy{{{}}, {{2}}}.ToProto(), topology));
        auto check = [&](T::EMode mode, const TConfirmed& expected) {
            TState state;
            state.Mode = mode;
            UNIT_ASSERT_C(state.GetConfirmedWrites() == expected, T::EMode_Name(mode));
            UNIT_ASSERT_C(expected.Covers(state.GetWritePolicy().WriteOk), T::EMode_Name(mode));
        };
        check(T::NORMAL, normal);
        check(T::DC_RECOVERY, normal);
        const auto projected = minimize({{1, 1}, {2}});
        for (auto mode : {T::DC_FAILURE_PENDING, T::DC_FAILURE, T::DC_RECOVERY_PENDING}) {
            check(mode, projected);
        }
        for (auto mode : {T::DC_FAILURE_PENDING_INV_1, T::DC_FAILURE_INV_1,
                T::DC_RECOVERY_PENDING_INV_1, T::SOLO_DC_RECOVERY_INV_1}) {
            check(mode, failure);
        }
        for (auto mode : {T::DC_FAILURE_PENDING_INV_2, T::DC_FAILURE_INV_2}) {
            check(mode, inv2);
        }
        for (auto mode : {T::SOLO_DC_INV_1_PENDING, T::SOLO_DC_INV_1,
                T::SOLO_DC_RECOVERY_PENDING_INV_1, T::SOLO_DC_RESURECTION}) {
            check(mode, one);
        }
        for (auto mode : {T::SOLO_DC_INV_2_PENDING, T::SOLO_DC_INV_2, T::SOLO_DC_RECOVERY_PENDING_INV_2}) {
            check(mode, solo);
        }
        check(T::SOLO_DC_DISASTER, zero);
        check(T::SOLO_DC_RESURECTION_PENDING, zero);
    }

    Y_UNIT_TEST(Mirror3dcWriteParticipation) {
        using T = NKikimrBlobStorage::TMirror3dcGroupState;
        using TState = TBlobStorageGroupInfo::TMirror3dcState;
        // Check all realm identities; counts remain indexed, rather than sorted across roles.
        for (ui32 c = 0; c < 3; ++c) {
            const ui32 a = (c + 1) % 3;
            const ui32 b = (c + 2) % 3;
            auto counts = [&](ui32 inA, ui32 inB, ui32 inC) {
                std::array<ui32, 3> result{};
                result[a] = inA;
                result[b] = inB;
                result[c] = inC;
                return result;
            };
            for (auto mode : {T::DC_RECOVERY_PENDING_INV_1, T::DC_RECOVERY}) {
                TState state{mode, c, {}};
                UNIT_ASSERT_VALUES_EQUAL(state.GetWriteOkLayouts().size(), 2);
                UNIT_ASSERT(state.CheckWriteOk(counts(2, 2, 0)));
                UNIT_ASSERT(state.CheckWriteOk(counts(1, 1, 1)));
                UNIT_ASSERT(!state.CheckWriteOk(counts(2, 0, 2)));
                UNIT_ASSERT(!state.CheckWriteOk(counts(0, 2, 2)));
                UNIT_ASSERT(!state.CheckWriteOk(counts(3, 0, 3)));
                UNIT_ASSERT(state.CheckWriteDurability(counts(1, 1, 1)));
                UNIT_ASSERT(!state.CheckWriteDurability(counts(2, 2, 0)));
            }
            TState failurePending{T::DC_FAILURE_PENDING, c, {}};
            UNIT_ASSERT(failurePending.CheckWriteOk(counts(2, 2, 0)));
            UNIT_ASSERT(!failurePending.CheckWriteOk(counts(2, 0, 3)));
            UNIT_ASSERT(!failurePending.CheckWriteDurability(counts(1, 0, 3)));
            UNIT_ASSERT(failurePending.CheckWriteDurability(counts(1, 1, 0)));
            TState recoveryPending{T::DC_RECOVERY_PENDING, c, {}};
            UNIT_ASSERT(recoveryPending.CheckWriteOk(counts(2, 2, 0)));
            UNIT_ASSERT(!recoveryPending.CheckWriteOk(counts(1, 2, 2)));
            UNIT_ASSERT(!recoveryPending.CheckWriteOk(counts(2, 1, 2)));
            for (auto mode : {T::SOLO_DC_DISASTER, T::SOLO_DC_RESURECTION_PENDING,
                    T::SOLO_DC_RESURECTION, T::SOLO_DC_INV_1_PENDING, T::SOLO_DC_INV_1}) {
                TState state{mode, c, a};
                UNIT_ASSERT_VALUES_EQUAL(state.GetWriteOkLayouts().size(), 1);
                UNIT_ASSERT(state.CheckWriteOk(counts(2, 0, 0)));
                UNIT_ASSERT(!state.CheckWriteOk(counts(1, 3, 3)));
                UNIT_ASSERT(!state.CheckWriteDurability(counts(1, 3, 3)));
                UNIT_ASSERT(state.CheckWriteDurability(counts(2, 0, 0)));
            }
            TState soloRecovery{T::SOLO_DC_RECOVERY_PENDING_INV_1, c, a};
            UNIT_ASSERT(soloRecovery.CheckWriteOk(counts(2, 2, 0)));
            UNIT_ASSERT(!soloRecovery.CheckWriteOk(counts(0, 2, 3)));
            UNIT_ASSERT(!soloRecovery.CheckWriteOk(counts(2, 0, 3)));
            UNIT_ASSERT(soloRecovery.CheckWriteDurability(counts(1, 1, 0)));
        }
    }

    Y_UNIT_TEST(FailRealmStates) {
        using TInfo = TBlobStorageGroupInfo;
        using TProto = NKikimrBlobStorage::TFailRealmStates;
        NKikimrBlobStorage::TGroupInfo group;
        group.SetGroupID(0);
        group.SetGroupGeneration(1);
        group.SetErasureSpecies(TBlobStorageGroupType::ErasureMirror3dc);
        for (ui32 realm = 0; realm < 3; ++realm) {
            auto* ring = group.AddRings();
            for (ui32 domain = 0; domain < 3; ++domain) {
                ring->AddFailDomains()->AddVDiskLocations();
            }
        }
        const auto original = TInfo::Parse(group, nullptr, nullptr);
        UNIT_ASSERT(original);
        auto add = [&](ui32 realm, TProto::EState state) {
            auto* item = group.MutableFailRealmStates()->AddFailRealm();
            item->SetFailRealmIdx(realm);
            item->SetState(state);
        };
        add(1, TProto::Joining);
        add(0, TProto::Excluded);
        add(1, TProto::Joining);
        add(2, TProto::Included);
        const auto info = TInfo::Parse(group, nullptr, nullptr);
        UNIT_ASSERT(info);
        UNIT_ASSERT_VALUES_EQUAL(info->FailRealmStates.size(), 2);
        UNIT_ASSERT(info->GetFailRealmState(0) == TInfo::EFailRealmState::Excluded);
        UNIT_ASSERT(info->GetFailRealmState(1) == TInfo::EFailRealmState::Joining);
        UNIT_ASSERT(info->GetFailRealmState(2) == TInfo::EFailRealmState::Included);
        UNIT_ASSERT(info->HasStoragePolicyChanged(*original));
        TInfo copy(info, TVDiskID(0, 2, 0, 0, 0), TActorId());
        UNIT_ASSERT(copy.FailRealmStates == info->FailRealmStates);
        UNIT_ASSERT(!copy.HasStoragePolicyChanged(*info));
        auto* policy = group.MutableWritePolicy();
        policy->AddWriteOk()->AddPartsPerFailRealm(2);
        policy->AddWriteDurability()->AddPartsPerFailRealm(2);
        group.SetBridgeProxyGroupId(0);
        group.MutableBridgeGroupState()->AddPile()->SetGroupId(0);
        const auto inlineInfo = TInfo::Parse(group, nullptr, nullptr);
        UNIT_ASSERT(inlineInfo);
        const auto view = inlineInfo->MakeInlinePileView();
        UNIT_ASSERT(view->FailRealmStates == inlineInfo->FailRealmStates);
        UNIT_ASSERT(view->WritePolicy == inlineInfo->WritePolicy);
        add(1, TProto::Excluded);
        UNIT_ASSERT(!TInfo::Parse(group, nullptr, nullptr));
        group.ClearFailRealmStates();
        add(3, TProto::Excluded);
        UNIT_ASSERT(!TInfo::Parse(group, nullptr, nullptr));
        group.ClearFailRealmStates();
        group.MutableFailRealmStates()->AddFailRealm()->SetFailRealmIdx(0);
        UNIT_ASSERT(!TInfo::Parse(group, nullptr, nullptr));
        group.ClearFailRealmStates();
        group.MutableFailRealmStates()->AddFailRealm()->SetState(TProto::Included);
        UNIT_ASSERT(!TInfo::Parse(group, nullptr, nullptr));
        group.ClearFailRealmStates();
        auto* invalid = group.MutableFailRealmStates()->AddFailRealm();
        invalid->SetFailRealmIdx(0);
        invalid->GetReflection()->MutableUnknownFields(invalid)->AddVarint(2, 99);
        UNIT_ASSERT(!TInfo::Parse(group, nullptr, nullptr));
        group.ClearFailRealmStates();
        add(2, TProto::Leaving);
        UNIT_ASSERT(TInfo::Parse(group, nullptr, nullptr));
        group.ClearFailRealmStates();
        UNIT_ASSERT(TInfo::Parse(group, nullptr, nullptr)->FailRealmStates.empty());
    }

    Y_UNIT_TEST(WritePolicyLayouts) {
        const TBlobStorageGroupInfo::TTopology topology(TBlobStorageGroupType::ErasureMirror3dc, 3, 3, 1, true);
        struct TCase {
            TVector<ui32> Layout;
            TVector<ui32> Success;
            TVector<ui32> Failure;
        };
        for (const auto& item : TVector<TCase>{
                {{2}, {0, 2, 0}, {1, 0, 0}},
                {{2, 2}, {2, 0, 2}, {2, 1, 1}},
                {{1, 2}, {1, 0, 2}, {3, 0, 0}},
                {{1, 1}, {1, 0, 1}, {2, 0, 0}},
                {{1, 1, 1}, {1, 1, 1}, {3, 3, 0}},
                {{2, 2, 2}, {2, 3, 2}, {3, 1, 3}}}) {
            NKikimrBlobStorage::TGroupWritePolicy proto;
            auto* layout = proto.AddWriteOk();
            for (ui32 count : item.Layout) {
                layout->AddPartsPerFailRealm(count);
            }
            proto.AddWriteDurability()->CopyFrom(*layout);
            const auto policy = TBlobStorageGroupInfo::TWritePolicy::Parse(proto, topology);
            UNIT_ASSERT(policy);
            UNIT_ASSERT(policy->CheckWriteOk(item.Success));
            UNIT_ASSERT(policy->CheckWriteDurability(item.Success));
            UNIT_ASSERT(!policy->CheckWriteOk(item.Failure));
            UNIT_ASSERT(!policy->CheckWriteDurability(item.Failure));
            UNIT_ASSERT(TBlobStorageGroupInfo::TWritePolicy::Parse(policy->ToProto(), topology) == policy);
        }
    }

    Y_UNIT_TEST(WritePolicyAlternativesAndValidation) {
        const TBlobStorageGroupInfo::TTopology topology(TBlobStorageGroupType::ErasureMirror3dc, 3, 3, 1, true);
        NKikimrBlobStorage::TGroupWritePolicy proto;
        auto* layout = proto.AddWriteOk();
        layout->AddPartsPerFailRealm(2);
        layout->AddPartsPerFailRealm(2);
        layout = proto.AddWriteOk();
        for (ui32 i = 0; i < 3; ++i) {
            layout->AddPartsPerFailRealm(1);
        }
        layout = proto.AddWriteDurability();
        for (ui32 i = 0; i < 3; ++i) {
            layout->AddPartsPerFailRealm(2);
        }
        const auto policy = TBlobStorageGroupInfo::TWritePolicy::Parse(proto, topology);
        UNIT_ASSERT(policy);
        UNIT_ASSERT(policy->CheckWriteOk({2, 2, 0}));
        UNIT_ASSERT(policy->CheckWriteOk({1, 1, 1}));
        UNIT_ASSERT(!policy->CheckWriteOk({2, 1, 0}));
        UNIT_ASSERT(!policy->CheckWriteDurability({2, 2, 0}));
        UNIT_ASSERT(policy->CheckWriteDurability({2, 2, 2}));
        UNIT_ASSERT(!policy->CheckWriteOk({}));
        proto.AddWriteOk()->CopyFrom(proto.GetWriteOk(0));
        UNIT_ASSERT(TBlobStorageGroupInfo::TWritePolicy::Parse(proto, topology) == policy);
        proto.ClearWriteOk();
        UNIT_ASSERT(!TBlobStorageGroupInfo::TWritePolicy::Parse(proto, topology));
        for (const auto& invalid : TVector<TVector<ui32>>{{}, {0}, {4}, {1, 1, 1, 1}}) {
            auto malformed = policy->ToProto();
            auto* bad = malformed.MutableWriteOk(0);
            bad->ClearPartsPerFailRealm();
            for (ui32 count : invalid) {
                bad->AddPartsPerFailRealm(count);
            }
            UNIT_ASSERT(!TBlobStorageGroupInfo::TWritePolicy::Parse(malformed, topology));
        }
        auto malformed = policy->ToProto();
        malformed.ClearWriteDurability();
        UNIT_ASSERT(!TBlobStorageGroupInfo::TWritePolicy::Parse(malformed, topology));
    }

    Y_UNIT_TEST(WritePolicyBlock42) {
        const TBlobStorageGroupInfo::TTopology topology(TBlobStorageGroupType::Erasure4Plus2Block, 1, 8, 1, true);
        NKikimrBlobStorage::TGroupWritePolicy proto;
        proto.AddWriteOk()->AddPartsPerFailRealm(4);
        proto.AddWriteDurability()->AddPartsPerFailRealm(6);
        const auto policy = TBlobStorageGroupInfo::TWritePolicy::Parse(proto, topology);
        UNIT_ASSERT(policy);
        UNIT_ASSERT(policy->CheckWriteOk({4}));
        UNIT_ASSERT(!policy->CheckWriteOk({3}));
        UNIT_ASSERT(policy->CheckWriteDurability({6}));
        UNIT_ASSERT(!policy->CheckWriteDurability({5}));
        for (ui32 count : {3u, 9u}) {
            auto invalid = proto;
            invalid.MutableWriteOk(0)->SetPartsPerFailRealm(0, count);
            UNIT_ASSERT(!TBlobStorageGroupInfo::TWritePolicy::Parse(invalid, topology));
        }
        auto invalid = proto;
        invalid.MutableWriteOk(0)->AddPartsPerFailRealm(4);
        UNIT_ASSERT(!TBlobStorageGroupInfo::TWritePolicy::Parse(invalid, topology));
        NKikimrBlobStorage::TFailRealmStates excluded;
        auto* realm = excluded.AddFailRealm();
        realm->SetFailRealmIdx(0);
        realm->SetState(NKikimrBlobStorage::TFailRealmStates::Excluded);
        UNIT_ASSERT(TBlobStorageGroupInfo::ParseFailRealmStates(excluded, topology));
        realm->SetFailRealmIdx(1);
        UNIT_ASSERT(!TBlobStorageGroupInfo::ParseFailRealmStates(excluded, topology));
    }

    Y_UNIT_TEST(TestBelongsToSubgroup) {
        for (ui32 disks = 1; disks < 4; ++disks) {
            for (auto [erasureType, _] : TErasureType::ErasureNames) {
                if (erasureType == TBlobStorageGroupType::ErasureMirror3dc) {
                    continue;
                }

                const ui32 numFailDomains = TBlobStorageGroupType(erasureType).BlobSubgroupSize();
                TBlobStorageGroupInfo info(erasureType, disks, numFailDomains);

                for (ui32 hashIdx = 0; hashIdx < 1000; ++hashIdx) {
                    ui32 hash = 640480 + 13 * hashIdx;
                    TBlobStorageGroupInfo::TVDiskIds vdiskIds;
                    info.PickSubgroup(hash, &vdiskIds, nullptr);
                    TVector<bool> isMissingDomain;
                    auto realm0 = info.FailRealmsBegin();
                    isMissingDomain.resize(realm0.GetNumFailDomainsPerFailRealm());
                    for (const auto& domain : realm0.GetFailRealmFailDomains()) {
                        isMissingDomain[domain.FailDomainOrderNumber] = true;
                    }
                    for (ui32 i = 0; i < vdiskIds.size(); ++i) {
                        isMissingDomain[vdiskIds[i].FailDomain] = false;
                    }
                    for (auto it = realm0.FailRealmFailDomainsBegin(); it != realm0.FailRealmFailDomainsEnd(); ++it) {
                        if (isMissingDomain[it.GetFailDomainIdx()]) {
                            ui32 vDisksSz = it.GetNumVDisksPerFailDomain();
                            for (ui32 vdiskIdx = 0; vdiskIdx < vDisksSz; ++vdiskIdx) {
                                TVDiskID id(0, 1, 0, it.GetFailDomainIdx(), vdiskIdx);
                                bool isReplica = info.BelongsToSubgroup(id, hash);
                                UNIT_ASSERT(!isReplica);
                            }
                        }
                    }

                    for (ui32 i = 0; i < vdiskIds.size(); ++i) {
                        ui32 realmIdx = vdiskIds[i].FailRealm;
                        ui32 domainIdx = vdiskIds[i].FailDomain;
                        for (auto vdiskIt = realm0.FailRealmVDisksBegin(); vdiskIt != realm0.FailRealmVDisksEnd(); ++vdiskIt) {
                            if (vdiskIt.GetFailDomainIdx() != domainIdx) {
                                continue;
                            }
                            ui32 vdiskIdx = vdiskIt.GetVDiskIdx();
                            TVDiskID id(0, 1, realmIdx, domainIdx, vdiskIdx);
                            bool isReplica = info.BelongsToSubgroup(id, hash);
                            UNIT_ASSERT(isReplica == (vdiskIdx == vdiskIds[i].VDisk));
                        }
                    }
                }
            }
        }
    }

    Y_UNIT_TEST(SubgroupPartLayout) {
        TLogoBlobID id(1, 1, 1, 0, 100, 0);

        for (auto [erasureType, _] : TErasureType::ErasureNames) {
            if (erasureType == TBlobStorageGroupType::ErasureMirror3dc || erasureType == TBlobStorageGroupType::ErasureMirror3of4) {
                continue;
            }

            const ui32 numFailDomains = TBlobStorageGroupType(erasureType).BlobSubgroupSize();
            TBlobStorageGroupInfo info(erasureType, 1, numFailDomains);

            TBlobStorageGroupInfo::TVDiskIds ids;
            info.PickSubgroup(id.Hash(), &ids, nullptr);

            const ui32 totalPartCount = info.Type.TotalPartCount();
            const ui32 handoff = info.Type.Handoff();
            const ui32 numHandoff1 = handoff >= 1 ? totalPartCount : 0;
            const ui32 numHandoff2 = handoff >= 2 ? totalPartCount : 0;

            for (ui32 main = 0; main < (1U << totalPartCount); ++main) {
                for (ui32 handoff1 = 0; handoff1 < (1U << numHandoff1); ++handoff1) {
                    for (ui32 handoff2 = 0; handoff2 < (1U << numHandoff2); ++handoff2) {
                        TIngress ingress;
                        for (ui32 i = 0; i < totalPartCount; ++i) {
                            if (main & (1 << i)) {
                                auto iOpt = TIngress::CreateIngressWithLocal(&info.GetTopology(),
                                                                             ids[i],
                                                                             TLogoBlobID(id, i + 1));
                                ingress.Merge(*iOpt);
                            }
                            if (handoff1 & (1 << i)) {
                                auto iOpt = TIngress::CreateIngressWithLocal(&info.GetTopology(),
                                                                             ids[totalPartCount],
                                                                             TLogoBlobID(id, i + 1));
                                ingress.Merge(*iOpt);
                            }
                            if (handoff2 & (1 << i)) {
                                auto iOpt = TIngress::CreateIngressWithLocal(&info.GetTopology(),
                                                                             ids[totalPartCount + 1],
                                                                             TLogoBlobID(id, i + 1));
                                ingress.Merge(*iOpt);
                            }
                        }
                        TSubgroupPartLayout layout;
                        for (ui32 i = 0; i < totalPartCount; ++i) {
                            if (main & (1 << i)) {
                                layout.AddItem(i, i, info.Type);
                            }
                            if (handoff1 & (1 << i)) {
                                layout.AddItem(totalPartCount, i, info.Type);
                            }
                            if (handoff2 & (1 << i)) {
                                layout.AddItem(totalPartCount + 1, i, info.Type);
                            }
                        }

                        UNIT_ASSERT_EQUAL(layout, TSubgroupPartLayout::CreateFromIngress(ingress, info.Type));

//                        Cerr << "main# " << main << " hanoff1# " << handoff1 << " handoff2# " << handoff2 << " num# " << num << " num2# " << num2 << Endl;
                    }
                }
            }
        }
    }

    Y_UNIT_TEST(GroupQuorumCheckerOrdinary) {
        for (auto [erasure, _] : TErasureType::ErasureNames) {
            if (erasure == TBlobStorageGroupType::ErasureMirror3dc || erasure == TBlobStorageGroupType::ErasureMirror3of4) {
                // separate test for mirror-3-dc
                continue;
            }

            const ui32 numFailDomains = 10;
            const ui32 numVDisks = 3;
            TBlobStorageGroupInfo info(erasure, numVDisks, numFailDomains);

            // calculate number of handoff disks
            const ui32 numHandoff = info.Type.Handoff();
            const ui32 numMasks = 1U << numFailDomains;

            // calculate sets of disks
            TVector<ui32> goodMask, badMask;
            for (ui32 mask = 0; mask < numMasks; ++mask) {
                (static_cast<ui32>(std::popcount(mask)) <= numHandoff ? goodMask : badMask).push_back(mask);
            }

            auto createGroupVDisks = [&](ui32 domainMask, ui32 vdiskMask) {
                TBlobStorageGroupInfo::TGroupVDisks vdisks(&info.GetTopology());
                for (const auto& vdisk : info.GetVDisks()) {
                    if (((domainMask >> vdisk.FailDomainOrderNumber) & 1) && ((vdiskMask >> vdisk.VDiskIdShort.VDisk) & 1)) {
                        auto vd = info.GetVDiskId(vdisk.OrderNumber);
                        vdisks += TBlobStorageGroupInfo::TGroupVDisks(&info.GetTopology(), vd);
                    }
                }
                return vdisks;
            };

            const auto& checker = info.GetQuorumChecker();
            for (ui32 vdiskMask = 1; vdiskMask < (1U << numVDisks); ++vdiskMask) {
                for (ui32 domainMask : goodMask) {
                    UNIT_ASSERT(checker.CheckQuorumForGroup(~createGroupVDisks(domainMask, vdiskMask)));
                }
                for (ui32 domainMask : badMask) {
                    UNIT_ASSERT(!checker.CheckQuorumForGroup(~createGroupVDisks(domainMask, vdiskMask)));
                }
            }
        }
    }

    Y_UNIT_TEST(GroupQuorumCheckerMirror3dc) {
        const ui32 numFailRealms = 3;
        const ui32 numFailDomains = 4;
        TBlobStorageGroupInfo info(TBlobStorageGroupType::ErasureMirror3dc, 1, numFailDomains, numFailRealms);

        auto domainMask = [&](ui32 realm, ui32 domain) {
            return 1U << (realm * numFailDomains + domain);
        };

        TVector<ui32> goodMask;

        goodMask.push_back(0);

        for (ui32 realm = 0; realm < numFailRealms; ++realm) {
            for (ui32 mask = 1; mask < (1U << numFailDomains); ++mask) {
                // first, create failed domains mask for a single fail realm, iterating through all possible options
                // for failed fail domains
                ui32 domMask = 0;
                for (ui32 i = 0; i < numFailDomains; ++i) {
                    if ((mask >> i) & 1) {
                        domMask |= domainMask(realm, i);
                    }
                }

                // push that mask to good ones
                goodMask.push_back(domMask);

                // now iterate through all other realms (not including current one) and add one more failed fail domain
                // to the common mask
                for (ui32 i = 0; i < numFailRealms; ++i) {
                    if (i != realm) {
                        for (ui32 j = 0; j < numFailDomains; ++j) {
                            goodMask.push_back(domMask | domainMask(i, j));
                        }
                    }
                }
            }
        }

        // sort options
        std::sort(goodMask.begin(), goodMask.end());

        // create bad mask
        TVector<ui32> allMasks;
        for (ui32 i = 0; i < (1U << (numFailRealms * numFailDomains)); ++i) {
            allMasks.push_back(i);
        }

        TVector<ui32> badMask;
        std::set_difference(allMasks.begin(), allMasks.end(), goodMask.begin(), goodMask.end(), std::back_inserter(badMask));

        auto createGroupVDisks = [&](ui32 domainMask) {
            TBlobStorageGroupInfo::TGroupVDisks vdisks(&info.GetTopology());
            for (const auto& vdisk : info.GetVDisks()) {
                if ((domainMask >> vdisk.FailDomainOrderNumber) & 1) {
                    auto vd = info.GetVDiskId(vdisk.OrderNumber);
                    vdisks += TBlobStorageGroupInfo::TGroupVDisks(&info.GetTopology(), vd);
                }
            }
            return vdisks;
        };

        const auto& checker = info.GetQuorumChecker();
        for (ui32 domainMask : goodMask) {
            UNIT_ASSERT(checker.CheckQuorumForGroup(~createGroupVDisks(domainMask)));
        }
        for (ui32 domainMask : badMask) {
            UNIT_ASSERT(!checker.CheckQuorumForGroup(~createGroupVDisks(domainMask)));
        }
    }
}

} // namespace NKikimr

