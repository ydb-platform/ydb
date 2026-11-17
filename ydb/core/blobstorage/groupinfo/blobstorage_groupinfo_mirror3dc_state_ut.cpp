#include "blobstorage_groupinfo_mirror3dc_state.h"

#include <ydb/core/protos/blobstorage_base.pb.h>
#include <library/cpp/testing/unittest/registar.h>
#include <algorithm>

namespace NKikimr {
namespace {
using TFailRealm = NKikimrBlobStorage::TDataCenterState;
using ELayout = EMirror3dcLayout;
struct TStateCase {
    EMirror3dcGroupState State;
    TMirror3dcFailRealmStates Realms;
    ELayout Write;
    ELayout Durability;
    ELayout Expected;
};
// Literal expectations from the agreed 17-state graph.
constexpr TStateCase Cases[] = {
    {NKikimrBlobStorage::TGroupState::NORMAL, {TFailRealm::INCLUDED, TFailRealm::INCLUDED, TFailRealm::INCLUDED}, ELayout::LAYOUT_111_22, ELayout::LAYOUT_111, ELayout::LAYOUT_111_22},
    {NKikimrBlobStorage::TGroupState::DC_RECOVERY, {TFailRealm::INCLUDED, TFailRealm::INCLUDED, TFailRealm::RECOVERING}, ELayout::LAYOUT_111_22, ELayout::LAYOUT_111, ELayout::LAYOUT_11_2},
    {NKikimrBlobStorage::TGroupState::DC_RECOVERY_INV1, {TFailRealm::INCLUDED, TFailRealm::INCLUDED, TFailRealm::RECOVERING}, ELayout::LAYOUT_111_22, ELayout::LAYOUT_111, ELayout::LAYOUT_11},
    {NKikimrBlobStorage::TGroupState::DC_FAILURE, {TFailRealm::INCLUDED, TFailRealm::INCLUDED, TFailRealm::EXCLUDED}, ELayout::LAYOUT_22, ELayout::LAYOUT_0, ELayout::LAYOUT_11_2},
    {NKikimrBlobStorage::TGroupState::DC_FAILURE_INV1, {TFailRealm::INCLUDED, TFailRealm::INCLUDED, TFailRealm::EXCLUDED}, ELayout::LAYOUT_22, ELayout::LAYOUT_11, ELayout::LAYOUT_11},
    {NKikimrBlobStorage::TGroupState::DC_FAILURE_INV2, {TFailRealm::INCLUDED, TFailRealm::INCLUDED, TFailRealm::EXCLUDED}, ELayout::LAYOUT_22, ELayout::LAYOUT_22, ELayout::LAYOUT_22},
    {NKikimrBlobStorage::TGroupState::DC_SOLO_DISASTER, {TFailRealm::INCLUDED, TFailRealm::EXCLUDED, TFailRealm::EXCLUDED}, ELayout::LAYOUT_2, ELayout::LAYOUT_0, ELayout::LAYOUT_0},
    {NKikimrBlobStorage::TGroupState::DC_SOLO_INV1, {TFailRealm::INCLUDED, TFailRealm::EXCLUDED, TFailRealm::EXCLUDED}, ELayout::LAYOUT_2, ELayout::LAYOUT_1, ELayout::LAYOUT_1},
    {NKikimrBlobStorage::TGroupState::DC_SOLO_INV2, {TFailRealm::INCLUDED, TFailRealm::EXCLUDED, TFailRealm::EXCLUDED}, ELayout::LAYOUT_2, ELayout::LAYOUT_2, ELayout::LAYOUT_2},
    {NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY, {TFailRealm::INCLUDED, TFailRealm::RECOVERING, TFailRealm::EXCLUDED}, ELayout::LAYOUT_2, ELayout::LAYOUT_1, ELayout::LAYOUT_0},
    {NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY_INV1, {TFailRealm::INCLUDED, TFailRealm::RECOVERING, TFailRealm::EXCLUDED}, ELayout::LAYOUT_22, ELayout::LAYOUT_11, ELayout::LAYOUT_1},
    {NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY_INV2, {TFailRealm::INCLUDED, TFailRealm::RECOVERING, TFailRealm::EXCLUDED}, ELayout::LAYOUT_22, ELayout::LAYOUT_11, ELayout::LAYOUT_2},
    {NKikimrBlobStorage::TGroupState::DC_FAILURE_INV1_PENDING, {TFailRealm::INCLUDED, TFailRealm::INCLUDED, TFailRealm::EXCLUDED}, ELayout::LAYOUT_22, ELayout::LAYOUT_11, ELayout::LAYOUT_11_2},
    {NKikimrBlobStorage::TGroupState::DC_FAILURE_INV2_PENDING, {TFailRealm::INCLUDED, TFailRealm::INCLUDED, TFailRealm::EXCLUDED}, ELayout::LAYOUT_22, ELayout::LAYOUT_22, ELayout::LAYOUT_11},
    {NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY_INV1_PENDING, {TFailRealm::INCLUDED, TFailRealm::RECOVERING, TFailRealm::EXCLUDED}, ELayout::LAYOUT_2, ELayout::LAYOUT_1, ELayout::LAYOUT_1},
    {NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY_INV2_PENDING, {TFailRealm::INCLUDED, TFailRealm::RECOVERING, TFailRealm::EXCLUDED}, ELayout::LAYOUT_2, ELayout::LAYOUT_11, ELayout::LAYOUT_2},
    {NKikimrBlobStorage::TGroupState::DC_SOLO_INV2_PENDING, {TFailRealm::INCLUDED, TFailRealm::EXCLUDED, TFailRealm::EXCLUDED}, ELayout::LAYOUT_2, ELayout::LAYOUT_2, ELayout::LAYOUT_1},
};

static_assert([] {
    for (int value = NKikimrBlobStorage::TGroupState::EState_MIN; value <= NKikimrBlobStorage::TGroupState::EState_MAX; ++value) {
        const auto roles = GetMirror3dcRealmStates(static_cast<EMirror3dcGroupState>(value));
        if (!roles || !std::is_sorted(roles->begin(), roles->end())) {
            return false;
        }
    }
    return true;
}(), "Canonical realm roles must be sorted for every group state");
}

Y_UNIT_TEST_SUITE(TGroupStateTest) {
    Y_UNIT_TEST(PoliciesAndPhysicalPermutations) {
        UNIT_ASSERT_VALUES_EQUAL(std::size(Cases), 17);
        for (const auto& test : Cases) {
            const auto policy = GetMirror3dcGroupLayouts(test.State);
            UNIT_ASSERT(policy);
            UNIT_ASSERT(policy->Write == test.Write);
            UNIT_ASSERT(policy->Durability == test.Durability);
            UNIT_ASSERT(policy->Ensured == test.Expected);
            auto realms = test.Realms;
            std::sort(realms.begin(), realms.end());
            do {
                const auto config = MakeMirror3dcGroupConfiguration(test.State, realms);
                UNIT_ASSERT(config);
                UNIT_ASSERT(config->Realms == realms);
                UNIT_ASSERT(IsValidMirror3dcGroupConfiguration(*config));
            } while (std::next_permutation(realms.begin(), realms.end()));
        }
        UNIT_ASSERT(!GetMirror3dcGroupLayouts(static_cast<EMirror3dcGroupState>(-1)));
        for (int sentinel : {std::numeric_limits<int>::min(), std::numeric_limits<int>::max()}) {
            EMirror3dcGroupState state = static_cast<EMirror3dcGroupState>(sentinel);
            UNIT_ASSERT(!GetMirror3dcGroupLayouts(state));
            UNIT_ASSERT(!GetMirror3dcRealmStates(state));
        }
        UNIT_ASSERT(!MakeMirror3dcGroupConfiguration(static_cast<EMirror3dcGroupState>(17), {TFailRealm::INCLUDED, TFailRealm::INCLUDED, TFailRealm::INCLUDED}));
        UNIT_ASSERT(!MakeMirror3dcGroupConfiguration(NKikimrBlobStorage::TGroupState::NORMAL, {TFailRealm::INCLUDED, TFailRealm::RECOVERING, TFailRealm::EXCLUDED}));
    }

    Y_UNIT_TEST(PersistedProtobufStatesRoundTrip) {
        NKikimrBlobStorage::TFailRealmStates empty;
        UNIT_ASSERT(!empty.HasGroupState()); // Older records still take the migration path.
        for (const auto& test : Cases) {
            NKikimrBlobStorage::TFailRealmStates record;
            record.SetGroupState(test.State);
            for (const auto realm : test.Realms) {
                record.AddState(realm);
            }
            NKikimrBlobStorage::TFailRealmStates restored;
            UNIT_ASSERT(restored.ParseFromString(record.SerializeAsString()));
            UNIT_ASSERT(restored.HasGroupState());
            UNIT_ASSERT(restored.GetGroupState() == test.State);
            UNIT_ASSERT(IsValidMirror3dcGroupConfiguration({restored.GetGroupState(),
                {restored.GetState(0), restored.GetState(1), restored.GetState(2)}}));
        }
    }

    Y_UNIT_TEST(PendingCannotBeInferredFromRealmStates) {
        const TMirror3dcFailRealmStates realms{TFailRealm::INCLUDED, TFailRealm::EXCLUDED, TFailRealm::INCLUDED};
        const TMirror3dcGroupConfiguration failure{NKikimrBlobStorage::TGroupState::DC_FAILURE, realms};
        const TMirror3dcGroupConfiguration pending{NKikimrBlobStorage::TGroupState::DC_FAILURE_INV1_PENDING, realms};
        UNIT_ASSERT(IsValidMirror3dcGroupConfiguration(failure));
        UNIT_ASSERT(IsValidMirror3dcGroupConfiguration(pending));
        UNIT_ASSERT(!IsMirror3dcReplicationTarget(failure.State, TFailRealm::INCLUDED));
        UNIT_ASSERT(IsMirror3dcReplicationTarget(pending.State, TFailRealm::INCLUDED));
        UNIT_ASSERT(!RecoveringComplete(failure));
        const auto completed = RecoveringComplete(pending);
        UNIT_ASSERT(completed && completed->State == NKikimrBlobStorage::TGroupState::DC_FAILURE_INV1);
        UNIT_ASSERT((completed->Realms == TMirror3dcFailRealmStates{TFailRealm::INCLUDED, TFailRealm::EXCLUDED, TFailRealm::INCLUDED}));
        UNIT_ASSERT(IsAllowedMirror3dcGroupTransition(failure, pending));
        UNIT_ASSERT(IsAllowedMirror3dcGroupTransition(pending, *completed));
        UNIT_ASSERT(!IsAllowedMirror3dcGroupTransition(failure, *completed));
    }

    Y_UNIT_TEST(AllowedEdgesHaveValidPolicies) {
        UNIT_ASSERT(IsAllowedMirror3dcGroupTransition(NKikimrBlobStorage::TGroupState::DC_FAILURE, NKikimrBlobStorage::TGroupState::DC_FAILURE_INV1_PENDING));
        UNIT_ASSERT(IsAllowedMirror3dcGroupTransition(NKikimrBlobStorage::TGroupState::DC_FAILURE_INV1_PENDING, NKikimrBlobStorage::TGroupState::DC_FAILURE_INV1));
        UNIT_ASSERT(!IsAllowedMirror3dcGroupTransition(NKikimrBlobStorage::TGroupState::DC_FAILURE, NKikimrBlobStorage::TGroupState::DC_FAILURE_INV1));
        for (const auto& edge : Mirror3dcAllowedGroupTransitions) {
            UNIT_ASSERT(GetMirror3dcGroupLayouts(edge.From));
            UNIT_ASSERT(GetMirror3dcGroupLayouts(edge.To));
        }
    }

    Y_UNIT_TEST(AvailabilityRetainsPhysicalRealmIdentities) {
        const TMirror3dcGroupConfiguration before{NKikimrBlobStorage::TGroupState::DC_FAILURE_INV2, {TFailRealm::INCLUDED, TFailRealm::EXCLUDED, TFailRealm::INCLUDED}};
        const auto returned = ReturnFailRealm(before, 1);
        UNIT_ASSERT(returned && returned->State == NKikimrBlobStorage::TGroupState::DC_RECOVERY_INV1);
        UNIT_ASSERT((returned->Realms == TMirror3dcFailRealmStates{TFailRealm::INCLUDED, TFailRealm::RECOVERING, TFailRealm::INCLUDED}));
        std::optional<TMirror3dcGroupConfiguration> takenDown = TakeDownFailRealm(*returned, 1);
        UNIT_ASSERT(takenDown && *takenDown == TMirror3dcGroupConfiguration(NKikimrBlobStorage::TGroupState::DC_FAILURE_INV1, before.Realms));
        const auto completed = RecoveringComplete(*returned);
        UNIT_ASSERT(completed && completed->State == NKikimrBlobStorage::TGroupState::NORMAL);
        UNIT_ASSERT((completed->Realms == TMirror3dcFailRealmStates{TFailRealm::INCLUDED, TFailRealm::INCLUDED, TFailRealm::INCLUDED}));
        UNIT_ASSERT(!TakeDownFailRealm(before, 3));
        UNIT_ASSERT(!ReturnFailRealm(before, 0));
        UNIT_ASSERT(!IsAllowedMirror3dcGroupTransition(before,
            {NKikimrBlobStorage::TGroupState::DC_RECOVERY_INV1, {TFailRealm::RECOVERING, TFailRealm::INCLUDED, TFailRealm::INCLUDED}}));
    }

    Y_UNIT_TEST(TransitionsRejectPhysicalRealmPermutations) {
        // Both endpoints are valid and the group-state edge exists. Reassigning
        // the excluded DC still changes multiple physical realms and must fail.
        const TMirror3dcGroupConfiguration failure{NKikimrBlobStorage::TGroupState::DC_FAILURE,
            {TFailRealm::INCLUDED, TFailRealm::INCLUDED, TFailRealm::EXCLUDED}};
        const TMirror3dcGroupConfiguration recovery{NKikimrBlobStorage::TGroupState::DC_RECOVERY,
            {TFailRealm::INCLUDED, TFailRealm::INCLUDED, TFailRealm::RECOVERING}};
        auto swappedRecovery = recovery;
        std::swap(swappedRecovery.Realms[1], swappedRecovery.Realms[2]);
        UNIT_ASSERT(IsValidMirror3dcGroupConfiguration(swappedRecovery));
        UNIT_ASSERT(IsAllowedMirror3dcGroupTransition(failure, recovery));
        UNIT_ASSERT(!IsAllowedMirror3dcGroupTransition(failure, swappedRecovery));

        const TMirror3dcGroupConfiguration invariant{NKikimrBlobStorage::TGroupState::DC_FAILURE_INV1,
            {TFailRealm::INCLUDED, TFailRealm::INCLUDED, TFailRealm::EXCLUDED}};
        const TMirror3dcGroupConfiguration solo{NKikimrBlobStorage::TGroupState::DC_SOLO_INV1,
            {TFailRealm::INCLUDED, TFailRealm::EXCLUDED, TFailRealm::EXCLUDED}};
        auto swappedSolo = solo;
        std::swap(swappedSolo.Realms[0], swappedSolo.Realms[2]);
        UNIT_ASSERT(IsValidMirror3dcGroupConfiguration(swappedSolo));
        UNIT_ASSERT(IsAllowedMirror3dcGroupTransition(invariant, solo));
        UNIT_ASSERT(!IsAllowedMirror3dcGroupTransition(invariant, swappedSolo));

        for (const auto& test : Cases) {
            const TMirror3dcGroupConfiguration before{test.State, test.Realms};
            auto roles = test.Realms;
            std::sort(roles.begin(), roles.end());
            do {
                UNIT_ASSERT_VALUES_EQUAL(IsAllowedMirror3dcGroupTransition(before, {test.State, roles}),
                    roles == before.Realms);
            } while (std::next_permutation(roles.begin(), roles.end()));
        }
    }

    Y_UNIT_TEST(SoloRecoveryHasTwoSystemStages) {
        const TMirror3dcGroupConfiguration initial{NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY, {TFailRealm::EXCLUDED, TFailRealm::RECOVERING, TFailRealm::INCLUDED}};
        const auto pending = RecoveringComplete(initial);
        UNIT_ASSERT(pending && pending->State == NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY_INV1_PENDING);
        UNIT_ASSERT((pending->Realms == TMirror3dcFailRealmStates{TFailRealm::EXCLUDED, TFailRealm::RECOVERING, TFailRealm::INCLUDED}));
        UNIT_ASSERT(GetMirror3dcGroupLayouts(pending->State)->Write == ELayout::LAYOUT_2);
        const auto ready = RecoveringComplete(*pending);
        UNIT_ASSERT(ready && ready->State == NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY_INV1);
        UNIT_ASSERT(ready->Realms == pending->Realms);
        UNIT_ASSERT(GetMirror3dcGroupLayouts(ready->State)->Write == ELayout::LAYOUT_22);
        const auto restored = RecoveringComplete(*ready);
        UNIT_ASSERT(restored && restored->State == NKikimrBlobStorage::TGroupState::DC_FAILURE_INV1);
        UNIT_ASSERT((restored->Realms == TMirror3dcFailRealmStates{TFailRealm::EXCLUDED, TFailRealm::INCLUDED, TFailRealm::INCLUDED}));
    }

    Y_UNIT_TEST(ReplicationTargetsFollowTheGraph) {
        for (const auto state : {NKikimrBlobStorage::TGroupState::DC_FAILURE, NKikimrBlobStorage::TGroupState::DC_FAILURE_INV1, NKikimrBlobStorage::TGroupState::DC_FAILURE_INV2,
                NKikimrBlobStorage::TGroupState::DC_SOLO_DISASTER, NKikimrBlobStorage::TGroupState::DC_SOLO_INV1, NKikimrBlobStorage::TGroupState::DC_SOLO_INV2}) {
            for (const auto realm : {TFailRealm::INCLUDED, TFailRealm::INCLUDED, TFailRealm::INCLUDED, TFailRealm::RECOVERING, TFailRealm::EXCLUDED}) {
                UNIT_ASSERT(!IsMirror3dcReplicationTarget(state, realm));
            }
        }
        for (const auto state : {NKikimrBlobStorage::TGroupState::DC_RECOVERY_INV1, NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY_INV1,
                NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY_INV2, NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY_INV1_PENDING, NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY_INV2_PENDING}) {
            UNIT_ASSERT(IsMirror3dcReplicationTarget(state, TFailRealm::RECOVERING));
            UNIT_ASSERT(!IsMirror3dcReplicationTarget(state, TFailRealm::INCLUDED));
            UNIT_ASSERT(!IsMirror3dcReplicationTarget(state, TFailRealm::INCLUDED));
        }
        UNIT_ASSERT(IsMirror3dcReplicationTarget(NKikimrBlobStorage::TGroupState::DC_RECOVERY, TFailRealm::INCLUDED));
        UNIT_ASSERT(IsMirror3dcReplicationTarget(NKikimrBlobStorage::TGroupState::DC_RECOVERY, TFailRealm::RECOVERING));
        UNIT_ASSERT(GetMirror3dcReadLayout(NKikimrBlobStorage::TGroupState::DC_SOLO_INV2_PENDING) == ELayout::LAYOUT_1);
    }
}
} // namespace NKikimr
