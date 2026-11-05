#include "blobstorage_groupinfo_mirror3dc_state.h"
#include <library/cpp/testing/unittest/registar.h>
#include <algorithm>

namespace NKikimr {
namespace {
using TGroupState = TMirror3dcGroupState;
using TFailRealm = NKikimrBlobStorage::TGroupInfo::TFailRealm;
using ELayout = EMirror3dcLayout;
using ETransitionKind = EMirror3dcTransitionKind;
struct TStateCase {
    EMirror3dcGroupState State;
    TMirror3dcFailRealmStates Realms;
    L Write;
    L Durability;
    L Expected;
};
// Literal expectations from the agreed 17-state graph.
constexpr TStateCase Cases[] = {
    {TGroupState::NORMAL, {TFailRealm::N0, TFailRealm::N0, TFailRealm::N0}, ELayout::LAYOUT_111_22, ELayout::LAYOUT_111, ELayout::LAYOUT_111_22},
    {TGroupState::DC_RECOVERY, {TFailRealm::N0, TFailRealm::N0, TFailRealm::R}, ELayout::LAYOUT_111_22, ELayout::LAYOUT_111, ELayout::LAYOUT_11_2},
    {TGroupState::DC_RECOVERY_INV1, {TFailRealm::N1, TFailRealm::N1, TFailRealm::R}, ELayout::LAYOUT_111_22, ELayout::LAYOUT_111, ELayout::LAYOUT_11},
    {TGroupState::DC_FAILURE, {TFailRealm::N0, TFailRealm::N0, TFailRealm::F}, ELayout::LAYOUT_22, ELayout::LAYOUT_0, ELayout::LAYOUT_11_2},
    {TGroupState::DC_FAILURE_INV1, {TFailRealm::N1, TFailRealm::N1, TFailRealm::F}, ELayout::LAYOUT_22, ELayout::LAYOUT_11, ELayout::LAYOUT_11},
    {TGroupState::DC_FAILURE_INV2, {TFailRealm::N2, TFailRealm::N2, TFailRealm::F}, ELayout::LAYOUT_22, ELayout::LAYOUT_22, ELayout::LAYOUT_22},
    {TGroupState::DC_SOLO_DISASTER, {TFailRealm::N0, TFailRealm::F, TFailRealm::F}, ELayout::LAYOUT_2, ELayout::LAYOUT_0, ELayout::LAYOUT_0},
    {TGroupState::DC_SOLO_INV1, {TFailRealm::N1, TFailRealm::F, TFailRealm::F}, ELayout::LAYOUT_2, ELayout::LAYOUT_1, ELayout::LAYOUT_1},
    {TGroupState::DC_SOLO_INV2, {TFailRealm::N2, TFailRealm::F, TFailRealm::F}, ELayout::LAYOUT_2, ELayout::LAYOUT_2, ELayout::LAYOUT_2},
    {TGroupState::DC_SOLO_RECOVERY, {TFailRealm::N0, TFailRealm::R, TFailRealm::F}, ELayout::LAYOUT_2, ELayout::LAYOUT_1, ELayout::LAYOUT_0},
    {TGroupState::DC_SOLO_RECOVERY_INV1, {TFailRealm::N1, TFailRealm::R, TFailRealm::F}, ELayout::LAYOUT_22, ELayout::LAYOUT_11, ELayout::LAYOUT_1},
    {TGroupState::DC_SOLO_RECOVERY_INV2, {TFailRealm::N2, TFailRealm::R, TFailRealm::F}, ELayout::LAYOUT_22, ELayout::LAYOUT_11, ELayout::LAYOUT_2},
    {TGroupState::DC_FAILURE_INV1_PENDING, {TFailRealm::N0, TFailRealm::N0, TFailRealm::F}, ELayout::LAYOUT_22, ELayout::LAYOUT_11, ELayout::LAYOUT_11_2},
    {TGroupState::DC_FAILURE_INV2_PENDING, {TFailRealm::N1, TFailRealm::N1, TFailRealm::F}, ELayout::LAYOUT_22, ELayout::LAYOUT_22, ELayout::LAYOUT_11},
    {TGroupState::DC_SOLO_RECOVERY_INV1_PENDING, {TFailRealm::N1, TFailRealm::R, TFailRealm::F}, ELayout::LAYOUT_2, ELayout::LAYOUT_1, ELayout::LAYOUT_1},
    {TGroupState::DC_SOLO_RECOVERY_INV2_PENDING, {TFailRealm::N2, TFailRealm::R, TFailRealm::F}, ELayout::LAYOUT_2, ELayout::LAYOUT_11, ELayout::LAYOUT_2},
    {TGroupState::DC_SOLO_INV2_PENDING, {TFailRealm::N1, TFailRealm::F, TFailRealm::F}, ELayout::LAYOUT_2, ELayout::LAYOUT_2, ELayout::LAYOUT_1},
};
}

Y_UNIT_TEST_SUITE(TMirror3dcGroupStateTest) {
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
        UNIT_ASSERT(!MakeMirror3dcGroupConfiguration(static_cast<EMirror3dcGroupState>(17), {TFailRealm::N0, TFailRealm::N0, TFailRealm::N0}));
        UNIT_ASSERT(!MakeMirror3dcGroupConfiguration(TGroupState::NORMAL, {TFailRealm::N0, TFailRealm::R, TFailRealm::F}));
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
        const TMirror3dcFailRealmStates realms{TFailRealm::N0, TFailRealm::F, TFailRealm::N0};
        const TMirror3dcGroupConfiguration failure{TGroupState::DC_FAILURE, realms};
        const TMirror3dcGroupConfiguration pending{TGroupState::DC_FAILURE_INV1_PENDING, realms};
        UNIT_ASSERT(IsValidMirror3dcGroupConfiguration(failure));
        UNIT_ASSERT(IsValidMirror3dcGroupConfiguration(pending));
        UNIT_ASSERT(!IsMirror3dcReplicationTarget(failure.State, TFailRealm::N0));
        UNIT_ASSERT(IsMirror3dcReplicationTarget(pending.State, TFailRealm::N0));
        UNIT_ASSERT(!CompleteMirror3dcReplication(failure));
        const auto completed = CompleteMirror3dcReplication(pending);
        UNIT_ASSERT(completed && completed->State == TGroupState::DC_FAILURE_INV1);
        UNIT_ASSERT((completed->Realms == TMirror3dcFailRealmStates{TFailRealm::N1, TFailRealm::F, TFailRealm::N1}));
        UNIT_ASSERT(IsAllowedMirror3dcGroupTransition(failure, pending));
        UNIT_ASSERT(IsAllowedMirror3dcGroupTransition(pending, *completed));
        UNIT_ASSERT(!IsAllowedMirror3dcGroupTransition(failure, *completed));
    }

    Y_UNIT_TEST(AdminAndSystemEdgesAreDistinct) {
        UNIT_ASSERT(IsAllowedMirror3dcGroupTransition(TGroupState::DC_FAILURE, TGroupState::DC_FAILURE_INV1_PENDING, ETransitionKind::Admin));
        UNIT_ASSERT(!IsAllowedMirror3dcGroupTransition(TGroupState::DC_FAILURE, TGroupState::DC_FAILURE_INV1_PENDING, ETransitionKind::System));
        UNIT_ASSERT(IsAllowedMirror3dcGroupTransition(TGroupState::DC_FAILURE_INV1_PENDING, TGroupState::DC_FAILURE_INV1, ETransitionKind::System));
        UNIT_ASSERT(!IsAllowedMirror3dcGroupTransition(TGroupState::DC_FAILURE_INV1_PENDING, TGroupState::DC_FAILURE_INV1, ETransitionKind::Admin));
        for (const auto& edge : Mirror3dcAllowedGroupTransitions) {
            UNIT_ASSERT(GetMirror3dcGroupLayouts(edge.From));
            UNIT_ASSERT(GetMirror3dcGroupLayouts(edge.To));
        }
    }

    Y_UNIT_TEST(AvailabilityRetainsPhysicalRealmIdentities) {
        const TMirror3dcGroupConfiguration before{TGroupState::DC_FAILURE_INV2, {TFailRealm::N2, TFailRealm::F, TFailRealm::N2}};
        const auto returned = ChangeMirror3dcRealmAvailability(before, 1, false);
        UNIT_ASSERT(returned && returned->State == TGroupState::DC_RECOVERY_INV1);
        UNIT_ASSERT((returned->Realms == TMirror3dcFailRealmStates{TFailRealm::N1, TFailRealm::R, TFailRealm::N1}));
        const auto completed = CompleteMirror3dcReplication(*returned);
        UNIT_ASSERT(completed && completed->State == TGroupState::NORMAL);
        UNIT_ASSERT((completed->Realms == TMirror3dcFailRealmStates{TFailRealm::N0, TFailRealm::N0, TFailRealm::N0}));
        UNIT_ASSERT(!ChangeMirror3dcRealmAvailability(before, 3, true));
        UNIT_ASSERT(!ChangeMirror3dcRealmAvailability(before, 0, false));
        UNIT_ASSERT(!IsAllowedMirror3dcGroupTransition(before,
            {TGroupState::DC_RECOVERY_INV1, {TFailRealm::R, TFailRealm::N1, TFailRealm::N1}}));
    }

    Y_UNIT_TEST(SoloRecoveryHasTwoSystemStages) {
        const TMirror3dcGroupConfiguration initial{TGroupState::DC_SOLO_RECOVERY, {TFailRealm::F, TFailRealm::R, TFailRealm::N0}};
        const auto pending = CompleteMirror3dcReplication(initial);
        UNIT_ASSERT(pending && pending->State == TGroupState::DC_SOLO_RECOVERY_INV1_PENDING);
        UNIT_ASSERT((pending->Realms == TMirror3dcFailRealmStates{TFailRealm::F, TFailRealm::R, TFailRealm::N1}));
        UNIT_ASSERT(GetMirror3dcGroupLayouts(pending->State)->Write == ELayout::LAYOUT_2);
        const auto ready = CompleteMirror3dcReplication(*pending);
        UNIT_ASSERT(ready && ready->State == TGroupState::DC_SOLO_RECOVERY_INV1);
        UNIT_ASSERT(ready->Realms == pending->Realms);
        UNIT_ASSERT(GetMirror3dcGroupLayouts(ready->State)->Write == ELayout::LAYOUT_22);
        const auto restored = CompleteMirror3dcReplication(*ready);
        UNIT_ASSERT(restored && restored->State == TGroupState::DC_FAILURE_INV1);
        UNIT_ASSERT((restored->Realms == TMirror3dcFailRealmStates{TFailRealm::F, TFailRealm::N1, TFailRealm::N1}));
    }

    Y_UNIT_TEST(ReplicationTargetsFollowTheGraph) {
        for (const auto state : {TGroupState::DC_FAILURE, TGroupState::DC_FAILURE_INV1, TGroupState::DC_FAILURE_INV2,
                TGroupState::DC_SOLO_DISASTER, TGroupState::DC_SOLO_INV1, TGroupState::DC_SOLO_INV2}) {
            for (const auto realm : {TFailRealm::N0, TFailRealm::N1, TFailRealm::N2, TFailRealm::R, TFailRealm::F}) {
                UNIT_ASSERT(!IsMirror3dcReplicationTarget(state, realm));
            }
        }
        for (const auto state : {TGroupState::DC_RECOVERY_INV1, TGroupState::DC_SOLO_RECOVERY_INV1,
                TGroupState::DC_SOLO_RECOVERY_INV2, TGroupState::DC_SOLO_RECOVERY_INV1_PENDING, TGroupState::DC_SOLO_RECOVERY_INV2_PENDING}) {
            UNIT_ASSERT(IsMirror3dcReplicationTarget(state, TFailRealm::R));
            UNIT_ASSERT(!IsMirror3dcReplicationTarget(state, TFailRealm::N1));
            UNIT_ASSERT(!IsMirror3dcReplicationTarget(state, TFailRealm::N2));
        }
        UNIT_ASSERT(IsMirror3dcReplicationTarget(TGroupState::DC_RECOVERY, TFailRealm::N0));
        UNIT_ASSERT(IsMirror3dcReplicationTarget(TGroupState::DC_RECOVERY, TFailRealm::R));
        UNIT_ASSERT(GetMirror3dcReadLayout(TGroupState::DC_SOLO_INV2_PENDING) == ELayout::LAYOUT_1);
    }
}
} // namespace NKikimr
