#include "blobstorage_groupinfo_mirror3dc_state.h"
#include <library/cpp/testing/unittest/registar.h>
#include <algorithm>

namespace NKikimr {
namespace {
using G = EMirror3dcGroupState;
using R = NKikimrBlobStorage::TGroupInfo::TFailRealm;
using L = EMirror3dcLayout;
using K = EMirror3dcTransitionKind;
struct TStateCase {
    G State;
    TMirror3dcFailRealmStates Realms;
    L Write;
    L Durability;
    L Expected;
};
// Literal expectations from the agreed 17-state graph.
constexpr TStateCase Cases[] = {
    {G::NORMAL, {R::N0, R::N0, R::N0}, L::LAYOUT_111_22, L::LAYOUT_111, L::LAYOUT_111_22},
    {G::DC_RECOVERY, {R::N0, R::N0, R::R}, L::LAYOUT_111_22, L::LAYOUT_111, L::LAYOUT_11_2},
    {G::DC_RECOVERY_INV1, {R::N1, R::N1, R::R}, L::LAYOUT_111_22, L::LAYOUT_111, L::LAYOUT_11},
    {G::DC_FAILURE, {R::N0, R::N0, R::F}, L::LAYOUT_22, L::LAYOUT_0, L::LAYOUT_11_2},
    {G::DC_FAILURE_INV1, {R::N1, R::N1, R::F}, L::LAYOUT_22, L::LAYOUT_11, L::LAYOUT_11},
    {G::DC_FAILURE_INV2, {R::N2, R::N2, R::F}, L::LAYOUT_22, L::LAYOUT_22, L::LAYOUT_22},
    {G::DC_SOLO_DISASTER, {R::N0, R::F, R::F}, L::LAYOUT_2, L::LAYOUT_0, L::LAYOUT_0},
    {G::DC_SOLO_INV1, {R::N1, R::F, R::F}, L::LAYOUT_2, L::LAYOUT_1, L::LAYOUT_1},
    {G::DC_SOLO_INV2, {R::N2, R::F, R::F}, L::LAYOUT_2, L::LAYOUT_2, L::LAYOUT_2},
    {G::DC_SOLO_RECOVERY, {R::N0, R::R, R::F}, L::LAYOUT_2, L::LAYOUT_11, L::LAYOUT_0},
    {G::DC_SOLO_RECOVERY_INV1, {R::N1, R::R, R::F}, L::LAYOUT_22, L::LAYOUT_11, L::LAYOUT_1},
    {G::DC_SOLO_RECOVERY_INV2, {R::N2, R::R, R::F}, L::LAYOUT_22, L::LAYOUT_11, L::LAYOUT_2},
    {G::DC_FAILURE_INV1_PENDING, {R::N0, R::N0, R::F}, L::LAYOUT_22, L::LAYOUT_11, L::LAYOUT_11_2},
    {G::DC_FAILURE_INV2_PENDING, {R::N1, R::N1, R::F}, L::LAYOUT_22, L::LAYOUT_22, L::LAYOUT_11},
    {G::DC_SOLO_RECOVERY_INV1_PENDING, {R::N1, R::R, R::F}, L::LAYOUT_2, L::LAYOUT_11, L::LAYOUT_1},
    {G::DC_SOLO_RECOVERY_INV2_PENDING, {R::N2, R::R, R::F}, L::LAYOUT_2, L::LAYOUT_11, L::LAYOUT_2},
    {G::DC_SOLO_INV2_PENDING, {R::N1, R::F, R::F}, L::LAYOUT_2, L::LAYOUT_2, L::LAYOUT_2},
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
        UNIT_ASSERT(!GetMirror3dcGroupLayouts(static_cast<G>(-1)));
        UNIT_ASSERT(!MakeMirror3dcGroupConfiguration(static_cast<G>(17), {R::N0, R::N0, R::N0}));
        UNIT_ASSERT(!MakeMirror3dcGroupConfiguration(G::NORMAL, {R::N0, R::R, R::F}));
    }

    Y_UNIT_TEST(PendingCannotBeInferredFromRealmStates) {
        const TMirror3dcFailRealmStates realms{R::N0, R::F, R::N0};
        const TMirror3dcGroupConfiguration failure{G::DC_FAILURE, realms};
        const TMirror3dcGroupConfiguration pending{G::DC_FAILURE_INV1_PENDING, realms};
        UNIT_ASSERT(IsValidMirror3dcGroupConfiguration(failure));
        UNIT_ASSERT(IsValidMirror3dcGroupConfiguration(pending));
        UNIT_ASSERT(!IsMirror3dcReplicationTarget(failure.State, R::N0));
        UNIT_ASSERT(IsMirror3dcReplicationTarget(pending.State, R::N0));
        UNIT_ASSERT(!CompleteMirror3dcReplication(failure));
        const auto completed = CompleteMirror3dcReplication(pending);
        UNIT_ASSERT(completed && completed->State == G::DC_FAILURE_INV1);
        UNIT_ASSERT((completed->Realms == TMirror3dcFailRealmStates{R::N1, R::F, R::N1}));
        UNIT_ASSERT(IsAllowedMirror3dcGroupTransition(failure, pending));
        UNIT_ASSERT(IsAllowedMirror3dcGroupTransition(pending, *completed));
        UNIT_ASSERT(!IsAllowedMirror3dcGroupTransition(failure, *completed));
    }

    Y_UNIT_TEST(AdminAndSystemEdgesAreDistinct) {
        UNIT_ASSERT(IsAllowedMirror3dcGroupTransition(G::DC_FAILURE, G::DC_FAILURE_INV1_PENDING, K::Admin));
        UNIT_ASSERT(!IsAllowedMirror3dcGroupTransition(G::DC_FAILURE, G::DC_FAILURE_INV1_PENDING, K::System));
        UNIT_ASSERT(IsAllowedMirror3dcGroupTransition(G::DC_FAILURE_INV1_PENDING, G::DC_FAILURE_INV1, K::System));
        UNIT_ASSERT(!IsAllowedMirror3dcGroupTransition(G::DC_FAILURE_INV1_PENDING, G::DC_FAILURE_INV1, K::Admin));
        for (const auto& edge : Mirror3dcAllowedGroupTransitions) {
            UNIT_ASSERT(GetMirror3dcGroupLayouts(edge.From));
            UNIT_ASSERT(GetMirror3dcGroupLayouts(edge.To));
        }
    }

    Y_UNIT_TEST(AvailabilityRetainsPhysicalRealmIdentities) {
        const TMirror3dcGroupConfiguration before{G::DC_FAILURE_INV2, {R::N2, R::F, R::N2}};
        const auto returned = ChangeMirror3dcRealmAvailability(before, 1, false);
        UNIT_ASSERT(returned && returned->State == G::DC_RECOVERY_INV1);
        UNIT_ASSERT((returned->Realms == TMirror3dcFailRealmStates{R::N1, R::R, R::N1}));
        const auto completed = CompleteMirror3dcReplication(*returned);
        UNIT_ASSERT(completed && completed->State == G::NORMAL);
        UNIT_ASSERT((completed->Realms == TMirror3dcFailRealmStates{R::N0, R::N0, R::N0}));
        UNIT_ASSERT(!ChangeMirror3dcRealmAvailability(before, 3, true));
        UNIT_ASSERT(!ChangeMirror3dcRealmAvailability(before, 0, false));
        UNIT_ASSERT(!IsAllowedMirror3dcGroupTransition(before,
            {G::DC_RECOVERY_INV1, {R::R, R::N1, R::N1}}));
    }

    Y_UNIT_TEST(SoloRecoveryHasTwoSystemStages) {
        const TMirror3dcGroupConfiguration initial{G::DC_SOLO_RECOVERY, {R::F, R::R, R::N0}};
        const auto pending = CompleteMirror3dcReplication(initial);
        UNIT_ASSERT(pending && pending->State == G::DC_SOLO_RECOVERY_INV1_PENDING);
        UNIT_ASSERT((pending->Realms == TMirror3dcFailRealmStates{R::F, R::R, R::N1}));
        UNIT_ASSERT(GetMirror3dcGroupLayouts(pending->State)->Write == L::LAYOUT_2);
        const auto ready = CompleteMirror3dcReplication(*pending);
        UNIT_ASSERT(ready && ready->State == G::DC_SOLO_RECOVERY_INV1);
        UNIT_ASSERT(ready->Realms == pending->Realms);
        UNIT_ASSERT(GetMirror3dcGroupLayouts(ready->State)->Write == L::LAYOUT_22);
        const auto restored = CompleteMirror3dcReplication(*ready);
        UNIT_ASSERT(restored && restored->State == G::DC_FAILURE_INV1);
        UNIT_ASSERT((restored->Realms == TMirror3dcFailRealmStates{R::F, R::N1, R::N1}));
    }

    Y_UNIT_TEST(ReplicationTargetsFollowTheGraph) {
        for (const auto state : {G::DC_FAILURE, G::DC_FAILURE_INV1, G::DC_FAILURE_INV2,
                G::DC_SOLO_DISASTER, G::DC_SOLO_INV1, G::DC_SOLO_INV2}) {
            for (const auto realm : {R::N0, R::N1, R::N2, R::R, R::F}) {
                UNIT_ASSERT(!IsMirror3dcReplicationTarget(state, realm));
            }
        }
        for (const auto state : {G::DC_RECOVERY_INV1, G::DC_SOLO_RECOVERY_INV1,
                G::DC_SOLO_RECOVERY_INV2, G::DC_SOLO_RECOVERY_INV1_PENDING, G::DC_SOLO_RECOVERY_INV2_PENDING}) {
            UNIT_ASSERT(IsMirror3dcReplicationTarget(state, R::R));
            UNIT_ASSERT(!IsMirror3dcReplicationTarget(state, R::N1));
            UNIT_ASSERT(!IsMirror3dcReplicationTarget(state, R::N2));
        }
        UNIT_ASSERT(IsMirror3dcReplicationTarget(G::DC_RECOVERY, R::N0));
        UNIT_ASSERT(IsMirror3dcReplicationTarget(G::DC_RECOVERY, R::R));
        UNIT_ASSERT(GetMirror3dcReadLayout(G::DC_SOLO_INV2_PENDING) == L::LAYOUT_1);
    }
}
} // namespace NKikimr
