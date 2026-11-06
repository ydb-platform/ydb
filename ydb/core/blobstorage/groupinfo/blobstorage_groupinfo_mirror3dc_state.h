#pragma once

#include <ydb/core/protos/blobstorage_base3.pb.h>

#include <algorithm>
#include <array>
#include <limits>
#include <optional>

namespace NKikimr {

// State definitions and the complete state diagram live in blobstorage_base3.proto.
using EMirror3dcGroupState = NKikimrBlobStorage::TGroupState::EState;

using EMirror3dcFailRealmState = NKikimrBlobStorage::TDataCenterState::EState;
using TMirror3dcFailRealmStates = std::array<EMirror3dcFailRealmState, 3>;

constexpr bool IsIncluded(EMirror3dcFailRealmState state) {
    return state == NKikimrBlobStorage::TDataCenterState::INCLUDED;
}

constexpr bool IsExcluded(EMirror3dcFailRealmState state) {
    return state == NKikimrBlobStorage::TDataCenterState::EXCLUDED;
}

constexpr bool IsRecovering(EMirror3dcFailRealmState state) {
    return state == NKikimrBlobStorage::TDataCenterState::RECOVERING;
}

struct TMirror3dcGroupState {
    EMirror3dcGroupState State = NKikimrBlobStorage::TGroupState::NORMAL;
    // Indexed by physical failrealm and stored with the group generation.
    TMirror3dcFailRealmStates FailRealmStates{};

    bool operator==(const TMirror3dcGroupState&) const = default;
};

// Each number is a minimum copy count in a distinct failrealm; OR is an alternative.
enum class EMirror3dcLayout {
    LAYOUT_111_22,
    LAYOUT_11_2,
    LAYOUT_11,
    LAYOUT_22,
    LAYOUT_0,
    LAYOUT_1,
    LAYOUT_2,
    LAYOUT_111,
};

struct TMirror3dcGroupLayouts {
    EMirror3dcLayout Write;      // W: sufficient to acknowledge a new write.
    EMirror3dcLayout Durability; // D: background replication target.
    EMirror3dcLayout Ensured;    // E: guarantee for all confirmed blobs.
};

constexpr std::optional<TMirror3dcGroupLayouts> GetMirror3dcGroupLayouts(EMirror3dcGroupState state) {
    using ELayout = EMirror3dcLayout;
    switch (state) {
        // Proto3 adds sentinel values that do not represent group states.
        case static_cast<EMirror3dcGroupState>(std::numeric_limits<int>::min()):
        case static_cast<EMirror3dcGroupState>(std::numeric_limits<int>::max()):
            return std::nullopt;
        case NKikimrBlobStorage::TGroupState::NORMAL:
            return TMirror3dcGroupLayouts{ELayout::LAYOUT_111_22, ELayout::LAYOUT_111, ELayout::LAYOUT_111_22};
        case NKikimrBlobStorage::TGroupState::DC_FAILURE:
            return TMirror3dcGroupLayouts{ELayout::LAYOUT_22, ELayout::LAYOUT_0, ELayout::LAYOUT_11_2};
        case NKikimrBlobStorage::TGroupState::DC_FAILURE_INV1:
            return TMirror3dcGroupLayouts{ELayout::LAYOUT_22, ELayout::LAYOUT_11, ELayout::LAYOUT_11};
        case NKikimrBlobStorage::TGroupState::DC_FAILURE_INV2:
            return TMirror3dcGroupLayouts{ELayout::LAYOUT_22, ELayout::LAYOUT_22, ELayout::LAYOUT_22};
        case NKikimrBlobStorage::TGroupState::DC_RECOVERY:
            return TMirror3dcGroupLayouts{ELayout::LAYOUT_111_22, ELayout::LAYOUT_111, ELayout::LAYOUT_11_2};
        case NKikimrBlobStorage::TGroupState::DC_RECOVERY_INV1:
            return TMirror3dcGroupLayouts{ELayout::LAYOUT_111_22, ELayout::LAYOUT_111, ELayout::LAYOUT_11};
        case NKikimrBlobStorage::TGroupState::DC_SOLO_DISASTER:
            return TMirror3dcGroupLayouts{ELayout::LAYOUT_2, ELayout::LAYOUT_0, ELayout::LAYOUT_0};
        case NKikimrBlobStorage::TGroupState::DC_SOLO_INV1:
            return TMirror3dcGroupLayouts{ELayout::LAYOUT_2, ELayout::LAYOUT_1, ELayout::LAYOUT_1};
        case NKikimrBlobStorage::TGroupState::DC_SOLO_INV2:
            return TMirror3dcGroupLayouts{ELayout::LAYOUT_2, ELayout::LAYOUT_2, ELayout::LAYOUT_2};
        case NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY:
            return TMirror3dcGroupLayouts{ELayout::LAYOUT_2, ELayout::LAYOUT_1, ELayout::LAYOUT_0};
        case NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY_INV1:
            return TMirror3dcGroupLayouts{ELayout::LAYOUT_22, ELayout::LAYOUT_11, ELayout::LAYOUT_1};
        case NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY_INV2:
            return TMirror3dcGroupLayouts{ELayout::LAYOUT_22, ELayout::LAYOUT_11, ELayout::LAYOUT_2};
        case NKikimrBlobStorage::TGroupState::DC_FAILURE_INV1_PENDING:
            return TMirror3dcGroupLayouts{ELayout::LAYOUT_22, ELayout::LAYOUT_11, ELayout::LAYOUT_11_2};
        case NKikimrBlobStorage::TGroupState::DC_FAILURE_INV2_PENDING:
            return TMirror3dcGroupLayouts{ELayout::LAYOUT_22, ELayout::LAYOUT_22, ELayout::LAYOUT_11};
        case NKikimrBlobStorage::TGroupState::DC_SOLO_INV2_PENDING:
            return TMirror3dcGroupLayouts{ELayout::LAYOUT_2, ELayout::LAYOUT_2, ELayout::LAYOUT_1};
        case NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY_INV1_PENDING:
            return TMirror3dcGroupLayouts{ELayout::LAYOUT_2, ELayout::LAYOUT_1, ELayout::LAYOUT_1};
        case NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY_INV2_PENDING:
            return TMirror3dcGroupLayouts{ELayout::LAYOUT_2, ELayout::LAYOUT_11, ELayout::LAYOUT_2};
    }
    return std::nullopt;
}

// Reads use the confirmed guarantee E, including while replication is pending.
constexpr EMirror3dcLayout GetMirror3dcReadLayout(EMirror3dcGroupState state) {
    return GetMirror3dcGroupLayouts(state)->Ensured;
}

constexpr std::optional<TMirror3dcFailRealmStates> GetMirror3dcRealmStates(EMirror3dcGroupState state) {
    using TFailRealm = NKikimrBlobStorage::TDataCenterState;
    switch (state) {
        // Proto3 adds sentinel values that do not represent group states.
        case static_cast<EMirror3dcGroupState>(std::numeric_limits<int>::min()):
        case static_cast<EMirror3dcGroupState>(std::numeric_limits<int>::max()):
            return std::nullopt;
        case NKikimrBlobStorage::TGroupState::NORMAL:
            return TMirror3dcFailRealmStates{TFailRealm::INCLUDED, TFailRealm::INCLUDED, TFailRealm::INCLUDED};
        case NKikimrBlobStorage::TGroupState::DC_FAILURE:
            return TMirror3dcFailRealmStates{TFailRealm::INCLUDED, TFailRealm::INCLUDED, TFailRealm::EXCLUDED};
        case NKikimrBlobStorage::TGroupState::DC_FAILURE_INV1:
            return TMirror3dcFailRealmStates{TFailRealm::INCLUDED, TFailRealm::INCLUDED, TFailRealm::EXCLUDED};
        case NKikimrBlobStorage::TGroupState::DC_FAILURE_INV2:
            return TMirror3dcFailRealmStates{TFailRealm::INCLUDED, TFailRealm::INCLUDED, TFailRealm::EXCLUDED};
        case NKikimrBlobStorage::TGroupState::DC_RECOVERY:
            return TMirror3dcFailRealmStates{TFailRealm::INCLUDED, TFailRealm::INCLUDED, TFailRealm::RECOVERING};
        case NKikimrBlobStorage::TGroupState::DC_RECOVERY_INV1:
            return TMirror3dcFailRealmStates{TFailRealm::INCLUDED, TFailRealm::INCLUDED, TFailRealm::RECOVERING};
        case NKikimrBlobStorage::TGroupState::DC_SOLO_DISASTER:
            return TMirror3dcFailRealmStates{TFailRealm::INCLUDED, TFailRealm::EXCLUDED, TFailRealm::EXCLUDED};
        case NKikimrBlobStorage::TGroupState::DC_SOLO_INV1:
            return TMirror3dcFailRealmStates{TFailRealm::INCLUDED, TFailRealm::EXCLUDED, TFailRealm::EXCLUDED};
        case NKikimrBlobStorage::TGroupState::DC_SOLO_INV2:
            return TMirror3dcFailRealmStates{TFailRealm::INCLUDED, TFailRealm::EXCLUDED, TFailRealm::EXCLUDED};
        case NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY:
            return TMirror3dcFailRealmStates{TFailRealm::INCLUDED, TFailRealm::RECOVERING, TFailRealm::EXCLUDED};
        case NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY_INV1:
            return TMirror3dcFailRealmStates{TFailRealm::INCLUDED, TFailRealm::RECOVERING, TFailRealm::EXCLUDED};
        case NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY_INV2:
            return TMirror3dcFailRealmStates{TFailRealm::INCLUDED, TFailRealm::RECOVERING, TFailRealm::EXCLUDED};
        case NKikimrBlobStorage::TGroupState::DC_FAILURE_INV1_PENDING:
            return TMirror3dcFailRealmStates{TFailRealm::INCLUDED, TFailRealm::INCLUDED, TFailRealm::EXCLUDED};
        case NKikimrBlobStorage::TGroupState::DC_FAILURE_INV2_PENDING:
            return TMirror3dcFailRealmStates{TFailRealm::INCLUDED, TFailRealm::INCLUDED, TFailRealm::EXCLUDED};
        case NKikimrBlobStorage::TGroupState::DC_SOLO_INV2_PENDING:
            return TMirror3dcFailRealmStates{TFailRealm::INCLUDED, TFailRealm::EXCLUDED, TFailRealm::EXCLUDED};
        case NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY_INV1_PENDING:
            return TMirror3dcFailRealmStates{TFailRealm::INCLUDED, TFailRealm::RECOVERING, TFailRealm::EXCLUDED};
        case NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY_INV2_PENDING:
            return TMirror3dcFailRealmStates{TFailRealm::INCLUDED, TFailRealm::RECOVERING, TFailRealm::EXCLUDED};
    }
    return std::nullopt;
}

// Logical roles are mapped to physical realms by the supplied tuple. All changes
// retain realm identities, including when copy guarantees change on return to three DCs.
struct TMirror3dcGroupConfiguration {
    EMirror3dcGroupState State = NKikimrBlobStorage::TGroupState::NORMAL;
    TMirror3dcFailRealmStates Realms{};
    bool operator==(const TMirror3dcGroupConfiguration&) const = default;
};

struct TMirror3dcTransition {
    EMirror3dcGroupState From;
    EMirror3dcGroupState To;
};

inline constexpr TMirror3dcTransition Mirror3dcAllowedGroupTransitions[] = {
    {NKikimrBlobStorage::TGroupState::NORMAL, NKikimrBlobStorage::TGroupState::DC_FAILURE},
    {NKikimrBlobStorage::TGroupState::DC_FAILURE, NKikimrBlobStorage::TGroupState::DC_FAILURE_INV1_PENDING},
    {NKikimrBlobStorage::TGroupState::DC_FAILURE_INV1, NKikimrBlobStorage::TGroupState::DC_FAILURE_INV2_PENDING},
    {NKikimrBlobStorage::TGroupState::DC_FAILURE_INV1, NKikimrBlobStorage::TGroupState::DC_SOLO_INV1},
    {NKikimrBlobStorage::TGroupState::DC_FAILURE_INV2, NKikimrBlobStorage::TGroupState::DC_SOLO_INV2},
    {NKikimrBlobStorage::TGroupState::DC_FAILURE, NKikimrBlobStorage::TGroupState::DC_RECOVERY},
    {NKikimrBlobStorage::TGroupState::DC_RECOVERY, NKikimrBlobStorage::TGroupState::DC_FAILURE},
    {NKikimrBlobStorage::TGroupState::DC_FAILURE_INV1, NKikimrBlobStorage::TGroupState::DC_RECOVERY_INV1},
    {NKikimrBlobStorage::TGroupState::DC_RECOVERY_INV1, NKikimrBlobStorage::TGroupState::DC_FAILURE_INV1},
    {NKikimrBlobStorage::TGroupState::DC_FAILURE_INV2, NKikimrBlobStorage::TGroupState::DC_RECOVERY_INV1},
    {NKikimrBlobStorage::TGroupState::DC_FAILURE, NKikimrBlobStorage::TGroupState::DC_SOLO_DISASTER},
    {NKikimrBlobStorage::TGroupState::DC_FAILURE_INV1_PENDING, NKikimrBlobStorage::TGroupState::DC_RECOVERY},
    {NKikimrBlobStorage::TGroupState::DC_FAILURE_INV2_PENDING, NKikimrBlobStorage::TGroupState::DC_RECOVERY_INV1},
    {NKikimrBlobStorage::TGroupState::DC_SOLO_DISASTER, NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY},
    {NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY, NKikimrBlobStorage::TGroupState::DC_SOLO_DISASTER},
    {NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY_INV1, NKikimrBlobStorage::TGroupState::DC_SOLO_INV1},
    {NKikimrBlobStorage::TGroupState::DC_FAILURE_INV1_PENDING, NKikimrBlobStorage::TGroupState::DC_FAILURE_INV1},
    {NKikimrBlobStorage::TGroupState::DC_FAILURE_INV2_PENDING, NKikimrBlobStorage::TGroupState::DC_FAILURE_INV2},
    {NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY_INV2_PENDING, NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY_INV2},
    {NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY_INV2, NKikimrBlobStorage::TGroupState::DC_FAILURE_INV1},
    {NKikimrBlobStorage::TGroupState::DC_SOLO_INV1, NKikimrBlobStorage::TGroupState::DC_SOLO_INV2_PENDING},
    {NKikimrBlobStorage::TGroupState::DC_SOLO_INV1, NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY_INV1_PENDING},
    {NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY_INV1_PENDING, NKikimrBlobStorage::TGroupState::DC_SOLO_INV1},
    {NKikimrBlobStorage::TGroupState::DC_SOLO_INV2, NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY_INV2_PENDING},
    {NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY_INV2_PENDING, NKikimrBlobStorage::TGroupState::DC_SOLO_INV2},
    {NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY_INV2, NKikimrBlobStorage::TGroupState::DC_SOLO_INV2},
    {NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY_INV1, NKikimrBlobStorage::TGroupState::DC_FAILURE_INV1},
    {NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY_INV1_PENDING, NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY_INV1},
    {NKikimrBlobStorage::TGroupState::DC_FAILURE_INV2_PENDING, NKikimrBlobStorage::TGroupState::DC_SOLO_INV2_PENDING},
    {NKikimrBlobStorage::TGroupState::DC_FAILURE_INV1_PENDING, NKikimrBlobStorage::TGroupState::DC_SOLO_DISASTER},
    {NKikimrBlobStorage::TGroupState::DC_SOLO_INV2_PENDING, NKikimrBlobStorage::TGroupState::DC_SOLO_INV2},
    {NKikimrBlobStorage::TGroupState::DC_RECOVERY_INV1, NKikimrBlobStorage::TGroupState::NORMAL},
    {NKikimrBlobStorage::TGroupState::DC_RECOVERY, NKikimrBlobStorage::TGroupState::NORMAL},
    {NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY, NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY_INV1_PENDING},
};

constexpr bool IsAllowedMirror3dcGroupTransition(EMirror3dcGroupState from, EMirror3dcGroupState to) {
    for (const auto& edge : Mirror3dcAllowedGroupTransitions) {
        if (edge.From == from && edge.To == to) {
            return true;
        }
    }
    return false;
}

// Map the group state to I/X/R roles without changing physical realm identities.
constexpr std::optional<TMirror3dcGroupConfiguration> MakeMirror3dcGroupConfiguration(
        EMirror3dcGroupState state, const TMirror3dcFailRealmStates& roles) {
    const auto expected = GetMirror3dcRealmStates(state);
    if (!expected) {
        return std::nullopt;
    }
    auto sortedRoles = roles;
    std::sort(sortedRoles.begin(), sortedRoles.end());
    if (sortedRoles != *expected) {
        return std::nullopt;
    }
    return TMirror3dcGroupConfiguration{state, roles};
}

constexpr bool IsValidMirror3dcGroupConfiguration(const TMirror3dcGroupConfiguration& config) {
    return MakeMirror3dcGroupConfiguration(config.State, config.Realms).has_value();
}

constexpr bool IsAllowedMirror3dcGroupTransition(const TMirror3dcGroupConfiguration& before,
        const TMirror3dcGroupConfiguration& after) {
    if (!IsValidMirror3dcGroupConfiguration(before) || !IsValidMirror3dcGroupConfiguration(after)) {
        return false;
    }
    unsigned changedRealms = 0;
    for (unsigned realm = 0; realm < before.Realms.size(); ++realm) {
        if (before.Realms[realm] != after.Realms[realm]) {
            ++changedRealms;
        }
    }
    // A nontrivial permutation of physical realm roles changes at least two positions.
    if (changedRealms > 1) {
        return false;
    }
    if (before.State == after.State) {
        return changedRealms == 0;
    }
    return IsAllowedMirror3dcGroupTransition(before.State, after.State);
}

constexpr bool IsMirror3dcReplicationTarget(EMirror3dcGroupState state, EMirror3dcFailRealmState realm) {
    switch (state) {
        case NKikimrBlobStorage::TGroupState::NORMAL:
        case NKikimrBlobStorage::TGroupState::DC_RECOVERY:
        case NKikimrBlobStorage::TGroupState::DC_FAILURE_INV1_PENDING:
        case NKikimrBlobStorage::TGroupState::DC_FAILURE_INV2_PENDING:
        case NKikimrBlobStorage::TGroupState::DC_SOLO_INV2_PENDING:
        case NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY:
            return !IsExcluded(realm);
        case NKikimrBlobStorage::TGroupState::DC_RECOVERY_INV1:
        case NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY_INV1_PENDING:
        case NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY_INV2_PENDING:
        case NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY_INV1:
        case NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY_INV2:
            return IsRecovering(realm);
        default:
            return false;
    }
}

// Advance after replication has established the target guarantee in every required realm.
constexpr std::optional<TMirror3dcGroupConfiguration> RecoveringComplete(
        const TMirror3dcGroupConfiguration& before) {
    using TFailRealm = NKikimrBlobStorage::TDataCenterState;
    if (!IsValidMirror3dcGroupConfiguration(before)) {
        return std::nullopt;
    }
    EMirror3dcGroupState next;
    switch (before.State) {
        case NKikimrBlobStorage::TGroupState::DC_FAILURE_INV1_PENDING: next = NKikimrBlobStorage::TGroupState::DC_FAILURE_INV1; break;
        case NKikimrBlobStorage::TGroupState::DC_FAILURE_INV2_PENDING: next = NKikimrBlobStorage::TGroupState::DC_FAILURE_INV2; break;
        case NKikimrBlobStorage::TGroupState::DC_SOLO_INV2_PENDING: next = NKikimrBlobStorage::TGroupState::DC_SOLO_INV2; break;
        case NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY: next = NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY_INV1_PENDING; break;
        case NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY_INV1_PENDING: next = NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY_INV1; break;
        case NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY_INV2_PENDING: next = NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY_INV2; break;
        case NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY_INV1:
        case NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY_INV2: next = NKikimrBlobStorage::TGroupState::DC_FAILURE_INV1; break;
        case NKikimrBlobStorage::TGroupState::DC_RECOVERY:
        case NKikimrBlobStorage::TGroupState::DC_RECOVERY_INV1: next = NKikimrBlobStorage::TGroupState::NORMAL; break;
        default: return std::nullopt;
    }
    auto roles = before.Realms;
    if (next == NKikimrBlobStorage::TGroupState::NORMAL || next == NKikimrBlobStorage::TGroupState::DC_FAILURE_INV1) {
        for (auto& realm : roles) {
            if (IsRecovering(realm)) {
                realm = TFailRealm::INCLUDED;
            }
        }
    }
    return MakeMirror3dcGroupConfiguration(next, roles);
}

constexpr std::optional<TMirror3dcGroupConfiguration> ChangeMirror3dcRealmAvailability(
        const TMirror3dcGroupConfiguration& before, unsigned realm, bool excluded) {
    using TFailRealm = NKikimrBlobStorage::TDataCenterState;
    const bool validRealmIndex = realm < before.Realms.size();
    const bool validConfiguration = IsValidMirror3dcGroupConfiguration(before);
    if (!validRealmIndex || !validConfiguration) {
        return std::nullopt;
    }
    const bool currentlyExcluded = IsExcluded(before.Realms[realm]);
    const bool availabilityChanges = excluded != currentlyExcluded;
    if (!availabilityChanges) {
        return std::nullopt;
    }
    auto roles = before.Realms;
    roles[realm] = excluded ? TFailRealm::EXCLUDED : TFailRealm::RECOVERING;
    for (const auto& edge : Mirror3dcAllowedGroupTransitions) {
        if (edge.From == before.State) {
            const auto next = MakeMirror3dcGroupConfiguration(edge.To, roles);
            if (next && IsAllowedMirror3dcGroupTransition(before, *next)) {
                return next;
            }
        }
    }
    return std::nullopt;
}

constexpr std::optional<TMirror3dcGroupConfiguration> TakeDownFailRealm(
        const TMirror3dcGroupConfiguration& before, unsigned realm) {
    return ChangeMirror3dcRealmAvailability(before, realm, true);
}

constexpr std::optional<TMirror3dcGroupConfiguration> ReturnFailRealm(
        const TMirror3dcGroupConfiguration& before, unsigned realm) {
    return ChangeMirror3dcRealmAvailability(before, realm, false);
}

} // namespace NKikimr
