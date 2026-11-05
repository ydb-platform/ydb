#pragma once

#include <ydb/core/protos/blobstorage_base.pb.h>

#include <array>
#include <optional>

namespace NKikimr {

// State definitions and the complete state diagram live in blobstorage_base.proto.
using TMirror3dcGroupState = NKikimrBlobStorage::TMirror3dcGroupState;
using EMirror3dcGroupState = TMirror3dcGroupState::EState;

using EMirror3dcFailRealmState = NKikimrBlobStorage::TGroupInfo::TFailRealm::EState;
using TMirror3dcFailRealmStates = std::array<EMirror3dcFailRealmState, 3>;

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
    using TGroupState = TMirror3dcGroupState;
    using ELayout = EMirror3dcLayout;
    switch (state) {
        case TGroupState::NORMAL:
            return TMirror3dcGroupLayouts{ELayout::LAYOUT_111_22, ELayout::LAYOUT_111, ELayout::LAYOUT_111_22};
        case TGroupState::DC_FAILURE:
            return TMirror3dcGroupLayouts{ELayout::LAYOUT_22, ELayout::LAYOUT_0, ELayout::LAYOUT_11_2};
        case TGroupState::DC_FAILURE_INV1:
            return TMirror3dcGroupLayouts{ELayout::LAYOUT_22, ELayout::LAYOUT_11, ELayout::LAYOUT_11};
        case TGroupState::DC_FAILURE_INV2:
            return TMirror3dcGroupLayouts{ELayout::LAYOUT_22, ELayout::LAYOUT_22, ELayout::LAYOUT_22};
        case TGroupState::DC_RECOVERY:
            return TMirror3dcGroupLayouts{ELayout::LAYOUT_111_22, ELayout::LAYOUT_111, ELayout::LAYOUT_11_2};
        case TGroupState::DC_RECOVERY_INV1:
            return TMirror3dcGroupLayouts{ELayout::LAYOUT_111_22, ELayout::LAYOUT_111, ELayout::LAYOUT_11};
        case TGroupState::DC_SOLO_DISASTER:
            return TMirror3dcGroupLayouts{ELayout::LAYOUT_2, ELayout::LAYOUT_0, ELayout::LAYOUT_0};
        case TGroupState::DC_SOLO_INV1:
            return TMirror3dcGroupLayouts{ELayout::LAYOUT_2, ELayout::LAYOUT_1, ELayout::LAYOUT_1};
        case TGroupState::DC_SOLO_INV2:
            return TMirror3dcGroupLayouts{ELayout::LAYOUT_2, ELayout::LAYOUT_2, ELayout::LAYOUT_2};
        case TGroupState::DC_SOLO_RECOVERY:
            return TMirror3dcGroupLayouts{ELayout::LAYOUT_2, ELayout::LAYOUT_1, ELayout::LAYOUT_0};
        case TGroupState::DC_SOLO_RECOVERY_INV1:
            return TMirror3dcGroupLayouts{ELayout::LAYOUT_22, ELayout::LAYOUT_11, ELayout::LAYOUT_1};
        case TGroupState::DC_SOLO_RECOVERY_INV2:
            return TMirror3dcGroupLayouts{ELayout::LAYOUT_22, ELayout::LAYOUT_11, ELayout::LAYOUT_2};
        case TGroupState::DC_FAILURE_INV1_PENDING:
            return TMirror3dcGroupLayouts{ELayout::LAYOUT_22, ELayout::LAYOUT_11, ELayout::LAYOUT_11_2};
        case TGroupState::DC_FAILURE_INV2_PENDING:
            return TMirror3dcGroupLayouts{ELayout::LAYOUT_22, ELayout::LAYOUT_22, ELayout::LAYOUT_11};
        case TGroupState::DC_SOLO_INV2_PENDING:
            return TMirror3dcGroupLayouts{ELayout::LAYOUT_2, ELayout::LAYOUT_2, ELayout::LAYOUT_1};
        case TGroupState::DC_SOLO_RECOVERY_INV1_PENDING:
            return TMirror3dcGroupLayouts{ELayout::LAYOUT_2, ELayout::LAYOUT_1, ELayout::LAYOUT_1};
        case TGroupState::DC_SOLO_RECOVERY_INV2_PENDING:
            return TMirror3dcGroupLayouts{ELayout::LAYOUT_2, ELayout::LAYOUT_11, ELayout::LAYOUT_2};
        default:
            return std::nullopt;
    }
}

// Reads use the confirmed guarantee E, including while replication is pending.
constexpr EMirror3dcLayout GetMirror3dcReadLayout(EMirror3dcGroupState state) {
    return GetMirror3dcGroupLayouts(state)->Ensured;
}

constexpr std::optional<TMirror3dcFailRealmStates> GetMirror3dcRealmStates(EMirror3dcGroupState state) {
    using TGroupState = TMirror3dcGroupState;
    using TFailRealm = NKikimrBlobStorage::TGroupInfo::TFailRealm;
    switch (state) {
        case TGroupState::NORMAL:
            return TMirror3dcFailRealmStates{TFailRealm::N0, TFailRealm::N0, TFailRealm::N0};
        case TGroupState::DC_FAILURE:
            return TMirror3dcFailRealmStates{TFailRealm::N0, TFailRealm::N0, TFailRealm::F};
        case TGroupState::DC_FAILURE_INV1:
            return TMirror3dcFailRealmStates{TFailRealm::N1, TFailRealm::N1, TFailRealm::F};
        case TGroupState::DC_FAILURE_INV2:
            return TMirror3dcFailRealmStates{TFailRealm::N2, TFailRealm::N2, TFailRealm::F};
        case TGroupState::DC_RECOVERY:
            return TMirror3dcFailRealmStates{TFailRealm::N0, TFailRealm::N0, TFailRealm::R};
        case TGroupState::DC_RECOVERY_INV1:
            return TMirror3dcFailRealmStates{TFailRealm::N1, TFailRealm::N1, TFailRealm::R};
        case TGroupState::DC_SOLO_DISASTER:
            return TMirror3dcFailRealmStates{TFailRealm::N0, TFailRealm::F, TFailRealm::F};
        case TGroupState::DC_SOLO_INV1:
            return TMirror3dcFailRealmStates{TFailRealm::N1, TFailRealm::F, TFailRealm::F};
        case TGroupState::DC_SOLO_INV2:
            return TMirror3dcFailRealmStates{TFailRealm::N2, TFailRealm::F, TFailRealm::F};
        case TGroupState::DC_SOLO_RECOVERY:
            return TMirror3dcFailRealmStates{TFailRealm::N0, TFailRealm::R, TFailRealm::F};
        case TGroupState::DC_SOLO_RECOVERY_INV1:
            return TMirror3dcFailRealmStates{TFailRealm::N1, TFailRealm::R, TFailRealm::F};
        case TGroupState::DC_SOLO_RECOVERY_INV2:
            return TMirror3dcFailRealmStates{TFailRealm::N2, TFailRealm::R, TFailRealm::F};
        case TGroupState::DC_FAILURE_INV1_PENDING:
            return TMirror3dcFailRealmStates{TFailRealm::N0, TFailRealm::N0, TFailRealm::F};
        case TGroupState::DC_FAILURE_INV2_PENDING:
            return TMirror3dcFailRealmStates{TFailRealm::N1, TFailRealm::N1, TFailRealm::F};
        case TGroupState::DC_SOLO_INV2_PENDING:
            return TMirror3dcFailRealmStates{TFailRealm::N1, TFailRealm::F, TFailRealm::F};
        case TGroupState::DC_SOLO_RECOVERY_INV1_PENDING:
            return TMirror3dcFailRealmStates{TFailRealm::N1, TFailRealm::R, TFailRealm::F};
        case TGroupState::DC_SOLO_RECOVERY_INV2_PENDING:
            return TMirror3dcFailRealmStates{TFailRealm::N2, TFailRealm::R, TFailRealm::F};
        default:
            return std::nullopt;
    }
}

// Legacy migration only: realm states cannot distinguish PENDING states.
// Realm identities do not affect the group classification. Mixed N states use
// the weakest guarantee, e.g. N2/N0/F is DC_FAILURE, N2/N1/F is DC_FAILURE_INV1.
// Unsupported combinations (including no N realm, multiple R realms, or N2 in N/N/R) return nullopt.
constexpr std::optional<EMirror3dcGroupState> GetMirror3dcGroupState(
        EMirror3dcFailRealmState first, EMirror3dcFailRealmState second, EMirror3dcFailRealmState third) {
    using TFailRealm = NKikimrBlobStorage::TGroupInfo::TFailRealm;
    unsigned failures = 0;
    unsigned recovering = 0;
    unsigned minCopies = 2;
    bool hasTwoCopyGuarantee = false;
    const EMirror3dcFailRealmState realms[] = {first, second, third};
    for (const auto realm : realms) {
        switch (realm) {
            case TFailRealm::N0:
                minCopies = 0;
                break;
            case TFailRealm::N1:
                if (minCopies > 1) {
                    minCopies = 1;
                }
                break;
            case TFailRealm::N2:
                hasTwoCopyGuarantee = true;
                break;
            case TFailRealm::F:
                ++failures;
                break;
            case TFailRealm::R:
                ++recovering;
                break;
            default:
                return std::nullopt;
        }
    }
    using TGroupState = TMirror3dcGroupState;
    if (recovering > 1 || failures + recovering == 3) {
        return std::nullopt;
    }
    if (recovering == 1) {
        if (failures == 1) {
            return minCopies == 0 ? TGroupState::DC_SOLO_RECOVERY
                : minCopies == 1 ? TGroupState::DC_SOLO_RECOVERY_INV1 : TGroupState::DC_SOLO_RECOVERY_INV2;
        }
        // W accepts 1+1+1, so neither normal realm can retain an N2 guarantee.
        if (hasTwoCopyGuarantee) {
            return std::nullopt;
        }
        return minCopies == 0 ? TGroupState::DC_RECOVERY : TGroupState::DC_RECOVERY_INV1;
    }
    if (failures == 2) {
        return minCopies == 0 ? TGroupState::DC_SOLO_DISASTER
            : minCopies == 1 ? TGroupState::DC_SOLO_INV1 : TGroupState::DC_SOLO_INV2;
    }
    if (failures == 1) {
        return minCopies == 0 ? TGroupState::DC_FAILURE
            : minCopies == 1 ? TGroupState::DC_FAILURE_INV1 : TGroupState::DC_FAILURE_INV2;
    }
    // Stronger per-realm guarantees do not require another NORMAL group state.
    return TGroupState::NORMAL;
}

// A role change crosses an F/R boundary; changing only an N copy guarantee keeps the role.
constexpr bool IsMirror3dcFailRealmRoleChange(EMirror3dcFailRealmState before, EMirror3dcFailRealmState after) {
    using TFailRealm = NKikimrBlobStorage::TGroupInfo::TFailRealm;
    const bool beforeHasSpecialRole = before == TFailRealm::F || before == TFailRealm::R;
    const bool afterHasSpecialRole = after == TFailRealm::F || after == TFailRealm::R;
    return before != after && (beforeHasSpecialRole || afterHasSpecialRole);
}

// Logical roles are mapped to physical realms by the supplied tuple. All changes
// retain realm identities, including when N2 is downgraded on return to three DCs.
struct TMirror3dcGroupConfiguration {
    EMirror3dcGroupState State = TMirror3dcGroupState::NORMAL;
    TMirror3dcFailRealmStates Realms{};
    bool operator==(const TMirror3dcGroupConfiguration&) const = default;
};

enum class EMirror3dcTransitionKind { Admin, System };
struct TMirror3dcTransition {
    EMirror3dcGroupState From;
    EMirror3dcGroupState To;
    EMirror3dcTransitionKind Kind;
};

inline constexpr TMirror3dcTransition Mirror3dcAllowedGroupTransitions[] = {
    {TMirror3dcGroupState::NORMAL, TMirror3dcGroupState::DC_FAILURE, EMirror3dcTransitionKind::Admin},
    {TMirror3dcGroupState::DC_FAILURE, TMirror3dcGroupState::DC_FAILURE_INV1_PENDING, EMirror3dcTransitionKind::Admin},
    {TMirror3dcGroupState::DC_FAILURE_INV1, TMirror3dcGroupState::DC_FAILURE_INV2_PENDING, EMirror3dcTransitionKind::Admin},
    {TMirror3dcGroupState::DC_FAILURE_INV1, TMirror3dcGroupState::DC_SOLO_INV1, EMirror3dcTransitionKind::Admin},
    {TMirror3dcGroupState::DC_FAILURE_INV2, TMirror3dcGroupState::DC_SOLO_INV2, EMirror3dcTransitionKind::Admin},
    {TMirror3dcGroupState::DC_FAILURE, TMirror3dcGroupState::DC_RECOVERY, EMirror3dcTransitionKind::Admin},
    {TMirror3dcGroupState::DC_RECOVERY, TMirror3dcGroupState::DC_FAILURE, EMirror3dcTransitionKind::Admin},
    {TMirror3dcGroupState::DC_FAILURE_INV1, TMirror3dcGroupState::DC_RECOVERY_INV1, EMirror3dcTransitionKind::Admin},
    {TMirror3dcGroupState::DC_RECOVERY_INV1, TMirror3dcGroupState::DC_FAILURE_INV1, EMirror3dcTransitionKind::Admin},
    {TMirror3dcGroupState::DC_FAILURE_INV2, TMirror3dcGroupState::DC_RECOVERY_INV1, EMirror3dcTransitionKind::Admin},
    {TMirror3dcGroupState::DC_FAILURE, TMirror3dcGroupState::DC_SOLO_DISASTER, EMirror3dcTransitionKind::Admin},
    {TMirror3dcGroupState::DC_FAILURE_INV1_PENDING, TMirror3dcGroupState::DC_RECOVERY, EMirror3dcTransitionKind::Admin},
    {TMirror3dcGroupState::DC_FAILURE_INV2_PENDING, TMirror3dcGroupState::DC_RECOVERY_INV1, EMirror3dcTransitionKind::Admin},
    {TMirror3dcGroupState::DC_SOLO_DISASTER, TMirror3dcGroupState::DC_SOLO_RECOVERY, EMirror3dcTransitionKind::Admin},
    {TMirror3dcGroupState::DC_SOLO_RECOVERY, TMirror3dcGroupState::DC_SOLO_DISASTER, EMirror3dcTransitionKind::Admin},
    {TMirror3dcGroupState::DC_SOLO_RECOVERY_INV1, TMirror3dcGroupState::DC_SOLO_INV1, EMirror3dcTransitionKind::Admin},
    {TMirror3dcGroupState::DC_FAILURE_INV1_PENDING, TMirror3dcGroupState::DC_FAILURE_INV1, EMirror3dcTransitionKind::System},
    {TMirror3dcGroupState::DC_FAILURE_INV2_PENDING, TMirror3dcGroupState::DC_FAILURE_INV2, EMirror3dcTransitionKind::System},
    {TMirror3dcGroupState::DC_SOLO_RECOVERY_INV2_PENDING, TMirror3dcGroupState::DC_SOLO_RECOVERY_INV2, EMirror3dcTransitionKind::System},
    {TMirror3dcGroupState::DC_SOLO_RECOVERY_INV2, TMirror3dcGroupState::DC_FAILURE_INV1, EMirror3dcTransitionKind::System},
    {TMirror3dcGroupState::DC_SOLO_INV1, TMirror3dcGroupState::DC_SOLO_INV2_PENDING, EMirror3dcTransitionKind::Admin},
    {TMirror3dcGroupState::DC_SOLO_INV1, TMirror3dcGroupState::DC_SOLO_RECOVERY_INV1_PENDING, EMirror3dcTransitionKind::Admin},
    {TMirror3dcGroupState::DC_SOLO_RECOVERY_INV1_PENDING, TMirror3dcGroupState::DC_SOLO_INV1, EMirror3dcTransitionKind::Admin},
    {TMirror3dcGroupState::DC_SOLO_INV2, TMirror3dcGroupState::DC_SOLO_RECOVERY_INV2_PENDING, EMirror3dcTransitionKind::Admin},
    {TMirror3dcGroupState::DC_SOLO_RECOVERY_INV2_PENDING, TMirror3dcGroupState::DC_SOLO_INV2, EMirror3dcTransitionKind::Admin},
    {TMirror3dcGroupState::DC_SOLO_RECOVERY_INV2, TMirror3dcGroupState::DC_SOLO_INV2, EMirror3dcTransitionKind::Admin},
    {TMirror3dcGroupState::DC_SOLO_RECOVERY_INV1, TMirror3dcGroupState::DC_FAILURE_INV1, EMirror3dcTransitionKind::System},
    {TMirror3dcGroupState::DC_SOLO_RECOVERY_INV1_PENDING, TMirror3dcGroupState::DC_SOLO_RECOVERY_INV1, EMirror3dcTransitionKind::System},
    {TMirror3dcGroupState::DC_FAILURE_INV2_PENDING, TMirror3dcGroupState::DC_SOLO_INV2_PENDING, EMirror3dcTransitionKind::Admin},
    {TMirror3dcGroupState::DC_FAILURE_INV1_PENDING, TMirror3dcGroupState::DC_SOLO_DISASTER, EMirror3dcTransitionKind::Admin},
    {TMirror3dcGroupState::DC_SOLO_INV2_PENDING, TMirror3dcGroupState::DC_SOLO_INV2, EMirror3dcTransitionKind::System},
    {TMirror3dcGroupState::DC_RECOVERY_INV1, TMirror3dcGroupState::NORMAL, EMirror3dcTransitionKind::System},
    {TMirror3dcGroupState::DC_RECOVERY, TMirror3dcGroupState::NORMAL, EMirror3dcTransitionKind::System},
    {TMirror3dcGroupState::DC_SOLO_RECOVERY, TMirror3dcGroupState::DC_SOLO_RECOVERY_INV1_PENDING, EMirror3dcTransitionKind::System},
};

constexpr bool IsAllowedMirror3dcGroupTransition(EMirror3dcGroupState from, EMirror3dcGroupState to,
        EMirror3dcTransitionKind kind) {
    for (const auto& edge : Mirror3dcAllowedGroupTransitions) {
        if (edge.From == from && edge.To == to && edge.Kind == kind) {
            return true;
        }
    }
    return false;
}

// Normalize copy guarantees while keeping each realm's N/R/F role in place.
constexpr std::optional<TMirror3dcGroupConfiguration> MakeMirror3dcGroupConfiguration(
        EMirror3dcGroupState state, const TMirror3dcFailRealmStates& roles) {
    using TFailRealm = NKikimrBlobStorage::TGroupInfo::TFailRealm;
    const auto expected = GetMirror3dcRealmStates(state);
    if (!expected) {
        return std::nullopt;
    }
    const auto role = [](EMirror3dcFailRealmState realm) {
        return realm == TFailRealm::F ? 2 : realm == TFailRealm::R ? 1 : realm >= TFailRealm::N0 && realm <= TFailRealm::N2 ? 0 : -1;
    };
    TMirror3dcGroupConfiguration result{state, roles};
    std::array<bool, 3> used{};
    for (unsigned physical = 0; physical < 3; ++physical) {
        bool found = false;
        for (unsigned logical = 0; logical < 3; ++logical) {
            if (!used[logical] && role(roles[physical]) >= 0 && role(roles[physical]) == role((*expected)[logical])) {
                result.Realms[physical] = (*expected)[logical];
                used[logical] = true;
                found = true;
                break;
            }
        }
        if (!found) {
            return std::nullopt;
        }
    }
    return result;
}

constexpr bool IsValidMirror3dcGroupConfiguration(const TMirror3dcGroupConfiguration& config) {
    const auto normalized = MakeMirror3dcGroupConfiguration(config.State, config.Realms);
    if (!normalized) {
        return false;
    }
    for (unsigned realm = 0; realm < 3; ++realm) {
        if (config.Realms[realm] < normalized->Realms[realm]) {
            return false;
        }
    }
    return true;
}

constexpr bool IsAllowedMirror3dcGroupTransition(const TMirror3dcGroupConfiguration& before,
        const TMirror3dcGroupConfiguration& after) {
    if (!IsValidMirror3dcGroupConfiguration(before) || !IsValidMirror3dcGroupConfiguration(after)) {
        return false;
    }
    if (before == after) {
        return true;
    }
    unsigned roleChanges = 0;
    for (unsigned realm = 0; realm < 3; ++realm) {
        roleChanges += IsMirror3dcFailRealmRoleChange(before.Realms[realm], after.Realms[realm]);
    }
    return roleChanges <= 1 && (IsAllowedMirror3dcGroupTransition(before.State, after.State,
        EMirror3dcTransitionKind::Admin) || IsAllowedMirror3dcGroupTransition(before.State, after.State,
        EMirror3dcTransitionKind::System));
}

// Legacy configurations are classified once, before an authoritative state is persisted.
constexpr std::optional<TMirror3dcGroupConfiguration> MigrateMirror3dcGroupConfiguration(
        const TMirror3dcFailRealmStates& realms) {
    const auto state = GetMirror3dcGroupState(realms[0], realms[1], realms[2]);
    return state ? std::optional(TMirror3dcGroupConfiguration{*state, realms}) : std::nullopt;
}

constexpr bool IsMirror3dcReplicationTarget(EMirror3dcGroupState state, EMirror3dcFailRealmState realm) {
    using TGroupState = TMirror3dcGroupState;
    using TFailRealm = NKikimrBlobStorage::TGroupInfo::TFailRealm;
    switch (state) {
        case TGroupState::NORMAL:
        case TGroupState::DC_RECOVERY:
        case TGroupState::DC_FAILURE_INV1_PENDING:
        case TGroupState::DC_FAILURE_INV2_PENDING:
        case TGroupState::DC_SOLO_INV2_PENDING:
        case TGroupState::DC_SOLO_RECOVERY:
            return realm != TFailRealm::F;
        case TGroupState::DC_RECOVERY_INV1:
        case TGroupState::DC_SOLO_RECOVERY_INV1_PENDING:
        case TGroupState::DC_SOLO_RECOVERY_INV2_PENDING:
        case TGroupState::DC_SOLO_RECOVERY_INV1:
        case TGroupState::DC_SOLO_RECOVERY_INV2:
            return realm == TFailRealm::R;
        default:
            return false;
    }
}

constexpr std::optional<TMirror3dcGroupConfiguration> CompleteMirror3dcReplication(
        const TMirror3dcGroupConfiguration& before) {
    using TGroupState = TMirror3dcGroupState;
    using TFailRealm = NKikimrBlobStorage::TGroupInfo::TFailRealm;
    if (!IsValidMirror3dcGroupConfiguration(before)) {
        return std::nullopt;
    }
    EMirror3dcGroupState next;
    switch (before.State) {
        case TGroupState::DC_FAILURE_INV1_PENDING: next = TGroupState::DC_FAILURE_INV1; break;
        case TGroupState::DC_FAILURE_INV2_PENDING: next = TGroupState::DC_FAILURE_INV2; break;
        case TGroupState::DC_SOLO_INV2_PENDING: next = TGroupState::DC_SOLO_INV2; break;
        case TGroupState::DC_SOLO_RECOVERY: next = TGroupState::DC_SOLO_RECOVERY_INV1_PENDING; break;
        case TGroupState::DC_SOLO_RECOVERY_INV1_PENDING: next = TGroupState::DC_SOLO_RECOVERY_INV1; break;
        case TGroupState::DC_SOLO_RECOVERY_INV2_PENDING: next = TGroupState::DC_SOLO_RECOVERY_INV2; break;
        case TGroupState::DC_SOLO_RECOVERY_INV1:
        case TGroupState::DC_SOLO_RECOVERY_INV2: next = TGroupState::DC_FAILURE_INV1; break;
        case TGroupState::DC_RECOVERY:
        case TGroupState::DC_RECOVERY_INV1: next = TGroupState::NORMAL; break;
        default: return std::nullopt;
    }
    auto roles = before.Realms;
    if (next == TGroupState::NORMAL || next == TGroupState::DC_FAILURE_INV1) {
        for (auto& realm : roles) {
            if (realm == TFailRealm::R) {
                realm = TFailRealm::N1;
            }
        }
    }
    return MakeMirror3dcGroupConfiguration(next, roles);
}

constexpr std::optional<TMirror3dcGroupConfiguration> ChangeMirror3dcRealmAvailability(
        const TMirror3dcGroupConfiguration& before, unsigned realm, bool excluded) {
    using TFailRealm = NKikimrBlobStorage::TGroupInfo::TFailRealm;
    if (realm >= 3 || !IsValidMirror3dcGroupConfiguration(before)
            || (excluded ? before.Realms[realm] == TFailRealm::F : before.Realms[realm] != TFailRealm::F)) {
        return std::nullopt;
    }
    auto roles = before.Realms;
    roles[realm] = excluded ? TFailRealm::F : TFailRealm::R;
    for (const auto& edge : Mirror3dcAllowedGroupTransitions) {
        if (edge.From == before.State && edge.Kind == EMirror3dcTransitionKind::Admin) {
            const auto next = MakeMirror3dcGroupConfiguration(edge.To, roles);
            if (next && IsAllowedMirror3dcGroupTransition(before, *next)) {
                return next;
            }
        }
    }
    return std::nullopt;
}

} // namespace NKikimr
