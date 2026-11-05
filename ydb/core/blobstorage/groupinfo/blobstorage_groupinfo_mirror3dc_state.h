#pragma once

#include <ydb/core/protos/blobstorage_base.pb.h>

#include <array>
#include <optional>

namespace NKikimr {

// Mirror-3-dc group state diagram. The group state is authoritative.
// Realm roles: I = Included, X = Excluded, R = Recovering.
// Copy guarantees and pending progress are represented by the group state and D/W/E.
// E = Layout Ensurance: guaranteed layouts for confirmed blobs.
// D = Layout Durability: background replication target.
// W = Write Layout: layouts sufficient to acknowledge a new write.
// OR separates alternative layouts; + combines copy counts in distinct failrealms.
// Each box contains realm roles and D, W and E.
// Columns describe logical roles; transitions retain physical failrealm identities.
//
//     +---------------------------------+
// +-->|              NORMAL             |
// |   | I/I/I                           |------------------------------+
// |   | D: 1+1+1                        |                              |
// |   | W: 1+1+1 OR 2+2                 |                              |
// |   | E: 1+1+1 OR 2+2                 |                              |
// |   +---------------------------------+                              |
// |                                                                    |
// |                                                                    |
// |                                                                    |
// |                                                                    |
// |                                                                    |
// |                                                                    |
// |                                                                    |
// |                                                                    |
// |                                                                    |
// |                                                                    |       +-----------------------------------------------------------------------------------------------+
// |                                                                    v       v                                                                                               |
// |   +---------------------------------+             +----------------+-------+--------+             +---------------------------------+             +------------------------+--------+
// +<--|           DC_RECOVERY           |<----------->|            DC_FAILURE           |------------>|         DC_SOLO_DISASTER        |<----------->|         DC_SOLO_RECOVERY        |
// |   | I/I/R                           |             | I/I/X                           |             | I/X/X                           |             | I/R/X                           |
// |   | D: 1+1+1                        |             | D: 1+1                          |             | D: 1                            |             | D: 1+1                          |
// |   | W: 1+1+1 OR 2+2                 |             | W: 2+2                          |             | W: 2                            |             | W: 2+2                          |
// |   | E: 1+1 OR 2                     |             | E: 1+1 OR 2                     |             | E: 0                            |             | E: 0                            |
// |   +---------------------------------+             +----------------+----------------+             +----------------+----------------+             +----------------+----------------+
// |                                                                    |                                               |                                               |
// |                                                                    |                                               |                                               |
// |                                                                    v                                               |                                               |
// |                                                   +----------------+----------------+                              |                                               |
// |                                                   |     DC_FAILURE_INV1_PENDING     |                              |                                               |
// |                                                   | I/I/X                           |                              |                                               |
// |                                                   | D: 1+1                          |                              |                                               |
// |                                                   | W: 2+2                          |                              |                                               |
// |                                                   | E: 1+1 OR 2                     |                              |                                               |
// |                                                   +----------------+----------------+                              |                                               |
// |                                                                    |                                               |                                               |
// |                                                                    |                                               |                                               |
// |                                                                    |       +---------------------------------------=-----------------------------------------------=-------+
// |                                                                    v       v                                       v                                               v       |
// |   +---------------------------------+             +----------------+-------+--------+             +----------------+----------------+             +----------------+-------+--------+
// +<--|         DC_RECOVERY_INV1        |<----------->|         DC_FAILURE_INV1         |------------>|           DC_SOLO_INV1          |<----------->|      DC_SOLO_RECOVERY_INV1      |
//     | I/I/R                           |             | I/I/X                           |             | I/X/X                           |             | I/R/X                           |
//     | D: 1+1+1                        |<-----+      | D: 2+2                          |             | D: 2                            |             | D: 1+1                          |
//     | W: 1+1+1 OR 2+2                 |      |      | W: 2+2                          |             | W: 2                            |             | W: 2+2                          |
//     | E: 1+1                          |      |      | E: 1+1                          |<------+     | E: 1                            |             | E: 1                            |
//     +---------------------------------+      |      +----------------+-------+--------+       |     +----------------+----------------+             +---------------------------------+
//                                              |                       |                        |                      |
//                                              |                       |                        |                      |
//                                              |                       v                        |                      |
//                                              |      +----------------+----------------+       |                      |
//                                              |      |     DC_FAILURE_INV2_PENDING     |       |                      |
//                                              |      | I/I/X                           |       |                      |
//                                              |      | D: 2+2                          |       |                      |
//                                              |      | W: 2+2                          |       |                      |
//                                              |      | E: 1+1                          |       |                      |
//                                              |      +----------------+----------------+       |                      |
//                                              |                       |                        |                      |
//                                              |                       |                        |                      |
//                                              |                       |                        +----------------------=-------------------------------------------------------+
//                                              |                       v                                               v                                                       |
//                                              |      +----------------+----------------+             +----------------+----------------+             +------------------------+--------+
//                                              +------|         DC_FAILURE_INV2         |------------>|           DC_SOLO_INV2          |<----------->|      DC_SOLO_RECOVERY_INV2      |
//                                                     | I/I/X                           |             | I/X/X                           |             | I/R/X                           |
//                                                     | D: 2+2                          |             | D: 2                            |             | D: 1+1                          |
//                                                     | W: 2+2                          |             | W: 2                            |             | W: 2+2                          |
//                                                     | E: 2+2                          |             | E: 2                            |             | E: 2                            |
//                                                     +---------------------------------+             +---------------------------------+             +---------------------------------+
//
//
// + marks turns/junctions; = is a crossing WITHOUT a connection.
// Internal progress may change I/X/R, D and E without an administrator action.
// A transition arrow does not itself replicate data; target guarantees must be established.
//
enum class EMirror3dcGroupState {
    NORMAL = 0,
    DC_FAILURE = 1,
    DC_FAILURE_INV1 = 2,
    DC_FAILURE_INV2 = 3,
    DC_RECOVERY = 4,
    DC_RECOVERY_INV1 = 5,
    DC_SOLO_DISASTER = 6,
    DC_SOLO_INV1 = 7,
    DC_SOLO_INV2 = 8,
    DC_SOLO_RECOVERY = 9,
    DC_SOLO_RECOVERY_INV1 = 10,
    DC_SOLO_RECOVERY_INV2 = 11,
    DC_FAILURE_INV1_PENDING = 12,
    DC_FAILURE_INV2_PENDING = 13,
    DC_SOLO_INV2_PENDING = 14,
    DC_SOLO_RECOVERY_INV1_PENDING = 15,
    DC_SOLO_RECOVERY_INV2_PENDING = 16,
};

using EMirror3dcFailRealmState = NKikimrBlobStorage::TGroupInfo::TFailRealm::EState;
using TMirror3dcFailRealmStates = std::array<EMirror3dcFailRealmState, 3>;

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
    EMirror3dcLayout Write;
    EMirror3dcLayout Durability;
    EMirror3dcLayout Ensured; // Expected layout E; see GetMirror3dcReadLayout for confirmed guarantees.
};

constexpr std::optional<TMirror3dcGroupLayouts> GetMirror3dcGroupLayouts(EMirror3dcGroupState state) {
    using G = EMirror3dcGroupState;
    using L = EMirror3dcLayout;
    switch (state) {
        case G::NORMAL:
            return TMirror3dcGroupLayouts{L::LAYOUT_111_22, L::LAYOUT_111, L::LAYOUT_111_22};
        case G::DC_FAILURE:
            return TMirror3dcGroupLayouts{L::LAYOUT_22, L::LAYOUT_0, L::LAYOUT_11_2};
        case G::DC_FAILURE_INV1:
            return TMirror3dcGroupLayouts{L::LAYOUT_22, L::LAYOUT_11, L::LAYOUT_11};
        case G::DC_FAILURE_INV2:
            return TMirror3dcGroupLayouts{L::LAYOUT_22, L::LAYOUT_22, L::LAYOUT_22};
        case G::DC_RECOVERY:
            return TMirror3dcGroupLayouts{L::LAYOUT_111_22, L::LAYOUT_111, L::LAYOUT_11_2};
        case G::DC_RECOVERY_INV1:
            return TMirror3dcGroupLayouts{L::LAYOUT_111_22, L::LAYOUT_111, L::LAYOUT_11};
        case G::DC_SOLO_DISASTER:
            return TMirror3dcGroupLayouts{L::LAYOUT_2, L::LAYOUT_0, L::LAYOUT_0};
        case G::DC_SOLO_INV1:
            return TMirror3dcGroupLayouts{L::LAYOUT_2, L::LAYOUT_1, L::LAYOUT_1};
        case G::DC_SOLO_INV2:
            return TMirror3dcGroupLayouts{L::LAYOUT_2, L::LAYOUT_2, L::LAYOUT_2};
        case G::DC_SOLO_RECOVERY:
            return TMirror3dcGroupLayouts{L::LAYOUT_2, L::LAYOUT_11, L::LAYOUT_0};
        case G::DC_SOLO_RECOVERY_INV1:
            return TMirror3dcGroupLayouts{L::LAYOUT_22, L::LAYOUT_11, L::LAYOUT_1};
        case G::DC_SOLO_RECOVERY_INV2:
            return TMirror3dcGroupLayouts{L::LAYOUT_22, L::LAYOUT_11, L::LAYOUT_2};
        case G::DC_FAILURE_INV1_PENDING:
            return TMirror3dcGroupLayouts{L::LAYOUT_22, L::LAYOUT_11, L::LAYOUT_11_2};
        case G::DC_FAILURE_INV2_PENDING:
            return TMirror3dcGroupLayouts{L::LAYOUT_22, L::LAYOUT_22, L::LAYOUT_11};
        case G::DC_SOLO_INV2_PENDING:
            return TMirror3dcGroupLayouts{L::LAYOUT_2, L::LAYOUT_2, L::LAYOUT_2};
        case G::DC_SOLO_RECOVERY_INV1_PENDING:
            return TMirror3dcGroupLayouts{L::LAYOUT_2, L::LAYOUT_11, L::LAYOUT_1};
        case G::DC_SOLO_RECOVERY_INV2_PENDING:
            return TMirror3dcGroupLayouts{L::LAYOUT_2, L::LAYOUT_11, L::LAYOUT_2};
    }
    return std::nullopt;
}

// A pending two-copy target does not establish a second copy of old blobs yet.
constexpr EMirror3dcLayout GetMirror3dcReadLayout(EMirror3dcGroupState state) {
    return state == EMirror3dcGroupState::DC_SOLO_INV2_PENDING
        ? EMirror3dcLayout::LAYOUT_1 : GetMirror3dcGroupLayouts(state)->Ensured;
}

constexpr std::optional<TMirror3dcFailRealmStates> GetMirror3dcRealmStates(EMirror3dcGroupState state) {
    using G = EMirror3dcGroupState;
    using R = NKikimrBlobStorage::TGroupInfo::TFailRealm;
    switch (state) {
        case G::NORMAL: return TMirror3dcFailRealmStates{R::N0, R::N0, R::N0};
        case G::DC_FAILURE: return TMirror3dcFailRealmStates{R::N0, R::N0, R::F};
        case G::DC_FAILURE_INV1: return TMirror3dcFailRealmStates{R::N1, R::N1, R::F};
        case G::DC_FAILURE_INV2: return TMirror3dcFailRealmStates{R::N2, R::N2, R::F};
        case G::DC_RECOVERY: return TMirror3dcFailRealmStates{R::N0, R::N0, R::R};
        case G::DC_RECOVERY_INV1: return TMirror3dcFailRealmStates{R::N1, R::N1, R::R};
        case G::DC_SOLO_DISASTER: return TMirror3dcFailRealmStates{R::N0, R::F, R::F};
        case G::DC_SOLO_INV1: return TMirror3dcFailRealmStates{R::N1, R::F, R::F};
        case G::DC_SOLO_INV2: return TMirror3dcFailRealmStates{R::N2, R::F, R::F};
        case G::DC_SOLO_RECOVERY: return TMirror3dcFailRealmStates{R::N0, R::R, R::F};
        case G::DC_SOLO_RECOVERY_INV1: return TMirror3dcFailRealmStates{R::N1, R::R, R::F};
        case G::DC_SOLO_RECOVERY_INV2: return TMirror3dcFailRealmStates{R::N2, R::R, R::F};
        case G::DC_FAILURE_INV1_PENDING: return TMirror3dcFailRealmStates{R::N0, R::N0, R::F};
        case G::DC_FAILURE_INV2_PENDING: return TMirror3dcFailRealmStates{R::N1, R::N1, R::F};
        case G::DC_SOLO_INV2_PENDING: return TMirror3dcFailRealmStates{R::N1, R::F, R::F};
        case G::DC_SOLO_RECOVERY_INV1_PENDING: return TMirror3dcFailRealmStates{R::N1, R::R, R::F};
        case G::DC_SOLO_RECOVERY_INV2_PENDING: return TMirror3dcFailRealmStates{R::N2, R::R, R::F};
    }
    return std::nullopt;
}

// Legacy migration only: realm states cannot distinguish PENDING states.
// Realm identities do not affect the group classification. Mixed N states use
// the weakest guarantee, e.g. N2/N0/F is DC_FAILURE, N2/N1/F is DC_FAILURE_INV1.
// Unsupported combinations (including no N realm, multiple R realms, or N2 in N/N/R) return nullopt.
constexpr std::optional<EMirror3dcGroupState> GetMirror3dcGroupState(
        EMirror3dcFailRealmState first, EMirror3dcFailRealmState second, EMirror3dcFailRealmState third) {
    using R = NKikimrBlobStorage::TGroupInfo::TFailRealm;
    unsigned failures = 0;
    unsigned recovering = 0;
    unsigned minCopies = 2;
    bool hasTwoCopyGuarantee = false;
    const EMirror3dcFailRealmState realms[] = {first, second, third};
    for (const auto realm : realms) {
        switch (realm) {
            case R::N0:
                minCopies = 0;
                break;
            case R::N1:
                if (minCopies > 1) {
                    minCopies = 1;
                }
                break;
            case R::N2:
                hasTwoCopyGuarantee = true;
                break;
            case R::F:
                ++failures;
                break;
            case R::R:
                ++recovering;
                break;
            default:
                return std::nullopt;
        }
    }
    using G = EMirror3dcGroupState;
    if (recovering > 1 || failures + recovering == 3) {
        return std::nullopt;
    }
    if (recovering == 1) {
        if (failures == 1) {
            return minCopies == 0 ? G::DC_SOLO_RECOVERY
                : minCopies == 1 ? G::DC_SOLO_RECOVERY_INV1 : G::DC_SOLO_RECOVERY_INV2;
        }
        // W accepts 1+1+1, so neither normal realm can retain an N2 guarantee.
        if (hasTwoCopyGuarantee) {
            return std::nullopt;
        }
        return minCopies == 0 ? G::DC_RECOVERY : G::DC_RECOVERY_INV1;
    }
    if (failures == 2) {
        return minCopies == 0 ? G::DC_SOLO_DISASTER
            : minCopies == 1 ? G::DC_SOLO_INV1 : G::DC_SOLO_INV2;
    }
    if (failures == 1) {
        return minCopies == 0 ? G::DC_FAILURE
            : minCopies == 1 ? G::DC_FAILURE_INV1 : G::DC_FAILURE_INV2;
    }
    // Stronger per-realm guarantees do not require another NORMAL group state.
    return G::NORMAL;
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
    EMirror3dcGroupState State = EMirror3dcGroupState::NORMAL;
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
    {EMirror3dcGroupState::NORMAL, EMirror3dcGroupState::DC_FAILURE, EMirror3dcTransitionKind::Admin},
    {EMirror3dcGroupState::DC_FAILURE, EMirror3dcGroupState::DC_FAILURE_INV1_PENDING, EMirror3dcTransitionKind::Admin},
    {EMirror3dcGroupState::DC_FAILURE_INV1, EMirror3dcGroupState::DC_FAILURE_INV2_PENDING, EMirror3dcTransitionKind::Admin},
    {EMirror3dcGroupState::DC_FAILURE_INV1, EMirror3dcGroupState::DC_SOLO_INV1, EMirror3dcTransitionKind::Admin},
    {EMirror3dcGroupState::DC_FAILURE_INV2, EMirror3dcGroupState::DC_SOLO_INV2, EMirror3dcTransitionKind::Admin},
    {EMirror3dcGroupState::DC_FAILURE, EMirror3dcGroupState::DC_RECOVERY, EMirror3dcTransitionKind::Admin},
    {EMirror3dcGroupState::DC_RECOVERY, EMirror3dcGroupState::DC_FAILURE, EMirror3dcTransitionKind::Admin},
    {EMirror3dcGroupState::DC_FAILURE_INV1, EMirror3dcGroupState::DC_RECOVERY_INV1, EMirror3dcTransitionKind::Admin},
    {EMirror3dcGroupState::DC_RECOVERY_INV1, EMirror3dcGroupState::DC_FAILURE_INV1, EMirror3dcTransitionKind::Admin},
    {EMirror3dcGroupState::DC_FAILURE_INV2, EMirror3dcGroupState::DC_RECOVERY_INV1, EMirror3dcTransitionKind::Admin},
    {EMirror3dcGroupState::DC_FAILURE, EMirror3dcGroupState::DC_SOLO_DISASTER, EMirror3dcTransitionKind::Admin},
    {EMirror3dcGroupState::DC_FAILURE_INV1_PENDING, EMirror3dcGroupState::DC_RECOVERY, EMirror3dcTransitionKind::Admin},
    {EMirror3dcGroupState::DC_FAILURE_INV2_PENDING, EMirror3dcGroupState::DC_RECOVERY_INV1, EMirror3dcTransitionKind::Admin},
    {EMirror3dcGroupState::DC_SOLO_DISASTER, EMirror3dcGroupState::DC_SOLO_RECOVERY, EMirror3dcTransitionKind::Admin},
    {EMirror3dcGroupState::DC_SOLO_RECOVERY, EMirror3dcGroupState::DC_SOLO_DISASTER, EMirror3dcTransitionKind::Admin},
    {EMirror3dcGroupState::DC_SOLO_RECOVERY_INV1, EMirror3dcGroupState::DC_SOLO_INV1, EMirror3dcTransitionKind::Admin},
    {EMirror3dcGroupState::DC_FAILURE_INV1_PENDING, EMirror3dcGroupState::DC_FAILURE_INV1, EMirror3dcTransitionKind::System},
    {EMirror3dcGroupState::DC_FAILURE_INV2_PENDING, EMirror3dcGroupState::DC_FAILURE_INV2, EMirror3dcTransitionKind::System},
    {EMirror3dcGroupState::DC_SOLO_RECOVERY_INV2_PENDING, EMirror3dcGroupState::DC_SOLO_RECOVERY_INV2, EMirror3dcTransitionKind::System},
    {EMirror3dcGroupState::DC_SOLO_RECOVERY_INV2, EMirror3dcGroupState::DC_FAILURE_INV1, EMirror3dcTransitionKind::System},
    {EMirror3dcGroupState::DC_SOLO_INV1, EMirror3dcGroupState::DC_SOLO_INV2_PENDING, EMirror3dcTransitionKind::Admin},
    {EMirror3dcGroupState::DC_SOLO_INV1, EMirror3dcGroupState::DC_SOLO_RECOVERY_INV1_PENDING, EMirror3dcTransitionKind::Admin},
    {EMirror3dcGroupState::DC_SOLO_RECOVERY_INV1_PENDING, EMirror3dcGroupState::DC_SOLO_INV1, EMirror3dcTransitionKind::Admin},
    {EMirror3dcGroupState::DC_SOLO_INV2, EMirror3dcGroupState::DC_SOLO_RECOVERY_INV2_PENDING, EMirror3dcTransitionKind::Admin},
    {EMirror3dcGroupState::DC_SOLO_RECOVERY_INV2_PENDING, EMirror3dcGroupState::DC_SOLO_INV2, EMirror3dcTransitionKind::Admin},
    {EMirror3dcGroupState::DC_SOLO_RECOVERY_INV2, EMirror3dcGroupState::DC_SOLO_INV2, EMirror3dcTransitionKind::Admin},
    {EMirror3dcGroupState::DC_SOLO_RECOVERY_INV1, EMirror3dcGroupState::DC_FAILURE_INV1, EMirror3dcTransitionKind::System},
    {EMirror3dcGroupState::DC_SOLO_RECOVERY_INV1_PENDING, EMirror3dcGroupState::DC_SOLO_RECOVERY_INV1, EMirror3dcTransitionKind::System},
    {EMirror3dcGroupState::DC_FAILURE_INV2_PENDING, EMirror3dcGroupState::DC_SOLO_INV2_PENDING, EMirror3dcTransitionKind::Admin},
    {EMirror3dcGroupState::DC_FAILURE_INV1_PENDING, EMirror3dcGroupState::DC_SOLO_DISASTER, EMirror3dcTransitionKind::Admin},
    {EMirror3dcGroupState::DC_SOLO_INV2_PENDING, EMirror3dcGroupState::DC_SOLO_INV2, EMirror3dcTransitionKind::System},
    {EMirror3dcGroupState::DC_RECOVERY_INV1, EMirror3dcGroupState::NORMAL, EMirror3dcTransitionKind::System},
    {EMirror3dcGroupState::DC_RECOVERY, EMirror3dcGroupState::NORMAL, EMirror3dcTransitionKind::System},
    {EMirror3dcGroupState::DC_SOLO_RECOVERY, EMirror3dcGroupState::DC_SOLO_RECOVERY_INV1_PENDING, EMirror3dcTransitionKind::System},
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
    using R = NKikimrBlobStorage::TGroupInfo::TFailRealm;
    const auto expected = GetMirror3dcRealmStates(state);
    if (!expected) {
        return std::nullopt;
    }
    const auto role = [](EMirror3dcFailRealmState realm) {
        return realm == R::F ? 2 : realm == R::R ? 1 : realm >= R::N0 && realm <= R::N2 ? 0 : -1;
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
    using G = EMirror3dcGroupState;
    using R = NKikimrBlobStorage::TGroupInfo::TFailRealm;
    switch (state) {
        case G::NORMAL:
        case G::DC_RECOVERY:
        case G::DC_FAILURE_INV1_PENDING:
        case G::DC_FAILURE_INV2_PENDING:
        case G::DC_SOLO_INV2_PENDING:
        case G::DC_SOLO_RECOVERY:
            return realm != R::F;
        case G::DC_RECOVERY_INV1:
        case G::DC_SOLO_RECOVERY_INV1_PENDING:
        case G::DC_SOLO_RECOVERY_INV2_PENDING:
        case G::DC_SOLO_RECOVERY_INV1:
        case G::DC_SOLO_RECOVERY_INV2:
            return realm == R::R;
        default:
            return false;
    }
}

constexpr std::optional<TMirror3dcGroupConfiguration> CompleteMirror3dcReplication(
        const TMirror3dcGroupConfiguration& before) {
    using G = EMirror3dcGroupState;
    using R = NKikimrBlobStorage::TGroupInfo::TFailRealm;
    if (!IsValidMirror3dcGroupConfiguration(before)) {
        return std::nullopt;
    }
    G next;
    switch (before.State) {
        case G::DC_FAILURE_INV1_PENDING: next = G::DC_FAILURE_INV1; break;
        case G::DC_FAILURE_INV2_PENDING: next = G::DC_FAILURE_INV2; break;
        case G::DC_SOLO_INV2_PENDING: next = G::DC_SOLO_INV2; break;
        case G::DC_SOLO_RECOVERY: next = G::DC_SOLO_RECOVERY_INV1_PENDING; break;
        case G::DC_SOLO_RECOVERY_INV1_PENDING: next = G::DC_SOLO_RECOVERY_INV1; break;
        case G::DC_SOLO_RECOVERY_INV2_PENDING: next = G::DC_SOLO_RECOVERY_INV2; break;
        case G::DC_SOLO_RECOVERY_INV1:
        case G::DC_SOLO_RECOVERY_INV2: next = G::DC_FAILURE_INV1; break;
        case G::DC_RECOVERY:
        case G::DC_RECOVERY_INV1: next = G::NORMAL; break;
        default: return std::nullopt;
    }
    auto roles = before.Realms;
    if (next == G::NORMAL || next == G::DC_FAILURE_INV1) {
        for (auto& realm : roles) {
            if (realm == R::R) {
                realm = R::N1;
            }
        }
    }
    return MakeMirror3dcGroupConfiguration(next, roles);
}

constexpr std::optional<TMirror3dcGroupConfiguration> ChangeMirror3dcRealmAvailability(
        const TMirror3dcGroupConfiguration& before, unsigned realm, bool excluded) {
    using R = NKikimrBlobStorage::TGroupInfo::TFailRealm;
    if (realm >= 3 || !IsValidMirror3dcGroupConfiguration(before)
            || (excluded ? before.Realms[realm] == R::F : before.Realms[realm] != R::F)) {
        return std::nullopt;
    }
    auto roles = before.Realms;
    roles[realm] = excluded ? R::F : R::R;
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
