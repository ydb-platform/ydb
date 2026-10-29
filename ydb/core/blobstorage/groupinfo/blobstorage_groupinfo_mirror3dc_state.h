#pragma once

#include <ydb/core/protos/blobstorage_base.pb.h>

#include <optional>

namespace NKikimr {

// Derived mirror-3-dc group states. The group state is determined by the failrealm
// states and their copy guarantees. The administrator selects I/X per realm.
// I = Include, X = Exclude: ADMINISTRATIVE states selected by the administrator.
// N0/N1/N2, F and R are INTERNAL states: NoFailure with a minimum copy count,
// Failure DC, and Recovery respectively. N0 does not mean that the realm is empty.
// N1/N2 mean at least one/two copies of every confirmed blob in that failrealm.
// E = Layout Ensurance: guaranteed layouts for confirmed blobs.
// D = Layout Durability: background replication target.
// W = Write Layout: layouts sufficient to acknowledge a new write.
// OR separates alternative layouts; + combines copy counts in distinct failrealms.
// Each box contains internal states, the administrative I/X mask, D and E.
// W is shared by each column and shown below it.
// Columns describe logical roles; transitions retain physical failrealm identities.
//
//     +---------------------------------+
// +-->|              NORMAL             |
// |   | N0/N0/N0                  I/I/I |------------------------------+
// |   | D: 1+1+1                        |                              |
// |   | E: 1+1+1 OR 2+2                 |                              |
// |   +---------------------------------+                              |
// |                                                                    |
// |                                                                    |
// |                                                                    |
// |                                                                    |       +-----------------------------------------------------------------------------------------------+
// |                                                                    v       v                                                                                               |
// |   +---------------------------------+             +----------------+-------+--------+             +---------------------------------+             +------------------------+--------+
// +<--|           DC_RECOVERY           |<----------->|            DC_FAILURE           |------------>|         DC_SOLO_DISASTER        |<----------->|         DC_SOLO_RECOVERY        |
// |   | N0/N0/R                   I/I/I |             | N0/N0/F                   I/I/X |             | N0/F/F                    I/X/X |             | N0/R/F                    I/I/X |
// |   | D: 1+1+1                        |             | D: 1+1                          |             | D: 1                            |             | D: 1+1                          |
// |   | E: 1+1 OR 2                     |             | E: 1+1 OR 2                     |------+      | E: 0                            |             | E: 0                            |
// |   +---------------------------------+             +----------------+----------------+      |      +----------------+----------------+             +----------------+----------------+
// |                                                                    |                       |                       |                                               |
// |                                                                    |                       | opt (required N1)     |                                               |
// |                                                                    |                       |                       |                                               |
// |                                                                    |                       |                       |                                               |
// |                                                                    |                       +---------------+       |                                               |
// |                                                                    |                                       |       |                                               |
// |                                                                    |       +-------------------------------=-------=-----------------------------------------------=-------+
// |                                                                    v       v                               v       v                                               v       |
// |   +---------------------------------+             +----------------+-------+--------+             +--------+-------+----------------+             +----------------+-------+--------+
// +<--|         DC_RECOVERY_INV1        |<----------->|         DC_FAILURE_INV1         |------------>|           DC_SOLO_INV1          |<----------->|      DC_SOLO_RECOVERY_INV1      |
// |   | N1/N1/R                   I/I/I |             | N1/N1/F                   I/I/X |             | N1/F/F                    I/X/X |             | N1/R/F                    I/I/X |
// |   | D: 1+1+1                        |             | D: 2+2                          |             | D: 2                            |             | D: 1+1                          |
// |   | E: 1+1                          |             | E: 1+1                          |------+      | E: 1                            |             | E: 1                            |
// |   +---------------------------------+             +----------------+-------+--------+      |      +----------------+----------------+             +---------------------------------+
// |                                                                    |       ^               |                       |
// |                                                                    |       |               | opt (required N2)     |
// |                                                                    |       |               |                       |
// |                                                                    |       |               |                       |
// |                                                                    |       |               +---------------+       |
// |                                                                    |       |                               |       |
// |                                                                    |       +-------------------------------=-------=-------------------------------------------------------+
// |                                                                    v                                       v       v                                                       |
// |   +---------------------------------+             +----------------+----------------+             +--------+-------+----------------+             +------------------------+--------+
// +<--|         DC_RECOVERY_INV2        |<----------->|         DC_FAILURE_INV2         |------------>|           DC_SOLO_INV2          |<----------->|      DC_SOLO_RECOVERY_INV2      |
//     | N2/N2/R                   I/I/I |             | N2/N2/F                   I/I/X |             | N2/F/F                    I/X/X |             | N2/R/F                    I/I/X |
//     | D: 1+1+1                        |             | D: 2+2                          |             | D: 2                            |             | D: 1+1                          |
//     | E: 1+1+1 OR 2+2                 |             | E: 2+2                          |             | E: 2                            |             | E: 2                            |
//     +---------------------------------+             +---------------------------------+             +---------------------------------+             +---------------------------------+
//
//     ADMIN: I/I/I                                    ADMIN: I/I/X                                    ADMIN: I/X/X                                    ADMIN: I/I/X
//     W: 1+1+1 OR 2+2                                 W: 2+2                                          W: 2                                            W: 2+2
//
// + marks turns/junctions; = is a crossing WITHOUT a connection.
// Optional (opt) transitions require the target guarantee in the selected surviving realm
// before completing the transition.
// Internal progress may change N/F/R, D and E without changing administrative I/X.
// A transition arrow does not itself replicate data; target guarantees must be established.
//
enum class EMirror3dcGroupState {
    NORMAL,                    // N0/N0/N0; I/I/I.

    DC_FAILURE,                // N0/N0/F; N1/N0/F; I/I/X.
    DC_FAILURE_INV1,           // N1/N1/F; N2/N1/F; I/I/X.
    DC_FAILURE_INV2,           // N2/N2/F; I/I/X.

    DC_RECOVERY,               // N0/N0/R; N1/N0/R; I/I/I.
    DC_RECOVERY_INV1,          // N1/N1/R; N2/N1/R; I/I/I.
    DC_RECOVERY_INV2,          // N2/N2/R; I/I/I.

    DC_SOLO_DISASTER,          // N0/F/F; I/X/X.
    DC_SOLO_INV1,              // N1/F/F; I/X/X.
    DC_SOLO_INV2,              // N2/F/F; I/X/X.

    DC_SOLO_RECOVERY,          // N0/R/F; I/I/X.
    DC_SOLO_RECOVERY_INV1,     // N1/R/F; I/I/X.
    DC_SOLO_RECOVERY_INV2,     // N2/R/F; I/I/X.
};

using EMirror3dcFailRealmState = NKikimrBlobStorage::TGroupInfo::TFailRealm::EState;

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

// Realm identities do not affect the group classification. Mixed N states use
// the weakest guarantee, e.g. N2/N0/F is DC_FAILURE, N2/N1/F is DC_FAILURE_INV1.
// Unsupported combinations (including no N realm or multiple R realms) return nullopt.
constexpr std::optional<EMirror3dcGroupState> GetMirror3dcGroupState(
        EMirror3dcFailRealmState first, EMirror3dcFailRealmState second, EMirror3dcFailRealmState third) {
    using R = NKikimrBlobStorage::TGroupInfo::TFailRealm;
    unsigned failures = 0;
    unsigned recovering = 0;
    unsigned minCopies = 2;
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
        return minCopies == 0 ? G::DC_RECOVERY
            : minCopies == 1 ? G::DC_RECOVERY_INV1 : G::DC_RECOVERY_INV2;
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

// Return W/D/E from the diagram. An invalid enum value has no layout policy.
constexpr std::optional<TMirror3dcGroupLayouts> GetMirror3dcGroupLayouts(EMirror3dcGroupState state) {
    using G = EMirror3dcGroupState;
    using L = EMirror3dcLayout;
    switch (state) {
        case G::NORMAL:
        case G::DC_RECOVERY_INV2:
            return TMirror3dcGroupLayouts{L::LAYOUT_111_22, L::LAYOUT_111, L::LAYOUT_111_22};
        case G::DC_FAILURE:
            return TMirror3dcGroupLayouts{L::LAYOUT_22, L::LAYOUT_11, L::LAYOUT_11_2};
        case G::DC_FAILURE_INV1:
            return TMirror3dcGroupLayouts{L::LAYOUT_22, L::LAYOUT_22, L::LAYOUT_11};
        case G::DC_FAILURE_INV2:
            return TMirror3dcGroupLayouts{L::LAYOUT_22, L::LAYOUT_22, L::LAYOUT_22};
        case G::DC_RECOVERY:
            return TMirror3dcGroupLayouts{L::LAYOUT_111_22, L::LAYOUT_111, L::LAYOUT_11_2};
        case G::DC_RECOVERY_INV1:
            return TMirror3dcGroupLayouts{L::LAYOUT_111_22, L::LAYOUT_111, L::LAYOUT_11};
        case G::DC_SOLO_DISASTER:
            return TMirror3dcGroupLayouts{L::LAYOUT_2, L::LAYOUT_1, L::LAYOUT_0};
        case G::DC_SOLO_INV1:
            return TMirror3dcGroupLayouts{L::LAYOUT_2, L::LAYOUT_2, L::LAYOUT_1};
        case G::DC_SOLO_INV2:
            return TMirror3dcGroupLayouts{L::LAYOUT_2, L::LAYOUT_2, L::LAYOUT_2};
        case G::DC_SOLO_RECOVERY:
            return TMirror3dcGroupLayouts{L::LAYOUT_22, L::LAYOUT_11, L::LAYOUT_0};
        case G::DC_SOLO_RECOVERY_INV1:
            return TMirror3dcGroupLayouts{L::LAYOUT_22, L::LAYOUT_11, L::LAYOUT_1};
        case G::DC_SOLO_RECOVERY_INV2:
            return TMirror3dcGroupLayouts{L::LAYOUT_22, L::LAYOUT_11, L::LAYOUT_2};
    }
    return std::nullopt;
}

} // namespace NKikimr
