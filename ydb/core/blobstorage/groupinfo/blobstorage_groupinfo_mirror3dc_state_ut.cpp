#include "blobstorage_groupinfo_mirror3dc_state.h"

#include <array>

namespace NKikimr {
namespace {

using R = NKikimrBlobStorage::TGroupInfo::TFailRealm;
using G = EMirror3dcGroupState;
using L = EMirror3dcLayout;

struct TStateCase {
    std::array<EMirror3dcFailRealmState, 3> Realms;
    std::optional<G> Expected;
};

// All 35 unordered triples, including mixed guarantees and unsupported combinations.
constexpr TStateCase StateCases[] = {
    {{R::N0, R::N0, R::N0}, G::NORMAL},
    {{R::N0, R::N0, R::N1}, G::NORMAL},
    {{R::N0, R::N0, R::N2}, G::NORMAL},
    {{R::N0, R::N0, R::F}, G::DC_FAILURE},
    {{R::N0, R::N0, R::R}, G::DC_RECOVERY},
    {{R::N0, R::N1, R::N1}, G::NORMAL},
    {{R::N0, R::N1, R::N2}, G::NORMAL},
    {{R::N0, R::N1, R::F}, G::DC_FAILURE},
    {{R::N0, R::N1, R::R}, G::DC_RECOVERY},
    {{R::N0, R::N2, R::N2}, G::NORMAL},
    {{R::N0, R::N2, R::F}, G::DC_FAILURE},
    {{R::N0, R::N2, R::R}, G::DC_RECOVERY},
    {{R::N0, R::F, R::F}, G::DC_SOLO_DISASTER},
    {{R::N0, R::F, R::R}, G::DC_SOLO_RECOVERY},
    {{R::N0, R::R, R::R}, std::nullopt},
    {{R::N1, R::N1, R::N1}, G::NORMAL},
    {{R::N1, R::N1, R::N2}, G::NORMAL},
    {{R::N1, R::N1, R::F}, G::DC_FAILURE_INV1},
    {{R::N1, R::N1, R::R}, G::DC_RECOVERY_INV1},
    {{R::N1, R::N2, R::N2}, G::NORMAL},
    {{R::N1, R::N2, R::F}, G::DC_FAILURE_INV1},
    {{R::N1, R::N2, R::R}, G::DC_RECOVERY_INV1},
    {{R::N1, R::F, R::F}, G::DC_SOLO_INV1},
    {{R::N1, R::F, R::R}, G::DC_SOLO_RECOVERY_INV1},
    {{R::N1, R::R, R::R}, std::nullopt},
    {{R::N2, R::N2, R::N2}, G::NORMAL},
    {{R::N2, R::N2, R::F}, G::DC_FAILURE_INV2},
    {{R::N2, R::N2, R::R}, G::DC_RECOVERY_INV2},
    {{R::N2, R::F, R::F}, G::DC_SOLO_INV2},
    {{R::N2, R::F, R::R}, G::DC_SOLO_RECOVERY_INV2},
    {{R::N2, R::R, R::R}, std::nullopt},
    {{R::F, R::F, R::F}, std::nullopt},
    {{R::F, R::F, R::R}, std::nullopt},
    {{R::F, R::R, R::R}, std::nullopt},
    {{R::R, R::R, R::R}, std::nullopt},
};

constexpr bool CheckGroupStates() {
    constexpr R::EState states[] = {R::N0, R::N1, R::N2, R::F, R::R};
    for (const auto a : states) {
        for (const auto b : states) {
            for (const auto c : states) {
                std::array<R::EState, 3> sorted = {a, b, c};
                // Sort only the oracle key; call the actual function in the original order.
                for (unsigned i = 0; i < 3; ++i) {
                    for (unsigned j = i + 1; j < 3; ++j) {
                        if (sorted[i] > sorted[j]) {
                            const auto tmp = sorted[i];
                            sorted[i] = sorted[j];
                            sorted[j] = tmp;
                        }
                    }
                }
                unsigned matches = 0;
                for (const auto& test : StateCases) {
                    if (sorted == test.Realms) {
                        ++matches;
                        if (GetMirror3dcGroupState(a, b, c) != test.Expected) {
                            return false;
                        }
                    }
                }
                if (matches != 1) {
                    return false;
                }
            }
        }
    }
    return true;
}

struct TLayoutCase {
    G State;
    L Write;
    L Durability;
    L Ensured;
};

// Independent W/D/E expectations from the diagram, including every group state.
constexpr TLayoutCase LayoutCases[] = {
    {G::NORMAL, L::LAYOUT_111_22, L::LAYOUT_111, L::LAYOUT_111_22},
    {G::DC_FAILURE, L::LAYOUT_22, L::LAYOUT_11, L::LAYOUT_11_2},
    {G::DC_FAILURE_INV1, L::LAYOUT_22, L::LAYOUT_22, L::LAYOUT_11},
    {G::DC_FAILURE_INV2, L::LAYOUT_22, L::LAYOUT_22, L::LAYOUT_22},
    {G::DC_RECOVERY, L::LAYOUT_111_22, L::LAYOUT_111, L::LAYOUT_11_2},
    {G::DC_RECOVERY_INV1, L::LAYOUT_111_22, L::LAYOUT_111, L::LAYOUT_11},
    {G::DC_RECOVERY_INV2, L::LAYOUT_111_22, L::LAYOUT_111, L::LAYOUT_111_22},
    {G::DC_SOLO_DISASTER, L::LAYOUT_2, L::LAYOUT_1, L::LAYOUT_0},
    {G::DC_SOLO_INV1, L::LAYOUT_2, L::LAYOUT_2, L::LAYOUT_1},
    {G::DC_SOLO_INV2, L::LAYOUT_2, L::LAYOUT_2, L::LAYOUT_2},
    {G::DC_SOLO_RECOVERY, L::LAYOUT_22, L::LAYOUT_11, L::LAYOUT_0},
    {G::DC_SOLO_RECOVERY_INV1, L::LAYOUT_22, L::LAYOUT_11, L::LAYOUT_1},
    {G::DC_SOLO_RECOVERY_INV2, L::LAYOUT_22, L::LAYOUT_11, L::LAYOUT_2},
};

constexpr bool CheckLayouts() {
    for (const auto& test : LayoutCases) {
        const auto layouts = GetMirror3dcGroupLayouts(test.State);
        if (!layouts || layouts->Write != test.Write || layouts->Durability != test.Durability
                || layouts->Ensured != test.Ensured) {
            return false;
        }
    }
    return true;
}

static_assert(CheckGroupStates()); // All 5^3 = 125 ordered triples, evaluated at compile time.
static_assert(CheckLayouts());
static_assert(!GetMirror3dcGroupLayouts(static_cast<G>(-1)));

// Protobuf enums can contain unknown values; never classify them as NORMAL.
constexpr auto UnknownRealm = static_cast<R::EState>(5);
static_assert(!GetMirror3dcGroupState(UnknownRealm, R::N0, R::N0));
static_assert(!GetMirror3dcGroupState(R::N0, UnknownRealm, R::N0));
static_assert(!GetMirror3dcGroupState(R::N0, R::N0, UnknownRealm));

} // namespace
} // namespace NKikimr
