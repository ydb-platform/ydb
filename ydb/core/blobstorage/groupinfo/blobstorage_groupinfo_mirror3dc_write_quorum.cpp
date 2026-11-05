#include "blobstorage_groupinfo_mirror3dc_write_quorum.h"

namespace NKikimr {
namespace {

enum class EReplicaRole : ui32 {
    Main = 0,
    Handoff1 = 1,
    Handoff2 = 2,
};

constexpr EReplicaRole GetReplicaRole(ui32 idxInSubgroup) {
    return static_cast<EReplicaRole>(idxInSubgroup / 3);
}

constexpr ui32 GetReplicaMask(ui32 realm, EReplicaRole role) {
    return 1u << (realm + 3 * static_cast<ui32>(role));
}

} // namespace

ui32 TMirror3dcWriteQuorumChecker::GetPartsToResurrect(const TSubgroupPartLayout& parts, ui32 idxInSubgroup) const {
    const std::optional<EMirror3dcGroupState> state = GetMirror3dcGroupState(Realms[0], Realms[1], Realms[2]);
    Y_ABORT_UNLESS(state);
    const EMirror3dcLayout durability = GetMirror3dcGroupLayouts(*state)->Durability;
    using TFailRealm = NKikimrBlobStorage::TGroupInfo::TFailRealm;
    Y_ABORT_UNLESS(idxInSubgroup < 9);
    const std::array<ui32, 3> copies = CountParts(parts);
    const ui32 localRealm = idxInSubgroup % 3;
    const EReplicaRole localRole = GetReplicaRole(idxInSubgroup);
    const bool localRealmIsFailed = Realms[localRealm] == TFailRealm::F;
    const bool durabilitySatisfied = CheckCounts(copies, durability);
    const bool localPartAlreadyPresent = (parts.GetDisksWithPart(localRealm) & GetReplicaMask(localRealm, localRole)) != 0;
    if (localRealmIsFailed || durabilitySatisfied || localPartAlreadyPresent) {
        return 0;
    }

    ui32 requiredRealms = 0;
    ui32 requiredCopies = 0;
    switch (durability) {
        case EMirror3dcLayout::LAYOUT_111:
            requiredRealms = 3;
            requiredCopies = 1;
            break;
        case EMirror3dcLayout::LAYOUT_11:
            requiredRealms = 2;
            requiredCopies = 1;
            break;
        case EMirror3dcLayout::LAYOUT_22:
            requiredRealms = 2;
            requiredCopies = 2;
            break;
        case EMirror3dcLayout::LAYOUT_1:
            requiredRealms = 1;
            requiredCopies = 1;
            break;
        case EMirror3dcLayout::LAYOUT_2:
            requiredRealms = 1;
            requiredCopies = 2;
            break;
        default:
            Y_ABORT("Unsupported mirror-3-dc durability layout");
    }
    ui32 normalRealms = 0;
    ui32 activeDisks = 0;
    for (ui32 realm = 0; realm < 3; ++realm) {
        if (Realms[realm] != TFailRealm::F) {
            activeDisks |= 0b001001001u << realm;
            normalRealms += Realms[realm] != TFailRealm::R;
        }
    }
    const ui32 present = parts.GetDisksWithPart(0) | parts.GetDisksWithPart(1) | parts.GetDisksWithPart(2);
    const bool hasActiveSource = (present & activeDisks) != 0;
    const bool localRealmIsRecovering = Realms[localRealm] == TFailRealm::R;
    const bool normalRealmsCanSatisfyDurability = normalRealms >= requiredRealms;
    const bool skipRecoveringRealm = localRealmIsRecovering && normalRealmsCanSatisfyDurability;
    if (!hasActiveSource || skipRecoveringRealm) {
        return 0;
    }
    if (copies[localRealm] >= requiredCopies || localRole == EReplicaRole::Handoff2) {
        return 0;
    }
    const ui32 localCopies = parts.GetDisksWithPart(localRealm);
    const bool handoff2Present = (localCopies & GetReplicaMask(localRealm, EReplicaRole::Handoff2)) != 0;
    // One-copy durability uses Main when the realm is empty. A second copy
    // may use Handoff1 only when Handoff2 does not already hold the part.
    if (localRole == EReplicaRole::Handoff1 && (requiredCopies == 1 || handoff2Present)) {
        return 0;
    }
    return 1u << localRealm;
}

} // namespace NKikimr
