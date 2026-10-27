#pragma once

#include "blobstorage_groupinfo_mirror3dc_state.h"
#include "blobstorage_groupinfo_partlayout.h"
#include "blobstorage_groupinfo_sets.h"

#include <array>
#include <bit>

namespace NKikimr {

// Write acknowledgement and FULL use W; absence checks use Ensured (E).
class TMirror3dcWriteQuorumChecker {
    std::array<EMirror3dcFailRealmState, 3> Realms; // In subgroup part order.
    EMirror3dcLayout WriteLayout;
    EMirror3dcLayout EnsuredLayout;

    TMirror3dcWriteQuorumChecker(const std::array<EMirror3dcFailRealmState, 3>& realms,
            EMirror3dcLayout writeLayout, EMirror3dcLayout ensuredLayout)
        : Realms(realms)
        , WriteLayout(writeLayout)
        , EnsuredLayout(ensuredLayout)
    {}

    bool CheckCounts(const std::array<ui32, 3>& copies, EMirror3dcLayout layout) const {
        using R = NKikimrBlobStorage::TGroupInfo::TFailRealm;
        // Index is the required copy count; index 0 counts all realms of that role.
        std::array<ui32, 3> normal{};
        std::array<ui32, 3> recovering{};
        for (ui32 realm = 0; realm < 3; ++realm) {
            std::array<ui32, 3>* counts = nullptr;
            switch (Realms[realm]) {
                case R::N0:
                case R::N1:
                case R::N2:
                    counts = &normal;
                    break;
                case R::R:
                    counts = &recovering;
                    break;
                default: // F never contributes; unknown states are rejected by the factory.
                    continue;
            }
            for (ui32 minimum = 0; minimum <= 2; ++minimum) {
                (*counts)[minimum] += copies[realm] >= minimum;
            }
        }
        const auto check = [&](ui32 requiredRealms, ui32 requiredCopies) {
            if (normal[0] >= requiredRealms) {
                // A layout that fits in N must be satisfied entirely by N.
                return normal[requiredCopies] >= requiredRealms;
            }
            // Otherwise every N must participate; R supplies the missing realms.
            return normal[requiredCopies] == normal[0]
                && recovering[requiredCopies] >= requiredRealms - normal[0];
        };
        using L = EMirror3dcLayout;
        switch (layout) {
            case L::LAYOUT_111_22:
                return check(3, 1) || check(2, 2);
            case L::LAYOUT_11_2:
                return check(2, 1) || check(1, 2);
            case L::LAYOUT_11:
                return check(2, 1);
            case L::LAYOUT_22:
                return check(2, 2);
            case L::LAYOUT_0:
                return check(0, 0); // No minimum copy guarantee.
            case L::LAYOUT_1:
                return check(1, 1);
            case L::LAYOUT_2:
                return check(1, 2);
            case L::LAYOUT_111:
                return check(3, 1);
        }
        Y_ABORT("Invalid layout"); // The factory rejects unknown enum values.
    }

    static bool IsValidLayout(EMirror3dcLayout layout) {
        using L = EMirror3dcLayout;
        switch (layout) {
            case L::LAYOUT_111_22:
            case L::LAYOUT_11_2:
            case L::LAYOUT_11:
            case L::LAYOUT_22:
            case L::LAYOUT_0:
            case L::LAYOUT_1:
            case L::LAYOUT_2:
            case L::LAYOUT_111:
                return true;
        }
        return false;
    }

    static std::array<ui32, 3> CountParts(const TSubgroupPartLayout& parts) {
        std::array<ui32, 3> copies{};
        for (ui32 part = 0; part < 3; ++part) {
            // GetDisksWithPart(part): bit d means subgroup disk d has this part.
            // Zero-based part indices match subgroup realm columns:
            //   part 0 -> disks 0, 3, 6 -> 0b001001001
            //   part 1 -> disks 1, 4, 7 -> 0b010010010
            //   part 2 -> disks 2, 5, 8 -> 0b100100100
            // Binary masks are shown from bit 8 (left) to bit 0 (right).
            // Shifting by part aligns its three valid positions with bits 0, 3, 6.
            // The mask removes other positions; popcount counts distinct valid copies.
            copies[part] = std::popcount((parts.GetDisksWithPart(part) >> part) & 0b001001001);
        }
        return copies;
    }

public:
    static bool CheckFailModelForGroup(const TBlobStorageGroupInfo& info,
            const TBlobStorageGroupInfo::TGroupVDisks& failedDisks) {
        using G = TBlobStorageGroupInfo;
        using R = NKikimrBlobStorage::TGroupInfo::TFailRealm;
        const auto& topology = info.GetTopology();
        if (info.Type.GetErasure() != TBlobStorageGroupType::ErasureMirror3dc
                || topology.GetTotalFailRealmsNum() != 3 || failedDisks.GetTopology() != &topology) {
            return false;
        }
        const std::array<EMirror3dcFailRealmState, 3> realms = {
            info.GetFailRealmState(0), info.GetFailRealmState(1), info.GetFailRealmState(2)};
        const auto state = GetMirror3dcGroupState(realms[0], realms[1], realms[2]);
        if (!state) {
            return false;
        }
        const auto layouts = *GetMirror3dcGroupLayouts(*state);
        const TMirror3dcWriteQuorumChecker checker(realms, layouts.Write, layouts.Ensured);
        const auto failedDomains = G::TGroupFailDomains::CreateFromGroupDiskSet(
            failedDisks, G::TGroupFailDomains::EDiskCondition::ANY);
        std::array<ui32, 3> uncertain{};
        for (ui32 realm = 0; realm < 3; ++realm) {
            if (realms[realm] != R::F) {
                for (const auto& domain : topology.FailRealms[realm].FailDomains) {
                    uncertain[realm] += failedDomains[domain.FailDomainOrderNumber];
                }
            }
        }
        // A range is complete only if no blob's ensured layout can hide in failed domains.
        return !checker.CheckCounts(uncertain, layouts.Ensured);
    }

    static bool CheckQuorumForFullSync(const TBlobStorageGroupInfo& info,
            const TBlobStorageGroupInfo::TGroupVDisks& syncedDisks) {
        const TBlobStorageGroupInfo::TTopology& topology = info.GetTopology();
        if (info.Type.GetErasure() != TBlobStorageGroupType::ErasureMirror3dc
                || topology.GetTotalFailRealmsNum() != 3 || syncedDisks.GetTopology() != &topology) {
            return false;
        }
        const std::optional<EMirror3dcGroupState> state = GetMirror3dcGroupState(info.GetFailRealmState(0),
            info.GetFailRealmState(1), info.GetFailRealmState(2));
        if (!state) {
            return false;
        }
        if (GetMirror3dcGroupLayouts(*state)->Ensured != EMirror3dcLayout::LAYOUT_0) {
            return CheckFailModelForGroup(info, ~syncedDisks);
        }
        // E=0 cannot prove completeness by intersection. Collect every surviving normal source.
        bool hasNormalSource = false;
        for (ui32 realm = 0; realm < 3; ++realm) {
            if (info.IsFailedFailRealm(realm) || info.IsRecoveringFailRealm(realm)) {
                continue;
            }
            hasNormalSource = true;
            for (const TBlobStorageGroupInfo::TFailDomain& domain : topology.FailRealms[realm].FailDomains) {
                for (const TBlobStorageGroupInfo::TVDiskInfo& disk : domain.VDisks) {
                    if (!syncedDisks[disk.OrderNumber]) {
                        return false;
                    }
                }
            }
        }
        return hasNormalSource;
    }

    EMirror3dcLayout GetWriteLayout() const {
        return WriteLayout;
    }

    EMirror3dcLayout GetEnsuredLayout() const {
        return EnsuredLayout;
    }

    static std::optional<TMirror3dcWriteQuorumChecker> TryCreate(
            const std::array<EMirror3dcFailRealmState, 3>& realms,
            EMirror3dcLayout writeLayout, EMirror3dcLayout ensuredLayout) {
        if (!GetMirror3dcGroupState(realms[0], realms[1], realms[2])
                || !IsValidLayout(writeLayout) || !IsValidLayout(ensuredLayout)) {
            return std::nullopt;
        }
        return TMirror3dcWriteQuorumChecker(realms, writeLayout, ensuredLayout);
    }

    static std::optional<TMirror3dcWriteQuorumChecker> TryCreate(const TBlobStorageGroupInfo& info, ui32 blobHash) {
        const auto& topology = info.GetTopology();
        if (info.Type.GetErasure() != TBlobStorageGroupType::ErasureMirror3dc
                || topology.GetTotalFailRealmsNum() != 3) {
            return std::nullopt;
        }
        std::array<EMirror3dcFailRealmState, 3> realms;
        TBlobStorageGroupInfo::TOrderNums subgroup;
        topology.PickSubgroup(blobHash, subgroup);
        for (ui32 part = 0; part < 3; ++part) {
            const ui32 physicalRealm = topology.GetVDiskId(subgroup[part]).FailRealm;
            realms[part] = info.GetFailRealmState(physicalRealm);
        }
        const auto state = GetMirror3dcGroupState(realms[0], realms[1], realms[2]);
        if (!state) {
            return std::nullopt;
        }
        const auto layouts = *GetMirror3dcGroupLayouts(*state);
        return TMirror3dcWriteQuorumChecker(realms, layouts.Write, layouts.Ensured);
    }

    bool CheckQuorumForSubgroup(const TBlobStorageGroupInfo::TSubgroupVDisks& disks) const {
        if (disks.GetNumBits() != 9) {
            return false;
        }
        std::array<ui32, 3> copies{};
        for (ui32 disk = 0; disk < 9; ++disk) {
            copies[disk % 3] += disks[disk];
        }
        return CheckCounts(copies, WriteLayout);
    }

    bool CheckQuorumForLayout(const TSubgroupPartLayout& parts) const {
        return CheckCounts(CountParts(parts), WriteLayout);
    }

    ui32 GetPartsToResurrect(const TSubgroupPartLayout& parts, ui32 idxInSubgroup) const {
        using TFailRealm = NKikimrBlobStorage::TGroupInfo::TFailRealm;
        Y_ABORT_UNLESS(idxInSubgroup < 9);
        const std::optional<EMirror3dcGroupState> state = GetMirror3dcGroupState(Realms[0], Realms[1], Realms[2]);
        Y_ABORT_UNLESS(state);
        const EMirror3dcLayout durability = GetMirror3dcGroupLayouts(*state)->Durability;
        const std::array<ui32, 3> copies = CountParts(parts);
        const ui32 localRealm = idxInSubgroup % 3;
        const bool localRealmIsFailed = Realms[localRealm] == TFailRealm::F;
        const bool durabilitySatisfied = CheckCounts(copies, durability);
        const bool localPartAlreadyPresent = (parts.GetDisksWithPart(localRealm) & (1u << idxInSubgroup)) != 0;
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
        return copies[localRealm] < requiredCopies ? 1u << localRealm : 0;
    }

    // parts contains known copies; failedDisks includes errors and NOT_YET replies.
    // R participates only in layout alternatives that need it; F never contributes.
    // For E > 0, uncertainty that can hide E takes precedence over known copies.
    TBlobStorageGroupInfo::EBlobState GetBlobState(const TSubgroupPartLayout& parts,
            const TBlobStorageGroupInfo::TSubgroupVDisks& failedDisks) const {
        using G = TBlobStorageGroupInfo;
        using R = NKikimrBlobStorage::TGroupInfo::TFailRealm;
        if (failedDisks.GetNumBits() != 9) {
            return G::EBS_DISINTEGRATED;
        }
        const ui32 present = parts.GetDisksWithPart(0) | parts.GetDisksWithPart(1) | parts.GetDisksWithPart(2);
        bool hasCopy = false;
        std::array<ui32, 3> uncertain{};
        for (ui32 disk = 0; disk < 9; ++disk) {
            const ui32 realm = disk % 3;
            if (Realms[realm] != R::F) {
                // A mirror part can recover the blob even when stored in the wrong position.
                hasCopy |= bool(present & (1u << disk));
                uncertain[realm] += failedDisks[disk];
            }
        }

        if (EnsuredLayout == EMirror3dcLayout::LAYOUT_0 && hasCopy) {
            return CheckQuorumForLayout(parts) ? G::EBS_FULL : G::EBS_RECOVERABLE_FRAGMENTARY;
        } else if (EnsuredLayout == EMirror3dcLayout::LAYOUT_0) {
            return G::EBS_DISINTEGRATED;
        }

        // Absence is proved only if no minimum guaranteed layout fits on uncertain disks.
        // E is a lower bound, not the actual placement. With E=0, even all NODATA replies
        // cannot distinguish a blob that never existed from one whose copies were all lost.
        if (CheckCounts(uncertain, EnsuredLayout)) {
            return G::EBS_DISINTEGRATED;
        } else if (!hasCopy) {
            return G::EBS_UNRECOVERABLE_FRAGMENTARY;
        }

        return CheckQuorumForLayout(parts) ? G::EBS_FULL : G::EBS_RECOVERABLE_FRAGMENTARY;
    }
};

} // namespace NKikimr
