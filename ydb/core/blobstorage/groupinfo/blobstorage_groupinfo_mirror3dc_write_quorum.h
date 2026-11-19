#pragma once

#include "blobstorage_groupinfo_mirror3dc_state.h"
#include "blobstorage_groupinfo_partlayout.h"
#include "blobstorage_groupinfo_sets.h"

#include <array>
#include <bit>

namespace NKikimr {

// Write acknowledgement and FULL use W; absence checks use the confirmed read bound.
class TMirror3dcWriteQuorumChecker {
    std::array<EMirror3dcFailRealmState, 3> Realms; // In subgroup part order.
    EMirror3dcGroupState State;
    EMirror3dcLayout WriteLayout;
    EMirror3dcLayout EnsuredLayout;

    TMirror3dcWriteQuorumChecker(const std::array<EMirror3dcFailRealmState, 3>& realms,
            EMirror3dcLayout writeLayout, EMirror3dcLayout ensuredLayout, EMirror3dcGroupState state)
        : Realms(realms)
        , State(state)
        , WriteLayout(writeLayout)
        , EnsuredLayout(ensuredLayout)
    {}

    enum class EParticipants { Read, Write };

    bool CheckCounts(const std::array<ui32, 3>& copies, EMirror3dcLayout layout, EParticipants participants) const {
        // Index is the required copy count; index 0 counts all realms of that role.
        std::array<ui32, 3> normal{};
        std::array<ui32, 3> recovering{};
        for (ui32 realm = 0; realm < 3; ++realm) {
            EMirror3dcFailRealmState role = Realms[realm];
            bool participates = participants == EParticipants::Read ? IsReadable(role) : IsWritable(role);
            if (!participates) {
                continue;
            }
            std::array<ui32, 3>& counts = IsRecovering(role) ? recovering : normal;
            for (ui32 minimum = 0; minimum <= 2; ++minimum) {
                counts[minimum] += copies[realm] >= minimum;
            }
        }
        const auto check = [&](ui32 requiredRealms, ui32 requiredCopies) {
            if (normal[0] >= requiredRealms) {
                // A layout that fits in included realms must be satisfied entirely by them.
                return normal[requiredCopies] >= requiredRealms;
            }
            // Otherwise every included realm must participate; recovering realms supply the rest.
            return normal[requiredCopies] == normal[0]
                && recovering[requiredCopies] >= requiredRealms - normal[0];
        };
        switch (layout) {
            case EMirror3dcLayout::LAYOUT_111_22:
                return check(3, 1) || check(2, 2);
            case EMirror3dcLayout::LAYOUT_11_2:
                return check(2, 1) || check(1, 2);
            case EMirror3dcLayout::LAYOUT_11:
                return check(2, 1);
            case EMirror3dcLayout::LAYOUT_22:
                return check(2, 2);
            case EMirror3dcLayout::LAYOUT_1:
                return check(1, 1);
            case EMirror3dcLayout::LAYOUT_2:
                return check(1, 2);
            case EMirror3dcLayout::LAYOUT_111:
                return check(3, 1);
        }
        Y_ABORT("Invalid layout"); // The factory rejects unknown enum values.
    }

    bool HasStartupQuorum(const std::array<ui32, 3>& failed) const {
        if (CheckCounts(failed, EnsuredLayout, EParticipants::Read)) {
            return false;
        }

        // Check the least available blob subgroup: up to three failed domains per realm.
        std::array<ui32, 3> available{};
        for (ui32 realm = 0; realm < Realms.size(); ++realm) {
            available[realm] = 3 - Min<ui32>(3, failed[realm]);
            if (State == NKikimrBlobStorage::TGroupState::DC_RECOVERY_INV1 && IsRecovering(Realms[realm])) {
                available[realm] = 0; // Startup requires both Included realms.
            }
        }
        if (State == NKikimrBlobStorage::TGroupState::DC_SOLO_DISASTER) {
            // READ_ONLY still requires two surviving disks in its only Included realm.
            return CheckCounts(available, EMirror3dcLayout::LAYOUT_2, EParticipants::Write);
        }
        return CheckCounts(available, WriteLayout, EParticipants::Write);
    }

    static bool IsValidLayout(EMirror3dcLayout layout) {
        switch (layout) {
            case EMirror3dcLayout::LAYOUT_111_22:
            case EMirror3dcLayout::LAYOUT_11_2:
            case EMirror3dcLayout::LAYOUT_11:
            case EMirror3dcLayout::LAYOUT_22:
            case EMirror3dcLayout::LAYOUT_1:
            case EMirror3dcLayout::LAYOUT_2:
            case EMirror3dcLayout::LAYOUT_111:
                return true;
        }
        return false;
    }

    static std::array<ui32, 3> CountFailedDomainsByRealm(const TBlobStorageGroupInfo::TGroupVDisks& disks) {
        using TFailDomains = TBlobStorageGroupInfo::TGroupFailDomains;
        const TFailDomains domains = TFailDomains::CreateFromGroupDiskSet(disks, TFailDomains::EDiskCondition::ANY);
        std::array<ui32, 3> counts{};
        for (ui32 realm = 0; realm < counts.size(); ++realm) {
            for (const auto& domain : disks.GetTopology()->FailRealms[realm].FailDomains) {
                counts[realm] += domains[domain.FailDomainOrderNumber];
            }
        }
        return counts;
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
    static bool CanHideEnsuredLayout(const TMirror3dcGroupConfiguration& config,
            const TBlobStorageGroupInfo::TGroupVDisks& uncertainDisks) {
        const auto& topology = *uncertainDisks.GetTopology();
        if (topology.GType.GetErasure() != TBlobStorageGroupType::ErasureMirror3dc
                || topology.GetTotalFailRealmsNum() != 3 || !IsValidMirror3dcGroupConfiguration(config)) {
            return true;
        }
        const TMirror3dcGroupLayouts layouts = *GetMirror3dcGroupLayouts(config.State);
        const TMirror3dcWriteQuorumChecker checker(config.Realms, layouts.Write, layouts.Ensured, config.State);
        return checker.CheckCounts(CountFailedDomainsByRealm(uncertainDisks), layouts.Ensured, EParticipants::Read);
    }

    static bool CheckReadFailModelForGroup(const TBlobStorageGroupInfo& info,
            const TBlobStorageGroupInfo::TGroupVDisks& failedDisks) {
        if (info.GetTopology().GetTotalFailRealmsNum() != 3 || failedDisks.GetTopology() != &info.GetTopology()) {
            return CheckFailModelForGroup(info, failedDisks);
        }
        const auto& state = info.GetMirror3dcState();
        return !CanHideEnsuredLayout({state.State, state.FailRealmStates}, failedDisks);
    }

    static bool CheckFailModelForGroup(const TBlobStorageGroupInfo& info,
            const TBlobStorageGroupInfo::TGroupVDisks& failedDisks) {
        const auto& topology = info.GetTopology();
        if (info.Type.GetErasure() != TBlobStorageGroupType::ErasureMirror3dc || failedDisks.GetTopology() != &topology) {
            return false;
        }
        if (topology.GetTotalFailRealmsNum() != 3) {
            return topology.GetQuorumChecker().CheckFailModelForGroup(failedDisks);
        }
        const TMirror3dcGroupState& groupState = info.GetMirror3dcState();
        const TMirror3dcFailRealmStates& realms = groupState.FailRealmStates;
        EMirror3dcGroupState state = groupState.State;
        if (!IsValidMirror3dcGroupConfiguration({state, realms})) {
            return false;
        }
        TMirror3dcGroupLayouts layouts = *GetMirror3dcGroupLayouts(state);
        TMirror3dcWriteQuorumChecker checker(realms, layouts.Write, GetMirror3dcReadLayout(state), state);
        return checker.HasStartupQuorum(CountFailedDomainsByRealm(failedDisks));
    }

    static bool CheckQuorumForFullSync(const TBlobStorageGroupInfo& info,
            const TBlobStorageGroupInfo::TGroupVDisks& syncedDisks) {
        const TBlobStorageGroupInfo::TTopology& topology = info.GetTopology();
        if (info.Type.GetErasure() != TBlobStorageGroupType::ErasureMirror3dc
                || topology.GetTotalFailRealmsNum() != 3 || syncedDisks.GetTopology() != &topology) {
            return false;
        }
        const TMirror3dcGroupState& groupState = info.GetMirror3dcState();
        EMirror3dcGroupState state = groupState.State;
        if (!IsValidMirror3dcGroupConfiguration({state, groupState.FailRealmStates})) {
            return false;
        }
        return CheckFailModelForGroup(info, ~syncedDisks);
    }

    EMirror3dcLayout GetWriteLayout() const {
        return WriteLayout;
    }

    EMirror3dcLayout GetEnsuredLayout() const {
        return EnsuredLayout;
    }

    static std::optional<TMirror3dcWriteQuorumChecker> TryCreate(const TMirror3dcFailRealmStates& realms, EMirror3dcGroupState state, EMirror3dcLayout writeLayout, EMirror3dcLayout ensuredLayout) {
        if (!IsValidMirror3dcGroupConfiguration({state, realms}) || !IsValidLayout(writeLayout) || !IsValidLayout(ensuredLayout)) {
            return std::nullopt;
        }
        return TMirror3dcWriteQuorumChecker(realms, writeLayout, ensuredLayout, state);
    }

    static std::optional<TMirror3dcWriteQuorumChecker> TryCreate(const TBlobStorageGroupInfo& info, ui32 blobHash) {
        const auto& topology = info.GetTopology();
        if (info.Type.GetErasure() != TBlobStorageGroupType::ErasureMirror3dc
                || topology.GetTotalFailRealmsNum() != 3) {
            return std::nullopt;
        }
        const TMirror3dcGroupState& groupState = info.GetMirror3dcState();
        TMirror3dcFailRealmStates realms;
        TBlobStorageGroupInfo::TOrderNums subgroup;
        topology.PickSubgroup(blobHash, subgroup);
        for (ui32 part = 0; part < 3; ++part) {
            const ui32 physicalRealm = topology.GetVDiskId(subgroup[part]).FailRealm;
            realms[part] = groupState.FailRealmStates[physicalRealm];
        }
        EMirror3dcGroupState state = groupState.State;
        if (!IsValidMirror3dcGroupConfiguration({state, realms})) {
            return std::nullopt;
        }
        TMirror3dcGroupLayouts layouts = *GetMirror3dcGroupLayouts(state);
        return TMirror3dcWriteQuorumChecker(realms, layouts.Write, GetMirror3dcReadLayout(state), state);
    }

    bool CheckQuorumForSubgroup(const TBlobStorageGroupInfo::TSubgroupVDisks& disks) const {
        if (disks.GetNumBits() != 9) {
            return false;
        }
        std::array<ui32, 3> copies{};
        for (ui32 disk = 0; disk < 9; ++disk) {
            copies[disk % 3] += disks[disk];
        }
        return CheckCounts(copies, WriteLayout, EParticipants::Write);
    }

    bool CheckFailModelForSubgroup(const TBlobStorageGroupInfo::TSubgroupVDisks& failedDisks) const {
        if (failedDisks.GetNumBits() != 9) {
            return false;
        }
        std::array<ui32, 3> failed{};
        for (ui32 disk = 0; disk < 9; ++disk) {
            failed[disk % 3] += failedDisks[disk];
        }
        return HasStartupQuorum(failed);
    }

    bool CheckQuorumForLayout(const TSubgroupPartLayout& parts) const {
        // Existing copies in Leaving realms still contribute to the blob layout.
        if (HasMirror3dcLeavingRealm(State)) {
            return CheckCounts(CountParts(parts), WriteLayout, EParticipants::Read);
        }
        return CheckWriteQuorumForLayout(parts);
    }

    bool CheckWriteQuorumForLayout(const TSubgroupPartLayout& parts) const {
        return CheckCounts(CountParts(parts), WriteLayout, EParticipants::Write);
    }

    bool CheckEnsuredLayout(const TSubgroupPartLayout& parts, const TBlobStorageGroupInfo::TSubgroupVDisks& unknownDisks) const {
        if (unknownDisks.GetNumBits() != 9) {
            return false;
        }
        std::array<ui32, 3> possible = CountParts(parts);
        for (ui32 disk = 0; disk < 9; ++disk) {
            ui32 part = disk % 3;
            if (unknownDisks[disk] && !(parts.GetDisksWithPart(part) & (1u << disk))) {
                ++possible[part];
            }
        }
        return CheckCounts(possible, EnsuredLayout, EParticipants::Read);
    }

    ui32 GetPartsToResurrect(const TSubgroupPartLayout& parts, ui32 idxInSubgroup) const {
        Y_ABORT_UNLESS(idxInSubgroup < 9);
        EMirror3dcLayout durability = GetMirror3dcGroupLayouts(State)->Durability;
        std::array<ui32, 3> copies = CountParts(parts);
        ui32 localRealm = idxInSubgroup % 3;
        // Single-copy recovery states explicitly target R even when I already has a copy.
        bool durabilitySatisfied = !IsRecovering(Realms[localRealm]) && CheckCounts(copies, durability, EParticipants::Write);
        bool localPartAlreadyPresent = (parts.GetDisksWithPart(localRealm) & (1u << idxInSubgroup)) != 0;
        if (!IsMirror3dcReplicationTarget(State, Realms[localRealm]) || durabilitySatisfied || localPartAlreadyPresent) {
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
        ui32 diskIndexInRealm = idxInSubgroup / 3;
        if (diskIndexInRealm >= requiredCopies) {
            return 0;
        }
        ui32 includedRealms = 0;
        ui32 activeDisks = 0;
        for (ui32 realm = 0; realm < 3; ++realm) {
            includedRealms += IsIncluded(Realms[realm]);
            if (IsReadable(Realms[realm])) {
                activeDisks |= 0b001001001u << realm;
            }
        }
        bool includedRealmsCoverLayout = includedRealms >= requiredRealms;
        bool layoutRequiresMultipleRealms = requiredRealms > 1;
        bool skipRecoveringRealm = IsRecovering(Realms[localRealm])
            && layoutRequiresMultipleRealms && includedRealmsCoverLayout;
        ui32 present = parts.GetDisksWithPart(0) | parts.GetDisksWithPart(1) | parts.GetDisksWithPart(2);
        bool hasActiveSource = (present & activeDisks) != 0;
        if (!hasActiveSource || skipRecoveringRealm) {
            return 0;
        }
        return copies[localRealm] < requiredCopies ? 1u << localRealm : 0;
    }

    // parts contains known copies; failedDisks includes errors and NOT_YET replies.
    // I and P can supply data or hide E. A recovering realm can only contribute to W.
    // For E > 0, uncertainty that can hide E takes precedence over known copies.
    TBlobStorageGroupInfo::EBlobState GetBlobState(const TSubgroupPartLayout& parts,
            const TBlobStorageGroupInfo::TSubgroupVDisks& failedDisks) const {
        if (failedDisks.GetNumBits() != 9) {
            return TBlobStorageGroupInfo::EBS_DISINTEGRATED;
        }
        const ui32 present = parts.GetDisksWithPart(0) | parts.GetDisksWithPart(1) | parts.GetDisksWithPart(2);
        bool hasCopy = false;
        std::array<ui32, 3> uncertain{};
        for (ui32 disk = 0; disk < 9; ++disk) {
            const ui32 realm = disk % 3;
            if (IsReadable(Realms[realm])) {
                // A mirror part can recover the blob even when stored in the wrong position.
                hasCopy |= bool(present & (1u << disk));
                uncertain[realm] += failedDisks[disk];
            }
        }

        // Absence is proved only if no minimum guaranteed layout fits on uncertain disks.
        if (CheckCounts(uncertain, EnsuredLayout, EParticipants::Read)) {
            return TBlobStorageGroupInfo::EBS_DISINTEGRATED;
        } else if (!hasCopy) {
            return TBlobStorageGroupInfo::EBS_UNRECOVERABLE_FRAGMENTARY;
        }

        return CheckQuorumForLayout(parts) ? TBlobStorageGroupInfo::EBS_FULL : TBlobStorageGroupInfo::EBS_RECOVERABLE_FRAGMENTARY;
    }
};

} // namespace NKikimr
