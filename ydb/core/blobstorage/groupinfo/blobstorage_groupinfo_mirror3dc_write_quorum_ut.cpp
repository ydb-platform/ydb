#include "blobstorage_groupinfo_mirror3dc_write_quorum.h"

#include <library/cpp/testing/unittest/registar.h>

#include <algorithm>

using TDataCenterState = NKikimrBlobStorage::TDataCenterState;

namespace NKikimr {
namespace {

using TCopies = std::array<ui32, 3>;

TBlobStorageGroupInfo MakeInfo(const TMirror3dcGroupState& states) {
    TBlobStorageGroupInfo::TDynamicInfo dynamic(TGroupId::Zero(), 1);
    dynamic.Mirror3dcState = states;
    return TBlobStorageGroupInfo(TBlobStorageGroupInfo::TTopology(
        TBlobStorageGroupType::ErasureMirror3dc, 3, 4, 1), std::move(dynamic), "");
}

std::optional<TMirror3dcWriteQuorumChecker> TryCreateChecker(const TBlobStorageGroupInfo& info, ui32 hash) {
    return TMirror3dcWriteQuorumChecker::TryCreate(info, hash);
}

std::optional<TMirror3dcWriteQuorumChecker> TryCreateChecker(const TMirror3dcFailRealmStates& roles,
        EMirror3dcGroupState state, EMirror3dcLayout write, EMirror3dcLayout ensured) {
    return TMirror3dcWriteQuorumChecker::TryCreate(roles, state, write, ensured);
}

std::optional<TMirror3dcWriteQuorumChecker> TryCreateChecker(const TMirror3dcGroupState& states, EMirror3dcLayout write, EMirror3dcLayout ensured) {
    TBlobStorageGroupInfo info = MakeInfo(states);
    EMirror3dcGroupState state = info.GetMirror3dcState().State;
    return TMirror3dcWriteQuorumChecker::TryCreate(
        {info.GetFailRealmState(0), info.GetFailRealmState(1), info.GetFailRealmState(2)}, state, write, ensured);
}

void CheckCase(const TMirror3dcGroupState& states, const TCopies& copies, bool expected, std::optional<EMirror3dcLayout> layout = {}) {
    // Permute physical realm identities, then rotate the subgroup relative to them.
    std::array<ui32, 3> permutation = {0, 1, 2};
    do {
        TBlobStorageGroupInfo info = MakeInfo({states.State, {states.FailRealmStates[permutation[0]], states.FailRealmStates[permutation[1]], states.FailRealmStates[permutation[2]]}});
        for (ui32 hash = 0; hash < 3; ++hash) {
            TBlobStorageGroupInfo::TVDiskIds subgroup;
            info.PickSubgroup(hash, &subgroup, nullptr);
            std::optional<TMirror3dcWriteQuorumChecker> checker = layout
                ? TryCreateChecker(TMirror3dcFailRealmStates{
                    info.GetFailRealmState(subgroup[0].FailRealm),
                    info.GetFailRealmState(subgroup[1].FailRealm),
                    info.GetFailRealmState(subgroup[2].FailRealm)}, info.GetMirror3dcState().State, *layout,
                    GetMirror3dcGroupLayouts(states.State)->Ensured)
                : TryCreateChecker(info, hash);
            UNIT_ASSERT(checker);
            TBlobStorageGroupInfo::TSubgroupVDisks disks(&info.GetTopology());
            TSubgroupPartLayout parts;
            for (ui32 disk = 0; disk < 9; ++disk) {
                const ui32 realm = subgroup[disk].FailRealm;
                if (disk / 3 < copies[permutation[realm]]) {
                    disks += TBlobStorageGroupInfo::TSubgroupVDisks(&info.GetTopology(), disk);
                    parts.AddItem(disk, disk % 3, info.Type);
                }
            }
            UNIT_ASSERT_VALUES_EQUAL(checker->CheckQuorumForSubgroup(disks), expected);
            UNIT_ASSERT_VALUES_EQUAL(checker->CheckQuorumForLayout(parts), expected);
        }
    } while (std::next_permutation(permutation.begin(), permutation.end()));
}

void CheckBlobStateCase(const TMirror3dcGroupState& states, const TCopies& copies, const TCopies& failures,
        TBlobStorageGroupInfo::EBlobState expected, std::optional<EMirror3dcLayout> ensured = {}) {
    for (ui32 realm = 0; realm < 3; ++realm) {
        UNIT_ASSERT(copies[realm] + failures[realm] <= 3);
    }
    std::array<ui32, 3> permutation = {0, 1, 2};
    do {
        TBlobStorageGroupInfo info = MakeInfo({states.State, {states.FailRealmStates[permutation[0]], states.FailRealmStates[permutation[1]], states.FailRealmStates[permutation[2]]}});
        for (ui32 hash = 0; hash < 3; ++hash) {
            TBlobStorageGroupInfo::TVDiskIds subgroup;
            info.PickSubgroup(hash, &subgroup, nullptr);
            TMirror3dcGroupLayouts layouts = *GetMirror3dcGroupLayouts(states.State);
            std::optional<TMirror3dcWriteQuorumChecker> checker = ensured
                ? TryCreateChecker(TMirror3dcFailRealmStates{
                    info.GetFailRealmState(subgroup[0].FailRealm),
                    info.GetFailRealmState(subgroup[1].FailRealm),
                    info.GetFailRealmState(subgroup[2].FailRealm)}, info.GetMirror3dcState().State, layouts.Write, *ensured)
                : TryCreateChecker(info, hash);
            UNIT_ASSERT(checker);
            TBlobStorageGroupInfo::TSubgroupVDisks failed(&info.GetTopology());
            TSubgroupPartLayout parts;
            for (ui32 disk = 0; disk < 9; ++disk) {
                const ui32 realm = permutation[subgroup[disk].FailRealm];
                if (disk / 3 < copies[realm]) {
                    parts.AddItem(disk, disk % 3, info.Type);
                }
                if (disk / 3 >= 3 - failures[realm]) {
                    failed += TBlobStorageGroupInfo::TSubgroupVDisks(&info.GetTopology(), disk);
                }
            }
            UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(checker->GetBlobState(parts, failed)), static_cast<ui32>(expected));
        }
    } while (std::next_permutation(permutation.begin(), permutation.end()));
}

} // namespace

Y_UNIT_TEST_SUITE(TMirror3dcWriteQuorumTest) {

    Y_UNIT_TEST(FullSyncQuorumUsesIncludedSourceDuringSoloRecovery) {

        using TGroupVDisks = TBlobStorageGroupInfo::TGroupVDisks;
        for (ui32 sourceRealm = 0; sourceRealm < 3; ++sourceRealm) {
            TMirror3dcGroupState states{NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY_INV1, {TDataCenterState::EXCLUDED, TDataCenterState::EXCLUDED, TDataCenterState::EXCLUDED}};
            states.FailRealmStates[sourceRealm] = TDataCenterState::INCLUDED;
            states.FailRealmStates[(sourceRealm + 1) % 3] = TDataCenterState::RECOVERING;
            const TBlobStorageGroupInfo info = MakeInfo(states);
            const TBlobStorageGroupInfo::TTopology& topology = info.GetTopology();
            TGroupVDisks synced(&topology);
            UNIT_ASSERT(!TMirror3dcWriteQuorumChecker::CheckQuorumForFullSync(info, synced));
            // Replies from R and X cannot replace the surviving normal source.
            for (ui32 disk = 0; disk < info.GetTotalVDisksNum(); ++disk) {
                const TVDiskID vdiskId = info.GetVDiskId(disk);
                if (vdiskId.FailRealm != sourceRealm) {
                    synced |= TGroupVDisks(&topology, vdiskId);
                }
            }
            UNIT_ASSERT(!TMirror3dcWriteQuorumChecker::CheckQuorumForFullSync(info, synced));
            const TVector<TBlobStorageGroupInfo::TFailDomain>& domains = topology.FailRealms[sourceRealm].FailDomains;
            for (ui32 domain = 0; domain < domains.size(); ++domain) {
                for (const TBlobStorageGroupInfo::TVDiskInfo& disk : domains[domain].VDisks) {
                    synced |= TGroupVDisks(&topology, info.GetVDiskId(disk.OrderNumber));
                }
                UNIT_ASSERT_VALUES_EQUAL(TMirror3dcWriteQuorumChecker::CheckQuorumForFullSync(info, synced),
                    domain + 1 == domains.size());
            }
        }
    }

    Y_UNIT_TEST(OsirisRestoresDurabilityInsteadOfWriteQuorum) {

        const TBlobStorageGroupType type(TBlobStorageGroupType::ErasureMirror3dc);
        std::optional<TMirror3dcWriteQuorumChecker> checker = TryCreateChecker(
            {NKikimrBlobStorage::TGroupState::DC_RECOVERY, {TDataCenterState::INCLUDED, TDataCenterState::INCLUDED, TDataCenterState::RECOVERING}},
            EMirror3dcLayout::LAYOUT_111_22, EMirror3dcLayout::LAYOUT_111_22);
        UNIT_ASSERT(checker);
        TSubgroupPartLayout parts;
        parts.AddItem(0, 0, type);
        parts.AddItem(3, 0, type);
        parts.AddItem(1, 1, type);
        parts.AddItem(4, 1, type);
        UNIT_ASSERT(checker->CheckQuorumForLayout(parts)); // W is satisfied by 2+2.
        UNIT_ASSERT_VALUES_EQUAL(checker->GetPartsToResurrect(parts, 2), 1u << 2); // D requires 1+1+1.
        parts.AddItem(2, 2, type);
        UNIT_ASSERT_VALUES_EQUAL(checker->GetPartsToResurrect(parts, 5), 0);

        checker = TryCreateChecker(
            {NKikimrBlobStorage::TGroupState::DC_FAILURE_INV1_PENDING, {TDataCenterState::INCLUDED, TDataCenterState::INCLUDED, TDataCenterState::EXCLUDED}},
            EMirror3dcLayout::LAYOUT_22, EMirror3dcLayout::LAYOUT_11_2);
        TSubgroupPartLayout degraded;
        degraded.AddItem(0, 0, type);
        degraded.AddItem(1, 1, type);
        UNIT_ASSERT(!checker->CheckQuorumForLayout(degraded)); // W requires 2+2.
        UNIT_ASSERT_VALUES_EQUAL(checker->GetPartsToResurrect(degraded, 3), 0); // D only requires 1+1.
        UNIT_ASSERT_VALUES_EQUAL(checker->GetPartsToResurrect(degraded, 2), 0); // X is excluded.
    }

    Y_UNIT_TEST(TwoCopyDurabilityReplicatesBothIncludedRealms) {
        TBlobStorageGroupInfo info = MakeInfo({NKikimrBlobStorage::TGroupState::DC_FAILURE_INV2_PENDING,
            {TDataCenterState::INCLUDED, TDataCenterState::INCLUDED, TDataCenterState::EXCLUDED}});
        std::optional<TMirror3dcWriteQuorumChecker> checker = TryCreateChecker(info, 0);
        UNIT_ASSERT(checker);
        TBlobStorageGroupInfo::TVDiskIds subgroup;
        info.PickSubgroup(0, &subgroup, nullptr);
        TSubgroupPartLayout parts;
        for (ui32 part = 0; part < 3; ++part) {
            if (subgroup[part].FailRealm != 2) {
                parts.AddItem(part, part, info.Type);
            }
        }
        for (ui32 part = 0; part < 3; ++part) {
            const ui32 expected = subgroup[part].FailRealm == 2 ? 0 : 1u << part;
            UNIT_ASSERT_VALUES_EQUAL(checker->GetPartsToResurrect(parts, part + 3), expected);
        }
    }

    Y_UNIT_TEST(DurabilityLimitsTargetDiskIndexWithinRealm) {
        for (EMirror3dcGroupState state : {NKikimrBlobStorage::TGroupState::DC_FAILURE_INV1_PENDING, NKikimrBlobStorage::TGroupState::DC_FAILURE_INV2_PENDING}) {
            const ui32 requiredCopies = state == NKikimrBlobStorage::TGroupState::DC_FAILURE_INV1_PENDING ? 1 : 2;
            TBlobStorageGroupInfo info = MakeInfo({state,
                {TDataCenterState::INCLUDED, TDataCenterState::INCLUDED, TDataCenterState::EXCLUDED}});
            for (ui32 hash = 0; hash < 3; ++hash) {
                std::optional<TMirror3dcWriteQuorumChecker> checker = TryCreateChecker(info, hash);
                UNIT_ASSERT(checker);
                TBlobStorageGroupInfo::TVDiskIds subgroup;
                info.PickSubgroup(hash, &subgroup, nullptr);
                TSubgroupPartLayout parts;
                ui32 sourcePart = 0;
                while (subgroup[sourcePart].FailRealm != 0) {
                    ++sourcePart;
                }
                parts.AddItem(sourcePart, sourcePart, info.Type);
                // Each VDisk sees the same snapshot. Only the first D positions
                // in each included realm may schedule another copy.
                for (ui32 disk = 0; disk < 9; ++disk) {
                    const ui32 part = disk % 3;
                    const bool eligible = subgroup[disk].FailRealm != 2
                        && disk / 3 < requiredCopies && disk != sourcePart;
                    UNIT_ASSERT_VALUES_EQUAL(checker->GetPartsToResurrect(parts, disk), eligible ? 1u << part : 0);
                }
            }
        }
    }

    Y_UNIT_TEST(OsirisRequiresAnActiveSourceAndUsesRecoveringRealmWhenNeeded) {

        const TBlobStorageGroupType type(TBlobStorageGroupType::ErasureMirror3dc);
        const std::optional<TMirror3dcWriteQuorumChecker> checker = TryCreateChecker(
            {NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY, {TDataCenterState::INCLUDED, TDataCenterState::RECOVERING, TDataCenterState::EXCLUDED}},
            EMirror3dcLayout::LAYOUT_22, EMirror3dcLayout::LAYOUT_0);
        UNIT_ASSERT(checker);
        TSubgroupPartLayout parts;
        UNIT_ASSERT_VALUES_EQUAL(checker->GetPartsToResurrect(parts, 0), 0);
        parts.AddItem(2, 2, type); // A source in X cannot be used.
        UNIT_ASSERT_VALUES_EQUAL(checker->GetPartsToResurrect(parts, 0), 0);
        parts.AddItem(0, 0, type);
        UNIT_ASSERT_VALUES_EQUAL(checker->GetPartsToResurrect(parts, 1), 1u << 1);
        UNIT_ASSERT_VALUES_EQUAL(checker->GetPartsToResurrect(parts, 3), 0);
        parts.AddItem(1, 1, type);
        UNIT_ASSERT_VALUES_EQUAL(checker->GetPartsToResurrect(parts, 4), 0);
    }
    Y_UNIT_TEST(OneCopyDurabilityStillReplicatesRecoveringRealm) {
        for (const auto state : {NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY,
                NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY_INV1_PENDING}) {
            TBlobStorageGroupInfo info = MakeInfo({state, {TDataCenterState::INCLUDED, TDataCenterState::RECOVERING, TDataCenterState::EXCLUDED}});
            std::optional<TMirror3dcWriteQuorumChecker> checker = TryCreateChecker(info, 0);
            UNIT_ASSERT(checker);
            TBlobStorageGroupInfo::TVDiskIds subgroup;
            info.PickSubgroup(0, &subgroup, nullptr);
            TSubgroupPartLayout parts;
            ui32 recoveringPart = 3;
            for (ui32 part = 0; part < 3; ++part) {
                if (subgroup[part].FailRealm == 0) {
                    parts.AddItem(part, part, info.Type);
                } else if (subgroup[part].FailRealm == 1) {
                    recoveringPart = part;
                }
            }
            UNIT_ASSERT(recoveringPart < 3);
            UNIT_ASSERT_VALUES_EQUAL(checker->GetPartsToResurrect(parts, recoveringPart), 1u << recoveringPart);
            parts.AddItem(recoveringPart, recoveringPart, info.Type);
            UNIT_ASSERT_VALUES_EQUAL(checker->GetPartsToResurrect(parts, recoveringPart + 3), 0);
        }
    }

    Y_UNIT_TEST(PrimaryStateSelectsPolicyForIdenticalRealmTuple) {
        const TMirror3dcGroupState realms{NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY_INV2, {TDataCenterState::INCLUDED, TDataCenterState::RECOVERING, TDataCenterState::EXCLUDED}};
        const auto pending = MakeInfo({NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY_INV2_PENDING, realms.FailRealmStates});
        const auto ready = MakeInfo(realms);
        const auto pendingChecker = TryCreateChecker(pending, 0);
        const auto readyChecker = TryCreateChecker(ready, 0);
        UNIT_ASSERT(pendingChecker && readyChecker);
        UNIT_ASSERT(pendingChecker->GetWriteLayout() == EMirror3dcLayout::LAYOUT_2);
        UNIT_ASSERT(readyChecker->GetWriteLayout() == EMirror3dcLayout::LAYOUT_22);
        const auto soloPending = MakeInfo({NKikimrBlobStorage::TGroupState::DC_SOLO_INV2_PENDING, {TDataCenterState::INCLUDED, TDataCenterState::EXCLUDED, TDataCenterState::EXCLUDED}});
        const auto soloChecker = TryCreateChecker(soloPending, 0);
        UNIT_ASSERT(soloChecker);
        UNIT_ASSERT(soloChecker->GetEnsuredLayout() == EMirror3dcLayout::LAYOUT_1);
        UNIT_ASSERT(GetMirror3dcGroupLayouts(NKikimrBlobStorage::TGroupState::DC_SOLO_INV2_PENDING)->Ensured == EMirror3dcLayout::LAYOUT_1);
        TBlobStorageGroupInfo::TVDiskIds subgroup;
        soloPending.PickSubgroup(0, &subgroup, nullptr);
        TBlobStorageGroupInfo::TSubgroupVDisks uncertain(&soloPending.GetTopology());
        for (ui32 disk = 0; disk < 9; ++disk) {
            if (subgroup[disk].FailRealm == 0) {
                uncertain += TBlobStorageGroupInfo::TSubgroupVDisks(&soloPending.GetTopology(), disk);
                break;
            }
        }
        UNIT_ASSERT(soloChecker->GetBlobState({}, uncertain) == TBlobStorageGroupInfo::EBS_DISINTEGRATED);
    }

    Y_UNIT_TEST(NormalGroupFailuresMatchLegacy) {
        TBlobStorageGroupInfo info = MakeInfo({NKikimrBlobStorage::TGroupState::NORMAL, {TDataCenterState::INCLUDED, TDataCenterState::INCLUDED, TDataCenterState::INCLUDED}});
        const auto& legacy = info.GetTopology().GetQuorumChecker();
        for (ui32 mask = 0; mask < (1u << info.GetTotalVDisksNum()); ++mask) {
            const auto failed = TBlobStorageGroupInfo::TGroupVDisks::CreateFromMask(&info.GetTopology(), mask);
            UNIT_ASSERT_VALUES_EQUAL(TMirror3dcWriteQuorumChecker::CheckFailModelForGroup(info, failed),
                legacy.CheckFailModelForGroup(failed));
        }
    }

    Y_UNIT_TEST(GroupFailuresUseEnsuredRealmRoles) {
        struct TCase {
            TMirror3dcGroupState States;
            TCopies Failures;
            bool Expected;
        };
        const TCase cases[] = {
            {{NKikimrBlobStorage::TGroupState::DC_SOLO_INV1, {TDataCenterState::INCLUDED, TDataCenterState::EXCLUDED, TDataCenterState::EXCLUDED}}, {0, 4, 4}, true},
            {{NKikimrBlobStorage::TGroupState::DC_SOLO_INV1, {TDataCenterState::INCLUDED, TDataCenterState::EXCLUDED, TDataCenterState::EXCLUDED}}, {1, 0, 0}, false},
            {{NKikimrBlobStorage::TGroupState::DC_SOLO_INV2, {TDataCenterState::INCLUDED, TDataCenterState::EXCLUDED, TDataCenterState::EXCLUDED}}, {1, 4, 4}, true},
            {{NKikimrBlobStorage::TGroupState::DC_SOLO_INV2, {TDataCenterState::INCLUDED, TDataCenterState::EXCLUDED, TDataCenterState::EXCLUDED}}, {2, 0, 0}, false},
            {{NKikimrBlobStorage::TGroupState::DC_RECOVERY_INV1, {TDataCenterState::INCLUDED, TDataCenterState::INCLUDED, TDataCenterState::RECOVERING}}, {1, 1, 4}, false},
            {{NKikimrBlobStorage::TGroupState::DC_RECOVERY_INV1, {TDataCenterState::INCLUDED, TDataCenterState::INCLUDED, TDataCenterState::RECOVERING}}, {2, 2, 0}, false},
            {{NKikimrBlobStorage::TGroupState::NORMAL, {TDataCenterState::EXCLUDED, TDataCenterState::EXCLUDED, TDataCenterState::EXCLUDED}}, {}, false},
        };
        for (const auto& test : cases) {
            std::array<ui32, 3> permutation = {0, 1, 2};
            do {
                TBlobStorageGroupInfo info = MakeInfo({test.States.State, {test.States.FailRealmStates[permutation[0]], test.States.FailRealmStates[permutation[1]],
                    test.States.FailRealmStates[permutation[2]]}});
                TBlobStorageGroupInfo::TGroupVDisks failed(&info.GetTopology());
                for (const auto& disk : info.GetVDisks()) {
                    const auto id = info.GetVDiskId(disk.OrderNumber);
                    if (id.FailDomain < test.Failures[permutation[id.FailRealm]]) {
                        failed += TBlobStorageGroupInfo::TGroupVDisks(&info.GetTopology(), id);
                    }
                }
                UNIT_ASSERT_VALUES_EQUAL(TMirror3dcWriteQuorumChecker::CheckFailModelForGroup(info, failed),
                    test.Expected);
            } while (std::next_permutation(permutation.begin(), permutation.end()));
        }
    }

    Y_UNIT_TEST(NormalMatchesLegacyQuorum) {
        TBlobStorageGroupInfo info = MakeInfo({NKikimrBlobStorage::TGroupState::NORMAL, {TDataCenterState::INCLUDED, TDataCenterState::INCLUDED, TDataCenterState::INCLUDED}});
        // MakeInfo explicitly uses ErasureMirror3dc. Take its topology checker directly,
        // independently of any generation-aware checker selection in GroupInfo.
        const TBlobStorageGroupInfo::IQuorumChecker& legacyMirror3dc = info.GetTopology().GetQuorumChecker();
        std::optional<TMirror3dcWriteQuorumChecker> checker = TryCreateChecker(info, 0);
        UNIT_ASSERT(checker);
        // Only 512 reply masks; no multi-status Cartesian product.
        for (ui32 mask = 0; mask < (1u << 9); ++mask) {
            const auto disks = TBlobStorageGroupInfo::TSubgroupVDisks::CreateFromMask(&info.GetTopology(), mask);
            UNIT_ASSERT_VALUES_EQUAL(checker->CheckQuorumForSubgroup(disks),
                legacyMirror3dc.CheckQuorumForSubgroup(disks));
        }
    }

    Y_UNIT_TEST(NormalPartLayoutsMatchLegacyQuorum) {
        // NORMAL is I/I/I: W = 1+1+1 OR 2+2, as in ordinary mirror-3-dc.
        const TMirror3dcGroupState states = {NKikimrBlobStorage::TGroupState::NORMAL, {TDataCenterState::INCLUDED, TDataCenterState::INCLUDED, TDataCenterState::INCLUDED}};
        TBlobStorageGroupInfo info = MakeInfo(states);
        // Compare with the ordinary ErasureMirror3dc topology checker, not GroupInfo's default.
        const TBlobStorageGroupInfo::IQuorumChecker& legacyMirror3dc = info.GetTopology().GetQuorumChecker();
        const TBlobStorageGroupInfo::TSubgroupVDisks noFailures(&info.GetTopology());
        for (ui32 hash = 0; hash < 3; ++hash) {
            std::optional<TMirror3dcWriteQuorumChecker> checker = TryCreateChecker(info, hash);
            UNIT_ASSERT(checker);
            // Every subset of the nine valid part placements, including empty and full.
            for (ui32 mask = 0; mask < (1u << 9); ++mask) {
                const auto disks = TBlobStorageGroupInfo::TSubgroupVDisks::CreateFromMask(&info.GetTopology(), mask);
                TSubgroupPartLayout parts;
                for (ui32 disk = 0; disk < 9; ++disk) {
                    if (disks[disk]) {
                        parts.AddItem(disk, disk % 3, info.Type);
                    }
                }
                UNIT_ASSERT(checker->GetBlobState(parts, noFailures) == legacyMirror3dc.GetBlobState(parts, noFailures));
                const bool expected = legacyMirror3dc.CheckQuorumForSubgroup(disks);
                UNIT_ASSERT_VALUES_EQUAL(checker->CheckQuorumForSubgroup(disks), expected);
                UNIT_ASSERT_VALUES_EQUAL(checker->CheckQuorumForLayout(parts), expected);
                UNIT_ASSERT_VALUES_EQUAL(checker->CheckQuorumForLayout(parts),
                    legacyMirror3dc.GetBlobState(parts, noFailures) == TBlobStorageGroupInfo::EBS_FULL);
            }
        }
    }

    Y_UNIT_TEST(NormalMixedRepliesMatchLegacy) {
        // Per realm: {correct copies, uncertain replies}; the remaining replies are NODATA.
        // Disk identities within a realm are symmetric: 10^3 count combinations suffice.
        constexpr std::array<std::array<ui32, 2>, 10> replies = {{
            {0, 0}, {0, 1}, {0, 2}, {0, 3}, {1, 0},
            {1, 1}, {1, 2}, {2, 0}, {2, 1}, {3, 0},
        }};
        const TMirror3dcGroupState states = {NKikimrBlobStorage::TGroupState::NORMAL, {TDataCenterState::INCLUDED, TDataCenterState::INCLUDED, TDataCenterState::INCLUDED}};
        TBlobStorageGroupInfo info = MakeInfo(states);
        const TBlobStorageGroupInfo::IQuorumChecker& legacy = info.GetTopology().GetQuorumChecker();
        for (ui32 hash = 0; hash < 3; ++hash) {
            std::optional<TMirror3dcWriteQuorumChecker> checker = TryCreateChecker(info, hash);
            UNIT_ASSERT(checker);
            ui32 matching = 0;
            for (const auto& a : replies) {
                for (const auto& b : replies) {
                    for (const auto& c : replies) {
                        const std::array counts = {a, b, c};
                        TSubgroupPartLayout parts;
                        TBlobStorageGroupInfo::TSubgroupVDisks present(&info.GetTopology());
                        TBlobStorageGroupInfo::TSubgroupVDisks failed(&info.GetTopology());
                        for (ui32 disk = 0; disk < 9; ++disk) {
                            const auto& count = counts[disk % 3];
                            if (disk / 3 < count[0]) {
                                parts.AddItem(disk, disk % 3, info.Type);
                                present += TBlobStorageGroupInfo::TSubgroupVDisks(&info.GetTopology(), disk);
                            }
                            if (disk / 3 >= 3 - count[1]) {
                                failed += TBlobStorageGroupInfo::TSubgroupVDisks(&info.GetTopology(), disk);
                            }
                        }
                        const auto actual = checker->GetBlobState(parts, failed);
                        const auto old = legacy.GetBlobState(parts, failed);
                        // W = 1+1+1 OR 2+2: both positive and still-attainable quorums match.
                        UNIT_ASSERT_VALUES_EQUAL(checker->CheckQuorumForSubgroup(present),
                            legacy.CheckQuorumForSubgroup(present));
                        UNIT_ASSERT_VALUES_EQUAL(checker->CheckQuorumForSubgroup(~failed),
                            legacy.CheckQuorumForSubgroup(~failed));
                        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(actual), static_cast<ui32>(old));
                        ++matching;
                    }
                }
            }
            UNIT_ASSERT_VALUES_EQUAL(matching, 1000);
        }
    }

    Y_UNIT_TEST(NormalMisplacedPartsDifferFromLegacyFull) {
        TBlobStorageGroupInfo info = MakeInfo({NKikimrBlobStorage::TGroupState::NORMAL, {TDataCenterState::INCLUDED, TDataCenterState::INCLUDED, TDataCenterState::INCLUDED}});
        const TBlobStorageGroupInfo::IQuorumChecker& legacy = info.GetTopology().GetQuorumChecker();
        std::optional<TMirror3dcWriteQuorumChecker> checker = TryCreateChecker(info, 0);
        UNIT_ASSERT(checker);
        TSubgroupPartLayout parts;
        for (ui32 disk = 0; disk < 3; ++disk) {
            parts.AddItem(disk, (disk + 1) % 3, info.Type);
        }
        const TBlobStorageGroupInfo::TSubgroupVDisks noFailures(&info.GetTopology());
        // Legacy sees 1+1+1 involved disks; W here counts only correctly placed parts.
        UNIT_ASSERT(legacy.GetBlobState(parts, noFailures) == TBlobStorageGroupInfo::EBS_FULL);
        UNIT_ASSERT(checker->GetBlobState(parts, noFailures) == TBlobStorageGroupInfo::EBS_RECOVERABLE_FRAGMENTARY);
    }

    Y_UNIT_TEST(BlobCompletenessUsesWriteInsteadOfEnsured) {
        // W = 2+2, E = 1+1 OR 2: meeting either minimum guarantee is not enough for FULL.
        CheckBlobStateCase({NKikimrBlobStorage::TGroupState::DC_FAILURE, {TDataCenterState::INCLUDED, TDataCenterState::INCLUDED, TDataCenterState::EXCLUDED}}, {1, 1, 0}, {}, TBlobStorageGroupInfo::EBS_RECOVERABLE_FRAGMENTARY);
        CheckBlobStateCase({NKikimrBlobStorage::TGroupState::DC_FAILURE, {TDataCenterState::INCLUDED, TDataCenterState::INCLUDED, TDataCenterState::EXCLUDED}}, {2, 0, 0}, {}, TBlobStorageGroupInfo::EBS_RECOVERABLE_FRAGMENTARY);
        CheckBlobStateCase({NKikimrBlobStorage::TGroupState::DC_FAILURE, {TDataCenterState::INCLUDED, TDataCenterState::INCLUDED, TDataCenterState::EXCLUDED}}, {1, 0, 0}, {}, TBlobStorageGroupInfo::EBS_RECOVERABLE_FRAGMENTARY);
        CheckBlobStateCase({NKikimrBlobStorage::TGroupState::DC_FAILURE, {TDataCenterState::INCLUDED, TDataCenterState::INCLUDED, TDataCenterState::EXCLUDED}}, {2, 2, 0}, {}, TBlobStorageGroupInfo::EBS_FULL);
        // W = 2+2, E = 1+1: FULL still requires two copies in both I realms.
        CheckBlobStateCase({NKikimrBlobStorage::TGroupState::DC_FAILURE_INV1, {TDataCenterState::INCLUDED, TDataCenterState::INCLUDED, TDataCenterState::EXCLUDED}}, {2, 0, 0}, {}, TBlobStorageGroupInfo::EBS_RECOVERABLE_FRAGMENTARY);
        CheckBlobStateCase({NKikimrBlobStorage::TGroupState::DC_FAILURE_INV1, {TDataCenterState::INCLUDED, TDataCenterState::INCLUDED, TDataCenterState::EXCLUDED}}, {1, 1, 0}, {}, TBlobStorageGroupInfo::EBS_RECOVERABLE_FRAGMENTARY);
        CheckBlobStateCase({NKikimrBlobStorage::TGroupState::DC_FAILURE_INV1, {TDataCenterState::INCLUDED, TDataCenterState::INCLUDED, TDataCenterState::EXCLUDED}}, {2, 2, 0}, {}, TBlobStorageGroupInfo::EBS_FULL);
        // W = E = 2+2: one copy in each realm is recoverable but not FULL.
        CheckBlobStateCase({NKikimrBlobStorage::TGroupState::DC_FAILURE_INV2, {TDataCenterState::INCLUDED, TDataCenterState::INCLUDED, TDataCenterState::EXCLUDED}}, {1, 1, 0}, {}, TBlobStorageGroupInfo::EBS_RECOVERABLE_FRAGMENTARY);
        CheckBlobStateCase({NKikimrBlobStorage::TGroupState::DC_FAILURE_INV2, {TDataCenterState::INCLUDED, TDataCenterState::INCLUDED, TDataCenterState::EXCLUDED}}, {2, 2, 0}, {}, TBlobStorageGroupInfo::EBS_FULL);
        // W = 2 with E = 1 or E = 2: the same actual placement gives the same completeness.
        CheckBlobStateCase({NKikimrBlobStorage::TGroupState::DC_SOLO_INV1, {TDataCenterState::INCLUDED, TDataCenterState::EXCLUDED, TDataCenterState::EXCLUDED}}, {1, 0, 0}, {}, TBlobStorageGroupInfo::EBS_RECOVERABLE_FRAGMENTARY);
        CheckBlobStateCase({NKikimrBlobStorage::TGroupState::DC_SOLO_INV1, {TDataCenterState::INCLUDED, TDataCenterState::EXCLUDED, TDataCenterState::EXCLUDED}}, {2, 0, 0}, {}, TBlobStorageGroupInfo::EBS_FULL);
        CheckBlobStateCase({NKikimrBlobStorage::TGroupState::DC_SOLO_INV2, {TDataCenterState::INCLUDED, TDataCenterState::EXCLUDED, TDataCenterState::EXCLUDED}}, {1, 0, 0}, {}, TBlobStorageGroupInfo::EBS_RECOVERABLE_FRAGMENTARY);
        CheckBlobStateCase({NKikimrBlobStorage::TGroupState::DC_SOLO_INV2, {TDataCenterState::INCLUDED, TDataCenterState::EXCLUDED, TDataCenterState::EXCLUDED}}, {2, 0, 0}, {}, TBlobStorageGroupInfo::EBS_FULL);
        // W = 1+1+1 OR 2+2, D = 1+1+1: FULL follows W and does not wait for R.
        CheckBlobStateCase({NKikimrBlobStorage::TGroupState::DC_RECOVERY_INV1, {TDataCenterState::INCLUDED, TDataCenterState::INCLUDED, TDataCenterState::RECOVERING}}, {2, 2, 0}, {}, TBlobStorageGroupInfo::EBS_FULL);
    }

    Y_UNIT_TEST(EnsuredChangesAbsenceButNotCompleteness) {
        // Fix W = 2 and the physical roles; vary only the minimum guarantee E.
        for (const auto ensured : {EMirror3dcLayout::LAYOUT_1, EMirror3dcLayout::LAYOUT_2}) {
            CheckBlobStateCase({NKikimrBlobStorage::TGroupState::DC_SOLO_INV1, {TDataCenterState::INCLUDED, TDataCenterState::EXCLUDED, TDataCenterState::EXCLUDED}}, {1, 0, 0}, {}, TBlobStorageGroupInfo::EBS_RECOVERABLE_FRAGMENTARY, ensured);
            CheckBlobStateCase({NKikimrBlobStorage::TGroupState::DC_SOLO_INV1, {TDataCenterState::INCLUDED, TDataCenterState::EXCLUDED, TDataCenterState::EXCLUDED}}, {2, 0, 0}, {}, TBlobStorageGroupInfo::EBS_FULL, ensured);
        }
        // One uncertain disk can hide E = 1, but cannot hide E = 2.
        CheckBlobStateCase({NKikimrBlobStorage::TGroupState::DC_SOLO_INV1, {TDataCenterState::INCLUDED, TDataCenterState::EXCLUDED, TDataCenterState::EXCLUDED}}, {}, {1, 0, 0}, TBlobStorageGroupInfo::EBS_DISINTEGRATED, EMirror3dcLayout::LAYOUT_1);
        CheckBlobStateCase({NKikimrBlobStorage::TGroupState::DC_SOLO_INV1, {TDataCenterState::INCLUDED, TDataCenterState::EXCLUDED, TDataCenterState::EXCLUDED}}, {}, {1, 0, 0}, TBlobStorageGroupInfo::EBS_UNRECOVERABLE_FRAGMENTARY, EMirror3dcLayout::LAYOUT_2);
    }

    Y_UNIT_TEST(ExcessiveFailuresTakePrecedenceOverKnownCopies) {
        // E = 1+1+1 OR 2+2: errors can hide 2+2, even though known copies satisfy W.
        CheckBlobStateCase({NKikimrBlobStorage::TGroupState::NORMAL, {TDataCenterState::INCLUDED, TDataCenterState::INCLUDED, TDataCenterState::INCLUDED}}, {1, 1, 1}, {2, 2, 0}, TBlobStorageGroupInfo::EBS_DISINTEGRATED);
        // The same failure rule applies when only a fragment is known.
        CheckBlobStateCase({NKikimrBlobStorage::TGroupState::NORMAL, {TDataCenterState::INCLUDED, TDataCenterState::INCLUDED, TDataCenterState::INCLUDED}}, {1, 0, 0}, {2, 3, 0}, TBlobStorageGroupInfo::EBS_DISINTEGRATED);
        // E = 1 in I: a copy in R cannot compensate for loss of trust in I.
        CheckBlobStateCase({NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY_INV1, {TDataCenterState::INCLUDED, TDataCenterState::RECOVERING, TDataCenterState::EXCLUDED}}, {0, 1, 0}, {3, 0, 0}, TBlobStorageGroupInfo::EBS_DISINTEGRATED);
    }

    Y_UNIT_TEST(FailureToleranceDependsOnEnsuredLayout) {
        // E = 1+1 OR 2: two uncertain disks in one I are excessive; E = 1+1 tolerates them.
        CheckBlobStateCase({NKikimrBlobStorage::TGroupState::DC_FAILURE, {TDataCenterState::INCLUDED, TDataCenterState::INCLUDED, TDataCenterState::EXCLUDED}}, {1, 2, 0}, {2, 0, 3}, TBlobStorageGroupInfo::EBS_DISINTEGRATED);
        CheckBlobStateCase({NKikimrBlobStorage::TGroupState::DC_FAILURE_INV1, {TDataCenterState::INCLUDED, TDataCenterState::INCLUDED, TDataCenterState::EXCLUDED}}, {1, 2, 0}, {2, 0, 3}, TBlobStorageGroupInfo::EBS_RECOVERABLE_FRAGMENTARY);
        // E = 2+2: 2+1 uncertain disks are tolerated, 2+2 are excessive; X is ignored.
        CheckBlobStateCase({NKikimrBlobStorage::TGroupState::DC_FAILURE_INV2, {TDataCenterState::INCLUDED, TDataCenterState::INCLUDED, TDataCenterState::EXCLUDED}}, {1, 1, 0}, {2, 1, 3}, TBlobStorageGroupInfo::EBS_RECOVERABLE_FRAGMENTARY);
        CheckBlobStateCase({NKikimrBlobStorage::TGroupState::DC_FAILURE_INV2, {TDataCenterState::INCLUDED, TDataCenterState::INCLUDED, TDataCenterState::EXCLUDED}}, {1, 1, 0}, {2, 2, 3}, TBlobStorageGroupInfo::EBS_DISINTEGRATED);
        // E = 1 versus E = 2: one uncertain disk in I invalidates only the first guarantee.
        CheckBlobStateCase({NKikimrBlobStorage::TGroupState::DC_SOLO_INV1, {TDataCenterState::INCLUDED, TDataCenterState::EXCLUDED, TDataCenterState::EXCLUDED}}, {2, 0, 0}, {1, 3, 3}, TBlobStorageGroupInfo::EBS_DISINTEGRATED);
        CheckBlobStateCase({NKikimrBlobStorage::TGroupState::DC_SOLO_INV2, {TDataCenterState::INCLUDED, TDataCenterState::EXCLUDED, TDataCenterState::EXCLUDED}}, {2, 0, 0}, {1, 3, 3}, TBlobStorageGroupInfo::EBS_FULL);
        // In I/R/X, E fits in I alone; copies in R do not change that failure threshold.
        CheckBlobStateCase({NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY_INV1, {TDataCenterState::INCLUDED, TDataCenterState::RECOVERING, TDataCenterState::EXCLUDED}}, {2, 2, 0}, {1, 0, 3}, TBlobStorageGroupInfo::EBS_DISINTEGRATED);
        CheckBlobStateCase({NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY_INV2, {TDataCenterState::INCLUDED, TDataCenterState::RECOVERING, TDataCenterState::EXCLUDED}}, {2, 2, 0}, {1, 0, 3}, TBlobStorageGroupInfo::EBS_FULL);
        // I/I/R is RECOVERY_INV1: E = 1+1 can be hidden in I alone, regardless of R.
        CheckBlobStateCase({NKikimrBlobStorage::TGroupState::DC_RECOVERY_INV1, {TDataCenterState::INCLUDED, TDataCenterState::INCLUDED, TDataCenterState::RECOVERING}}, {2, 2, 0}, {1, 1, 0}, TBlobStorageGroupInfo::EBS_DISINTEGRATED);
        CheckBlobStateCase({NKikimrBlobStorage::TGroupState::DC_RECOVERY_INV1, {TDataCenterState::INCLUDED, TDataCenterState::INCLUDED, TDataCenterState::RECOVERING}}, {2, 2, 0}, {1, 1, 3}, TBlobStorageGroupInfo::EBS_DISINTEGRATED);
    }

    Y_UNIT_TEST(AbsenceRequiresExcludingEveryEnsuredLayout) {
        // E = 1+1 OR 2: a single uncertain disk cannot hide E; either two in I or one per I can.
        CheckBlobStateCase({NKikimrBlobStorage::TGroupState::DC_FAILURE, {TDataCenterState::INCLUDED, TDataCenterState::INCLUDED, TDataCenterState::EXCLUDED}}, {}, {1, 0, 0}, TBlobStorageGroupInfo::EBS_UNRECOVERABLE_FRAGMENTARY);
        CheckBlobStateCase({NKikimrBlobStorage::TGroupState::DC_FAILURE, {TDataCenterState::INCLUDED, TDataCenterState::INCLUDED, TDataCenterState::EXCLUDED}}, {}, {2, 0, 0}, TBlobStorageGroupInfo::EBS_DISINTEGRATED);
        CheckBlobStateCase({NKikimrBlobStorage::TGroupState::DC_FAILURE, {TDataCenterState::INCLUDED, TDataCenterState::INCLUDED, TDataCenterState::EXCLUDED}}, {}, {1, 1, 0}, TBlobStorageGroupInfo::EBS_DISINTEGRATED);
        // E = 1+1: uncertainty confined to one I cannot hide the whole layout.
        CheckBlobStateCase({NKikimrBlobStorage::TGroupState::DC_FAILURE_INV1, {TDataCenterState::INCLUDED, TDataCenterState::INCLUDED, TDataCenterState::EXCLUDED}}, {}, {3, 0, 0}, TBlobStorageGroupInfo::EBS_UNRECOVERABLE_FRAGMENTARY);
        CheckBlobStateCase({NKikimrBlobStorage::TGroupState::DC_FAILURE_INV1, {TDataCenterState::INCLUDED, TDataCenterState::INCLUDED, TDataCenterState::EXCLUDED}}, {}, {1, 1, 0}, TBlobStorageGroupInfo::EBS_DISINTEGRATED);
        // E = 2+2: both I realms must have at least two uncertain disks to hide E.
        CheckBlobStateCase({NKikimrBlobStorage::TGroupState::DC_FAILURE_INV2, {TDataCenterState::INCLUDED, TDataCenterState::INCLUDED, TDataCenterState::EXCLUDED}}, {}, {2, 1, 0}, TBlobStorageGroupInfo::EBS_UNRECOVERABLE_FRAGMENTARY);
        CheckBlobStateCase({NKikimrBlobStorage::TGroupState::DC_FAILURE_INV2, {TDataCenterState::INCLUDED, TDataCenterState::INCLUDED, TDataCenterState::EXCLUDED}}, {}, {2, 2, 0}, TBlobStorageGroupInfo::EBS_DISINTEGRATED);
        // E = 1 or E = 2 in I: one uncertain disk hides only the first guarantee.
        CheckBlobStateCase({NKikimrBlobStorage::TGroupState::DC_SOLO_INV1, {TDataCenterState::INCLUDED, TDataCenterState::EXCLUDED, TDataCenterState::EXCLUDED}}, {}, {1, 0, 0}, TBlobStorageGroupInfo::EBS_DISINTEGRATED);
        CheckBlobStateCase({NKikimrBlobStorage::TGroupState::DC_SOLO_INV2, {TDataCenterState::INCLUDED, TDataCenterState::EXCLUDED, TDataCenterState::EXCLUDED}}, {}, {1, 0, 0}, TBlobStorageGroupInfo::EBS_UNRECOVERABLE_FRAGMENTARY);
        CheckBlobStateCase({NKikimrBlobStorage::TGroupState::DC_SOLO_INV2, {TDataCenterState::INCLUDED, TDataCenterState::EXCLUDED, TDataCenterState::EXCLUDED}}, {}, {2, 0, 0}, TBlobStorageGroupInfo::EBS_DISINTEGRATED);
    }

    Y_UNIT_TEST(RecoveryFailuresParticipateInEachEnsuredAlternative) {
        // Explicit E = 1+1+1 OR 2+2: R may hide a copy in 1+1+1, but cannot replace I in 2+2.
        CheckBlobStateCase({NKikimrBlobStorage::TGroupState::DC_RECOVERY_INV1, {TDataCenterState::INCLUDED, TDataCenterState::INCLUDED, TDataCenterState::RECOVERING}}, {}, {1, 1, 0}, TBlobStorageGroupInfo::EBS_UNRECOVERABLE_FRAGMENTARY, EMirror3dcLayout::LAYOUT_111_22);
        CheckBlobStateCase({NKikimrBlobStorage::TGroupState::DC_RECOVERY_INV1, {TDataCenterState::INCLUDED, TDataCenterState::INCLUDED, TDataCenterState::RECOVERING}}, {}, {1, 1, 1}, TBlobStorageGroupInfo::EBS_DISINTEGRATED, EMirror3dcLayout::LAYOUT_111_22);
        CheckBlobStateCase({NKikimrBlobStorage::TGroupState::DC_RECOVERY_INV1, {TDataCenterState::INCLUDED, TDataCenterState::INCLUDED, TDataCenterState::RECOVERING}}, {}, {1, 1, 3}, TBlobStorageGroupInfo::EBS_DISINTEGRATED, EMirror3dcLayout::LAYOUT_111_22);
        CheckBlobStateCase({NKikimrBlobStorage::TGroupState::DC_RECOVERY_INV1, {TDataCenterState::INCLUDED, TDataCenterState::INCLUDED, TDataCenterState::RECOVERING}}, {}, {2, 0, 2}, TBlobStorageGroupInfo::EBS_UNRECOVERABLE_FRAGMENTARY, EMirror3dcLayout::LAYOUT_111_22);
        CheckBlobStateCase({NKikimrBlobStorage::TGroupState::DC_RECOVERY_INV1, {TDataCenterState::INCLUDED, TDataCenterState::INCLUDED, TDataCenterState::RECOVERING}}, {}, {2, 2, 0}, TBlobStorageGroupInfo::EBS_DISINTEGRATED, EMirror3dcLayout::LAYOUT_111_22);
        CheckBlobStateCase({NKikimrBlobStorage::TGroupState::DC_RECOVERY_INV1, {TDataCenterState::INCLUDED, TDataCenterState::INCLUDED, TDataCenterState::RECOVERING}}, {}, {2, 2, 3}, TBlobStorageGroupInfo::EBS_DISINTEGRATED, EMirror3dcLayout::LAYOUT_111_22);
        CheckBlobStateCase({NKikimrBlobStorage::TGroupState::DC_RECOVERY_INV1, {TDataCenterState::INCLUDED, TDataCenterState::INCLUDED, TDataCenterState::RECOVERING}}, {}, {1, 0, 0}, TBlobStorageGroupInfo::EBS_UNRECOVERABLE_FRAGMENTARY, EMirror3dcLayout::LAYOUT_111_22);
        CheckBlobStateCase({NKikimrBlobStorage::TGroupState::DC_RECOVERY_INV1, {TDataCenterState::INCLUDED, TDataCenterState::INCLUDED, TDataCenterState::RECOVERING}}, {}, {1, 0, 3}, TBlobStorageGroupInfo::EBS_UNRECOVERABLE_FRAGMENTARY, EMirror3dcLayout::LAYOUT_111_22);
        // Explicit E = 1+1+1 requires uncertainty in all three realms; known 1+1+1 still satisfies W.
        CheckBlobStateCase({NKikimrBlobStorage::TGroupState::DC_RECOVERY, {TDataCenterState::INCLUDED, TDataCenterState::INCLUDED, TDataCenterState::RECOVERING}}, {}, {1, 1, 0}, TBlobStorageGroupInfo::EBS_UNRECOVERABLE_FRAGMENTARY, EMirror3dcLayout::LAYOUT_111);
        CheckBlobStateCase({NKikimrBlobStorage::TGroupState::DC_RECOVERY, {TDataCenterState::INCLUDED, TDataCenterState::INCLUDED, TDataCenterState::RECOVERING}}, {}, {1, 1, 3}, TBlobStorageGroupInfo::EBS_DISINTEGRATED, EMirror3dcLayout::LAYOUT_111);
        CheckBlobStateCase({NKikimrBlobStorage::TGroupState::DC_RECOVERY, {TDataCenterState::INCLUDED, TDataCenterState::INCLUDED, TDataCenterState::RECOVERING}}, {1, 1, 1}, {}, TBlobStorageGroupInfo::EBS_FULL, EMirror3dcLayout::LAYOUT_111);
        // E = 1 fits in I alone: R cannot hide the guaranteed copy; R-only data cannot satisfy W = 2+2.
        CheckBlobStateCase({NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY_INV1, {TDataCenterState::INCLUDED, TDataCenterState::RECOVERING, TDataCenterState::EXCLUDED}}, {}, {0, 3, 3}, TBlobStorageGroupInfo::EBS_UNRECOVERABLE_FRAGMENTARY);
        CheckBlobStateCase({NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY_INV1, {TDataCenterState::INCLUDED, TDataCenterState::RECOVERING, TDataCenterState::EXCLUDED}}, {0, 2, 0}, {}, TBlobStorageGroupInfo::EBS_RECOVERABLE_FRAGMENTARY);
        // X cannot contribute known copies or uncertainty to E = 2.
        CheckBlobStateCase({NKikimrBlobStorage::TGroupState::DC_SOLO_INV2, {TDataCenterState::INCLUDED, TDataCenterState::EXCLUDED, TDataCenterState::EXCLUDED}}, {0, 3, 3}, {}, TBlobStorageGroupInfo::EBS_UNRECOVERABLE_FRAGMENTARY);
    }

    Y_UNIT_TEST(EmptyNormalBlobMatchesLegacyForEveryFailureMask) {
        TBlobStorageGroupInfo info = MakeInfo({NKikimrBlobStorage::TGroupState::NORMAL, {TDataCenterState::INCLUDED, TDataCenterState::INCLUDED, TDataCenterState::INCLUDED}});
        std::optional<TMirror3dcWriteQuorumChecker> checker = TryCreateChecker(info, 0);
        UNIT_ASSERT(checker);
        const TBlobStorageGroupInfo::IQuorumChecker& legacy = info.GetTopology().GetQuorumChecker();
        for (ui32 mask = 0; mask < (1u << 9); ++mask) {
            const auto failed = TBlobStorageGroupInfo::TSubgroupVDisks::CreateFromMask(&info.GetTopology(), mask);
            UNIT_ASSERT(checker->GetBlobState({}, failed) == legacy.GetBlobState({}, failed));
        }
    }

    Y_UNIT_TEST(MisplacedCopyIsRecoverableButNotFull) {
        TBlobStorageGroupInfo info = MakeInfo({NKikimrBlobStorage::TGroupState::DC_SOLO_INV1, {TDataCenterState::INCLUDED, TDataCenterState::EXCLUDED, TDataCenterState::EXCLUDED}}); // W = 2 in physical realm 0.
        std::optional<TMirror3dcWriteQuorumChecker> checker = TryCreateChecker(info, 0);
        UNIT_ASSERT(checker);
        TSubgroupPartLayout misplaced;
        misplaced.AddItem(0, 1, info.Type); // Disks 0 and 3 require part 0; part 1 still contains the data.
        misplaced.AddItem(3, 1, info.Type);
        UNIT_ASSERT(checker->GetBlobState(misplaced, {&info.GetTopology()}) == TBlobStorageGroupInfo::EBS_RECOVERABLE_FRAGMENTARY);
    }

    Y_UNIT_TEST(FailedRealmDoesNotCount) {
        // W = 2+2 in I/I: both realms have two copies.
        CheckCase({NKikimrBlobStorage::TGroupState::DC_FAILURE, {TDataCenterState::INCLUDED, TDataCenterState::INCLUDED, TDataCenterState::EXCLUDED}}, {2, 2, 0}, true);
        // W = 2+2 in I/I: the second I has only one copy; X cannot supplement it.
        CheckCase({NKikimrBlobStorage::TGroupState::DC_FAILURE, {TDataCenterState::INCLUDED, TDataCenterState::INCLUDED, TDataCenterState::EXCLUDED}}, {2, 1, 3}, false);
        // W = 2+2 in I/I: one copy in each I is insufficient; X is ignored.
        CheckCase({NKikimrBlobStorage::TGroupState::DC_FAILURE, {TDataCenterState::INCLUDED, TDataCenterState::INCLUDED, TDataCenterState::EXCLUDED}}, {1, 1, 3}, false);
        // W = 2+2 in I/I: three copies in one I cannot replace the missing second realm.
        CheckCase({NKikimrBlobStorage::TGroupState::DC_FAILURE, {TDataCenterState::INCLUDED, TDataCenterState::INCLUDED, TDataCenterState::EXCLUDED}}, {3, 0, 3}, false);
        // W = 2+2 in I/I: both realms have two copies despite different old-blob guarantees.
        CheckCase({NKikimrBlobStorage::TGroupState::DC_FAILURE_INV1, {TDataCenterState::INCLUDED, TDataCenterState::INCLUDED, TDataCenterState::EXCLUDED}}, {2, 2, 0}, true);
        // W = 2+2 in I/I: the I guarantees do not replace actual copies of this write.
        CheckCase({NKikimrBlobStorage::TGroupState::DC_FAILURE_INV2, {TDataCenterState::INCLUDED, TDataCenterState::INCLUDED, TDataCenterState::EXCLUDED}}, {1, 1, 0}, false);
    }

    Y_UNIT_TEST(RecoveryNeedsBothNormalRealms) {
        // I/I/R (DC_RECOVERY):      W = 1+1+1 OR 2+2.
        // I/I/R (DC_RECOVERY_INV1): W = 1+1+1 OR 2+2.
        for (const auto state : {NKikimrBlobStorage::TGroupState::DC_RECOVERY, NKikimrBlobStorage::TGroupState::DC_RECOVERY_INV1}) {
            // W = 1+1+1 OR 2+2: 1+1+1 is satisfied across I/I/R.
            CheckCase({state, {TDataCenterState::INCLUDED, TDataCenterState::INCLUDED, TDataCenterState::RECOVERING}}, {1, 1, 1}, true);
            // W = 1+1+1 OR 2+2: 2+2 is satisfied by both I realms, without R.
            CheckCase({state, {TDataCenterState::INCLUDED, TDataCenterState::INCLUDED, TDataCenterState::RECOVERING}}, {2, 2, 0}, true);
            // W = 1+1+1 OR 2+2: I/R cannot provide 2+2 instead of I/I; 1+1+1 is also missing.
            CheckCase({state, {TDataCenterState::INCLUDED, TDataCenterState::INCLUDED, TDataCenterState::RECOVERING}}, {2, 0, 2}, false);
            CheckBlobStateCase({state, {TDataCenterState::INCLUDED, TDataCenterState::INCLUDED, TDataCenterState::RECOVERING}}, {2, 0, 2}, {}, TBlobStorageGroupInfo::EBS_RECOVERABLE_FRAGMENTARY);
            // W = 1+1+1 OR 2+2: no copy in R for 1+1+1, and only one in the second I for 2+2.
            CheckCase({state, {TDataCenterState::INCLUDED, TDataCenterState::INCLUDED, TDataCenterState::RECOVERING}}, {2, 1, 0}, false);
        }
    }

    Y_UNIT_TEST(SoloUsesWriteLayoutNotEnsuredLayout) {
        // I/X/X (DC_SOLO_INV1):     W = 2; E = 1.
        // I/X/X (DC_SOLO_INV2):     W = 2; E = 2.
        // I/R/X (DC_SOLO_RECOVERY_INV1): W = 2+2; E = 1.
        // I/R/X (DC_SOLO_RECOVERY_INV2): W = 2+2; E = 2.
        for (const auto state : {NKikimrBlobStorage::TGroupState::DC_SOLO_INV1, NKikimrBlobStorage::TGroupState::DC_SOLO_INV2}) {
            // W = 2 in I: two copies suffice regardless of the ensured layout E.
            CheckCase({state, {TDataCenterState::INCLUDED, TDataCenterState::EXCLUDED, TDataCenterState::EXCLUDED}}, {2, 0, 0}, true);
            // W = 2 in I: one copy is insufficient; copies in both X realms are ignored.
            CheckCase({state, {TDataCenterState::INCLUDED, TDataCenterState::EXCLUDED, TDataCenterState::EXCLUDED}}, {1, 3, 3}, false);
            // W = 2+2 in I/R: both participating realms have two copies.
            CheckCase({state == NKikimrBlobStorage::TGroupState::DC_SOLO_INV1 ? NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY_INV1 : NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY_INV2, {TDataCenterState::INCLUDED, TDataCenterState::RECOVERING, TDataCenterState::EXCLUDED}}, {2, 2, 0}, true);
            // W = 2+2 in I/R: R has only one copy; X cannot supplement it.
            CheckCase({state == NKikimrBlobStorage::TGroupState::DC_SOLO_INV1 ? NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY_INV1 : NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY_INV2, {TDataCenterState::INCLUDED, TDataCenterState::RECOVERING, TDataCenterState::EXCLUDED}}, {2, 1, 3}, false);
            // W = 2+2 in I/R: I has only one copy; X cannot supplement it.
            CheckCase({state == NKikimrBlobStorage::TGroupState::DC_SOLO_INV1 ? NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY_INV1 : NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY_INV2, {TDataCenterState::INCLUDED, TDataCenterState::RECOVERING, TDataCenterState::EXCLUDED}}, {1, 2, 3}, false);
        }
    }

    Y_UNIT_TEST(AllExplicitLayouts) {
        const TMirror3dcGroupState states = {NKikimrBlobStorage::TGroupState::DC_RECOVERY_INV1, {TDataCenterState::INCLUDED, TDataCenterState::INCLUDED, TDataCenterState::RECOVERING}};
        struct TCase {
            EMirror3dcLayout Layout;
            TCopies Copies;
            bool Expected;
        };
        const TCase cases[] = {
            {EMirror3dcLayout::LAYOUT_111_22, {1, 1, 1}, true},
            {EMirror3dcLayout::LAYOUT_111_22, {2, 2, 0}, true},
            {EMirror3dcLayout::LAYOUT_111_22, {2, 0, 2}, false},
            {EMirror3dcLayout::LAYOUT_11_2, {1, 1, 0}, true},
            {EMirror3dcLayout::LAYOUT_11_2, {2, 0, 0}, true},
            {EMirror3dcLayout::LAYOUT_11_2, {1, 0, 3}, false},
            {EMirror3dcLayout::LAYOUT_11, {1, 1, 0}, true},
            {EMirror3dcLayout::LAYOUT_11, {1, 0, 3}, false},
            {EMirror3dcLayout::LAYOUT_22, {2, 2, 0}, true},
            {EMirror3dcLayout::LAYOUT_22, {2, 0, 2}, false},
            {EMirror3dcLayout::LAYOUT_0, {0, 0, 0}, true},
            {EMirror3dcLayout::LAYOUT_1, {1, 0, 0}, true},
            {EMirror3dcLayout::LAYOUT_1, {0, 0, 3}, false},
            {EMirror3dcLayout::LAYOUT_2, {2, 0, 0}, true},
            {EMirror3dcLayout::LAYOUT_2, {1, 0, 3}, false},
            {EMirror3dcLayout::LAYOUT_111, {1, 1, 1}, true},
            {EMirror3dcLayout::LAYOUT_111, {2, 2, 0}, false},
        };
        for (const auto& test : cases) {
            CheckCase(states, test.Copies, test.Expected, test.Layout);
        }
    }

    Y_UNIT_TEST(RecoveringRealmsOnlyFillMissingNormalRealms) {
        CheckCase({NKikimrBlobStorage::TGroupState::NORMAL, {TDataCenterState::INCLUDED, TDataCenterState::INCLUDED, TDataCenterState::INCLUDED}}, {2, 0, 2}, true, EMirror3dcLayout::LAYOUT_22);
        CheckCase({NKikimrBlobStorage::TGroupState::DC_RECOVERY, {TDataCenterState::INCLUDED, TDataCenterState::INCLUDED, TDataCenterState::RECOVERING}}, {2, 0, 2}, false, EMirror3dcLayout::LAYOUT_22);
        CheckCase({NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY_INV1, {TDataCenterState::INCLUDED, TDataCenterState::RECOVERING, TDataCenterState::EXCLUDED}}, {2, 2, 0}, true, EMirror3dcLayout::LAYOUT_22);
        CheckCase({NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY_INV1, {TDataCenterState::INCLUDED, TDataCenterState::RECOVERING, TDataCenterState::EXCLUDED}}, {1, 1, 0}, true, EMirror3dcLayout::LAYOUT_11);
        CheckCase({NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY_INV1, {TDataCenterState::INCLUDED, TDataCenterState::RECOVERING, TDataCenterState::EXCLUDED}}, {0, 3, 3}, false, EMirror3dcLayout::LAYOUT_11);
        CheckCase({NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY_INV1, {TDataCenterState::INCLUDED, TDataCenterState::RECOVERING, TDataCenterState::EXCLUDED}}, {2, 0, 0}, true, EMirror3dcLayout::LAYOUT_11_2);
        CheckCase({NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY_INV1, {TDataCenterState::INCLUDED, TDataCenterState::RECOVERING, TDataCenterState::EXCLUDED}}, {1, 1, 0}, true, EMirror3dcLayout::LAYOUT_11_2);
        CheckCase({NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY_INV1, {TDataCenterState::INCLUDED, TDataCenterState::RECOVERING, TDataCenterState::EXCLUDED}}, {1, 0, 3}, false, EMirror3dcLayout::LAYOUT_11_2);
        CheckCase({NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY_INV1, {TDataCenterState::INCLUDED, TDataCenterState::RECOVERING, TDataCenterState::EXCLUDED}}, {1, 0, 0}, true, EMirror3dcLayout::LAYOUT_1);
        CheckCase({NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY_INV1, {TDataCenterState::INCLUDED, TDataCenterState::RECOVERING, TDataCenterState::EXCLUDED}}, {0, 3, 3}, false, EMirror3dcLayout::LAYOUT_1);
        CheckCase({NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY_INV1, {TDataCenterState::INCLUDED, TDataCenterState::RECOVERING, TDataCenterState::EXCLUDED}}, {3, 3, 3}, false, EMirror3dcLayout::LAYOUT_111);
        CheckCase({NKikimrBlobStorage::TGroupState::DC_SOLO_INV1, {TDataCenterState::INCLUDED, TDataCenterState::EXCLUDED, TDataCenterState::EXCLUDED}}, {3, 3, 3}, false, EMirror3dcLayout::LAYOUT_11);
    }

    Y_UNIT_TEST(InvalidExplicitPolicyPreventsConstruction) {
        const TMirror3dcGroupState states = {NKikimrBlobStorage::TGroupState::NORMAL, {TDataCenterState::INCLUDED, TDataCenterState::INCLUDED, TDataCenterState::INCLUDED}};
        UNIT_ASSERT(!TryCreateChecker(states, EMirror3dcLayout::LAYOUT_111_22, static_cast<EMirror3dcLayout>(-1)));
        UNIT_ASSERT(!TryCreateChecker(states, EMirror3dcLayout::LAYOUT_111_22, static_cast<EMirror3dcLayout>(8)));
        UNIT_ASSERT(!TryCreateChecker(states, static_cast<EMirror3dcLayout>(-1), EMirror3dcLayout::LAYOUT_111_22));
        UNIT_ASSERT(!TryCreateChecker(states, static_cast<EMirror3dcLayout>(8), EMirror3dcLayout::LAYOUT_111_22));
        UNIT_ASSERT(!TryCreateChecker(TMirror3dcGroupState{NKikimrBlobStorage::TGroupState::NORMAL, {TDataCenterState::EXCLUDED, TDataCenterState::EXCLUDED, TDataCenterState::EXCLUDED}}, EMirror3dcLayout::LAYOUT_0, EMirror3dcLayout::LAYOUT_0));
    }

    Y_UNIT_TEST(OnlyCorrectPartsOnDistinctDisksCount) {
        TBlobStorageGroupInfo info = MakeInfo({NKikimrBlobStorage::TGroupState::NORMAL, {TDataCenterState::INCLUDED, TDataCenterState::INCLUDED, TDataCenterState::INCLUDED}});
        std::optional<TMirror3dcWriteQuorumChecker> checker = TryCreateChecker(info, 0);
        UNIT_ASSERT(checker);
        TSubgroupPartLayout wrongParts;
        for (ui32 disk = 0; disk < 9; ++disk) {
            wrongParts.AddItem(disk, (disk + 1) % 3, info.Type);
        }
        UNIT_ASSERT(!checker->CheckQuorumForLayout(wrongParts));
        TSubgroupPartLayout duplicates;
        for (ui32 part = 0; part < 3; ++part) {
            duplicates.AddItem(0, part, info.Type);
            duplicates.AddItem(0, part, info.Type);
        }
        UNIT_ASSERT(!checker->CheckQuorumForLayout(duplicates));
        wrongParts.AddItem(0, 0, info.Type);
        wrongParts.AddItem(1, 1, info.Type);
        wrongParts.AddItem(2, 2, info.Type);
        UNIT_ASSERT(checker->CheckQuorumForLayout(wrongParts));
    }

    Y_UNIT_TEST(InvalidStatesPreventConstruction) {
        for (const auto& states : {
                TMirror3dcGroupState{NKikimrBlobStorage::TGroupState::NORMAL, {TDataCenterState::EXCLUDED, TDataCenterState::EXCLUDED, TDataCenterState::EXCLUDED}}, TMirror3dcGroupState{NKikimrBlobStorage::TGroupState::NORMAL, {TDataCenterState::RECOVERING, TDataCenterState::EXCLUDED, TDataCenterState::EXCLUDED}},
                TMirror3dcGroupState{NKikimrBlobStorage::TGroupState::NORMAL, {TDataCenterState::INCLUDED, TDataCenterState::RECOVERING, TDataCenterState::RECOVERING}}, TMirror3dcGroupState{NKikimrBlobStorage::TGroupState::NORMAL, {static_cast<EMirror3dcFailRealmState>(77), TDataCenterState::INCLUDED, TDataCenterState::INCLUDED}}}) {
            TBlobStorageGroupInfo info = MakeInfo(states);
            UNIT_ASSERT(!TryCreateChecker(info, 0));
        }
    }

    Y_UNIT_TEST(UnsupportedGeometryPreventsConstruction) {
        const TBlobStorageGroupInfo wide(TBlobStorageGroupType::ErasureMirror3dc, 1, 3, 4);
        const TBlobStorageGroupInfo block(TBlobStorageGroupType::Erasure4Plus2Block);
        UNIT_ASSERT(!TryCreateChecker(wide, 0));
        UNIT_ASSERT(!TryCreateChecker(block, 0));
        const auto disks = ~TBlobStorageGroupInfo::TSubgroupVDisks(&block.GetTopology());
        const auto normal = MakeInfo({NKikimrBlobStorage::TGroupState::NORMAL, {TDataCenterState::INCLUDED, TDataCenterState::INCLUDED, TDataCenterState::INCLUDED}});
        std::optional<TMirror3dcWriteQuorumChecker> checker = TryCreateChecker(normal, 0);
        UNIT_ASSERT(checker);
        UNIT_ASSERT(!checker->CheckQuorumForSubgroup(disks));
    }

    Y_UNIT_TEST(GenerationStateIsNotTakenFromCachedTopology) {
        const auto oldInfo = MakeInfo({NKikimrBlobStorage::TGroupState::NORMAL, {TDataCenterState::INCLUDED, TDataCenterState::INCLUDED, TDataCenterState::INCLUDED}});
        TBlobStorageGroupInfo::TDynamicInfo dynamic(oldInfo.GroupID, oldInfo.GroupGeneration + 1);
        dynamic.Mirror3dcState = oldInfo.GetDynamicInfo().Mirror3dcState;
        dynamic.Mirror3dcState.FailRealmStates[2] = NKikimrBlobStorage::TDataCenterState::EXCLUDED;
        dynamic.Mirror3dcState.State = NKikimrBlobStorage::TGroupState::DC_FAILURE;
        const TBlobStorageGroupInfo newInfo(oldInfo.PickTopology(), std::move(dynamic), "", {},
            NPDisk::DEVICE_TYPE_UNKNOWN);
        const auto disks = TBlobStorageGroupInfo::TSubgroupVDisks::CreateFromMask(&oldInfo.GetTopology(), 0x7);
        const auto oldChecker = TryCreateChecker(oldInfo, 0);
        const auto newChecker = TryCreateChecker(newInfo, 0);
        UNIT_ASSERT(oldChecker && newChecker);
        UNIT_ASSERT(oldChecker->CheckQuorumForSubgroup(disks));
        UNIT_ASSERT(!newChecker->CheckQuorumForSubgroup(disks));
    }
}

} // namespace NKikimr
