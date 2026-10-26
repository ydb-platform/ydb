#include "blobstorage_groupinfo_mirror3dc_write_quorum.h"

#include <library/cpp/testing/unittest/registar.h>

#include <algorithm>

namespace NKikimr {
namespace {

using R = NKikimrBlobStorage::TGroupInfo::TFailRealm;
using TStates = std::array<R::EState, 3>;
using TCopies = std::array<ui32, 3>;
using L = EMirror3dcLayout;
using G = TBlobStorageGroupInfo;

TBlobStorageGroupInfo MakeInfo(const TStates& states) {
    TBlobStorageGroupInfo::TDynamicInfo dynamic(TGroupId::Zero(), 1);
    dynamic.FailRealmStates.assign(states.begin(), states.end());
    return TBlobStorageGroupInfo(TBlobStorageGroupInfo::TTopology(
        TBlobStorageGroupType::ErasureMirror3dc, 3, 4, 1), std::move(dynamic), "");
}

void CheckCase(const TStates& states, const TCopies& copies, bool expected, std::optional<L> layout = {}) {
    // Permute physical realm identities, then rotate the subgroup relative to them.
    std::array<ui32, 3> permutation = {0, 1, 2};
    do {
        const auto info = MakeInfo({states[permutation[0]], states[permutation[1]], states[permutation[2]]});
        for (ui32 hash = 0; hash < 3; ++hash) {
            TBlobStorageGroupInfo::TVDiskIds subgroup;
            info.PickSubgroup(hash, &subgroup, nullptr);
            const auto checker = layout
                ? TMirror3dcWriteQuorumChecker::TryCreate(TStates{
                    info.GetFailRealmState(subgroup[0].FailRealm),
                    info.GetFailRealmState(subgroup[1].FailRealm),
                    info.GetFailRealmState(subgroup[2].FailRealm)}, *layout,
                    GetMirror3dcGroupLayouts(*GetMirror3dcGroupState(states[0], states[1], states[2]))->Ensured)
                : TMirror3dcWriteQuorumChecker::TryCreate(info, hash);
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

void CheckBlobStateCase(const TStates& states, const TCopies& copies, const TCopies& failures,
        G::EBlobState expected, std::optional<L> ensured = {}) {
    for (ui32 realm = 0; realm < 3; ++realm) {
        UNIT_ASSERT(copies[realm] + failures[realm] <= 3);
    }
    std::array<ui32, 3> permutation = {0, 1, 2};
    do {
        const auto info = MakeInfo({states[permutation[0]], states[permutation[1]], states[permutation[2]]});
        for (ui32 hash = 0; hash < 3; ++hash) {
            G::TVDiskIds subgroup;
            info.PickSubgroup(hash, &subgroup, nullptr);
            const auto layouts = *GetMirror3dcGroupLayouts(*GetMirror3dcGroupState(states[0], states[1], states[2]));
            const auto checker = ensured
                ? TMirror3dcWriteQuorumChecker::TryCreate(TStates{
                    info.GetFailRealmState(subgroup[0].FailRealm),
                    info.GetFailRealmState(subgroup[1].FailRealm),
                    info.GetFailRealmState(subgroup[2].FailRealm)}, layouts.Write, *ensured)
                : TMirror3dcWriteQuorumChecker::TryCreate(info, hash);
            UNIT_ASSERT(checker);
            G::TSubgroupVDisks failed(&info.GetTopology());
            TSubgroupPartLayout parts;
            for (ui32 disk = 0; disk < 9; ++disk) {
                const ui32 realm = permutation[subgroup[disk].FailRealm];
                if (disk / 3 < copies[realm]) {
                    parts.AddItem(disk, disk % 3, info.Type);
                }
                if (disk / 3 >= 3 - failures[realm]) {
                    failed += G::TSubgroupVDisks(&info.GetTopology(), disk);
                }
            }
            UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(checker->GetBlobState(parts, failed)), static_cast<ui32>(expected));
        }
    } while (std::next_permutation(permutation.begin(), permutation.end()));
}

} // namespace

Y_UNIT_TEST_SUITE(TMirror3dcWriteQuorumTest) {
    Y_UNIT_TEST(NormalGroupFailuresMatchLegacy) {
        const auto info = MakeInfo({R::N0, R::N0, R::N0});
        const auto& legacy = info.GetTopology().GetQuorumChecker();
        for (ui32 mask = 0; mask < (1u << info.GetTotalVDisksNum()); ++mask) {
            const auto failed = G::TGroupVDisks::CreateFromMask(&info.GetTopology(), mask);
            UNIT_ASSERT_VALUES_EQUAL(TMirror3dcWriteQuorumChecker::CheckFailModelForGroup(info, failed),
                legacy.CheckFailModelForGroup(failed));
        }
    }

    Y_UNIT_TEST(GroupFailuresUseEnsuredRealmRoles) {
        struct TCase {
            TStates States;
            TCopies Failures;
            bool Expected;
        };
        const TCase cases[] = {
            {{R::N1, R::F, R::F}, {0, 4, 4}, true},
            {{R::N1, R::F, R::F}, {1, 0, 0}, false},
            {{R::N2, R::F, R::F}, {1, 4, 4}, true},
            {{R::N2, R::F, R::F}, {2, 0, 0}, false},
            {{R::N2, R::N2, R::R}, {1, 1, 4}, false},
            {{R::N2, R::N2, R::R}, {2, 2, 0}, false},
            {{R::F, R::F, R::F}, {}, false},
        };
        for (const auto& test : cases) {
            std::array<ui32, 3> permutation = {0, 1, 2};
            do {
                const auto info = MakeInfo({test.States[permutation[0]], test.States[permutation[1]],
                    test.States[permutation[2]]});
                G::TGroupVDisks failed(&info.GetTopology());
                for (const auto& disk : info.GetVDisks()) {
                    const auto id = info.GetVDiskId(disk.OrderNumber);
                    if (id.FailDomain < test.Failures[permutation[id.FailRealm]]) {
                        failed += G::TGroupVDisks(&info.GetTopology(), id);
                    }
                }
                UNIT_ASSERT_VALUES_EQUAL(TMirror3dcWriteQuorumChecker::CheckFailModelForGroup(info, failed),
                    test.Expected);
            } while (std::next_permutation(permutation.begin(), permutation.end()));
        }
    }

    Y_UNIT_TEST(NormalMatchesLegacyQuorum) {
        const auto info = MakeInfo({R::N0, R::N0, R::N0});
        // MakeInfo explicitly uses ErasureMirror3dc. Take its topology checker directly,
        // independently of any generation-aware checker selection in GroupInfo.
        const TBlobStorageGroupInfo::IQuorumChecker& legacyMirror3dc = info.GetTopology().GetQuorumChecker();
        const auto checker = TMirror3dcWriteQuorumChecker::TryCreate(info, 0);
        UNIT_ASSERT(checker);
        // Only 512 reply masks; no multi-status Cartesian product.
        for (ui32 mask = 0; mask < (1u << 9); ++mask) {
            const auto disks = TBlobStorageGroupInfo::TSubgroupVDisks::CreateFromMask(&info.GetTopology(), mask);
            UNIT_ASSERT_VALUES_EQUAL(checker->CheckQuorumForSubgroup(disks),
                legacyMirror3dc.CheckQuorumForSubgroup(disks));
        }
    }

    Y_UNIT_TEST(NormalPartLayoutsMatchLegacyQuorum) {
        // NORMAL is N0/N0/N0: W = 1+1+1 OR 2+2, as in ordinary mirror-3-dc.
        const TStates states = {R::N0, R::N0, R::N0};
        UNIT_ASSERT(GetMirror3dcGroupState(states[0], states[1], states[2]) == EMirror3dcGroupState::NORMAL);
        const auto info = MakeInfo(states);
        // Compare with the ordinary ErasureMirror3dc topology checker, not GroupInfo's default.
        const TBlobStorageGroupInfo::IQuorumChecker& legacyMirror3dc = info.GetTopology().GetQuorumChecker();
        const TBlobStorageGroupInfo::TSubgroupVDisks noFailures(&info.GetTopology());
        for (ui32 hash = 0; hash < 3; ++hash) {
            const auto checker = TMirror3dcWriteQuorumChecker::TryCreate(info, hash);
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
        const TStates states = {R::N0, R::N0, R::N0};
        const auto info = MakeInfo(states);
        const G::IQuorumChecker& legacy = info.GetTopology().GetQuorumChecker();
        for (ui32 hash = 0; hash < 3; ++hash) {
            const auto checker = TMirror3dcWriteQuorumChecker::TryCreate(info, hash);
            UNIT_ASSERT(checker);
            ui32 matching = 0;
            for (const auto& a : replies) {
                for (const auto& b : replies) {
                    for (const auto& c : replies) {
                        const std::array counts = {a, b, c};
                        TSubgroupPartLayout parts;
                        G::TSubgroupVDisks present(&info.GetTopology());
                        G::TSubgroupVDisks failed(&info.GetTopology());
                        for (ui32 disk = 0; disk < 9; ++disk) {
                            const auto& count = counts[disk % 3];
                            if (disk / 3 < count[0]) {
                                parts.AddItem(disk, disk % 3, info.Type);
                                present += G::TSubgroupVDisks(&info.GetTopology(), disk);
                            }
                            if (disk / 3 >= 3 - count[1]) {
                                failed += G::TSubgroupVDisks(&info.GetTopology(), disk);
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
        const auto info = MakeInfo({R::N0, R::N0, R::N0});
        const G::IQuorumChecker& legacy = info.GetTopology().GetQuorumChecker();
        const auto checker = TMirror3dcWriteQuorumChecker::TryCreate(info, 0);
        UNIT_ASSERT(checker);
        TSubgroupPartLayout parts;
        for (ui32 disk = 0; disk < 3; ++disk) {
            parts.AddItem(disk, (disk + 1) % 3, info.Type);
        }
        const G::TSubgroupVDisks noFailures(&info.GetTopology());
        // Legacy sees 1+1+1 involved disks; W here counts only correctly placed parts.
        UNIT_ASSERT(legacy.GetBlobState(parts, noFailures) == G::EBS_FULL);
        UNIT_ASSERT(checker->GetBlobState(parts, noFailures) == G::EBS_RECOVERABLE_FRAGMENTARY);
    }

    Y_UNIT_TEST(BlobCompletenessUsesWriteInsteadOfEnsured) {
        // W = 2+2, E = 1+1 OR 2: meeting either minimum guarantee is not enough for FULL.
        CheckBlobStateCase({R::N0, R::N0, R::F}, {1, 1, 0}, {}, G::EBS_RECOVERABLE_FRAGMENTARY);
        CheckBlobStateCase({R::N0, R::N0, R::F}, {2, 0, 0}, {}, G::EBS_RECOVERABLE_FRAGMENTARY);
        CheckBlobStateCase({R::N0, R::N0, R::F}, {1, 0, 0}, {}, G::EBS_RECOVERABLE_FRAGMENTARY);
        CheckBlobStateCase({R::N0, R::N0, R::F}, {2, 2, 0}, {}, G::EBS_FULL);
        // W = 2+2, E = 1+1: FULL still requires two copies in both N realms.
        CheckBlobStateCase({R::N1, R::N1, R::F}, {2, 0, 0}, {}, G::EBS_RECOVERABLE_FRAGMENTARY);
        CheckBlobStateCase({R::N1, R::N1, R::F}, {1, 1, 0}, {}, G::EBS_RECOVERABLE_FRAGMENTARY);
        CheckBlobStateCase({R::N1, R::N1, R::F}, {2, 2, 0}, {}, G::EBS_FULL);
        // W = E = 2+2: one copy in each realm is recoverable but not FULL.
        CheckBlobStateCase({R::N2, R::N2, R::F}, {1, 1, 0}, {}, G::EBS_RECOVERABLE_FRAGMENTARY);
        CheckBlobStateCase({R::N2, R::N2, R::F}, {2, 2, 0}, {}, G::EBS_FULL);
        // W = 2 with E = 1 or E = 2: the same actual placement gives the same completeness.
        CheckBlobStateCase({R::N1, R::F, R::F}, {1, 0, 0}, {}, G::EBS_RECOVERABLE_FRAGMENTARY);
        CheckBlobStateCase({R::N1, R::F, R::F}, {2, 0, 0}, {}, G::EBS_FULL);
        CheckBlobStateCase({R::N2, R::F, R::F}, {1, 0, 0}, {}, G::EBS_RECOVERABLE_FRAGMENTARY);
        CheckBlobStateCase({R::N2, R::F, R::F}, {2, 0, 0}, {}, G::EBS_FULL);
        // W = 1+1+1 OR 2+2, D = 1+1+1: FULL follows W and does not wait for R.
        CheckBlobStateCase({R::N2, R::N2, R::R}, {2, 2, 0}, {}, G::EBS_FULL);
    }

    Y_UNIT_TEST(EnsuredChangesAbsenceButNotCompleteness) {
        // Fix W = 2 and the physical roles; vary only the minimum guarantee E.
        for (const auto ensured : {L::LAYOUT_1, L::LAYOUT_2}) {
            CheckBlobStateCase({R::N1, R::F, R::F}, {1, 0, 0}, {}, G::EBS_RECOVERABLE_FRAGMENTARY, ensured);
            CheckBlobStateCase({R::N1, R::F, R::F}, {2, 0, 0}, {}, G::EBS_FULL, ensured);
        }
        // One uncertain disk can hide E = 1, but cannot hide E = 2.
        CheckBlobStateCase({R::N1, R::F, R::F}, {}, {1, 0, 0}, G::EBS_DISINTEGRATED, L::LAYOUT_1);
        CheckBlobStateCase({R::N1, R::F, R::F}, {}, {1, 0, 0}, G::EBS_UNRECOVERABLE_FRAGMENTARY, L::LAYOUT_2);
    }

    Y_UNIT_TEST(ExcessiveFailuresTakePrecedenceOverKnownCopies) {
        // E = 1+1+1 OR 2+2: errors can hide 2+2, even though known copies satisfy W.
        CheckBlobStateCase({R::N0, R::N0, R::N0}, {1, 1, 1}, {2, 2, 0}, G::EBS_DISINTEGRATED);
        // The same failure rule applies when only a fragment is known.
        CheckBlobStateCase({R::N0, R::N0, R::N0}, {1, 0, 0}, {2, 3, 0}, G::EBS_DISINTEGRATED);
        // E = 1 in N: a copy in R cannot compensate for loss of trust in N.
        CheckBlobStateCase({R::N1, R::R, R::F}, {0, 1, 0}, {3, 0, 0}, G::EBS_DISINTEGRATED);
    }

    Y_UNIT_TEST(FailureToleranceDependsOnEnsuredLayout) {
        // E = 1+1 OR 2: two uncertain disks in one N are excessive; E = 1+1 tolerates them.
        CheckBlobStateCase({R::N0, R::N0, R::F}, {1, 2, 0}, {2, 0, 3}, G::EBS_DISINTEGRATED);
        CheckBlobStateCase({R::N1, R::N1, R::F}, {1, 2, 0}, {2, 0, 3}, G::EBS_RECOVERABLE_FRAGMENTARY);
        // E = 2+2: 2+1 uncertain disks are tolerated, 2+2 are excessive; F is ignored.
        CheckBlobStateCase({R::N2, R::N2, R::F}, {1, 1, 0}, {2, 1, 3}, G::EBS_RECOVERABLE_FRAGMENTARY);
        CheckBlobStateCase({R::N2, R::N2, R::F}, {1, 1, 0}, {2, 2, 3}, G::EBS_DISINTEGRATED);
        // E = 1 versus E = 2: one uncertain disk in N invalidates only the first guarantee.
        CheckBlobStateCase({R::N1, R::F, R::F}, {2, 0, 0}, {1, 3, 3}, G::EBS_DISINTEGRATED);
        CheckBlobStateCase({R::N2, R::F, R::F}, {2, 0, 0}, {1, 3, 3}, G::EBS_FULL);
        // In N/R/F, E fits in N alone; copies in R do not change that failure threshold.
        CheckBlobStateCase({R::N1, R::R, R::F}, {2, 2, 0}, {1, 0, 3}, G::EBS_DISINTEGRATED);
        CheckBlobStateCase({R::N2, R::R, R::F}, {2, 2, 0}, {1, 0, 3}, G::EBS_FULL);
        // E = 1+1+1 OR 2+2: an error in R completes the hidden 1+1+1 alternative.
        CheckBlobStateCase({R::N2, R::N2, R::R}, {2, 2, 0}, {1, 1, 0}, G::EBS_FULL);
        CheckBlobStateCase({R::N2, R::N2, R::R}, {2, 2, 0}, {1, 1, 3}, G::EBS_DISINTEGRATED);
    }

    Y_UNIT_TEST(AbsenceRequiresExcludingEveryEnsuredLayout) {
        // E = 1+1 OR 2: a single uncertain disk cannot hide E; either two in N or one per N can.
        CheckBlobStateCase({R::N0, R::N0, R::F}, {}, {1, 0, 0}, G::EBS_UNRECOVERABLE_FRAGMENTARY);
        CheckBlobStateCase({R::N0, R::N0, R::F}, {}, {2, 0, 0}, G::EBS_DISINTEGRATED);
        CheckBlobStateCase({R::N0, R::N0, R::F}, {}, {1, 1, 0}, G::EBS_DISINTEGRATED);
        // E = 1+1: uncertainty confined to one N cannot hide the whole layout.
        CheckBlobStateCase({R::N1, R::N1, R::F}, {}, {3, 0, 0}, G::EBS_UNRECOVERABLE_FRAGMENTARY);
        CheckBlobStateCase({R::N1, R::N1, R::F}, {}, {1, 1, 0}, G::EBS_DISINTEGRATED);
        // E = 2+2: both N realms must have at least two uncertain disks to hide E.
        CheckBlobStateCase({R::N2, R::N2, R::F}, {}, {2, 1, 0}, G::EBS_UNRECOVERABLE_FRAGMENTARY);
        CheckBlobStateCase({R::N2, R::N2, R::F}, {}, {2, 2, 0}, G::EBS_DISINTEGRATED);
        // E = 1 or E = 2 in N: one uncertain disk hides only the first guarantee.
        CheckBlobStateCase({R::N1, R::F, R::F}, {}, {1, 0, 0}, G::EBS_DISINTEGRATED);
        CheckBlobStateCase({R::N2, R::F, R::F}, {}, {1, 0, 0}, G::EBS_UNRECOVERABLE_FRAGMENTARY);
        CheckBlobStateCase({R::N2, R::F, R::F}, {}, {2, 0, 0}, G::EBS_DISINTEGRATED);
    }

    Y_UNIT_TEST(RecoveryFailuresParticipateInEachEnsuredAlternative) {
        // E = 1+1+1 OR 2+2: R may hide a copy in 1+1+1, but cannot replace N in 2+2.
        CheckBlobStateCase({R::N2, R::N2, R::R}, {}, {1, 1, 0}, G::EBS_UNRECOVERABLE_FRAGMENTARY);
        CheckBlobStateCase({R::N2, R::N2, R::R}, {}, {1, 1, 1}, G::EBS_DISINTEGRATED);
        CheckBlobStateCase({R::N2, R::N2, R::R}, {}, {1, 1, 3}, G::EBS_DISINTEGRATED);
        CheckBlobStateCase({R::N2, R::N2, R::R}, {}, {2, 0, 2}, G::EBS_UNRECOVERABLE_FRAGMENTARY);
        CheckBlobStateCase({R::N2, R::N2, R::R}, {}, {2, 2, 0}, G::EBS_DISINTEGRATED);
        CheckBlobStateCase({R::N2, R::N2, R::R}, {}, {2, 2, 3}, G::EBS_DISINTEGRATED);
        CheckBlobStateCase({R::N2, R::N2, R::R}, {}, {1, 0, 0}, G::EBS_UNRECOVERABLE_FRAGMENTARY);
        CheckBlobStateCase({R::N2, R::N2, R::R}, {}, {1, 0, 3}, G::EBS_UNRECOVERABLE_FRAGMENTARY);
        // Explicit E = 1+1+1 requires uncertainty in all three realms; known 1+1+1 still satisfies W.
        CheckBlobStateCase({R::N0, R::N0, R::R}, {}, {1, 1, 0}, G::EBS_UNRECOVERABLE_FRAGMENTARY, L::LAYOUT_111);
        CheckBlobStateCase({R::N0, R::N0, R::R}, {}, {1, 1, 3}, G::EBS_DISINTEGRATED, L::LAYOUT_111);
        CheckBlobStateCase({R::N0, R::N0, R::R}, {1, 1, 1}, {}, G::EBS_FULL, L::LAYOUT_111);
        // E = 1 fits in N alone: R cannot hide the guaranteed copy; R-only data cannot satisfy W = 2+2.
        CheckBlobStateCase({R::N1, R::R, R::F}, {}, {0, 3, 3}, G::EBS_UNRECOVERABLE_FRAGMENTARY);
        CheckBlobStateCase({R::N1, R::R, R::F}, {0, 2, 0}, {}, G::EBS_RECOVERABLE_FRAGMENTARY);
        // F cannot contribute known copies or uncertainty to E = 2.
        CheckBlobStateCase({R::N2, R::F, R::F}, {0, 3, 3}, {}, G::EBS_UNRECOVERABLE_FRAGMENTARY);
    }

    Y_UNIT_TEST(EmptyNormalBlobMatchesLegacyForEveryFailureMask) {
        const auto info = MakeInfo({R::N0, R::N0, R::N0});
        const auto checker = TMirror3dcWriteQuorumChecker::TryCreate(info, 0);
        UNIT_ASSERT(checker);
        const G::IQuorumChecker& legacy = info.GetTopology().GetQuorumChecker();
        for (ui32 mask = 0; mask < (1u << 9); ++mask) {
            const auto failed = G::TSubgroupVDisks::CreateFromMask(&info.GetTopology(), mask);
            UNIT_ASSERT(checker->GetBlobState({}, failed) == legacy.GetBlobState({}, failed));
        }
    }

    Y_UNIT_TEST(MisplacedCopyIsRecoverableButNotFull) {
        const auto info = MakeInfo({R::N1, R::F, R::F}); // W = 2 in physical realm 0.
        const auto checker = TMirror3dcWriteQuorumChecker::TryCreate(info, 0);
        UNIT_ASSERT(checker);
        TSubgroupPartLayout misplaced;
        misplaced.AddItem(0, 1, info.Type); // Disks 0 and 3 require part 0; part 1 still contains the data.
        misplaced.AddItem(3, 1, info.Type);
        UNIT_ASSERT(checker->GetBlobState(misplaced, {&info.GetTopology()}) == G::EBS_RECOVERABLE_FRAGMENTARY);
    }

    Y_UNIT_TEST(FailedRealmDoesNotCount) {
        // W = 2+2 in N0/N0: both realms have two copies.
        CheckCase({R::N0, R::N0, R::F}, {2, 2, 0}, true);
        // W = 2+2 in N0/N0: the second N0 has only one copy; F cannot supplement it.
        CheckCase({R::N0, R::N0, R::F}, {2, 1, 3}, false);
        // W = 2+2 in N0/N0: one copy in each N0 is insufficient; F is ignored.
        CheckCase({R::N0, R::N0, R::F}, {1, 1, 3}, false);
        // W = 2+2 in N0/N0: three copies in one N0 cannot replace the missing second realm.
        CheckCase({R::N0, R::N0, R::F}, {3, 0, 3}, false);
        // W = 2+2 in N1/N2: both realms have two copies despite different old-blob guarantees.
        CheckCase({R::N1, R::N2, R::F}, {2, 2, 0}, true);
        // W = 2+2 in N2/N2: the N2 guarantees do not replace actual copies of this write.
        CheckCase({R::N2, R::N2, R::F}, {1, 1, 0}, false);
    }

    Y_UNIT_TEST(RecoveryNeedsBothNormalRealms) {
        // N0/N0/R (DC_RECOVERY):      W = 1+1+1 OR 2+2.
        // N1/N1/R (DC_RECOVERY_INV1): W = 1+1+1 OR 2+2.
        // N2/N2/R (DC_RECOVERY_INV2): W = 1+1+1 OR 2+2.
        for (const auto state : {R::N0, R::N1, R::N2}) {
            // W = 1+1+1 OR 2+2: 1+1+1 is satisfied across N/N/R.
            CheckCase({state, state, R::R}, {1, 1, 1}, true);
            // W = 1+1+1 OR 2+2: 2+2 is satisfied by both N realms, without R.
            CheckCase({state, state, R::R}, {2, 2, 0}, true);
            // W = 1+1+1 OR 2+2: N/R cannot provide 2+2 instead of N/N; 1+1+1 is also missing.
            CheckCase({state, state, R::R}, {2, 0, 2}, false);
            CheckBlobStateCase({state, state, R::R}, {2, 0, 2}, {}, G::EBS_RECOVERABLE_FRAGMENTARY);
            // W = 1+1+1 OR 2+2: no copy in R for 1+1+1, and only one in the second N for 2+2.
            CheckCase({state, state, R::R}, {2, 1, 0}, false);
        }
    }

    Y_UNIT_TEST(SoloUsesWriteLayoutNotEnsuredLayout) {
        // N1/F/F (DC_SOLO_INV1):     W = 2; E = 1.
        // N2/F/F (DC_SOLO_INV2):     W = 2; E = 2.
        // N1/R/F (DC_SOLO_RECOVERY_INV1): W = 2+2; E = 1.
        // N2/R/F (DC_SOLO_RECOVERY_INV2): W = 2+2; E = 2.
        for (const auto state : {R::N1, R::N2}) {
            // W = 2 in N: two copies suffice regardless of the ensured layout E.
            CheckCase({state, R::F, R::F}, {2, 0, 0}, true);
            // W = 2 in N: one copy is insufficient; copies in both F realms are ignored.
            CheckCase({state, R::F, R::F}, {1, 3, 3}, false);
            // W = 2+2 in N/R: both participating realms have two copies.
            CheckCase({state, R::R, R::F}, {2, 2, 0}, true);
            // W = 2+2 in N/R: R has only one copy; F cannot supplement it.
            CheckCase({state, R::R, R::F}, {2, 1, 3}, false);
            // W = 2+2 in N/R: N has only one copy; F cannot supplement it.
            CheckCase({state, R::R, R::F}, {1, 2, 3}, false);
        }
    }

    Y_UNIT_TEST(AllExplicitLayouts) {
        const TStates states = {R::N2, R::N1, R::R};
        struct TCase {
            L Layout;
            TCopies Copies;
            bool Expected;
        };
        const TCase cases[] = {
            {L::LAYOUT_111_22, {1, 1, 1}, true},
            {L::LAYOUT_111_22, {2, 2, 0}, true},
            {L::LAYOUT_111_22, {2, 0, 2}, false},
            {L::LAYOUT_11_2, {1, 1, 0}, true},
            {L::LAYOUT_11_2, {2, 0, 0}, true},
            {L::LAYOUT_11_2, {1, 0, 3}, false},
            {L::LAYOUT_11, {1, 1, 0}, true},
            {L::LAYOUT_11, {1, 0, 3}, false},
            {L::LAYOUT_22, {2, 2, 0}, true},
            {L::LAYOUT_22, {2, 0, 2}, false},
            {L::LAYOUT_0, {0, 0, 0}, true},
            {L::LAYOUT_1, {1, 0, 0}, true},
            {L::LAYOUT_1, {0, 0, 3}, false},
            {L::LAYOUT_2, {2, 0, 0}, true},
            {L::LAYOUT_2, {1, 0, 3}, false},
            {L::LAYOUT_111, {1, 1, 1}, true},
            {L::LAYOUT_111, {2, 2, 0}, false},
        };
        for (const auto& test : cases) {
            CheckCase(states, test.Copies, test.Expected, test.Layout);
        }
    }

    Y_UNIT_TEST(RecoveringRealmsOnlyFillMissingNormalRealms) {
        CheckCase({R::N0, R::N0, R::N0}, {2, 0, 2}, true, L::LAYOUT_22);
        CheckCase({R::N0, R::N0, R::R}, {2, 0, 2}, false, L::LAYOUT_22);
        CheckCase({R::N1, R::R, R::F}, {2, 2, 0}, true, L::LAYOUT_22);
        CheckCase({R::N1, R::R, R::F}, {1, 1, 0}, true, L::LAYOUT_11);
        CheckCase({R::N1, R::R, R::F}, {0, 3, 3}, false, L::LAYOUT_11);
        CheckCase({R::N1, R::R, R::F}, {2, 0, 0}, true, L::LAYOUT_11_2);
        CheckCase({R::N1, R::R, R::F}, {1, 1, 0}, true, L::LAYOUT_11_2);
        CheckCase({R::N1, R::R, R::F}, {1, 0, 3}, false, L::LAYOUT_11_2);
        CheckCase({R::N1, R::R, R::F}, {1, 0, 0}, true, L::LAYOUT_1);
        CheckCase({R::N1, R::R, R::F}, {0, 3, 3}, false, L::LAYOUT_1);
        CheckCase({R::N1, R::R, R::F}, {3, 3, 3}, false, L::LAYOUT_111);
        CheckCase({R::N1, R::F, R::F}, {3, 3, 3}, false, L::LAYOUT_11);
    }

    Y_UNIT_TEST(InvalidExplicitPolicyPreventsConstruction) {
        const TStates states = {R::N0, R::N0, R::N0};
        UNIT_ASSERT(!TMirror3dcWriteQuorumChecker::TryCreate(states, L::LAYOUT_111_22, static_cast<L>(-1)));
        UNIT_ASSERT(!TMirror3dcWriteQuorumChecker::TryCreate(states, L::LAYOUT_111_22, static_cast<L>(8)));
        UNIT_ASSERT(!TMirror3dcWriteQuorumChecker::TryCreate(states, static_cast<L>(-1), L::LAYOUT_111_22));
        UNIT_ASSERT(!TMirror3dcWriteQuorumChecker::TryCreate(states, static_cast<L>(8), L::LAYOUT_111_22));
        UNIT_ASSERT(!TMirror3dcWriteQuorumChecker::TryCreate(TStates{R::F, R::F, R::F}, L::LAYOUT_0, L::LAYOUT_0));
    }

    Y_UNIT_TEST(OnlyCorrectPartsOnDistinctDisksCount) {
        const auto info = MakeInfo({R::N0, R::N0, R::N0});
        const auto checker = TMirror3dcWriteQuorumChecker::TryCreate(info, 0);
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
                TStates{R::F, R::F, R::F}, TStates{R::R, R::F, R::F},
                TStates{R::N0, R::R, R::R}, TStates{static_cast<R::EState>(5), R::N0, R::N0}}) {
            const auto info = MakeInfo(states);
            UNIT_ASSERT(!TMirror3dcWriteQuorumChecker::TryCreate(info, 0));
        }
    }

    Y_UNIT_TEST(UnsupportedGeometryPreventsConstruction) {
        const TBlobStorageGroupInfo wide(TBlobStorageGroupType::ErasureMirror3dc, 1, 3, 4);
        const TBlobStorageGroupInfo block(TBlobStorageGroupType::Erasure4Plus2Block);
        UNIT_ASSERT(!TMirror3dcWriteQuorumChecker::TryCreate(wide, 0));
        UNIT_ASSERT(!TMirror3dcWriteQuorumChecker::TryCreate(block, 0));
        const auto disks = ~TBlobStorageGroupInfo::TSubgroupVDisks(&block.GetTopology());
        const auto normal = MakeInfo({R::N0, R::N0, R::N0});
        const auto checker = TMirror3dcWriteQuorumChecker::TryCreate(normal, 0);
        UNIT_ASSERT(checker);
        UNIT_ASSERT(!checker->CheckQuorumForSubgroup(disks));
    }

    Y_UNIT_TEST(GenerationStateIsNotTakenFromCachedTopology) {
        const auto oldInfo = MakeInfo({R::N0, R::N0, R::N0});
        TBlobStorageGroupInfo::TDynamicInfo dynamic(oldInfo.GroupID, oldInfo.GroupGeneration + 1);
        dynamic.FailRealmStates = oldInfo.GetDynamicInfo().FailRealmStates;
        dynamic.FailRealmStates[2] = R::F;
        const TBlobStorageGroupInfo newInfo(oldInfo.PickTopology(), std::move(dynamic), "", {},
            NPDisk::DEVICE_TYPE_UNKNOWN);
        const auto disks = TBlobStorageGroupInfo::TSubgroupVDisks::CreateFromMask(&oldInfo.GetTopology(), 0x7);
        const auto oldChecker = TMirror3dcWriteQuorumChecker::TryCreate(oldInfo, 0);
        const auto newChecker = TMirror3dcWriteQuorumChecker::TryCreate(newInfo, 0);
        UNIT_ASSERT(oldChecker && newChecker);
        UNIT_ASSERT(oldChecker->CheckQuorumForSubgroup(disks));
        UNIT_ASSERT(!newChecker->CheckQuorumForSubgroup(disks));
    }
}

} // namespace NKikimr
