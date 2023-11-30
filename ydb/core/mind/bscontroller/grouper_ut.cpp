#include "defs.h"

#include <ydb/library/actors/util/affinity.h>
#include <library/cpp/svnversion/svnversion.h>
#include <library/cpp/testing/unittest/registar.h>

#include "grouper.h"

namespace NKikimr {

enum ELevel {
    LevelDomain = 50,
    LevelRoom = 75,
    LevelRack = 100,
    LevelServer = 150,
    LevelDrive = 200
};

Y_UNIT_TEST_SUITE(TBlobStorageControllerGrouperTest) {

    static void construct_group_and_assert_it_is_valid(
        TVector<NBsController::TCandidate> candidates,
        const ui32 domainCount,
        const ui32 candidatesPerDomainCount = 1
    ) {
        TVector<TVector<const NBsController::TCandidate*>> group;
        bool isOk = NBsController::GroupFromCandidates(candidates, domainCount, candidatesPerDomainCount, group);
        UNIT_ASSERT(isOk);
        isOk = NBsController::VerifyGroup(group, domainCount, candidatesPerDomainCount);
        UNIT_ASSERT(isOk);
    }

    static void construct_group_and_assert_it_is_NOT_valid(
        TVector<NBsController::TCandidate> candidates,
        const ui32 domainCount,
        const ui32 candidatesPerDomainCount = 1
    ) {
        TVector<TVector<const NBsController::TCandidate*>> group;
        bool isOk = NBsController::GroupFromCandidates(candidates, domainCount, candidatesPerDomainCount, group);
        UNIT_ASSERT(!isOk);
    }

    Y_UNIT_TEST(TestGroupFromCandidatesEmpty) {
        TVector<NBsController::TCandidate> candidates;
        construct_group_and_assert_it_is_NOT_valid(candidates, 4, 1);
    }

    Y_UNIT_TEST(TestGroupFromCandidatesTrivial) {
        ui32 nodeId = 0;
        ui32 pDiskId = 0;
        ui32 badness = 0;
        const ui32 vDiskSlotId = 1;
        TVector<NBsController::TCandidate> candidates;
        for (ui32 rackIdx = 0; rackIdx < 4; ++rackIdx) {
            TFailDomain failDomain;
            failDomain.Levels[LevelRack] = rackIdx;
            candidates.push_back(NBsController::TCandidate(failDomain, LevelDomain, LevelDrive,
                badness, nodeId, pDiskId, vDiskSlotId));
            ++badness;
            ++nodeId;
            ++pDiskId;
        }

        // Assert
        construct_group_and_assert_it_is_valid(candidates, 4);
    }

    Y_UNIT_TEST(TestGroupFromCandidatesHuge) {
        ui32 nodeId = 0;
        const ui32 vDiskSlotId = 1;

        TVector<NBsController::TCandidate> candidates;
        for (ui32 domainIdx = 0; domainIdx < 3; ++domainIdx) {
            for (ui32 roomIdx = 0; roomIdx < 10; ++roomIdx) {
                for (ui32 rackIdx = 0; rackIdx < 8; ++rackIdx) {
                    for (ui32 serverIdx = 0; serverIdx < 16; ++serverIdx) {
                        ui32 pDiskId = 0;
                        for (ui32 driveIdx = 0; driveIdx < 6; ++driveIdx) {
                            TFailDomain failDomain;
                            failDomain.Levels[LevelDomain] = domainIdx;
                            failDomain.Levels[LevelRoom] = roomIdx;
                            failDomain.Levels[LevelRack] = rackIdx;
                            failDomain.Levels[LevelServer] = serverIdx;
                            failDomain.Levels[LevelDrive] = driveIdx;
                            candidates.push_back(NBsController::TCandidate(failDomain, LevelRack, LevelRack,
                                (ui32)rand(), nodeId, pDiskId, vDiskSlotId));
                            ++nodeId;
                            ++pDiskId;
                        }
                    }
                }
            }
        }

        // Assert
        construct_group_and_assert_it_is_valid(candidates, 8, 4);
        construct_group_and_assert_it_is_NOT_valid(candidates, 9, 4);
    }

    static TVector<NBsController::TCandidate> create_server(
        const ui32 domainIdx, const ui32 roomIdx, const ui32 rackIdx, const ui32 serverIdx,
        TVector<std::pair<ui32, ui32>> driveIdx_badness
    ) {
        const ui32 vDiskSlotId = 1;

        TVector<NBsController::TCandidate> candidates;

        for (auto x: driveIdx_badness) {
            const ui32 driveIdx = x.first;
            const ui32 badness = x.second;

            TFailDomain failDomain;
            failDomain.Levels[LevelDomain] = domainIdx;
            failDomain.Levels[LevelRoom] = roomIdx;
            failDomain.Levels[LevelRack] = rackIdx;
            failDomain.Levels[LevelServer] = serverIdx;
            failDomain.Levels[LevelDrive] = driveIdx;

            candidates.push_back(
                NBsController::TCandidate(
                    failDomain, LevelRack, LevelRack, badness, serverIdx, driveIdx, vDiskSlotId
                )
            );
        }

        return candidates;
    }

    Y_UNIT_TEST(when_one_server_per_rack_in_4_racks_then_can_construct_group_with_4_domains) {
        TVector<NBsController::TCandidate> candidates;

        for (ui32 rackIdx = 0; rackIdx < 4; ++rackIdx) {
            auto s = create_server(1, 1, rackIdx, 1, {{1, 2}});
            std::copy(s.begin(), s.end(), std::back_inserter(candidates));
        }

        // Assert
        construct_group_and_assert_it_is_valid(candidates, 4);
        construct_group_and_assert_it_is_NOT_valid(candidates, 4, 2);
        construct_group_and_assert_it_is_NOT_valid(candidates, 4, 3);
    }

    Y_UNIT_TEST(when_one_server_per_rack_in_4_racks_then_can_construct_group_with_4_domains_and_one_small_node) {
        const ui32 domainIdx = 1;
        const ui32 roomIdx = 0;

        TVector<NBsController::TCandidate> candidates;

        auto s1 = create_server(domainIdx, roomIdx, 1, 1, {{1, 0}});
        auto s2 = create_server(domainIdx, roomIdx, 2, 1, {{2, 0}});
        auto s3 = create_server(domainIdx, roomIdx, 3, 1, {{3, 0}});
        auto s4 = create_server(domainIdx, roomIdx, 4, 1, {{4, 0}});

        std::copy(s1.begin(), s1.end(), std::back_inserter(candidates));
        std::copy(s2.begin(), s2.end(), std::back_inserter(candidates));
        std::copy(s3.begin(), s3.end(), std::back_inserter(candidates));
        std::copy(s4.begin(), s4.end(), std::back_inserter(candidates));

        // Assert
        construct_group_and_assert_it_is_valid(candidates, 4);
    }
}

} // namespace NKikimr

