#include "statestorage.h"

#include <util/generic/xrange.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

Y_UNIT_TEST_SUITE(TStateStorageConfig) {

    void FillStateStorageInfo(TStateStorageInfo *info, ui32 replicas, ui32 nToSelect, ui32 replicasInRing, bool useRingSpecificNodeSelection) {
        info->NToSelect = nToSelect;

        info->Rings.resize(replicas);
        for (ui32 i : xrange(replicas)) {
            for (ui32 j : xrange(replicasInRing)) {
                info->Rings[i].Replicas.push_back(TActorId(i, i, i + j, i));
                info->Rings[i].UseRingSpecificNodeSelection = useRingSpecificNodeSelection;
            }
        }
    }

    ui64 StabilityRun(ui32 replicas, ui32 nToSelect, ui32 replicasInRing, bool useRingSpecificNodeSelection) {
        ui64 retHash = 0;

        TStateStorageInfo info;
        FillStateStorageInfo(&info, replicas, nToSelect, replicasInRing, useRingSpecificNodeSelection);

        TStateStorageInfo::TSelection selection;
        for (ui64 tabletId = 8000000; tabletId < 9000000; ++tabletId) {
            info.SelectReplicas(tabletId, &selection);
            Y_ABORT_UNLESS(nToSelect == selection.Sz);
            for (ui32 idx : xrange(nToSelect))
                retHash = CombineHashes<ui64>(retHash, selection.SelectedReplicas[idx].Hash());
        }
        return retHash;
    }

    double UniqueCombinationsRun(ui32 replicas, ui32 nToSelect, ui32 replicasInRing, bool useRingSpecificNodeSelection) {
        const ui64 tabletStartId = 8000000;
        const ui64 tabletCount = 1000000;
        TStateStorageInfo info;
        FillStateStorageInfo(&info, replicas, nToSelect, replicasInRing, useRingSpecificNodeSelection);

        THashSet<ui64> hashes;

        TStateStorageInfo::TSelection selection;
        for (ui64 tabletId = tabletStartId; tabletId < tabletStartId + tabletCount; ++tabletId) {
            ui64 selectionHash = 0;
            info.SelectReplicas(tabletId, &selection);
            Y_ABORT_UNLESS(nToSelect == selection.Sz);
            for (ui32 idx : xrange(nToSelect))
                selectionHash = CombineHashes<ui64>(selectionHash, selection.SelectedReplicas[idx].Hash());
            hashes.insert(selectionHash);
        }
        return static_cast<double>(hashes.size()) / static_cast<double>(tabletCount);
    }

    Y_UNIT_TEST(TestReplicaSelection) {
        UNIT_ASSERT(StabilityRun(3, 3, 1, false) == 17606246762804570019ULL);
        UNIT_ASSERT(StabilityRun(13, 3, 1, false) == 421354124534079828ULL);
        UNIT_ASSERT(StabilityRun(13, 9, 1, false) == 10581416019959162949ULL);
        UNIT_ASSERT(StabilityRun(3, 3, 1, true) == 17606246762804570019ULL);
        UNIT_ASSERT(StabilityRun(13, 3, 1, true) == 421354124534079828ULL);
        UNIT_ASSERT(StabilityRun(13, 9, 1, true) == 10581416019959162949ULL);
    }

    Y_UNIT_TEST(TestMultiReplicaFailDomains) {
        UNIT_ASSERT(StabilityRun(3, 3, 3, false) == 12043409773822600429ULL);
        UNIT_ASSERT(StabilityRun(13, 3, 5, false) == 3265154396592024904ULL);
        UNIT_ASSERT(StabilityRun(13, 9, 8, false) == 12079940289459527060ULL);
        UNIT_ASSERT(StabilityRun(3, 3, 3, true) == 7845257406715748850ULL);
        UNIT_ASSERT(StabilityRun(13, 3, 5, true) == 1986618578793030392ULL);
        UNIT_ASSERT(StabilityRun(13, 9, 8, true) == 6173011524598124144ULL);
    }

    Y_UNIT_TEST(TestReplicaSelectionUniqueCombinations) {
        UNIT_ASSERT_DOUBLES_EQUAL(UniqueCombinationsRun(13, 3, 1, false), 0.000206, 1e-7);
        UNIT_ASSERT_DOUBLES_EQUAL(UniqueCombinationsRun(13, 3, 3, false), 0.000519, 1e-7);
        UNIT_ASSERT_DOUBLES_EQUAL(UniqueCombinationsRun(113, 3, 1, false), 0.009091, 1e-7);
        UNIT_ASSERT_DOUBLES_EQUAL(UniqueCombinationsRun(113, 3, 5, false), 0.045251, 1e-7);
        UNIT_ASSERT_DOUBLES_EQUAL(UniqueCombinationsRun(113, 9, 1, false), 0.009237, 1e-7);
        UNIT_ASSERT_DOUBLES_EQUAL(UniqueCombinationsRun(113, 9, 8, false), 0.01387, 1e-7);
        UNIT_ASSERT_DOUBLES_EQUAL(UniqueCombinationsRun(13, 3, 1, true), 0.000206, 1e-7);
        UNIT_ASSERT_DOUBLES_EQUAL(UniqueCombinationsRun(13, 3, 3, true), 0.004263, 1e-7);
        UNIT_ASSERT_DOUBLES_EQUAL(UniqueCombinationsRun(113, 3, 1, true), 0.009091, 1e-7);
        UNIT_ASSERT_DOUBLES_EQUAL(UniqueCombinationsRun(113, 3, 5, true), 0.63673, 1e-7);
        UNIT_ASSERT_DOUBLES_EQUAL(UniqueCombinationsRun(113, 9, 1, true), 0.009237, 1e-7);
        UNIT_ASSERT_DOUBLES_EQUAL(UniqueCombinationsRun(113, 9, 8, true), 0.072514, 1e-7);
    }

    double UniformityRun(ui32 replicas, ui32 nToSelect, ui32 replicasInRing, bool useRingSpecificNodeSelection) {
        THashMap<TActorId, ui32> history;

        TStateStorageInfo info;
        FillStateStorageInfo(&info, replicas, nToSelect, replicasInRing, useRingSpecificNodeSelection);

        TStateStorageInfo::TSelection selection;
        for (ui64 tabletId = 8000000; tabletId < 9000000; ++tabletId) {
            info.SelectReplicas(tabletId, &selection);
            Y_ABORT_UNLESS(nToSelect == selection.Sz);
            for (ui32 idx : xrange(nToSelect))
                history[selection.SelectedReplicas[idx]] += 1;
        }

        ui32 mn = history.begin()->second;
        ui32 mx = history.begin()->second;

        for (auto &x : history) {
            const ui32 cur = x.second;
            if (cur < mn)
                mn = cur;
            if (cur > mx)
                mx = cur;
        }

        return static_cast<double>(mx - mn) / static_cast<double>(mx);
    }

    Y_UNIT_TEST(UniformityTest) {
        UNIT_ASSERT(UniformityRun(13, 3, 1, false) < 0.10);
        UNIT_ASSERT(UniformityRun(13, 3, 3, false) < 0.10);
        UNIT_ASSERT(UniformityRun(113, 3, 1, false) < 0.10);
        UNIT_ASSERT(UniformityRun(113, 3, 5, false) < 0.10);
        UNIT_ASSERT(UniformityRun(113, 9, 1, false) < 0.10);
        UNIT_ASSERT(UniformityRun(113, 9, 8, false) < 0.10);
        UNIT_ASSERT(UniformityRun(13, 3, 1, true) < 0.10);
        UNIT_ASSERT(UniformityRun(13, 3, 3, true) < 0.10);
        UNIT_ASSERT(UniformityRun(113, 3, 1, true) < 0.10);
        UNIT_ASSERT(UniformityRun(113, 3, 5, true) < 0.10);
        UNIT_ASSERT(UniformityRun(113, 9, 1, true) < 0.10);
        UNIT_ASSERT(UniformityRun(113, 9, 8, true) < 0.10);
    }
}

}
