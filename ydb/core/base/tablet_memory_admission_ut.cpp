#include "tablet_memory_admission.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>
#include <util/generic/vector.h>

namespace NKikimr::NMemory {

namespace {

// Stands in for the tablet that owns an admission: starts what it lets through and remembers it
struct TFixture {
    TVector<ui64> Started;
    TVector<EAdmitSource> Sources;
    TMemoryAdmission<ui64, TFixture> Admission{*this};

    void StartAdmitted(ui64&& id, EAdmitSource source) {
        Started.push_back(id);
        Sources.push_back(source);
    }

    void Admit(ui64 id, ui64 charge) {
        Admission.Admit(id, ui64(id), charge);
    }

    void SetZone(EMemoryZone zone) {
        Admission.OnZoneChanged(zone);
    }
};

}

Y_UNIT_TEST_SUITE(TTabletMemoryAdmission) {

Y_UNIT_TEST(GreenAdmitsEverything) {
    TFixture fixture;
    for (int i = 1; i <= 4; ++i) {
        fixture.Admit(i, 10);
    }
    UNIT_ASSERT_VALUES_EQUAL(fixture.Started.size(), 4u);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Admission.GetStats().RunningBytes, 40u);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Admission.GetStats().PostponedCount, 0u);
    UNIT_ASSERT(fixture.Sources.back() == EAdmitSource::Immediate);
}

Y_UNIT_TEST(YellowForbidsGrowthAboveTheWatermark) {
    TFixture fixture;
    fixture.Admit(1, 100);
    fixture.Admit(2, 100);

    // The watermark is the running bytes seen on entering the zone, so nothing may grow past it
    fixture.SetZone(EMemoryZone::Yellow);
    fixture.Admit(3, 10);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Started.size(), 2u);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Admission.GetStats().PostponedCount, 1u);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Admission.GetStats().PostponedBytes, 10u);

    // Room freed under the watermark lets the waiting item in
    fixture.Admission.Release(1);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Started.size(), 3u);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Admission.GetStats().RunningBytes, 110u);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Admission.GetStats().PostponedCount, 0u);
    UNIT_ASSERT(fixture.Sources.back() == EAdmitSource::FromQueue);
}

Y_UNIT_TEST(RedRunsOneAtATime) {
    TFixture fixture;
    fixture.SetZone(EMemoryZone::Red);
    fixture.Admit(1, 10);
    fixture.Admit(2, 10);
    fixture.Admit(3, 10);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Started.size(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Admission.GetStats().PostponedCount, 2u);

    // Still FIFO, one at a time
    fixture.Admission.Release(1);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Started.size(), 2u);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Started.back(), 2u);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Admission.GetStats().PostponedCount, 1u);

    // A green zone the tablet is told about drains the rest
    fixture.SetZone(EMemoryZone::Green);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Started.size(), 3u);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Admission.GetStats().HeldBytes(), 20u);
}

Y_UNIT_TEST(EmptyRunSetAlwaysAdmitsTheHead) {
    TFixture fixture;
    fixture.SetZone(EMemoryZone::Red);
    fixture.Admit(1, 1_GB);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Started.size(), 1u);

    fixture.SetZone(EMemoryZone::Yellow);
    fixture.Admission.Release(1);
    fixture.Admit(2, 1_GB);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Started.size(), 2u);
}

Y_UNIT_TEST(UntoldZoneBehavesAsGreen) {
    TFixture fixture;
    for (int i = 1; i <= 3; ++i) {
        fixture.Admit(i, 1_GB);
    }
    UNIT_ASSERT_VALUES_EQUAL(fixture.Started.size(), 3u);
}

    Y_UNIT_TEST(EveryYellowIntervalHasItsOwnWatermark) {
        TFixture owner;
        auto& admission = owner.Admission;
        admission.Admit(1, 1, 100);
        admission.OnZoneChanged(EMemoryZone::Yellow);
        admission.Admit(2, 2, 40);
        admission.Release(1);
        admission.OnZoneChanged(EMemoryZone::Green);
        admission.OnZoneChanged(EMemoryZone::Yellow);
        admission.Admit(3, 3, 60);
        UNIT_ASSERT_VALUES_EQUAL(admission.GetStats().RunningBytes, 40u);
        UNIT_ASSERT_VALUES_EQUAL(admission.GetStats().PostponedBytes, 60u);
    }

    Y_UNIT_TEST(GreenDrainsQueuedItemsWithoutACompletion) {
        TFixture owner;
        auto& admission = owner.Admission;
        admission.OnZoneChanged(EMemoryZone::Red);
        admission.Admit(1, 1, 100);
        admission.Admit(2, 2, 40);
        admission.OnZoneChanged(EMemoryZone::Green);
        UNIT_ASSERT_VALUES_EQUAL(owner.Started.size(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(admission.GetStats().PostponedCount, 0u);
        // A delivered recovery before enqueue also takes effect immediately.
        admission.Admit(3, 3, 60);
        UNIT_ASSERT_VALUES_EQUAL(owner.Started.size(), 3u);
    }

    Y_UNIT_TEST(CancellingHeadAllowsTheNextFittingItemToStart) {
        TFixture owner;
        auto& admission = owner.Admission;
        admission.Admit(1, 1, 100);
        admission.OnZoneChanged(EMemoryZone::Yellow);
        admission.Admit(2, 2, 40);
        admission.Release(1);
        admission.Admit(3, 3, 100);
        admission.Admit(4, 4, 20);
        auto cancelled = admission.CancelQueued(3);
        UNIT_ASSERT(cancelled);
        UNIT_ASSERT_VALUES_EQUAL(*cancelled, 3u);
        UNIT_ASSERT_VALUES_EQUAL(admission.GetStats().RunningBytes, 60u);
        UNIT_ASSERT_VALUES_EQUAL(admission.GetStats().RunningCount, 2u);
        UNIT_ASSERT_VALUES_EQUAL(admission.GetStats().PostponedBytes, 0u);
        UNIT_ASSERT_VALUES_EQUAL(owner.Started.back(), 4u);
        UNIT_ASSERT(!admission.CancelQueued(3));
        UNIT_ASSERT(!admission.CancelQueued(2));
        admission.Release(2);
        admission.Release(4);
        UNIT_ASSERT_VALUES_EQUAL(admission.GetStats().HeldBytes(), 0u);
    }
Y_UNIT_TEST(DuplicateUidsCannotReplaceRunningOrQueuedCharges) {
    TFixture fixture;
    auto& gate = fixture.Admission;
    gate.OnZoneChanged(EMemoryZone::Red);
    UNIT_ASSERT(gate.Admit(1, 1, 100) == EAdmitResult::Started);
    UNIT_ASSERT(gate.Admit(2, 2, 40) == EAdmitResult::Queued);
    UNIT_ASSERT(gate.Admit(1, 3, 500) == EAdmitResult::Duplicate);
    UNIT_ASSERT(gate.Admit(2, 4, 600) == EAdmitResult::Duplicate);
    UNIT_ASSERT_VALUES_EQUAL(gate.GetStats().RunningBytes, 100u);
    UNIT_ASSERT_VALUES_EQUAL(gate.GetStats().PostponedBytes, 40u);
    UNIT_ASSERT_VALUES_EQUAL(gate.GetStats().PostponedCount, 1u);
    gate.Release(1);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Started.back(), 2u);
    gate.Release(2);
    UNIT_ASSERT_VALUES_EQUAL(gate.GetStats().HeldBytes(), 0u);
}

Y_UNIT_TEST(IdleYellowReusesTheProgressRequestsFootprint) {
    TFixture fixture;
    fixture.SetZone(EMemoryZone::Yellow);
    fixture.Admit(1, 100);
    fixture.Admission.Release(1);
    fixture.Admit(2, 40);
    fixture.Admit(3, 60);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Admission.GetStats().RunningCount, 2u);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Admission.GetStats().RunningBytes, 100u);
    fixture.Admit(4, 1);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Admission.GetStats().PostponedCount, 1u);
}

Y_UNIT_TEST(CancellationPreservesFifoAfterRemovingAMiddleItem) {
    TFixture fixture;
    fixture.SetZone(EMemoryZone::Red);
    for (ui64 uid = 1; uid <= 1000; ++uid) fixture.Admit(uid, 1);
    for (ui64 uid = 1001; uid <= 11000; ++uid) UNIT_ASSERT(!fixture.Admission.CancelQueued(uid));
    UNIT_ASSERT(fixture.Admission.CancelQueued(500));
    UNIT_ASSERT_VALUES_EQUAL(fixture.Admission.GetStats().PostponedCount, 998u);
    for (ui64 uid = 1; uid <= 1000; ++uid) fixture.Admission.Release(uid);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Started.size(), 999u);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Started[498], 499u);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Started[499], 501u);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Admission.GetStats().HeldBytes(), 0u);
}

}

}
