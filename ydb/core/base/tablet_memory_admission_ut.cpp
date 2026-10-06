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

}

}
