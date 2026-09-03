#include "types.h"

#include <ydb/core/control/lib/immediate_control_board_impl.h>
#include <ydb/core/control/lib/immediate_control_board_wrapper.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NKikimr::NBlobDepot;

namespace {

TIntrusivePtr<TControlBoard> MakeBlobDepotIcb() {
    auto icb = MakeIntrusive<TControlBoard>();
    icb->CreateConfigControls(true);
    return icb;
}

} // namespace

Y_UNIT_TEST_SUITE(BlobDepotS3InFlightLimits) {
    Y_UNIT_TEST(EffectiveLimitUsesIcbCeilingWhenCurrentIsUnbounded) {
        UNIT_ASSERT_VALUES_EQUAL(EffectiveS3InFlightLimit(Max<ui32>(), 3), 3);
        UNIT_ASSERT_VALUES_EQUAL(EffectiveS3InFlightLimit(Max<ui32>(), 32), 32);
    }

    Y_UNIT_TEST(EffectiveLimitUsesAdaptiveCurrentAfterSlowDown) {
        UNIT_ASSERT_VALUES_EQUAL(EffectiveS3InFlightLimit(1, 32), 1);
        UNIT_ASSERT_VALUES_EQUAL(EffectiveS3InFlightLimit(2, 32), 2);
    }

    Y_UNIT_TEST(EffectiveLimitAppliesLoweredIcbImmediately) {
        UNIT_ASSERT_VALUES_EQUAL(EffectiveS3InFlightLimit(32, 1), 1);
        UNIT_ASSERT_VALUES_EQUAL(EffectiveS3InFlightLimit(4, 3), 3);
    }

    Y_UNIT_TEST(EffectiveLimitClampsNonPositiveIcbToOne) {
        UNIT_ASSERT_VALUES_EQUAL(EffectiveS3InFlightLimit(Max<ui32>(), 0), 1);
        UNIT_ASSERT_VALUES_EQUAL(EffectiveS3InFlightLimit(5, 0), 1);
    }

    Y_UNIT_TEST(DeleteInFlightIcbIsSharedAndAffectsEffectiveLimit) {
        auto icb = MakeBlobDepotIcb();

        TControlWrapper tabletLimit(3, 1, 1'000'000'000);
        TControlBoard::RegisterSharedControl(tabletLimit, icb->BlobDepotControls.S3MaxDeletesInFlight);

        ui32 currentMax = Max<ui32>();
        UNIT_ASSERT_VALUES_EQUAL(EffectiveS3InFlightLimit(currentMax, tabletLimit), 3);

        TControlWrapper updater(3, 1, 1'000'000'000);
        TControlBoard::RegisterSharedControl(updater, icb->BlobDepotControls.S3MaxDeletesInFlight);
        updater = 1;

        UNIT_ASSERT_VALUES_EQUAL(i64(tabletLimit), 1);
        UNIT_ASSERT_VALUES_EQUAL(EffectiveS3InFlightLimit(currentMax, tabletLimit), 1);

        updater = 10;
        currentMax = 1;
        UNIT_ASSERT_VALUES_EQUAL(EffectiveS3InFlightLimit(currentMax, tabletLimit), 1);

        currentMax = 4;
        UNIT_ASSERT_VALUES_EQUAL(EffectiveS3InFlightLimit(currentMax, tabletLimit), 4);

        currentMax = Max<ui32>();
        UNIT_ASSERT_VALUES_EQUAL(EffectiveS3InFlightLimit(currentMax, tabletLimit), 10);
    }

    Y_UNIT_TEST(WriteAndGetInFlightIcbDefaults) {
        auto icb = MakeBlobDepotIcb();

        TControlWrapper writes(32, 1, 1'000'000'000);
        TControlWrapper gets(32, 1, 1'000'000'000);
        TControlBoard::RegisterSharedControl(writes, icb->BlobDepotControls.S3MaxWritesInFlight);
        TControlBoard::RegisterSharedControl(gets, icb->BlobDepotControls.S3MaxGetsInFlight);

        UNIT_ASSERT_VALUES_EQUAL(i64(writes), 32);
        UNIT_ASSERT_VALUES_EQUAL(i64(gets), 32);
        UNIT_ASSERT_VALUES_EQUAL(EffectiveS3InFlightLimit(Max<ui32>(), writes), 32);
        UNIT_ASSERT_VALUES_EQUAL(EffectiveS3InFlightLimit(Max<ui32>(), gets), 32);
    }

    Y_UNIT_TEST(ObjectsToDeleteAtOnceIcbIsCappedAtS3Limit) {
        auto icb = MakeBlobDepotIcb();

        TControlWrapper batch(10, 1, 1000);
        TControlBoard::RegisterSharedControl(batch, icb->BlobDepotControls.S3MaxObjectsToDeleteAtOnce);
        UNIT_ASSERT_VALUES_EQUAL(i64(batch), 10);

        TControlBoard::SetValue(1000, icb->BlobDepotControls.S3MaxObjectsToDeleteAtOnce);
        UNIT_ASSERT_VALUES_EQUAL(i64(batch), 1000);

        TControlBoard::SetValue(1001, icb->BlobDepotControls.S3MaxObjectsToDeleteAtOnce);
        UNIT_ASSERT_VALUES_EQUAL(i64(batch), 1000);
    }
}
