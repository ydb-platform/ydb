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

// Mimics the growth step shared by the write/delete/get limiters: after enough successful
// batches the counter is bumped by one until it reaches the ICB ceiling.
void GrowTowardsCeiling(ui32& currentMax, i64 icbValue) {
    const ui32 limit = S3ControlLimit(icbValue);
    if (currentMax < limit) {
        ++currentMax;
    }
}

} // namespace

Y_UNIT_TEST_SUITE(BlobDepotS3InFlightLimits) {
    Y_UNIT_TEST(ControlLimitClampsNonPositiveIcbToOne) {
        UNIT_ASSERT_VALUES_EQUAL(S3ControlLimit(0), 1);
        UNIT_ASSERT_VALUES_EQUAL(S3ControlLimit(-1), 1);
        UNIT_ASSERT_VALUES_EQUAL(S3ControlLimit(Min<i64>()), 1);
        UNIT_ASSERT_VALUES_EQUAL(S3ControlLimit(3), 3);
        UNIT_ASSERT_VALUES_EQUAL(S3ControlLimit(32), 32);
    }

    Y_UNIT_TEST(LoweredIcbPressesCurrentDown) {
        ui32 currentMax = 32;

        UNIT_ASSERT(ClampToS3ControlLimit(currentMax, 4));
        UNIT_ASSERT_VALUES_EQUAL(currentMax, 4);

        UNIT_ASSERT(ClampToS3ControlLimit(currentMax, 0));
        UNIT_ASSERT_VALUES_EQUAL(currentMax, 1);
    }

    Y_UNIT_TEST(RaisedIcbLeavesCurrentToGrowOnItsOwn) {
        // After a SlowDown the counter sits at 1 and must not jump back to the ceiling at once.
        ui32 currentMax = 1;
        UNIT_ASSERT(!ClampToS3ControlLimit(currentMax, 32));
        UNIT_ASSERT_VALUES_EQUAL(currentMax, 1);

        GrowTowardsCeiling(currentMax, 32);
        UNIT_ASSERT_VALUES_EQUAL(currentMax, 2);
    }

    Y_UNIT_TEST(CurrentConvergesToIcbCeiling) {
        ui32 currentMax = 1;
        for (ui32 i = 0; i < 10; ++i) {
            ClampToS3ControlLimit(currentMax, 3);
            GrowTowardsCeiling(currentMax, 3);
        }
        UNIT_ASSERT_VALUES_EQUAL(currentMax, 3);

        // Lowering the ceiling mid-flight takes effect on the very next gate check.
        ClampToS3ControlLimit(currentMax, 2);
        UNIT_ASSERT_VALUES_EQUAL(currentMax, 2);
    }

    Y_UNIT_TEST(DeleteInFlightIcbIsSharedAndPressesCurrentDown) {
        auto icb = MakeBlobDepotIcb();

        TControlWrapper tabletLimit(3, 1, 1'000'000'000);
        TControlBoard::RegisterSharedControl(tabletLimit, icb->BlobDepotControls.S3MaxDeletesInFlight);

        ui32 currentMax = S3ControlLimit(tabletLimit);
        UNIT_ASSERT_VALUES_EQUAL(currentMax, 3);

        TControlWrapper updater(3, 1, 1'000'000'000);
        TControlBoard::RegisterSharedControl(updater, icb->BlobDepotControls.S3MaxDeletesInFlight);
        updater = 1;

        UNIT_ASSERT_VALUES_EQUAL(i64(tabletLimit), 1);
        UNIT_ASSERT(ClampToS3ControlLimit(currentMax, tabletLimit));
        UNIT_ASSERT_VALUES_EQUAL(currentMax, 1);

        // Raising the ceiling back does not release the counter immediately.
        updater = 10;
        UNIT_ASSERT_VALUES_EQUAL(i64(tabletLimit), 10);
        UNIT_ASSERT(!ClampToS3ControlLimit(currentMax, tabletLimit));
        UNIT_ASSERT_VALUES_EQUAL(currentMax, 1);

        GrowTowardsCeiling(currentMax, tabletLimit);
        UNIT_ASSERT_VALUES_EQUAL(currentMax, 2);
    }

    Y_UNIT_TEST(WriteAndGetInFlightIcbDefaults) {
        auto icb = MakeBlobDepotIcb();

        TControlWrapper writes(32, 1, 1'000'000'000);
        TControlWrapper gets(32, 1, 1'000'000'000);
        TControlBoard::RegisterSharedControl(writes, icb->BlobDepotControls.S3MaxWritesInFlight);
        TControlBoard::RegisterSharedControl(gets, icb->BlobDepotControls.S3MaxGetsInFlight);

        UNIT_ASSERT_VALUES_EQUAL(i64(writes), 32);
        UNIT_ASSERT_VALUES_EQUAL(i64(gets), 32);
        UNIT_ASSERT_VALUES_EQUAL(S3ControlLimit(writes), 32);
        UNIT_ASSERT_VALUES_EQUAL(S3ControlLimit(gets), 32);
    }

    Y_UNIT_TEST(ObjectsToDeleteAtOnceIcbIsCappedAtS3Limit) {
        auto icb = MakeBlobDepotIcb();

        TControlWrapper batch(10, 1, 1000);
        TControlBoard::RegisterSharedControl(batch, icb->BlobDepotControls.S3MaxObjectsToDeleteAtOnce);
        UNIT_ASSERT_VALUES_EQUAL(i64(batch), 10);

        // S3 DeleteObjects accepts at most 1000 keys, so the ICB must not let us go over it.
        TControlBoard::SetValue(1000, icb->BlobDepotControls.S3MaxObjectsToDeleteAtOnce);
        UNIT_ASSERT_VALUES_EQUAL(i64(batch), 1000);

        TControlBoard::SetValue(1001, icb->BlobDepotControls.S3MaxObjectsToDeleteAtOnce);
        UNIT_ASSERT_VALUES_EQUAL(i64(batch), 1000);
    }
}
