#include <ydb/library/yql/dq/actors/compute/dq_source_watermark_tracker.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYql::NDq {

Y_UNIT_TEST_SUITE(TDqSourceWatermarkTrackerTest) {
    Y_UNIT_TEST(StartWatermark) {
        TDqSourceWatermarkTracker<ui32> tracker(TDuration::Seconds(2), TInstant::Seconds(11), 2);

        const auto actual1 = tracker.NotifyNewPartitionTime(0, TInstant::Seconds(11));
        UNIT_ASSERT_VALUES_EQUAL(actual1, TInstant::Seconds(10));

        const auto actual2 = tracker.NotifyNewPartitionTime(1, TInstant::Seconds(11));
        UNIT_ASSERT_VALUES_EQUAL(actual2.Defined(), false); // Start watermark was returned already, we shouldn't return it 2-nd time
    }

    Y_UNIT_TEST(WatermarkMovement1) {
        const auto startWatermark = TInstant::Seconds(10);

        TDqSourceWatermarkTracker<ui32> tracker(TDuration::Seconds(2), startWatermark, 2);

        tracker.NotifyNewPartitionTime(0, TInstant::Seconds(12));
        const auto actual = tracker.NotifyNewPartitionTime(1, TInstant::Seconds(12));
        UNIT_ASSERT_VALUES_EQUAL(actual, TInstant::Seconds(12));
    }

    Y_UNIT_TEST(WatermarkMovement2) {
        const auto startWatermark = TInstant::Seconds(10);

        TDqSourceWatermarkTracker<ui32> tracker(TDuration::Seconds(2), startWatermark, 2);

        tracker.NotifyNewPartitionTime(0, TInstant::Seconds(12));
        const auto actual = tracker.NotifyNewPartitionTime(1, TInstant::Seconds(13));
        UNIT_ASSERT_VALUES_EQUAL(actual, TInstant::Seconds(12));
    }

    Y_UNIT_TEST(WatermarkMovement3) {
        const auto startWatermark = TInstant::Seconds(10);

        TDqSourceWatermarkTracker<ui32> tracker(TDuration::Seconds(2), startWatermark, 2);

        tracker.NotifyNewPartitionTime(0, TInstant::Seconds(13));
        const auto actual = tracker.NotifyNewPartitionTime(1, TInstant::Seconds(13));
        UNIT_ASSERT_VALUES_EQUAL(actual, TInstant::Seconds(12));
    }

    Y_UNIT_TEST(WatermarkMovement4) {
        const auto startWatermark = TInstant::Seconds(10);

        TDqSourceWatermarkTracker<ui32> tracker(TDuration::Seconds(2), startWatermark, 2);

        tracker.NotifyNewPartitionTime(0, TInstant::Seconds(12));
        const auto actual = tracker.NotifyNewPartitionTime(1, TInstant::Seconds(14));
        UNIT_ASSERT_VALUES_EQUAL(actual, TInstant::Seconds(12));
    }

    Y_UNIT_TEST(WatermarkFarMovement) {
        const auto startWatermark = TInstant::Seconds(10);

        TDqSourceWatermarkTracker<ui32> tracker(TDuration::Seconds(2), startWatermark, 2);

        tracker.NotifyNewPartitionTime(0, TInstant::Seconds(12));
        tracker.NotifyNewPartitionTime(1, TInstant::Seconds(30));
        const auto actual = tracker.NotifyNewPartitionTime(0, TInstant::Seconds(30));
        UNIT_ASSERT_VALUES_EQUAL(actual, TInstant::Seconds(30));
    }

    Y_UNIT_TEST(WaitExpectedPartitionsCount) {
        const auto startWatermark = TInstant::Seconds(10);

        TDqSourceWatermarkTracker<ui32> tracker(TDuration::Seconds(2), startWatermark, 2);

        tracker.NotifyNewPartitionTime(0, TInstant::Seconds(12));
        const auto actual1 = tracker.NotifyNewPartitionTime(0, TInstant::Seconds(30));
        UNIT_ASSERT_VALUES_EQUAL(actual1.Defined(), false); // Since expectedPartitionsCount is 2, we shouldn't move watermark

        const auto actual2 = tracker.NotifyNewPartitionTime(1, TInstant::Seconds(30));
        UNIT_ASSERT_VALUES_EQUAL(actual2,  TInstant::Seconds(30));
    }
}

}
