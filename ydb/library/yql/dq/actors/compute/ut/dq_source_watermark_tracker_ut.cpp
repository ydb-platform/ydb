#include <ydb/library/yql/dq/actors/compute/dq_source_watermark_tracker.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYql::NDq {

namespace {
    TDqSourceWatermarkTracker<ui32> InitTracker(
        TDuration granularitty = TDuration::Seconds(2),
        TInstant startWatermark = TInstant::Seconds(10),
        TDuration lateArrivalDelay = TDuration::Seconds(4),
        bool IdlePartitionsEnabled = false,
        TInstant systemTime = TInstant())
    {
        return TDqSourceWatermarkTracker<ui32>(
            granularitty,
            startWatermark,
            IdlePartitionsEnabled,
            lateArrivalDelay,
            systemTime);
    }

    TDqSourceWatermarkTracker<ui32> InitTrackerPreset2() {
        return InitTracker(
            TDuration::Seconds(5),
            TInstant::Seconds(10),
            TDuration::Seconds(2),
            true,
            TInstant::Seconds(10));
    }
}

Y_UNIT_TEST_SUITE(TDqSourceWatermarkTrackerTest) {
    Y_UNIT_TEST(StartWatermark1) {
        auto tracker = InitTracker(TDuration::Seconds(2), TInstant::Seconds(11));

        const auto actual1 = tracker.NotifyNewPartitionTime(0, TInstant::Seconds(11), TInstant());
        UNIT_ASSERT_VALUES_EQUAL(actual1, TInstant::Seconds(10));

        const auto actual2 = tracker.NotifyNewPartitionTime(1, TInstant::Seconds(11), TInstant());
        UNIT_ASSERT_VALUES_EQUAL(actual2.Defined(), false);
    }

    Y_UNIT_TEST(StartWatermark2) {
        auto tracker = InitTracker();

        const auto actual1 = tracker.NotifyNewPartitionTime(0, TInstant::Seconds(12), TInstant());
        UNIT_ASSERT_VALUES_EQUAL(actual1, TInstant::Seconds(10));

        const auto actual2 = tracker.NotifyNewPartitionTime(1, TInstant::Seconds(14), TInstant());
        UNIT_ASSERT_VALUES_EQUAL(actual2.Defined(), false);

        const auto actual3 = tracker.NotifyNewPartitionTime(1, TInstant::Seconds(16), TInstant());
        UNIT_ASSERT_VALUES_EQUAL(actual3, TInstant::Seconds(12));
    }

    Y_UNIT_TEST(StartWatermark3) {
        auto tracker = InitTrackerPreset2();

        const auto actual1 = tracker.NotifyNewPartitionTime(0, TInstant::Seconds(19), TInstant());
        UNIT_ASSERT_VALUES_EQUAL(actual1, TInstant::Seconds(15));
    }

    Y_UNIT_TEST(WatermarkMovement1) {
        auto tracker = InitTracker();

        tracker.NotifyNewPartitionTime(0, TInstant::Seconds(12), TInstant());
        const auto actual = tracker.NotifyNewPartitionTime(1, TInstant::Seconds(16), TInstant());
        UNIT_ASSERT_VALUES_EQUAL(actual, TInstant::Seconds(12));
    }

    Y_UNIT_TEST(WatermarkMovement2) {
        auto tracker = InitTracker();

        tracker.NotifyNewPartitionTime(0, TInstant::Seconds(13), TInstant());
        const auto actual = tracker.NotifyNewPartitionTime(1, TInstant::Seconds(16), TInstant());
        UNIT_ASSERT_VALUES_EQUAL(actual, TInstant::Seconds(12));
    }

    Y_UNIT_TEST(WatermarkMovement3) {
        auto tracker = InitTracker();

        tracker.NotifyNewPartitionTime(0, TInstant::Seconds(16), TInstant());
        const auto actual = tracker.NotifyNewPartitionTime(1, TInstant::Seconds(13), TInstant());
        UNIT_ASSERT_VALUES_EQUAL(actual.Defined(), false);
    }

    Y_UNIT_TEST(WatermarkMovement4) {
        auto tracker = InitTracker();

        const auto actual1 = tracker.NotifyNewPartitionTime(0, TInstant::Seconds(12), TInstant());
        UNIT_ASSERT_VALUES_EQUAL(actual1, TInstant::Seconds(10));

        const auto actual2 = tracker.NotifyNewPartitionTime(1, TInstant::Seconds(16), TInstant());
        UNIT_ASSERT_VALUES_EQUAL(actual2, TInstant::Seconds(12));

        const auto actual3 = tracker.NotifyNewPartitionTime(1, TInstant::Seconds(18), TInstant());
        UNIT_ASSERT_VALUES_EQUAL(actual3, TInstant::Seconds(14));
    }

    Y_UNIT_TEST(IdleFirstShouldReturnStartWatermark) {
        auto tracker = InitTrackerPreset2();

        const auto watermark = tracker.HandleIdleness(TInstant::Seconds(10));
        UNIT_ASSERT_VALUES_EQUAL(watermark, TInstant::Seconds(10));
        const auto watermark2 = tracker.HandleIdleness(TInstant::Seconds(10));
        UNIT_ASSERT(!watermark2.Defined());
    }

    Y_UNIT_TEST(Idle1) {
        auto tracker = InitTrackerPreset2();

        const auto watermark = tracker.NotifyNewPartitionTime(0, TInstant::Seconds(22), TInstant::Seconds(22));
        UNIT_ASSERT_VALUES_EQUAL(watermark, TInstant::Seconds(20));

        const auto watermark2 = tracker.HandleIdleness(TInstant::Seconds(22));
        UNIT_ASSERT_VALUES_EQUAL(watermark2, TMaybe<TInstant>());

        const auto watermark3 = tracker.HandleIdleness(TInstant::Seconds(23));
        UNIT_ASSERT_VALUES_EQUAL(watermark3, TMaybe<TInstant>());

        const auto watermark4 = tracker.HandleIdleness(TInstant::Seconds(25));
        UNIT_ASSERT_VALUES_EQUAL(watermark4, TMaybe<TInstant>());

        const auto watermark5 = tracker.HandleIdleness(TInstant::Seconds(30));
        UNIT_ASSERT_VALUES_EQUAL(watermark5, TInstant::Seconds(25));
    }

    Y_UNIT_TEST(IdleNextCheckAt) {
        auto tracker = InitTrackerPreset2();

        const auto nextCheckAt1 = tracker.GetNextIdlenessCheckAt(TInstant::Seconds(10));
        UNIT_ASSERT_VALUES_EQUAL(nextCheckAt1, TInstant::Seconds(15));

        const auto nextCheckAt2 = tracker.GetNextIdlenessCheckAt(TInstant::Seconds(11));
        UNIT_ASSERT_VALUES_EQUAL(nextCheckAt2, TInstant::Seconds(15));

        const auto nextCheckAt3 = tracker.GetNextIdlenessCheckAt(TInstant::Seconds(15));
        UNIT_ASSERT_VALUES_EQUAL(nextCheckAt3, TInstant::Seconds(20));
    }
}

}
