#include <ydb/library/yql/dq/actors/compute/dq_source_watermark_tracker.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYql::NDq {

namespace {
    TDqSourceWatermarkTracker<ui32> InitTracker(
        bool idlePartitionsEnabled = false
    ) {
        return TDqSourceWatermarkTracker<ui32>(
            TDuration::Seconds(5),
            idlePartitionsEnabled,
            TDuration::Seconds(1),
            TInstant::Now()
        );
    }

    TDqSourceWatermarkTracker<ui32> InitTrackerWithIdleness() {
        return InitTracker(true);
    }
}

Y_UNIT_TEST_SUITE(TDqSourceWatermarkTrackerTest) {

    auto WatermarkMovementCommon(bool idleness) {
        auto tracker = InitTracker(idleness);

        {
            auto ok = tracker.RegisterPartition(0);
            UNIT_ASSERT(ok);
        }
        {
            const auto actual = tracker.NotifyNewPartitionTime(0, TInstant::Seconds(7), {});
            UNIT_ASSERT_VALUES_EQUAL(TInstant::Seconds(5), actual);
        }
        {
            const auto actual = tracker.NotifyNewPartitionTime(0, TInstant::Seconds(12), {});
            UNIT_ASSERT_VALUES_EQUAL(TInstant::Seconds(10), actual);
        }
        {
            const auto actual = tracker.NotifyNewPartitionTime(0, TInstant::Seconds(15), {});
            UNIT_ASSERT_VALUES_EQUAL(Nothing(), actual);
        }
        {
            auto ok = tracker.RegisterPartition(1);
            UNIT_ASSERT(ok);
        }
        {
            auto ok = tracker.RegisterPartition(1);
            UNIT_ASSERT(!ok);
        }
        {
            const auto actual = tracker.NotifyNewPartitionTime(0, TInstant::Seconds(13), {});
            UNIT_ASSERT_VALUES_EQUAL(Nothing(), actual);
        }
        for (auto i = 12; i < 17; ++i) {
            const auto actual = tracker.NotifyNewPartitionTime(1, TInstant::Seconds(i), {});
            UNIT_ASSERT_VALUES_EQUAL_C(Nothing(), actual, i);
        }
        {
            const auto actual = tracker.NotifyNewPartitionTime(1, TInstant::Seconds(17), {});
            UNIT_ASSERT_VALUES_EQUAL(Nothing(), actual);
        }
        {
            const auto actual = tracker.NotifyNewPartitionTime(0, TInstant::Seconds(23), {});
            UNIT_ASSERT_VALUES_EQUAL(TInstant::Seconds(15), actual);
        }
        {
            auto ok = tracker.RegisterPartition(2);
            UNIT_ASSERT(ok);
        }
        {
            const auto actual = tracker.NotifyNewPartitionTime(2, TInstant::Seconds(22), {});
            UNIT_ASSERT_VALUES_EQUAL(Nothing(), actual);
        }
        {
            const auto actual = tracker.NotifyNewPartitionTime(1, TInstant::Seconds(31), {});
            UNIT_ASSERT_VALUES_EQUAL(TInstant::Seconds(20), actual);
        }
        {
            const auto actual = tracker.NotifyNewPartitionTime(0, TInstant::Seconds(42), {});
            UNIT_ASSERT_VALUES_EQUAL(Nothing(), actual);
        }
        {
            const auto actual = tracker.NotifyNewPartitionTime(2, TInstant::Seconds(26), {});
            UNIT_ASSERT_VALUES_EQUAL(TInstant::Seconds(25), actual);
        }
        return tracker;
    }

    Y_UNIT_TEST(WatermarkMovement) {
        WatermarkMovementCommon(false);
    }

    Y_UNIT_TEST(WatermarkMovementWithIdleness) {
        WatermarkMovementCommon(true);
    }

    Y_UNIT_TEST(IdleFirstShouldReturnNothing) {
        auto tracker = InitTrackerWithIdleness();

        {
            const auto actual = tracker.HandleIdleness(TInstant::Seconds(10));
            UNIT_ASSERT_VALUES_EQUAL(Nothing(), actual);
        }
    }

    Y_UNIT_TEST(Idle1) {
        auto tracker = InitTrackerWithIdleness();

        {
            auto ok = tracker.RegisterPartition(0);
            UNIT_ASSERT(ok);
        }
        {
            const auto actual = tracker.NotifyNewPartitionTime(0, TInstant::Seconds(2), TInstant::Seconds(12));
            UNIT_ASSERT_VALUES_EQUAL(TInstant::Seconds(0), actual);
        }
        for (auto i = 12; i < 15; ++i) {
            const auto actual = tracker.HandleIdleness(TInstant::Seconds(i));
            UNIT_ASSERT_VALUES_EQUAL_C(Nothing(), actual, i);
        }
        {
            const auto actual = tracker.HandleIdleness(TInstant::Seconds(15));
            UNIT_ASSERT_VALUES_EQUAL(TInstant::Seconds(10), actual);
        }
        {
            const auto actual = tracker.HandleIdleness(TInstant::Seconds(20));
            UNIT_ASSERT_VALUES_EQUAL(TInstant::Seconds(15), actual);
        }
    }

    Y_UNIT_TEST(IdleNextCheckAt) {
        auto tracker = InitTrackerWithIdleness();

        {
            const auto actual = tracker.GetNextIdlenessCheckAt(TInstant::Seconds(10));
            UNIT_ASSERT_VALUES_EQUAL(TInstant::Seconds(15), actual);
        }
        for (auto i = 10; i < 15; ++i) {
            const auto actual = tracker.GetNextIdlenessCheckAt(TInstant::Seconds(i));
            UNIT_ASSERT_VALUES_EQUAL_C(TInstant::Seconds(15), actual, i);
        }
        {
            const auto actual = tracker.GetNextIdlenessCheckAt(TInstant::Seconds(15));
            UNIT_ASSERT_VALUES_EQUAL(TInstant::Seconds(20), actual);
        }
        {
            const auto actual = tracker.GetNextIdlenessCheckAt(TInstant::Seconds(20));
            UNIT_ASSERT_VALUES_EQUAL(TInstant::Seconds(25), actual);
        }
    }
}

} // namespace NYql::NDq
