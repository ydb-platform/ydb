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
            TDuration::Seconds(1),
            TInstant::Now()
        );
    }

    TDqSourceWatermarkTracker<ui32> InitTrackerWithIdleness() {
        return InitTracker(true);
    }
}

Y_UNIT_TEST_SUITE(TDqSourceWatermarkTrackerTest) {

    auto WatermarkMovementCommon(bool idleness, TInstant systemTime) {
        auto tracker = InitTracker(idleness);

        {
            auto ok = tracker.RegisterPartition(0, systemTime);
            UNIT_ASSERT(ok);
        }
        {
            const auto actual = tracker.NotifyNewPartitionTime(0, TInstant::Seconds(7), systemTime);
            UNIT_ASSERT_VALUES_EQUAL(TInstant::Seconds(5), actual);
        }
        {
            const auto actual = tracker.NotifyNewPartitionTime(0, TInstant::Seconds(12), systemTime);
            UNIT_ASSERT_VALUES_EQUAL(TInstant::Seconds(10), actual);
        }
        {
            const auto actual = tracker.NotifyNewPartitionTime(0, TInstant::Seconds(15), systemTime);
            UNIT_ASSERT_VALUES_EQUAL(Nothing(), actual);
        }
        {
            auto ok = tracker.RegisterPartition(1, systemTime);
            UNIT_ASSERT(ok);
        }
        {
            auto ok = tracker.RegisterPartition(1, systemTime);
            UNIT_ASSERT(!ok);
        }
        {
            const auto actual = tracker.NotifyNewPartitionTime(0, TInstant::Seconds(13), systemTime);
            UNIT_ASSERT_VALUES_EQUAL(Nothing(), actual);
        }
        for (auto i = 12; i < 17; ++i) {
            const auto actual = tracker.NotifyNewPartitionTime(1, TInstant::Seconds(i), systemTime);
            UNIT_ASSERT_VALUES_EQUAL_C(Nothing(), actual, i);
        }
        {
            const auto actual = tracker.NotifyNewPartitionTime(1, TInstant::Seconds(17), systemTime);
            UNIT_ASSERT_VALUES_EQUAL(Nothing(), actual);
        }
        {
            const auto actual = tracker.NotifyNewPartitionTime(0, TInstant::Seconds(23), systemTime);
            UNIT_ASSERT_VALUES_EQUAL(TInstant::Seconds(15), actual);
        }
        {
            auto ok = tracker.RegisterPartition(2, systemTime);
            UNIT_ASSERT(ok);
        }
        {
            const auto actual = tracker.NotifyNewPartitionTime(2, TInstant::Seconds(22), systemTime);
            UNIT_ASSERT_VALUES_EQUAL(Nothing(), actual);
        }
        {
            const auto actual = tracker.NotifyNewPartitionTime(1, TInstant::Seconds(31), systemTime);
            UNIT_ASSERT_VALUES_EQUAL(TInstant::Seconds(20), actual);
        }
        {
            const auto actual = tracker.NotifyNewPartitionTime(0, TInstant::Seconds(42), systemTime);
            UNIT_ASSERT_VALUES_EQUAL(Nothing(), actual);
        }
        {
            const auto actual = tracker.NotifyNewPartitionTime(2, TInstant::Seconds(26), systemTime);
            UNIT_ASSERT_VALUES_EQUAL(TInstant::Seconds(25), actual);
        }
        return tracker;
    }

    Y_UNIT_TEST(WatermarkMovement) {
        WatermarkMovementCommon(false, TInstant::Seconds(10));
    }

    Y_UNIT_TEST(WatermarkMovementWithIdleness) {
        WatermarkMovementCommon(true, TInstant::Seconds(10));
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

#if 0
        {
            auto ok = tracker.RegisterPartition(0, TInstant::Zero());
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
#else
        // TODO compose a new test; semantic changed: partition idleness no longer produces fake events
        // so, new test scenario should be:
        // 1) registering two partitions,
        // 2) produce some events with both, receive regular watermarks
        // 3) then simulate idleness in one partition, receive idleness watermark, then regular watermarks
        // 4) then simulate no activity on both, receive no watermark (regular or idle)
        // 5) then similate resume activity on one partition, receive regular watermarks
        // 6) then simulate resume activity on second partition, receive regular watermarks
#endif
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
