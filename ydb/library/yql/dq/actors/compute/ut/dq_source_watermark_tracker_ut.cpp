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
            TDuration::Seconds(10),
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
        // 1) registering two partitions,
        TInstant systemTime = TInstant::Seconds(10);
        {
            auto ok = tracker.RegisterPartition(0, systemTime);
            UNIT_ASSERT(ok);
        }
        {
            auto ok = tracker.RegisterPartition(1, systemTime);
            UNIT_ASSERT(ok);
        }
        // 2) produce some events with both, receive regular watermarks
        systemTime += TDuration::Seconds(1);
        {
            const auto actual = tracker.NotifyNewPartitionTime(0, TInstant::Seconds(7), systemTime);
            UNIT_ASSERT_VALUES_EQUAL(Nothing(), actual);
        }
        systemTime += TDuration::Seconds(1);
        {
            const auto actual = tracker.NotifyNewPartitionTime(1, TInstant::Seconds(11), systemTime);
            UNIT_ASSERT_VALUES_EQUAL(TInstant::Seconds(5), actual);
        }
        systemTime += TDuration::Seconds(1);
        {
            const auto actual = tracker.HandleIdleness(systemTime);
            UNIT_ASSERT_VALUES_EQUAL(Nothing(), actual);
        }
        systemTime += TDuration::Seconds(11);
        // 3) then simulate idleness in one partition, receive idleness watermark, 
        {
            const auto actual = tracker.NotifyNewPartitionTime(1, TInstant::Seconds(16), systemTime);
            UNIT_ASSERT_VALUES_EQUAL(Nothing(), actual);
        }
        {
            const auto actual = tracker.HandleIdleness(systemTime);
            UNIT_ASSERT_VALUES_EQUAL(TInstant::Seconds(15), actual);
        }
        // 3a) then regular watermarks
        systemTime += TDuration::Seconds(1);
        {
            const auto actual = tracker.NotifyNewPartitionTime(1, TInstant::Seconds(22), systemTime);
            UNIT_ASSERT_VALUES_EQUAL(TInstant::Seconds(20), actual);
        }
        systemTime += TDuration::Seconds(1);
        {
            const auto actual = tracker.HandleIdleness(systemTime);
            UNIT_ASSERT_VALUES_EQUAL(Nothing(), actual);
        }
        // 4) then simulate no activity on both, receive no watermark (regular or idle)
        systemTime += TDuration::Seconds(11);
        {
            const auto actual = tracker.HandleIdleness(systemTime);
            UNIT_ASSERT_VALUES_EQUAL(Nothing(), actual);
        }
        // 5) then similate resume activity on one partition, receive regular watermarks
        systemTime += TDuration::Seconds(11);
        {
            const auto actual = tracker.NotifyNewPartitionTime(0, TInstant::Seconds(22), systemTime);
            UNIT_ASSERT_VALUES_EQUAL(Nothing(), actual);
        }
        systemTime += TDuration::Seconds(1);
        {
            const auto actual = tracker.HandleIdleness(systemTime);
            UNIT_ASSERT_VALUES_EQUAL(Nothing(), actual);
        }
        systemTime += TDuration::Seconds(1);
        {
            const auto actual = tracker.NotifyNewPartitionTime(0, TInstant::Seconds(27), systemTime);
            UNIT_ASSERT_VALUES_EQUAL(TInstant::Seconds(25), actual);
        }
        systemTime += TDuration::Seconds(1);
        {
            const auto actual = tracker.HandleIdleness(systemTime);
            UNIT_ASSERT_VALUES_EQUAL(Nothing(), actual);
        }
        // 6) then simulate resume activity on second partition, receive regular watermarks
        systemTime += TDuration::Seconds(1);
        {
            const auto actual = tracker.NotifyNewPartitionTime(1, TInstant::Seconds(32), systemTime);
            UNIT_ASSERT_VALUES_EQUAL(Nothing(), actual);
        }
        systemTime += TDuration::Seconds(1);
        {
            const auto actual = tracker.HandleIdleness(systemTime);
            UNIT_ASSERT_VALUES_EQUAL(Nothing(), actual);
        }
        systemTime += TDuration::Seconds(1);
        {
            const auto actual = tracker.NotifyNewPartitionTime(0, TInstant::Seconds(34), systemTime);
            UNIT_ASSERT_VALUES_EQUAL(TInstant::Seconds(30), actual);
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
