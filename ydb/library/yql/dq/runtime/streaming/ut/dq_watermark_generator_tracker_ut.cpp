#include <ydb/library/yql/dq/runtime/streaming/dq_watermark_generator_tracker.h>

#include <library/cpp/testing/unittest/registar.h>

#include <utility>
#include <vector>

namespace NYql::NDq {

namespace {

    TPartitionKey Partition(ui64 partitionId) {
        return TPartitionKey{
            .Cluster = "test",
            .PartitionId = partitionId,
        };
    }

    TDqWatermarkGeneratorTracker InitTracker(
        TDqWatermarkGeneratorTracker::TNotifyHandler notifyHandler = {}
    ) {
        auto result = TDqWatermarkGeneratorTracker("Test ");

        result.SetSettings(
            TDuration::Seconds(5),
            true,
            TDuration::Seconds(1),
            TDuration::Seconds(10)
        );
        result.SetNotifyHandler(std::move(notifyHandler));

        return result;
    }

} // anonymous namespace

Y_UNIT_TEST_SUITE(TDqWatermarkGeneratorTrackerTest) {
    Y_UNIT_TEST(HandlesIdlenessWithoutNotifyHandler) {
        auto tracker = InitTracker();

        UNIT_ASSERT(tracker.RegisterPartition(Partition(0), TInstant::Seconds(10)));
        UNIT_ASSERT(tracker.RegisterPartition(Partition(1), TInstant::Seconds(10)));

        UNIT_ASSERT_VALUES_EQUAL(
            Nothing(),
            tracker.NotifyNewPartitionTime(Partition(0), TInstant::Seconds(7), TInstant::MilliSeconds(20'500))
        );

        UNIT_ASSERT_VALUES_EQUAL(Nothing(), tracker.HandleIdleness(TInstant::MilliSeconds(19'999)));
        UNIT_ASSERT_VALUES_EQUAL(TInstant::Seconds(5), tracker.HandleIdleness(TInstant::Seconds(30)));
    }

    Y_UNIT_TEST(UsesNotifyHandlerForScheduling) {
        std::vector<TInstant> scheduledChecks;
        auto tracker = InitTracker([&](TInstant checkTime) {
            scheduledChecks.push_back(checkTime);
        });

        UNIT_ASSERT(tracker.RegisterPartition(Partition(0), TInstant::Seconds(10)));
        UNIT_ASSERT(tracker.RegisterPartition(Partition(1), TInstant::Seconds(10)));
        UNIT_ASSERT_VALUES_EQUAL(std::vector<TInstant>{TInstant::Seconds(30)}, scheduledChecks);

        UNIT_ASSERT_VALUES_EQUAL(
            Nothing(),
            tracker.NotifyNewPartitionTime(Partition(0), TInstant::Seconds(7), TInstant::MilliSeconds(20'500))
        );
        UNIT_ASSERT_VALUES_EQUAL(std::vector<TInstant>{TInstant::Seconds(30)}, scheduledChecks);

        UNIT_ASSERT_VALUES_EQUAL(TInstant::Seconds(5), tracker.HandleIdleness(TInstant::Seconds(30)));
        UNIT_ASSERT_VALUES_EQUAL(
            std::vector<TInstant>({TInstant::Seconds(30), TInstant::Seconds(40)}),
            scheduledChecks
        );
    }
}

} // namespace NYql::NDq
