#include <ydb/core/persqueue/pqtablet/partition/consumer_offset_tracker.h>

#include <library/cpp/testing/gtest/gtest.h>

using namespace NKikimr::NPQ;

static const std::vector<TDataKey> KEYS = {
    {
        .Key = TKey::ForBody(TKey::TypeData, TPartitionId(0), 10, 0, 0, 0),
        .Timestamp = TInstant::Seconds(100)
    },
    {
        .Key = TKey::ForBody(TKey::TypeData, TPartitionId(0), 20, 0, 0, 0),
        .Timestamp = TInstant::Seconds(200)
    },
    {
        .Key = TKey::ForBody(TKey::TypeData, TPartitionId(0), 30, 0, 0, 0),
        .Timestamp = TInstant::Seconds(300)
    },
    {
        .Key = TKey::ForBody(TKey::TypeData, TPartitionId(0), 40, 5, 0, 0),
        .Timestamp = TInstant::Seconds(400)
    },
    {
        .Key = TKey::ForBody(TKey::TypeData, TPartitionId(0), 50, 0, 0, 0),
        .Timestamp = TInstant::Seconds(500)
    },
};

static const TInstant NOW = TInstant::Seconds(800);

static std::vector<std::tuple<size_t, TDataKey, TDataKey>> Pairs() {
    std::vector<std::tuple<size_t, TDataKey, TDataKey>> r;
    for (size_t i = 1; i < KEYS.size(); ++i) {
        r.emplace_back(i, KEYS[i - 1], KEYS[i - 0]);
    }
    return r;
}

TEST(TImportantConsumerOffsetTrackerTest, EmptyConsumersList) {
    TImportantConsumerOffsetTracker tracker({});

    for (const auto& [idx, currentKey, nextKey] : Pairs()) {
        EXPECT_FALSE(tracker.ShouldKeepCurrentKey(currentKey, nextKey, NOW)) << "case=" << idx;
    }
}

TEST(TImportantConsumerOffsetTrackerTest, Unbound) {
    std::vector<TImportantConsumerOffsetTracker::TConsumerOffset> consumers{
        {.AvailabilityPeriod = TDuration::Max(), .Offset = 35},
    };
    TImportantConsumerOffsetTracker tracker(std::move(consumers));
    EXPECT_FALSE(tracker.ShouldKeepCurrentKey(KEYS[0], KEYS[1], NOW));
    EXPECT_FALSE(tracker.ShouldKeepCurrentKey(KEYS[1], KEYS[2], NOW));
    EXPECT_TRUE(tracker.ShouldKeepCurrentKey(KEYS[2], KEYS[3], NOW));
    EXPECT_TRUE(tracker.ShouldKeepCurrentKey(KEYS[3], KEYS[4], NOW));
}

TEST(TImportantConsumerOffsetTrackerTest, Expired) {
    std::vector<TImportantConsumerOffsetTracker::TConsumerOffset> consumers{
        {.AvailabilityPeriod = TDuration::Seconds(15), .Offset = 35},
    };
    TImportantConsumerOffsetTracker tracker(std::move(consumers));
    EXPECT_FALSE(tracker.ShouldKeepCurrentKey(KEYS[0], KEYS[1], NOW));
    EXPECT_FALSE(tracker.ShouldKeepCurrentKey(KEYS[1], KEYS[2], NOW));
    EXPECT_FALSE(tracker.ShouldKeepCurrentKey(KEYS[2], KEYS[3], NOW));
    EXPECT_FALSE(tracker.ShouldKeepCurrentKey(KEYS[3], KEYS[4], NOW));
}

TEST(TImportantConsumerOffsetTrackerTest, UnboundAndExpired) {
    std::vector<TImportantConsumerOffsetTracker::TConsumerOffset> consumers{
        {.AvailabilityPeriod = TDuration::Seconds(15), .Offset = 35},
        {.AvailabilityPeriod = TDuration::Max(), .Offset = 25},
    };
    TImportantConsumerOffsetTracker tracker(std::move(consumers));
    EXPECT_FALSE(tracker.ShouldKeepCurrentKey(KEYS[0], KEYS[1], NOW));
    EXPECT_TRUE(tracker.ShouldKeepCurrentKey(KEYS[1], KEYS[2], NOW));
    EXPECT_TRUE(tracker.ShouldKeepCurrentKey(KEYS[2], KEYS[3], NOW));
    EXPECT_TRUE(tracker.ShouldKeepCurrentKey(KEYS[3], KEYS[4], NOW));
}


TEST(TImportantConsumerOffsetTrackerTest, ReadUnboundAndExpired) {
    std::vector<TImportantConsumerOffsetTracker::TConsumerOffset> consumers{
        {.AvailabilityPeriod = TDuration::Seconds(15), .Offset = 35},
        {.AvailabilityPeriod = TDuration::Max(), .Offset = 75},
    };
    TImportantConsumerOffsetTracker tracker(std::move(consumers));
    EXPECT_FALSE(tracker.ShouldKeepCurrentKey(KEYS[0], KEYS[1], NOW));
    EXPECT_FALSE(tracker.ShouldKeepCurrentKey(KEYS[1], KEYS[2], NOW));
    EXPECT_FALSE(tracker.ShouldKeepCurrentKey(KEYS[2], KEYS[3], NOW));
    EXPECT_FALSE(tracker.ShouldKeepCurrentKey(KEYS[3], KEYS[4], NOW));
}


TEST(TImportantConsumerOffsetTrackerTest, ReadUnboundAndWaiting300) {
    std::vector<TImportantConsumerOffsetTracker::TConsumerOffset> consumers{
        {.AvailabilityPeriod = TDuration::Seconds(300), .Offset = 35},
        {.AvailabilityPeriod = TDuration::Max(), .Offset = 75},
    };
    TImportantConsumerOffsetTracker tracker(std::move(consumers));
    EXPECT_FALSE(tracker.ShouldKeepCurrentKey(KEYS[0], KEYS[1], NOW));
    EXPECT_FALSE(tracker.ShouldKeepCurrentKey(KEYS[1], KEYS[2], NOW));
    EXPECT_FALSE(tracker.ShouldKeepCurrentKey(KEYS[2], KEYS[3], NOW));
    EXPECT_FALSE(tracker.ShouldKeepCurrentKey(KEYS[3], KEYS[4], NOW));
}


TEST(TImportantConsumerOffsetTrackerTest, ReadUnboundAndWaiting400) {
    std::vector<TImportantConsumerOffsetTracker::TConsumerOffset> consumers{
        {.AvailabilityPeriod = TDuration::Seconds(400), .Offset = 35},
        {.AvailabilityPeriod = TDuration::Max(), .Offset = 75},
    };
    TImportantConsumerOffsetTracker tracker(std::move(consumers));
    EXPECT_FALSE(tracker.ShouldKeepCurrentKey(KEYS[0], KEYS[1], NOW));
    EXPECT_FALSE(tracker.ShouldKeepCurrentKey(KEYS[1], KEYS[2], NOW));
    EXPECT_FALSE(tracker.ShouldKeepCurrentKey(KEYS[2], KEYS[3], NOW));
    EXPECT_TRUE(tracker.ShouldKeepCurrentKey(KEYS[3], KEYS[4], NOW));
}

TEST(TImportantConsumerOffsetTrackerTest, ReadUnboundAndWaiting500) {
    std::vector<TImportantConsumerOffsetTracker::TConsumerOffset> consumers{
        {.AvailabilityPeriod = TDuration::Seconds(500), .Offset = 35},
        {.AvailabilityPeriod = TDuration::Max(), .Offset = 75},
    };
    TImportantConsumerOffsetTracker tracker(std::move(consumers));
    EXPECT_FALSE(tracker.ShouldKeepCurrentKey(KEYS[0], KEYS[1], NOW));
    EXPECT_FALSE(tracker.ShouldKeepCurrentKey(KEYS[1], KEYS[2], NOW));
    EXPECT_TRUE(tracker.ShouldKeepCurrentKey(KEYS[2], KEYS[3], NOW));
    EXPECT_TRUE(tracker.ShouldKeepCurrentKey(KEYS[3], KEYS[4], NOW));
}

TEST(TImportantConsumerOffsetTrackerTest, Waiting400And500) {
    std::vector<TImportantConsumerOffsetTracker::TConsumerOffset> consumers{
        {.AvailabilityPeriod = TDuration::Seconds(400), .Offset = 35},
        {.AvailabilityPeriod = TDuration::Seconds(500), .Offset = 35},
    };
    TImportantConsumerOffsetTracker tracker(std::move(consumers));
    EXPECT_FALSE(tracker.ShouldKeepCurrentKey(KEYS[0], KEYS[1], NOW));
    EXPECT_FALSE(tracker.ShouldKeepCurrentKey(KEYS[1], KEYS[2], NOW));
    EXPECT_TRUE(tracker.ShouldKeepCurrentKey(KEYS[2], KEYS[3], NOW));
    EXPECT_TRUE(tracker.ShouldKeepCurrentKey(KEYS[3], KEYS[4], NOW));
}

TEST(TImportantConsumerOffsetTrackerTest, Waiting400AndRead500) {
    std::vector<TImportantConsumerOffsetTracker::TConsumerOffset> consumers{
        {.AvailabilityPeriod = TDuration::Seconds(400), .Offset = 35},
        {.AvailabilityPeriod = TDuration::Seconds(500), .Offset = 45},
    };
    TImportantConsumerOffsetTracker tracker(std::move(consumers));
    EXPECT_FALSE(tracker.ShouldKeepCurrentKey(KEYS[0], KEYS[1], NOW));
    EXPECT_FALSE(tracker.ShouldKeepCurrentKey(KEYS[1], KEYS[2], NOW));
    EXPECT_FALSE(tracker.ShouldKeepCurrentKey(KEYS[2], KEYS[3], NOW));
    EXPECT_TRUE(tracker.ShouldKeepCurrentKey(KEYS[3], KEYS[4], NOW));
}

TEST(TImportantConsumerOffsetTrackerTest, ExactMatchSingleConsumerMaxRetention) {
    std::vector<TImportantConsumerOffsetTracker::TConsumerOffset> consumers{
        {.AvailabilityPeriod = TDuration::Max(), .Offset = 30},
    };
    TImportantConsumerOffsetTracker tracker(std::move(consumers));
    EXPECT_FALSE(tracker.ShouldKeepCurrentKey(KEYS[0], KEYS[1], NOW));
    EXPECT_FALSE(tracker.ShouldKeepCurrentKey(KEYS[1], KEYS[2], NOW));
    EXPECT_TRUE(tracker.ShouldKeepCurrentKey(KEYS[2], KEYS[3], NOW));
    EXPECT_TRUE(tracker.ShouldKeepCurrentKey(KEYS[3], KEYS[4], NOW));
}

TEST(TImportantConsumerOffsetTrackerTest, ExactMatchSingleConsumerFiniteRetention) {
    std::vector<TImportantConsumerOffsetTracker::TConsumerOffset> consumers{
        {.AvailabilityPeriod = TDuration::Seconds(500), .Offset = 30},
    };
    TImportantConsumerOffsetTracker tracker(std::move(consumers));
    EXPECT_FALSE(tracker.ShouldKeepCurrentKey(KEYS[0], KEYS[1], NOW));
    EXPECT_FALSE(tracker.ShouldKeepCurrentKey(KEYS[1], KEYS[2], NOW));
    EXPECT_TRUE(tracker.ShouldKeepCurrentKey(KEYS[2], KEYS[3], NOW));
    EXPECT_TRUE(tracker.ShouldKeepCurrentKey(KEYS[3], KEYS[4], NOW));
}

TEST(TImportantConsumerOffsetTrackerTest, ExactMatchMultipleConsumers) {
    std::vector<TImportantConsumerOffsetTracker::TConsumerOffset> consumers{
        {.AvailabilityPeriod = TDuration::Seconds(400), .Offset = 20},
        {.AvailabilityPeriod = TDuration::Max(), .Offset = 30},
    };
    TImportantConsumerOffsetTracker tracker(std::move(consumers));
    EXPECT_FALSE(tracker.ShouldKeepCurrentKey(KEYS[0], KEYS[1], NOW));
    EXPECT_FALSE(tracker.ShouldKeepCurrentKey(KEYS[1], KEYS[2], NOW));
    EXPECT_TRUE(tracker.ShouldKeepCurrentKey(KEYS[2], KEYS[3], NOW));
    EXPECT_TRUE(tracker.ShouldKeepCurrentKey(KEYS[3], KEYS[4], NOW));
}

TEST(TImportantConsumerOffsetTrackerTest, MultipleExactMatches) {
    std::vector<TImportantConsumerOffsetTracker::TConsumerOffset> consumers{
        {.AvailabilityPeriod = TDuration::Seconds(500), .Offset = 20},
        {.AvailabilityPeriod = TDuration::Max(), .Offset = 30},
    };
    TImportantConsumerOffsetTracker tracker(std::move(consumers));
    EXPECT_FALSE(tracker.ShouldKeepCurrentKey(KEYS[0], KEYS[1], NOW));
    EXPECT_FALSE(tracker.ShouldKeepCurrentKey(KEYS[1], KEYS[2], NOW));
    EXPECT_TRUE(tracker.ShouldKeepCurrentKey(KEYS[2], KEYS[3], NOW));
    EXPECT_TRUE(tracker.ShouldKeepCurrentKey(KEYS[3], KEYS[4], NOW));
}

TEST(TImportantConsumerOffsetTrackerTest, UnboundKeepNextBlob) {
    std::vector<TImportantConsumerOffsetTracker::TConsumerOffset> consumers{
        {.AvailabilityPeriod = TDuration::Max(), .Offset = 40},
    };
    TImportantConsumerOffsetTracker tracker(std::move(consumers));
    EXPECT_FALSE(tracker.ShouldKeepCurrentKey(KEYS[0], KEYS[1], NOW));
    EXPECT_FALSE(tracker.ShouldKeepCurrentKey(KEYS[1], KEYS[2], NOW));
    EXPECT_TRUE(tracker.ShouldKeepCurrentKey(KEYS[2], KEYS[3], NOW));
    EXPECT_TRUE(tracker.ShouldKeepCurrentKey(KEYS[3], KEYS[4], NOW));
}
