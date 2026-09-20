#include <gtest/gtest.h>

#include <yt/yt/library/profiling/summary.h>

namespace NYT::NProfiling {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TSummarySnapshotTest, RecordUpdatesLast)
{
    TSummarySnapshot<int> summary;

    summary.Record(10);
    EXPECT_EQ(10, summary.Sum());
    EXPECT_EQ(10, summary.Min());
    EXPECT_EQ(10, summary.Max());
    EXPECT_EQ(10, summary.Last());
    EXPECT_EQ(1, summary.Count());

    summary.Record(5);
    EXPECT_EQ(15, summary.Sum());
    EXPECT_EQ(5, summary.Min());
    EXPECT_EQ(10, summary.Max());
    EXPECT_EQ(5, summary.Last());
    EXPECT_EQ(2, summary.Count());
}

TEST(TSummarySnapshotTest, AddUsesLastFromRightHandNonEmptySnapshot)
{
    TSummarySnapshot<int> summary;
    summary.Record(10);
    summary.Record(5);

    TSummarySnapshot<int> other;
    other.Record(7);
    other.Record(11);

    summary.Add(other);

    EXPECT_EQ(33, summary.Sum());
    EXPECT_EQ(5, summary.Min());
    EXPECT_EQ(11, summary.Max());
    EXPECT_EQ(11, summary.Last());
    EXPECT_EQ(4, summary.Count());
}

TEST(TSummarySnapshotTest, AddPreservesLastWhenRightHandSnapshotIsEmpty)
{
    TSummarySnapshot<int> summary;
    summary.Record(10);
    summary.Record(5);

    summary.Add(TSummarySnapshot<int>());

    EXPECT_EQ(15, summary.Sum());
    EXPECT_EQ(5, summary.Min());
    EXPECT_EQ(10, summary.Max());
    EXPECT_EQ(5, summary.Last());
    EXPECT_EQ(2, summary.Count());
}

TEST(TSummarySnapshotTest, AddToEmptySnapshotCopiesLast)
{
    TSummarySnapshot<int> summary;

    TSummarySnapshot<int> other;
    other.Record(7);
    other.Record(11);

    summary += other;

    EXPECT_EQ(18, summary.Sum());
    EXPECT_EQ(7, summary.Min());
    EXPECT_EQ(11, summary.Max());
    EXPECT_EQ(11, summary.Last());
    EXPECT_EQ(2, summary.Count());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NProfiling
