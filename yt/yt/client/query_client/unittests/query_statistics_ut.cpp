#include <yt/yt/client/query_client/query_statistics.h>

#include <yt/yt_proto/yt/client/query_client/proto/query_statistics.pb.h>

#include <yt/yt/core/test_framework/framework.h>

#include <gtest/gtest.h>

namespace NYT::NQueryClient {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TQueryStatisticsScanOrderTest, MergeCarriesScanOrderIntoFreshInstance)
{
    // The coordinators re-aggregate the top-level statistics into a default-constructed instance
    // whenever the aggregation is depth-wise, which is the default.
    TQueryStatistics statistics;
    statistics.ScanOrder = EReportedScanOrder::Reversed;

    TQueryStatistics aggregated;
    aggregated.Merge(statistics);

    EXPECT_EQ(EReportedScanOrder::Reversed, aggregated.ScanOrder);
}

TEST(TQueryStatisticsScanOrderTest, MergeKeepsScanOrderWhenBothSidesAgree)
{
    TQueryStatistics statistics;
    statistics.ScanOrder = EReportedScanOrder::Ordered;

    TQueryStatistics other;
    other.ScanOrder = EReportedScanOrder::Ordered;

    statistics.Merge(other);

    EXPECT_EQ(EReportedScanOrder::Ordered, statistics.ScanOrder);
}

TEST(TQueryStatisticsScanOrderTest, MergeReportsMixedWhenScanOrdersDisagree)
{
    TQueryStatistics statistics;
    statistics.ScanOrder = EReportedScanOrder::Unordered;

    TQueryStatistics other;
    other.ScanOrder = EReportedScanOrder::Reversed;

    statistics.Merge(other);

    EXPECT_EQ(EReportedScanOrder::Mixed, statistics.ScanOrder);
}

TEST(TQueryStatisticsScanOrderTest, MergeLeavesScanOrderUnknownWhenNeitherSideHasOne)
{
    TQueryStatistics statistics;
    statistics.Merge(TQueryStatistics());

    EXPECT_EQ(EReportedScanOrder::Unknown, statistics.ScanOrder);
}

TEST(TQueryStatisticsScanOrderTest, InnerStatisticsDoNotAffectTheTopLevelScanOrder)
{
    // Collapsed subqueries need not share a scan order: join prefetch subqueries sit alongside
    // the plan ones.
    TQueryStatistics ordered;
    ordered.ScanOrder = EReportedScanOrder::Ordered;

    TQueryStatistics unordered;
    unordered.ScanOrder = EReportedScanOrder::Unordered;

    TQueryStatistics statistics;
    statistics.ScanOrder = EReportedScanOrder::Reversed;
    statistics.AddInnerStatistics(ordered);
    statistics.AddInnerStatistics(unordered);

    TQueryStatistics aggregated;
    aggregated.Merge(statistics);

    EXPECT_EQ(EReportedScanOrder::Reversed, aggregated.ScanOrder);
    ASSERT_EQ(1, std::ssize(aggregated.InnerStatistics));
    EXPECT_EQ(EReportedScanOrder::Mixed, aggregated.InnerStatistics[0].ScanOrder);
}

TEST(TQueryStatisticsScanOrderTest, ScanOrderRoundTripsThroughProto)
{
    TQueryStatistics statistics;
    statistics.ScanOrder = EReportedScanOrder::Reversed;

    NProto::TQueryStatistics serialized;
    ToProto(&serialized, statistics);

    TQueryStatistics deserialized;
    FromProto(&deserialized, serialized);

    EXPECT_EQ(EReportedScanOrder::Reversed, deserialized.ScanOrder);
}

TEST(TQueryStatisticsScanOrderTest, ScanOrderIsUnknownWhenTheProtoLacksTheField)
{
    // What a server that predates the field sends.
    NProto::TQueryStatistics serialized;
    ASSERT_FALSE(serialized.has_scan_order());

    TQueryStatistics deserialized;
    deserialized.ScanOrder = EReportedScanOrder::Reversed;
    FromProto(&deserialized, serialized);

    EXPECT_EQ(EReportedScanOrder::Unknown, deserialized.ScanOrder);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NQueryClient
