#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/library/cgroup/statistics.h>

namespace NYT::NCGroups {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TCGroupStatisticsFetcherTest, Singleton)
{
    const auto* a = TSelfCGroupsStatisticsFetcher::Get();
    ASSERT_NE(a, nullptr);
    const auto* b = TSelfCGroupsStatisticsFetcher::Get();
    EXPECT_EQ(a, b);
}

TEST(TSelfCGroupsStatisticsFetcherTest, IsControllerV2)
{
    EXPECT_NO_THROW(TSelfCGroupsStatisticsFetcher::Get()->IsControllerV2(ECGroupController::Memory));
}

TEST(TSelfCGroupsStatisticsFetcherTest, MemoryStatistics)
{
    auto stats = TSelfCGroupsStatisticsFetcher::Get()->GetMemoryStatistics();
    EXPECT_GT(stats.AnonWithSwapCached, 0);
    EXPECT_GT(stats.Cache, 0);
}

TEST(TSelfCGroupsStatisticsFetcherTest, MemoryLimits)
{
    EXPECT_NO_THROW(TSelfCGroupsStatisticsFetcher::Get()->GetMemoryLimits());
}

TEST(TSelfCGroupsStatisticsFetcherTest, CpuStatistics)
{
    auto stats = TSelfCGroupsStatisticsFetcher::Get()->GetCpuStatistics();
    EXPECT_GT(stats.UserTime, TDuration::Zero());
    EXPECT_GT(stats.SystemTime, TDuration::Zero());
}

TEST(TSelfCGroupsStatisticsFetcherTest, CpuThrottlingStatistics)
{
    EXPECT_NO_THROW(TSelfCGroupsStatisticsFetcher::Get()->GetCpuThrottlingStatistics());
}

TEST(TSelfCGroupsStatisticsFetcherTest, BlockIOStatistics)
{
    auto stats = TSelfCGroupsStatisticsFetcher::Get()->GetIOStatistics();
    EXPECT_GE(stats.IOReadByte, 0);
}

TEST(TSelfCGroupsStatisticsFetcherTest, OomKillCount)
{
    auto count = TSelfCGroupsStatisticsFetcher::Get()->GetOomKillCount();
    EXPECT_GE(count, 0);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NCGroups
