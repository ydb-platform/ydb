#include <library/cpp/yt/containers/expiring_set.h>

#include <library/cpp/testing/gtest/gtest.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

TInstant operator""_ts(unsigned long long seconds)
{
    return TInstant::Zero() + TDuration::Seconds(seconds);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TExpiringSetTest, Empty)
{
    TExpiringSet<int> set;
    EXPECT_EQ(set.GetSize(), 0);
}

TEST(TExpiringSetTest, ExpireSingle)
{
    TExpiringSet<int> set;
    set.SetTtl(TDuration::Seconds(2));

    set.Insert(0_ts, 1);
    EXPECT_EQ(set.GetSize(), 1);

    set.Expire(1_ts);
    EXPECT_EQ(set.GetSize(), 1);

    set.Expire(2_ts);
    EXPECT_EQ(set.GetSize(), 0);
}

TEST(TExpiringSetTest, ExpireBatch)
{
    TExpiringSet<int> set;
    set.SetTtl(TDuration::Seconds(2));

    set.InsertMany(0_ts, std::vector<int>{1, 2, 3});
    EXPECT_EQ(set.GetSize(), 3);

    set.Expire(1_ts);
    EXPECT_EQ(set.GetSize(), 3);

    set.Expire(2_ts);
    EXPECT_EQ(set.GetSize(), 0);
}

TEST(TExpiringSetTest, Reinsert)
{
    TExpiringSet<int> set;
    set.SetTtl(TDuration::Seconds(2));

    set.Insert(0_ts, 1);
    EXPECT_EQ(set.GetSize(), 1);

    set.Insert(1_ts, 1);
    EXPECT_EQ(set.GetSize(), 1);

    set.Expire(2_ts);
    EXPECT_EQ(set.GetSize(), 1);

    set.Expire(3_ts);
    EXPECT_EQ(set.GetSize(), 0);
}

TEST(TExpiringSetTest, Contains)
{
    TExpiringSet<int> set;
    set.SetTtl(TDuration::Seconds(1));

    EXPECT_FALSE(set.Contains(1));

    set.Insert(0_ts, 1);
    EXPECT_TRUE(set.Contains(1));

    set.Expire(1_ts);
    EXPECT_FALSE(set.Contains(1));
}

TEST(TExpiringSetTest, Clear)
{
    TExpiringSet<int> set;
    set.SetTtl(TDuration::Seconds(1));

    set.Insert(0_ts, 1);
    EXPECT_EQ(set.GetSize(), 1);
    EXPECT_TRUE(set.Contains(1));

    set.Clear();
    EXPECT_EQ(set.GetSize(), 0);
    EXPECT_FALSE(set.Contains(1));
}

TEST(TExpiringSetTest, RemoveBeforeExpire)
{
    TExpiringSet<int> set;
    set.SetTtl(TDuration::Seconds(1));

    set.Insert(0_ts, 1);
    EXPECT_EQ(set.GetSize(), 1);
    EXPECT_TRUE(set.Contains(1));

    set.Remove(1);
    EXPECT_EQ(set.GetSize(), 0);
    EXPECT_FALSE(set.Contains(1));
}

TEST(TExpiringSetTest, RemoveAfterExpire)
{
    TExpiringSet<int> set;
    set.SetTtl(TDuration::Seconds(1));

    set.Insert(0_ts, 1);
    set.Expire(2_ts);

    EXPECT_EQ(set.GetSize(), 0);
    EXPECT_FALSE(set.Contains(1));

    set.Remove(1);
    EXPECT_EQ(set.GetSize(), 0);
    EXPECT_FALSE(set.Contains(1));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
