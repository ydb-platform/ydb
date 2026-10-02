#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/random.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TRandomGeneratorTest, DifferentTypes)
{
    TRandomGenerator rg1(100500);
    TRandomGenerator rg2(100500);

    EXPECT_EQ(rg1.Generate<ui64>(), rg2.Generate<ui64>());
    EXPECT_EQ(rg1.Generate<i64>(),  rg2.Generate<i64>());
    EXPECT_EQ(rg1.Generate<ui32>(), rg2.Generate<ui32>());
    EXPECT_EQ(rg1.Generate<i32>(),  rg2.Generate<i32>());
    EXPECT_EQ(rg1.Generate<char>(), rg2.Generate<char>());

    EXPECT_EQ(rg1.Generate<double>(), rg2.Generate<double>());
}

TEST(TRandomGeneratorTest, Many)
{
    TRandomGenerator rg1(100500);
    TRandomGenerator rg2(100500);

    for (int i = 0; i < 1000; ++i) {
        EXPECT_EQ(rg1.Generate<ui64>(), rg2.Generate<ui64>());
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST(TGenerateRandomIndexesTest, None)
{
    auto result = GetRandomIndexes(10, 0);
    EXPECT_TRUE(result.empty());
}

TEST(TGenerateRandomIndexesTest, One)
{
    auto result = GetRandomIndexes(10, 1);
    EXPECT_EQ(std::ssize(result), 1);
    EXPECT_GE(result[0], 0ul);
    EXPECT_LT(result[0], 10ul);
}

TEST(TGenerateRandomIndexesTest, AlmostCompleteSet)
{
    auto result = GetRandomIndexes(5, 4);
    EXPECT_EQ(std::ssize(result), 4);
    std::sort(result.begin(), result.end());
    EXPECT_GE(result.front(), 0ull);
    EXPECT_LT(result.back(), 5ull);
    EXPECT_EQ(std::unique(result.begin(), result.end()), result.end());
}

TEST(TGenerateRandomIndexesTest, CompleteSet)
{
    auto result = GetRandomIndexes(5, 10);
    EXPECT_EQ(std::ssize(result), 5);
    std::sort(result.begin(), result.end());
    EXPECT_EQ(result.front(), 0ull);
    EXPECT_EQ(result.back(), 4ull);
    EXPECT_EQ(std::unique(result.begin(), result.end()), result.end());
}

TEST(TGenerateRandomIndexesTest, SmallSubset)
{
    auto result = GetRandomIndexes(10, 2);
    EXPECT_EQ(std::ssize(result), 2);
    std::sort(result.begin(), result.end());
    EXPECT_GE(result.front(), 0ull);
    EXPECT_LT(result.back(), 10ull);
    EXPECT_EQ(std::unique(result.begin(), result.end()), result.end());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
