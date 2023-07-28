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

} // namespace
} // namespace NYT
