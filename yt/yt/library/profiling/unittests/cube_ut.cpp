#include <gtest/gtest.h>

#include <yt/yt/library/profiling/solomon/cube.h>

namespace NYT::NProfiling {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TCube, GaugeProjections)
{
    TCube<double> cube(12, 0);

    cube.StartIteration();
    cube.Add({ 1 });
    cube.Add({ 1 });
    cube.Update({ 1 }, 1.0);
    cube.Update({ 1 }, 3.0);
    cube.FinishIteration();

    const auto& p = cube.GetProjections();
    auto it = p.find(TTagIdList{ 1 });
    ASSERT_TRUE(it != p.end());

    EXPECT_EQ(4.0, it->second.Values[0]);

    cube.StartIteration();
    cube.Remove({ 1 });
    cube.FinishIteration();

    ASSERT_EQ(static_cast<size_t>(1), cube.GetProjections().size());

    cube.StartIteration();
    cube.Remove({ 1 });
    cube.FinishIteration();

    ASSERT_EQ(static_cast<size_t>(0), cube.GetProjections().size());
}

TEST(TCube, Rollup)
{
    TCube<i64> cube(12, 0);
    cube.StartIteration();
    cube.Add({});
    cube.Update({}, 1);
    cube.FinishIteration();

    for (int i = 0; i < 100; i++) {
        cube.StartIteration();
        if (i % 2 == 0) {
            cube.Update({}, 1);
        }
        cube.FinishIteration();
    }

    auto it = cube.GetProjections().find(TTagIdList{});
    ASSERT_TRUE(it != cube.GetProjections().end());

    ASSERT_EQ(it->second.Rollup, static_cast<i64>(51 - 12 / 2));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NProfiling
