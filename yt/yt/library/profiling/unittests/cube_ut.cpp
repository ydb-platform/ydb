#include <gtest/gtest.h>

#include <yt/yt/library/profiling/solomon/cube.h>

namespace NYT::NProfiling {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TCubeTest, GaugeProjections)
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

    EXPECT_EQ(4.0, (*it)->Values()[0]);

    cube.StartIteration();
    cube.Remove({ 1 });
    cube.FinishIteration();

    ASSERT_EQ(static_cast<size_t>(1), cube.GetProjections().size());

    cube.StartIteration();
    cube.Remove({ 1 });
    cube.FinishIteration();

    ASSERT_EQ(static_cast<size_t>(0), cube.GetProjections().size());
}

TEST(TCubeTest, Rollup)
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

    ASSERT_EQ((*it)->Rollup, static_cast<i64>(51 - 12 / 2));
}

TEST(TCubeTest, ProjectionCookieStableAcrossRehash)
{
    TCube<i64> cube(/*windowSize*/ 12, /*nextIteration*/ 0);

    TProjectionSet projections;
    projections.Resize(1);
    projections.SetEnabled(false);

    auto cookie = cube.AddAll(TTagIdSet(projections, TTagIdList{1}));
    for (int tagId = 2; tagId < 10'000; ++tagId) {
        cube.AddAll(TTagIdSet(projections, TTagIdList{tagId}));
    }

    cube.StartIteration();
    cube.UpdateAll(cookie, 7);
    cube.FinishIteration();

    auto it = cube.GetProjections().find(TTagIdList{1});
    ASSERT_NE(it, cube.GetProjections().end());
    EXPECT_EQ((*it)->Values()[0], 7);
}

TEST(TCubeTest, WindowWiderThanOneMaskWord)
{
    TCube<i64> cube(/*windowSize*/ 100, /*nextIteration*/ 0);
    cube.Add({1});

    for (int iteration = 0; iteration < 70; ++iteration) {
        cube.StartIteration();
        if (iteration == 66) {
            cube.Update({1}, 5);
        }
        cube.FinishIteration();
    }

    auto it = cube.GetProjections().find(TTagIdList{1});
    ASSERT_NE(it, cube.GetProjections().end());
    const auto& projection = **it;
    EXPECT_TRUE(projection.HasValue()[66]);
    EXPECT_FALSE(projection.HasValue()[65]);
    EXPECT_FALSE(projection.HasValue()[2]);
    EXPECT_EQ(projection.Values()[66], 5);
}

TEST(TCubeTest, RemoveAllKeepsSharedProjection)
{
    TCube<i64> cube(/*windowSize*/ 12, /*nextIteration*/ 0);

    TProjectionSet projections;
    projections.Resize(1);
    projections.SetEnabled(false);
    TTagIdSet tagSet(projections, TTagIdList{1});

    auto firstCookie = cube.AddAll(tagSet);
    auto secondCookie = cube.AddAll(tagSet);
    ASSERT_EQ(cube.GetProjections().size(), 1u);

    cube.RemoveAll(std::move(firstCookie));
    ASSERT_EQ(cube.GetProjections().size(), 1u);

    cube.StartIteration();
    cube.UpdateAll(secondCookie, 7);
    cube.FinishIteration();

    auto it = cube.GetProjections().find(TTagIdList{1});
    ASSERT_NE(it, cube.GetProjections().end());
    EXPECT_EQ((*it)->Values()[0], 7);

    cube.RemoveAll(std::move(secondCookie));
    EXPECT_TRUE(cube.GetProjections().empty());
}

TEST(TCubeTest, RejectsProjectionCookieFromAnotherCube)
{
    TCube<i64> firstCube(/*windowSize*/ 12, /*nextIteration*/ 0);
    TCube<i64> secondCube(/*windowSize*/ 12, /*nextIteration*/ 0);

    TProjectionSet projections;
    projections.Resize(1);
    projections.SetEnabled(false);
    TTagIdSet tagSet(projections, TTagIdList{1});

    auto cookie = firstCube.AddAll(tagSet);
    EXPECT_DEATH({ secondCube.UpdateAll(cookie, 1); }, "cookie.OwnerCubeId_ == CubeId_");
    EXPECT_DEATH({ secondCube.RemoveAll(std::move(cookie)); }, "cookie.OwnerCubeId_ == CubeId_");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NProfiling
