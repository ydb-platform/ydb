#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/three_level_stable_vector.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

// TODO(ponasenko-rs): Parametrize these tests and add death test.

TEST(TThreeLevelStableVectorTest, Simple332)
{
    constexpr auto deep = 3;
    constexpr auto shallow = 3;
    constexpr auto coefficient = 2;
    constexpr auto total = deep * shallow * coefficient;

    auto vector = TThreeLevelStableVector<int, deep, shallow, total>();

    std::vector<int*> pointers;
    pointers.reserve(total);

    for (int i = 0; i < total; ++i) {
        vector.PushBack(i);
        pointers.push_back(&vector[i]);
    }

    for (int i = 0; i < total; ++i) {
        EXPECT_EQ(vector[i], i);
        EXPECT_EQ(&vector[i], pointers[i]);
    }
}

TEST(TThreeLevelStableVectorTest, Simple331)
{
    constexpr auto deep = 3;
    constexpr auto shallow = 3;
    constexpr auto coefficient = 1;
    constexpr auto total = deep * shallow * coefficient;

    auto vector = TThreeLevelStableVector<int, deep, shallow, total>();

    std::vector<int*> pointers;
    pointers.reserve(total);

    for (int i = 0; i < total; ++i) {
        vector.PushBack(i);
        pointers.push_back(&vector[i]);
    }

    for (int i = 0; i < total; ++i) {
        EXPECT_EQ(vector[i], i);
        EXPECT_EQ(&vector[i], pointers[i]);
    }
}

TEST(TThreeLevelStableVectorTest, Simple133)
{
    constexpr auto deep = 1;
    constexpr auto shallow = 3;
    constexpr auto coefficient = 3;
    constexpr auto total = deep * shallow * coefficient;

    auto vector = TThreeLevelStableVector<int, deep, shallow, total>();

    std::vector<int*> pointers;
    pointers.reserve(total);

    for (int i = 0; i < total; ++i) {
        vector.PushBack(i);
        pointers.push_back(&vector[i]);
    }

    for (int i = 0; i < total; ++i) {
        EXPECT_EQ(vector[i], i);
        EXPECT_EQ(&vector[i], pointers[i]);
    }
}


TEST(TThreeLevelStableVectorTest, Simple313)
{
    constexpr auto deep = 3;
    constexpr auto shallow = 1;
    constexpr auto coefficient = 3;
    constexpr auto total = deep * shallow * coefficient;

    auto vector = TThreeLevelStableVector<int, deep, shallow, total>();

    std::vector<int*> pointers;
    pointers.reserve(total);

    for (int i = 0; i < total; ++i) {
        vector.PushBack(i);
        pointers.push_back(&vector[i]);
    }

    for (int i = 0; i < total; ++i) {
        EXPECT_EQ(vector[i], i);
        EXPECT_EQ(&vector[i], pointers[i]);
    }
}

TEST(TThreeLevelStableVectorTest, Simple111)
{
    constexpr auto deep = 1;
    constexpr auto shallow = 1;
    constexpr auto coefficient = 1;
    constexpr auto total = deep * shallow * coefficient;

    auto vector = TThreeLevelStableVector<int, deep, shallow, total>();

    std::vector<int*> pointers;
    pointers.reserve(total);

    for (int i = 0; i < total; ++i) {
        vector.PushBack(i);
        pointers.push_back(&vector[i]);
    }

    for (int i = 0; i < total; ++i) {
        EXPECT_EQ(vector[i], i);
        EXPECT_EQ(&vector[i], pointers[i]);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
