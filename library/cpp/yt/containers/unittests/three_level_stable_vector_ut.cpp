#include <library/cpp/yt/containers/three_level_stable_vector.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <iterator>
#include <string>
#include <vector>

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

TEST(TThreeLevelStableVectorTest, Empty)
{
    auto vector = TThreeLevelStableVector<int, 3, 3, 9>();
    EXPECT_EQ(0, std::ssize(vector));
    EXPECT_TRUE(vector.Empty());

    vector.PushBack(42);
    EXPECT_EQ(1, std::ssize(vector));
    EXPECT_FALSE(vector.Empty());
}

TEST(TThreeLevelStableVectorTest, Mutate)
{
    constexpr auto total = 9;

    auto vector = TThreeLevelStableVector<int, 3, 3, total>();
    for (int i = 0; i < total; ++i) {
        vector.PushBack(0);
    }

    for (int i = 0; i < total; ++i) {
        vector[i] = 2 * i;
    }
    for (int i = 0; i < total; ++i) {
        EXPECT_EQ(vector[i], 2 * i);
    }
}

TEST(TThreeLevelStableVectorTest, NonPodElements)
{
    constexpr auto total = 9;

    auto vector = TThreeLevelStableVector<std::string, 3, 3, total>();
    for (int i = 0; i < total; ++i) {
        vector.PushBack(std::to_string(i));
    }

    for (int i = 0; i < total; ++i) {
        EXPECT_EQ(vector[i], std::to_string(i));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
