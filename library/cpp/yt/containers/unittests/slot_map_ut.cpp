#include <library/cpp/yt/containers/slot_map.h>

#include <library/cpp/yt/compact_containers/compact_vector.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <memory>
#include <vector>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

using TIntPtr = std::shared_ptr<int>;

TIntPtr MakeInt(int value)
{
    return std::make_shared<int>(value);
}

std::vector<int> DrainSorted(TSlotMap<TIntPtr>& map)
{
    std::vector<int> result;
    map.ExtractAll([&] (TIntPtr value) {
        result.push_back(*value);
    });
    std::sort(result.begin(), result.end());
    return result;
}

////////////////////////////////////////////////////////////////////////////////

TEST(TSlotMapTest, Empty)
{
    TSlotMap<TIntPtr> map;
    EXPECT_TRUE(map.IsEmpty());
    EXPECT_EQ(0, map.GetSize());
}

TEST(TSlotMapTest, InsertAndExtract)
{
    TSlotMap<TIntPtr> map;

    auto index = map.Insert(MakeInt(42));
    EXPECT_FALSE(map.IsEmpty());
    EXPECT_EQ(1, map.GetSize());

    auto value = map.Extract(index);
    EXPECT_EQ(42, *value);
    EXPECT_TRUE(map.IsEmpty());
    EXPECT_EQ(0, map.GetSize());
}

TEST(TSlotMapTest, DistinctIndices)
{
    TSlotMap<TIntPtr> map;

    auto a = map.Insert(MakeInt(1));
    auto b = map.Insert(MakeInt(2));
    auto c = map.Insert(MakeInt(3));

    EXPECT_NE(a, b);
    EXPECT_NE(b, c);
    EXPECT_NE(a, c);
    EXPECT_EQ(3, map.GetSize());
    EXPECT_THAT(DrainSorted(map), ::testing::ElementsAre(1, 2, 3));
}

TEST(TSlotMapTest, SlotReuse)
{
    TSlotMap<TIntPtr> map;

    auto a = map.Insert(MakeInt(1));
    map.Insert(MakeInt(2));

    EXPECT_EQ(1, *map.Extract(a));

    // The freed slot should be recycled by the next insert.
    auto c = map.Insert(MakeInt(3));
    EXPECT_EQ(a, c);
    EXPECT_EQ(2, map.GetSize());
    EXPECT_THAT(DrainSorted(map), ::testing::ElementsAre(2, 3));
}

TEST(TSlotMapTest, ExtractAllSkipsHoles)
{
    TSlotMap<TIntPtr> map;

    auto a = map.Insert(MakeInt(1));
    map.Insert(MakeInt(2));
    auto c = map.Insert(MakeInt(3));
    map.Insert(MakeInt(4));

    map.Extract(a);
    map.Extract(c);

    EXPECT_THAT(DrainSorted(map), ::testing::ElementsAre(2, 4));
    EXPECT_TRUE(map.IsEmpty());

    // The map is fully reusable after draining.
    auto index = map.Insert(MakeInt(5));
    EXPECT_EQ(TSlotMapIndex(0), index);
    EXPECT_EQ(5, *map.Extract(index));
}

TEST(TSlotMapTest, DestroysValueOnExtract)
{
    TSlotMap<TIntPtr> map;

    auto value = MakeInt(1);
    std::weak_ptr<int> weak = value;
    auto index = map.Insert(std::move(value));
    EXPECT_FALSE(weak.expired());

    // The map no longer holds a reference; only the extracted value does.
    auto extracted = map.Extract(index);
    extracted.reset();
    EXPECT_TRUE(weak.expired());
}

TEST(TSlotMapTest, DestroysValuesOnExtractAll)
{
    TSlotMap<TIntPtr> map;

    auto value = MakeInt(1);
    std::weak_ptr<int> weak = value;
    map.Insert(std::move(value));

    map.ExtractAll([] (TIntPtr) { });
    EXPECT_TRUE(weak.expired());
}

////////////////////////////////////////////////////////////////////////////////

// A stateful traits object using a caller-supplied sentinel as the tombstone.
struct TSentinelTraits
{
    int Sentinel = 0;

    template <class U>
    bool IsEmpty(const U& value) const
    {
        return value == Sentinel;
    }

    template <class U>
    U MakeEmpty() const
    {
        return Sentinel;
    }
};

TEST(TSlotMapTest, CustomTraits)
{
    TSlotMap<int, std::vector, TSentinelTraits> map(TSentinelTraits{.Sentinel = -1});

    // Zero is an ordinary value here since the sentinel is -1.
    auto a = map.Insert(0);
    map.Insert(5);
    EXPECT_EQ(2, map.GetSize());

    EXPECT_EQ(0, map.Extract(a));
    auto c = map.Insert(7);
    EXPECT_EQ(a, c);

    std::vector<int> values;
    map.ExtractAll([&] (int value) {
        values.push_back(value);
    });
    std::sort(values.begin(), values.end());
    EXPECT_THAT(values, ::testing::ElementsAre(5, 7));
}

////////////////////////////////////////////////////////////////////////////////

template <class U>
using TCompactVector2 = TCompactVector<U, 2>;

TEST(TSlotMapTest, CustomVector)
{
    TSlotMap<TIntPtr, TCompactVector2> map;

    auto a = map.Insert(MakeInt(1));
    map.Insert(MakeInt(2));
    // Force reallocation past the inline capacity.
    map.Insert(MakeInt(3));

    EXPECT_EQ(3, map.GetSize());
    EXPECT_EQ(1, *map.Extract(a));

    auto d = map.Insert(MakeInt(4));
    EXPECT_EQ(a, d);

    std::vector<int> values;
    map.ExtractAll([&] (TIntPtr value) {
        values.push_back(*value);
    });
    std::sort(values.begin(), values.end());
    EXPECT_THAT(values, ::testing::ElementsAre(2, 3, 4));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
