#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/bitmap.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

const auto BitmapTestValues = ::testing::Values(
    std::tuple(3, std::set{1}),
    std::tuple(10, std::set{0, 1, 2, 5, 7}),
    std::tuple(62, std::set{0, 1, 2, 59, 60, 61}),
    std::tuple(63, std::set{0, 1, 2, 60, 61, 62}),
    std::tuple(64, std::set{0, 1, 2, 60, 61, 62, 63}),
    std::tuple(5000, std::set{0, 1, 15, 62, 63, 64, 65, 180, 4999}));

////////////////////////////////////////////////////////////////////////////////

void FillBitmap(auto& bitmap, std::set<int> values)
{
    for (int key : values) {
        bitmap.Set(key);
    }
}

void ExpectBitmapEqualsSet(auto& bitmap, int size, std::set<int> data)
{
    for (int i = 0; i < size; ++i) {
        EXPECT_EQ(data.contains(i), bitmap[i]);
    }
}

////////////////////////////////////////////////////////////////////////////////

class TCompactBitmapTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::tuple<
        int,
        std::set<int>
    >>
{
public:
    auto MakeBitmap(int bitSize, const std::set<int>& values)
    {
        TCompactBitmap bitmap;
        bitmap.Initialize(bitSize);
        FillBitmap(bitmap, values);
        return bitmap;
    }
};

TEST_P(TCompactBitmapTest, TestBase)
{
    auto [size, data] = GetParam();

    auto bitmap = MakeBitmap(size, data);
    ExpectBitmapEqualsSet(bitmap, size, data);
}

TEST_P(TCompactBitmapTest, TestCopy)
{
    auto [size, data] = GetParam();

    auto bitmap = MakeBitmap(size, data);

    TCompactBitmap copy1;
    copy1.CopyFrom(bitmap, size);

    TCompactBitmap copy2;
    copy2.Initialize(15);
    for (int i = 0; i < 10; ++i) {
        copy2.Set(i);
    }
    copy2.CopyFrom(bitmap, size);

    TCompactBitmap copy3;
    copy3.Initialize(2345);
    for (int i = 0; i < 2345; ++i) {
        copy3.Set(i);
    }
    copy3.CopyFrom(copy2, size);

    for (int i = 0; i < size; ++i) {
        if (data.contains(i)) {
            EXPECT_TRUE(bitmap[i]);
            EXPECT_TRUE(copy1[i]);
            EXPECT_TRUE(copy2[i]);
            EXPECT_TRUE(copy3[i]);
        } else {
            EXPECT_FALSE(bitmap[i]);
            EXPECT_FALSE(copy1[i]);
            EXPECT_FALSE(copy2[i]);
            EXPECT_FALSE(copy3[i]);
        }
    }
}

TEST_P(TCompactBitmapTest, TestMove)
{
    auto [size, data] = GetParam();

    auto bitmap = MakeBitmap(size, data);

    TCompactBitmap copy1(std::move(bitmap));

    for (int i = 0; i < size; ++i) {
        if (data.contains(i)) {
            EXPECT_TRUE(copy1[i]);
        } else {
            EXPECT_FALSE(copy1[i]);
        }
    }

    TCompactBitmap copy2;
    copy2.Initialize(15);
    for (int i = 0; i < 10; ++i) {
        copy2.Set(i);
    }
    copy2 = std::move(copy1);

    for (int i = 0; i < size; ++i) {
        if (data.contains(i)) {
            EXPECT_TRUE(copy2[i]);
        } else {
            EXPECT_FALSE(copy2[i]);
        }
    }

    TCompactBitmap copy3;
    copy3.Initialize(2345);
    for (int i = 0; i < 2345; ++i) {
        copy3.Set(i);
    }
    copy3 = std::move(copy2);

    for (int i = 0; i < size; ++i) {
        if (data.contains(i)) {
            EXPECT_TRUE(copy3[i]);
        } else {
            EXPECT_FALSE(copy3[i]);
        }
    }
}

TEST(TCompactBitmapTest, CopyUninitialized)
{
    TCompactBitmap a;
    TCompactBitmap b(std::move(a));
    TCompactBitmap c;
    c = std::move(a);
    a.CopyFrom(b, 4);
    b.CopyFrom(a, 13123);
}

INSTANTIATE_TEST_SUITE_P(TCompactBitmapTest, TCompactBitmapTest, BitmapTestValues);

////////////////////////////////////////////////////////////////////////////////

class TBitmapOutputTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::tuple<
        int,
        std::set<int>
    >>
{
public:
    auto MakeBitmap(int bitSize, const std::set<int>& values)
    {
        TBitmapOutput bitmap(bitSize);
        FillBitmap(bitmap, values);
        return bitmap;
    }
};

TEST_P(TBitmapOutputTest, TestBase)
{
    auto [capacity, data] = GetParam();

    auto bitmap = MakeBitmap(capacity, data);
    ExpectBitmapEqualsSet(bitmap, bitmap.GetBitSize(), data);
}

TEST_P(TBitmapOutputTest, TestAppend)
{
    auto [capacity, data] = GetParam();

    TBitmapOutput bitmap(capacity);

    int expectedSize = data.empty() ? 0 : *data.rbegin() + 1;
    for (int i = 0; i < expectedSize; ++i) {
        bitmap.Append(data.contains(i));
    }

    ASSERT_EQ(static_cast<int>(bitmap.GetBitSize()), expectedSize);
    ExpectBitmapEqualsSet(bitmap, bitmap.GetBitSize(), data);
}

INSTANTIATE_TEST_SUITE_P(TBitmapOutputTest, TBitmapOutputTest, BitmapTestValues);

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
