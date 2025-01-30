#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/fenwick_tree.h>

#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/misc/random.h>

#include <algorithm>
#include <vector>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TFenwickTreeTest, TPushBackTest)
{
    TFenwickTree<int> tree;

    for (int i = 0; i < 10; ++i) {
        tree.PushBack(i + 1);
    }
    EXPECT_EQ(tree.Size(), 10);

    for (int i = 0; i <= tree.Size(); ++i) {
        EXPECT_EQ(i * (i + 1) / 2, tree.GetCumulativeSum(i));
    }

    for (int i = 0; i < 5; ++i) {
        tree.PopBack();
    }
    EXPECT_EQ(tree.Size(), 5);

    for (int i = 0; i <= tree.Size(); ++i) {
        EXPECT_EQ(i * (i + 1) / 2, tree.GetCumulativeSum(i));
    }
}

TEST(TFenwickTreeTest, TSetValueTest)
{
    TFenwickTree<i64> tree;

    for (int i = 0; i < 10; ++i) {
        tree.EmplaceBack();
    }

    for (int i = 0; i <= tree.Size(); ++i) {
        EXPECT_EQ(0, tree.GetCumulativeSum(i));
    }

    for (int i = 0; i < tree.Size(); ++i) {
        // Setting values in pseudo-random order. Note that gcd(3, 10) = 1.
        int index = (i * 3) % tree.Size();

        tree.SetValue(index, index + 1);
    }

    for (int i = 0; i <= tree.Size(); ++i) {
        EXPECT_EQ(i * (i + 1) / 2, tree.GetCumulativeSum(i));
    }
}

TEST(TFenwickTreeTest, TLowerBoundTest1)
{
    TFenwickTree<int> tree;

    tree.PushBack(1);
    tree.PushBack(2);
    tree.PushBack(0);
    tree.PushBack(0);
    tree.PushBack(5);

    EXPECT_EQ(0, tree.LowerBound(0));
    EXPECT_EQ(1, tree.LowerBound(1));
    EXPECT_EQ(2, tree.LowerBound(2));
    EXPECT_EQ(2, tree.LowerBound(3));
    EXPECT_EQ(5, tree.LowerBound(4));
    EXPECT_EQ(5, tree.LowerBound(5));
    EXPECT_EQ(5, tree.LowerBound(6));
    EXPECT_EQ(5, tree.LowerBound(7));
    EXPECT_EQ(5, tree.LowerBound(8));
    EXPECT_EQ(6, tree.LowerBound(9));
    EXPECT_EQ(6, tree.LowerBound(10000));
}

TEST(TFenwickTreeTest, TLowerBoundTest2)
{
    TFenwickTree<int> tree;
    std::vector<int> prefixSums{0};

    TRandomGenerator generator;

    for (int i = 0; i < 16; ++i) {
        int value = generator.Generate<ui32>() % 100;
        tree.PushBack(value);
        prefixSums.push_back(prefixSums.back() + value);
    }

    for (int i = 0; i < 1600; ++i) {
        EXPECT_EQ(tree.LowerBound(i), std::lower_bound(prefixSums.begin(), prefixSums.end(), i) - prefixSums.begin());
    }
}

TEST(TFenwickTreeTest, TUpperBoundTest)
{
    TFenwickTree<int> tree;
    std::vector<int> prefixSums{0};

    TRandomGenerator generator;

    for (int i = 0; i < 16; ++i) {
        int value = generator.Generate<ui32>() % 100;
        tree.PushBack(value);
        prefixSums.push_back(prefixSums.back() + value);
    }

    for (int i = 0; i < 1600; ++i) {
        EXPECT_EQ(tree.UpperBound(i), std::upper_bound(prefixSums.begin(), prefixSums.end(), i) - prefixSums.begin());
    }
}

TEST(TFenwickTreeTest, TFloatTest)
{
    TFenwickTree<double> tree;

    tree.PushBack(0.5);
    tree.PushBack(0.1);
    tree.PushBack(0.4);

    EXPECT_FLOAT_EQ(tree.GetCumulativeSum(0), 0.0);
    EXPECT_FLOAT_EQ(tree.GetCumulativeSum(1), 0.5);
    EXPECT_FLOAT_EQ(tree.GetCumulativeSum(2), 0.6);
    EXPECT_FLOAT_EQ(tree.GetCumulativeSum(3), 1.0);
}

TEST(TFenwickTreeTest, TCustomStructureTest)
{
    struct TItem
    {
        int X = 0;
        int Y = 0;

        TItem() = default;

        TItem(int x, int y)
            : X(x)
            , Y(y)
        { }

        TItem operator+(const TItem& other) const
        {
            return {X + other.X, Y + other.Y};
        }

        bool operator==(const TItem& other) const
        {
            return std::tie(X, Y) == std::tie(other.X, other.Y);
        }
    };

    TFenwickTree<TItem> tree;

    tree.PushBack({1, 1});
    tree.EmplaceBack(2, 2);

    EXPECT_EQ(tree.GetCumulativeSum(2), TItem(3, 3));

    auto cmp = [] (TItem lhs, TItem rhs) {
        return std::tie(lhs.X, lhs.Y) < std::tie(rhs.X, rhs.Y);
    };

    EXPECT_EQ(tree.LowerBound(TItem(1, 1), cmp), 1);
    EXPECT_EQ(tree.UpperBound(TItem(1, 1), cmp), 2);

    auto cmpLower = [] (TItem lhs, int rhs) {
        return lhs.X < rhs;
    };

    EXPECT_EQ(tree.LowerBound(1, cmpLower), 1);

    auto cmpUpper = [] (int rhs, TItem lhs) {
        return rhs < lhs.X;
    };

    EXPECT_EQ(tree.UpperBound(1, cmpUpper), 2);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
