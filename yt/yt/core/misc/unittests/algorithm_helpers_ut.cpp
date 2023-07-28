#include <yt/yt/library/numeric/algorithm_helpers.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

template <class TIter, class TPredicate>
TIter OrderedSearch(TIter begin, TIter end, TPredicate pred)
{
    auto result1 = BinarySearch(begin, end, pred);
    auto result2 = ExponentialSearch(begin, end, pred);

    EXPECT_EQ(result1, result2);
    return result1;
}

template <class TIter, class TPredicate>
std::reverse_iterator<TIter> BinarySearchReverse(
    TIter begin,
    TIter end,
    TPredicate pred)
{
    auto result1 = std::make_reverse_iterator(OrderedSearch(begin, end, pred));
    auto result2 = OrderedSearch(std::make_reverse_iterator(end), std::make_reverse_iterator(begin), [&] (auto it) {
        return !pred(it);
    });

    EXPECT_EQ(result1, result2);
    return result1;
}

TEST(TAlgorithmHelpersTest, BinarySearch)
{
    {
        std::vector<TString> v;
        auto it = NYT::LowerBound(v.begin(), v.end(), "test");
        EXPECT_EQ(it, v.end());
    }

    {
        int data[] = {1, 2, 3, 4, 5, 6, 7, 8, 9};

        auto it = NYT::LowerBound(data, data + Y_ARRAY_SIZE(data), 2);
        EXPECT_EQ(it - data, 1);

        it = NYT::LowerBound(data, data + Y_ARRAY_SIZE(data), 10);
        EXPECT_EQ(it, data + Y_ARRAY_SIZE(data));
    }

    {
        std::vector<size_t> data = {10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0};

        {
            auto it = OrderedSearch(data.begin(), data.end(), [] (auto it) {
                return *it > 9;
            });
            EXPECT_EQ(it, data.begin() + 1);
        }
        {
            auto it = OrderedSearch(data.begin(), data.end(), [] (auto it) {
                return *it > 11;
            });
            EXPECT_EQ(it, data.begin());
        }
        {
            auto it = NYT::LowerBound(data.rbegin(), data.rend(), 1u);
            EXPECT_EQ(it, data.rbegin() + 1);
        }
    }
}

TEST(TAlgorithmHelpersTest, BinarySearchReverse)
{
    {
        int data[] = {1, 2, 3, 4, 5, 6, 7, 8, 9};

        for (int i = 0; i < 11; ++i) {
            // Return iterator to last element less than i.
            // If there is no such element result is equal to data.rend().
            auto it = BinarySearchReverse(data, data + Y_ARRAY_SIZE(data), [&] (auto it) {
                return *it < i;
            });

            auto expected = std::make_reverse_iterator(data + (i > 0 ? i - 1 : 0));
            EXPECT_EQ(it, expected);
        }
    }
}

TEST(TAlgorithmHelpersTest, ExponentialSearch)
{
    {
        int data[] = {1, 2, 3, 4, 5, 6, 7, 8, 9};

        for (int i = 0; i < 11; ++i) {
            auto it = ExpLowerBound(data, data + Y_ARRAY_SIZE(data), i);
            EXPECT_EQ(it, data + (i > 0 ? i - 1 : 0));
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
