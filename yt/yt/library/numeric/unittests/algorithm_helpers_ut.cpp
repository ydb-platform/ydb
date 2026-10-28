#include <library/cpp/testing/gtest/gtest.h>

#include <yt/yt/library/numeric/algorithm_helpers.h>

#include <algorithm>
#include <vector>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TAlgorithmHelpersTest, LowerUpperBoundOracle)
{
    for (int size = 0; size <= 200; ++size) {
        std::vector<int> data;
        data.reserve(size);
        for (int i = 0; i < size; ++i) {
            data.push_back((i / 2) * 2);
        }

        for (int value = -2; value <= size + 2; ++value) {
            auto expectedLower = std::lower_bound(data.begin(), data.end(), value);
            auto expectedUpper = std::upper_bound(data.begin(), data.end(), value);

            EXPECT_EQ(LowerBound(data.begin(), data.end(), value), expectedLower)
                << "size=" << size << " value=" << value;
            EXPECT_EQ(UpperBound(data.begin(), data.end(), value), expectedUpper)
                << "size=" << size << " value=" << value;
            EXPECT_EQ(ExpLowerBound(data.begin(), data.end(), value), expectedLower)
                << "size=" << size << " value=" << value;
            EXPECT_EQ(ExpUpperBound(data.begin(), data.end(), value), expectedUpper)
                << "size=" << size << " value=" << value;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
