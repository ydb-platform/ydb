#include <library/cpp/testing/gtest/gtest.h>

#include <yt/yt/library/numeric/util.h>

#include <limits>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TUtilTest
    : public ::testing::Test
{ };

////////////////////////////////////////////////////////////////////////////////

TEST_F(TUtilTest, TestMidpoint)
{
    struct TTestCase
    {
        TString name;
        int64_t a;
        int64_t b;
        int64_t expected;
    };

    const std::vector<TTestCase> testCases = {
        {
            /* name */ "0_to_9",
            /* a */ 0,
            /* b */ 9,
            /* expected */ 4
        },
        {
            /* name */ "0_to_10",
            /* a */ 0,
            /* b */ 10,
            /* expected */ 5
        },
        {
            /* name */ "9_to_0",
            /* a */ 9,
            /* b */ 0,
            /* expected */ 5
        },
        {
            /* name */ "5_to_6",
            /* a */ 5,
            /* b */ 6,
            /* expected */ 5
        },
        {
            /* name */ "6_to_5",
            /* a */ 6,
            /* b */ 5,
            /* expected */ 6
        },
        {
            /* name */ "-5_to_-6",
            /* a */ -5,
            /* b */ -6,
            /* expected */ -5
        },
        {
            /* name */ "5_to_-6",
            /* a */ 5,
            /* b */ -6,
            /* expected */ 0
        },
        {
            /* name */ "0_to_-10",
            /* a */ 0,
            /* b */ -10,
            /* expected */ -5
        },
        {
            /* name */ "minint_to_maxint",
            /* a */ std::numeric_limits<int64_t>::min(),
            /* b */ std::numeric_limits<int64_t>::max(),
            /* expected */ -1
        },
        {
            /* name */ "-5_to_maxint",
            /* a */ -5,
            /* b */ std::numeric_limits<int64_t>::max(),
            /* expected */ (std::numeric_limits<int64_t>::max() - 5) / 2
        },
        {
            /* name */ "5_to_maxint",
            /* a */ 5,
            /* b */ std::numeric_limits<int64_t>::max(),
            /* expected */ 5 + (std::numeric_limits<int64_t>::max() - 5) / 2
        }
    };

    for (const auto& testCase : testCases) {
        EXPECT_EQ(testCase.expected, Midpoint(testCase.a, testCase.b)) << "In the test case " << testCase.name;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
