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
        const std::string Name;
        const int64_t Lhs;
        const int64_t Rhs;
        const int64_t Expected;
    };

    const std::vector<TTestCase> testCases = {
        {
            .Name = "0_to_9",
            .Lhs = 0,
            .Rhs = 9,
            .Expected = 4,
        },
        {
            .Name = "0_to_10",
            .Lhs = 0,
            .Rhs = 10,
            .Expected = 5,
        },
        {
            .Name = "9_to_0",
            .Lhs = 9,
            .Rhs = 0,
            .Expected = 5,
        },
        {
            .Name = "5_to_6",
            .Lhs = 5,
            .Rhs = 6,
            .Expected = 5,
        },
        {
            .Name = "6_to_5",
            .Lhs = 6,
            .Rhs = 5,
            .Expected = 6,
        },
        {
            .Name = "-5_to_-6",
            .Lhs = -5,
            .Rhs = -6,
            .Expected = -5,
        },
        {
            .Name = "5_to_-6",
            .Lhs = 5,
            .Rhs = -6,
            .Expected = 0,
        },
        {
            .Name = "0_to_-10",
            .Lhs = 0,
            .Rhs = -10,
            .Expected = -5,
        },
        {
            .Name = "minint_to_maxint",
            .Lhs = std::numeric_limits<int64_t>::min(),
            .Rhs = std::numeric_limits<int64_t>::max(),
            .Expected = -1,
        },
        {
            .Name = "-5_to_maxint",
            .Lhs = -5,
            .Rhs = std::numeric_limits<int64_t>::max(),
            .Expected = (std::numeric_limits<int64_t>::max() - 5) / 2,
        },
        {
            .Name = "5_to_maxint",
            .Lhs = 5,
            .Rhs = std::numeric_limits<int64_t>::max(),
            .Expected = 5 + (std::numeric_limits<int64_t>::max() - 5) / 2,
        }
    };

    for (const auto& testCase : testCases) {
        EXPECT_EQ(testCase.Expected, Midpoint(testCase.Lhs, testCase.Rhs)) << "In the test case " << testCase.Name;
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TUtilTest, TestSignedSaturationArithmeticOperations)
{
    struct TTestCase
    {
        const std::string Name;
        const i64 Lhs;
        const i64 Rhs;
        const i64 Expected;
        const std::function<i64(i64, i64)> Operation;
    };

    const std::vector<TTestCase> testCases = {
        {
            .Name = "signed_mul_0_9",
            .Lhs = 0,
            .Rhs = 9,
            .Expected = 0,
            .Operation = &SignedSaturationArithmeticMultiply,
        },
        {
            .Name = "signed_mul_1_117",
            .Lhs = 1,
            .Rhs = 117,
            .Expected = 117,
            .Operation = &SignedSaturationArithmeticMultiply,
        },
        {
            .Name = "signed_mul_10^9_10^12",
            .Lhs = 1000000000,
            .Rhs = 1000000000000,
            .Expected = std::numeric_limits<i64>::max(),
            .Operation = &SignedSaturationArithmeticMultiply,
        },
        {
            .Name = "signed_mul_-10^9_10^12",
            .Lhs = -1000000000,
            .Rhs = 1000000000000,
            .Expected = std::numeric_limits<i64>::min(),
            .Operation = &SignedSaturationArithmeticMultiply,
        },
        {
            .Name = "signed_mul_-10^9_-10^12",
            .Lhs = -1000000000,
            .Rhs = -1000000000000,
            .Expected = std::numeric_limits<i64>::max(),
            .Operation = &SignedSaturationArithmeticMultiply,
        },
        {
            .Name = "signed_mul_minint_maxint",
            .Lhs = std::numeric_limits<i64>::min(),
            .Rhs = std::numeric_limits<i64>::max(),
            .Expected = std::numeric_limits<i64>::min(),
            .Operation = &SignedSaturationArithmeticMultiply,
        },
        {
            .Name = "signed_add_1_117",
            .Lhs = 1,
            .Rhs = 117,
            .Expected = 118,
            .Operation = &SignedSaturationArithmeticAdd,
        },
        {
            .Name = "signed_add_maxint_maxint",
            .Lhs = std::numeric_limits<i64>::max(),
            .Rhs = std::numeric_limits<i64>::max(),
            .Expected = std::numeric_limits<i64>::max(),
            .Operation = &SignedSaturationArithmeticAdd,
        },
        {
            .Name = "signed_add_maxint-1_1",
            .Lhs = std::numeric_limits<i64>::max() - 1,
            .Rhs = 1,
            .Expected = std::numeric_limits<i64>::max(),
            .Operation = &SignedSaturationArithmeticAdd,
        },
        {
            .Name = "signed_add_maxint-1_2",
            .Lhs = std::numeric_limits<i64>::max() - 1,
            .Rhs = 2,
            .Expected = std::numeric_limits<i64>::max(),
            .Operation = &SignedSaturationArithmeticAdd,
        },
        {
            .Name = "signed_add_maxint-2_1",
            .Lhs = std::numeric_limits<i64>::max() - 2,
            .Rhs = 1,
            .Expected = std::numeric_limits<i64>::max() - 1,
            .Operation = &SignedSaturationArithmeticAdd,
        },
        {
            .Name = "signed_add_minint+1_-1",
            .Lhs = std::numeric_limits<i64>::min() + 1,
            .Rhs = -1,
            .Expected = std::numeric_limits<i64>::min(),
            .Operation = &SignedSaturationArithmeticAdd,
        },
        {
            .Name = "signed_add_minint+1_-2",
            .Lhs = std::numeric_limits<i64>::min() + 1,
            .Rhs = -2,
            .Expected = std::numeric_limits<i64>::min(),
            .Operation = &SignedSaturationArithmeticAdd,
        },
        {
            .Name = "signed_add_minint+2_-1",
            .Lhs = std::numeric_limits<i64>::min() + 2,
            .Rhs = -1,
            .Expected = std::numeric_limits<i64>::min() + 1,
            .Operation = &SignedSaturationArithmeticAdd,
        },
        {
            .Name = "signed_add_minint_maxint",
            .Lhs = std::numeric_limits<i64>::min(),
            .Rhs = std::numeric_limits<i64>::max(),
            .Expected = -1,
            .Operation = &SignedSaturationArithmeticAdd,
        },
    };

    for (const auto& testCase : testCases) {
        EXPECT_EQ(testCase.Expected, testCase.Operation(testCase.Lhs, testCase.Rhs)) << "In the test case " << testCase.Name;
    }
}

TEST_F(TUtilTest, TestUnsignedSaturationArithmeticOperations)
{
    struct TTestCase
    {
        const std::string Name;
        const i64 Lhs;
        const i64 Rhs;
        const i64 Max = std::numeric_limits<i64>::max();
        const i64 Expected;
        const std::function<i64(i64, i64, i64)> Operation;
    };

    const std::vector<TTestCase> testCases = {
        {
            .Name = "unsigned_mul_0_9",
            .Lhs = 0,
            .Rhs = 9,
            .Expected = 0,
            .Operation = &UnsignedSaturationArithmeticMultiply,
        },
        {
            .Name = "unsigned_mul_1_117",
            .Lhs = 1,
            .Rhs = 117,
            .Expected = 117,
            .Operation = &UnsignedSaturationArithmeticMultiply,
        },
        {
            .Name = "unsigned_mul_2_117_117",
            .Lhs = 2,
            .Rhs = 117,
            .Max = 117,
            .Expected = 117,
            .Operation = &UnsignedSaturationArithmeticMultiply,
        },
        {
            .Name = "unsigned_mul_10^9_10^12",
            .Lhs = 1000000000,
            .Rhs = 1000000000000,
            .Expected = std::numeric_limits<i64>::max(),
            .Operation = &UnsignedSaturationArithmeticMultiply,
        },
        {
            .Name = "unsigned_mul_10^9_10^12_12345",
            .Lhs = 1000000000,
            .Rhs = 1000000000000,
            .Max = 12345,
            .Expected = 12345,
            .Operation = &UnsignedSaturationArithmeticMultiply,
        },
        {
            .Name = "unsigned_add_1_117",
            .Lhs = 1,
            .Rhs = 117,
            .Expected = 118,
            .Operation = &UnsignedSaturationArithmeticAdd,
        },
        {
            .Name = "unsigned_add_10^7_0_12345",
            .Lhs = 10000000,
            .Rhs = 0,
            .Max = 12345,
            .Expected = 12345,
            .Operation = &UnsignedSaturationArithmeticAdd,
        },
        {
            .Name = "unsigned_add_maxint_maxint",
            .Lhs = std::numeric_limits<i64>::max(),
            .Rhs = std::numeric_limits<i64>::max(),
            .Expected = std::numeric_limits<i64>::max(),
            .Operation = &UnsignedSaturationArithmeticAdd,
        },
        {
            .Name = "unsigned_add_maxint-1_1",
            .Lhs = std::numeric_limits<i64>::max() - 1,
            .Rhs = 1,
            .Expected = std::numeric_limits<i64>::max(),
            .Operation = &UnsignedSaturationArithmeticAdd,
        },
        {
            .Name = "unsigned_add_maxint-1_2",
            .Lhs = std::numeric_limits<i64>::max() - 1,
            .Rhs = 2,
            .Expected = std::numeric_limits<i64>::max(),
            .Operation = &UnsignedSaturationArithmeticAdd,
        },
        {
            .Name = "unsigned_add_maxint-2_1",
            .Lhs = std::numeric_limits<i64>::max() - 2,
            .Rhs = 1,
            .Expected = std::numeric_limits<i64>::max() - 1,
            .Operation = &UnsignedSaturationArithmeticAdd,
        },
    };

    for (const auto& testCase : testCases) {
        EXPECT_EQ(testCase.Expected, testCase.Operation(testCase.Lhs, testCase.Rhs, testCase.Max)) << "In the test case " << testCase.Name;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
