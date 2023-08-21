#include <library/cpp/testing/gtest/gtest.h>

#include <yt/yt/library/numeric/binary_search.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TBinarySearchTest
    : public ::testing::Test
{ };

////////////////////////////////////////////////////////////////////////////////

TEST_F(TBinarySearchTest, TestDoubleToBitset)
{
    struct TTestCase
    {
        TString name;
        double value;
        uint64_t bitset;
    };

    const std::vector<TTestCase> testCases = {
        {
            /* name */ "positive_zero",
            /* value */ 0.0,
            /* bitset */ 0x8000000000000000
        },
        {
            /* name */ "negative_zero",
            /* value */ -0.0,
            /* bitset */ 0x7FFFFFFFFFFFFFFF
        },
        {
            /* name */ "positive_infinity",
            /* value */ std::numeric_limits<double>::infinity(),
            /* bitset */ 0xFFF0000000000000
        },
        {
            /* name */ "negative_infinity",
            /* value */ -std::numeric_limits<double>::infinity(),
            /* bitset */ 0x000FFFFFFFFFFFFF
        },
        {
            /* name */ "lowest_finite_value",
            /* value */ std::numeric_limits<double>::lowest(),
            /* bitset */ 0x0010000000000000
        },
        {
            /* name */ "max_finite_value",
            /* value */ std::numeric_limits<double>::max(),
            /* bitset */ 0xFFEFFFFFFFFFFFFF
        },
        {
            /* name */ "PI",
            /* value */ 3.14159265358979323846264338327950288419716939937510582097494459230781640628620899863,
            /* bitset */ NDetail::DoubleToBitset(3.14159265358979323846264338327950288419716939937510582097494459230781640628620899863)
        },
        {
            /* name */ "random_bits",
            /* value */ NDetail::BitsetToDouble(0xD9544A64AF4DB207),
            /* bitset */ 0xD9544A64AF4DB207
        }
    };

    for (const auto& testCase : testCases) {
        EXPECT_EQ(testCase.bitset, NDetail::DoubleToBitset(testCase.value)) << "In the test case " << testCase.name;

        EXPECT_EQ(testCase.value, NDetail::BitsetToDouble(testCase.bitset)) << "In the test case " << testCase.name;
    }
}

TEST_F(TBinarySearchTest, TestFloatingPointLowerBound)
{
    struct TTestCase
    {
        TString name;
        double lo;
        double hi;
        std::function<bool(double)> predicate;
        std::optional<double> expectedResult;
    };

    const std::vector<TTestCase> testCases = {
        {
            /* name */ "sqrt2",
            /* lo */ 0,
            /* hi */ 2,
            /* predicate */ [] (double x) { return x * x >= 2; },
            /* expectedResult */ std::nullopt
        },
        {
            /* name */ "sqrt0.1",
            /* lo */ 0,
            /* hi */ 1,
            /* predicate */ [] (double x) { return x * x >= 0.1; },
            /* expectedResult */ std::nullopt
        },
        {
            /* name */ "positive_infinity",
            /* lo */ -std::numeric_limits<double>::infinity(),
            /* hi */ std::numeric_limits<double>::infinity(),
            /* predicate */ [] (double x) { return x >= std::numeric_limits<double>::infinity(); },
            /* expectedResult */ std::numeric_limits<double>::infinity()
        },
        {
            /* name */ "negative_infinity",
            /* lo */ -std::numeric_limits<double>::infinity(),
            /* hi */ std::numeric_limits<double>::infinity(),
            /* predicate */ [] (double x) { return x >= -std::numeric_limits<double>::infinity(); },
            /* expectedResult */ -std::numeric_limits<double>::infinity()
        },
        {
            /* name */ "double_max",
            /* lo */ -std::numeric_limits<double>::infinity(),
            /* hi */ std::numeric_limits<double>::infinity(),
            /* predicate */ [] (double x) { return x >= std::numeric_limits<double>::max(); },
            /* expectedResult */ std::numeric_limits<double>::max()
        },
        {
            /* name */ "before_double_max",
            /* lo */ -std::numeric_limits<double>::infinity(),
            /* hi */ std::numeric_limits<double>::infinity(),
            /* predicate */ [] (double x) { return x >= std::nextafter(std::numeric_limits<double>::max(), 0); },
            /* expectedResult */ std::nextafter(std::numeric_limits<double>::max(), 0)
        },
        {
            /* name */ "minus_pi",
            /* lo */ -std::numeric_limits<double>::infinity(),
            /* hi */ std::numeric_limits<double>::infinity(),
            /* predicate */ [] (double x) { return x >= -3.14159265358979323846264338327950288419716939937510582097494459230781640628620899863; },
            /* expectedResult */ -3.14159265358979323846264338327950288419716939937510582097494459230781640628620899863
        },
        {
            /* name */ "minus_zero",
            /* lo */ std::numeric_limits<double>::lowest(),
            /* hi */ std::numeric_limits<double>::max(),
            /* predicate */ [] (double x) { return x >= 0; }, // Notice that |-0.0 >= 0|.
            /* expectedResult */ -0.0
        },
        {
            /* name */ "plus_zero",
            /* lo */ std::numeric_limits<double>::lowest(),
            /* hi */ std::numeric_limits<double>::max(),
            /* predicate */ [] (double x) { return x >= 0 && !signbit(x); },
            /* expectedResult */ 0.0
        },
        {
            /* name */ "predicate_always_true",
            /* lo */ -17,
            /* hi */ 1000,
            /* predicate */ [] (double /*x*/) { return true; },
            /* expectedResult */ -17
        },
        {
            /* name */ "min_positive_number_1",
            /* lo */ -17,
            /* hi */ 1000,
            /* predicate */ [] (double x) { return x >= std::numeric_limits<double>::min(); },
            /* expectedResult */ std::numeric_limits<double>::min()
        },
        {
            /* name */ "min_positive_number_2",
            /* lo */ std::numeric_limits<double>::lowest(),
            /* hi */ std::numeric_limits<double>::max(),
            /* predicate */ [] (double x) { return x >= std::numeric_limits<double>::min(); },
            /* expectedResult */ std::numeric_limits<double>::min()
        }
    };

    for (const auto& testCase : testCases) {
        int callCount = 0;
        const auto countingPredicate = [&callCount, &testCase] (double x) {
            callCount++;
            return testCase.predicate(x);
        };

        double result = FloatingPointLowerBound(testCase.lo, testCase.hi, countingPredicate);
        EXPECT_TRUE(testCase.predicate(result))
            << "Invalid result in the test case " << testCase.name;
        if (testCase.expectedResult) {
            EXPECT_EQ(result, *testCase.expectedResult)
                << "The result differs from expected in the test case " << testCase.name;
            if (result == *testCase.expectedResult) {
                // To distinguish between 0.0 and -0.0, check the sign bit explicitly.
                EXPECT_EQ(std::signbit(result), std::signbit(*testCase.expectedResult))
                    << "Signbit differs from expected in the test case " << testCase.name << ". "
                    << "Perhaps, invalid handling of -0.0 and 0.0";
            }
        }
        if (result > testCase.lo) {
            EXPECT_FALSE(testCase.predicate(std::nextafter(result, testCase.lo)))
                << "The result is not minimal in the test case " << testCase.name;
        }
        EXPECT_LE(callCount, 70)
            << "Too many calls to the predicate in the test case " << testCase.name;
    }
}

TEST_F(TBinarySearchTest, TestFloatingPointInverseLowerBound)
{
    struct TTestCase
    {
        TString name;
        double lo;
        double hi;
        std::function<bool(double)> predicate;
        std::optional<double> expectedResult;
    };

    const std::vector<TTestCase> testCases = {
        {
            /* name */ "sqrt2",
            /* lo */ 0,
            /* hi */ 2,
            /* predicate */ [] (double x) { return x * x <= 2; },
            /* expectedResult */ std::nullopt
        },
        {
            /* name */ "sqrt0.1",
            /* lo */ 0,
            /* hi */ 1,
            /* predicate */ [] (double x) { return x * x <= 0.1; },
            /* expectedResult */ std::nullopt
        },
        {
            /* name */ "positive_infinity",
            /* lo */ -std::numeric_limits<double>::infinity(),
            /* hi */ std::numeric_limits<double>::infinity(),
            /* predicate */ [] (double x) { return x <= std::numeric_limits<double>::infinity(); },
            /* expectedResult */ std::numeric_limits<double>::infinity()
        },
        {
            /* name */ "negative_infinity",
            /* lo */ -std::numeric_limits<double>::infinity(),
            /* hi */ std::numeric_limits<double>::infinity(),
            /* predicate */ [] (double x) { return x <= -std::numeric_limits<double>::infinity(); },
            /* expectedResult */ -std::numeric_limits<double>::infinity()
        },
        {
            /* name */ "double_max",
            /* lo */ -std::numeric_limits<double>::infinity(),
            /* hi */ std::numeric_limits<double>::infinity(),
            /* predicate */ [] (double x) { return x <= std::numeric_limits<double>::max(); },
            /* expectedResult */ std::numeric_limits<double>::max()
        },
        {
            /* name */ "before_double_max",
            /* lo */ -std::numeric_limits<double>::infinity(),
            /* hi */ std::numeric_limits<double>::infinity(),
            /* predicate */ [] (double x) { return x <= std::nextafter(std::numeric_limits<double>::max(), 0); },
            /* expectedResult */ std::nextafter(std::numeric_limits<double>::max(), 0)
        },
        {
            /* name */ "minus_pi",
            /* lo */ -std::numeric_limits<double>::infinity(),
            /* hi */ std::numeric_limits<double>::infinity(),
            /* predicate */ [] (double x) { return x <= -3.14159265358979323846264338327950288419716939937510582097494459230781640628620899863; },
            /* expectedResult */ -3.14159265358979323846264338327950288419716939937510582097494459230781640628620899863
        },
        {
            /* name */ "minus_zero",
            /* lo */ std::numeric_limits<double>::lowest(),
            /* hi */ std::numeric_limits<double>::max(),
            /* predicate */ [] (double x) { return x <= 0 && signbit(x); },
            /* expectedResult */ -0.0
        },
        {
            /* name */ "plus_zero",
            /* lo */ std::numeric_limits<double>::lowest(),
            /* hi */ std::numeric_limits<double>::max(),
            /* predicate */ [] (double x) { return x <= 0; },
            /* expectedResult */ 0.0
        },
        {
            /* name */ "predicate_always_true",
            /* lo */ -17,
            /* hi */ 1000,
            /* predicate */ [] (double /*x*/) { return true; },
            /* expectedResult */ 1000
        },
        {
            /* name */ "min_positive_number_1",
            /* lo */ -17,
            /* hi */ 1000,
            /* predicate */ [] (double x) { return x <= std::numeric_limits<double>::min(); },
            /* expectedResult */ std::numeric_limits<double>::min()
        },
        {
            /* name */ "min_positive_number_2",
            /* lo */ std::numeric_limits<double>::lowest(),
            /* hi */ std::numeric_limits<double>::max(),
            /* predicate */ [] (double x) { return x <= std::numeric_limits<double>::min(); },
            /* expectedResult */ std::numeric_limits<double>::min()
        }
    };

    for (const auto& testCase : testCases) {
        int callCount = 0;
        const auto countingPredicate = [&callCount, &testCase] (double x) {
            callCount++;
            return testCase.predicate(x);
        };

        double result = FloatingPointInverseLowerBound(testCase.lo, testCase.hi, countingPredicate);
        EXPECT_TRUE(testCase.predicate(result))
            << "Invalid result in the test case " << testCase.name;
        if (testCase.expectedResult) {
            EXPECT_EQ(result, *testCase.expectedResult)
                << "The result differs from expected in the test case " << testCase.name;
            if (result == *testCase.expectedResult) {
                // To distinguish between 0.0 and -0.0, check the sign bit explicitly.
                EXPECT_EQ(std::signbit(result), std::signbit(*testCase.expectedResult))
                    << "Signbit differs from expected in the test case " << testCase.name << ". "
                    << "Perhaps, invalid handling of -0.0 and 0.0";
            }
        }
        if (result < testCase.hi) {
            EXPECT_FALSE(testCase.predicate(std::nextafter(result, testCase.hi)))
                << "The result is not maximal in the test case " << testCase.name;
        }
        EXPECT_LE(callCount, 70)
            << "Too many calls to the predicate in the test case " << testCase.name;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
