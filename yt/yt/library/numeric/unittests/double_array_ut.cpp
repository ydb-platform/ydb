#include <library/cpp/testing/gtest/gtest.h>

#include <yt/yt/library/numeric/double_array.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TDoubleArrayTest
    : public testing::Test
{
protected:
    TDoubleArrayTest() = default;
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TDoubleArrayTest, TestOperatorAt)
{
    TDoubleArray<5> arr = {7, 3, 12, 43, -3.1};
    EXPECT_EQ(7, arr[0]);
    EXPECT_EQ(3, arr[1]);
    EXPECT_EQ(12, arr[2]);
    EXPECT_EQ(43, arr[3]);
    EXPECT_EQ(-3.1, arr[4]);

    arr[2] = 1.7;
    EXPECT_EQ(1.7, arr[2]);
}

TEST_F(TDoubleArrayTest, TestExample)
{
    TDoubleArray<4> vec1 = {1, 2, 3, 4};
    EXPECT_TRUE(vec1[3] == 4);
    EXPECT_TRUE(TDoubleArray<4>::All(vec1, [] (double x) { return x > 0; }));
    EXPECT_TRUE(MinComponent(vec1) == 1);

    TDoubleArray<4> vec2 = {4, 3, 2, 1};
    EXPECT_TRUE(vec1 + vec2 == TDoubleArray<4>::FromDouble(5));

    // |vec1 * vec1| wouldn't work because multiplication is not defined for mathematical vectors.
    auto vec1Square = TDoubleArray<4>::Apply(vec1, [] (double x) { return x * x; });
    EXPECT_TRUE(TDoubleArray<4>::All(vec1, vec1Square, [] (double x, double y) { return y == x * x; }));
}

TEST_F(TDoubleArrayTest, TestPlusMinus)
{
    struct TTestCase
    {
        TString Name;
        TDoubleArray<4> Arg1;
        TDoubleArray<4> Arg2;
        TDoubleArray<4> ExpectedSum;
        TDoubleArray<4> ExpectedDiff;
    };

    const std::vector<TTestCase> testCases = {
        {
            /* Name */ "Zero_&_Zero",
            /* Arg1 */ TDoubleArray<4>::Zero(),
            /* Arg2 */ TDoubleArray<4>::Zero(),
            /* ExpectedSum */ TDoubleArray<4>::Zero(),
            /* ExpectedDiff */ TDoubleArray<4>::Zero()
        },
        {
            /* Name */ "Iota_&_Zero",
            /* Arg1 */ {1, 2, 3, 4},
            /* Arg2 */ {0, 0, 0, 0},
            /* ExpectedSum */ {1, 2, 3, 4},
            /* ExpectedDiff */ {1, 2, 3, 4}
        },
        {
            /* Name */ "Iota_&_Random",
            /* Arg1 */ {17, 4.1, 14.23, 14},
            /* Arg2 */ {1, 2, 3, 4},
            /* ExpectedSum */ {18, 6.1, 17.23, 18},
            /* ExpectedDiff */ {16, 2.1, 11.23, 10}
        }
    };

    for (const auto& testCase : testCases) {
        auto testCaseMsg = "In the test case " + testCase.Name;

        EXPECT_TRUE(TDoubleArray<4>::Near(testCase.Arg1 + testCase.Arg2, testCase.ExpectedSum, 1e-9)) << testCaseMsg;
        EXPECT_TRUE(TDoubleArray<4>::Near(testCase.Arg2 + testCase.Arg1, testCase.ExpectedSum, 1e-9)) << testCaseMsg;

        EXPECT_TRUE(TDoubleArray<4>::Near(testCase.Arg1 - testCase.Arg2, testCase.ExpectedDiff, 1e-9)) << testCaseMsg;
        EXPECT_TRUE(TDoubleArray<4>::Near(testCase.Arg2 - testCase.Arg1, -testCase.ExpectedDiff, 1e-9)) << testCaseMsg;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
