#include <yt/yt/library/numeric/double_array_format.h>
#include <yt/yt/library/numeric/piecewise_linear_function.h>
#include <yt/yt/library/numeric/piecewise_linear_function-test.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <util/string/cast.h>


namespace NYT {

using ::ToString;

////////////////////////////////////////////////////////////////////////////////

class TPiecewiseLinearFunctionTest
    : public testing::Test
{
protected:
    struct TSample {
        double Argument;
        double ExpectedLeftLimit;
        double ExpectedRightLimit;
    };

    static std::vector<double> AddNeighbourPointsAndSort(
        const std::vector<double>& points,
        double leftBound,
        double rightBound)
    {
        std::vector<double> result{};

        for (double point : points) {
            result.push_back(point);
            result.push_back(std::nextafter(point, leftBound));
            result.push_back(std::nextafter(point, rightBound));
        }

        std::sort(begin(result), end(result));
        return result;
    }

    template <class TValue>
    static TPiecewiseLinearFunction<TValue> BuildFunctionFromPoints(
    const std::vector<typename TPiecewiseLinearFunction<TValue>::TBuilder::TPoint>& points)
    {
        typename TPiecewiseLinearFunction<TValue>::TBuilder builder;
        for (const auto& point : points) {
            builder.AddPoint(point);
        }
        return builder.Finish();
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TPiecewiseLinearFunctionTest, TestInterpolationProperties)
{
    const std::vector<TPiecewiseSegment<double>> segments = {
        {{0, 0}, {1, 1}},
        {{0, 1}, {1, 0}},
        {{12.7, 1.1}, {15.2, 3.517}},
        {{-1.57324932741e7, -4.12387218764e8}, {8.24325235234e9, 5.1234325324324e9}},
        {{-1.57324932741e7, 4.12387218764e8}, {8.24325235234e9, -5.1234325324324e9}},
        {{-1.57324932741e7, 0.1}, {8.24325235234e9, 0.23123}},
        {{-1.57324932741e7, 0.23123}, {8.24325235234e9, 0.1}},
        {{1.123, 0.23123}, {1.123, 0.1}},
        {{1.123, 0.1}, {1.123, 0.23123}},
        {{std::nextafter(1.123, 0), 0.1}, {std::nextafter(1.123, 2), 0.23123}},
        {{std::nextafter(1.123, 0), 0.23123}, {std::nextafter(1.123, 2), 0.1}},
        {{std::nextafter(1.123, 0), -4.12387218764e8}, {std::nextafter(1.123, 2), 5.1234325324324e9}},
        {{std::nextafter(1.123, 0), 5.1234325324324e9}, {std::nextafter(1.123, 2), -4.12387218764e8}},
    };

    const std::vector<double> normalizedSamplePoints = {
        0, std::nextafter(0, 1), 0.000000001, 0.1, 0.3, 0.31415926535897932384626,
        0.4, 0.455555555, 0.49999999999, 0.5, 0.5172, 0.6, 0.7123,
        0.8, 0.9, 0.999999999999, 1.0
    };

    // Test that interpolation is precise on trivial segment.
    {
        auto segment = TPiecewiseSegment<double>({0, 0}, {1, 1});
        auto points = AddNeighbourPointsAndSort(normalizedSamplePoints, 0, 1);

        for (double point : points) {
            EXPECT_EQ(point, segment.ValueAt(point));
        }
    }

    // Test that interpolation is monotonic and is exact at bounds on different segments.
    for (int segmentIdx = 0; segmentIdx < std::ssize(segments); segmentIdx++) {
        const auto& segment = segments[segmentIdx];
        auto msg = NYT::Format(
            "For segment #%vequal to { {%v, %v}, {%v, %v} }",
            segmentIdx,
            segment.LeftBound(),
            segment.LeftValue(),
            segment.RightBound(),
            segment.RightValue());

        // Test on endpoints.
        {
            EXPECT_EQ(segment.LeftValue(), segment.LeftLimitAt(segment.LeftBound()));
            EXPECT_EQ(segment.RightValue(), segment.RightLimitAt(segment.RightBound()));
        }

        // Test |InterpolateNormalized|.
        {
            // Prepare points for test.
            std::vector<double> interpolatePoints = AddNeighbourPointsAndSort(normalizedSamplePoints, 0, 1);

            // Test monotonicity.
            double prevResult = segment.LeftValue();
            for (double point : interpolatePoints) {
                double result = segment.InterpolateNormalized(point);
                if (segment.LeftValue() <= segment.RightValue()) {
                    EXPECT_LE(prevResult, result) << msg;
                } else {
                    EXPECT_GE(prevResult, result) << msg;
                }
                prevResult = result;
            }
        }

        // Test |Get|.
        {
            // Prepare points for test.
            std::vector<double> interpolatePoints;
            for (double normalizedPoint : normalizedSamplePoints) {
                // NB: It is ironic that we use linear interpolation to test linear interpolation. But it is perfectly correct here.
                double point = segment.LeftBound() + (segment.RightBound() - segment.LeftBound()) * normalizedPoint;
                if (point > segment.RightBound()) {
                    point = segment.RightBound();
                }

                interpolatePoints.push_back(point);
            }
            interpolatePoints = AddNeighbourPointsAndSort(interpolatePoints, segment.LeftBound(), segment.RightBound());

            // |RightBound()| might be missing because of the imperfect interpolation used inside the loop.
            interpolatePoints.push_back(segment.RightBound());
            interpolatePoints.push_back(std::nextafter(segment.RightBound(), segment.LeftBound()));
            std::sort(begin(interpolatePoints), end(interpolatePoints));

            // Test monotonicity.
            double prevResult = segment.LeftValue();
            for (double point : interpolatePoints) {
                double result = segment.ValueAt(point);
                if (segment.LeftValue() <= segment.RightValue()) {
                    EXPECT_LE(prevResult, result) << msg;
                } else {
                    EXPECT_GE(prevResult, result) << msg;
                }
                prevResult = result;
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TPiecewiseLinearFunctionTest, TestSortOrMergeImpl)
{
    struct TTestCase {
        TString Name;
        std::vector<double> Input;
        int ExpectedNumberOfPivots;
        std::vector<double> ExpectedOutput;
    };

    const std::vector<TTestCase> testCases = {
        {
            /* Name */ "sortedInput",
            /* Input */ {0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
            /* ExpectedNumberOfPivots */ 2,
            /* ExpectedOutput */ {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
        },
        {
            /* Name */ "twoSortedSegments",
            /* Input */ {
                4, 5, 6, 7, 8, 9,
                0, 1, 2, 3
            },
            /* ExpectedNumberOfPivots */ 3,
            /* ExpectedOutput */ {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
        },
        {
            /* Name */ "threeSortedSegments",
            /* Input */ {
                8, 9,
                4, 5, 6, 7,
                0, 1, 2, 3
            },
            /* ExpectedNumberOfPivots */ 4,
            /* ExpectedOutput */ {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
        },
        {
            /* Name */ "fourSortedSegments",
            /* Input */ {
                8, 9,
                6, 7,
                3, 4, 5,
                0, 1, 2
            },
            /* ExpectedNumberOfPivots */ 5,
            /* ExpectedOutput */ {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
        },
        {
            /* Name */ "fiveSortedSegments",
            /* Input */ {
                4, 4.1, 4.2, 4.3,
                3, 3.1,
                2, 2.1,
                1, 1.1, 1.2,
                0, 0.1
            },
            /* ExpectedNumberOfPivots */ 6,
            /* ExpectedOutput */ {
                0, 0.1,
                1, 1.1, 1.2,
                2, 2.1,
                3, 3.1,
                4, 4.1, 4.2, 4.3
            }
        },
        {
            /* Name */ "sixSortedSegments",
            /* Input */ {
                5, 5.1,
                4, 4.1, 4.2, 4.3,
                3, 3.1,
                2, 2.1,
                1, 1.1, 1.2,
                0, 0.1
            },
            /* ExpectedNumberOfPivots */ 7,
            /* ExpectedOutput */ {
                0, 0.1,
                1, 1.1, 1.2,
                2, 2.1,
                3, 3.1,
                4, 4.1, 4.2, 4.3,
                5, 5.1
            }
        },
        {
            /* Name */ "sevenSortedSegments",
            /* Input */ {
                6, 6.1, 6.2,
                5, 5.1,
                4, 4.1, 4.2, 4.3,
                3, 3.1,
                2, 2.1,
                1, 1.1, 1.2,
                0, 0.1
            },
            /* ExpectedNumberOfPivots */ 8,
            /* ExpectedOutput */ {
                0, 0.1,
                1, 1.1, 1.2,
                2, 2.1,
                3, 3.1,
                4, 4.1, 4.2, 4.3,
                5, 5.1,
                6, 6.1, 6.2
            }
        },
        {
            /* Name */ "eightSortedSegments",
            /* Input */ {
                7, 7.1,
                6, 6.1, 6.2,
                5, 5.1,
                4, 4.1, 4.2, 4.3,
                3, 3.1,
                2, 2.1,
                1, 1.1, 1.2,
                0, 0.1
            },
            /* ExpectedNumberOfPivots */ 9,
            /* ExpectedOutput */ {
                0, 0.1,
                1, 1.1, 1.2,
                2, 2.1,
                3, 3.1,
                4, 4.1, 4.2, 4.3,
                5, 5.1,
                6, 6.1, 6.2,
                7, 7.1
            }
        }
    };

    for (const auto& testCase : testCases) {
        auto testCaseMsg = NYT::Format("In the test case %v", testCase.Name);

        std::vector<double> vec = testCase.Input;
        std::vector<double> buffer(vec.size());
        NDetail::TPivotsVector mergePivots;
        NDetail::TPivotsVector pivotsBuffer;

        EXPECT_TRUE(NDetail::FindMergePivots(&vec, &mergePivots)) << testCaseMsg;
        EXPECT_EQ(testCase.ExpectedNumberOfPivots, std::ssize(mergePivots)) << testCaseMsg;
        NDetail::SortOrMergeImpl(&vec, &buffer, &mergePivots, &pivotsBuffer);

        EXPECT_TRUE(std::is_sorted(begin(vec), end(vec))) << testCaseMsg;
        EXPECT_EQ(testCase.ExpectedOutput, vec) << testCaseMsg;
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TPiecewiseLinearFunctionTest, TestSum)
{
    struct TTestCase {
        TString Name;
        std::vector<TPiecewiseLinearFunction<double>> Functions;
        double ExpectedLeftBound;
        double ExpectedRightBound;
        std::vector<TSample> Samples;
    };

    const std::vector<TTestCase> testCases = {
        {
            /* Name */ "sumOfOneFunction",
            /* Functions */ {
                TPiecewiseLinearFunction<double>::Linear(0.1, 17, 1.2, 42)
            },
            /* ExpectedLeftBound */ 0.1,
            /* ExpectedRightBound */ 1.2,
            /* Samples */ {
                {
                    /* Argument */ 0.1,
                    /* ExpectedLeftLimit */ 17,
                    /* ExpectedRightLimit */ 17,
                },
                {
                    /* Argument */ 0.3,
                    /* ExpectedLeftLimit */ 21.5454545454545454545454545454545454545454545454,
                    /* ExpectedRightLimit */ 21.5454545454545454545454545454545454545454545454,
                },
                {
                    /* Argument */ 1.2,
                    /* ExpectedLeftLimit */ 42,
                    /* ExpectedRightLimit */ 42,
                }
            }
        },
        {
            /* Name */ "sumOfLinearFunctionAndConstantSameBounds",
            /* Functions */ {
                TPiecewiseLinearFunction<double>::Linear(0.1, 17, 1.2, 42),
                TPiecewiseLinearFunction<double>::Constant(0.1, 1.2, 1)
            },
            /* ExpectedLeftBound */ 0.1,
            /* ExpectedRightBound */ 1.2,
            /* Samples */ {
                {
                    /* Argument */ 0.1,
                    /* ExpectedLeftLimit */ 18,
                    /* ExpectedRightLimit */ 18,
                },
                {
                    /* Argument */ 0.3,
                    /* ExpectedLeftLimit */ 22.5454545454545454545454545454545454545454545454,
                    /* ExpectedRightLimit */ 22.5454545454545454545454545454545454545454545454,
                },
                {
                    /* Argument */ 1.2,
                    /* ExpectedLeftLimit */ 43,
                    /* ExpectedRightLimit */ 43,
                }
            }
        },
        {
            /* Name */ "sumOfLinearFunctionAndConstantDifferentBounds",
            /* Functions */ {
                TPiecewiseLinearFunction<double>::Linear(0.1, 17, 1.2, 42),
                TPiecewiseLinearFunction<double>::Constant(0.3, 1.5, 1)
            },
            /* ExpectedLeftBound */ 0.3,
            /* ExpectedRightBound */ 1.2,
            /* Samples */ {
                {
                    /* Argument */ 0.3,
                    /* ExpectedLeftLimit */ 22.5454545454545454545454545454545454545454545454,
                    /* ExpectedRightLimit */ 22.5454545454545454545454545454545454545454545454,
                },
                {
                    /* Argument */ 1.2,
                    /* ExpectedLeftLimit */ 43,
                    /* ExpectedRightLimit */ 43,
                }
            }
        },
        {
            /* Name */ "sumOfTwoLinearFunctions",
            /* Functions */ {
                TPiecewiseLinearFunction<double>::Linear(1, 1, 3, 5),
                TPiecewiseLinearFunction<double>::Linear(1, 5, 3, 1)
            },
            /* ExpectedLeftBound */ 1,
            /* ExpectedRightBound */ 3,
            /* Samples */ {
                {
                    /* Argument */ 1,
                    /* ExpectedLeftLimit */ 6,
                    /* ExpectedRightLimit */ 6,
                },
                {
                    /* Argument */ 2.117117117,
                    /* ExpectedLeftLimit */ 6,
                    /* ExpectedRightLimit */ 6,
                },
                {
                    /* Argument */ 3,
                    /* ExpectedLeftLimit */ 6,
                    /* ExpectedRightLimit */ 6,
                },
            }
        },
        {
            /* Name */ "sumOfThreeLinearFunctions",
            /* Functions */ {
                TPiecewiseLinearFunction<double>::Linear(1, 1, 5, 5),
                TPiecewiseLinearFunction<double>::Linear(2, 1, 7, 0),
                TPiecewiseLinearFunction<double>::Linear(-3, 9, 5, 1)
            },
            /* ExpectedLeftBound */ 2,
            /* ExpectedRightBound */ 5,
            /* Samples */ {
                {
                    /* Argument */ 2,
                    /* ExpectedLeftLimit */ 7,
                    /* ExpectedRightLimit */ 7,
                },
                {
                    /* Argument */ 3,
                    /* ExpectedLeftLimit */ 6.8,
                    /* ExpectedRightLimit */ 6.8,
                },
                {
                    /* Argument */ 5,
                    /* ExpectedLeftLimit */ 6.4,
                    /* ExpectedRightLimit */ 6.4,
                },
            }
        },
        {
            /* Name */ "sumWithDiscontinuousFunction",
            /* Functions */ {
                BuildFunctionFromPoints<double>({{0, 0}, {1, 0}, {1, 0.5}, {2, 1}}),
                TPiecewiseLinearFunction<double>::Linear(0, 1, 2, 0),
            },
            /* ExpectedLeftBound */ 0,
            /* ExpectedRightBound */ 2,
            /* Samples */ {
                {
                    /* Argument */ 0,
                    /* ExpectedLeftLimit */ 1,
                    /* ExpectedRightLimit */ 1,
                },
                {
                    /* Argument */ 0.5,
                    /* ExpectedLeftLimit */ 0.75,
                    /* ExpectedRightLimit */ 0.75,
                },
                {
                    /* Argument */ 1,
                    /* ExpectedLeftLimit */ 0.5,
                    /* ExpectedRightLimit */ 1,
                },
                {
                    /* Argument */ 1.5,
                    /* ExpectedLeftLimit */ 1,
                    /* ExpectedRightLimit */ 1,
                },
                {
                    /* Argument */ 2,
                    /* ExpectedLeftLimit */ 1,
                    /* ExpectedRightLimit */ 1,
                },
            }
        }
    };

    for (const auto& testCase : testCases) {
        auto testCaseMsg = NYT::Format("In the test case %v", testCase.Name);

        auto sumResult = TPiecewiseLinearFunction<double>::Sum(testCase.Functions);
        EXPECT_EQ(testCase.ExpectedLeftBound, sumResult.LeftFunctionBound()) << testCaseMsg;
        EXPECT_EQ(testCase.ExpectedRightBound, sumResult.RightFunctionBound()) << testCaseMsg;

        for (const auto& sample : testCase.Samples) {
            auto sampleMsg = NYT::Format("At point %v", sample.Argument);

            EXPECT_EQ(sample.ExpectedLeftLimit, sumResult.ValueAt(sample.Argument)) << testCaseMsg << sampleMsg;
            EXPECT_EQ(sample.ExpectedLeftLimit, sumResult.LeftLimitAt(sample.Argument)) << testCaseMsg << sampleMsg;
            EXPECT_EQ(sample.ExpectedRightLimit, sumResult.RightLimitAt(sample.Argument)) << testCaseMsg << sampleMsg;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TPiecewiseLinearFunctionTest, TestPointwiseMin)
{
    struct TTestCase {
        TString Name;
        std::vector<TPiecewiseLinearFunction<double>> Functions;
        double ExpectedLeftBound;
        double ExpectedRightBound;
        std::vector<TSample> Samples;
    };

    const std::vector<TTestCase> testCases = {
        {
            /* Name */ "minOfOneFunction",
            /* Functions */ {
                TPiecewiseLinearFunction<double>::Linear(0.1, 17, 1.2, 42)
            },
            /* ExpectedLeftBound */ 0.1,
            /* ExpectedRightBound */ 1.2,
            /* Samples */ {
                {
                    /* Argument */ 0.1,
                    /* ExpectedLeftLimit */ 17,
                    /* ExpectedRightLimit */ 17,
                },
                {
                    /* Argument */ 0.3,
                    /* ExpectedLeftLimit */ 21.5454545454545454545454545454545454545454545454,
                    /* ExpectedRightLimit */ 21.5454545454545454545454545454545454545454545454,
                },
                {
                    /* Argument */ 1.2,
                    /* ExpectedLeftLimit */ 42,
                    /* ExpectedRightLimit */ 42,
                }
            }
        },
        {
            /* Name */ "minOfLinearFunctionAndConstantSameBounds",
            /* Functions */ {
                TPiecewiseLinearFunction<double>::Linear(0.1, 17, 1.2, 42),
                TPiecewiseLinearFunction<double>::Constant(0.1, 1.2, 33)
            },
            /* ExpectedLeftBound */ 0.1,
            /* ExpectedRightBound */ 1.2,
            /* Samples */ {
                {
                    /* Argument */ 0.1,
                    /* ExpectedLeftLimit */ 17,
                    /* ExpectedRightLimit */ 17,
                },
                {
                    /* Argument */ 0.3,
                    /* ExpectedLeftLimit */ 21.5454545454545454545454545454545454545454545454,
                    /* ExpectedRightLimit */ 21.5454545454545454545454545454545454545454545454,
                },
                {
                    /* Argument */ 1,
                    /* ExpectedLeftLimit */ 33,
                    /* ExpectedRightLimit */ 33,
                },
                {
                    /* Argument */ 1.2,
                    /* ExpectedLeftLimit */ 33,
                    /* ExpectedRightLimit */ 33,
                }
            }
        },
        {
            /* Name */ "minOfLinearFunctionAndConstantDifferentBounds",
            /* Functions */ {
                TPiecewiseLinearFunction<double>::Linear(0.1, 17, 1.2, 42),
                TPiecewiseLinearFunction<double>::Constant(0.3, 1.5, 33)
            },
            /* ExpectedLeftBound */ 0.3,
            /* ExpectedRightBound */ 1.2,
            /* Samples */ {
                {
                    /* Argument */ 0.3,
                    /* ExpectedLeftLimit */ 21.5454545454545454545454545454545454545454545454,
                    /* ExpectedRightLimit */ 21.5454545454545454545454545454545454545454545454,
                },
                {
                    /* Argument */ 1,
                    /* ExpectedLeftLimit */ 33,
                    /* ExpectedRightLimit */ 33,
                },
                {
                    /* Argument */ 1.2,
                    /* ExpectedLeftLimit */ 33,
                    /* ExpectedRightLimit */ 33,
                }
            }
        },
        {
            /* Name */ "minOfTwoLinearFunctions",
            /* Functions */ {
                TPiecewiseLinearFunction<double>::Linear(1, 1, 5, 5),
                TPiecewiseLinearFunction<double>::Linear(1, 5, 5, 1)
            },
            /* ExpectedLeftBound */ 1,
            /* ExpectedRightBound */ 5,
            /* Samples */ {
                {
                    /* Argument */ 1,
                    /* ExpectedLeftLimit */ 1,
                    /* ExpectedRightLimit */ 1,
                },
                {
                    /* Argument */ 2,
                    /* ExpectedLeftLimit */ 2,
                    /* ExpectedRightLimit */ 2,
                },
                {
                    /* Argument */ 3,
                    /* ExpectedLeftLimit */ 3,
                    /* ExpectedRightLimit */ 3,
                },
                {
                    /* Argument */ 5,
                    /* ExpectedLeftLimit */ 1,
                    /* ExpectedRightLimit */ 1,
                },
            }
        },
        {
            /* Name */ "minOfThreeLinearFunctions",
            /* Functions */ {
                TPiecewiseLinearFunction<double>::Linear(1, 1, 5, 5),
                TPiecewiseLinearFunction<double>::Linear(0, 2, 10, 2),
                TPiecewiseLinearFunction<double>::Linear(-3, 9, 5, 1)
            },
            /* ExpectedLeftBound */ 1,
            /* ExpectedRightBound */ 5,
            /* Samples */ {
                {
                    /* Argument */ 1,
                    /* ExpectedLeftLimit */ 1,
                    /* ExpectedRightLimit */ 1,
                },
                {
                    /* Argument */ 2,
                    /* ExpectedLeftLimit */ 2,
                    /* ExpectedRightLimit */ 2,
                },
                {
                    /* Argument */ 3,
                    /* ExpectedLeftLimit */ 2,
                    /* ExpectedRightLimit */ 2,
                },
                {
                    /* Argument */ 4,
                    /* ExpectedLeftLimit */ 2,
                    /* ExpectedRightLimit */ 2,
                },
                {
                    /* Argument */ 5,
                    /* ExpectedLeftLimit */ 1,
                    /* ExpectedRightLimit */ 1,
                },
            }
        },
        {
            /* Name */ "minWithDiscontinuousFunctionContinuousResult",
            /* Functions */ {
                BuildFunctionFromPoints<double>({{0, 0}, {1, 0}, {1, 1}, {2, 2}}),
                TPiecewiseLinearFunction<double>::Linear(0, 0, 2, -1),
            },
            /* ExpectedLeftBound */ 0,
            /* ExpectedRightBound */ 2,
            /* Samples */ {
                {
                    /* Argument */ 0,
                    /* ExpectedLeftLimit */ 0,
                    /* ExpectedRightLimit */ 0,
                },
                {
                    /* Argument */ 0.5,
                    /* ExpectedLeftLimit */ -0.25,
                    /* ExpectedRightLimit */ -0.25,
                },
                {
                    /* Argument */ 1,
                    /* ExpectedLeftLimit */ -0.5,
                    /* ExpectedRightLimit */ -0.5,
                },
                {
                    /* Argument */ 2,
                    /* ExpectedLeftLimit */ -1,
                    /* ExpectedRightLimit */ -1,
                },
            }
        },
        {
            /* Name */ "minWithDiscontinuousFunctionDiscontinuousResult",
            /* Functions */ {
                BuildFunctionFromPoints<double>({{0, 0}, {1, 0}, {1, 1}, {2, 2}}),
                TPiecewiseLinearFunction<double>::Linear(0, 1, 2, 0),
            },
            /* ExpectedLeftBound */ 0,
            /* ExpectedRightBound */ 2,
            /* Samples */ {
                {
                    /* Argument */ 0,
                    /* ExpectedLeftLimit */ 0,
                    /* ExpectedRightLimit */ 0,
                },
                {
                    /* Argument */ 0.5,
                    /* ExpectedLeftLimit */ 0,
                    /* ExpectedRightLimit */ 0,
                },
                {
                    /* Argument */ 1,
                    /* ExpectedLeftLimit */ 0,
                    /* ExpectedRightLimit */ 0.5,
                },
                {
                    /* Argument */ 1.5,
                    /* ExpectedLeftLimit */ 0.25,
                    /* ExpectedRightLimit */ 0.25,
                },
                {
                    /* Argument */ 2,
                    /* ExpectedLeftLimit */ 0,
                    /* ExpectedRightLimit */ 0,
                },
            }
        }
    };

    for (const auto& testCase : testCases) {
        auto testCaseMsg = NYT::Format("In the test case %v", testCase.Name);

        auto minResult = PointwiseMin<double>(testCase.Functions);
        EXPECT_EQ(testCase.ExpectedLeftBound, minResult.LeftFunctionBound()) << testCaseMsg;
        EXPECT_EQ(testCase.ExpectedRightBound, minResult.RightFunctionBound()) << testCaseMsg;

        for (const auto& sample : testCase.Samples) {
            auto sampleMsg = NYT::Format("At point %v", sample.Argument);

            EXPECT_EQ(sample.ExpectedLeftLimit, minResult.ValueAt(sample.Argument)) << testCaseMsg << sampleMsg;
            EXPECT_EQ(sample.ExpectedLeftLimit, minResult.LeftLimitAt(sample.Argument)) << testCaseMsg << sampleMsg;
            EXPECT_EQ(sample.ExpectedRightLimit, minResult.RightLimitAt(sample.Argument)) << testCaseMsg << sampleMsg;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TPiecewiseLinearFunctionTest, TestCompose)
{
    struct TTestCase {
        TString Name;
        TPiecewiseLinearFunction<double> Lhs;
        TPiecewiseLinearFunction<double> Rhs;
        std::vector<TSample> Samples;
    };

    const std::vector<TTestCase> testCases = {
        {
            /* Name */ "compositionLinearWithIdentity",
            /* Lhs */ TPiecewiseLinearFunction<double>::Linear(0.1, 17, 1.2, 42),
            /* Rhs */ TPiecewiseLinearFunction<double>::Linear(0.1, 0.1, 1.2, 1.2),
            /* Samples */ {
                {
                    /* Argument */ 0.1,
                    /* ExpectedLeftLimit */ 17,
                    /* ExpectedRightLimit */ 17,
                },
                {
                    /* Argument */ 0.3,
                    /* ExpectedLeftLimit */ 21.5454545454545454545454545454545454545454545454,
                    /* ExpectedRightLimit */ 21.5454545454545454545454545454545454545454545454,
                },
                {
                    /* Argument */ 1.2,
                    /* ExpectedLeftLimit */ 42,
                    /* ExpectedRightLimit */ 42,
                }
            }
        },
        {
            /* Name */ "compositionLinearWithLinear",
            /* Lhs */ TPiecewiseLinearFunction<double>::Linear(0.1, 17, 1.2, 42),
            /* Rhs */ TPiecewiseLinearFunction<double>::Linear(0, 0.1, 100, 1.2),
            /* Samples */ {
                {
                    /* Argument */ 0,
                    /* ExpectedLeftLimit */ 17,
                    /* ExpectedRightLimit */ 17,
                },
                {
                    /* Argument */ 20,
                    /* ExpectedLeftLimit */ 22,
                    /* ExpectedRightLimit */ 22,
                },
                {
                    /* Argument */ 100,
                    /* ExpectedLeftLimit */ 42,
                    /* ExpectedRightLimit */ 42,
                }
            }
        },
        {
            /* Name */ "compositionLinearWithLinearInjection",
            /* Lhs */ TPiecewiseLinearFunction<double>::Linear(0.1, 17, 1.2, 42),
            /* Rhs */ TPiecewiseLinearFunction<double>::Linear(0, 0.3, 100, 0.98),
            /* Samples */ {
                {
                    /* Argument */ 0,
                    /* ExpectedLeftLimit */ 21.5454545454545454545454545454545454545454545454,
                    /* ExpectedRightLimit */ 21.5454545454545454545454545454545454545454545454,
                },
                {
                    /* Argument */ 100,
                    /* ExpectedLeftLimit */ 37,
                    /* ExpectedRightLimit */ 37,
                }
            }
        },
        {
            /* Name */ "compositionLinearWithDiscontinuous",
            /* Lhs */ TPiecewiseLinearFunction<double>::Linear(0.1, 17, 1.2, 42),
            /* Rhs */ BuildFunctionFromPoints<double>({{0, 0.1}, {1, 0.3}, {1, 0.98}, {2, 1.2}}),
            /* Samples */ {
                {
                    /* Argument */ 0,
                    /* ExpectedLeftLimit */ 17,
                    /* ExpectedRightLimit */ 17,
                },
                {
                    /* Argument */ 1,
                    /* ExpectedLeftLimit */ 21.5454545454545454545454545454545454545454545454,
                    /* ExpectedRightLimit */ 37,
                },
                {
                    /* Argument */ 2,
                    /* ExpectedLeftLimit */ 42,
                    /* ExpectedRightLimit */ 42,
                }
            }
        },
        {
            /* Name */ "compositionLinearWithPlateauing",
            /* Lhs */ TPiecewiseLinearFunction<double>::Linear(0.1, 17, 1.2, 42),
            /* Rhs */ BuildFunctionFromPoints<double>({{0, 0.1}, {1, 0.3}, {2, 0.3}, {3, 1.2}}),
            /* Samples */ {
                {
                    /* Argument */ 0,
                    /* ExpectedLeftLimit */ 17,
                    /* ExpectedRightLimit */ 17,
                },
                {
                    /* Argument */ 1,
                    /* ExpectedLeftLimit */ 21.5454545454545454545454545454545454545454545454,
                    /* ExpectedRightLimit */ 21.5454545454545454545454545454545454545454545454,
                },
                {
                    /* Argument */ 2,
                    /* ExpectedLeftLimit */ 21.5454545454545454545454545454545454545454545454,
                    /* ExpectedRightLimit */ 21.5454545454545454545454545454545454545454545454,
                },
                {
                    /* Argument */ 3,
                    /* ExpectedLeftLimit */ 42,
                    /* ExpectedRightLimit */ 42,
                }
            }
        },
        {
            // NB (eshcherbin): We use a pair of simpler functions here to avoid precision problems.
            /* Name */ "compositionDiscontinuousWithLinear",
            /* Lhs */ BuildFunctionFromPoints<double>({{0, 0}, {1, 1}, {1, 2}, {2, 3}}),
            /* Rhs */ TPiecewiseLinearFunction<double>::Linear(0, 0, 2, 2),
            /* Samples */ {
                {
                    /* Argument */ 0,
                    /* ExpectedLeftLimit */ 0,
                    /* ExpectedRightLimit */ 0,
                },
                {
                    /* Argument */ 1,
                    /* ExpectedLeftLimit */ 1,
                    /* ExpectedRightLimit */ 2,
                },
                {
                    /* Argument */ 2,
                    /* ExpectedLeftLimit */ 3,
                    /* ExpectedRightLimit */ 3,
                },
            }
        },
        {
            /* Name */ "compositionPlateauingWithLinear",
            /* Lhs */ BuildFunctionFromPoints<double>({{17, 0}, {22, 1}, {37, 1}, {42, 2}}),
            /* Rhs */ TPiecewiseLinearFunction<double>::Linear(0.1, 17, 1.2, 42),
            /* Samples */ {
                {
                    /* Argument */ 0.1,
                    /* ExpectedLeftLimit */ 0,
                    /* ExpectedRightLimit */ 0,
                },
                {
                    /* Argument */ 0.32,
                    /* ExpectedLeftLimit */ 1,
                    /* ExpectedRightLimit */ 1,
                },
                {
                    /* Argument */ 0.98,
                    /* ExpectedLeftLimit */ 1,
                    /* ExpectedRightLimit */ 1,
                },
                {
                    /* Argument */ 1.2,
                    /* ExpectedLeftLimit */ 2,
                    /* ExpectedRightLimit */ 2,
                },
            }
        },
        {
            /* Name */ "compositionDiscontinuousWithPlateauing",
            /* Lhs */ BuildFunctionFromPoints<double>({{0, 0}, {3, 3}, {3, 6}, {6, 9}}),
            /* Rhs */ BuildFunctionFromPoints<double>({{0, 0}, {1, 3}, {3, 3}, {4, 6}}),
            /* Samples */ {
                {
                    /* Argument */ 0,
                    /* ExpectedLeftLimit */ 0,
                    /* ExpectedRightLimit */ 0,
                },
                {
                    /* Argument */ 1,
                    /* ExpectedLeftLimit */ 3,
                    /* ExpectedRightLimit */ 3,
                },
                {
                    /* Argument */ 2,
                    /* ExpectedLeftLimit */ 3,
                    /* ExpectedRightLimit */ 3,
                },
                {
                    /* Argument */ 3,
                    /* ExpectedLeftLimit */ 3,
                    /* ExpectedRightLimit */ 6,
                },
                {
                    /* Argument */ 4,
                    /* ExpectedLeftLimit */ 9,
                    /* ExpectedRightLimit */ 9,
                },
            }
        },
        {
            /* Name */ "compositionMonster",
            /* Lhs */ BuildFunctionFromPoints<double>({{0, 0}, {3, 3}, {3, 5}, {5, 5}, {7, 7}}),
            /* Rhs */ BuildFunctionFromPoints<double>({{0, 0}, {1, 2}, {2, 2}, {2, 4}, {4, 6}}),
            /* Samples */ {
                {
                    /* Argument */ 0,
                    /* ExpectedLeftLimit */ 0,
                    /* ExpectedRightLimit */ 0,
                },
                {
                    /* Argument */ 1,
                    /* ExpectedLeftLimit */ 2,
                    /* ExpectedRightLimit */ 2,
                },
                {
                    /* Argument */ 1.2,
                    /* ExpectedLeftLimit */ 2,
                    /* ExpectedRightLimit */ 2,
                },
                {
                    /* Argument */ 2,
                    /* ExpectedLeftLimit */ 2,
                    /* ExpectedRightLimit */ 5,
                },
                {
                    /* Argument */ 3,
                    /* ExpectedLeftLimit */ 5,
                    /* ExpectedRightLimit */ 5,
                },
                {
                    /* Argument */ 4,
                    /* ExpectedLeftLimit */ 6,
                    /* ExpectedRightLimit */ 6,
                },
            }
        },
    };

    for (const auto& testCase : testCases) {
        auto testCaseMsg = NYT::Format("In the test case %v", testCase.Name);

        auto result = testCase.Lhs.Compose(testCase.Rhs);
        EXPECT_EQ(testCase.Rhs.LeftFunctionBound(), result.LeftFunctionBound()) << testCaseMsg;
        EXPECT_EQ(testCase.Rhs.RightFunctionBound(), result.RightFunctionBound()) << testCaseMsg;

        for (const auto& sample : testCase.Samples) {
            auto sampleMsg = NYT::Format("At point %v", sample.Argument);

            EXPECT_EQ(sample.ExpectedLeftLimit, result.ValueAt(sample.Argument)) << testCaseMsg << sampleMsg;
            EXPECT_EQ(sample.ExpectedLeftLimit, result.LeftLimitAt(sample.Argument)) << testCaseMsg << sampleMsg;
            EXPECT_EQ(sample.ExpectedRightLimit, result.RightLimitAt(sample.Argument)) << testCaseMsg << sampleMsg;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TPiecewiseLinearFunctionTest, TestTransformations)
{
    struct TTestCase {
        TString Name;
        TPiecewiseLinearFunction<double> Function;
        TPiecewiseLinearFunction<double> ExpectedTransposedFunction;
    };

    const std::vector<TTestCase> testCases = {
        {
            /* Name */ "linear",
            /* Function */ TPiecewiseLinearFunction<double>::Linear(0.1, 17, 1.2, 42),
            /* ExpectedTransposedFunction */ TPiecewiseLinearFunction<double>::Linear(17, 0.1, 42, 1.2),
        },
        {
            /* Name */ "constant",
            /* Function */ TPiecewiseLinearFunction<double>::Constant(0.1, 1.2, 42),
            /* ExpectedTransposedFunction */ TPiecewiseLinearFunction<double>::Linear(42, 0.1, 42, 1.2),
        },
        {
            /* Name */ "discontinuous",
            /* Function */ BuildFunctionFromPoints<double>({{0, 0}, {1, 1}, {1, 2}, {2, 3}}),
            /* ExpectedTransposedFunction */ BuildFunctionFromPoints<double>({{0, 0}, {1, 1}, {2, 1}, {3, 2}}),
        },
        {
            /* Name */ "plateauing",
            /* Function */ BuildFunctionFromPoints<double>({{0, 0}, {1, 1}, {2, 1}, {3, 2}}),
            /* ExpectedTransposedFunction */ BuildFunctionFromPoints<double>({{0, 0}, {1, 1}, {1, 2}, {2, 3}}),
        },
    };

    for (const auto& testCase : testCases) {
        auto testCaseMsg = NYT::Format("In the test case %v", testCase.Name);

        const auto& function = testCase.Function;
        EXPECT_EQ(testCase.ExpectedTransposedFunction, function.Transpose()) << testCaseMsg;
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TPiecewiseLinearFunctionTest, TestPiecewiseSegmentScalar)
{
    struct TDefinedOnInterval {
        double LeftBound;
        double RightBound;
        bool ExpectedIsDefined;
    };

    struct TTestCase {
        TString Name;
        TPiecewiseSegment<double> Segment;
        std::pair<double, double> ExpectedBounds;
        std::pair<double, double> ExpectedValues;
        std::vector<TDefinedOnInterval> Intervals;
        std::vector<TSample> Samples;
        bool ExpectedIsVertical;
        bool ExpectedIsHorizontal;
        bool ExpectedIsPoint;
        bool ExpectedIsTilted;
    };

    const std::vector<TTestCase> testCases = {
        {
            /* Name */ "tiltedSegment",
            /* Segment */ {{0, 0}, {2, 1}},
            /* ExpectedBounds */ {0, 2},
            /* ExpectedValues */ {0, 1},
            /* Intervals */ {
                {0, 2, true},
                {0, 1, true},
                {1, 2, true},
                {0.3, 0.6, true},
                {-1, 3, false},
                {-1, 1, false},
                {10, 11, false},
                {2, 3, false}
            },
            /* Samples */ {
                {0, 0, 0},
                {0.5, 0.25, 0.25},
                {1, 0.5, 0.5},
                {2, 1, 1},
            },
            /* ExpectedIsVertical */ false,
            /* ExpectedIsHorizontal */ false,
            /* ExpectedIsPoint */ false,
            /* ExpectedIsTilted */ true
        },
        {
            /* Name */ "diagonalSegment",
            /* Segment */ {{0, 0}, {1, 1}},
            /* ExpectedBounds */ {0, 1},
            /* ExpectedValues */ {0, 1},
            /* Intervals */ {
                {0, 1, true},
                {0, 0.5, true},
                {0.5, 1, true},
                {0.3, 0.6, true},
                {-1, 2, false},
                {-1, 0.5, false},
                {10, 11, false},
                {1, 2, false}
            },
            /* Samples */ {
                {0, 0, 0},
                {0.5, 0.5, 0.5},
                {1, 1, 1},
            },
            /* ExpectedIsVertical */ false,
            /* ExpectedIsHorizontal */ false,
            /* ExpectedIsPoint */ false,
            /* ExpectedIsTilted */ true
        },
        {
            /* Name */ "verticalSegment",
            /* Segment */ {{0, 0}, {0, 1}},
            /* ExpectedBounds */ {0, 0},
            /* ExpectedValues */ {0, 1},
            /* Intervals */ {
                {0, 0, true},
                {0, 0.5, false},
                {0.3, 0.6, false},
                {-1, 2, false},
            },
            /* Samples */ {
                {0, 0, 1},
            },
            /* ExpectedIsVertical */ true,
            /* ExpectedIsHorizontal */ false,
            /* ExpectedIsPoint */ false,
            /* ExpectedIsTilted */ false
        },
        {
            /* Name */ "horizontalSegment",
            /* Segment */ {{0, 0}, {1, 0}},
            /* ExpectedBounds */ {0, 1},
            /* ExpectedValues */ {0, 0},
            /* Intervals */ {
                {0, 1, true},
                {0, 0.5, true},
                {0.5, 1, true},
                {0.3, 0.6, true},
                {-1, 2, false},
                {-1, 0.5, false},
                {10, 11, false},
                {1, 2, false}
            },
            /* Samples */ {
                {0, 0, 0},
                {0.5, 0, 0},
                {1, 0, 0},
            },
            /* ExpectedIsVertical */ false,
            /* ExpectedIsHorizontal */ true,
            /* ExpectedIsPoint */ false,
            /* ExpectedIsTilted */ false
        },
        {
            /* Name */ "pointSegment",
            /* Segment */ {{0, 0}, {0, 0}},
            /* ExpectedBounds */ {0, 0},
            /* ExpectedValues */ {0, 0},
            /* Intervals */ {
                {0, 0, true},
                {0, 0.5, false},
                {0.3, 0.6, false},
                {-1, 2, false},
            },
            /* Samples */ {
                {0, 0, 0},
            },
            /* ExpectedIsVertical */ false,
            /* ExpectedIsHorizontal */ false,
            /* ExpectedIsPoint */ true,
            /* ExpectedIsTilted */ false
        }
    };

    for (const auto& testCase : testCases) {
        auto testCaseMsg = NYT::Format("In the test case %v", testCase.Name);

        const auto& segment = testCase.Segment;

        EXPECT_EQ(testCase.ExpectedBounds.first, segment.LeftBound()) << testCaseMsg;
        EXPECT_EQ(testCase.ExpectedBounds.second, segment.RightBound()) << testCaseMsg;
        EXPECT_EQ(testCase.ExpectedValues.first, segment.LeftValue()) << testCaseMsg;
        EXPECT_EQ(testCase.ExpectedValues.second, segment.RightValue()) << testCaseMsg;

        for (const auto& [leftBound, rightBound, expectedIsDefined] : testCase.Intervals) {
            auto sampleMsg = NYT::Format("At interval [ %v, %v ]", leftBound, rightBound);

            EXPECT_EQ(expectedIsDefined, segment.IsDefinedOn(leftBound, rightBound)) << testCaseMsg << ". " << sampleMsg;
        }

        for (const auto& [arg, expectedLeftLimit, expectedRightLimit] : testCase.Samples) {
            auto sampleMsg = NYT::Format("At point %v", arg);

            EXPECT_TRUE(segment.IsDefinedAt(arg)) << testCaseMsg << ". " << sampleMsg;
            EXPECT_EQ(expectedLeftLimit, segment.LeftLimitAt(arg)) << testCaseMsg << ". " << sampleMsg;
            EXPECT_EQ(expectedRightLimit, segment.RightLimitAt(arg)) << testCaseMsg << ". " << sampleMsg;
            auto expectedLeftRightLimit = std::pair(expectedLeftLimit, expectedRightLimit);
            EXPECT_EQ(expectedLeftRightLimit, segment.LeftRightLimitAt(arg)) << testCaseMsg << ". " << sampleMsg;
            EXPECT_EQ(expectedLeftLimit, segment.ValueAt(arg)) << testCaseMsg << ". " << sampleMsg;
        }

        EXPECT_EQ(testCase.ExpectedIsVertical, segment.IsVertical()) << testCaseMsg;
        EXPECT_EQ(testCase.ExpectedIsHorizontal, segment.IsHorizontal()) << testCaseMsg;
        EXPECT_EQ(testCase.ExpectedIsPoint, segment.IsPoint()) << testCaseMsg;
        EXPECT_EQ(testCase.ExpectedIsTilted, segment.IsTilted()) << testCaseMsg;
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TPiecewiseLinearFunctionTest, TestPiecewiseSegmentTransformationsScalar)
{
    struct TScaledSegment {
        double Scale;
        TPiecewiseSegment<double> ExpectedSegment;
    };

    struct TShiftedSegment {
        double DeltaBound;
        double DeltaValue;
        TPiecewiseSegment<double> ExpectedSegment;
    };

    struct TTestCase {
        TString Name;
        TPiecewiseSegment<double> Segment;
        TPiecewiseSegment<double> ExpectedTransposedSegment;
        std::vector<TScaledSegment> ExpectedScaledSegments;
        std::vector<TShiftedSegment> ExpectedShiftedSegments;
    };

    const std::vector<TTestCase> testCases = {
        {
            /* Name */ "tiltedSegment",
            /* Segment */ {{0, 0}, {2, 1}},
            /* ExpectedTransposedSegment */ {{0, 0}, {1, 2}},
            /* ExpectedScaledSegments */ {
                {0.5, {{0, 0}, {4, 1}}},
                {1, {{0, 0}, {2, 1}}},
                {2, {{0, 0}, {1, 1}}},
            },
            /* ExpectedShiftedSegments */ {
                {1, 0, {{1, 0}, {3, 1}}},
                {0, 1, {{0, 1}, {2, 2}}},
                {1, 1, {{1, 1}, {3, 2}}},
            },
        },
        {
            /* Name */ "tiltedSegment2",
            /* Segment */ {{10, 0}, {12, 1}},
            /* ExpectedTransposedSegment */ {{0, 10}, {1, 12}},
            /* ExpectedScaledSegments */ {
                {0.5, {{20, 0}, {24, 1}}},
                {1, {{10, 0}, {12, 1}}},
                {2, {{5, 0}, {6, 1}}},
            },
            /* ExpectedShiftedSegments */ {
                {1, 0, {{11, 0}, {13, 1}}},
                {0, 1, {{10, 1}, {12, 2}}},
                {1, 1, {{11, 1}, {13, 2}}},
            },
        },
        {
            /* Name */ "diagonalSegment",
            /* Segment */ {{0, 0}, {1, 1}},
            /* ExpectedTransposedSegment */ {{0, 0}, {1, 1}},
            /* ExpectedScaledSegments */ {
                {0.5, {{0, 0}, {2, 1}}},
                {1, {{0, 0}, {1, 1}}},
                {2, {{0, 0}, {0.5, 1}}},
            },
            /* ExpectedShiftedSegments */ {
                {1, 0, {{1, 0}, {2, 1}}},
                {0, 1, {{0, 1}, {1, 2}}},
                {1, 1, {{1, 1}, {2, 2}}},
            },
        },
        {
            /* Name */ "verticalSegment",
            /* Segment */ {{0, 0}, {0, 1}},
            /* ExpectedTransposedSegment */ {{0, 0}, {1, 0}},
            /* ExpectedScaledSegments */ {
                {0.5, {{0, 0}, {0, 1}}},
                {1, {{0, 0}, {0, 1}}},
                {2, {{0, 0}, {0, 1}}},
            },
            /* ExpectedShiftedSegments */ {
                {1, 0, {{1, 0}, {1, 1}}},
                {0, 1, {{0, 1}, {0, 2}}},
                {1, 1, {{1, 1}, {1, 2}}},
            },
        },
        {
            /* Name */ "horizontalSegment",
            /* Segment */ {{0, 0}, {1, 0}},
            /* ExpectedTransposedSegment */ {{0, 0}, {0, 1}},
            /* ExpectedScaledSegments */ {
                {0.5, {{0, 0}, {2, 0}}},
                {1, {{0, 0}, {1, 0}}},
                {2, {{0, 0}, {0.5, 0}}},
            },
            /* ExpectedShiftedSegments */ {
                {1, 0, {{1, 0}, {2, 0}}},
                {0, 1, {{0, 1}, {1, 1}}},
                {1, 1, {{1, 1}, {2, 1}}},
            },
        },
        {
            /* Name */ "pointSegment",
            /* Segment */ {{0, 0}, {0, 0}},
            /* ExpectedTransposedSegment */ {{0, 0}, {0, 0}},
            /* ExpectedScaledSegments */ {
                {0.5, {{0, 0}, {0, 0}}},
                {1, {{0, 0}, {0, 0}}},
                {2, {{0, 0}, {0, 0}}},
            },
            /* ExpectedShiftedSegments */ {
                {1, 0, {{1, 0}, {1, 0}}},
                {0, 1, {{0, 1}, {0, 1}}},
                {1, 1, {{1, 1}, {1, 1}}},
            },
        }
    };

    for (const auto& testCase : testCases) {
        auto testCaseMsg = NYT::Format("In the test case %v", testCase.Name);

        const auto& segment = testCase.Segment;

        EXPECT_EQ(testCase.ExpectedTransposedSegment, segment.Transpose()) << testCaseMsg;

        for (const auto& [scale, expectedSegment] : testCase.ExpectedScaledSegments) {
            auto sampleMsg = NYT::Format("At scale %v", scale);

            EXPECT_EQ(expectedSegment, segment.ScaleArgument(scale)) << testCaseMsg << ". " << sampleMsg;
        }

        for (const auto& [deltaBound, deltaValue, expectedSegment] : testCase.ExpectedShiftedSegments) {
            auto sampleMsg = NYT::Format("At shift [ %v, %v ]", deltaBound, deltaValue);

            EXPECT_EQ(expectedSegment, segment.Shift(deltaBound, deltaValue)) << testCaseMsg << ". " << sampleMsg;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

// TODO(eshcherbin): Add tests for vector-valued segment and function, when they are officially out.

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
