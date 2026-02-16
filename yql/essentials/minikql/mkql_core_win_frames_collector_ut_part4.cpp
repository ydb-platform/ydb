#include "mkql_core_win_frames_collector_test_helper.h"

using namespace NKikimr::NMiniKQL;
using namespace NKikimr::NMiniKQL::NTest::NWindow;

// clang-format off
Y_UNIT_TEST_SUITE(TCoreWinFramesCollectorTestPart4) {

Y_UNIT_TEST(RangeIncremental_BasicIncremental) {
    TTestCase<ui64, ESortOrder::Asc> testCase = {
        .RangeIncrementals = {
            TInputRange<TRangeVariant>{5, EDirection::Following}
        },
        .InputElements = {ui64(10), ui64(15), ui64(20), ui64(25), ui64(30)},
        .ExpectedStates = {
            {
                .CurrentElement = 10,
                .QueueContent = {10, 15, 20},
                .RangeIncrementalChecks = {{1, 2}},
            },
            {
                .CurrentElement = 15,
                .QueueContent = {15, 20, 25},
                .RangeIncrementalChecks = {{1, 2}},
            },
            {
                .CurrentElement = 20,
                .QueueContent = {20, 25, 30},
                .RangeIncrementalChecks = {{1, 2}},
            },
            {
                .CurrentElement = 25,
                .QueueContent = {25, 30},
                .RangeIncrementalChecks = {{1, 2}},
            },
            {
                .CurrentElement = 30,
                .QueueContent = {30},
                .RangeIncrementalChecks = {{0, 1}},
            }
        }
    };

    RunTestCase(testCase);
}

Y_UNIT_TEST(RowIncremental_BasicIncremental) {
    TTestCase<ui64, ESortOrder::Asc> testCase = {
        .RowIncrementals = {
            TInputRow{2, EDirection::Following}
        },
        .InputElements = {ui64(10), ui64(20), ui64(30), ui64(40), ui64(50)},
        .ExpectedStates = {
            {
                .CurrentElement = 10,
                .QueueContent = {10, 20, 30},
                .RowIncrementalChecks = {{2, 3}},
            },
            {
                .CurrentElement = 20,
                .QueueContent = {20, 30, 40},
                .RowIncrementalChecks = {{2, 3}},
            },
            {
                .CurrentElement = 30,
                .QueueContent = {30, 40, 50},
                .RowIncrementalChecks = {{2, 3}},
            },
            {
                .CurrentElement = 40,
                .QueueContent = {40, 50},
                .RowIncrementalChecks = {{1, 2}},
            },
            {
                .CurrentElement = 50,
                .QueueContent = {50},
                .RowIncrementalChecks = {{0, 1}},
            }
        }
    };

    RunTestCase(testCase);
}

Y_UNIT_TEST(RangeIncremental_WithGaps) {
    TTestCase<ui64, ESortOrder::Asc> testCase = {
        .RangeIncrementals = {
            TInputRange<TRangeVariant>{10, EDirection::Following}
        },
        .InputElements = {ui64(10), ui64(25), ui64(40), ui64(55)},
        .ExpectedStates = {
            {
                .CurrentElement = 10,
                .QueueContent = {10, 25},
                .RangeIncrementalChecks = {{0, 1}},
            },
            {
                .CurrentElement = 25,
                .QueueContent = {25, 40},
                .RangeIncrementalChecks = {{0, 1}},
            },
            {
                .CurrentElement = 40,
                .QueueContent = {40, 55},
                .RangeIncrementalChecks = {{0, 1}},
            },
            {
                .CurrentElement = 55,
                .QueueContent = {55},
                .RangeIncrementalChecks = {{0, 1}},
            }
        }
    };

    RunTestCase(testCase);
}

Y_UNIT_TEST(RangeIncremental_MultipleNewElements) {
    TTestCase<ui64, ESortOrder::Asc> testCase = {
        .RangeIncrementals = {
            TInputRange<TRangeVariant>{15, EDirection::Following}
        },
        .InputElements = {ui64(10), ui64(12), ui64(14), ui64(16), ui64(18), ui64(20)},
        .ExpectedStates = {
            {
                .CurrentElement = 10,
                .QueueContent = {10, 12, 14, 16, 18, 20},
                .RangeIncrementalChecks = {{5, 6}},
            },
            {
                .CurrentElement = 12,
                .QueueContent = {12, 14, 16, 18, 20},
                .RangeIncrementalChecks = {{4, 5}},
            },
            {
                .CurrentElement = 14,
                .QueueContent = {14, 16, 18, 20},
                .RangeIncrementalChecks = {{3, 4}},
            },
            {
                .CurrentElement = 16,
                .QueueContent = {16, 18, 20},
                .RangeIncrementalChecks = {{2, 3}},
            },
            {
                .CurrentElement = 18,
                .QueueContent = {18, 20},
                .RangeIncrementalChecks = {{1, 2}},
            },
            {
                .CurrentElement = 20,
                .QueueContent = {20},
                .RangeIncrementalChecks = {{0, 1}},
            }
        }
    };

    RunTestCase(testCase);
}

Y_UNIT_TEST(CombinedDelta_RangeAndRow) {
    TTestCase<ui64, ESortOrder::Asc> testCase = {
        .RangeIncrementals = {
            TInputRange<TRangeVariant>{10, EDirection::Following}
        },
        .RowIncrementals = {
            TInputRow{1, EDirection::Following}
        },
        .InputElements = {ui64(10), ui64(15), ui64(20), ui64(30)},
        .ExpectedStates = {
            {
                .CurrentElement = 10,
                .QueueContent = {10, 15, 20, 30},
                .RangeIncrementalChecks = {{2, 3}},
                .RowIncrementalChecks = {{1, 2}}
            },
            {
                .CurrentElement = 15,
                .QueueContent = {15, 20, 30},
                .RangeIncrementalChecks = {{1, 2}},
                .RowIncrementalChecks = {{1, 2}}
            },
            {
                .CurrentElement = 20,
                .QueueContent = {20, 30},
                .RangeIncrementalChecks = {{1, 2}},
                .RowIncrementalChecks = {{1, 2}}
            },
            {
                .CurrentElement = 30,
                .QueueContent = {30},
                .RangeIncrementalChecks = {{0, 1}},
                .RowIncrementalChecks = {{0, 1}}
            }
        }
    };

    RunTestCase(testCase);
}

Y_UNIT_TEST(RangeIncremental_DescendingOrder) {
    TTestCase<ui64, ESortOrder::Desc> testCase = {
        .RangeIncrementals = {
            TInputRange<TRangeVariant>{5, EDirection::Following}
        },
        .InputElements = {ui64(30), TYield(), TYield(), ui64(25), ui64(20), ui64(15), ui64(10)},
        .ExpectedStates = {
            {
                .CurrentElement = 30,
                .QueueContent = {30, 25, 20},
                .RangeIncrementalChecks = {{1, 2}},
            },
            {
                .CurrentElement = 25,
                .QueueContent = {25, 20, 15},
                .RangeIncrementalChecks = {{1, 2}},
            },
            {
                .CurrentElement = 20,
                .QueueContent = {20, 15, 10},
                .RangeIncrementalChecks = {{1, 2}},
            },
            {
                .CurrentElement = 15,
                .QueueContent = {15, 10},
                .RangeIncrementalChecks = {{1, 2}},
            },
            {
                .CurrentElement = 10,
                .QueueContent = {10},
                .RangeIncrementalChecks = {{0, 1}},
            }
        }
    };

    RunTestCase(testCase);
}

Y_UNIT_TEST(TwoRangeIntervalsAndTwoRowIntervalsDeltas_MixedDirections) {
    TTestCase<ui64, ESortOrder::Asc> testCase = {
        .RangeIncrementals = {
            TInputRange<TRangeVariant>{3, EDirection::Following},
            TInputRange<TRangeVariant>{5, EDirection::Preceding},
        },
        .RowIncrementals = {
            TInputRow(1, EDirection::Following),
            TInputRow(1, EDirection::Preceding),
        },
        .InputElements = {ui64(1), ui64(2), ui64(4), ui64(8), TYield()},
        .ExpectedStates = {
            {
                .CurrentElement = 1,
                .QueueContent = {1, 2, 4, 8},
                .RangeIncrementalChecks = {
                    {2, 3},
                    TEmptyInterval{}
                },
                .RowIncrementalChecks = {
                    {1, 2},
                    TEmptyInterval{}
                },
            },
            {
                .CurrentElement = 2,
                .QueueContent = {1, 2, 4, 8},
                .RangeIncrementalChecks = {
                    {2, 3},
                    TEmptyInterval{}
                },
                .RowIncrementalChecks = {
                    {2, 3},
                    {0, 1}
                },
            },
            {
                .CurrentElement = 4,
                .QueueContent = {1, 2, 4, 8},
                .RangeIncrementalChecks = {
                    {2, 3},
                    TEmptyInterval{}
                },
                .RowIncrementalChecks = {
                    {3, 4},
                    {1, 2}
                },
            },
            {
                .CurrentElement = 8,
                .QueueContent = {2, 4, 8},
                .RangeIncrementalChecks = {
                    {2, 3},
                    {0, 1}
                },
                .RowIncrementalChecks = {
                    {2, 3},
                    {1, 2}
                }
            }
        }
    };

    RunTestCase(testCase);
}

Y_UNIT_TEST(RowIntervalAndRowDelta_Desc) {
    TTestCase<i64, ESortOrder::Desc> testCase = {
        .RowIntervals = {
            TInputRowWindowFrame(
                TInputRow(1, EDirection::Preceding),
                TInputRow(1, EDirection::Following)
            )
        },
        .RowIncrementals = {
            TInputRow{1, EDirection::Following}
        },
        .InputElements = {TYield(), i64(8), i64(5), TYield(), i64(2), i64(-5)},
        .ExpectedStates = {
            {
                .CurrentElement = 8,
                .QueueContent = {8, 5},
                .RowIntervalChecks = {{0, 2}},
                .RowIncrementalChecks = {{1, 2}}
            },
            {
                .CurrentElement = 5,
                .QueueContent = {8, 5, 2},
                .RowIntervalChecks = {{0, 3}},
                .RowIncrementalChecks = {{2, 3}}
            },
            {
                .CurrentElement = 2,
                .QueueContent = {5, 2, -5},
                .RowIntervalChecks = {{0, 3}},
                .RowIncrementalChecks = {{2, 3}}
            },
            {
                .CurrentElement = -5,
                .QueueContent = {2, -5},
                .RowIntervalChecks = {{0, 2}},
                .RowIncrementalChecks = {{1, 2}}
            }
        }
    };

    RunTestCase(testCase);
}

Y_UNIT_TEST(RangeIntervalAndRangeDelta_Desc) {
    TTestCase<i64, ESortOrder::Desc> testCase = {
        .RangeIntervals = {
            TInputRangeWindowFrame<TRangeVariant>{
                TInputRange<TRangeVariant>{3, EDirection::Preceding},
                TInputRange<TRangeVariant>{3, EDirection::Following}
            }
        },
        .RangeIncrementals = {
            TInputRange<TRangeVariant>{3, EDirection::Following}
        },
        .InputElements = {i64(8), i64(5), TYield(), i64(2), i64(-5)},
        .ExpectedStates = {
            {
                .CurrentElement = 8,
                .QueueContent = {8, 5, 2},
                .RangeIntervalChecks = {{0, 2}},
                .RangeIncrementalChecks = {{1, 2}}
            },
            {
                .CurrentElement = 5,
                .QueueContent = {8, 5, 2, -5},
                .RangeIntervalChecks = {{0, 3}},
                .RangeIncrementalChecks = {{2, 3}}
            },
            {
                .CurrentElement = 2,
                .QueueContent = {5, 2, -5},
                .RangeIntervalChecks = {{0, 2}},
                .RangeIncrementalChecks = {{1, 2}}
            },
            {
                .CurrentElement = -5,
                .QueueContent = {-5},
                .RangeIntervalChecks = {{0, 1}},
                .RangeIncrementalChecks = {{0, 1}}
            }
        }
    };

    RunTestCase(testCase);
}

}
