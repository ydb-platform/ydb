#include "mkql_core_win_frames_collector_test_helper.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr::NMiniKQL;
using namespace NKikimr::NMiniKQL::NTest::NWindow;

// clang-format off
Y_UNIT_TEST_SUITE(TCoreWinFramesCollectorTestPart2) {

Y_UNIT_TEST(RowAndRangeIntervals_Combined) {
    TTestCase<ui64, ESortOrder::Asc> testCase = {
        .RowIntervals = {
            TInputRowWindowFrame(
                TInputRow(1, EDirection::Preceding),
                TInputRow(1, EDirection::Following)
            )
        },
        .RangeIntervals = {
            TInputRangeWindowFrame<TRangeVariant>{
                TInputRange<TRangeVariant>{5, EDirection::Preceding},
                TInputRange<TRangeVariant>{5, EDirection::Following}
            }
        },
        .InputElements = {ui64(10), ui64(15), ui64(20), ui64(30), ui64(40)},
        .ExpectedStates = {
            {
                .CurrentElement = 10,
                .QueueContent = {10, 15, 20},
                .RowIntervalChecks = {{0, 2}},
                .RangeIntervalChecks = {{0, 2}}
            },
            {
                .CurrentElement = 15,
                .QueueContent = {10, 15, 20, 30},
                .RowIntervalChecks = {{0, 3}},
                .RangeIntervalChecks = {{0, 3}}
            },
            {
                .CurrentElement = 20,
                .QueueContent = {15, 20, 30},
                .RowIntervalChecks = {{0, 3}},
                .RangeIntervalChecks = {{0, 2}}
            },
            {
                .CurrentElement = 30,
                .QueueContent = {20, 30, 40},
                .RowIntervalChecks = {{0, 3}},
                .RangeIntervalChecks = {{1, 2}}
            },
            {
                .CurrentElement = 40,
                .QueueContent = {30, 40},
                .RowIntervalChecks = {{0, 2}},
                .RangeIntervalChecks = {{1, 2}}
            }
        }
    };

    RunTestCase(testCase);

}

Y_UNIT_TEST(RowAndRangeIntervals_RowCompletelyLeft) {
    TTestCase<ui64, ESortOrder::Asc> testCase = {
        .RowIntervals = {
            TInputRowWindowFrame(
                TInputRow(3, EDirection::Preceding),
                TInputRow(1, EDirection::Preceding)
            )
        },
        .RangeIntervals = {
            TInputRangeWindowFrame<TRangeVariant>{
                TInputRange<TRangeVariant>{5, EDirection::Preceding},
                TInputRange<TRangeVariant>{5, EDirection::Following}
            }
        },
        .InputElements = {ui64(10), ui64(15), ui64(20), ui64(25), ui64(30), ui64(35)},
        .ExpectedStates = {
            {
                .CurrentElement = 10,
                .QueueContent = {10, 15, 20},
                .RowIntervalChecks = {{0, 0}},
                .RangeIntervalChecks = {{0, 2}}
            },
            {
                .CurrentElement = 15,
                .QueueContent = {10, 15, 20, 25},
                .RowIntervalChecks = {{0, 1}},
                .RangeIntervalChecks = {{0, 3}}
            },
            {
                .CurrentElement = 20,
                .QueueContent = {10, 15, 20, 25, 30},
                .RowIntervalChecks = {{0, 2}},
                .RangeIntervalChecks = {{1, 4}}
            },
            {
                .CurrentElement = 25,
                .QueueContent = {10, 15, 20, 25, 30, 35},
                .RowIntervalChecks = {{0, 3}},
                .RangeIntervalChecks = {{2, 5}}
            },
            {
                .CurrentElement = 30,
                .QueueContent = {15, 20, 25, 30, 35},
                .RowIntervalChecks = {{0, 3}},
                .RangeIntervalChecks = {{2, 5}}
            },
            {
                .CurrentElement = 35,
                .QueueContent = {20, 25, 30, 35},
                .RowIntervalChecks = {{0, 3}},
                .RangeIntervalChecks = {{2, 4}}
            }
        }
    };

    RunTestCase(testCase);
}

Y_UNIT_TEST(RowAndRangeIntervals_RowCompletelyRight) {
    TTestCase<ui64, ESortOrder::Asc> testCase = {
        .RowIntervals = {
            TInputRowWindowFrame(
                TInputRow(1, EDirection::Following),
                TInputRow(3, EDirection::Following)
            )
        },
        .RangeIntervals = {
            TInputRangeWindowFrame<TRangeVariant>{
                TInputRange<TRangeVariant>{5, EDirection::Preceding},
                TInputRange<TRangeVariant>{5, EDirection::Following}
            }
        },
        .InputElements = {ui64(10), ui64(15), ui64(20), ui64(25), ui64(30), ui64(35)},
        .ExpectedStates = {
            {
                .CurrentElement = 10,
                .QueueContent = {10, 15, 20, 25},
                .RowIntervalChecks = {{1, 4}},
                .RangeIntervalChecks = {{0, 2}}
            },
            {
                .CurrentElement = 15,
                .QueueContent = {10, 15, 20, 25, 30},
                .RowIntervalChecks = {{2, 5}},
                .RangeIntervalChecks = {{0, 3}}
            },
            {
                .CurrentElement = 20,
                .QueueContent = {15, 20, 25, 30, 35},
                .RowIntervalChecks = {{2, 5}},
                .RangeIntervalChecks = {{0, 3}}
            },
            {
                .CurrentElement = 25,
                .QueueContent = {20, 25, 30, 35},
                .RowIntervalChecks = {{2, 4}},
                .RangeIntervalChecks = {{0, 3}}
            },
            {
                .CurrentElement = 30,
                .QueueContent = {25, 30, 35},
                .RowIntervalChecks = {{2, 3}},
                .RangeIntervalChecks = {{0, 3}}
            },
            {
                .CurrentElement = 35,
                .QueueContent = {30, 35},
                .RowIntervalChecks = {{2, 2}},
                .RangeIntervalChecks = {{0, 2}}
            }
        }
    };

    RunTestCase(testCase);
}

Y_UNIT_TEST(RowAndRangeIntervals_RowPartiallyLeft) {
    TTestCase<ui64, ESortOrder::Asc> testCase = {
        .RowIntervals = {
            TInputRowWindowFrame(
                TInputRow(2, EDirection::Preceding),
                TInputRow(0, EDirection::Following)
            )
        },
        .RangeIntervals = {
            TInputRangeWindowFrame<TRangeVariant>{
                TInputRange<TRangeVariant>{3, EDirection::Preceding},
                TInputRange<TRangeVariant>{7, EDirection::Following}
            }
        },
        .InputElements = {ui64(10), ui64(15), ui64(20), ui64(25), ui64(30)},
        .ExpectedStates = {
            {
                .CurrentElement = 10,
                .QueueContent = {10, 15, 20},
                .RowIntervalChecks = {{0, 1}},
                .RangeIntervalChecks = {{0, 2}}
            },
            {
                .CurrentElement = 15,
                .QueueContent = {10, 15, 20, 25},
                .RowIntervalChecks = {{0, 2}},
                .RangeIntervalChecks = {{1, 3}}
            },
            {
                .CurrentElement = 20,
                .QueueContent = {10, 15, 20, 25, 30},
                .RowIntervalChecks = {{0, 3}},
                .RangeIntervalChecks = {{2, 4}}
            },
            {
                .CurrentElement = 25,
                .QueueContent = {15, 20, 25, 30},
                .RowIntervalChecks = {{0, 3}},
                .RangeIntervalChecks = {{2, 4}}
            },
            {
                .CurrentElement = 30,
                .QueueContent = {20, 25, 30},
                .RowIntervalChecks = {{0, 3}},
                .RangeIntervalChecks = {{2, 3}}
            }
        }
    };

    RunTestCase(testCase);
}

Y_UNIT_TEST(RowAndRangeIntervals_RowPartiallyRight) {
    TTestCase<ui64, ESortOrder::Asc> testCase = {
        .RowIntervals = {
            TInputRowWindowFrame(
                TInputRow(0, EDirection::Preceding),
                TInputRow(2, EDirection::Following)
            )
        },
        .RangeIntervals = {
            TInputRangeWindowFrame<TRangeVariant>{
                TInputRange<TRangeVariant>{7, EDirection::Preceding},
                TInputRange<TRangeVariant>{3, EDirection::Following}
            }
        },
        .InputElements = {ui64(10), ui64(15), ui64(20), ui64(25), ui64(30)},
        .ExpectedStates = {
            {
                .CurrentElement = 10,
                .QueueContent = {10, 15, 20},
                .RowIntervalChecks = {{0, 3}},
                .RangeIntervalChecks = {{0, 1}}
            },
            {
                .CurrentElement = 15,
                .QueueContent = {10, 15, 20, 25},
                .RowIntervalChecks = {{1, 4}},
                .RangeIntervalChecks = {{0, 2}}
            },
            {
                .CurrentElement = 20,
                .QueueContent = {15, 20, 25, 30},
                .RowIntervalChecks = {{1, 4}},
                .RangeIntervalChecks = {{0, 2}}
            },
            {
                .CurrentElement = 25,
                .QueueContent = {20, 25, 30},
                .RowIntervalChecks = {{1, 3}},
                .RangeIntervalChecks = {{0, 2}}
            },
            {
                .CurrentElement = 30,
                .QueueContent = {25, 30},
                .RowIntervalChecks = {{1, 2}},
                .RangeIntervalChecks = {{0, 2}}
            }
        }
    };

    RunTestCase(testCase);
}

Y_UNIT_TEST(RowInterval_Uint64MaxValuesRow) {
    TTestCase<ui64, ESortOrder::Asc> testCase = {
        .RowIntervals = {
            TInputRowWindowFrame(
                TInputRow(std::numeric_limits<ui32>::max(), EDirection::Preceding),
                TInputRow(std::numeric_limits<ui32>::max(), EDirection::Following)
            )
        },
        .InputElements = {ui64(0), std::numeric_limits<ui64>::max()},
        .ExpectedStates = {
            {
                .CurrentElement = 0,
                .QueueContent = {0, std::numeric_limits<ui64>::max()},
                .RowIntervalChecks = {{0, 2}},
            },
            {
                .CurrentElement = std::numeric_limits<ui64>::max(),
                .QueueContent = {0, std::numeric_limits<ui64>::max()},
                .RowIntervalChecks = {{0, 2}},
            }
        }
    };

    RunTestCase(testCase);
}

Y_UNIT_TEST(RowInterval_Uint64MaxValuesRange) {
    TTestCase<ui64, ESortOrder::Asc> testCase = {
        .RangeIntervals = {
            TInputRangeWindowFrame<TRangeVariant>(
                TInputRange<TRangeVariant>(std::numeric_limits<ui64>::max(), EDirection::Preceding),
                TInputRange<TRangeVariant>(std::numeric_limits<ui64>::max(), EDirection::Following)
            )
        },
        .InputElements = {ui64(0), std::numeric_limits<ui64>::max()},
        .ExpectedStates = {
            {
                .CurrentElement = 0,
                .QueueContent = {0, std::numeric_limits<ui64>::max()},
                .RangeIntervalChecks = {{0, 2}}
            },
            {
                .CurrentElement = std::numeric_limits<ui64>::max(),
                .QueueContent = {0, std::numeric_limits<ui64>::max()},
                .RangeIntervalChecks = {{0, 2}}
            }
        }
    };

    RunTestCase(testCase);
}

Y_UNIT_TEST(RangeInterval_FloatExtremeValues) {
    float negMax = -std::numeric_limits<float>::max();
    float posMax = std::numeric_limits<float>::max();
    float nan1 = std::numeric_limits<float>::quiet_NaN();
    float nan2 = std::numeric_limits<float>::quiet_NaN();

    TTestCase<float, ESortOrder::Asc> testCase = {
        .RangeIntervals = {
            TInputRangeWindowFrame<TRangeVariant>{
                TInputRange<TRangeVariant>{100, EDirection::Preceding},
                TInputRange<TRangeVariant>{100, EDirection::Following}
            }
        },
        .InputElements = {negMax, posMax, nan1, nan2},
        .ExpectedStates = {
            {
                .CurrentElement = negMax,
                .QueueContent = {negMax, posMax},
                .RangeIntervalChecks = {{0, 1}}
            },
            {
                .CurrentElement = posMax,
                .QueueContent = {posMax, nan1},
                .RangeIntervalChecks = {{0, 1}}
            },
            {
                .CurrentElement = nan1,
                .QueueContent = {nan1, nan2},
                .RangeIntervalChecks = {{0, 2}}
            },
            {
                .CurrentElement = nan2,
                .QueueContent = {nan1, nan2},
                .RangeIntervalChecks = {{0, 2}}
            }
        }
    };

    RunTestCase(testCase);
}


Y_UNIT_TEST(TwoRangeEmptyIntervals_BothDirections_Asc) {
    TTestCase<ui64, ESortOrder::Asc> testCase = {
        .RangeIntervals = {
            TInputRangeWindowFrame<TRangeVariant>{
                TInputRange<TRangeVariant>{5, EDirection::Following},
                TInputRange<TRangeVariant>{6, EDirection::Following}
            },
            TInputRangeWindowFrame<TRangeVariant>{
                TInputRange<TRangeVariant>{6, EDirection::Preceding},
                TInputRange<TRangeVariant>{5, EDirection::Preceding}
            }
        },
        .InputElements = {ui64(10), ui64(20), ui64(30), ui64(40)},
        .ExpectedStates = {
            {
                .CurrentElement = 10,
                .QueueContent = {10, 20},
                .RangeIntervalChecks = {
                    {1, 1},
                    {0, 0}
                }
            },
            {
                .CurrentElement = 20,
                .QueueContent = {20, 30},
                .RangeIntervalChecks = {
                    {1, 1},
                    {0, 0}
                }
            },
            {
                .CurrentElement = 30,
                .QueueContent = {30, 40},
                .RangeIntervalChecks = {
                    {1, 1},
                    {0, 0}
                }
            },
            {
                .CurrentElement = 40,
                .QueueContent = {40},
                .RangeIntervalChecks = {
                    {1, 1},
                    {0, 0}
                }
            }
        }
    };

    RunTestCase(testCase);
}


Y_UNIT_TEST(TwoRangeEmptyIntervals_LargeOffsets_Asc) {
    TTestCase<ui64, ESortOrder::Asc> testCase = {
        .RangeIntervals = {
            TInputRangeWindowFrame<TRangeVariant>{
                TInputRange<TRangeVariant>{14, EDirection::Following},
                TInputRange<TRangeVariant>{15, EDirection::Following}
            },
            TInputRangeWindowFrame<TRangeVariant>{
                TInputRange<TRangeVariant>{15, EDirection::Preceding},
                TInputRange<TRangeVariant>{14, EDirection::Preceding}
            }
        },
        .InputElements = {ui64(10), ui64(20), ui64(30), ui64(40)},
        .ExpectedStates = {
            {
                .CurrentElement = 10,
                .QueueContent = {10, 20, 30},
                .RangeIntervalChecks = {
                    {2, 2},
                    {0, 0}
                }
            },
            {
                .CurrentElement = 20,
                .QueueContent = {10, 20, 30, 40},
                .RangeIntervalChecks = {
                    {3, 3},
                    {0, 0}
                }
            },
            {
                .CurrentElement = 30,
                .QueueContent = {20, 30, 40},
                .RangeIntervalChecks = {
                    {3, 3},
                    {0, 0}
                }
            },
            {
                .CurrentElement = 40,
                .QueueContent = {30, 40},
                .RangeIntervalChecks = {
                    {2, 2},
                    {0, 0}
                }
            }
        }
    };

    RunTestCase(testCase);
}


Y_UNIT_TEST(RangeInterval_InvalidMinMaxOrder_Asc) {
    TTestCase<ui64, ESortOrder::Asc> testCase = {
        .RangeIntervals = {
            TInputRangeWindowFrame<TRangeVariant>{
                TInputRange<TRangeVariant>{10, EDirection::Following},
                TInputRange<TRangeVariant>{10, EDirection::Preceding}
            }
        },
        .InputElements = {ui64(10), ui64(20), ui64(30)},
        .ExpectedStates = {
            {
                .CurrentElement = 10,
                .QueueContent = {10},
                .RangeIntervalChecks = {
                    {1, 0}
                }
            },
            {
                .CurrentElement = 20,
                .QueueContent = {20},
                .RangeIntervalChecks = {
                    {1, 0}
                }
            },
            {
                .CurrentElement = 30,
                .QueueContent = {30},
                .RangeIntervalChecks = {
                    {1, 0}
                }
            }
        }
    };

    RunTestCase(testCase);
}


Y_UNIT_TEST(RangeInterval_InvalidMinMaxOrderLarge_Asc) {
    TTestCase<ui64, ESortOrder::Asc> testCase = {
        .RangeIntervals = {
            TInputRangeWindowFrame<TRangeVariant>{
                TInputRange<TRangeVariant>{25, EDirection::Following},
                TInputRange<TRangeVariant>{25, EDirection::Preceding}
            }
        },
        .InputElements = {ui64(10), ui64(20), ui64(30)},
        .ExpectedStates = {
            {
                .CurrentElement = 10,
                .QueueContent = {10},
                .RangeIntervalChecks = {
                    {1, 0}
                }
            },
            {
                .CurrentElement = 20,
                .QueueContent = {20},
                .RangeIntervalChecks = {
                    {1, 0}
                }
            },
            {
                .CurrentElement = 30,
                .QueueContent = {30},
                .RangeIntervalChecks = {
                    {1, 0}
                }
            }
        }
    };

    RunTestCase(testCase);
}

Y_UNIT_TEST(BasicRowInterval_Desc) {
    TTestCase<ui64, ESortOrder::Desc> testCase = {
        .RowIntervals = {
            TInputRowWindowFrame(
                TInputRow(2, EDirection::Preceding),
                TInputRow(2, EDirection::Following)
            )
        },
        .InputElements = {ui64(50), ui64(40), ui64(30), ui64(20), ui64(10)},
        .ExpectedStates = {
            {
                .CurrentElement = 50,
                .QueueContent = {50, 40, 30},
                .RowIntervalChecks = {{0, 3}},
            },
            {
                .CurrentElement = 40,
                .QueueContent = {50, 40, 30, 20},
                .RowIntervalChecks = {{0, 4}},
            },
            {
                .CurrentElement = 30,
                .QueueContent = {50, 40, 30, 20, 10},
                .RowIntervalChecks = {{0, 5}},
            },
            {
                .CurrentElement = 20,
                .QueueContent = {40, 30, 20, 10},
                .RowIntervalChecks = {{0, 4}},
            },
            {
                .CurrentElement = 10,
                .QueueContent = {30, 20, 10},
                .RowIntervalChecks = {{0, 3}},
            }
        }
    };

    RunTestCase(testCase);
}

Y_UNIT_TEST(BasicRangeInterval_Desc) {
    TTestCase<ui64, ESortOrder::Desc> testCase = {
        .RangeIntervals = {
            TInputRangeWindowFrame<TRangeVariant>{
                TInputRange<TRangeVariant>{5, EDirection::Preceding},
                TInputRange<TRangeVariant>{5, EDirection::Following}
            }
        },
        .InputElements = {ui64(30), ui64(25), TYield(), ui64(20), ui64(15), TYield(), ui64(10)},
        .ExpectedStates = {
            {
                .CurrentElement = 30,
                .QueueContent = {30, 25, 20},
                .RangeIntervalChecks = {{0, 2}}
            },
            {
                .CurrentElement = 25,
                .QueueContent = {30, 25, 20, 15},
                .RangeIntervalChecks = {{0, 3}}
            },
            {
                .CurrentElement = 20,
                .QueueContent = {25, 20, 15, 10},
                .RangeIntervalChecks = {{0, 3}}
            },
            {
                .CurrentElement = 15,
                .QueueContent = {20, 15, 10},
                .RangeIntervalChecks = {{0, 3}}
            },
            {
                .CurrentElement = 10,
                .QueueContent = {15, 10},
                .RangeIntervalChecks = {{0, 2}}
            }
        }
    };

    RunTestCase(testCase);
}

Y_UNIT_TEST(RowAndRangeIntervals_Desc) {
    TTestCase<ui64, ESortOrder::Desc> testCase = {
        .RowIntervals = {
            TInputRowWindowFrame(
                TInputRow(1, EDirection::Preceding),
                TInputRow(1, EDirection::Following)
            )
        },
        .RangeIntervals = {
            TInputRangeWindowFrame<TRangeVariant>{
                TInputRange<TRangeVariant>{5, EDirection::Preceding},
                TInputRange<TRangeVariant>{5, EDirection::Following}
            }
        },
        .InputElements = {ui64(40), ui64(30), ui64(20), ui64(15), ui64(10)},
        .ExpectedStates = {
            {
                .CurrentElement = 40,
                .QueueContent = {40, 30},
                .RowIntervalChecks = {{0, 2}},
                .RangeIntervalChecks = {{0, 1}}
            },
            {
                .CurrentElement = 30,
                .QueueContent = {40, 30, 20},
                .RowIntervalChecks = {{0, 3}},
                .RangeIntervalChecks = {{1, 2}}
            },
            {
                .CurrentElement = 20,
                .QueueContent = {30, 20, 15, 10},
                .RowIntervalChecks = {{0, 3}},
                .RangeIntervalChecks = {{1, 3}}
            },
            {
                .CurrentElement = 15,
                .QueueContent = {20, 15, 10},
                .RowIntervalChecks = {{0, 3}},
                .RangeIntervalChecks = {{0, 3}}
            },
            {
                .CurrentElement = 10,
                .QueueContent = {15, 10},
                .RowIntervalChecks = {{0, 2}},
                .RangeIntervalChecks = {{0, 2}}
            }
        }
    };

    RunTestCase(testCase);
}

Y_UNIT_TEST(RangeInterval_StringLength) {
    TTestCase<TString, ESortOrder::Asc, size_t> testCase = {
        .RangeIntervals = {
            TInputRangeWindowFrame<TRangeVariant>{
                TInputRange<TRangeVariant>{1, EDirection::Preceding},
                TInputRange<TRangeVariant>{0, EDirection::Following}
            }
        },
        .ElementGetter = [](const TString& str) -> TMaybe<size_t> { return TMaybe<size_t>(str.size()); },
        .InputElements = {TString("a"), TString("aa"), TString("aaa"), TString("aaaa")},
        .ExpectedStates = {
            {
                .CurrentElement = "a",
                .QueueContent = {"a", "aa"},
                .RangeIntervalChecks = {{0, 1}}
            },
            {
                .CurrentElement = "aa",
                .QueueContent = {"a", "aa", "aaa"},
                .RangeIntervalChecks = {{0, 2}}
            },
            {
                .CurrentElement = "aaa",
                .QueueContent = {"aa", "aaa", "aaaa"},
                .RangeIntervalChecks = {{0, 2}}
            },
            {
                .CurrentElement = "aaaa",
                .QueueContent = {"aaa", "aaaa"},
                .RangeIntervalChecks = {{0, 2}}
            }
        }
    };

    RunTestCase(testCase);
}

Y_UNIT_TEST(RangeInterval_WithOptionals_Asc) {
    TTestCase<TMaybe<ui64>, ESortOrder::Asc, ui64> testCase = {
        .RangeIntervals = {
            TInputRangeWindowFrame<TRangeVariant>{
                TInputRange<TRangeVariant>{0, EDirection::Following},
                TInputRange<TRangeVariant>{1, EDirection::Following}
            }
        },
        .ElementGetter = [](const TMaybe<ui64>& elem) -> TMaybe<ui64> {
            return elem;
        },
        .InputElements = {TMaybe<ui64>(), TMaybe<ui64>(), TMaybe<ui64>(1), TMaybe<ui64>(2), TMaybe<ui64>(3)},
        .ExpectedStates = {
            {
                .CurrentElement = TMaybe<ui64>(),
                .QueueContent = {TMaybe<ui64>(), TMaybe<ui64>(), TMaybe<ui64>(1)},
                .RangeIntervalChecks = {{0, 2}}
            },
            {
                .CurrentElement = TMaybe<ui64>(),
                .QueueContent = {TMaybe<ui64>(), TMaybe<ui64>(), TMaybe<ui64>(1)},
                .RangeIntervalChecks = {{0, 2}}
            },
            {
                .CurrentElement = TMaybe<ui64>(1),
                .QueueContent = {TMaybe<ui64>(1), TMaybe<ui64>(2), TMaybe<ui64>(3)},
                .RangeIntervalChecks = {{0, 2}}
            },
            {
                .CurrentElement = TMaybe<ui64>(2),
                .QueueContent = {TMaybe<ui64>(2), TMaybe<ui64>(3)},
                .RangeIntervalChecks = {{0, 2}}
            },
            {
                .CurrentElement = TMaybe<ui64>(3),
                .QueueContent = {TMaybe<ui64>(3)},
                .RangeIntervalChecks = {{0, 1}}
            }
        }
    };

    RunTestCase(testCase);
}


Y_UNIT_TEST(RangeInterval_WithOptionals_Desc) {
    TTestCase<TMaybe<ui8>, ESortOrder::Desc, ui8> testCase = {
        .RangeIntervals = {
            TInputRangeWindowFrame<TRangeVariant>{
                TInputRange<TRangeVariant>{2, EDirection::Following},
                TInputRange<TRangeVariant>{10, EDirection::Following}
            }
        },
        .ElementGetter = [](const TMaybe<ui8>& elem) -> TMaybe<ui8> {
            return elem;
        },
        .InputElements = {TMaybe<ui8>(), TMaybe<ui8>(), TMaybe<ui8>(3), TMaybe<ui8>(2), TMaybe<ui8>(1)},
        .ExpectedStates = {
            {
                .CurrentElement = TMaybe<ui8>(),
                .QueueContent = {TMaybe<ui8>(), TMaybe<ui8>(), TMaybe<ui8>(3)},
                .RangeIntervalChecks = {{0, 2}}
            },
            {
                .CurrentElement = TMaybe<ui8>(),
                .QueueContent = {TMaybe<ui8>(), TMaybe<ui8>(), TMaybe<ui8>(3)},
                .RangeIntervalChecks = {{0, 2}}
            },
            {
                .CurrentElement = TMaybe<ui8>(3),
                .QueueContent = {TMaybe<ui8>(3), TMaybe<ui8>(2), TMaybe<ui8>(1)},
                .RangeIntervalChecks = {{2, 3}}
            },
            {
                .CurrentElement = TMaybe<ui8>(2),
                .QueueContent = {TMaybe<ui8>(2), TMaybe<ui8>(1)},
                .RangeIntervalChecks = {{2, 2}}
            },
            {
                .CurrentElement = TMaybe<ui8>(1),
                .QueueContent = {TMaybe<ui8>(1)},
                .RangeIntervalChecks = {{1, 1}}
            }
        }
    };

    RunTestCase(testCase);
}

Y_UNIT_TEST(RangeInterval_RepeatedElements) {
    TTestCase<ui64, ESortOrder::Asc> testCase = {
        .RangeIntervals = {
            TInputRangeWindowFrame<TRangeVariant>{
                TInputRange<TRangeVariant>{10, EDirection::Preceding},
                TInputRange<TRangeVariant>{0, EDirection::Following}
            }
        },
        .InputElements = {ui64(1), ui64(2), ui64(2), ui64(4)},
        .ExpectedStates = {
            {
                .CurrentElement = 1,
                .QueueContent = {1, 2},
                .RangeIntervalChecks = {{0, 1}}
            },
            {
                .CurrentElement = 2,
                .QueueContent = {1, 2, 2, 4},
                .RangeIntervalChecks = {{0, 3}}
            },
            {
                .CurrentElement = 2,
                .QueueContent = {1, 2, 2, 4},
                .RangeIntervalChecks = {{0, 3}}
            },
            {
                .CurrentElement = 4,
                .QueueContent = {1, 2, 2, 4},
                .RangeIntervalChecks = {{0, 4}}
            }
        }
    };

    RunTestCase(testCase);
}

Y_UNIT_TEST(RangeInterval_WithOptionals_Desc_EmptyInterval) {
    TTestCase<TMaybe<ui8>, ESortOrder::Desc, ui8> testCase = {
        .RangeIntervals = {
            TInputRangeWindowFrame<TRangeVariant>{
                TInputRange<TRangeVariant>{10, EDirection::Following},
                TInputRange<TRangeVariant>{2, EDirection::Following}
            }
        },
        .ElementGetter = [](const TMaybe<ui8>& elem) -> TMaybe<ui8> {
            return elem;
        },
        .InputElements = {TMaybe<ui8>(), TMaybe<ui8>(), TMaybe<ui64>(3), TMaybe<ui64>(2), TMaybe<ui64>(1)},
        .ExpectedStates = {
            {
                .CurrentElement = TMaybe<ui64>(),
                .QueueContent = {TMaybe<ui64>(), TMaybe<ui64>(), TMaybe<ui64>(3)},
                .RangeIntervalChecks = {{0, 2}}
            },
            {
                .CurrentElement = TMaybe<ui64>(),
                .QueueContent = {TMaybe<ui64>(), TMaybe<ui64>(), TMaybe<ui64>(3)},
                .RangeIntervalChecks = {{0, 2}}
            },
            {
                .CurrentElement = TMaybe<ui64>(3),
                .QueueContent = {TMaybe<ui64>(3), TMaybe<ui64>(2), TMaybe<ui64>(1)},
                .RangeIntervalChecks = {{3, 3}}
            },
            {
                .CurrentElement = TMaybe<ui64>(2),
                .QueueContent = {TMaybe<ui64>(2), TMaybe<ui64>(1)},
                .RangeIntervalChecks = {{2, 2}}
            },
            {
                .CurrentElement = TMaybe<ui64>(1),
                .QueueContent = {TMaybe<ui64>(1)},
                .RangeIntervalChecks = {{1, 1}}
            }
        }
    };

    RunTestCase(testCase);
}

}
