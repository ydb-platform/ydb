#include "mkql_core_win_frames_collector_test_helper.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr::NMiniKQL;
using namespace NKikimr::NMiniKQL::NTest::NWindow;

// clang-format off
Y_UNIT_TEST_SUITE(TCoreWinFramesCollectorTestPart3) {

Y_UNIT_TEST(RangeInterval_WithNulls_MultipleIntervals_Asc) {
    TTestCase<TMaybe<ui64>, ESortOrder::Asc, ui64> testCase = {
        .RangeIntervals = {
            TInputRangeWindowFrame<TRangeVariant>{
                TInputRange<TRangeVariant>::Inf(EDirection::Preceding),
                TInputRange<TRangeVariant>::Inf(EDirection::Following)
            },
            TInputRangeWindowFrame<TRangeVariant>{
                TInputRange<TRangeVariant>{5, EDirection::Preceding},
                TInputRange<TRangeVariant>::Inf(EDirection::Following)
            },
            TInputRangeWindowFrame<TRangeVariant>{
                TInputRange<TRangeVariant>::Inf(EDirection::Preceding),
                TInputRange<TRangeVariant>{3, EDirection::Following}
            },
            TInputRangeWindowFrame<TRangeVariant>{
                TInputRange<TRangeVariant>{5, EDirection::Preceding},
                TInputRange<TRangeVariant>{3, EDirection::Following}
            }
        },
        .ElementGetter = [](const TMaybe<ui64>& elem) -> TMaybe<ui64> {
            return elem;
        },
        .InputElements = {TMaybe<ui64>(), TMaybe<ui64>(), TMaybe<ui64>(1), TMaybe<ui64>(4), TMaybe<ui64>(7), TMaybe<ui64>()},
        .ExpectedStates = {
            {
                .CurrentElement = TMaybe<ui64>(),
                .QueueContent = {TMaybe<ui64>(), TMaybe<ui64>(), TMaybe<ui64>(1), TMaybe<ui64>(4), TMaybe<ui64>(7), TMaybe<ui64>()},
                .RangeIntervalChecks = {
                    {0, 6},
                    {0, 6},
                    {0, 2},
                    {0, 2}
                }
            },
            {
                .CurrentElement = TMaybe<ui64>(),
                .QueueContent = {TMaybe<ui64>(), TMaybe<ui64>(), TMaybe<ui64>(1), TMaybe<ui64>(4), TMaybe<ui64>(7), TMaybe<ui64>()},
                .RangeIntervalChecks = {
                    {0, 6},
                    {0, 6},
                    {0, 2},
                    {0, 2}
                }
            },
            {
                .CurrentElement = TMaybe<ui64>(1),
                .QueueContent = {TMaybe<ui64>(), TMaybe<ui64>(), TMaybe<ui64>(1), TMaybe<ui64>(4), TMaybe<ui64>(7), TMaybe<ui64>()},
                .RangeIntervalChecks = {
                    {0, 6},
                    {2, 6},
                    {0, 4},
                    {2, 4}
                }
            },
            {
                .CurrentElement = TMaybe<ui64>(4),
                .QueueContent = {TMaybe<ui64>(), TMaybe<ui64>(), TMaybe<ui64>(1), TMaybe<ui64>(4), TMaybe<ui64>(7), TMaybe<ui64>()},
                .RangeIntervalChecks = {
                    {0, 6},
                    {2, 6},
                    {0, 5},
                    {2, 5}
                }
            },
            {
                .CurrentElement = TMaybe<ui64>(7),
                .QueueContent = {TMaybe<ui64>(), TMaybe<ui64>(), TMaybe<ui64>(1), TMaybe<ui64>(4), TMaybe<ui64>(7), TMaybe<ui64>()},
                .RangeIntervalChecks = {
                    {0, 6},
                    {3, 6},
                    {0, 5},
                    {3, 5}
                }
            },
            {
                .CurrentElement = TMaybe<ui64>(),
                .QueueContent = {TMaybe<ui64>(), TMaybe<ui64>(), TMaybe<ui64>(1), TMaybe<ui64>(4), TMaybe<ui64>(7), TMaybe<ui64>()},
                .RangeIntervalChecks = {
                    {0, 6},
                    {5, 6},
                    {0, 6},
                    {5, 6}
                }
            }
        }
    };

    RunTestCase(testCase);
}

Y_UNIT_TEST(RangeInterval_WithNullsLast_Following_Desc) {
    TTestCase<TMaybe<ui64>, ESortOrder::Desc, ui64> testCase = {
        .RangeIntervals = {
            TInputRangeWindowFrame<TRangeVariant>{
                TInputRange<TRangeVariant>{5, EDirection::Following},
                TInputRange<TRangeVariant>{7, EDirection::Following}
            },
        },
        .ElementGetter = [](const TMaybe<ui64>& elem) -> TMaybe<ui64> {
            return elem;
        },
        .InputElements = {TMaybe<ui64>(1), TMaybe<ui64>(), TMaybe<ui64>()},
        .ExpectedStates = {
            {
                .CurrentElement = TMaybe<ui64>(1),
                .QueueContent = {TMaybe<ui64>(1), TMaybe<ui64>()},
                .RangeIntervalChecks = {
                    TEmptyInterval{},
                }
            },
            {
                .CurrentElement = TMaybe<ui64>(),
                .QueueContent = {TMaybe<ui64>(), TMaybe<ui64>()},
                .RangeIntervalChecks = {
                    {0, 2},
                }
            },
            {
                .CurrentElement = TMaybe<ui64>(),
                .QueueContent = {TMaybe<ui64>(), TMaybe<ui64>()},
                .RangeIntervalChecks = {
                    {0, 2},
                }
            }
        }
    };

    RunTestCase(testCase);
}

Y_UNIT_TEST(RangeInterval_WithNullsFirst_Following_Asc) {
    TTestCase<TMaybe<ui64>, ESortOrder::Asc, ui64> testCase = {
        .RangeIntervals = {
            TInputRangeWindowFrame<TRangeVariant>{
                TInputRange<TRangeVariant>{5, EDirection::Following},
                TInputRange<TRangeVariant>{7, EDirection::Following}
            },
        },
        .ElementGetter = [](const TMaybe<ui64>& elem) -> TMaybe<ui64> {
            return elem;
        },
        .InputElements = {TMaybe<ui64>(), TMaybe<ui64>(), TMaybe<ui64>(1)},
        .ExpectedStates = {
            {
                .CurrentElement = TMaybe<ui64>(),
                .QueueContent = {TMaybe<ui64>(), TMaybe<ui64>(), TMaybe<ui64>(1)},
                .RangeIntervalChecks = {
                    {0, 2},
                }
            },
            {
                .CurrentElement = TMaybe<ui64>(),
                .QueueContent = {TMaybe<ui64>(), TMaybe<ui64>(), TMaybe<ui64>(1)},
                .RangeIntervalChecks = {
                    {0, 2},
                }
            },
            {
                .CurrentElement = TMaybe<ui64>(1),
                .QueueContent = {TMaybe<ui64>(1)},
                .RangeIntervalChecks = {
                    TEmptyInterval{},
                }
            }
        }
    };

    RunTestCase(testCase);
}

Y_UNIT_TEST(RangeInterval_WithNullsLast_Preceding_Asc) {
    TTestCase<TMaybe<ui64>, ESortOrder::Desc, ui64> testCase = {
        .RangeIntervals = {
            TInputRangeWindowFrame<TRangeVariant>{
                TInputRange<TRangeVariant>{5, EDirection::Preceding},
                TInputRange<TRangeVariant>{7, EDirection::Preceding}
            },
        },
        .ElementGetter = [](const TMaybe<ui64>& elem) -> TMaybe<ui64> {
            return elem;
        },
        .InputElements = {TMaybe<ui64>(1), TMaybe<ui64>(), TMaybe<ui64>()},
        .ExpectedStates = {
            {
                .CurrentElement = TMaybe<ui64>(1),
                .QueueContent = {TMaybe<ui64>(1)},
                .RangeIntervalChecks = {
                    TEmptyInterval{},
                }
            },
            {
                .CurrentElement = TMaybe<ui64>(),
                .QueueContent = {TMaybe<ui64>(), TMaybe<ui64>()},
                .RangeIntervalChecks = {
                    {0, 2},
                }
            },
            {
                .CurrentElement = TMaybe<ui64>(),
                .QueueContent = {TMaybe<ui64>(), TMaybe<ui64>()},
                .RangeIntervalChecks = {
                    {0, 2},
                }
            }
        }
    };

    RunTestCase(testCase);
}

Y_UNIT_TEST(RangeInterval_WithNullsFirst_Preceding_Asc) {
    TTestCase<TMaybe<ui64>, ESortOrder::Asc, ui64> testCase = {
        .RangeIntervals = {
            TInputRangeWindowFrame<TRangeVariant>{
                TInputRange<TRangeVariant>{5, EDirection::Preceding},
                TInputRange<TRangeVariant>{7, EDirection::Preceding}
            },
        },
        .ElementGetter = [](const TMaybe<ui64>& elem) -> TMaybe<ui64> {
            return elem;
        },
        .InputElements = {TMaybe<ui64>(), TMaybe<ui64>(), TMaybe<ui64>(1)},
        .ExpectedStates = {
            {
                .CurrentElement = TMaybe<ui64>(),
                .QueueContent = {TMaybe<ui64>(), TMaybe<ui64>(), TMaybe<ui64>(1)},
                .RangeIntervalChecks = {
                    {0, 2},
                }
            },
            {
                .CurrentElement = TMaybe<ui64>(),
                .QueueContent = {TMaybe<ui64>(), TMaybe<ui64>(), TMaybe<ui64>(1)},
                .RangeIntervalChecks = {
                    {0, 2},
                }
            },
            {
                .CurrentElement = TMaybe<ui64>(1),
                .QueueContent = {TMaybe<ui64>(1)},
                .RangeIntervalChecks = {
                    TEmptyInterval{},
                }
            }
        }
    };

    RunTestCase(testCase);
}

Y_UNIT_TEST(RangeInterval_WithNaNsLast_Following_Desc) {
    const float NaN = std::numeric_limits<float>::quiet_NaN();

    TTestCase<float, ESortOrder::Desc, float> testCase = {
        .RangeIntervals = {
            TInputRangeWindowFrame<TRangeVariant>{
                TInputRange<TRangeVariant>{5.0f, EDirection::Following},
                TInputRange<TRangeVariant>{7.0f, EDirection::Following}
            },
        },
        .ElementGetter = [](float elem) -> float {
            return elem;
        },
        .InputElements = {1.0f, NaN, NaN},
        .ExpectedStates = {
            {
                .CurrentElement = 1.0f,
                .QueueContent = {1.0f, NaN},
                .RangeIntervalChecks = {
                    TEmptyInterval{},
                }
            },
            {
                .CurrentElement = NaN,
                .QueueContent = {NaN, NaN},
                .RangeIntervalChecks = {
                    {0, 2},
                }
            },
            {
                .CurrentElement = NaN,
                .QueueContent = {NaN, NaN},
                .RangeIntervalChecks = {
                    {0, 2},
                }
            }
        }
    };

    RunTestCase(testCase);
}

Y_UNIT_TEST(RangeInterval_WithNaNsFirst_Following_Asc) {
    const float NaN = std::numeric_limits<float>::quiet_NaN();

    TTestCase<float, ESortOrder::Asc, float> testCase = {
        .RangeIntervals = {
            TInputRangeWindowFrame<TRangeVariant>{
                TInputRange<TRangeVariant>{5.0f, EDirection::Following},
                TInputRange<TRangeVariant>{7.0f, EDirection::Following}
            },
        },
        .ElementGetter = [](float elem) -> float {
            return elem;
        },
        .InputElements = {NaN, NaN, 1.0f},
        .ExpectedStates = {
            {
                .CurrentElement = NaN,
                .QueueContent = {NaN, NaN, 1.0f},
                .RangeIntervalChecks = {
                    {0, 2},
                }
            },
            {
                .CurrentElement = NaN,
                .QueueContent = {NaN, NaN, 1.0f},
                .RangeIntervalChecks = {
                    {0, 2},
                }
            },
            {
                .CurrentElement = 1.0f,
                .QueueContent = {1.0f},
                .RangeIntervalChecks = {
                    TEmptyInterval{},
                }
            }
        }
    };

    RunTestCase(testCase);
}

Y_UNIT_TEST(RangeInterval_WithNaNsLast_Preceding_Asc) {
    const float NaN = std::numeric_limits<float>::quiet_NaN();

    // Сохраняю тот же порядок сортировки (Desc), как и в исходном тесте
    TTestCase<float, ESortOrder::Desc, float> testCase = {
        .RangeIntervals = {
            TInputRangeWindowFrame<TRangeVariant>{
                TInputRange<TRangeVariant>{5.0f, EDirection::Preceding},
                TInputRange<TRangeVariant>{7.0f, EDirection::Preceding}
            },
        },
        .ElementGetter = [](float elem) -> float {
            return elem;
        },
        .InputElements = {1.0f, NaN, NaN},
        .ExpectedStates = {
            {
                .CurrentElement = 1.0f,
                .QueueContent = {1.0f},
                .RangeIntervalChecks = {
                    TEmptyInterval{},
                }
            },
            {
                .CurrentElement = NaN,
                .QueueContent = {NaN, NaN},
                .RangeIntervalChecks = {
                    {0, 2},
                }
            },
            {
                .CurrentElement = NaN,
                .QueueContent = {NaN, NaN},
                .RangeIntervalChecks = {
                    {0, 2},
                }
            }
        }
    };

    RunTestCase(testCase);
}

Y_UNIT_TEST(RangeInterval_WithNaNsFirst_Preceding_Asc) {
    const float NaN = std::numeric_limits<float>::quiet_NaN();

    TTestCase<float, ESortOrder::Asc, float> testCase = {
        .RangeIntervals = {
            TInputRangeWindowFrame<TRangeVariant>{
                TInputRange<TRangeVariant>{5.0f, EDirection::Preceding},
                TInputRange<TRangeVariant>{7.0f, EDirection::Preceding}
            },
        },
        .ElementGetter = [](float elem) -> float {
            return elem;
        },
        .InputElements = {NaN, NaN, 1.0f},
        .ExpectedStates = {
            {
                .CurrentElement = NaN,
                .QueueContent = {NaN, NaN, 1.0f},
                .RangeIntervalChecks = {
                    {0, 2},
                }
            },
            {
                .CurrentElement = NaN,
                .QueueContent = {NaN, NaN, 1.0f},
                .RangeIntervalChecks = {
                    {0, 2},
                }
            },
            {
                .CurrentElement = 1.0f,
                .QueueContent = {1.0f},
                .RangeIntervalChecks = {
                    TEmptyInterval{},
                }
            }
        }
    };

    RunTestCase(testCase);
}

Y_UNIT_TEST(RangeInterval_WithNulls_MultipleIntervals_Desc) {
    TTestCase<TMaybe<ui64>, ESortOrder::Desc, ui64> testCase = {
        .RangeIntervals = {
            TInputRangeWindowFrame<TRangeVariant>{
                TInputRange<TRangeVariant>::Inf(EDirection::Preceding),
                TInputRange<TRangeVariant>::Inf(EDirection::Following)
            },
            TInputRangeWindowFrame<TRangeVariant>{
                TInputRange<TRangeVariant>{5, EDirection::Preceding},
                TInputRange<TRangeVariant>::Inf(EDirection::Following)
            },
            TInputRangeWindowFrame<TRangeVariant>{
                TInputRange<TRangeVariant>::Inf(EDirection::Preceding),
                TInputRange<TRangeVariant>{3, EDirection::Following}
            },
            TInputRangeWindowFrame<TRangeVariant>{
                TInputRange<TRangeVariant>{5, EDirection::Preceding},
                TInputRange<TRangeVariant>{3, EDirection::Following}
            }
        },
        .ElementGetter = [](const TMaybe<ui64>& elem) -> TMaybe<ui64> {
            return elem;
        },
        .InputElements = {TMaybe<ui64>(), TMaybe<ui64>(), TMaybe<ui64>(7), TMaybe<ui64>(4), TMaybe<ui64>(1), TMaybe<ui64>()},
        .ExpectedStates = {
            {
                .CurrentElement = TMaybe<ui64>(),
                .QueueContent = {TMaybe<ui64>(), TMaybe<ui64>(), TMaybe<ui64>(7), TMaybe<ui64>(4), TMaybe<ui64>(1), TMaybe<ui64>()},
                .RangeIntervalChecks = {
                    {0, 6},
                    {0, 6},
                    {0, 2},
                    {0, 2}
                }
            },
            {
                .CurrentElement = TMaybe<ui64>(),
                .QueueContent = {TMaybe<ui64>(), TMaybe<ui64>(), TMaybe<ui64>(7), TMaybe<ui64>(4), TMaybe<ui64>(1), TMaybe<ui64>()},
                .RangeIntervalChecks = {
                    {0, 6},
                    {0, 6},
                    {0, 2},
                    {0, 2}
                }
            },
            {
                .CurrentElement = TMaybe<ui64>(7),
                .QueueContent = {TMaybe<ui64>(), TMaybe<ui64>(), TMaybe<ui64>(7), TMaybe<ui64>(4), TMaybe<ui64>(1), TMaybe<ui64>()},
                .RangeIntervalChecks = {
                    {0, 6},
                    {2, 6},
                    {0, 4},
                    {2, 4}
                }
            },
            {
                .CurrentElement = TMaybe<ui64>(4),
                .QueueContent = {TMaybe<ui64>(), TMaybe<ui64>(), TMaybe<ui64>(7), TMaybe<ui64>(4), TMaybe<ui64>(1), TMaybe<ui64>()},
                .RangeIntervalChecks = {
                    {0, 6},
                    {2, 6},
                    {0, 5},
                    {2, 5}
                }
            },
            {
                .CurrentElement = TMaybe<ui64>(1),
                .QueueContent = {TMaybe<ui64>(), TMaybe<ui64>(), TMaybe<ui64>(7), TMaybe<ui64>(4), TMaybe<ui64>(1), TMaybe<ui64>()},
                .RangeIntervalChecks = {
                    {0, 6},
                    {3, 6},
                    {0, 5},
                    {3, 5}
                }
            },
            {
                .CurrentElement = TMaybe<ui64>(),
                .QueueContent = {TMaybe<ui64>(), TMaybe<ui64>(), TMaybe<ui64>(7), TMaybe<ui64>(4), TMaybe<ui64>(1), TMaybe<ui64>()},
                .RangeIntervalChecks = {
                    {0, 6},
                    {5, 6},
                    {0, 6},
                    {5, 6}
                }
            }
        }
    };

    RunTestCase(testCase);
}

Y_UNIT_TEST(RowIntervalUnbounded) {
    TTestCase<ui64, ESortOrder::Asc> testCase = {
        .RowIntervals = {
            TInputRowWindowFrame(
                TInputRow::Inf(EDirection::Preceding),
                TInputRow::Inf(EDirection::Following)
            ),
            TInputRowWindowFrame(
                TInputRow(1, EDirection::Preceding),
                TInputRow(1, EDirection::Preceding)
            )
        },
        .InputElements = {ui64(10), ui64(20), ui64(30)},
        .ExpectedStates = {
            {
                .CurrentElement = 10,
                .QueueContent = {10, 20, 30},
                .RowIntervalChecks = {{0, 3}, {0, 0}},
            },
            {
                .CurrentElement = 20,
                .QueueContent = {10, 20, 30},
                .RowIntervalChecks = {{0, 3}, {0, 1}},
            },{
                .CurrentElement = 30,
                .QueueContent = {10, 20, 30},
                .RowIntervalChecks = {{0, 3}, {1, 2}},
            },
        }
    };

    RunTestCase(testCase);
}

}
