#include "mkql_core_win_frames_collector_test_helper.h"

using namespace NKikimr::NMiniKQL;
using namespace NKikimr::NMiniKQL::NTest::NWindow;

// clang-format off
Y_UNIT_TEST_SUITE(TCoreWinFramesCollectorTest) {

Y_UNIT_TEST(BasicRowInterval) {
    TTestCase<ui64, ESortOrder::Asc> testCase = {
        .RowIntervals = {
            TInputRowWindowFrame(
                TInputRow(2, EDirection::Preceding),
                TInputRow(2, EDirection::Following)
            )
        },
        .InputElements = {ui64(10), ui64(20), ui64(30), ui64(40), ui64(50)},
        .ExpectedStates = {
            {
                .CurrentElement = 10,
                .QueueContent = {10, 20, 30},
                .RowIntervalChecks = {{0, 3}},
            },
            {
                .CurrentElement = 20,
                .QueueContent = {10, 20, 30, 40},
                .RowIntervalChecks = {{0, 4}},
            },
            {
                .CurrentElement = 30,
                .QueueContent = {10, 20, 30, 40, 50},
                .RowIntervalChecks = {{0, 5}},
            },
            {
                .CurrentElement = 40,
                .QueueContent = {20, 30, 40, 50},
                .RowIntervalChecks = {{0, 4}},
            },
            {
                .CurrentElement = 50,
                .QueueContent = {30, 40, 50},
                .RowIntervalChecks = {{0, 3}},
            }
        }
    };

    RunTestCase(testCase);
}

Y_UNIT_TEST(BasicRowIntervalUnimportant) {
    TTestCase<ui64, ESortOrder::Unimportant> testCase = {
        .RowIntervals = {
            TInputRowWindowFrame(
                TInputRow(2, EDirection::Preceding),
                TInputRow(2, EDirection::Following)
            )
        },
        .InputElements = {ui64(10), ui64(20), ui64(30), ui64(40), ui64(50)},
        .ExpectedStates = {
            {
                .CurrentElement = 10,
                .QueueContent = {10, 20, 30},
                .RowIntervalChecks = {{0, 3}},
            },
            {
                .CurrentElement = 20,
                .QueueContent = {10, 20, 30, 40},
                .RowIntervalChecks = {{0, 4}},
            },
            {
                .CurrentElement = 30,
                .QueueContent = {10, 20, 30, 40, 50},
                .RowIntervalChecks = {{0, 5}},
            },
            {
                .CurrentElement = 40,
                .QueueContent = {20, 30, 40, 50},
                .RowIntervalChecks = {{0, 4}},
            },
            {
                .CurrentElement = 50,
                .QueueContent = {30, 40, 50},
                .RowIntervalChecks = {{0, 3}},
            }
        }
    };

    RunTestCase(testCase);
}

Y_UNIT_TEST(BothLeftRowIntervals) {
    TTestCase<ui64, ESortOrder::Asc> testCase = {
        .RowIntervals = {
            TInputRowWindowFrame(
                TInputRow(3, EDirection::Preceding),
                TInputRow(2, EDirection::Preceding)
            )
        },
        .InputElements = {ui64(10), ui64(20), ui64(30), ui64(40), ui64(50)},
        .ExpectedStates = {
            {
                .CurrentElement = 10,
                .QueueContent = {10},
                .RowIntervalChecks = {{0, 0}},
            },
            {
                .CurrentElement = 20,
                .QueueContent = {10, 20},
                .RowIntervalChecks = {{0, 0}},
            },
            {
                .CurrentElement = 30,
                .QueueContent = {10, 20, 30},
                .RowIntervalChecks = {{0, 1}},
            },
            {
                .CurrentElement = 40,
                .QueueContent = {10, 20, 30, 40},
                .RowIntervalChecks = {{0, 2}},
            },
            {
                .CurrentElement = 50,
                .QueueContent = {20, 30, 40, 50},
                .RowIntervalChecks = {{0, 2}},
            }
        }
    };

    RunTestCase(testCase);
}

Y_UNIT_TEST(BothRightRowIntervals) {
    TTestCase<ui64, ESortOrder::Asc> testCase = {
        .RowIntervals = {
            TInputRowWindowFrame(
                TInputRow(1, EDirection::Following),
                TInputRow(3, EDirection::Following)
            )
        },
        .InputElements = {ui64(10), ui64(20), ui64(30), ui64(40), ui64(50)},
        .ExpectedStates = {
            {
                .CurrentElement = 10,
                .QueueContent = {10, 20, 30, 40},
                .RowIntervalChecks = {{1, 4}},
            },
            {
                .CurrentElement = 20,
                .QueueContent = {20, 30, 40, 50},
                .RowIntervalChecks = {{1, 4}},
            },
            {
                .CurrentElement = 30,
                .QueueContent = {30, 40, 50},
                .RowIntervalChecks = {{1, 3}},
            },
            {
                .CurrentElement = 40,
                .QueueContent = {40, 50},
                .RowIntervalChecks = {{1, 2}},
            },
            {
                .CurrentElement = 50,
                .QueueContent = {50},
                .RowIntervalChecks = {{1, 1}},
            }
        }
    };

    RunTestCase(testCase);
}

Y_UNIT_TEST(MaxValueLeftInterval) {
    TTestCase<ui64, ESortOrder::Asc> testCase = {
        .RowIntervals = {
            TInputRowWindowFrame(
                TInputRow(std::numeric_limits<ui64>::max(), EDirection::Preceding),
                TInputRow(0, EDirection::Preceding)
            )
        },
        .InputElements = {TYield(), ui64(10), ui64(20), ui64(30), ui64(40), ui64(50)},
        .ExpectedStates = {
            {
                .CurrentElement = 10,
                .QueueContent = {10},
                .RowIntervalChecks = {{0, 1}},
            },
            {
                .CurrentElement = 20,
                .QueueContent = {10, 20},
                .RowIntervalChecks = {{0, 2}},
            },
            {
                .CurrentElement = 30,
                .QueueContent = {10, 20, 30},
                .RowIntervalChecks = {{0, 3}},
            },
            {
                .CurrentElement = 40,
                .QueueContent = {10, 20, 30, 40},
                .RowIntervalChecks = {{0, 4}},
            },
            {
                .CurrentElement = 50,
                .QueueContent = {10, 20, 30, 40, 50},
                .RowIntervalChecks = {{0, 5}},
            }
        }
    };

    RunTestCase(testCase);
}

Y_UNIT_TEST(MaxValueRightInterval) {
    TTestCase<ui64, ESortOrder::Asc> testCase = {
        .RowIntervals = {
            TInputRowWindowFrame(
                TInputRow(0, EDirection::Following),
                TInputRow(std::numeric_limits<ui32>::max(), EDirection::Following)
            )
        },
        .InputElements = {ui64(10), ui64(20), ui64(30), ui64(40), ui64(50)},
        .ExpectedStates = {
            {
                .CurrentElement = 10,
                .QueueContent = {10, 20, 30, 40, 50},
                .RowIntervalChecks = {{0, 5}},
            },
            {
                .CurrentElement = 20,
                .QueueContent = {20, 30, 40, 50},
                .RowIntervalChecks = {{0, 4}},
            },
            {
                .CurrentElement = 30,
                .QueueContent = {30, 40, 50},
                .RowIntervalChecks = {{0, 3}},
            },
            {
                .CurrentElement = 40,
                .QueueContent = {40, 50},
                .RowIntervalChecks = {{0, 2}},
            },
            {
                .CurrentElement = 50,
                .QueueContent = {50},
                .RowIntervalChecks = {{0, 1}},
            }
        }
    };

    RunTestCase(testCase);
}

Y_UNIT_TEST(BasicRangeInterval) {
    TTestCase<ui64, ESortOrder::Asc> testCase = {
        .RangeIntervals = {
            TInputRangeWindowFrame<ui64>{
                TInputRange<ui64>{5, EDirection::Preceding},
                TInputRange<ui64>{5, EDirection::Following}
            }
        },
        .InputElements = {ui64(10), ui64(15), TYield(), TYield(), ui64(20), ui64(25), ui64(30)},
        .ExpectedStates = {
            {
                .CurrentElement = 10,
                .QueueContent = {10, 15, 20},
                .RangeIntervalChecks = {{0, 2}}
            },
            {
                .CurrentElement = 15,
                .QueueContent = {10, 15, 20, 25},
                .RangeIntervalChecks = {{0, 3}}
            },
            {
                .CurrentElement = 20,
                .QueueContent = {15, 20, 25, 30},
                .RangeIntervalChecks = {{0, 3}}
            },
            {
                .CurrentElement = 25,
                .QueueContent = {20, 25, 30},
                .RangeIntervalChecks = {{0, 3}}
            },
            {
                .CurrentElement = 30,
                .QueueContent = {25, 30},
                .RangeIntervalChecks = {{0, 2}}
            }
        }
    };

    RunTestCase(testCase);
}

Y_UNIT_TEST(RangeIntervalPowersOfTwo) {
    TTestCase<ui64, ESortOrder::Asc> testCase = {
        .RangeIntervals = {
            TInputRangeWindowFrame<ui64>{
                TInputRange<ui64>{4, EDirection::Preceding},
                TInputRange<ui64>{4, EDirection::Following}
            }
        },
        .InputElements = {TYield(), TYield(), ui64(1), TYield(), ui64(2), ui64(4), ui64(8), ui64(16), ui64(32)},
        .ExpectedStates = {
            {
                .CurrentElement = 1,
                .QueueContent = {1, 2, 4, 8},
                .RangeIntervalChecks = {{0, 3}}
            },
            {
                .CurrentElement = 2,
                .QueueContent = {1, 2, 4, 8},
                .RangeIntervalChecks = {{0, 3}}
            },
            {
                .CurrentElement = 4,
                .QueueContent = {1, 2, 4, 8, 16},
                .RangeIntervalChecks = {{0, 4}}
            },
            {
                .CurrentElement = 8,
                .QueueContent = {4, 8, 16},
                .RangeIntervalChecks = {{0, 2}}
            },
            {
                .CurrentElement = 16,
                .QueueContent = {16, 32},
                .RangeIntervalChecks = {{0, 1}}
            },
            {
                .CurrentElement = 32,
                .QueueContent = {32},
                .RangeIntervalChecks = {{0, 1}}
            }
        }
    };

    RunTestCase(testCase);
}

Y_UNIT_TEST(SingleElement) {
    TTestCase<ui64, ESortOrder::Asc> testCase = {
        .RowIntervals = {
            TInputRowWindowFrame(
                TInputRow(2, EDirection::Preceding),
                TInputRow(2, EDirection::Following)
            )
        },
        .InputElements = {ui64(42), TYield(), TYield()},
        .ExpectedStates = {
            {
                .CurrentElement = 42,
                .QueueContent = {42},
                .RowIntervalChecks = {{0, 1}},
            }
        }
    };

    RunTestCase(testCase);
}

Y_UNIT_TEST(EmptyStream) {
    TTestCase<ui64, ESortOrder::Asc> testCase = {
        .RowIntervals = {
            TInputRowWindowFrame(
                TInputRow(2, EDirection::Preceding),
                TInputRow(2, EDirection::Following)
            )
        },
    };

    RunTestCase(testCase);
}

Y_UNIT_TEST(YieldOnlyStream) {
    TTestCase<ui64, ESortOrder::Asc> testCase = {
        .RowIntervals = {
            TInputRowWindowFrame(
                TInputRow(2, EDirection::Preceding),
                TInputRow(2, EDirection::Following)
            )
        },
        .InputElements = {TYield(), TYield()},
    };

    RunTestCase(testCase);
}

Y_UNIT_TEST(TwoRangeIntervals_OneInsideAnother) {
    TTestCase<ui64, ESortOrder::Asc> testCase = {
        .RangeIntervals = {
            TInputRangeWindowFrame<ui64>{
                TInputRange<ui64>{10, EDirection::Preceding},
                TInputRange<ui64>{10, EDirection::Following}
            },
            TInputRangeWindowFrame<ui64>{
                TInputRange<ui64>{5, EDirection::Preceding},
                TInputRange<ui64>{5, EDirection::Following}
            }
        },
        .InputElements = {ui64(10), ui64(20), ui64(30), ui64(40), ui64(50)},
        .ExpectedStates = {
            {
                .CurrentElement = 10,
                .QueueContent = {10, 20, 30},
                .RangeIntervalChecks = {
                    {0, 2},
                    {0, 1}
                }
            },
            {
                .CurrentElement = 20,
                .QueueContent = {10, 20, 30, 40},
                .RangeIntervalChecks = {
                    {0, 3},
                    {1, 2}
                }
            },
            {
                .CurrentElement = 30,
                .QueueContent = {20, 30, 40, 50},
                .RangeIntervalChecks = {
                    {0, 3},
                    {1, 2}
                }
            },
            {
                .CurrentElement = 40,
                .QueueContent = {30, 40, 50},
                .RangeIntervalChecks = {
                    {0, 3},
                    {1, 2}
                }
            },
            {
                .CurrentElement = 50,
                .QueueContent = {40, 50},
                .RangeIntervalChecks = {
                    {0, 2},
                    {1, 2}
                }
            }
        }
    };

    RunTestCase(testCase);
}

Y_UNIT_TEST(TwoRangeIntervals_PartiallyOverlapLeft) {
    TTestCase<ui64, ESortOrder::Asc> testCase = {
        .RangeIntervals = {
            TInputRangeWindowFrame<ui64>{
                TInputRange<ui64>{15, EDirection::Preceding},
                TInputRange<ui64>{5, EDirection::Following}
            },
            TInputRangeWindowFrame<ui64>{
                TInputRange<ui64>{5, EDirection::Preceding},
                TInputRange<ui64>{15, EDirection::Following}
            }
        },
        .InputElements = {ui64(10), ui64(20), ui64(30), ui64(40), ui64(50)},
        .ExpectedStates = {
            {
                .CurrentElement = 10,
                .QueueContent = {10, 20, 30},
                .RangeIntervalChecks = {
                    {0, 1},
                    {0, 2}
                }
            },
            {
                .CurrentElement = 20,
                .QueueContent = {10, 20, 30, 40},
                .RangeIntervalChecks = {
                    {0, 2},
                    {1, 3}
                }
            },
            {
                .CurrentElement = 30,
                .QueueContent = {20, 30, 40, 50},
                .RangeIntervalChecks = {
                    {0, 2},
                    {1, 3}
                }
            },
            {
                .CurrentElement = 40,
                .QueueContent = {30, 40, 50},
                .RangeIntervalChecks = {
                    {0, 2},
                    {1, 3}
                }
            },
            {
                .CurrentElement = 50,
                .QueueContent = {40, 50},
                .RangeIntervalChecks = {
                    {0, 2},
                    {1, 2}
                }
            }
        }
    };

    RunTestCase(testCase);
}

Y_UNIT_TEST(TwoRangeIntervals_CompletelyDisjointLeft) {
    TTestCase<ui64, ESortOrder::Asc> testCase = {
        .RangeIntervals = {
            TInputRangeWindowFrame<ui64>{
                TInputRange<ui64>{30, EDirection::Preceding},
                TInputRange<ui64>{20, EDirection::Preceding}
            },
            TInputRangeWindowFrame<ui64>{
                TInputRange<ui64>{10, EDirection::Preceding},
                TInputRange<ui64>{10, EDirection::Following}
            }
        },
        .InputElements = {ui64(10), ui64(20), ui64(30), ui64(40), ui64(50), ui64(60)},
        .ExpectedStates = {
            {
                .CurrentElement = 10,
                .QueueContent = {10, 20, 30},
                .RangeIntervalChecks = {
                    {0, 0},
                    {0, 2}
                }
            },
            {
                .CurrentElement = 20,
                .QueueContent = {10, 20, 30, 40},
                .RangeIntervalChecks = {
                    {0, 0},
                    {0, 3}
                }
            },
            {
                .CurrentElement = 30,
                .QueueContent = {10, 20, 30, 40, 50},
                .RangeIntervalChecks = {
                    {0, 1},
                    {1, 4}
                }
            },
            {
                .CurrentElement = 40,
                .QueueContent = {10, 20, 30, 40, 50, 60},
                .RangeIntervalChecks = {
                    {0, 2},
                    {2, 5}
                }
            },
            {
                .CurrentElement = 50,
                .QueueContent = {20, 30, 40, 50, 60},
                .RangeIntervalChecks = {
                    {0, 2},
                    {2, 5}
                }
            },
            {
                .CurrentElement = 60,
                .QueueContent = {30, 40, 50, 60},
                .RangeIntervalChecks = {
                    {0, 2},
                    {2, 4}
                }
            }
        }
    };

    RunTestCase(testCase);
}

Y_UNIT_TEST(TwoRangeIntervals_CompletelyDisjointRight) {
    TTestCase<ui64, ESortOrder::Asc> testCase = {
        .RangeIntervals = {
            TInputRangeWindowFrame<ui64>{
                TInputRange<ui64>{10, EDirection::Preceding},
                TInputRange<ui64>{10, EDirection::Following}
            },
            TInputRangeWindowFrame<ui64>{
                TInputRange<ui64>{20, EDirection::Following},
                TInputRange<ui64>{30, EDirection::Following}
            }
        },
        .InputElements = {ui64(10), ui64(20), ui64(30), ui64(40), ui64(50), ui64(60)},
        .ExpectedStates = {
            {
                .CurrentElement = 10,
                .QueueContent = {10, 20, 30, 40, 50},
                .RangeIntervalChecks = {
                    {0, 2},
                    {2, 4}
                }
            },
            {
                .CurrentElement = 20,
                .QueueContent = {10, 20, 30, 40, 50, 60},
                .RangeIntervalChecks = {
                    {0, 3},
                    {3, 5}
                }
            },
            {
                .CurrentElement = 30,
                .QueueContent = {20, 30, 40, 50, 60},
                .RangeIntervalChecks = {
                    {0, 3},
                    {3, 5}
                }
            },
            {
                .CurrentElement = 40,
                .QueueContent = {30, 40, 50, 60},
                .RangeIntervalChecks = {
                    {0, 3},
                    {3, 4}
                }
            },
            {
                .CurrentElement = 50,
                .QueueContent = {40, 50, 60},
                .RangeIntervalChecks = {
                    {0, 3},
                    {3, 3}
                }
            },
            {
                .CurrentElement = 60,
                .QueueContent = {50, 60},
                .RangeIntervalChecks = {
                    {0, 2},
                    {2, 2}
                }
            }
        }
    };

    RunTestCase(testCase);
}

Y_UNIT_TEST(TwoRangeIntervals_PartiallyOverlapRight) {
    TTestCase<ui64, ESortOrder::Asc> testCase = {
        .RangeIntervals = {
            TInputRangeWindowFrame<ui64>{
                TInputRange<ui64>{5, EDirection::Preceding},
                TInputRange<ui64>{15, EDirection::Following}
            },
            TInputRangeWindowFrame<ui64>{
                TInputRange<ui64>{5, EDirection::Following},
                TInputRange<ui64>{25, EDirection::Following}
            }
        },
        .InputElements = {ui64(10), ui64(20), ui64(30), ui64(40), ui64(50), ui64(60)},
        .ExpectedStates = {
            {
                .CurrentElement = 10,
                .QueueContent = {10, 20, 30, 40},
                .RangeIntervalChecks = {
                    {0, 2},
                    {1, 3}
                }
            },
            {
                .CurrentElement = 20,
                .QueueContent = {20, 30, 40, 50},
                .RangeIntervalChecks = {
                    {0, 2},
                    {1, 3}
                }
            },
            {
                .CurrentElement = 30,
                .QueueContent = {30, 40, 50, 60},
                .RangeIntervalChecks = {
                    {0, 2},
                    {1, 3}
                }
            },
            {
                .CurrentElement = 40,
                .QueueContent = {40, 50, 60},
                .RangeIntervalChecks = {
                    {0, 2},
                    {1, 3}
                }
            },
            {
                .CurrentElement = 50,
                .QueueContent = {50, 60},
                .RangeIntervalChecks = {
                    {0, 2},
                    {1, 2}
                }
            },
            {
                .CurrentElement = 60,
                .QueueContent = {60},
                .RangeIntervalChecks = {
                    {0, 1},
                    {1, 1}
                }
            }
        }
    };

    RunTestCase(testCase);
}

}
