#define HARMONIZER_HISTORY_DEBUG
#include "debug.h"
#include "history.h"
#include <library/cpp/testing/unittest/registar.h>

using namespace NActors;

constexpr double secondInUs = 1'000'000.0;
ui64 secondInTs = Us2Ts(secondInUs);

Y_UNIT_TEST_SUITE(ValueHistoryTests) {
    
    Y_UNIT_TEST(TestConstructorAndInitialization) {
        TValueHistory<8> history;
        
        UNIT_ASSERT_VALUES_EQUAL(history.HistoryIdx, 0);
        UNIT_ASSERT_VALUES_EQUAL(history.LastTs, Max<ui64>());
        UNIT_ASSERT_VALUES_EQUAL(history.LastValue, 0.0);
        UNIT_ASSERT_VALUES_EQUAL(history.AccumulatedValue, 0.0);
        UNIT_ASSERT_VALUES_EQUAL(history.AccumulatedTs, 0);
        
        for (size_t i = 0; i < 8; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(history.History[i], 0.0);
        }
    }

    Y_UNIT_TEST(TestBufferSizePowerOfTwo) {
        UNIT_ASSERT(TValueHistory<1>::CheckBinaryPower(1));
        UNIT_ASSERT(TValueHistory<2>::CheckBinaryPower(2));
        UNIT_ASSERT(TValueHistory<4>::CheckBinaryPower(4));
        UNIT_ASSERT(TValueHistory<8>::CheckBinaryPower(8));
        UNIT_ASSERT(TValueHistory<16>::CheckBinaryPower(16));
        
        UNIT_ASSERT(!TValueHistory<1>::CheckBinaryPower(3));
        UNIT_ASSERT(!TValueHistory<1>::CheckBinaryPower(5));
        UNIT_ASSERT(!TValueHistory<1>::CheckBinaryPower(6));
        UNIT_ASSERT(!TValueHistory<1>::CheckBinaryPower(7));
    }

    Y_UNIT_TEST(TestCircularBufferFilling) {
        TValueHistory<4> history;
        ui64 baseTs = secondInUs;
        
        // increasing counter by 1 each second
        for (size_t i = 0; i < 8; ++i) {
            history.Register(baseTs + i * secondInTs, i * 1.0);
        }

        // expecting 1.0 for each second
        double expectedValues[] = {1.0, 1.0, 1.0, 1.0};
        for (size_t i = 0; i < 4; ++i) {
            UNIT_ASSERT_DOUBLES_EQUAL(history.History[i], expectedValues[i], 1e-6);
        }
    }

    Y_UNIT_TEST(TestBasicRegistration) {
        TValueHistory<4> history;
        ui64 baseTs = secondInUs;

        history.Register(baseTs, 1.0);
        UNIT_ASSERT_VALUES_EQUAL(history.LastTs, baseTs);
        UNIT_ASSERT_DOUBLES_EQUAL(history.LastValue, 1.0, 1e-6);

        history.Register(baseTs + secondInTs, 2.0);
        UNIT_ASSERT_DOUBLES_EQUAL(history.History[0], 1.0, 1e-6);
        UNIT_ASSERT_VALUES_EQUAL(history.HistoryIdx, 1);
    }

    Y_UNIT_TEST(TestRegistrationWithBackwardTime) {
        TValueHistory<4> history;
        ui64 baseTs = secondInUs;

        history.Register(baseTs, 1.0);
        
        history.Register(baseTs - 1000, 2.0);
        
        UNIT_ASSERT_VALUES_EQUAL(history.LastTs, baseTs - 1000);
        UNIT_ASSERT_DOUBLES_EQUAL(history.LastValue, 2.0, 1e-6);
        UNIT_ASSERT_DOUBLES_EQUAL(history.AccumulatedValue, 0.0, 1e-6);
        UNIT_ASSERT_VALUES_EQUAL(history.AccumulatedTs, 0);
    }

    Y_UNIT_TEST(TestRegistrationWithLargeTimeGap) {
        TValueHistory<4> history;
        ui64 baseTs = secondInUs;

        history.Register(baseTs, 1.0);
        
        history.Register(baseTs + 10 * secondInTs, 4.0);

        double expectedValue = (4.0 - 1.0) / 10;
        for (size_t i = 0; i < 4; ++i) {
            UNIT_ASSERT_DOUBLES_EQUAL(history.History[i], expectedValue, 1e-6);
        }
    }

    Y_UNIT_TEST(TestRegistrationOnSecondBoundary) {
        TValueHistory<4> history;
        ui64 baseTs = Us2Ts(1'000'000.0);

        history.Register(baseTs - Us2Ts(100'000.0), 1.0);
        history.Register(baseTs, 2.0);
        history.Register(baseTs + Us2Ts(100'000.0), 3.0);

        UNIT_ASSERT_VALUES_EQUAL(history.HistoryIdx, 0);
        UNIT_ASSERT_VALUES_EQUAL(history.LastTs, baseTs + Us2Ts(100'000.0));
        UNIT_ASSERT_VALUES_EQUAL(history.AccumulatedValue, 2.0);
        UNIT_ASSERT_DOUBLES_EQUAL(history.History[0], 0.0, 1e-6);
    }

    Y_UNIT_TEST(TestAccumulationWithinSecond) {
        TValueHistory<4> history;
        ui64 baseTs = 1000000;

        history.Register(baseTs, 1.0);
        history.Register(baseTs + Us2Ts(500'000.0), 2.0);
        
        UNIT_ASSERT_VALUES_EQUAL(history.AccumulatedTs, Us2Ts(500'000.0));
        UNIT_ASSERT_DOUBLES_EQUAL(history.AccumulatedValue, 1.0, 1e-6);
    }

    Y_UNIT_TEST(TestTransitionBetweenSeconds) {
        TValueHistory<4> history;
        ui64 baseTs = 1000000;

        history.Register(baseTs, 1.0);
        history.Register(baseTs + Us2Ts(1'500'000.0), 2.0);

        UNIT_ASSERT_VALUES_EQUAL(history.HistoryIdx, 1);
        UNIT_ASSERT_DOUBLES_EQUAL(history.History[0], 2.0/3, 1e-6);
        UNIT_ASSERT_DOUBLES_EQUAL(history.AccumulatedValue, 1.0/3, 1e-6);
        UNIT_ASSERT_DOUBLES_EQUAL(Ts2Us(history.AccumulatedTs), 500'000.0, 1e-3);
    }

    Y_UNIT_TEST(TestGetAvgPartForLastSeconds) {
        TValueHistory<4> history;
        ui64 baseTs = 1000000;

        float acc = 0;
        for (size_t i = 0; i < 4; ++i) {
            acc += i * 1.0;
            history.Register(baseTs + i * Us2Ts(1'000'000.0), acc);
        }

        UNIT_ASSERT_DOUBLES_EQUAL(history.GetAvgPartForLastSeconds(1), 3.0, 1e-6);
        UNIT_ASSERT_DOUBLES_EQUAL(history.GetAvgPartForLastSeconds(2), 2.5, 1e-6);
        UNIT_ASSERT_DOUBLES_EQUAL(history.GetAvgPartForLastSeconds(3), 2.0, 1e-6);
    }

    Y_UNIT_TEST(TestGetAvgPartWithTail) {
        TValueHistory<4> history;
        ui64 baseTs = 1000000;

        history.Register(baseTs, 1.0);
        history.Register(baseTs + Us2Ts(500'000.0), 2.0);

        UNIT_ASSERT_DOUBLES_EQUAL(history.GetAvgPartForLastSeconds<true>(1), 1.0, 1e-6);
        UNIT_ASSERT_DOUBLES_EQUAL(history.GetAvgPartForLastSeconds<false>(1), 0.0, 1e-6);
    }

    Y_UNIT_TEST(TestGetAvgPartWithIncompleteData) {
        TValueHistory<4> history;
        ui64 baseTs = 1000000;

        history.Register(baseTs, 1.0);
        history.Register(baseTs + Us2Ts(1'000'000.0), 2.0);

        UNIT_ASSERT_DOUBLES_EQUAL(history.GetAvgPartForLastSeconds<true>(3), 1.0/3, 1e-6);
        UNIT_ASSERT_DOUBLES_EQUAL(history.GetAvgPartForLastSeconds<false>(3), 1.0/3, 1e-6);
    }

    Y_UNIT_TEST(TestGetAvgPartOnBufferBoundary) {
        TValueHistory<4> history;
        ui64 baseTs = 1000000;

        for (size_t i = 0; i < 5; ++i) {
            history.Register(baseTs + i * Us2Ts(1'000'000.0), i * 1.0);
        }

        UNIT_ASSERT_DOUBLES_EQUAL(history.GetAvgPartForLastSeconds(4), 1.0, 1e-6);
    }

    Y_UNIT_TEST(TestGetMaxForLastSeconds) {
        TValueHistory<4> history;
        ui64 baseTs = 1000000;

        double acc = 0;
        for (size_t i = 0; i < 4; ++i) {
            acc += i * 1.0;
            history.Register(baseTs + i * Us2Ts(1'000'000.0), acc);
        }

        UNIT_ASSERT_DOUBLES_EQUAL(history.GetMaxForLastSeconds(1), 3.0, 1e-6);
        UNIT_ASSERT_DOUBLES_EQUAL(history.GetMaxForLastSeconds(2), 3.0, 1e-6);
        UNIT_ASSERT_DOUBLES_EQUAL(history.GetMaxForLastSeconds(3), 3.0, 1e-6);
    }

    Y_UNIT_TEST(TestGetMax) {
        TValueHistory<4> history;
        ui64 baseTs = 1000000;

        double acc = 0;
        for (size_t i = 0; i < 4; ++i) {
            acc += i * 1.0;
            history.Register(baseTs + i * Us2Ts(1'000'000.0), acc);
        }

        UNIT_ASSERT_DOUBLES_EQUAL(history.GetMax(), 3.0, 1e-6);
    }

    Y_UNIT_TEST(TestGetMaxWithNegativeValues) {
        TValueHistory<4> history;
        ui64 baseTs = 1000000;

        history.Register(baseTs, -1.0);
        history.Register(baseTs + Us2Ts(1'000'000.0), -2.0);
        history.Register(baseTs + Us2Ts(2'000'000.0), -3.0);
        history.Register(baseTs + Us2Ts(3'000'000.0), -4.0);
        history.Register(baseTs + Us2Ts(4'000'000.0), -5.0);

        UNIT_ASSERT_DOUBLES_EQUAL(history.GetMax(), -1.0, 1e-6);
    }

    Y_UNIT_TEST(TestGetMinForLastSeconds) {
        TValueHistory<4> history;
        ui64 baseTs = 1000000;

        double acc = 0;
        for (size_t i = 0; i < 4; ++i) {
            acc += (4 - i) * 1.0;
            history.Register(baseTs + i * Us2Ts(1'000'000.0), acc);
        }

        UNIT_ASSERT_DOUBLES_EQUAL(history.GetMinForLastSeconds(1), 1.0, 1e-6);
        UNIT_ASSERT_DOUBLES_EQUAL(history.GetMinForLastSeconds(2), 1.0, 1e-6);
        UNIT_ASSERT_DOUBLES_EQUAL(history.GetMinForLastSeconds(3), 1.0, 1e-6);
    }

    Y_UNIT_TEST(TestGetMin) {
        TValueHistory<4> history;
        ui64 baseTs = 1000000;

        double acc = 0;
        for (size_t i = 0; i < 5; ++i) {
            acc += (5 - i) * 1.0;
            history.Register(baseTs + i * Us2Ts(1'000'000.0), acc);
        }

        UNIT_ASSERT_DOUBLES_EQUAL(history.GetMin(), 1.0, 1e-6);
    }

    Y_UNIT_TEST(TestGetMinWithNegativeValues) {
        TValueHistory<4> history;
        ui64 baseTs = 1000000;

        history.Register(baseTs, -1.0);
        history.Register(baseTs + Us2Ts(1'000'000.0), -2.0);
        history.Register(baseTs + Us2Ts(2'000'000.0), -4.0);
        history.Register(baseTs + Us2Ts(3'000'000.0), -7.0);

        UNIT_ASSERT_DOUBLES_EQUAL(history.GetMin(), -3.0, 1e-6);
    }

    Y_UNIT_TEST(TestBufferOverflow) {
        TValueHistory<4> history;
        ui64 baseTs = 1000000;

        // Fill buffer
        double acc = 0;
        for (size_t i = 0; i < 5; ++i) {
            acc += i * 1.0;
            history.Register(baseTs + i * Us2Ts(1'000'000.0), acc);
        }

        // Add another value, which should overwrite the oldest
        acc += 5 * 1.0;
        history.Register(baseTs + 5 * Us2Ts(1'000'000.0), acc);

        UNIT_ASSERT_DOUBLES_EQUAL(history.GetMax(), 5.0, 1e-6);
        UNIT_ASSERT_DOUBLES_EQUAL(history.GetMin(), 2.0, 1e-6);
    }

    Y_UNIT_TEST(TestSmallTimeIntervals) {
        TValueHistory<4> history;
        ui64 baseTs = 1000000;

        history.Register(baseTs, 1.0);
        history.Register(baseTs + 1, 1.1); // Очень маленький интервал

        UNIT_ASSERT_DOUBLES_EQUAL(history.AccumulatedValue, 0.1, 1e-6);
        UNIT_ASSERT_VALUES_EQUAL(history.AccumulatedTs, 1);
    }

    Y_UNIT_TEST(TestLargeTimeIntervals) {
        TValueHistory<4> history;
        ui64 baseTs = 1000000;

        history.Register(baseTs, 1.0);
        history.Register(baseTs + 100 * secondInTs, 101.0); // Очень большой интервал

        double expectedValue = 1.0;
        for (size_t i = 0; i < 4; ++i) {
            UNIT_ASSERT_DOUBLES_EQUAL(history.History[i], expectedValue, 1e-6);
        }
    }

    Y_UNIT_TEST(TestPrecisionOverLongDuration) {
        TValueHistory<4> history;
        ui64 baseTs = 1000000;

        double acc = 0;
        int values[4] = {1, 2, 3, 4};
        for (size_t i = 0; i < 100'000; ++i) {
            acc += values[i % 4] * 1.0;
            history.Register(baseTs + i * Us2Ts(1'000'000.0), acc);
        }

        UNIT_ASSERT_DOUBLES_EQUAL(history.GetAvgPart(), 2.5, 1e-6);
    }

    Y_UNIT_TEST(TestRoundingDuringTimestampConversion) {
        TValueHistory<4> history;
        ui64 baseTs = 1000000;

        history.Register(baseTs, 1.0);
        history.Register(baseTs + Us2Ts(1'000'000.5), 2.0);

        double expectedValue = 1.0 / (1'000'000.5 / 1'000'000.0);
        UNIT_ASSERT_DOUBLES_EQUAL(history.GetAvgPartForLastSeconds<true>(1), expectedValue, 1e-6);
        UNIT_ASSERT_DOUBLES_EQUAL(history.GetAvgPartForLastSeconds<false>(1), expectedValue, 1e-6);
        double expectedValue2 = 1.0 - expectedValue;
        UNIT_ASSERT_DOUBLES_EQUAL(history.AccumulatedValue, expectedValue2, 1e-6);
    }

    Y_UNIT_TEST(TestAverageCalculationPrecision) {
        TValueHistory<4> history;
        ui64 baseTs = 1000000;

        history.Register(baseTs, 1.0);
        history.Register(baseTs + Us2Ts(1'000'000.0), 3.0);
        history.Register(baseTs + Us2Ts(2'000'000.0), 6.0);

        UNIT_ASSERT_DOUBLES_EQUAL(history.GetAvgPartForLastSeconds(2), 2.5, 1e-6);
    }
}
