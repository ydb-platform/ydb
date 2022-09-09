#include "stream_ru_calculator.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>

namespace NKikimr::NMetering {

Y_UNIT_TEST_SUITE(TStreamRequestUnitsCalculatorTest) {
    Y_UNIT_TEST(Basic) {
        TStreamRequestUnitsCalculator calculator(4_KB);
        UNIT_ASSERT_VALUES_EQUAL(calculator.GetRemainder(), 4_KB);

        UNIT_ASSERT_VALUES_EQUAL(calculator.CalcConsumption(0), 0);
        UNIT_ASSERT_VALUES_EQUAL(calculator.GetRemainder(), 4_KB);

        UNIT_ASSERT_VALUES_EQUAL(calculator.CalcConsumption(1), 0);
        UNIT_ASSERT_VALUES_EQUAL(calculator.GetRemainder(), 4_KB - 1);

        UNIT_ASSERT_VALUES_EQUAL(calculator.CalcConsumption(4_KB - 1), 0);
        UNIT_ASSERT_VALUES_EQUAL(calculator.GetRemainder(), 0);

        UNIT_ASSERT_VALUES_EQUAL(calculator.CalcConsumption(1), 1);
        UNIT_ASSERT_VALUES_EQUAL(calculator.GetRemainder(), 4_KB - 1);

        UNIT_ASSERT_VALUES_EQUAL(calculator.CalcConsumption(4_KB + 1), 1);
        UNIT_ASSERT_VALUES_EQUAL(calculator.GetRemainder(), 4_KB - 2);

        UNIT_ASSERT_VALUES_EQUAL(calculator.CalcConsumption(8_KB), 2);
        UNIT_ASSERT_VALUES_EQUAL(calculator.GetRemainder(), 4_KB - 2);
    }
}

} // NKikimr::NMetering
