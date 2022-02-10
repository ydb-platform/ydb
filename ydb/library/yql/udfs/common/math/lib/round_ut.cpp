#include "round.h" 
 
#include <library/cpp/testing/unittest/registar.h>
 
#include <util/string/hex.h> 
 
using namespace NMathUdf;

Y_UNIT_TEST_SUITE(TRound) {
    Y_UNIT_TEST(Basic) {
        double value = 1930.0 / 3361.0; 
        double result = RoundToDecimal<long double>(value, -3);
        double answer = 0.574;
        UNIT_ASSERT_VALUES_EQUAL(
            HexEncode(&result, sizeof(double)),
            HexEncode(&answer, sizeof(double)));
    }

    Y_UNIT_TEST(Mod) {
        UNIT_ASSERT_VALUES_EQUAL(*Mod(-1, 7), 6);
        UNIT_ASSERT_VALUES_EQUAL(*Mod(1, 7), 1);
        UNIT_ASSERT_VALUES_EQUAL(*Mod(0, 7), 0);

        UNIT_ASSERT_VALUES_EQUAL(*Mod(-1, -7), -1);
        UNIT_ASSERT_VALUES_EQUAL(*Mod(1, -7), -6);
        UNIT_ASSERT_VALUES_EQUAL(*Mod(0, -7), 0);

        UNIT_ASSERT_VALUES_EQUAL(*Mod(-15, 7), 6);
        UNIT_ASSERT_VALUES_EQUAL(*Mod(15, 7), 1);
        UNIT_ASSERT_VALUES_EQUAL(*Mod(14, 7), 0);
        UNIT_ASSERT_VALUES_EQUAL(*Mod(-14, 7), 0);

        UNIT_ASSERT_VALUES_EQUAL(*Mod(-15, -7), -1);
        UNIT_ASSERT_VALUES_EQUAL(*Mod(15, -7), -6);
        UNIT_ASSERT_VALUES_EQUAL(*Mod(14, -7), 0);
        UNIT_ASSERT_VALUES_EQUAL(*Mod(-14, -7), 0);

        UNIT_ASSERT(!Mod(-14, 0));
    }

    Y_UNIT_TEST(Rem) {
        UNIT_ASSERT_VALUES_EQUAL(*Rem(-1, 7), -1);
        UNIT_ASSERT_VALUES_EQUAL(*Rem(1, 7), 1);
        UNIT_ASSERT_VALUES_EQUAL(*Rem(0, 7), 0);

        UNIT_ASSERT_VALUES_EQUAL(*Rem(-1, -7), -1);
        UNIT_ASSERT_VALUES_EQUAL(*Rem(1, -7), 1);
        UNIT_ASSERT_VALUES_EQUAL(*Rem(0, -7), 0);

        UNIT_ASSERT_VALUES_EQUAL(*Rem(-15, 7), -1);
        UNIT_ASSERT_VALUES_EQUAL(*Rem(15, 7), 1);
        UNIT_ASSERT_VALUES_EQUAL(*Rem(14, 7), 0);
        UNIT_ASSERT_VALUES_EQUAL(*Rem(-14, 7), 0);

        UNIT_ASSERT_VALUES_EQUAL(*Rem(-15, -7), -1);
        UNIT_ASSERT_VALUES_EQUAL(*Rem(15, -7), 1);
        UNIT_ASSERT_VALUES_EQUAL(*Rem(14, -7), 0);
        UNIT_ASSERT_VALUES_EQUAL(*Rem(-14, -7), 0);
        UNIT_ASSERT(!Rem(-14, 0));
    }
} 
