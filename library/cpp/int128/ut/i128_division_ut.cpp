#include <library/cpp/testing/unittest/registar.h>

#include <library/cpp/int128/int128.h>

#include <util/generic/cast.h>

Y_UNIT_TEST_SUITE(I128DivisionBy1Suite) {
    Y_UNIT_TEST(I128Divide0By1) {
        i128 dividend = 0;
        i128 divider = 1;
        i128 expectedQuotient = 0;
        i128 expectedRemainder = 0;

        UNIT_ASSERT_EQUAL(dividend / divider, expectedQuotient);
        UNIT_ASSERT_EQUAL(dividend % divider, expectedRemainder);
    }

    Y_UNIT_TEST(I128Divide1By1) {
        i128 dividend = 1;
        i128 divider = 1;
        i128 expectedQuotient = 1;
        i128 expectedRemainder = 0;

        UNIT_ASSERT_EQUAL(dividend / divider, expectedQuotient);
        UNIT_ASSERT_EQUAL(dividend % divider, expectedRemainder);
    }

    Y_UNIT_TEST(I128Divide2By1) {
        i128 dividend = 2;
        i128 divider = 1;
        i128 expectedQuotient = 2;
        i128 expectedRemainder = 0;

        UNIT_ASSERT_EQUAL(dividend / divider, expectedQuotient);
        UNIT_ASSERT_EQUAL(dividend % divider, expectedRemainder);
    }

    Y_UNIT_TEST(I128Divide42By1) {
        i128 dividend = 42;
        i128 divider = 1;
        i128 expectedQuotient = 42;
        i128 expectedRemainder = 0;

        UNIT_ASSERT_EQUAL(dividend / divider, expectedQuotient);
        UNIT_ASSERT_EQUAL(dividend % divider, expectedRemainder);
    }

    Y_UNIT_TEST(I128DivideMaxUi64By1) {
        i128 dividend = std::numeric_limits<ui64>::max();
        i128 divider = 1;
        i128 expectedQuotient = std::numeric_limits<ui64>::max();
        i128 expectedRemainder = 0;

        UNIT_ASSERT_EQUAL(dividend / divider, expectedQuotient);
        UNIT_ASSERT_EQUAL(dividend % divider, expectedRemainder);
    }

    Y_UNIT_TEST(I128DivideMaxUi64Plus1By1) {
        i128 dividend = i128{std::numeric_limits<ui64>::max()} + i128{1};
        i128 divider = 1;
        i128 expectedQuotient = i128{std::numeric_limits<ui64>::max()} + i128{1};
        i128 expectedRemainder = 0;

        UNIT_ASSERT_EQUAL(dividend / divider, expectedQuotient);
        UNIT_ASSERT_EQUAL(dividend % divider, expectedRemainder);
    }

    Y_UNIT_TEST(I128DivideMaxUi64Plus42By1) {
        i128 dividend = i128{std::numeric_limits<ui64>::max()} + i128{42};
        i128 divider = 1;
        i128 expectedQuotient = i128{std::numeric_limits<ui64>::max()} + i128{42};
        i128 expectedRemainder = 0;

        UNIT_ASSERT_EQUAL(dividend / divider, expectedQuotient);
        UNIT_ASSERT_EQUAL(dividend % divider, expectedRemainder);
    }

    Y_UNIT_TEST(I128DivideMaxI128By1) {
        i128 dividend = std::numeric_limits<i128>::max();
        i128 divider = 1;
        i128 expectedQuotient = std::numeric_limits<i128>::max();
        i128 expectedRemainder = 0;

        UNIT_ASSERT_EQUAL(dividend / divider, expectedQuotient);
        UNIT_ASSERT_EQUAL(dividend % divider, expectedRemainder);
    }

    Y_UNIT_TEST(I128DivideMaxI128Minus1By1) {
        i128 dividend = std::numeric_limits<i128>::max() - 1;
        i128 divider = 1;
        i128 expectedQuotient = std::numeric_limits<i128>::max() - 1;
        i128 expectedRemainder = 0;

        UNIT_ASSERT_EQUAL(dividend / divider, expectedQuotient);
        UNIT_ASSERT_EQUAL(dividend % divider, expectedRemainder);
    }
}

Y_UNIT_TEST_SUITE(I128DivisionByEqualSuite) {
    Y_UNIT_TEST(I128Divide1ByEqual) {
        i128 dividend = 1;
        i128 divider = dividend;
        i128 expectedQuotient = 1;
        i128 expectedRemainder = 0;

        UNIT_ASSERT_EQUAL(dividend / divider, expectedQuotient);
        UNIT_ASSERT_EQUAL(dividend % divider, expectedRemainder);
    }

    Y_UNIT_TEST(I128Divide2ByEqual) {
        i128 dividend = 2;
        i128 divider = dividend;
        i128 expectedQuotient = 1;
        i128 expectedRemainder = 0;

        UNIT_ASSERT_EQUAL(dividend / divider, expectedQuotient);
        UNIT_ASSERT_EQUAL(dividend % divider, expectedRemainder);
    }

    Y_UNIT_TEST(I128Divide42ByEqual) {
        i128 dividend = 42;
        i128 divider = dividend;
        i128 expectedQuotient = 1;
        i128 expectedRemainder = 0;

        UNIT_ASSERT_EQUAL(dividend / divider, expectedQuotient);
        UNIT_ASSERT_EQUAL(dividend % divider, expectedRemainder);
    }

    Y_UNIT_TEST(I128DivideMaxUi64ByEqual) {
        i128 dividend = std::numeric_limits<ui64>::max();
        i128 divider = dividend;
        i128 expectedQuotient = 1;
        i128 expectedRemainder = 0;

        UNIT_ASSERT_EQUAL(dividend / divider, expectedQuotient);
        UNIT_ASSERT_EQUAL(dividend % divider, expectedRemainder);
    }

    Y_UNIT_TEST(I128DivideMaxUi64Plus1ByEqual) {
        i128 dividend = i128{std::numeric_limits<ui64>::max()} + i128{1};
        i128 divider = dividend;
        i128 expectedQuotient = 1;
        i128 expectedRemainder = 0;

        UNIT_ASSERT_EQUAL(dividend / divider, expectedQuotient);
        UNIT_ASSERT_EQUAL(dividend % divider, expectedRemainder);
    }

    Y_UNIT_TEST(I128DivideMaxUi64Plus42ByEqual) {
        i128 dividend = i128{std::numeric_limits<ui64>::max()} + i128{42};
        i128 divider = dividend;
        i128 expectedQuotient = 1;
        i128 expectedRemainder = 0;

        UNIT_ASSERT_EQUAL(dividend / divider, expectedQuotient);
        UNIT_ASSERT_EQUAL(dividend % divider, expectedRemainder);
    }

    Y_UNIT_TEST(I128DivideMaxI128ByEqual) {
        i128 dividend = std::numeric_limits<i128>::max();
        i128 divider = dividend;
        i128 expectedQuotient = 1;
        i128 expectedRemainder = 0;

        UNIT_ASSERT_EQUAL(dividend / divider, expectedQuotient);
        UNIT_ASSERT_EQUAL(dividend % divider, expectedRemainder);
    }

    Y_UNIT_TEST(I128DivideMaxI128Minus1ByEqual) {
        i128 dividend = std::numeric_limits<i128>::max() - 1;
        i128 divider = dividend;
        i128 expectedQuotient = 1;
        i128 expectedRemainder = 0;

        UNIT_ASSERT_EQUAL(dividend / divider, expectedQuotient);
        UNIT_ASSERT_EQUAL(dividend % divider, expectedRemainder);
    }
}

Y_UNIT_TEST_SUITE(I128DivisionLessByHigherSuite) {
    Y_UNIT_TEST(I128Divide42By84) {
        i128 dividend = 42;
        i128 divider = 84;
        i128 expectedQuotient = 0;
        i128 expectedRemainder = 42;

        UNIT_ASSERT_EQUAL(dividend / divider, expectedQuotient);
        UNIT_ASSERT_EQUAL(dividend % divider, expectedRemainder);
    }

    Y_UNIT_TEST(I128Divide42ByMaxUi64) {
        i128 dividend = 42;
        i128 divider = std::numeric_limits<ui64>::max();
        i128 expectedQuotient = 0;
        i128 expectedRemainder = 42;

        UNIT_ASSERT_EQUAL(dividend / divider, expectedQuotient);
        UNIT_ASSERT_EQUAL(dividend % divider, expectedRemainder);
    }

    Y_UNIT_TEST(I128Divide42ByMaxUi64Plus1) {
        i128 dividend = 42;
        i128 divider = i128{std::numeric_limits<ui64>::max()} + i128{1};
        i128 expectedQuotient = 0;
        i128 expectedRemainder = 42;

        UNIT_ASSERT_EQUAL(dividend / divider, expectedQuotient);
        UNIT_ASSERT_EQUAL(dividend % divider, expectedRemainder);
    }

    Y_UNIT_TEST(I128DivideMaxUi64ByMaxUi64Plus1) {
        i128 dividend = i128{std::numeric_limits<ui64>::max()};
        i128 divider = i128{std::numeric_limits<ui64>::max()} + i128{1};
        i128 expectedQuotient = 0;
        i128 expectedRemainder = i128{std::numeric_limits<ui64>::max()};

        UNIT_ASSERT_EQUAL(dividend / divider, expectedQuotient);
        UNIT_ASSERT_EQUAL(dividend % divider, expectedRemainder);
    }
}

Y_UNIT_TEST_SUITE(I128DivisionWithDifferentSigns) {
    Y_UNIT_TEST(DivisionPositiveByNegative) {
        i128 dividend = i128{100};
        i128 divider = i128{-33};
        i128 expectedQuotient = -3;
        i128 expectedRemainder = 1;
        i128 quotient = dividend / divider;
        i128 remainder = dividend % divider;

        UNIT_ASSERT_EQUAL(quotient, expectedQuotient);
        UNIT_ASSERT_EQUAL(remainder, expectedRemainder);
    }

    Y_UNIT_TEST(DivisionNegativeByPositive) {
        i128 dividend = i128{-100};
        i128 divider = i128{33};
        i128 expectedQuotient = -3;
        i128 expectedRemainder = -1;
        i128 quotient = dividend / divider;
        i128 remainder = dividend % divider;

        UNIT_ASSERT_EQUAL(quotient, expectedQuotient);
        UNIT_ASSERT_EQUAL(remainder, expectedRemainder);
    }

    Y_UNIT_TEST(DivisionNegativeByNegative) {
        i128 dividend = i128{-100};
        i128 divider = i128{-33};
        i128 expectedQuotient = 3;
        i128 expectedRemainder = -1;
        i128 quotient = dividend / divider;
        i128 remainder = dividend % divider;

        UNIT_ASSERT_EQUAL(quotient, expectedQuotient);
        UNIT_ASSERT_EQUAL(remainder, expectedRemainder);
    }
}

Y_UNIT_TEST_SUITE(i128DivisionBigByBigSuite) {
    Y_UNIT_TEST(i128DivideBigByBig1) {
        i128 dividend = {64, 0};
        i128 divider = {1, 0};
        i128 expectedQuotient = 64;
        i128 expectedRemainder = 0;

        UNIT_ASSERT_EQUAL(dividend / divider, expectedQuotient);
        UNIT_ASSERT_EQUAL(dividend % divider, expectedRemainder);
    }

    Y_UNIT_TEST(i128DivideBigByBig1_PosByNeg) {
        i128 dividend = i128{64, 0};
        i128 divider = -i128{1, 0};
        i128 expectedQuotient = -i128{64};
        i128 expectedRemainder = 0;

        UNIT_ASSERT_EQUAL(dividend / divider, expectedQuotient);
        UNIT_ASSERT_EQUAL(dividend % divider, expectedRemainder);
    }

    Y_UNIT_TEST(i128DivideBigByBig1_NegByPos) {
        i128 dividend = -i128{64, 0};
        i128 divider = i128{1, 0};
        i128 expectedQuotient = -i128{64};
        i128 expectedRemainder = 0;

        UNIT_ASSERT_EQUAL(dividend / divider, expectedQuotient);
        UNIT_ASSERT_EQUAL(dividend % divider, expectedRemainder);
    }

    Y_UNIT_TEST(i128DivideBigByBig1_NegByNeg) {
        i128 dividend = -i128{64, 0};
        i128 divider = -i128{1, 0};
        i128 expectedQuotient = i128{64};
        i128 expectedRemainder = 0;

        UNIT_ASSERT_EQUAL(dividend / divider, expectedQuotient);
        UNIT_ASSERT_EQUAL(dividend % divider, expectedRemainder);
    }

    Y_UNIT_TEST(i128DivideBigByBig2) {
        i128 dividend = {64, 0};
        i128 divider = {12, 5};
        i128 expectedQuotient = 5;
        i128 expectedRemainder = i128{3, 18446744073709551591ull}; // plz don't ask

        UNIT_ASSERT_EQUAL(dividend / divider, expectedQuotient);
        UNIT_ASSERT_EQUAL(dividend % divider, expectedRemainder);
    }

    Y_UNIT_TEST(i128DivideBigByBig2_PosByNeg) {
        i128 dividend = i128{64, 0};
        i128 divider = -i128{12, 5};
        i128 expectedQuotient = -5;
        i128 expectedRemainder = i128{3, 18446744073709551591ull};

        UNIT_ASSERT_EQUAL(dividend / divider, expectedQuotient);
        UNIT_ASSERT_EQUAL(dividend % divider, expectedRemainder);
    }

    Y_UNIT_TEST(i128DivideBigByBig2_NegByPos) {
        i128 dividend = -i128{64, 0};
        i128 divider = i128{12, 5};
        i128 expectedQuotient = -5;
        i128 expectedRemainder = -i128{3, 18446744073709551591ull};

        UNIT_ASSERT_EQUAL(dividend / divider, expectedQuotient);
        UNIT_ASSERT_EQUAL(dividend % divider, expectedRemainder);
    }

    Y_UNIT_TEST(i128DivideBigByBig2_NegByNeg) {
        i128 dividend = -i128{64, 0};
        i128 divider = -i128{12, 5};
        i128 expectedQuotient = 5;
        i128 expectedRemainder = -i128{3, 18446744073709551591ull};

        UNIT_ASSERT_EQUAL(dividend / divider, expectedQuotient);
        UNIT_ASSERT_EQUAL(dividend % divider, expectedRemainder);
    }

}

Y_UNIT_TEST_SUITE(i128DivisionAlgo) {
    Y_UNIT_TEST(ii128DivideAlgoCheck_PosByPos) {
        /*
            49672666804009505000000 / 10000000 == 4967266680400950
            49672666804009505000000 % 10000000 == 5000000
        */
        i128 dividend = {2692ull, 14031757583392049728ull};
        i64 divider = 10000000;
        i128 expectedQuotient = {0, 4967266680400950ull};
        i128 expectedRemainder = {0, 5000000ull};

        i128 quotient = dividend / divider;
        i128 reminder = dividend % divider;

        UNIT_ASSERT_EQUAL(quotient, expectedQuotient);
        UNIT_ASSERT_EQUAL(reminder, expectedRemainder);
    }

    Y_UNIT_TEST(ii128DivideAlgoCheck_PosByNeg) {
        /*
            49672666804009505000000 / -10000000 == -4967266680400950
            49672666804009505000000 % -10000000 == 5000000
        */
        i128 dividend = {2692ull, 14031757583392049728ull};
        i64 divider = -10000000;
        i128 expectedQuotient = -i128{0, 4967266680400950ull};
        i128 expectedRemainder = {0, 5000000ull};

        i128 quotient = dividend / divider;
        i128 reminder = dividend % divider;

        UNIT_ASSERT_EQUAL(quotient, expectedQuotient);
        UNIT_ASSERT_EQUAL(reminder, expectedRemainder);
    }

    Y_UNIT_TEST(ii128DivideAlgoCheck_NegByPos) {
        /*
            -49672666804009505000000 / 10000000 == -4967266680400950
            -49672666804009505000000 % 10000000 == -5000000
        */
        i128 dividend = -i128{2692ull, 14031757583392049728ull};
        i64 divider = 10000000;
        i128 expectedQuotient = -i128{0, 4967266680400950ull};
        i128 expectedRemainder = -i128{0, 5000000ull};

        i128 quotient = dividend / divider;
        i128 reminder = dividend % divider;

        UNIT_ASSERT_EQUAL(quotient, expectedQuotient);
        UNIT_ASSERT_EQUAL(reminder, expectedRemainder);
    }

    Y_UNIT_TEST(ii128DivideAlgoCheck_NegByNeg) {
        /*
            -49672666804009505000000 / -10000000 == 4967266680400950
            -49672666804009505000000 % -10000000 == -5000000
        */
        i128 dividend = -i128{2692ull, 14031757583392049728ull};
        i64 divider = -10000000;
        i128 expectedQuotient = {0, 4967266680400950ull};
        i128 expectedRemainder = -i128{0, 5000000ull};

        i128 quotient = dividend / divider;
        i128 reminder = dividend % divider;

        UNIT_ASSERT_EQUAL(quotient, expectedQuotient);
        UNIT_ASSERT_EQUAL(reminder, expectedRemainder);
    }

}
