#include <library/cpp/testing/unittest/registar.h>

#include <library/cpp/int128/int128.h>

#include <util/generic/cast.h>

Y_UNIT_TEST_SUITE(Ui128DivisionBy1Suite) {
    Y_UNIT_TEST(Ui128Divide0By1) {
        ui128 dividend = 0;
        ui128 divider = 1;
        ui128 expectedQuotient = 0;
        ui128 expectedRemainder = 0;

        UNIT_ASSERT_EQUAL(dividend / divider, expectedQuotient);
        UNIT_ASSERT_EQUAL(dividend % divider, expectedRemainder);
    }

    Y_UNIT_TEST(Ui128Divide1By1) {
        ui128 dividend = 1;
        ui128 divider = 1;
        ui128 expectedQuotient = 1;
        ui128 expectedRemainder = 0;

        UNIT_ASSERT_EQUAL(dividend / divider, expectedQuotient);
        UNIT_ASSERT_EQUAL(dividend % divider, expectedRemainder);
    }

    Y_UNIT_TEST(Ui128Divide2By1) {
        ui128 dividend = 2;
        ui128 divider = 1;
        ui128 expectedQuotient = 2;
        ui128 expectedRemainder = 0;

        UNIT_ASSERT_EQUAL(dividend / divider, expectedQuotient);
        UNIT_ASSERT_EQUAL(dividend % divider, expectedRemainder);
    }

    Y_UNIT_TEST(Ui128Divide42By1) {
        ui128 dividend = 42;
        ui128 divider = 1;
        ui128 expectedQuotient = 42;
        ui128 expectedRemainder = 0;

        UNIT_ASSERT_EQUAL(dividend / divider, expectedQuotient);
        UNIT_ASSERT_EQUAL(dividend % divider, expectedRemainder);
    }

    Y_UNIT_TEST(Ui128DivideMaxUi64By1) {
        ui128 dividend = std::numeric_limits<ui64>::max();
        ui128 divider = 1;
        ui128 expectedQuotient = std::numeric_limits<ui64>::max();
        ui128 expectedRemainder = 0;

        UNIT_ASSERT_EQUAL(dividend / divider, expectedQuotient);
        UNIT_ASSERT_EQUAL(dividend % divider, expectedRemainder);
    }

    Y_UNIT_TEST(Ui128DivideMaxUi64Plus1By1) {
        ui128 dividend = ui128{std::numeric_limits<ui64>::max()} + ui128{1};
        ui128 divider = 1;
        ui128 expectedQuotient = ui128{std::numeric_limits<ui64>::max()} + ui128{1};
        ui128 expectedRemainder = 0;

        UNIT_ASSERT_EQUAL(dividend / divider, expectedQuotient);
        UNIT_ASSERT_EQUAL(dividend % divider, expectedRemainder);
    }

    Y_UNIT_TEST(Ui128DivideMaxUi64Plus42By1) {
        ui128 dividend = ui128{std::numeric_limits<ui64>::max()} + ui128{42};
        ui128 divider = 1;
        ui128 expectedQuotient = ui128{std::numeric_limits<ui64>::max()} + ui128{42};
        ui128 expectedRemainder = 0;

        UNIT_ASSERT_EQUAL(dividend / divider, expectedQuotient);
        UNIT_ASSERT_EQUAL(dividend % divider, expectedRemainder);
    }

    Y_UNIT_TEST(Ui128DivideMaxUi128By1) {
        ui128 dividend = std::numeric_limits<ui128>::max();
        ui128 divider = 1;
        ui128 expectedQuotient = std::numeric_limits<ui128>::max();
        ui128 expectedRemainder = 0;

        UNIT_ASSERT_EQUAL(dividend / divider, expectedQuotient);
        UNIT_ASSERT_EQUAL(dividend % divider, expectedRemainder);
    }

    Y_UNIT_TEST(Ui128DivideMaxUi128Minus1By1) {
        ui128 dividend = std::numeric_limits<ui128>::max() - 1;
        ui128 divider = 1;
        ui128 expectedQuotient = std::numeric_limits<ui128>::max() - 1;
        ui128 expectedRemainder = 0;

        UNIT_ASSERT_EQUAL(dividend / divider, expectedQuotient);
        UNIT_ASSERT_EQUAL(dividend % divider, expectedRemainder);
    }
}

Y_UNIT_TEST_SUITE(Ui128DivisionByEqualSuite) {
    Y_UNIT_TEST(Ui128Divide1ByEqual) {
        ui128 dividend = 1;
        ui128 divider = dividend;
        ui128 expectedQuotient = 1;
        ui128 expectedRemainder = 0;

        UNIT_ASSERT_EQUAL(dividend / divider, expectedQuotient);
        UNIT_ASSERT_EQUAL(dividend % divider, expectedRemainder);
    }

    Y_UNIT_TEST(Ui128Divide2ByEqual) {
        ui128 dividend = 2;
        ui128 divider = dividend;
        ui128 expectedQuotient = 1;
        ui128 expectedRemainder = 0;

        UNIT_ASSERT_EQUAL(dividend / divider, expectedQuotient);
        UNIT_ASSERT_EQUAL(dividend % divider, expectedRemainder);
    }

    Y_UNIT_TEST(Ui128Divide42ByEqual) {
        ui128 dividend = 42;
        ui128 divider = dividend;
        ui128 expectedQuotient = 1;
        ui128 expectedRemainder = 0;

        UNIT_ASSERT_EQUAL(dividend / divider, expectedQuotient);
        UNIT_ASSERT_EQUAL(dividend % divider, expectedRemainder);
    }

    Y_UNIT_TEST(Ui128DivideMaxUi64ByEqual) {
        ui128 dividend = std::numeric_limits<ui64>::max();
        ui128 divider = dividend;
        ui128 expectedQuotient = 1;
        ui128 expectedRemainder = 0;

        UNIT_ASSERT_EQUAL(dividend / divider, expectedQuotient);
        UNIT_ASSERT_EQUAL(dividend % divider, expectedRemainder);
    }

    Y_UNIT_TEST(Ui128DivideMaxUi64Plus1ByEqual) {
        ui128 dividend = ui128{std::numeric_limits<ui64>::max()} + ui128{1};
        ui128 divider = dividend;
        ui128 expectedQuotient = 1;
        ui128 expectedRemainder = 0;

        UNIT_ASSERT_EQUAL(dividend / divider, expectedQuotient);
        UNIT_ASSERT_EQUAL(dividend % divider, expectedRemainder);
    }

    Y_UNIT_TEST(Ui128DivideMaxUi64Plus42ByEqual) {
        ui128 dividend = ui128{std::numeric_limits<ui64>::max()} + ui128{42};
        ui128 divider = dividend;
        ui128 expectedQuotient = 1;
        ui128 expectedRemainder = 0;

        UNIT_ASSERT_EQUAL(dividend / divider, expectedQuotient);
        UNIT_ASSERT_EQUAL(dividend % divider, expectedRemainder);
    }

    Y_UNIT_TEST(Ui128DivideMaxUi128ByEqual) {
        ui128 dividend = std::numeric_limits<ui128>::max();
        ui128 divider = dividend;
        ui128 expectedQuotient = 1;
        ui128 expectedRemainder = 0;

        UNIT_ASSERT_EQUAL(dividend / divider, expectedQuotient);
        UNIT_ASSERT_EQUAL(dividend % divider, expectedRemainder);
    }

    Y_UNIT_TEST(Ui128DivideMaxUi128Minus1ByEqual) {
        ui128 dividend = std::numeric_limits<ui128>::max() - 1;
        ui128 divider = dividend;
        ui128 expectedQuotient = 1;
        ui128 expectedRemainder = 0;

        UNIT_ASSERT_EQUAL(dividend / divider, expectedQuotient);
        UNIT_ASSERT_EQUAL(dividend % divider, expectedRemainder);
    }
}

Y_UNIT_TEST_SUITE(Ui128DivisionLessByHigherSuite) {
    Y_UNIT_TEST(Ui128Divide42By84) {
        ui128 dividend = 42;
        ui128 divider = 84;
        ui128 expectedQuotient = 0;
        ui128 expectedRemainder = 42;

        UNIT_ASSERT_EQUAL(dividend / divider, expectedQuotient);
        UNIT_ASSERT_EQUAL(dividend % divider, expectedRemainder);
    }

    Y_UNIT_TEST(Ui128Divide42ByMaxUi64) {
        ui128 dividend = 42;
        ui128 divider = std::numeric_limits<ui64>::max();
        ui128 expectedQuotient = 0;
        ui128 expectedRemainder = 42;

        UNIT_ASSERT_EQUAL(dividend / divider, expectedQuotient);
        UNIT_ASSERT_EQUAL(dividend % divider, expectedRemainder);
    }

    Y_UNIT_TEST(Ui128Divide42ByMaxUi64Plus1) {
        ui128 dividend = 42;
        ui128 divider = ui128{std::numeric_limits<ui64>::max()} + ui128{1};
        ui128 expectedQuotient = 0;
        ui128 expectedRemainder = 42;

        UNIT_ASSERT_EQUAL(dividend / divider, expectedQuotient);
        UNIT_ASSERT_EQUAL(dividend % divider, expectedRemainder);
    }

    Y_UNIT_TEST(Ui128DivideMaxUi64ByMaxUi64Plus1) {
        ui128 dividend = ui128{std::numeric_limits<ui64>::max()};
        ui128 divider = ui128{std::numeric_limits<ui64>::max()} + ui128{1};
        ui128 expectedQuotient = 0;
        ui128 expectedRemainder = ui128{std::numeric_limits<ui64>::max()};

        UNIT_ASSERT_EQUAL(dividend / divider, expectedQuotient);
        UNIT_ASSERT_EQUAL(dividend % divider, expectedRemainder);
    }
}

Y_UNIT_TEST_SUITE(Ui128DivisionBigByBigSuite) {
    Y_UNIT_TEST(Ui128DivideBigByBig1) {
        ui128 dividend = {64, 0};
        ui128 divider = {1, 0};
        ui128 expectedQuotient = 64;
        ui128 expectedRemainder = 0;

        UNIT_ASSERT_EQUAL(dividend / divider, expectedQuotient);
        UNIT_ASSERT_EQUAL(dividend % divider, expectedRemainder);
    }

    Y_UNIT_TEST(Ui128DivideBigByBig2) {
        ui128 dividend = {64, 0};
        ui128 divider = {12, 5};
        ui128 expectedQuotient = 5;
        ui128 expectedRemainder = ui128{3, 18446744073709551591ull}; // plz don't ask

        UNIT_ASSERT_EQUAL(dividend / divider, expectedQuotient);
        UNIT_ASSERT_EQUAL(dividend % divider, expectedRemainder);
    }
}

Y_UNIT_TEST_SUITE(Ui128DivisionAlgo) {
    Y_UNIT_TEST(Ui128DivideAlgoCheck) {
        /*
            49672666804009505000000 / 10000000 == 4967266680400950
            49672666804009505000000 % 10000000 == 5000000
        */
        ui128 dividend = {2692ull, 14031757583392049728ull};
        ui64 divider = 10000000;
        ui128 expectedQuotient = {0, 4967266680400950ull};
        ui128 expectedRemainder = {0, 5000000ull};

        ui128 quotient = dividend / divider;
        ui128 reminder = dividend % divider;

        UNIT_ASSERT_EQUAL(quotient, expectedQuotient);
        UNIT_ASSERT_EQUAL(reminder, expectedRemainder);
    }
}
