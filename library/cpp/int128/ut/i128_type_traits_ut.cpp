#include <library/cpp/testing/unittest/registar.h>

#include <library/cpp/int128/int128.h>

#include <util/generic/cast.h>

Y_UNIT_TEST_SUITE(I128TypeTraitsSuite) {
    Y_UNIT_TEST(OperatorNegate0) {
        const i128 n = 0;
        const i128 m = -n;
        UNIT_ASSERT(n == m);
    }

    Y_UNIT_TEST(OperatorNegate1) {
        const i128 n = 1;
        const i128 m = -n;
        const i128 expected = -1;
        UNIT_ASSERT(m == expected);
    }

    Y_UNIT_TEST(OperatorNegate2Pow64) {
        const i128 n = i128{1, 0};
        const i128 m = -n;
        const i128 expected = {static_cast<ui64>(-1), 0};
        UNIT_ASSERT(m == expected);
    }

    Y_UNIT_TEST(OperatorNegateNegate) {
        const i128 x = 1;
        const i128 y = -x;
        const i128 z = -y;
        UNIT_ASSERT(z == x);
    }

    Y_UNIT_TEST(AbsFromPositive) {
        const i128 n = 1;
        const i128 m = abs(n);
        UNIT_ASSERT(m == n);
    }

    Y_UNIT_TEST(AbsFromNegative) {
        const i128 n = -1;
        const i128 m = abs(n);
        const i128 expected = 1;
        UNIT_ASSERT(m == expected);
    }

    Y_UNIT_TEST(AbsFromZero) {
        const i128 n = 0;
        const i128 m = abs(n);
        UNIT_ASSERT(m == n);
    }

    Y_UNIT_TEST(SignbitOfPositive) {
        const i128 n = 1;
        UNIT_ASSERT(!std::signbit(n));
    }

    Y_UNIT_TEST(SignbitOfNegative) {
        const i128 n = -1;
        UNIT_ASSERT(std::signbit(n));
    }

    Y_UNIT_TEST(SignbitOfZero) {
        const i128 n = 0;
        UNIT_ASSERT(!std::signbit(n));
    }
}
