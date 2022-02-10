#include <library/cpp/testing/unittest/registar.h>

#include <library/cpp/int128/int128.h>

#include <util/generic/cast.h>

Y_UNIT_TEST_SUITE(I128ComparisonPositiveWithPositiveSuite) {
    Y_UNIT_TEST(PositivePositiveGreater) {
        UNIT_ASSERT(i128{1} > i128{0});
        UNIT_ASSERT(i128{2} > i128{1});
        UNIT_ASSERT(i128{42} > i128{0});
        UNIT_ASSERT(i128{42} > i128{1});
        i128 big = i128{1, 0};
        UNIT_ASSERT(big > i128{1});
        UNIT_ASSERT(std::numeric_limits<i128>::max() > i128{0});
    }

    Y_UNIT_TEST(PositivePositiveGreaterOrEqual) {
        UNIT_ASSERT(i128{1} >= i128{0});
        UNIT_ASSERT(i128{2} >= i128{1});
        UNIT_ASSERT(i128{42} >= i128{0});
        UNIT_ASSERT(i128{42} >= i128{1});
        i128 big = i128{1, 0};
        UNIT_ASSERT(big >= i128{1});
        UNIT_ASSERT(std::numeric_limits<i128>::max() >= i128{0});

        UNIT_ASSERT(i128{0} >= i128{0});
        UNIT_ASSERT(i128{1} >= i128{1});
        UNIT_ASSERT(i128{2} >= i128{2});
        UNIT_ASSERT(i128{42} >= i128{42});
        UNIT_ASSERT(big >= big);
        UNIT_ASSERT(std::numeric_limits<i128>::max() >= std::numeric_limits<i128>::max());
    }

    Y_UNIT_TEST(PositivePositiveLess) {
        UNIT_ASSERT(i128{0} < i128{1});
        UNIT_ASSERT(i128{1} < i128{2});
        UNIT_ASSERT(i128{0} < i128{42});
        UNIT_ASSERT(i128{1} < i128{42});
        i128 big = i128{1, 0};
        UNIT_ASSERT(i128{1} < big);
        UNIT_ASSERT(i128{0} < std::numeric_limits<i128>::max());
    }

    Y_UNIT_TEST(PositivePositiveLessOrEqual) {
        UNIT_ASSERT(i128{0} <= i128{1});
        UNIT_ASSERT(i128{1} <= i128{2});
        UNIT_ASSERT(i128{0} <= i128{42});
        UNIT_ASSERT(i128{1} <= i128{42});
        i128 big = i128{1, 0};
        UNIT_ASSERT(i128{1} <= big);
        UNIT_ASSERT(i128{0} <= std::numeric_limits<i128>::max());

        UNIT_ASSERT(i128{0} <= i128{0});
        UNIT_ASSERT(i128{1} <= i128{1});
        UNIT_ASSERT(i128{2} <= i128{2});
        UNIT_ASSERT(i128{42} <= i128{42});
        UNIT_ASSERT(big <= big);
        UNIT_ASSERT(std::numeric_limits<i128>::max() <= std::numeric_limits<i128>::max());
    }
}

Y_UNIT_TEST_SUITE(I128ComparisonPositiveWithNegativeSuite) {
    Y_UNIT_TEST(PositiveNegativeGreater) {
        UNIT_ASSERT(i128{0} > i128{-1});
        UNIT_ASSERT(i128{2} > i128{-1});
        UNIT_ASSERT(i128{0} > i128{-42});
        UNIT_ASSERT(i128{42} > i128{-1});
        i128 big = i128{1, 0};
        UNIT_ASSERT(big > i128{-1});
        UNIT_ASSERT(std::numeric_limits<i128>::max() > i128{-1});
    }

    Y_UNIT_TEST(PositiveNegativeGreaterOrEqual) {
        UNIT_ASSERT(i128{0} >= i128{-1});
        UNIT_ASSERT(i128{2} >= i128{-1});
        UNIT_ASSERT(i128{0} >= i128{-42});
        UNIT_ASSERT(i128{42} >= i128{-1});
        i128 big = i128{1, 0};
        UNIT_ASSERT(big >= i128{-1});
        UNIT_ASSERT(std::numeric_limits<i128>::max() >= i128{-1});
    }

    Y_UNIT_TEST(NegativePositiveLess) {
        UNIT_ASSERT(i128{-1} < i128{0});
        UNIT_ASSERT(i128{-1} < i128{2});
        UNIT_ASSERT(i128{-42} < i128{0});
        UNIT_ASSERT(i128{-1} < i128{42});
        i128 big = i128{1, 0};
        UNIT_ASSERT(i128{-1} < big);
        UNIT_ASSERT(i128{-1} < std::numeric_limits<i128>::max());
    }

    Y_UNIT_TEST(NegativePositiveLessOrEqual) {
        UNIT_ASSERT(i128{-1} <= i128{0});
        UNIT_ASSERT(i128{-1} <= i128{2});
        UNIT_ASSERT(i128{-42} <= i128{0});
        UNIT_ASSERT(i128{-1} <= i128{42});
        i128 big = i128{1, 0};
        UNIT_ASSERT(i128{-1} <= big);
        UNIT_ASSERT(i128{-1} <= std::numeric_limits<i128>::max());
    }
}

Y_UNIT_TEST_SUITE(I128ComparisonNegativeWithNegativeSuite) {
    Y_UNIT_TEST(NegativeNegativeGreater) {
        UNIT_ASSERT(i128{-1} > i128{-2});
        UNIT_ASSERT(i128{-2} > i128{-3});
        UNIT_ASSERT(i128{-1} > i128{-42});
        UNIT_ASSERT(i128{-42} > i128{-142});
        i128 big = -i128{1, 0};
        UNIT_ASSERT(i128{-1} > big);
        UNIT_ASSERT(i128{-1} > std::numeric_limits<i128>::min());
    }

    Y_UNIT_TEST(NegativeNegativeGreaterOrEqual) {
        UNIT_ASSERT(i128{-1} >= i128{-2});
        UNIT_ASSERT(i128{-2} >= i128{-3});
        UNIT_ASSERT(i128{-1} >= i128{-42});
        UNIT_ASSERT(i128{-42} >= i128{-142});
        i128 big = -i128{1, 0};
        UNIT_ASSERT(i128{-1} >= big);
        UNIT_ASSERT(i128{-1} >= std::numeric_limits<i128>::min());
    }

    Y_UNIT_TEST(NegativeNegativeLess) {
        UNIT_ASSERT(i128{-2} < i128{-1});
        UNIT_ASSERT(i128{-3} < i128{-2});
        UNIT_ASSERT(i128{-42} < i128{-1});
        UNIT_ASSERT(i128{-142} < i128{42});
        i128 big = -i128{1, 0};
        UNIT_ASSERT(big < i128{-1});
        UNIT_ASSERT(std::numeric_limits<i128>::min() < i128{-1});
    }

    Y_UNIT_TEST(NegativeNegativeLessOrEqual) {
        UNIT_ASSERT(i128{-2} <= i128{-1});
        UNIT_ASSERT(i128{-3} <= i128{-2});
        UNIT_ASSERT(i128{-42} <= i128{-1});
        UNIT_ASSERT(i128{-142} <= i128{42});
        i128 big = -i128{1, 0};
        UNIT_ASSERT(big <= i128{-1});
        UNIT_ASSERT(std::numeric_limits<i128>::min() <= i128{-1});
    }
}
