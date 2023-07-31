#include "size.h"

#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/maybe.h>

using namespace NGeo;

Y_UNIT_TEST_SUITE(TSizeTest) {
    Y_UNIT_TEST(TestFromString) {
        UNIT_ASSERT_EQUAL(TSize::Parse("0.15,0.67"), TSize(0.15, 0.67));
        UNIT_ASSERT_EQUAL(TSize::Parse("0.15 0.67", " "), TSize(0.15, 0.67));

        UNIT_ASSERT_EXCEPTION(TSize::Parse(""), TBadCastException);
        UNIT_ASSERT_EXCEPTION(TSize::Parse("Hello,world"), TBadCastException);
        UNIT_ASSERT_EXCEPTION(TSize::Parse("-1,-1"), TBadCastException);

        UNIT_ASSERT_EQUAL(TSize::Parse("424242 50", " "), TSize(424242., 50.));
        UNIT_ASSERT_EQUAL(TSize::Parse("50.,424242"), TSize(50., 424242.));
        UNIT_ASSERT_EQUAL(TSize::Parse("     0.01, 0.01"), TSize(0.01, 0.01));
        UNIT_ASSERT_EXCEPTION(TSize::Parse("0.01 ,0.01"), TBadCastException);
        UNIT_ASSERT_EXCEPTION(TSize::Parse("0.01,0.01 "), TBadCastException);
    }

    Y_UNIT_TEST(TestTryFromString) {
        UNIT_ASSERT(TSize::TryParse("1,2"));
        UNIT_ASSERT(!TSize::TryParse("-1,-2"));
        UNIT_ASSERT(!TSize::TryParse("1,2a"));
    }
}
