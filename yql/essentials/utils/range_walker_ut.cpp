#include "range_walker.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NYql;

Y_UNIT_TEST_SUITE(TRangeWalkerTests) {
    Y_UNIT_TEST(InvalidRange) {
        UNIT_ASSERT_EXCEPTION_CONTAINS(TRangeWalker<int>(2, 1), yexception, "Invalid range for walker");
    }

    Y_UNIT_TEST(SingleValueRange) {
        TRangeWalker<int> w(5, 5);
        UNIT_ASSERT_EQUAL(5, w.GetStart());
        UNIT_ASSERT_EQUAL(5, w.GetFinish());
        UNIT_ASSERT_EQUAL(1, w.GetRangeSize());

        for (int i = 0; i < 10; ++i) {
            UNIT_ASSERT_EQUAL(5, w.MoveToNext());
        }
    }

    Y_UNIT_TEST(ManyValuesRange) {
        TRangeWalker<int> w(5, 7);
        UNIT_ASSERT_EQUAL(5, w.GetStart());
        UNIT_ASSERT_EQUAL(7, w.GetFinish());
        UNIT_ASSERT_EQUAL(3, w.GetRangeSize());

        for (int i = 0; i < 10; ++i) {
            UNIT_ASSERT_EQUAL(5, w.MoveToNext());
            UNIT_ASSERT_EQUAL(6, w.MoveToNext());
            UNIT_ASSERT_EQUAL(7, w.MoveToNext());
        }
    }
}
