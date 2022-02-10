#include "udf_counter.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NYql::NUdf;

Y_UNIT_TEST_SUITE(TUdfCounter) {
    Y_UNIT_TEST(NullCounter) {
        TCounter c;
        c.Inc();
        c.Dec();
        c.Add(1);
        c.Sub(2);
        c.Set(3);
    }

    Y_UNIT_TEST(RealCounter) {
        i64 value = 10;
        TCounter c(&value);
        c.Inc();
        UNIT_ASSERT_VALUES_EQUAL(value, 11);
        c.Dec();
        UNIT_ASSERT_VALUES_EQUAL(value, 10);
        c.Add(1);
        UNIT_ASSERT_VALUES_EQUAL(value, 11);
        c.Sub(2);
        UNIT_ASSERT_VALUES_EQUAL(value, 9);
        c.Set(3);
        UNIT_ASSERT_VALUES_EQUAL(value, 3);
    }

    Y_UNIT_TEST(Copy) {
        i64 value = 1;
        TCounter c1(&value);
        TCounter c2(c1); // copy ctor
        c2.Inc();
        UNIT_ASSERT_VALUES_EQUAL(value, 2);
        TCounter c3;
        c3 = c1; // assign
        c3.Inc();
        UNIT_ASSERT_VALUES_EQUAL(value, 3);
        TCounter c4(std::move(c1)); // move ctor
        c4.Inc();
        UNIT_ASSERT_VALUES_EQUAL(value, 4);
        TCounter c5;
        c5 = std::move(c3); // move assign
        c5.Inc();
        UNIT_ASSERT_VALUES_EQUAL(value, 5);
    }
}
