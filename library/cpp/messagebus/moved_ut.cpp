#include <library/cpp/testing/unittest/registar.h>

#include "moved.h"

Y_UNIT_TEST_SUITE(TMovedTest) {
    Y_UNIT_TEST(Simple) {
        TMoved<THolder<int>> h1(MakeHolder<int>(10));
        TMoved<THolder<int>> h2 = h1;
        UNIT_ASSERT(!*h1);
        UNIT_ASSERT(!!*h2);
        UNIT_ASSERT_VALUES_EQUAL(10, **h2);
    }

    void Foo(TMoved<THolder<int>> h) {
        UNIT_ASSERT_VALUES_EQUAL(11, **h);
    }

    Y_UNIT_TEST(PassToFunction) {
        THolder<int> h(new int(11));
        Foo(h);
    }
}
