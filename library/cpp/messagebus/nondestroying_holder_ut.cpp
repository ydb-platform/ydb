#include <library/cpp/testing/unittest/registar.h>

#include "nondestroying_holder.h"

Y_UNIT_TEST_SUITE(TNonDestroyingHolder) {
    Y_UNIT_TEST(ToAutoPtr) {
        TNonDestroyingHolder<int> h(new int(11));
        TAutoPtr<int> i(h);
        UNIT_ASSERT_VALUES_EQUAL(11, *i);
        UNIT_ASSERT(!h);
    }
}
