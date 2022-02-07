#include <library/cpp/testing/unittest/registar.h>

#include "vector_swaps.h"

Y_UNIT_TEST_SUITE(TVectorSwapsTest) {
    Y_UNIT_TEST(Simple) {
        TVectorSwaps<THolder<unsigned>> v;
        for (unsigned i = 0; i < 100; ++i) {
            THolder<unsigned> tmp(new unsigned(i));
            v.push_back(tmp);
        }

        for (unsigned i = 0; i < 100; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(i, *v[i]);
        }
    }
}
