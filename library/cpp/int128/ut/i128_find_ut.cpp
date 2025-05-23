#include <library/cpp/testing/unittest/registar.h>

#include <library/cpp/int128/int128.h>

#include <util/generic/cast.h>
#include <util/generic/vector.h>

#include <type_traits>

Y_UNIT_TEST_SUITE(Int128FindSuite) {
    Y_UNIT_TEST(Int128Find) {
        const ui128 value = 10;
        TVector<ui128> list = {1, 2, 3, 4, 5, 11, 10};
        UNIT_ASSERT(Find(list, value) != list.end());
    }
}
