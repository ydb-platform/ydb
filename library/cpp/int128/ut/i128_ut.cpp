#include <library/cpp/testing/unittest/registar.h>

#include <library/cpp/int128/int128.h>

#include <util/generic/cast.h>

Y_UNIT_TEST_SUITE(I128Suite) {
    Y_UNIT_TEST(CreateI128FromUnsigned) {
        i128 v{ui64(1)};
        Y_UNUSED(v);
    }
}
