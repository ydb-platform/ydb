#include "validators.h"

#include <library/cpp/testing/unittest/registar.h>

Y_UNIT_TEST_SUITE(Init) {
    Y_UNIT_TEST(TWithDefaultParser) {
        TString err;
        ValidateStaticGroup({}, {}, err);
    }
}
