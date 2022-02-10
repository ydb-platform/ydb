#include "bits.h"
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

    Y_UNIT_TEST_SUITE(TBitsTest) {
        Y_UNIT_TEST(TestNaiveClz) {
            UNIT_ASSERT(sizeof(unsigned) == sizeof(ui32));
            UNIT_ASSERT_EQUAL(NaiveClz(58649), 16);
            UNIT_ASSERT_EQUAL(NaiveClz(0xF0000000), 0);
            UNIT_ASSERT_EQUAL(NaiveClz(0xF000000), 4);
            UNIT_ASSERT_EQUAL(NaiveClz(0x1), 31);
        }
    }

} // NKikimr
