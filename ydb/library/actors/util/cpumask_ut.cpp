#include "cpumask.h"

#include <library/cpp/testing/unittest/registar.h>

Y_UNIT_TEST_SUITE(TCpuMaskTest) {
    Y_UNIT_TEST(RejectsInvertedRange) {
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            TCpuMask("3-1"),
            yexception,
            "invalid cpu range '3-1'");
    }

    Y_UNIT_TEST(RejectsCpuIdAboveSafetyLimit) {
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            TCpuMask("65536"),
            yexception,
            "invalid cpu range '65536'");
    }
}
