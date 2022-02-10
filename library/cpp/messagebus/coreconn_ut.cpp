#include <library/cpp/testing/unittest/registar.h>

#include "coreconn.h"

#include <util/generic/yexception.h>

Y_UNIT_TEST_SUITE(TMakeIpVersionTest) {
    using namespace NBus;

    Y_UNIT_TEST(IpV4Allowed) {
        UNIT_ASSERT_EQUAL(MakeIpVersion(true, false), EIP_VERSION_4);
    }

    Y_UNIT_TEST(IpV6Allowed) {
        UNIT_ASSERT_EQUAL(MakeIpVersion(false, true), EIP_VERSION_6);
    }

    Y_UNIT_TEST(AllAllowed) {
        UNIT_ASSERT_EQUAL(MakeIpVersion(true, true), EIP_VERSION_ANY);
    }

    Y_UNIT_TEST(NothingAllowed) {
        UNIT_ASSERT_EXCEPTION(MakeIpVersion(false, false), yexception);
    }
}
