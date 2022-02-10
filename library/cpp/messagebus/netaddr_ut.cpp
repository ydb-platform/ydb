#include <library/cpp/testing/unittest/registar.h>

#include "netaddr.h"
#include "test_utils.h"

using namespace NBus;

Y_UNIT_TEST_SUITE(TNetAddr) {
    Y_UNIT_TEST(ResolveIpv4) {
        ASSUME_IP_V4_ENABLED;
        UNIT_ASSERT(TNetAddr("ns1.yandex.ru", 80, EIP_VERSION_4).IsIpv4());
    }

    Y_UNIT_TEST(ResolveIpv6) {
        UNIT_ASSERT(TNetAddr("ns1.yandex.ru", 80, EIP_VERSION_6).IsIpv6());
    }

    Y_UNIT_TEST(ResolveAny) {
        TNetAddr("ns1.yandex.ru", 80, EIP_VERSION_ANY);
    }
}
