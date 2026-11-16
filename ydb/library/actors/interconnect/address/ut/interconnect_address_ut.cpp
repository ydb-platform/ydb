#include <ydb/library/actors/interconnect/address/interconnect_address.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NInterconnect {

Y_UNIT_TEST_SUITE(TInterconnectAddress) {
    Y_UNIT_TEST(IsLinkLocalIPv4) {
        UNIT_ASSERT(TAddress("169.254.0.0", 0).IsLinkLocal());
        UNIT_ASSERT(TAddress("169.254.255.255", 0).IsLinkLocal());
        UNIT_ASSERT(!TAddress("169.253.255.255", 0).IsLinkLocal());
        UNIT_ASSERT(!TAddress("169.255.0.0", 0).IsLinkLocal());
    }

    Y_UNIT_TEST(IsLinkLocalIPv6) {
        UNIT_ASSERT(TAddress("fe80::", 0).IsLinkLocal());
        UNIT_ASSERT(TAddress("febf:ffff:ffff:ffff:ffff:ffff:ffff:ffff", 0).IsLinkLocal());
        UNIT_ASSERT(!TAddress("fe7f:ffff:ffff:ffff:ffff:ffff:ffff:ffff", 0).IsLinkLocal());
        UNIT_ASSERT(!TAddress("fec0::", 0).IsLinkLocal());
    }

    Y_UNIT_TEST(UnknownAddressIsNotLinkLocal) {
        UNIT_ASSERT(!TAddress("not-an-address", 0).IsLinkLocal());
    }
}

} // namespace NInterconnect
