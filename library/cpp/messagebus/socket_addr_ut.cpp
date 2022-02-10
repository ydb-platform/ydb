#include <library/cpp/testing/unittest/registar.h>

#include "netaddr.h"
#include "socket_addr.h"

#include <util/string/cast.h>

using namespace NBus;
using namespace NBus::NPrivate;

Y_UNIT_TEST_SUITE(TBusSocketAddr) {
    Y_UNIT_TEST(Simple) {
        UNIT_ASSERT_VALUES_EQUAL(TString("127.0.0.1:80"), ToString(TBusSocketAddr("127.0.0.1", 80)));
    }
}
