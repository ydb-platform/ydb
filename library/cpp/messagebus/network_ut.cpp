#include <library/cpp/testing/unittest/registar.h>

#include "network.h"

#include <library/cpp/messagebus/test/helper/fixed_port.h>

using namespace NBus;
using namespace NBus::NPrivate;
using namespace NBus::NTest;

namespace {
    int GetSockPort(SOCKET socket) {
        sockaddr_storage addr;
        Zero(addr);

        socklen_t len = sizeof(addr);

        int r = ::getsockname(socket, (sockaddr*)&addr, &len);
        UNIT_ASSERT(r >= 0);

        if (addr.ss_family == AF_INET) {
            sockaddr_in* addr_in = (sockaddr_in*)&addr;
            return InetToHost(addr_in->sin_port);
        } else if (addr.ss_family == AF_INET6) {
            sockaddr_in6* addr_in6 = (sockaddr_in6*)&addr;
            return InetToHost(addr_in6->sin6_port);
        } else {
            UNIT_FAIL("unknown AF");
            throw 1;
        }
    }
}

Y_UNIT_TEST_SUITE(Network) {
    Y_UNIT_TEST(BindOnPortConcrete) {
        if (!IsFixedPortTestAllowed()) {
            return;
        }

        TVector<TBindResult> r = BindOnPort(FixedPort, false).second;
        UNIT_ASSERT_VALUES_EQUAL(size_t(2), r.size());

        for (TVector<TBindResult>::iterator i = r.begin(); i != r.end(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(i->Addr.GetPort(), GetSockPort(i->Socket->operator SOCKET()));
        }
    }

    Y_UNIT_TEST(BindOnPortRandom) {
        TVector<TBindResult> r = BindOnPort(0, false).second;
        UNIT_ASSERT_VALUES_EQUAL(size_t(2), r.size());

        for (TVector<TBindResult>::iterator i = r.begin(); i != r.end(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(i->Addr.GetPort(), GetSockPort(i->Socket->operator SOCKET()));
            UNIT_ASSERT(i->Addr.GetPort() > 0);
        }

        UNIT_ASSERT_VALUES_EQUAL(r.at(0).Addr.GetPort(), r.at(1).Addr.GetPort());
    }

    Y_UNIT_TEST(BindOnBusyPort) {
        auto r = BindOnPort(0, false);

        UNIT_ASSERT_EXCEPTION_CONTAINS(BindOnPort(r.first, false), TSystemError, "failed to bind on port " + ToString(r.first));
    }
}
