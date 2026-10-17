#include <library/cpp/testing/unittest/registar.h>

#include <util/network/sock.h>

#include <library/cpp/testing/unittest/tests_data.h>

#include <ydb/core/testlib/test_client.h>

using namespace NKikimr;
using namespace NKikimr::Tests;

namespace {

bool ConnectTo(ui16 port) {
    const TInstant deadline = TInstant::Now() + TDuration::Seconds(60);
    while (TInstant::Now() < deadline) {
        TInet6StreamSocket socket;
        socket.CheckSock();
        TSockAddrInet6 address("::1", port);
        if (socket.Connect(&address) == 0) {
            return true;
        }
        Sleep(TDuration::MilliSeconds(200));
    }
    return false;
}

} // namespace

Y_UNIT_TEST_SUITE(SqlListener) {
    Y_UNIT_TEST(PortAcceptsConnection) {
        TPortManager ports;
        const ui16 busPort = ports.GetTcpPort();
        const ui16 sqlPort = ports.GetTcpPort();

        TServerSettings settings(busPort);
        settings.AppConfig->MutableSqlConfig()->SetEnable(true);
        settings.AppConfig->MutableSqlConfig()->SetListeningPort(sqlPort);
        settings.AppConfig->MutableSqlConfig()->SetListeningAddress("::");

        TServer server(settings);
        UNIT_ASSERT_C(ConnectTo(sqlPort), "sql listener did not accept a connection on port " << sqlPort);
    }
}
