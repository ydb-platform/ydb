#include <library/cpp/testing/unittest/registar.h>

#include <util/network/sock.h>

#include <library/cpp/testing/unittest/tests_data.h>

#include <ydb/core/testlib/test_client.h>

#include <poll.h>

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

THolder<TInet6StreamSocket> Open(ui16 port) {
    const TInstant deadline = TInstant::Now() + TDuration::Seconds(60);
    while (TInstant::Now() < deadline) {
        auto socket = MakeHolder<TInet6StreamSocket>();
        socket->CheckSock();
        TSockAddrInet6 address("::1", port);
        if (socket->Connect(&address) == 0) {
            return socket;
        }
        Sleep(TDuration::MilliSeconds(200));
    }
    return {};
}

void AppendBe32(TString& out, ui32 value) {
    char bytes[4] = {
        static_cast<char>((value >> 24) & 0xff),
        static_cast<char>((value >> 16) & 0xff),
        static_cast<char>((value >> 8) & 0xff),
        static_cast<char>(value & 0xff),
    };
    out.append(bytes, 4);
}

TString StartupMessage(const TString& database) {
    TString body;
    AppendBe32(body, 196608);
    body.append("user");
    body.push_back('\0');
    body.append("test");
    body.push_back('\0');
    body.append("database");
    body.push_back('\0');
    body.append(database);
    body.push_back('\0');
    body.push_back('\0');

    TString message;
    AppendBe32(message, static_cast<ui32>(4 + body.size()));
    message.append(body);
    return message;
}

bool SendAll(TInet6StreamSocket& socket, TStringBuf data) {
    while (!data.empty()) {
        const ssize_t written = socket.Send(data.data(), data.size());
        if (written <= 0) {
            return false;
        }
        data.Skip(written);
    }
    return true;
}

bool HasReadyForQuery(TStringBuf data) {
    const char ready[] = {'Z', 0, 0, 0, 5, 'I'};
    return data.find(TStringBuf(ready, sizeof(ready))) != TStringBuf::npos;
}

TString ReadReply(TInet6StreamSocket& socket) {
    TString data;
    const TInstant deadline = TInstant::Now() + TDuration::Seconds(10);
    while (TInstant::Now() < deadline) {
        pollfd pfd;
        pfd.fd = static_cast<int>(static_cast<SOCKET>(socket));
        pfd.events = POLLIN;
        pfd.revents = 0;
        const int ready = poll(&pfd, 1, 200);
        if (ready < 0) {
            break;
        }
        if (ready == 0) {
            continue;
        }
        char buf[1024];
        const ssize_t received = socket.Recv(buf, sizeof(buf));
        if (received > 0) {
            data.append(buf, received);
            if (HasReadyForQuery(data)) {
                return data;
            }
            continue;
        }
        break;
    }
    return data;
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

    Y_UNIT_TEST(ReadyForQueryRequiresKnownDatabase) {
        TPortManager ports;
        const ui16 busPort = ports.GetTcpPort();
        const ui16 sqlPort = ports.GetTcpPort();

        TServerSettings settings(busPort);
        settings.AppConfig->MutableSqlConfig()->SetEnable(true);
        settings.AppConfig->MutableSqlConfig()->SetListeningPort(sqlPort);
        settings.AppConfig->MutableSqlConfig()->SetListeningAddress("::");

        TServer server(settings);
        const TString database = "/" + settings.DomainName;

        {
            auto socket = Open(sqlPort);
            UNIT_ASSERT(socket);
            UNIT_ASSERT(SendAll(*socket, StartupMessage(database)));
            const TString reply = ReadReply(*socket);
            UNIT_ASSERT_C(HasReadyForQuery(reply), "known database " << database << " did not reach ReadyForQuery");
        }
        {
            auto socket = Open(sqlPort);
            UNIT_ASSERT(socket);
            UNIT_ASSERT(SendAll(*socket, StartupMessage("")));
            const TString reply = ReadReply(*socket);
            UNIT_ASSERT_C(!HasReadyForQuery(reply), "empty database reached ReadyForQuery");
            UNIT_ASSERT_C(reply.Contains("3D000"), "empty database did not return SQLSTATE 3D000");
        }
        {
            auto socket = Open(sqlPort);
            UNIT_ASSERT(socket);
            UNIT_ASSERT(SendAll(*socket, StartupMessage(database + "/missing")));
            const TString reply = ReadReply(*socket);
            UNIT_ASSERT_C(!HasReadyForQuery(reply), "unknown database reached ReadyForQuery");
            UNIT_ASSERT_C(reply.Contains("3D000"), "unknown database did not return SQLSTATE 3D000");
        }
    }
}
