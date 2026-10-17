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

TString ReadAtLeast(TInet6StreamSocket& socket, size_t bytes) {
    TString data;
    const TInstant deadline = TInstant::Now() + TDuration::Seconds(5);
    while (data.size() < bytes && TInstant::Now() < deadline) {
        pollfd pfd;
        pfd.fd = static_cast<int>(static_cast<SOCKET>(socket));
        pfd.events = POLLIN;
        pfd.revents = 0;
        if (poll(&pfd, 1, 200) <= 0) {
            continue;
        }
        char buf[64];
        const ssize_t received = socket.Recv(buf, sizeof(buf));
        if (received > 0) {
            data.append(buf, received);
            continue;
        }
        break;
    }
    return data;
}

TString QueryMessage(TStringBuf sql) {
    TString payload(sql);
    payload.push_back('\0');
    TString message;
    message.push_back('Q');
    AppendBe32(message, static_cast<ui32>(4 + payload.size()));
    message.append(payload);
    return message;
}

TString SslRequest() {
    TString message;
    AppendBe32(message, 8);
    AppendBe32(message, 80877103);
    return message;
}

TString CancelRequest() {
    TString message;
    AppendBe32(message, 16);
    AppendBe32(message, 80877102);
    AppendBe32(message, 1);
    AppendBe32(message, 1);
    return message;
}

TServerSettings SqlSettings(ui16 busPort, ui16 sqlPort) {
    TServerSettings settings(busPort);
    settings.AppConfig->MutableSqlConfig()->SetEnable(true);
    settings.AppConfig->MutableSqlConfig()->SetListeningPort(sqlPort);
    settings.AppConfig->MutableSqlConfig()->SetListeningAddress("::");
    return settings;
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

    Y_UNIT_TEST(UnsupportedQueryKeepsSession) {
        TPortManager ports;
        const ui16 sqlPort = ports.GetTcpPort();
        TServer server(SqlSettings(ports.GetTcpPort(), sqlPort));
        const TString database = "/" + server.GetSettings().DomainName;

        auto session = Open(sqlPort);
        UNIT_ASSERT(session);
        UNIT_ASSERT(SendAll(*session, StartupMessage(database)));
        UNIT_ASSERT(HasReadyForQuery(ReadReply(*session)));

        for (int query = 0; query < 2; ++query) {
            UNIT_ASSERT(SendAll(*session, QueryMessage("SELECT 1")));
            const TString reply = ReadReply(*session);
            UNIT_ASSERT_C(reply.Contains("0A000"), "query " << query << " did not return SQLSTATE 0A000");
            UNIT_ASSERT_C(reply.Contains("not supported"), "query " << query << " did not return the unsupported text");
            UNIT_ASSERT_C(HasReadyForQuery(reply), "query " << query << " did not return ReadyForQuery");
        }

        auto extended = Open(sqlPort);
        UNIT_ASSERT(extended);
        UNIT_ASSERT(SendAll(*extended, StartupMessage(database)));
        UNIT_ASSERT(HasReadyForQuery(ReadReply(*extended)));
        TString parse;
        parse.push_back('P');
        AppendBe32(parse, 4);
        UNIT_ASSERT(SendAll(*extended, parse));
        const TString extendedReply = ReadReply(*extended);
        UNIT_ASSERT(extendedReply.Contains("0A000"));
        UNIT_ASSERT(HasReadyForQuery(extendedReply));

        auto ssl = Open(sqlPort);
        UNIT_ASSERT(ssl);
        UNIT_ASSERT(SendAll(*ssl, SslRequest()));
        const TString sslReply = ReadAtLeast(*ssl, 1);
        UNIT_ASSERT_C(!sslReply.empty() && sslReply[0] == 'N', "SSL request was not refused with N");
        UNIT_ASSERT(SendAll(*ssl, StartupMessage(database)));
        UNIT_ASSERT(HasReadyForQuery(ReadReply(*ssl)));

        auto cancel = Open(sqlPort);
        UNIT_ASSERT(cancel);
        UNIT_ASSERT(SendAll(*cancel, CancelRequest()));
        const TString cancelReply = ReadReply(*cancel);
        UNIT_ASSERT(!HasReadyForQuery(cancelReply));

        UNIT_ASSERT(SendAll(*session, QueryMessage("SELECT 2")));
        const TString afterCancel = ReadReply(*session);
        UNIT_ASSERT(afterCancel.Contains("0A000"));
        UNIT_ASSERT(HasReadyForQuery(afterCancel));

        auto again = Open(sqlPort);
        UNIT_ASSERT_C(again, "listener died after Cancel");
        UNIT_ASSERT(SendAll(*again, StartupMessage(database)));
        UNIT_ASSERT(HasReadyForQuery(ReadReply(*again)));
    }

    // psql 16.9 (--no-psqlrc, interactive \\q) and PostgreSQL 18 libpq send no SQL
    // before the prompt. The stub answers the catalog reads named for that handshake.
    Y_UNIT_TEST(CatalogStartupQueries) {
        TPortManager ports;
        const ui16 sqlPort = ports.GetTcpPort();
        TServer server(SqlSettings(ports.GetTcpPort(), sqlPort));
        const TString database = "/" + server.GetSettings().DomainName;

        auto session = Open(sqlPort);
        UNIT_ASSERT(session);
        UNIT_ASSERT(SendAll(*session, StartupMessage(database)));
        UNIT_ASSERT(HasReadyForQuery(ReadReply(*session)));

        UNIT_ASSERT(SendAll(*session, QueryMessage("SELECT version()")));
        const TString version = ReadReply(*session);
        UNIT_ASSERT_C(version.Contains("PostgreSQL 18.0"), version);
        UNIT_ASSERT(HasReadyForQuery(version));
        UNIT_ASSERT(!version.Contains("0A000"));

        UNIT_ASSERT(SendAll(*session, QueryMessage("SELECT oid, typname FROM pg_catalog.pg_type")));
        const TString types = ReadReply(*session);
        UNIT_ASSERT_C(types.Contains("bool"), types);
        UNIT_ASSERT(types.Contains("int8"));
        UNIT_ASSERT(HasReadyForQuery(types));
        UNIT_ASSERT(!types.Contains("0A000"));

        UNIT_ASSERT(SendAll(*session, QueryMessage("SELECT * FROM pg_catalog.pg_class")));
        const TString extra = ReadReply(*session);
        UNIT_ASSERT(extra.Contains("0A000"));
        UNIT_ASSERT(extra.Contains("not supported"));
        UNIT_ASSERT(HasReadyForQuery(extra));
    }
}
