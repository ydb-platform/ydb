#include <library/cpp/testing/unittest/registar.h>

#include <util/network/sock.h>

#include <ydb/core/testlib/test_client.h>

#include <poll.h>

using namespace NKikimr;
using namespace NKikimr::Tests;

namespace {

void AppendBe16(TString& out, ui16 value) {
    char bytes[2] = {static_cast<char>((value >> 8) & 0xff), static_cast<char>(value & 0xff)};
    out.append(bytes, 2);
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

TString CString(TStringBuf value) {
    TString out(value);
    out.push_back('\0');
    return out;
}

TString Message(char type, TString payload) {
    TString out;
    out.push_back(type);
    AppendBe32(out, static_cast<ui32>(4 + payload.size()));
    out.append(payload);
    return out;
}

TString Startup(const TString& database) {
    TString body;
    AppendBe32(body, 196608);
    body.append(CString("user"));
    body.append(CString("test"));
    body.append(CString("database"));
    body.append(CString(database));
    body.push_back('\0');
    TString message;
    AppendBe32(message, static_cast<ui32>(4 + body.size()));
    message.append(body);
    return message;
}

TString Query(TStringBuf sql) {
    return Message('Q', CString(sql));
}

TString Parse(TStringBuf name, TStringBuf sql) {
    TString body = CString(name);
    body.append(CString(sql));
    AppendBe16(body, 0);
    return Message('P', body);
}

TString Bind(TStringBuf portal, TStringBuf statement, const TVector<TString>& params) {
    TString body = CString(portal);
    body.append(CString(statement));
    AppendBe16(body, 0);
    AppendBe16(body, static_cast<ui16>(params.size()));
    for (const TString& param : params) {
        AppendBe32(body, static_cast<ui32>(param.size()));
        body.append(param);
    }
    AppendBe16(body, 0);
    return Message('B', body);
}

TString Execute(TStringBuf portal) {
    TString body = CString(portal);
    AppendBe32(body, 0);
    return Message('E', body);
}

TString SyncMessage() {
    return Message('S', {});
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

bool HasReady(TStringBuf data) {
    const char ready[] = {'Z', 0, 0, 0, 5, 'I'};
    return data.find(TStringBuf(ready, sizeof(ready))) != TStringBuf::npos;
}

TString ReadUntilReady(TInet6StreamSocket& socket) {
    TString data;
    const TInstant deadline = TInstant::Now() + TDuration::Seconds(15);
    while (TInstant::Now() < deadline) {
        pollfd pfd;
        pfd.fd = static_cast<int>(static_cast<SOCKET>(socket));
        pfd.events = POLLIN;
        pfd.revents = 0;
        if (poll(&pfd, 1, 200) <= 0) {
            continue;
        }
        char buf[1024];
        const ssize_t received = socket.Recv(buf, sizeof(buf));
        if (received > 0) {
            data.append(buf, received);
            if (HasReady(data)) {
                return data;
            }
            continue;
        }
        break;
    }
    return data;
}

} // namespace

Y_UNIT_TEST_SUITE(ExtendedQuery) {
    Y_UNIT_TEST(ParameterIsReusedAndAnErrorKeepsTheConnection) {
        TPortManager ports;
        const ui16 sqlPort = ports.GetTcpPort();
        TServerSettings settings(ports.GetTcpPort());
        settings.AppConfig->MutableSqlConfig()->SetEnable(true);
        settings.AppConfig->MutableSqlConfig()->SetListeningPort(sqlPort);
        settings.AppConfig->MutableSqlConfig()->SetListeningAddress("::");
        TServer server(settings);
        TClient client(settings);
        client.InitRootScheme();
        const TString database = "/" + settings.DomainName;

        auto socket = Open(sqlPort);
        UNIT_ASSERT(socket);
        UNIT_ASSERT(SendAll(*socket, Startup(database)));
        UNIT_ASSERT(HasReady(ReadUntilReady(*socket)));

        UNIT_ASSERT(SendAll(*socket, Query("CREATE TABLE items (id int8 PRIMARY KEY, note text)")));
        UNIT_ASSERT(HasReady(ReadUntilReady(*socket)));
        UNIT_ASSERT(SendAll(*socket, Query("INSERT INTO items (id, note) VALUES (7, 'hello')")));
        const TString inserted = ReadUntilReady(*socket);
        UNIT_ASSERT_C(inserted.Contains("INSERT 0 1"), inserted);

        TString first;
        first.append(Parse("select_note", "SELECT note FROM items WHERE id = $1"));
        first.append(Bind("portal", "select_note", {"7"}));
        first.append(Execute("portal"));
        first.append(SyncMessage());
        UNIT_ASSERT(SendAll(*socket, first));
        const TString selected = ReadUntilReady(*socket);
        UNIT_ASSERT_C(selected.Contains("hello"), selected);
        UNIT_ASSERT(HasReady(selected));

        UNIT_ASSERT(SendAll(*socket, Execute("portal") + SyncMessage()));
        const TString again = ReadUntilReady(*socket);
        UNIT_ASSERT_C(again.Contains("hello"), again);

        TString broken;
        broken.append(Parse("missing", "SELECT note FROM missing WHERE id = $1"));
        broken.append(Bind("bad", "missing", {"7"}));
        broken.append(Execute("bad"));
        broken.append(SyncMessage());
        UNIT_ASSERT(SendAll(*socket, broken));
        const TString failed = ReadUntilReady(*socket);
        UNIT_ASSERT_C(failed.Contains("42P01"), failed);
        UNIT_ASSERT(HasReady(failed));

        UNIT_ASSERT(SendAll(*socket, Execute("portal") + SyncMessage()));
        const TString still = ReadUntilReady(*socket);
        UNIT_ASSERT_C(still.Contains("hello"), still);

        TString write;
        write.append(Parse("insert_item", "INSERT INTO items (id, note) VALUES ($1, $2)"));
        write.append(Bind("insert_portal", "insert_item", {"9", "extra"}));
        write.append(Execute("insert_portal"));
        write.append(SyncMessage());
        UNIT_ASSERT(SendAll(*socket, write));
        const TString wrote = ReadUntilReady(*socket);
        UNIT_ASSERT_C(wrote.Contains("INSERT 0 1"), wrote);

        UNIT_ASSERT(SendAll(*socket, Bind("portal", "select_note", {"9"}) + Execute("portal") + SyncMessage()));
        const TString extra = ReadUntilReady(*socket);
        UNIT_ASSERT_C(extra.Contains("extra"), extra);
    }
}
