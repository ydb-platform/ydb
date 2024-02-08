#include <ydb/library/actors/interconnect/poller_actor.h>
#include <ydb/library/actors/testlib/test_runtime.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/network/pair.h>
#include <util/network/socket.h>

using namespace NActors;

class TTestSocket: public TSharedDescriptor {
public:
    explicit TTestSocket(SOCKET fd)
        : Fd_(fd)
    {
    }

    int GetDescriptor() override {
        return Fd_;
    }

private:
    SOCKET Fd_;
};
using TTestSocketPtr = TIntrusivePtr<TTestSocket>;

// create pair of connected, non-blocking sockets
std::pair<TTestSocketPtr, TTestSocketPtr> NonBlockSockets() {
    SOCKET fds[2];
    SocketPair(fds);
    SetNonBlock(fds[0]);
    SetNonBlock(fds[1]);
    return {MakeIntrusive<TTestSocket>(fds[0]), MakeIntrusive<TTestSocket>(fds[1])};
}

std::pair<TTestSocketPtr, TTestSocketPtr> TcpSockets() {
    // create server (listening) socket
    SOCKET server = socket(AF_INET, SOCK_STREAM, 0);
    Y_ABORT_UNLESS(server != -1, "socket() failed with %s", strerror(errno));

    // bind it to local address with automatically picked port
    sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_port = 0;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (bind(server, (sockaddr*)&addr, sizeof(addr)) == -1) {
        Y_ABORT("bind() failed with %s", strerror(errno));
    } else if (listen(server, 1) == -1) {
        Y_ABORT("listen() failed with %s", strerror(errno));
    }

    // obtain local address for client
    socklen_t len = sizeof(addr);
    if (getsockname(server, (sockaddr*)&addr, &len) == -1) {
        Y_ABORT("getsockname() failed with %s", strerror(errno));
    }

    // create client socket
    SOCKET client = socket(AF_INET, SOCK_STREAM, 0);
    Y_ABORT_UNLESS(client != -1, "socket() failed with %s", strerror(errno));

    // connect to server
    if (connect(client, (sockaddr*)&addr, len) == -1) {
        Y_ABORT("connect() failed with %s", strerror(errno));
    }

    // accept connection from the other side
    SOCKET accepted = accept(server, nullptr, nullptr);
    Y_ABORT_UNLESS(accepted != -1, "accept() failed with %s", strerror(errno));

    // close server socket
    closesocket(server);

    return std::make_pair(MakeIntrusive<TTestSocket>(client), MakeIntrusive<TTestSocket>(accepted));
}

class TPollerActorTest: public TTestBase {
    UNIT_TEST_SUITE(TPollerActorTest);
        UNIT_TEST(Registration)
        UNIT_TEST(ReadNotification)
        UNIT_TEST(WriteNotification)
        UNIT_TEST(HangupNotification)
    UNIT_TEST_SUITE_END();

public:
    void SetUp() override {
        ActorSystem_ = MakeHolder<TTestActorRuntimeBase>();
        ActorSystem_->Initialize();

        PollerId_ = ActorSystem_->Register(CreatePollerActor());

        TDispatchOptions opts;
        opts.FinalEvents.emplace_back(TEvents::TSystem::Bootstrap, 1);
        ActorSystem_->DispatchEvents(opts);
    }

    void Registration() {
        auto [s1, s2] = NonBlockSockets();
        auto readerId = ActorSystem_->AllocateEdgeActor();
        auto writerId = ActorSystem_->AllocateEdgeActor();

        RegisterSocket(s1, readerId, writerId);

        // reader should receive event after socket registration
        TPollerToken::TPtr token;
        {
            auto ev = ActorSystem_->GrabEdgeEvent<TEvPollerRegisterResult>(readerId);
            token = ev->Get()->PollerToken;
        }

        // writer should receive event after socket registration
        {
            auto ev = ActorSystem_->GrabEdgeEvent<TEvPollerRegisterResult>(writerId);
            UNIT_ASSERT_EQUAL(token, ev->Get()->PollerToken);
        }
    }

    void ReadNotification() {
        auto [r, w] = NonBlockSockets();
        auto clientId = ActorSystem_->AllocateEdgeActor();
        RegisterSocket(r, clientId, {});

        // notification after registration
        TPollerToken::TPtr token;
        {
            auto ev = ActorSystem_->GrabEdgeEvent<TEvPollerRegisterResult>(clientId);
            token = ev->Get()->PollerToken;
        }

        char buf;

        // data not ready yet for read
        UNIT_ASSERT(read(r->GetDescriptor(), &buf, sizeof(buf)) == -1);
        UNIT_ASSERT(errno == EWOULDBLOCK);

        // request read poll
        token->Request(true, false);

        // write data
        UNIT_ASSERT(write(w->GetDescriptor(), "x", 1) == 1);

        // notification after socket become readable
        {
            auto ev = ActorSystem_->GrabEdgeEvent<TEvPollerReady>(clientId);
            UNIT_ASSERT_EQUAL(ev->Get()->Socket, r);
            UNIT_ASSERT(ev->Get()->Read);
            UNIT_ASSERT(!ev->Get()->Write);
        }

        // read data
        UNIT_ASSERT(read(r->GetDescriptor(), &buf, sizeof(buf)) == 1);
        UNIT_ASSERT_EQUAL('x', buf);

        // no more data to read
        UNIT_ASSERT(read(r->GetDescriptor(), &buf, sizeof(buf)) == -1);
        UNIT_ASSERT(errno == EWOULDBLOCK);
    }

    void WriteNotification() {
        auto [r, w] = TcpSockets();
        auto clientId = ActorSystem_->AllocateEdgeActor();
        SetNonBlock(w->GetDescriptor());
        RegisterSocket(w, TActorId{}, clientId);

        // notification after registration
        TPollerToken::TPtr token;
        {
            auto ev = ActorSystem_->GrabEdgeEvent<TEvPollerRegisterResult>(clientId);
            token = ev->Get()->PollerToken;
        }

        char buffer[4096];
        memset(buffer, 'x', sizeof(buffer));

        for (int i = 0; i < 1000; ++i) {
            // write as much as possible to send buffer
            ssize_t written = 0;
            for (;;) {
                ssize_t res = send(w->GetDescriptor(), buffer, sizeof(buffer), 0);
                if (res > 0) {
                    written += res;
                } else if (res == 0) {
                    UNIT_FAIL("unexpected zero return from send()");
                } else {
                    UNIT_ASSERT(res == -1);
                    if (errno == EINTR) {
                        continue;
                    } else if (errno == EWOULDBLOCK || errno == EAGAIN) {
                        token->Request(false, true);
                        break;
                    } else {
                        UNIT_FAIL("unexpected error from send()");
                    }
                }
            }
            Cerr << "written " << written << " bytes" << Endl;

            // read all written data from the read end
            for (;;) {
                char buffer[4096];
                ssize_t res = recv(r->GetDescriptor(), buffer, sizeof(buffer), 0);
                if (res > 0) {
                    UNIT_ASSERT(written >= res);
                    written -= res;
                    if (!written) {
                        break;
                    }
                } else if (res == 0) {
                    UNIT_FAIL("unexpected zero return from recv()");
                } else {
                    UNIT_ASSERT(res == -1);
                    if (errno == EINTR) {
                        continue;
                    } else {
                        UNIT_FAIL("unexpected error from recv()");
                    }
                }
            }

            // wait for notification after socket becomes writable again
            {
                auto ev = ActorSystem_->GrabEdgeEvent<TEvPollerReady>(clientId);
                UNIT_ASSERT_EQUAL(ev->Get()->Socket, w);
                UNIT_ASSERT(!ev->Get()->Read);
                UNIT_ASSERT(ev->Get()->Write);
            }
        }
    }

    void HangupNotification() {
        auto [r, w] = NonBlockSockets();
        auto clientId = ActorSystem_->AllocateEdgeActor();
        RegisterSocket(r, clientId, TActorId{});

        // notification after registration
        TPollerToken::TPtr token;
        {
            auto ev = ActorSystem_->GrabEdgeEvent<TEvPollerRegisterResult>(clientId);
            token = ev->Get()->PollerToken;
        }

        token->Request(true, false);
        ShutDown(w->GetDescriptor(), SHUT_RDWR);

        // notification after peer shuts down its socket
        {
            auto ev = ActorSystem_->GrabEdgeEvent<TEvPollerReady>(clientId);
            UNIT_ASSERT_EQUAL(ev->Get()->Socket, r);
            UNIT_ASSERT(ev->Get()->Read);
        }
    }

private:
    void RegisterSocket(TTestSocketPtr socket, TActorId readActorId, TActorId writeActorId) {
        auto ev = new TEvPollerRegister{socket, readActorId, writeActorId};
        ActorSystem_->Send(new IEventHandle(PollerId_, TActorId{}, ev));
    }

private:
    THolder<TTestActorRuntimeBase> ActorSystem_;
    TActorId PollerId_;
};

UNIT_TEST_SUITE_REGISTRATION(TPollerActorTest);
