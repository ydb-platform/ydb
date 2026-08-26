#include <ydb/core/ymq/http/peer_source_address.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <util/network/sock.h>
#include <util/network/socket.h>

using namespace NKikimr::NSQS;

namespace {

    template <class TAddr, class TSock>
    TString PeerAddressAfterConnect(const char* ip) {
        TAddr servAddr(ip, 0);
        TSock listener;
        TSock client;
        TSock accepted;
        listener.CheckSock();
        client.CheckSock();

        int yes = 1;
        CheckedSetSockOpt(listener, SOL_SOCKET, SO_REUSEADDR, yes, "SO_REUSEADDR");
        TBaseSocket::Check(listener.Bind(&servAddr), "bind");
        TBaseSocket::Check(listener.Listen(1), "listen");
        TBaseSocket::Check(client.Connect(&servAddr), "connect");

        TAddr peer;
        TBaseSocket::Check(listener.Accept(&accepted, &peer), "accept");
        return PeerSourceAddressFromSocket(accepted);
    }

} // namespace

TEST(PeerSourceAddressFromSocket, IPv4Loopback) {
    EXPECT_EQ((PeerAddressAfterConnect<TSockAddrInet, TInetStreamSocket>("127.0.0.1")), "127.0.0.1");
}

TEST(PeerSourceAddressFromSocket, IPv6Loopback) {
    try {
        EXPECT_EQ((PeerAddressAfterConnect<TSockAddrInet6, TInet6StreamSocket>("::1")), "::1");
    } catch (const TSystemError&) {
        GTEST_SKIP() << "IPv6 loopback is not available";
    }
}

TEST(PeerSourceAddressFromSocket, InvalidSocket) {
    EXPECT_EQ(PeerSourceAddressFromSocket(INVALID_SOCKET), "unknown");
}

TEST(PeerSourceAddressFromSocket, UnixDomainIsUnknown) {
    SOCKET fds[2];
    ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
    TSocketHolder a(fds[0]);
    TSocketHolder b(fds[1]);
    EXPECT_EQ(PeerSourceAddressFromSocket(a), "unknown");
}
