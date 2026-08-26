#include <ydb/library/net/source_address.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <util/network/sock.h>
#include <util/network/socket.h>

using namespace NKikimr::NNet;

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

    sockaddr_in MakeIPv4(const char* ip) {
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        Y_ABORT_UNLESS(inet_pton(AF_INET, ip, &addr.sin_addr) == 1);
        return addr;
    }

    sockaddr_in6 MakeIPv6(const char* ip) {
        sockaddr_in6 addr{};
        addr.sin6_family = AF_INET6;
        Y_ABORT_UNLESS(inet_pton(AF_INET6, ip, &addr.sin6_addr) == 1);
        return addr;
    }

} // namespace

TEST(FormatSourceAddress, NullIsUnknown) {
    EXPECT_EQ(FormatSourceAddress(nullptr), "unknown");
}

TEST(FormatSourceAddress, IPv4) {
    auto addr = MakeIPv4("192.0.2.1");
    EXPECT_EQ(FormatSourceAddress(reinterpret_cast<const sockaddr*>(&addr)), "192.0.2.1");
}

TEST(FormatSourceAddress, IPv4Loopback) {
    auto addr = MakeIPv4("127.0.0.1");
    EXPECT_EQ(FormatSourceAddress(reinterpret_cast<const sockaddr*>(&addr)), "127.0.0.1");
}

TEST(FormatSourceAddress, IPv6) {
    auto addr = MakeIPv6("2001:db8::1");
    EXPECT_EQ(FormatSourceAddress(reinterpret_cast<const sockaddr*>(&addr)), "2001:db8::1");
}

TEST(FormatSourceAddress, IPv6Loopback) {
    auto addr = MakeIPv6("::1");
    EXPECT_EQ(FormatSourceAddress(reinterpret_cast<const sockaddr*>(&addr)), "::1");
}

TEST(FormatSourceAddress, ISockAddrInetMatchesHttpProxyPath) {
    TSockAddrInet addr("203.0.113.42", 443);
    EXPECT_EQ(FormatSourceAddress(addr.SockAddr()), "203.0.113.42");
}

TEST(FormatSourceAddress, ISockAddrInet6MatchesHttpProxyPath) {
    TSockAddrInet6 addr("2001:db8::ff", 443);
    EXPECT_EQ(FormatSourceAddress(addr.SockAddr()), "2001:db8::ff");
}

TEST(FormatSourceAddress, UnknownFamilyIsUnknown) {
    sockaddr addr{};
    addr.sa_family = AF_UNIX;
    EXPECT_EQ(FormatSourceAddress(&addr), "unknown");
}

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
