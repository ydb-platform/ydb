#include "network.h"

#include <util/generic/maybe.h>
#include <util/generic/ptr.h>
#include <util/network/init.h>
#include <util/network/socket.h>
#include <util/system/platform.h>

using namespace NBus;
using namespace NBus::NPrivate;

namespace {
    TBindResult BindOnPortProto(int port, int af, bool reusePort) {
        Y_ABORT_UNLESS(af == AF_INET || af == AF_INET6, "wrong af");

        SOCKET fd = ::socket(af, SOCK_STREAM, 0);
        if (fd == INVALID_SOCKET) {
            ythrow TSystemError() << "failed to create a socket";
        }

        int one = 1;
        int r1 = SetSockOpt(fd, SOL_SOCKET, SO_REUSEADDR, one);
        if (r1 < 0) {
            ythrow TSystemError() << "failed to setsockopt SO_REUSEADDR";
        }

#ifdef SO_REUSEPORT
        if (reusePort) {
            int r = SetSockOpt(fd, SOL_SOCKET, SO_REUSEPORT, one);
            if (r < 0) {
                ythrow TSystemError() << "failed to setsockopt SO_REUSEPORT";
            }
        }
#else
        Y_UNUSED(reusePort);
#endif

        THolder<TOpaqueAddr> addr(new TOpaqueAddr);
        sockaddr* sa = addr->MutableAddr();
        sa->sa_family = af;
        socklen_t len;
        if (af == AF_INET) {
            len = sizeof(sockaddr_in);
            ((sockaddr_in*)sa)->sin_port = HostToInet((ui16)port);
            ((sockaddr_in*)sa)->sin_addr.s_addr = INADDR_ANY;
        } else {
            len = sizeof(sockaddr_in6);
            ((sockaddr_in6*)sa)->sin6_port = HostToInet((ui16)port);
        }

        if (af == AF_INET6) {
            FixIPv6ListenSocket(fd);
        }

        int r2 = ::bind(fd, sa, len);
        if (r2 < 0) {
            ythrow TSystemError() << "failed to bind on port " << port;
        }

        int rsn = ::getsockname(fd, addr->MutableAddr(), addr->LenPtr());
        if (rsn < 0) {
            ythrow TSystemError() << "failed to getsockname";
        }

        int r3 = ::listen(fd, 50);
        if (r3 < 0) {
            ythrow TSystemError() << "listen failed";
        }

        TBindResult r;
        r.Socket.Reset(new TSocketHolder(fd));
        r.Addr = TNetAddr(addr.Release());
        return r;
    }

    TMaybe<TBindResult> TryBindOnPortProto(int port, int af, bool reusePort) {
        try {
            return {BindOnPortProto(port, af, reusePort)};
        } catch (const TSystemError&) {
            return {};
        }
    }

    std::pair<unsigned, TVector<TBindResult>> AggregateBindResults(TBindResult&& r1, TBindResult&& r2) {
        Y_ABORT_UNLESS(r1.Addr.GetPort() == r2.Addr.GetPort(), "internal");
        std::pair<unsigned, TVector<TBindResult>> r;
        r.second.reserve(2);

        r.first = r1.Addr.GetPort();
        r.second.emplace_back(std::move(r1));
        r.second.emplace_back(std::move(r2));
        return r;
    }
}

std::pair<unsigned, TVector<TBindResult>> NBus::BindOnPort(int port, bool reusePort) {
    std::pair<unsigned, TVector<TBindResult>> r;
    r.second.reserve(2);

    if (port != 0) {
        return AggregateBindResults(BindOnPortProto(port, AF_INET, reusePort),
                                    BindOnPortProto(port, AF_INET6, reusePort));
    }

    // use nothrow versions in cycle
    for (int i = 0; i < 1000; ++i) {
        TMaybe<TBindResult> in4 = TryBindOnPortProto(0, AF_INET, reusePort);
        if (!in4) {
            continue;
        }

        TMaybe<TBindResult> in6 = TryBindOnPortProto(in4->Addr.GetPort(), AF_INET6, reusePort);
        if (!in6) {
            continue;
        }

        return AggregateBindResults(std::move(*in4), std::move(*in6));
    }

    TBindResult in4 = BindOnPortProto(0, AF_INET, reusePort);
    TBindResult in6 = BindOnPortProto(in4.Addr.GetPort(), AF_INET6, reusePort);
    return AggregateBindResults(std::move(in4), std::move(in6));
}

void NBus::NPrivate::SetSockOptTcpCork(SOCKET s, bool value) {
#ifdef _linux_
    CheckedSetSockOpt(s, IPPROTO_TCP, TCP_CORK, (int)value, "TCP_CORK");
#else
    Y_UNUSED(s);
    Y_UNUSED(value);
#endif
}

ssize_t NBus::NPrivate::SocketSend(SOCKET s, TArrayRef<const char> data) {
    int flags = 0;
#if defined(_linux_) || defined(_freebsd_)
    flags |= MSG_NOSIGNAL;
#endif
    ssize_t r = ::send(s, data.data(), data.size(), flags);
    if (r < 0) {
        Y_ABORT_UNLESS(LastSystemError() != EBADF, "bad fd");
    }
    return r;
}

ssize_t NBus::NPrivate::SocketRecv(SOCKET s, TArrayRef<char> buffer) {
    int flags = 0;
#if defined(_linux_) || defined(_freebsd_)
    flags |= MSG_NOSIGNAL;
#endif
    ssize_t r = ::recv(s, buffer.data(), buffer.size(), flags);
    if (r < 0) {
        Y_ABORT_UNLESS(LastSystemError() != EBADF, "bad fd");
    }
    return r;
}
