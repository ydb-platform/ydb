#pragma once

#include <library/cpp/logger/all.h>

#include <util/generic/buffer.h>
#include <util/generic/map.h>
#include <util/generic/vector.h>
#include <util/network/address.h>
#include <util/network/ip.h>
#include <util/network/socket.h>
#include <util/system/mutex.h>
#include <util/system/yassert.h>

#include <cerrno>
#include <util/generic/noncopyable.h>

class TAddrList: public TVector<NAddr::IRemoteAddrRef> {
private:
    using TBase = TVector<NAddr::IRemoteAddrRef>;

public:
    //msvc doesn't support base class constructor inheritance
    TAddrList() = default;

    template <typename T>
    TAddrList(T&& arg)
        : TBase(std::forward<T>(arg))
    {
    }

    template <typename T1, typename T2>
    TAddrList(T1&& arg1, T2&& arg2)
        : TBase(std::forward<T1>(arg1), std::forward<T2>(arg2))
    {
    }

    TAddrList(std::initializer_list<NAddr::IRemoteAddrRef> list)
        : TBase(list)
    {
    }

    static TAddrList MakeV4Addr(ui32 ip, TIpPort port) {
        return TAddrList({new NAddr::TIPv4Addr(TIpAddress(htonl(ip), htons(port)))});
    }

    std::pair<ui32, TIpPort> GetV4Addr() const {
        for (const auto& addrRef : *this) {
            const sockaddr* sa = addrRef->Addr();
            if (sa->sa_family == AF_INET) {
                const sockaddr_in* sin = reinterpret_cast<const sockaddr_in*>(sa);
                return std::make_pair(ntohl(sin->sin_addr.s_addr), ntohs(sin->sin_port));
            }
        }
        return std::make_pair(0, 0);
    }
};

class TSimpleSocketHandler {
public:
    TSimpleSocketHandler() = default;

    int Good() const {
        return static_cast<bool>(Socket);
    }

    int Connect(const TAddrList& addrs, TDuration timeout) {
        try {
            for (const auto& item : addrs) {
                const sockaddr* sa = item->Addr();
                TSocketHolder s(socket(sa->sa_family, SOCK_STREAM, 0));
                if (s.Closed()) {
                    continue;
                }

#ifndef WIN32
                if (fcntl(s, F_SETFD, FD_CLOEXEC)) // no inherit on fork()/exec()
                    return errno ? errno : EBADF;
#endif
                if (connect(s, sa, item->Len())) {
                    s.Close();
                    continue;
                }

                Socket.Reset(new TSocket(s.Release()));
                Socket->SetSocketTimeout(timeout.Seconds(), timeout.MilliSecondsOfSecond());
                Socket->SetZeroLinger();
                Socket->SetKeepAlive(true);
                return 0;
            }
        } catch (...) {
            return EBADF;
        }
        return errno ? errno : EBADF;
    }

    void Disconnect() {
        if (!Socket)
            return;
        Socket->ShutDown(SHUT_RDWR);
        Socket.Destroy();
    }

    void SetSocket(SOCKET fd) {
        Socket.Reset(new TSocket(fd));
    }

    void shutdown() {
        Socket->ShutDown(SHUT_WR);
    }

    int send(const void* message, size_t messlen) {
        return ((ssize_t)messlen == Socket->Send(message, messlen));
    }

    int peek() {
        char buf[1];
        return (1 == recv(*Socket, buf, 1, MSG_PEEK));
    }

    ssize_t read(void* buffer, size_t buflen) {
        return Socket->Recv(buffer, buflen);
    }

    THolder<TSocket> PickOutSocket() {
        return std::move(Socket);
    }

protected:
    THolder<TSocket> Socket;
};
