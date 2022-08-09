#pragma once

#include <optional>
#include <util/network/sock.h>

namespace NPG {

class TInet64StreamSocket: public TStreamSocket {
protected:
    TInet64StreamSocket(const TInet64StreamSocket& parent, SOCKET fd)
        : TStreamSocket(fd)
        , AF(parent.AF)
    {
    }

public:
    TInet64StreamSocket(int af = {}) {
        CreateSocket(af);
    }

    static bool IsIPv6(const TString& host) {
        if (host.find_first_not_of(":0123456789abcdef") != TString::npos) {
            return false;
        }
        if (std::count(host.begin(), host.end(), ':') < 2) {
            return false;
        }
        return true;
    }

    std::shared_ptr<ISockAddr> MakeAddress(const TString& address, int port) {
        if (!address) {
            if (AF == AF_INET6) {
                return std::make_shared<TSockAddrInet6>("::", port);
            } else {
                return std::make_shared<TSockAddrInet>(INADDR_ANY, port);
            }
        }
        if (IsIPv6(address)) {
            return std::make_shared<TSockAddrInet6>(address.data(), port);
        } else {
            return std::make_shared<TSockAddrInet>(address.data(), port);
        }
    }

    static std::shared_ptr<ISockAddr> MakeAddress(const sockaddr_storage& storage) {
        std::shared_ptr<ISockAddr> addr;
        switch (storage.ss_family) {
            case AF_INET:
                addr = std::make_shared<TSockAddrInet>();
                break;
            case AF_INET6:
                addr = std::make_shared<TSockAddrInet6>();
                break;
        }
        if (addr) {
            memcpy(addr->SockAddr(), &storage, addr->Size());
        }
        return addr;
    }

    std::optional<TInet64StreamSocket> Accept(std::shared_ptr<ISockAddr>& acceptedAddr) {
        sockaddr_storage addrStorage = {};
        socklen_t addrLen = sizeof(addrStorage);
        SOCKET s = accept((SOCKET)*this, reinterpret_cast<sockaddr*>(&addrStorage), &addrLen);
        if (s == INVALID_SOCKET) {
            return {};
        }
        acceptedAddr = MakeAddress(addrStorage);
        return TInet64StreamSocket(*this, s);
    }

protected:
    int AF = 0;

    void CreateSocket(int af) {
        SOCKET s;
        if (af == 0) {
            s = socket(AF = AF_INET6, SOCK_STREAM, 0);
            if (s < 0) {
                s = socket(AF = AF_INET, SOCK_STREAM, 0);
            }
        } else {
            s = socket(AF = af, SOCK_STREAM, 0);
        }
        if (AF == AF_INET6) {
            SetSockOpt(s, SOL_SOCKET, IPV6_V6ONLY, (int)false);
        }
        TSocketHolder sock(s);
        sock.Swap(*this);
    }
};

}
