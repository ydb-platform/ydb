#pragma once
#include <util/network/sock.h>
#include "http.h"

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

    std::shared_ptr<ISockAddr> MakeAddress(const TString& address, int port) {
        if (!address) {
            if (AF == AF_INET6) {
                return std::make_shared<TSockAddrInet6>("::", port);
            } else {
                return std::make_shared<TSockAddrInet>(INADDR_ANY, port);
            }
        }
        if (NHttp::IsIPv6(address)) {
            return std::make_shared<TSockAddrInet6>(address.data(), port);
        } else if (NHttp::IsIPv4(address)) {
            return std::make_shared<TSockAddrInet>(address.data(), port);
        }
        struct addrinfo hints = {
            .ai_flags = AI_PASSIVE,
            .ai_family = AF,
            .ai_socktype = SOCK_STREAM,
        };
        struct addrinfo* gai_res = nullptr;
        int gai_ret = getaddrinfo(address.data(), nullptr, &hints, &gai_res);
        std::shared_ptr<ISockAddr> result;
        if (gai_ret == 0 && gai_res->ai_addr) {
            switch (gai_res->ai_addr->sa_family) {
                case AF_INET6: {
                        std::shared_ptr<TSockAddrInet6> resultIp6 = std::make_shared<TSockAddrInet6>();
                        if (resultIp6->Size() >= gai_res->ai_addrlen) {
                            memcpy(resultIp6->SockAddr(), gai_res->ai_addr, gai_res->ai_addrlen);
                            resultIp6->SetPort(port);
                            result = std::move(resultIp6);
                        }
                    }
                    break;
                case AF_INET: {
                        std::shared_ptr<TSockAddrInet> resultIp4 = std::make_shared<TSockAddrInet>();
                        if (resultIp4->Size() >= gai_res->ai_addrlen) {
                            memcpy(resultIp4->SockAddr(), gai_res->ai_addr, gai_res->ai_addrlen);
                            resultIp4->SetPort(port);
                            result = std::move(resultIp4);
                        }
                    }
                    break;
            }
        }
        if (gai_res) {
            freeaddrinfo(gai_res);
        }
        if (result) {
            return result;
        }
        throw yexception() << "Unable to resolve address " << address;
    }

    static int GuessAddressFamily(const TString& address) {
        if (!address) {
            return 0;
        }
        if (NHttp::IsIPv6(address)) {
            return AF_INET6;
        } else if (NHttp::IsIPv4(address)) {
            return AF_INET;
        }
        struct addrinfo hints = {
            .ai_flags = AI_PASSIVE,
            .ai_family = 0,
            .ai_socktype = SOCK_STREAM,
        };
        int result = 0;
        struct addrinfo* gai_res = nullptr;
        int gai_ret = getaddrinfo(address.data(), nullptr, &hints, &gai_res);
        if (gai_ret == 0 && gai_res->ai_addr) {
            switch (gai_res->ai_addr->sa_family) {
                case AF_INET:
                case AF_INET6:
                    result = gai_res->ai_addr->sa_family;
                    break;
            }
        }
        if (gai_res) {
            freeaddrinfo(gai_res);
        }
        return result;
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
    int AF = AF_UNSPEC;

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
