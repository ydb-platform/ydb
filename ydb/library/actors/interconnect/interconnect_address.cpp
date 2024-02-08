#include "interconnect_address.h"

#include <util/string/cast.h>
#include <util/system/file.h>

#if defined(_linux_)
#include <sys/un.h>
#include <sys/stat.h>
#endif

namespace NInterconnect {
    TAddress::TAddress() {
        memset(&Addr, 0, sizeof(Addr));
    }

    TAddress::TAddress(NAddr::IRemoteAddr& addr) {
        socklen_t len = addr.Len();
        Y_ABORT_UNLESS(len <= sizeof(Addr));
        memcpy(&Addr.Generic, addr.Addr(), len);
    }

    int TAddress::GetFamily() const {
        return Addr.Generic.sa_family;
    }

    socklen_t TAddress::Size() const {
        switch (Addr.Generic.sa_family) {
            case AF_INET6:
                return sizeof(sockaddr_in6);
            case AF_INET:
                return sizeof(sockaddr_in);
            default:
                return 0;
        }
    }

    sockaddr* TAddress::SockAddr() {
        return &Addr.Generic;
    }

    const sockaddr* TAddress::SockAddr() const {
        return &Addr.Generic;
    }

    ui16 TAddress::GetPort() const {
        switch (Addr.Generic.sa_family) {
            case AF_INET6:
                return ntohs(Addr.Ipv6.sin6_port);
            case AF_INET:
                return ntohs(Addr.Ipv4.sin_port);
            default:
                return 0;
        }
    }

    TString TAddress::ToString() const {
        return GetAddress() + ":" + ::ToString(GetPort());
    }

    TAddress::TAddress(const char* addr, ui16 port) {
        memset(&Addr, 0, sizeof(Addr));
        if (inet_pton(Addr.Ipv6.sin6_family = AF_INET6, addr, &Addr.Ipv6.sin6_addr) > 0) {
            Addr.Ipv6.sin6_port = htons(port);
        } else if (inet_pton(Addr.Ipv4.sin_family = AF_INET, addr, &Addr.Ipv4.sin_addr) > 0) {
            Addr.Ipv4.sin_port = htons(port);
        }
    }

    TAddress::TAddress(const TString& addr, ui16 port)
        : TAddress(addr.data(), port)
    {}

    TAddress::TAddress(in_addr addr, ui16 port) {
        Addr.Ipv4.sin_family = AF_INET;
        Addr.Ipv4.sin_port = htons(port);
        Addr.Ipv4.sin_addr = addr;
    }

    TAddress::TAddress(in6_addr addr, ui16 port) {
        Addr.Ipv6.sin6_family = AF_INET6;
        Addr.Ipv6.sin6_port = htons(port);
        Addr.Ipv6.sin6_addr = addr;
    }

    TString TAddress::GetAddress() const {
        const void *src;
        socklen_t size;

        switch (Addr.Generic.sa_family) {
            case AF_INET6:
                std::tie(src, size) = std::make_tuple(&Addr.Ipv6.sin6_addr, INET6_ADDRSTRLEN);
                break;

            case AF_INET:
                std::tie(src, size) = std::make_tuple(&Addr.Ipv4.sin_addr, INET_ADDRSTRLEN);
                break;

            default:
                return TString();
        }

        char *buffer = static_cast<char*>(alloca(size));
        const char *p = inet_ntop(Addr.Generic.sa_family, const_cast<void*>(src), buffer, size);
        return p ? TString(p) : TString();
    }
}
