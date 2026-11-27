#include "interconnect_address.h"

#include <util/string/cast.h>
#include <util/system/file.h>

namespace NInterconnect {
    TAddress::TAddress() noexcept {
        memset(&Addr, 0, sizeof(Addr));
    }

    TAddress::TAddress(NAddr::IRemoteAddr& addr) noexcept {
        socklen_t len = addr.Len();
        Y_ABORT_UNLESS(len <= sizeof(Addr));
        memcpy(&Addr.Generic, addr.Addr(), len);
    }

    int TAddress::GetFamily() const noexcept {
        return Addr.Generic.sa_family;
    }

    socklen_t TAddress::Size() const noexcept {
        switch (Addr.Generic.sa_family) {
            case AF_INET6:
                return sizeof(sockaddr_in6);
            case AF_INET:
                return sizeof(sockaddr_in);
            default:
                return 0;
        }
    }

    sockaddr* TAddress::SockAddr() noexcept {
        return &Addr.Generic;
    }

    const sockaddr* TAddress::SockAddr() const noexcept {
        return &Addr.Generic;
    }

    ui16 TAddress::GetPort() const noexcept {
        switch (Addr.Generic.sa_family) {
            case AF_INET6:
                return ntohs(Addr.Ipv6.sin6_port);
            case AF_INET:
                return ntohs(Addr.Ipv4.sin_port);
            default:
                return 0;
        }
    }

    bool TAddress::IsLinkLocal() const noexcept {
        switch (Addr.Generic.sa_family) {
            case AF_INET:
                // Check whether the IPv4 address is link-local 169.254.0.0/16 (RFC3927)
                return (ntohl(Addr.Ipv4.sin_addr.s_addr) & 0xffff0000u) == 0xa9fe0000u;
            case AF_INET6:
                return IN6_IS_ADDR_LINKLOCAL(&Addr.Ipv6.sin6_addr);
            default:
                return false;
        }
    }

    TString TAddress::ToString() const {
        return GetAddress() + ":" + ::ToString(GetPort());
    }

    TAddress::TAddress(const char* addr, ui16 port) noexcept {
        memset(&Addr, 0, sizeof(Addr));
        if (inet_pton(Addr.Ipv6.sin6_family = AF_INET6, addr, &Addr.Ipv6.sin6_addr) > 0) {
            Addr.Ipv6.sin6_port = htons(port);
        } else if (inet_pton(Addr.Ipv4.sin_family = AF_INET, addr, &Addr.Ipv4.sin_addr) > 0) {
            Addr.Ipv4.sin_port = htons(port);
        }
    }

    TAddress::TAddress(const TString& addr, ui16 port) noexcept
        : TAddress(addr.data(), port)
    {}

    TAddress::TAddress(in_addr addr, ui16 port) noexcept {
        Addr.Ipv4.sin_family = AF_INET;
        Addr.Ipv4.sin_port = htons(port);
        Addr.Ipv4.sin_addr = addr;
    }

    TAddress::TAddress(in6_addr addr, ui16 port) noexcept {
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
