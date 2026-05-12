#pragma once

#include <util/system/defaults.h>
#include <util/network/init.h>
#include <util/network/address.h>
#include <util/generic/string.h>

namespace NInterconnect {
    class TAddress {
    public:
        union {
            sockaddr Generic;
            sockaddr_in Ipv4;
            sockaddr_in6 Ipv6;
        } Addr;

        using TV6Addr = in6_addr;
        TAddress() noexcept;
        TAddress(const char* addr, ui16 port) noexcept;
        TAddress(const TString& addr, ui16 port) noexcept;
        TAddress(in_addr addr, ui16 port) noexcept;
        TAddress(in6_addr addr, ui16 port) noexcept;
        TAddress(NAddr::IRemoteAddr& addr) noexcept;
        int GetFamily() const noexcept;
        socklen_t Size() const noexcept;
        ::sockaddr* SockAddr() noexcept;
        const ::sockaddr* SockAddr() const noexcept;
        ui16 GetPort() const noexcept;
        TString GetAddress() const;
        TString ToString() const;

        static TAddress AnyIPv4(ui16 port) noexcept {
            TAddress res;
            res.Addr.Ipv4.sin_family = AF_INET;
            res.Addr.Ipv4.sin_port = htons(port);
            res.Addr.Ipv4.sin_addr.s_addr = htonl(INADDR_ANY);
            return res;
        }

        static TAddress AnyIPv6(ui16 port) noexcept {
            TAddress res;
            res.Addr.Ipv6.sin6_family = AF_INET6;
            res.Addr.Ipv6.sin6_port = htons(port);
            res.Addr.Ipv6.sin6_addr = in6addr_any;
            return res;
        }
    };
}
