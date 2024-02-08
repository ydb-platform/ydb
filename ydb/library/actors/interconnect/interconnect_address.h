#pragma once

#include <util/system/defaults.h>
#include <util/network/init.h>
#include <util/network/address.h>
#include <util/generic/string.h>

namespace NInterconnect {
    class TAddress {
        union {
            sockaddr Generic;
            sockaddr_in Ipv4;
            sockaddr_in6 Ipv6;
        } Addr;

    public:
        TAddress();
        TAddress(const char* addr, ui16 port);
        TAddress(const TString& addr, ui16 port);
        TAddress(in_addr addr, ui16 port);
        TAddress(in6_addr addr, ui16 port);
        TAddress(NAddr::IRemoteAddr& addr);
        int GetFamily() const;
        socklen_t Size() const;
        ::sockaddr* SockAddr();
        const ::sockaddr* SockAddr() const;
        ui16 GetPort() const;
        TString GetAddress() const;
        TString ToString() const;

        static TAddress AnyIPv4(ui16 port) {
            TAddress res;
            res.Addr.Ipv4.sin_family = AF_INET;
            res.Addr.Ipv4.sin_port = htons(port);
            res.Addr.Ipv4.sin_addr.s_addr = htonl(INADDR_ANY);
            return res;
        }

        static TAddress AnyIPv6(ui16 port) {
            TAddress res;
            res.Addr.Ipv6.sin6_family = AF_INET6;
            res.Addr.Ipv6.sin6_port = htons(port);
            res.Addr.Ipv6.sin6_addr = in6addr_any;
            return res;
        }
    };
}
