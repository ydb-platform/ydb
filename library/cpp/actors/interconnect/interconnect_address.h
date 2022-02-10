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
        TAddress(NAddr::IRemoteAddr& addr);
        int GetFamily() const; 
        socklen_t Size() const; 
        ::sockaddr* SockAddr(); 
        const ::sockaddr* SockAddr() const; 
        ui16 GetPort() const; 
        TString GetAddress() const; 
        TString ToString() const; 
    };
}
