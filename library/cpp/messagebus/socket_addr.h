#pragma once

#include "hash.h"

#include <util/generic/hash.h>
#include <util/generic/utility.h>
#include <util/network/address.h>
#include <util/network/init.h>

#include <string.h>

namespace NBus {
    class TNetAddr;
}

namespace NBus {
    namespace NPrivate {
        enum EAddrFamily {
            ADDR_UNSPEC = AF_UNSPEC,
            ADDR_IPV4 = AF_INET,
            ADDR_IPV6 = AF_INET6,
        };

        class TBusIpAddr {
        private:
            EAddrFamily Af;

            union {
                in_addr In4;
                in6_addr In6;
            };

        public:
            TBusIpAddr() {
                Clear();
            }

            EAddrFamily GetAddrFamily() const {
                return Af;
            }

            void Clear() {
                Zero(*this);
            }

            in_addr GetInAddr() const {
                Y_ASSERT(Af == ADDR_IPV4);
                return In4;
            }

            void SetInAddr(const in_addr& in4) {
                Clear();
                Af = ADDR_IPV4;
                In4 = in4;
            }

            in6_addr GetIn6Addr() const {
                Y_ASSERT(Af == ADDR_IPV6);
                return In6;
            }

            void SetIn6Addr(const in6_addr& in6) {
                Clear();
                Af = ADDR_IPV6;
                In6 = in6;
            }

            bool operator==(const TBusIpAddr& that) const {
                return memcmp(this, &that, sizeof(that)) == 0;
            }
        };

        class TBusSocketAddr {
        public:
            TBusIpAddr IpAddr;
            ui16 Port;

            //Only makes sense for IPv6 link-local addresses
            ui32 IPv6ScopeID;

            TBusSocketAddr()
                : Port(0)
                , IPv6ScopeID(0)
            {
            }

            TBusSocketAddr(const NAddr::IRemoteAddr*);
            TBusSocketAddr(const TNetAddr&);
            TBusSocketAddr(TStringBuf host, unsigned port);

            TNetAddr ToNetAddr() const;

            bool operator==(const TBusSocketAddr& that) const {
                return IpAddr == that.IpAddr && Port == that.Port;
            }
        };

    }
}

template <>
struct THash<NBus::NPrivate::TBusIpAddr> {
    inline size_t operator()(const NBus::NPrivate::TBusIpAddr& a) const {
        return ComputeHash(TStringBuf((const char*)&a, sizeof(a)));
    }
};

template <>
struct THash<NBus::NPrivate::TBusSocketAddr> {
    inline size_t operator()(const NBus::NPrivate::TBusSocketAddr& a) const {
        return HashValues(a.IpAddr, a.Port);
    }
};
