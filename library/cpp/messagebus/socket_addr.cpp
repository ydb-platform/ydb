#include "socket_addr.h"

#include "netaddr.h"

#include <util/network/address.h>
#include <util/network/init.h>
#include <util/system/yassert.h>

using namespace NAddr;

using namespace NBus;
using namespace NBus::NPrivate;

static_assert(ADDR_UNSPEC == 0, "expect ADDR_UNSPEC == 0");

NBus::NPrivate::TBusSocketAddr::TBusSocketAddr(const NAddr::IRemoteAddr* addr)
    : IPv6ScopeID(0)
{
    const sockaddr* sa = addr->Addr();

    switch ((EAddrFamily)sa->sa_family) {
        case AF_UNSPEC: {
            IpAddr.Clear();
            Port = 0;
            break;
        }
        case AF_INET: {
            IpAddr.SetInAddr(((const sockaddr_in*)sa)->sin_addr);
            Port = InetToHost(((const sockaddr_in*)sa)->sin_port);
            break;
        }
        case AF_INET6: {
            IpAddr.SetIn6Addr(((const sockaddr_in6*)sa)->sin6_addr);
            Port = InetToHost(((const sockaddr_in*)sa)->sin_port);
            IPv6ScopeID = InetToHost(((const sockaddr_in6*)sa)->sin6_scope_id);
            break;
        }
        default:
            Y_ABORT("unknown address family");
    }
}

NBus::NPrivate::TBusSocketAddr::TBusSocketAddr(TStringBuf host, unsigned port) {
    *this = TNetAddr(host, port);
}

NBus::NPrivate::TBusSocketAddr::TBusSocketAddr(const TNetAddr& addr) {
    *this = TBusSocketAddr(&addr);
}

TNetAddr NBus::NPrivate::TBusSocketAddr::ToNetAddr() const {
    sockaddr_storage storage;
    Zero(storage);

    storage.ss_family = (ui16)IpAddr.GetAddrFamily();

    switch (IpAddr.GetAddrFamily()) {
        case ADDR_UNSPEC:
            return TNetAddr();
        case ADDR_IPV4: {
            ((sockaddr_in*)&storage)->sin_addr = IpAddr.GetInAddr();
            ((sockaddr_in*)&storage)->sin_port = HostToInet(Port);
            break;
        }
        case ADDR_IPV6: {
            ((sockaddr_in6*)&storage)->sin6_addr = IpAddr.GetIn6Addr();
            ((sockaddr_in6*)&storage)->sin6_port = HostToInet(Port);
            ((sockaddr_in6*)&storage)->sin6_scope_id = HostToInet(IPv6ScopeID);
            break;
        }
    }

    return TNetAddr(new TOpaqueAddr((sockaddr*)&storage));
}

template <>
void Out<TBusSocketAddr>(IOutputStream& out, const TBusSocketAddr& addr) {
    out << addr.ToNetAddr();
}
