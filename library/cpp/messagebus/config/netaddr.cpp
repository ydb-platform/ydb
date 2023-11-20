#include "netaddr.h"

#include <util/network/address.h>

#include <cstdlib>

namespace NBus {
    const char* ToCString(EIpVersion ipVersion) {
        switch (ipVersion) {
            case EIP_VERSION_ANY:
                return "EIP_VERSION_ANY";
            case EIP_VERSION_4:
                return "EIP_VERSION_4";
            case EIP_VERSION_6:
                return "EIP_VERSION_6";
        }
        Y_ABORT();
    }

    int ToAddrFamily(EIpVersion ipVersion) {
        switch (ipVersion) {
            case EIP_VERSION_ANY:
                return AF_UNSPEC;
            case EIP_VERSION_4:
                return AF_INET;
            case EIP_VERSION_6:
                return AF_INET6;
        }
        Y_ABORT();
    }

    class TNetworkAddressRef: private TNetworkAddress, public TAddrInfo {
    public:
        TNetworkAddressRef(const TNetworkAddress& na, const TAddrInfo& ai)
            : TNetworkAddress(na)
            , TAddrInfo(ai)
        {
        }
    };

    static bool Compare(const IRemoteAddr& l, const IRemoteAddr& r) noexcept {
        if (l.Addr()->sa_family != r.Addr()->sa_family) {
            return false;
        }

        switch (l.Addr()->sa_family) {
            case AF_INET: {
                return memcmp(&(((const sockaddr_in*)l.Addr())->sin_addr), &(((const sockaddr_in*)r.Addr())->sin_addr), sizeof(in_addr)) == 0 &&
                       ((const sockaddr_in*)l.Addr())->sin_port == ((const sockaddr_in*)r.Addr())->sin_port;
            }

            case AF_INET6: {
                return memcmp(&(((const sockaddr_in6*)l.Addr())->sin6_addr), &(((const sockaddr_in6*)r.Addr())->sin6_addr), sizeof(in6_addr)) == 0 &&
                       ((const sockaddr_in6*)l.Addr())->sin6_port == ((const sockaddr_in6*)r.Addr())->sin6_port;
            }
        }

        return memcmp(l.Addr(), r.Addr(), Min<size_t>(l.Len(), r.Len())) == 0;
    }

    TNetAddr::TNetAddr()
        : Ptr(new TOpaqueAddr)
    {
    }

    TNetAddr::TNetAddr(TAutoPtr<IRemoteAddr> addr)
        : Ptr(addr)
    {
        Y_ABORT_UNLESS(!!Ptr);
    }

    namespace {
        using namespace NAddr;

        const char* Describe(EIpVersion version) {
            switch (version) {
                case EIP_VERSION_4:
                    return "ipv4 address";
                case EIP_VERSION_6:
                    return "ipv6 address";
                case EIP_VERSION_ANY:
                    return "any address";
                default:
                    Y_ABORT("unreachable");
            }
        }

        TAutoPtr<IRemoteAddr> MakeAddress(const TNetworkAddress& na, EIpVersion requireVersion, EIpVersion preferVersion) {
            TAutoPtr<IRemoteAddr> addr;
            for (TNetworkAddress::TIterator it = na.Begin(); it != na.End(); ++it) {
                if (IsFamilyAllowed(it->ai_family, requireVersion)) {
                    if (IsFamilyAllowed(it->ai_family, preferVersion)) {
                        return new TNetworkAddressRef(na, &*it);
                    } else if (!addr) {
                        addr.Reset(new TNetworkAddressRef(na, &*it));
                    }
                }
            }
            return addr;
        }
        TAutoPtr<IRemoteAddr> MakeAddress(TStringBuf host, int port, EIpVersion requireVersion, EIpVersion preferVersion) {
            TString hostString(host);
            TNetworkAddress na(hostString, port);
            return MakeAddress(na, requireVersion, preferVersion);
        }
        TAutoPtr<IRemoteAddr> MakeAddress(const char* hostPort, EIpVersion requireVersion, EIpVersion preferVersion) {
            const char* portStr = strchr(hostPort, ':');
            if (!portStr) {
                ythrow TNetAddr::TError() << "port not specified in " << hostPort;
            }
            int port = atoi(portStr + 1);
            TNetworkAddress na(TString(hostPort, portStr), port);
            return MakeAddress(na, requireVersion, preferVersion);
        }
    }

    TNetAddr::TNetAddr(const char* hostPort, EIpVersion requireVersion /*= EIP_VERSION_ANY*/, EIpVersion preferVersion /*= EIP_VERSION_ANY*/)
        : Ptr(MakeAddress(hostPort, requireVersion, preferVersion))
    {
        if (!Ptr) {
            ythrow TNetAddr::TError() << "cannot resolve " << hostPort << " into " << Describe(requireVersion);
        }
    }

    TNetAddr::TNetAddr(TStringBuf host, int port, EIpVersion requireVersion /*= EIP_VERSION_ANY*/, EIpVersion preferVersion /*= EIP_VERSION_ANY*/)
        : Ptr(MakeAddress(host, port, requireVersion, preferVersion))
    {
        if (!Ptr) {
            ythrow TNetAddr::TError() << "cannot resolve " << host << ":" << port << " into " << Describe(requireVersion);
        }
    }

    TNetAddr::TNetAddr(const TNetworkAddress& na, EIpVersion requireVersion /*= EIP_VERSION_ANY*/, EIpVersion preferVersion /*= EIP_VERSION_ANY*/)
        : Ptr(MakeAddress(na, requireVersion, preferVersion))
    {
        if (!Ptr) {
            ythrow TNetAddr::TError() << "cannot resolve into " << Describe(requireVersion);
        }
    }

    TNetAddr::TNetAddr(const TNetworkAddress& na, const TAddrInfo& ai)
        : Ptr(new TNetworkAddressRef(na, ai))
    {
    }

    const sockaddr* TNetAddr::Addr() const {
        return Ptr->Addr();
    }

    socklen_t TNetAddr::Len() const {
        return Ptr->Len();
    }

    int TNetAddr::GetPort() const {
        switch (Ptr->Addr()->sa_family) {
            case AF_INET:
                return InetToHost(((sockaddr_in*)Ptr->Addr())->sin_port);
            case AF_INET6:
                return InetToHost(((sockaddr_in6*)Ptr->Addr())->sin6_port);
            default:
                Y_ABORT("unknown AF: %d", (int)Ptr->Addr()->sa_family);
                throw 1;
        }
    }

    bool TNetAddr::IsIpv4() const {
        return Ptr->Addr()->sa_family == AF_INET;
    }

    bool TNetAddr::IsIpv6() const {
        return Ptr->Addr()->sa_family == AF_INET6;
    }

    bool TNetAddr::operator==(const TNetAddr& rhs) const {
        return Ptr == rhs.Ptr || Compare(*Ptr, *rhs.Ptr);
    }

}

template <>
void Out<NBus::TNetAddr>(IOutputStream& out, const NBus::TNetAddr& addr) {
    Out<NAddr::IRemoteAddr>(out, addr);
}
