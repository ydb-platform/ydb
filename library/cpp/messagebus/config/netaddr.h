#pragma once

#include <util/digest/numeric.h>
#include <util/generic/hash.h>
#include <util/generic/ptr.h>
#include <util/generic/strbuf.h>
#include <util/generic/vector.h>
#include <util/network/address.h>

namespace NBus {
    using namespace NAddr;

    /// IP protocol version.
    enum EIpVersion {
        EIP_VERSION_4 = 1,
        EIP_VERSION_6 = 2,
        EIP_VERSION_ANY = EIP_VERSION_4 | EIP_VERSION_6,
    };

    inline bool IsFamilyAllowed(ui16 sa_family, EIpVersion ipVersion) {
        if (ipVersion == EIP_VERSION_4 && sa_family != AF_INET) {
            return false;
        }
        if (ipVersion == EIP_VERSION_6 && sa_family != AF_INET6) {
            return false;
        }
        return true;
    }

    const char* ToCString(EIpVersion);
    int ToAddrFamily(EIpVersion);

    /// Hold referenced pointer to address description structure (ex. sockaddr_storage)
    /// It's make possible to work with IPv4 / IPv6 addresses simultaneously
    class TNetAddr: public IRemoteAddr {
    public:
        class TError: public yexception {
        };

        TNetAddr();
        TNetAddr(TAutoPtr<IRemoteAddr> addr);
        TNetAddr(const char* hostPort, EIpVersion requireVersion = EIP_VERSION_ANY, EIpVersion preferVersion = EIP_VERSION_ANY);
        TNetAddr(TStringBuf host, int port, EIpVersion requireVersion = EIP_VERSION_ANY, EIpVersion preferVersion = EIP_VERSION_ANY);
        TNetAddr(const TNetworkAddress& na, EIpVersion requireVersion = EIP_VERSION_ANY, EIpVersion preferVersion = EIP_VERSION_ANY);
        TNetAddr(const TNetworkAddress& na, const TAddrInfo& ai);

        bool operator==(const TNetAddr&) const;
        bool operator!=(const TNetAddr& other) const {
            return !(*this == other);
        }
        inline explicit operator bool() const noexcept {
            return !!Ptr;
        }

        const sockaddr* Addr() const override;
        socklen_t Len() const override;

        bool IsIpv4() const;
        bool IsIpv6() const;
        int GetPort() const;

    private:
        TAtomicSharedPtr<IRemoteAddr> Ptr;
    };

    using TSockAddrInVector = TVector<TNetAddr>;

    struct TNetAddrHostPortHash {
        inline size_t operator()(const TNetAddr& a) const {
            const sockaddr* s = a.Addr();
            const sockaddr_in* const sa = reinterpret_cast<const sockaddr_in*>(s);
            const sockaddr_in6* const sa6 = reinterpret_cast<const sockaddr_in6*>(s);

            switch (s->sa_family) {
                case AF_INET:
                    return CombineHashes<size_t>(ComputeHash(TStringBuf(reinterpret_cast<const char*>(&sa->sin_addr), sizeof(sa->sin_addr))), IntHashImpl(sa->sin_port));

                case AF_INET6:
                    return CombineHashes<size_t>(ComputeHash(TStringBuf(reinterpret_cast<const char*>(&sa6->sin6_addr), sizeof(sa6->sin6_addr))), IntHashImpl(sa6->sin6_port));
            }

            return ComputeHash(TStringBuf(reinterpret_cast<const char*>(s), a.Len()));
        }
    };

}
