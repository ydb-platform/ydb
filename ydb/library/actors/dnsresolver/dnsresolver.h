#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/event_local.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <util/network/address.h>
#include <variant>

namespace NActors {
namespace NDnsResolver {

    struct TEvDns {
        enum EEv {
            EvGetHostByName = EventSpaceBegin(TEvents::ES_DNS),
            EvGetHostByNameResult,
            EvGetAddr,
            EvGetAddrResult,
        };

        /**
         * TEvGetHostByName returns the result of ares_gethostbyname
         */
        struct TEvGetHostByName : public TEventLocal<TEvGetHostByName, EvGetHostByName> {
            TString Name;
            int Family;

            explicit TEvGetHostByName(TString name, int family = AF_UNSPEC)
                : Name(std::move(name))
                , Family(family)
            { }
        };

        struct TEvGetHostByNameResult : public TEventLocal<TEvGetHostByNameResult, EvGetHostByNameResult> {
            TVector<struct in_addr> AddrsV4;
            TVector<struct in6_addr> AddrsV6;
            TString ErrorText;
            int Status = 0;
        };

        /**
         * TEvGetAddr returns a single address for a given hostname
         */
        struct TEvGetAddr : public TEventLocal<TEvGetAddr, EvGetAddr> {
            TString Name;
            int Family;

            explicit TEvGetAddr(TString name, int family = AF_UNSPEC)
                : Name(std::move(name))
                , Family(family)
            { }
        };

        struct TEvGetAddrResult : public TEventLocal<TEvGetAddrResult, EvGetAddrResult> {
            // N.B. "using" here doesn't work with Visual Studio compiler
            typedef struct in6_addr TIPv6Addr;
            typedef struct in_addr TIPv4Addr;

            std::variant<std::monostate, TIPv6Addr, TIPv4Addr> Addr;
            TString ErrorText;
            int Status = 0;

            bool IsV6() const {
                return std::holds_alternative<TIPv6Addr>(Addr);
            }

            bool IsV4() const {
                return std::holds_alternative<TIPv4Addr>(Addr);
            }

            const TIPv6Addr& GetAddrV6() const {
                const TIPv6Addr* p = std::get_if<TIPv6Addr>(&Addr);
                Y_ABORT_UNLESS(p, "Result is not an ipv6 address");
                return *p;
            }

            const TIPv4Addr& GetAddrV4() const {
                const TIPv4Addr* p = std::get_if<TIPv4Addr>(&Addr);
                Y_ABORT_UNLESS(p, "Result is not an ipv4 address");
                return *p;
            }
        };
    };

    struct TSimpleDnsResolverOptions {
        // Initial per-server timeout, grows exponentially with each retry
        TDuration Timeout = TDuration::Seconds(1);
        // Number of attempts per-server
        int Attempts = 2;
        // Optional list of custom dns servers (ip.v4[:port], ip::v6 or [ip::v6]:port format)
        TVector<TString> Servers;
        // Keep soket open between dns requests
        bool KeepSocket = true;
        // Force tcp to perform dns requests
        bool ForceTcp = false;
    };

    IActor* CreateSimpleDnsResolver(TSimpleDnsResolverOptions options = TSimpleDnsResolverOptions());

    struct TCachingDnsResolverOptions {
        // Soft expire time specifies delay before name is refreshed in background
        TDuration SoftNegativeExpireTime = TDuration::Seconds(1);
        TDuration SoftPositiveExpireTime = TDuration::Seconds(10);
        // Hard expire time specifies delay before the last result is forgotten
        TDuration HardNegativeExpireTime = TDuration::Seconds(10);
        TDuration HardPositiveExpireTime = TDuration::Hours(2);
        // Allow these request families
        bool AllowIPv6 = true;
        bool AllowIPv4 = true;
        // Optional counters
        NMonitoring::TDynamicCounterPtr MonCounters = nullptr;
    };

    IActor* CreateCachingDnsResolver(TActorId upstream, TCachingDnsResolverOptions options = TCachingDnsResolverOptions());

    struct TOnDemandDnsResolverOptions
        : public TSimpleDnsResolverOptions
        , public TCachingDnsResolverOptions
    {
    };

    IActor* CreateOnDemandDnsResolver(TOnDemandDnsResolverOptions options = TOnDemandDnsResolverOptions());

    /**
     * Returns actor id of a globally registered dns resolver
     */
    inline TActorId MakeDnsResolverActorId() {
        return TActorId(0, TStringBuf("dnsresolver"));
    }

} // namespace NDnsResolver
} // namespace NActors
