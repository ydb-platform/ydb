#pragma once

#include <library/cpp/deprecated/atomic/atomic.h>

#include <util/generic/map.h>
#include <util/generic/vector.h>
#include <util/network/address.h>
#include <util/system/mutex.h>
#include <util/datetime/base.h>

/** Asynchronous DNS resolver.
 *
 * This is NOT general purpose resolver! It is designed with very specific assumptions:
 * 1) there is relatively small and rarely changed set of resolved names (like, server pool in cluster)
 * 2) this names supposed to have addresses, absense of A record is equal to DNS error
 * 3) most of the time IP addresses do not change
 * 4) it's OK to return old IP address when DNS server not responding in time
 */

class TDnsCache {
public:
    TDnsCache(bool allowIpv4 = true, bool allowIpv6 = true, time_t entry_lifetime = 1800, time_t neg_lifetime = 1, ui32 request_timeout = 500000);
    ~TDnsCache();

    TString GetHostByAddr(const NAddr::IRemoteAddr&);

    // ip in network byte order
    TIpHost Get(const TString& host);

    /* use with AF_INET, AF_INET6 or AF_UNSPEC */
    NAddr::IRemoteAddrPtr GetAddr(const TString& host,
                                  int family,
                                  TIpPort port = 0,
                                  bool cacheOnly = false);

    void GetAllAddresses(const TString& host, TVector<NAddr::IRemoteAddrPtr>&);

    void GetStats(ui64& a_cache_hits, ui64& a_cache_misses,
                  ui64& ptr_cache_hits, ui64& ptr_cache_misses);

protected:
    bool ValidateHName(const TString& host) const noexcept;

private:
    struct TGHBNContext {
        TDnsCache* Owner;
        TString Hostname;
        int Family;
    };

    struct TGHBAContext {
        TDnsCache* Owner;
        in6_addr Addr;
    };

    struct THost {
        THost() noexcept {
        }

        TVector<TIpHost> AddrsV4;
        time_t ResolvedV4 = 0;
        time_t NotFoundV4 = 0;
        TAtomic InProgressV4 = 0;

        TVector<in6_addr> AddrsV6;
        time_t ResolvedV6 = 0;
        time_t NotFoundV6 = 0;
        TAtomic InProgressV6 = 0;

        TString AddrsV4ToString() const;
        TString AddrsV6ToString() const;

        bool IsStale(int family, const TDnsCache* ctx) const noexcept;
    };

    typedef TMap<TString, THost> THostCache;

    struct TAddr {
        TString Hostname;
        time_t Resolved = 0;
        time_t NotFound = 0;
        TAtomic InProgress = 0;
    };
    /* IRemoteAddr is annoingly hard to use, so I'll use in6_addr as key
     * and put v4 addrs in it.
     */
    struct TAddrCmp {
        bool operator()(const in6_addr& left, const in6_addr& right) const {
            for (size_t i = 0; i < sizeof(left); i++) {
                if (left.s6_addr[i] < right.s6_addr[i]) {
                    return true;
                } else if (left.s6_addr[i] > right.s6_addr[i]) {
                    return false;
                }
            }
            // equal
            return false;
        }
    };
    typedef TMap<in6_addr, TAddr, TAddrCmp> TAddrCache;

    const THost& Resolve(const TString&, int family, bool cacheOnly = false);

    const TAddr& ResolveAddr(const in6_addr&, int family);

    void WaitTask(TAtomic&);

    static void GHBNCallback(void* arg, int status, int timeouts,
                             struct hostent* info);

    static void GHBACallback(void* arg, int status, int timeouts,
                             struct hostent* info);

    const time_t EntryLifetime;
    const time_t NegativeLifetime;
    const TDuration Timeout;
    const bool AllowIpV4;
    const bool AllowIpV6;

    TMutex CacheMtx;
    THostCache HostCache;
    TAddrCache AddrCache;
    ui64 ACacheHits;
    ui64 ACacheMisses;
    ui64 PtrCacheHits;
    ui64 PtrCacheMisses;

    const static THost NullHost;

    TMutex AresMtx;
    void* Channel;

    struct TAresLibInit {
        TAresLibInit();
        ~TAresLibInit();
    };

    static TAresLibInit InitAresLib;
};
