#include "dnscache.h"
#include "probes.h"
#include "timekeeper.h"

#include <ares.h>
#include <util/system/guard.h>
#include <util/datetime/systime.h>

const TDnsCache::THost TDnsCache::NullHost;

LWTRACE_USING(DNSCACHELIB_PROVIDER);

static_assert(sizeof(ares_channel) == sizeof(void*), "expect sizeof(ares_channel) == sizeof(void *)");

TDnsCache::TDnsCache(bool allowIpv4, bool allowIpv6, time_t lifetime, time_t neg, ui32 timeout)
    : EntryLifetime(lifetime)
    , NegativeLifetime(neg)
    , Timeout(TDuration::MicroSeconds(timeout))
    , AllowIpV4(allowIpv4)
    , AllowIpV6(allowIpv6)
    , ACacheHits(0)
    , ACacheMisses(0)
    , PtrCacheHits(0)
    , PtrCacheMisses(0)
{
#ifdef _win_
    if (ares_library_init(ARES_LIB_INIT_WIN32) != ARES_SUCCESS) {
        LWPROBE(AresInitFailed);
        ythrow yexception() << "ares_init() failed";
    }
#endif

    ares_channel chan;

    if (ares_init(&chan) != ARES_SUCCESS) {
        LWPROBE(AresInitFailed);
        ythrow yexception() << "ares_init() failed";
    }
    Channel = chan;
    LWPROBE(Created);
}

TDnsCache::~TDnsCache(void) {
    ares_channel chan = static_cast<ares_channel>(Channel);

    ares_cancel(chan);
    ares_destroy(chan);
    LWPROBE(Destroyed);

#ifdef _win_
    ares_library_cleanup();
#endif
}

TString TDnsCache::GetHostByAddr(const NAddr::IRemoteAddr& addr) {
    in6_addr key;

    if (addr.Addr()->sa_family == AF_INET6) {
        const struct sockaddr_in6* s6 = (const struct sockaddr_in6*)(addr.Addr());
        memcpy(&key, &s6->sin6_addr, sizeof(s6->sin6_addr));
    } else if (addr.Addr()->sa_family == AF_INET) {
        const struct sockaddr_in* s4 = (const struct sockaddr_in*)(addr.Addr());
        memset(&key, 0, sizeof(key));
        memcpy(&key, &s4->sin_addr, sizeof(s4->sin_addr));
    } else {
        return "";
    }
    const TAddr& host = ResolveAddr(key, addr.Addr()->sa_family);

    return host.Hostname;
}

TIpHost TDnsCache::Get(const TString& hostname) {
    if (!AllowIpV4)
        return TIpHost(-1);

    const THost& addr = Resolve(hostname, AF_INET);

    TGuard<TMutex> lock(CacheMtx);
    if (addr.AddrsV4.empty()) {
        return TIpHost(-1);
    }
    return addr.AddrsV4.front();
}

NAddr::IRemoteAddrPtr TDnsCache::GetAddr(
    const TString& hostname,
    int family,
    TIpPort port,
    bool cacheOnly) {
    if (family != AF_INET && AllowIpV6) {
        const THost& addr = Resolve(hostname, AF_INET6, cacheOnly);

        TGuard<TMutex> lock(CacheMtx);
        if (!addr.AddrsV6.empty()) {
            struct sockaddr_in6 sin6;
            Zero(sin6);
            sin6.sin6_family = AF_INET6;
            sin6.sin6_addr = addr.AddrsV6.front();
            sin6.sin6_port = HostToInet(port);

            return MakeHolder<NAddr::TIPv6Addr>(sin6);
        }
    }

    if (family != AF_INET6 && AllowIpV4) {
        const THost& addr = Resolve(hostname, AF_INET, cacheOnly);

        TGuard<TMutex> lock(CacheMtx);
        if (!addr.AddrsV4.empty()) {
            return MakeHolder<NAddr::TIPv4Addr>(TIpAddress(addr.AddrsV4.front(), port));
        }
    }

    LWPROBE(FamilyMismatch, family, AllowIpV4, AllowIpV6);
    return nullptr;
}

void TDnsCache::GetAllAddresses(
    const TString& hostname,
    TVector<NAddr::IRemoteAddrPtr>& addrs) {
    if (AllowIpV4) {
        const THost& addr4 = Resolve(hostname, AF_INET);

        TGuard<TMutex> lock(CacheMtx);
        for (size_t i = 0; i < addr4.AddrsV4.size(); i++) {
            addrs.push_back(MakeHolder<NAddr::TIPv4Addr>(TIpAddress(addr4.AddrsV4[i], 0)));
        }
    }

    if (AllowIpV6) {
        const THost& addr6 = Resolve(hostname, AF_INET6);

        struct sockaddr_in6 sin6;
        Zero(sin6);
        sin6.sin6_family = AF_INET6;

        TGuard<TMutex> lock(CacheMtx);
        for (size_t i = 0; i < addr6.AddrsV6.size(); i++) {
            sin6.sin6_addr = addr6.AddrsV6[i];

            addrs.push_back(MakeHolder<NAddr::TIPv6Addr>(sin6));
        }
    }
}

void TDnsCache::GetStats(ui64& a_cache_hits, ui64& a_cache_misses,
                         ui64& ptr_cache_hits, ui64& ptr_cache_misses) {
    TGuard<TMutex> lock(CacheMtx);

    a_cache_hits = ACacheHits;
    a_cache_misses = ACacheMisses;
    ptr_cache_hits = PtrCacheHits;
    ptr_cache_misses = PtrCacheMisses;
}

bool TDnsCache::THost::IsStale(int family, const TDnsCache* ctx) const noexcept {
    time_t resolved = family == AF_INET ? ResolvedV4 : ResolvedV6;
    time_t notfound = family == AF_INET ? NotFoundV4 : NotFoundV6;

    if (TTimeKeeper::GetTime() - resolved < ctx->EntryLifetime)
        return false;

    if (TTimeKeeper::GetTime() - notfound < ctx->NegativeLifetime)
        return false;

    return true;
}

const TDnsCache::THost&
TDnsCache::Resolve(const TString& hostname, int family, bool cacheOnly) {
    if (!ValidateHName(hostname)) {
        LWPROBE(ResolveNullHost, hostname, family);
        return NullHost;
    }

    THostCache::iterator p;

    Y_ASSERT(family == AF_INET || family == AF_INET6);

    {
        TGuard<TMutex> lock(CacheMtx);
        p = HostCache.find(hostname);
        if (p != HostCache.end()) {
            if (!p->second.IsStale(family, this)) {
                /* Recently resolved, just return cached value */
                ACacheHits += 1;
                THost& host = p->second;
                LWPROBE(ResolveFromCache, hostname, family, host.AddrsV4ToString(), host.AddrsV6ToString(), ACacheHits);
                return host;
            } else {
                LWPROBE(ResolveCacheTimeout, hostname);
            }
        } else {
            /* Never resolved, create cache entry */
            LWPROBE(ResolveCacheNew, hostname);
            p = HostCache.insert(std::make_pair(hostname, THost())).first;
        }
        ACacheMisses += 1;
    }

    if (cacheOnly)
        return NullHost;

    TAtomic& inprogress = (family == AF_INET ? p->second.InProgressV4 : p->second.InProgressV6);

    {
        /* This way only! CacheMtx should always be taken AFTER AresMtx,
         * because later in ares_process it can only be done this way.
         * Lock order reversal will cause deadlock in unfortunate monents.
         */
        TGuard<TMutex> areslock(AresMtx);
        TGuard<TMutex> cachelock(CacheMtx);

        if (!inprogress) {
            ares_channel chan = static_cast<ares_channel>(Channel);
            TGHBNContext* ctx = new TGHBNContext();
            ctx->Owner = this;
            ctx->Hostname = hostname;
            ctx->Family = family;

            AtomicSet(inprogress, 1);
            ares_gethostbyname(chan, hostname.c_str(), family,
                               &TDnsCache::GHBNCallback, ctx);
        }
    }

    WaitTask(inprogress);

    LWPROBE(ResolveDone, hostname, family, p->second.AddrsV4ToString(), p->second.AddrsV6ToString());
    return p->second;
}

bool TDnsCache::ValidateHName(const TString& name) const noexcept {
    return name.size() > 0;
}

const TDnsCache::TAddr& TDnsCache::ResolveAddr(const in6_addr& addr, int family) {
    TAddrCache::iterator p;

    {
        TGuard<TMutex> lock(CacheMtx);
        p = AddrCache.find(addr);
        if (p != AddrCache.end()) {
            if (TTimeKeeper::GetTime() - p->second.Resolved < EntryLifetime || TTimeKeeper::GetTime() - p->second.NotFound < NegativeLifetime) {
                /* Recently resolved, just return cached value */
                PtrCacheHits += 1;
                return p->second;
            }
        } else {
            /* Never resolved, create cache entry */

            p = AddrCache.insert(std::make_pair(addr, TAddr())).first;
        }
        PtrCacheMisses += 1;
    }

    {
        /* This way only! CacheMtx should always be taken AFTER AresMtx,
         * because later in ares_process it can only be done this way.
         * Lock order reversal will cause deadlock in unfortunate monents.
         */
        TGuard<TMutex> areslock(AresMtx);
        TGuard<TMutex> cachelock(CacheMtx);

        if (!p->second.InProgress) {
            ares_channel chan = static_cast<ares_channel>(Channel);
            TGHBAContext* ctx = new TGHBAContext();
            ctx->Owner = this;
            ctx->Addr = addr;

            AtomicSet(p->second.InProgress, 1);
            ares_gethostbyaddr(chan, &addr,
                               family == AF_INET ? sizeof(in_addr) : sizeof(in6_addr),
                               family, &TDnsCache::GHBACallback, ctx);
        }
    }

    WaitTask(p->second.InProgress);

    return p->second;
}

void TDnsCache::WaitTask(TAtomic& flag) {
    const TInstant start = TInstant(TTimeKeeper::GetTimeval());

    while (AtomicGet(flag)) {
        ares_channel chan = static_cast<ares_channel>(Channel);

        struct pollfd pfd[ARES_GETSOCK_MAXNUM];
        int nfds;
        ares_socket_t socks[ARES_GETSOCK_MAXNUM];
        int bits;

        {
            TGuard<TMutex> lock(AresMtx);
            bits = ares_getsock(chan, socks, ARES_GETSOCK_MAXNUM);
            if (bits == 0) {
                /* other thread did our job */
                continue;
            }
        }

        for (nfds = 0; nfds < ARES_GETSOCK_MAXNUM; nfds++) {
            pfd[nfds].events = 0;
            pfd[nfds].revents = 0;
            if (ARES_GETSOCK_READABLE(bits, nfds)) {
                pfd[nfds].fd = socks[nfds];
                pfd[nfds].events |= POLLRDNORM | POLLIN;
            }
            if (ARES_GETSOCK_WRITABLE(bits, nfds)) {
                pfd[nfds].fd = socks[nfds];
                pfd[nfds].events |= POLLWRNORM | POLLOUT;
            }
            if (pfd[nfds].events == 0) {
                break;
            }
        }

        Y_ASSERT(nfds != 0);

        const TDuration left = TInstant(TTimeKeeper::GetTimeval()) - start;
        const TDuration wait = Max(Timeout - left, TDuration::Zero());

        int rv = poll(pfd, nfds, wait.MilliSeconds());

        if (rv == -1) {
            if (errno == EINTR) {
                continue;
            }
            /* Unknown error in select, can't recover. Just pretend there was no reply */
            rv = 0;
        }

        if (rv == 0) {
            /* poll() timed out */
            TGuard<TMutex> lock(AresMtx);
            ares_process_fd(chan, ARES_SOCKET_BAD, ARES_SOCKET_BAD);
        } else {
            for (int i = 0; i < nfds; i++) {
                if (pfd[i].revents == 0) {
                    continue;
                }
                TGuard<TMutex> lock(AresMtx);
                ares_process_fd(chan,
                                pfd[i].revents & (POLLRDNORM | POLLIN)
                                    ? pfd[i].fd
                                    : ARES_SOCKET_BAD,
                                pfd[i].revents & (POLLWRNORM | POLLOUT)
                                    ? pfd[i].fd
                                    : ARES_SOCKET_BAD);
            }
        }

        if (start + Timeout <= TInstant(TTimeKeeper::GetTimeval())) {
            break;
        }
    }
}

void TDnsCache::GHBNCallback(void* arg, int status, int, struct hostent* info) {
    THolder<TGHBNContext> ctx(static_cast<TGHBNContext*>(arg));
    TGuard<TMutex> lock(ctx->Owner->CacheMtx);
    THostCache::iterator p = ctx->Owner->HostCache.find(ctx->Hostname);

    Y_ASSERT(p != ctx->Owner->HostCache.end());

    time_t& resolved = (ctx->Family == AF_INET ? p->second.ResolvedV4 : p->second.ResolvedV6);
    time_t& notfound = (ctx->Family == AF_INET ? p->second.NotFoundV4 : p->second.NotFoundV6);
    TAtomic& inprogress = (ctx->Family == AF_INET ? p->second.InProgressV4 : p->second.InProgressV6);

    if (status == ARES_SUCCESS) {
        if (info->h_addrtype == AF_INET) {
            p->second.AddrsV4.clear();
            for (int i = 0; info->h_addr_list[i] != nullptr; i++) {
                p->second.AddrsV4.push_back(*(TIpHost*)(info->h_addr_list[i]));
            }
            /* It is possible to ask ares for IPv6 and have IPv4 addrs instead,
               so take care and set V4 timers anyway.
             */
            p->second.ResolvedV4 = TTimeKeeper::GetTime();
            p->second.ResolvedV4 = 0;
            AtomicSet(p->second.InProgressV4, 0);
        } else if (info->h_addrtype == AF_INET6) {
            p->second.AddrsV6.clear();
            for (int i = 0; info->h_addr_list[i] != nullptr; i++) {
                p->second.AddrsV6.push_back(*(struct in6_addr*)(info->h_addr_list[i]));
            }
        } else {
            Y_ABORT("unknown address type in ares callback");
        }
        resolved = TTimeKeeper::GetTime();
        notfound = 0;
    } else {
        notfound = TTimeKeeper::GetTime();
        resolved = 0;
    }
    AtomicSet(inprogress, 0);
}

void TDnsCache::GHBACallback(void* arg, int status, int, struct hostent* info) {
    THolder<TGHBAContext> ctx(static_cast<TGHBAContext*>(arg));
    TGuard<TMutex> lock(ctx->Owner->CacheMtx);
    TAddrCache::iterator p = ctx->Owner->AddrCache.find(ctx->Addr);

    Y_ASSERT(p != ctx->Owner->AddrCache.end());

    if (status == ARES_SUCCESS) {
        p->second.Hostname = info->h_name;
        p->second.Resolved = TTimeKeeper::GetTime();
        p->second.NotFound = 0;
    } else {
        p->second.NotFound = TTimeKeeper::GetTime();
        p->second.Resolved = 0;
    }
    AtomicSet(p->second.InProgress, 0);
}

TString TDnsCache::THost::AddrsV4ToString() const {
    TStringStream ss;
    bool first = false;
    for (TIpHost addr : AddrsV4) {
        ss << (first ? "" : " ") << IpToString(addr);
        first = false;
    }
    return ss.Str();
}

TString TDnsCache::THost::AddrsV6ToString() const {
    TStringStream ss;
    bool first = false;
    for (in6_addr addr : AddrsV6) {
        struct sockaddr_in6 sin6;
        Zero(sin6);
        sin6.sin6_family = AF_INET6;
        sin6.sin6_addr = addr;

        NAddr::TIPv6Addr addr6(sin6);
        ss << (first ? "" : " ") << NAddr::PrintHost(addr6);
        first = false;
    }
    return ss.Str();
}

TDnsCache::TAresLibInit::TAresLibInit() {
#ifdef _win_
    const auto res = ares_library_init(ARES_LIB_INIT_ALL);
    Y_ABORT_UNLESS(res == 0);
#endif
}

TDnsCache::TAresLibInit::~TAresLibInit() {
#ifdef _win_
    ares_library_cleanup();
#endif
}

TDnsCache::TAresLibInit TDnsCache::InitAresLib;
