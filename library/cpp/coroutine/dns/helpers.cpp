#include "helpers.h"
#include "coro.h"
#include "async.h"
#include "cache.h"

#include <util/digest/city.h>
#include <util/generic/hash_set.h>

using namespace NAddr;
using namespace NAsyncDns;

namespace {
    typedef ui64 TAddrHash;

    inline TAddrHash Hash(const IRemoteAddrRef& addr) {
        return CityHash64((const char*)addr->Addr(), addr->Len());
    }

    inline IRemoteAddrRef ConstructIP4(void* data, ui16 port) {
        return new TIPv4Addr(TIpAddress(*(ui32*)data, port));
    }

    inline IRemoteAddrRef ConstructIP6(void* data, ui16 port) {
        sockaddr_in6 res;

        Zero(res);

        res.sin6_family = AF_INET6;
        res.sin6_port = HostToInet(port);
        memcpy(&res.sin6_addr.s6_addr, data, sizeof(res.sin6_addr.s6_addr));

        return new TIPv6Addr(res);
    }

    inline IRemoteAddrRef Construct(const hostent* h, void* data, ui16 port) {
        switch (h->h_addrtype) {
            case AF_INET:
                return ConstructIP4(data, port);

            case AF_INET6:
                return ConstructIP6(data, port);
        }

        //real shit happens
        abort();
    }

    template <class It, class T>
    static bool FindByHash(It b, It e, T t) {
        while (b != e) {
            if (Hash(*b) == t) {
                return true;
            }

            ++b;
        }

        return false;
    }

    inline size_t LstLen(char** lst) noexcept {
        size_t ret = 0;

        while (*lst) {
            ++ret;
            ++lst;
        }

        return ret;
    }
}

void TResolveAddr::OnComplete(const TResult& r) {
    const hostent* h = r.Result;

    if (!h) {
        Status.push_back(r.Status);

        return;
    }

    char** lst = h->h_addr_list;

    typedef THashSet<TAddrHash> THashes;
    TAutoPtr<THashes> hashes;

    if ((Result.size() + LstLen(lst)) > 8) {
        hashes.Reset(new THashes());

        for (const auto& it : Result) {
            hashes->insert(Hash(it));
        }
    }

    while (*lst) {
        IRemoteAddrRef addr = Construct(h, *lst, Port);

        if (!hashes) {
            if (!FindByHash(Result.begin(), Result.end(), Hash(addr))) {
                Result.push_back(addr);
            }
        } else {
            const TAddrHash h = Hash(addr);

            if (hashes->find(h) == hashes->end()) {
                hashes->insert(h);
                Result.push_back(addr);
            }
        }

        ++lst;
    }
}

void NAsyncDns::ResolveAddr(TContResolver& resolver, const TString& host, ui16 port, TAddrs& result) {
    TResolveAddr cb(port);

    resolver.Resolve(TNameRequest(host.data(), AF_UNSPEC, &cb));

    if (cb.Result) {
        for (auto status : cb.Status) {
            //we have some results, so skip empty responses for aaaa requests
            CheckPartialAsyncStatus(status);
        }
    } else {
        for (auto status : cb.Status) {
            CheckAsyncStatus(status);
        }
    }

    cb.Result.swap(result);
}

void NAsyncDns::ResolveAddr(TContResolver& resolver, const TString& addr, TAddrs& result) {
    ResolveAddr(resolver, addr, 80, result);
}

void NAsyncDns::ResolveAddr(TContResolver& resolver, const TString& host, ui16 port, TAddrs& result, TContDnsCache* cache) {
    if (cache) {
        cache->LookupOrResolve(resolver, host, port, result);
    } else {
        ResolveAddr(resolver, host, port, result);
    }
}

void NAsyncDns::ResolveAddr(TContResolver& resolver, const TString& addr, TAddrs& result, TContDnsCache* cache) {
    ResolveAddr(resolver, addr, 80, result, cache);
}
