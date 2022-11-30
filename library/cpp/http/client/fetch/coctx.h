#pragma once

#include <library/cpp/coroutine/engine/impl.h>

#include <util/thread/singleton.h>

namespace NAsyncDns {
    class TContResolver;
    class TContDnsCache;
}

namespace NHttpFetcher {
    struct TCoCtx {
        TContExecutor* Executor;
        NAsyncDns::TContResolver* Resolver;
        NAsyncDns::TContDnsCache* DnsCache;

        TCoCtx(TContExecutor* executor, NAsyncDns::TContResolver* resolver, NAsyncDns::TContDnsCache* dnsCache = nullptr)
            : Executor(executor)
            , Resolver(resolver)
            , DnsCache(dnsCache)
        {
        }

        TCont* Cont() {
            return Executor->Running();
        }
    };

    inline TCoCtx*& CoCtx() {
        return *FastTlsSingletonWithPriority<TCoCtx*, 0>();
    }

    class TCoCtxSetter {
    public:
        TCoCtxSetter(TContExecutor* executor, NAsyncDns::TContResolver* resolver, NAsyncDns::TContDnsCache* dnsCache = nullptr)
            : Instance(executor, resolver, dnsCache)
        {
            Y_VERIFY(!CoCtx(), "coCtx already exists");
            CoCtx() = &Instance;
        }

        ~TCoCtxSetter() {
            CoCtx() = nullptr;
        }

    private:
        TCoCtx Instance;
    };
}
