#include "cache.h"

#include "async.h"
#include "coro.h"
#include "helpers.h"

#include <library/cpp/cache/cache.h>

#include <library/cpp/coroutine/engine/impl.h>
#include <library/cpp/coroutine/engine/events.h>

using namespace NAddr;
using namespace NAsyncDns;

class TContDnsCache::TImpl {
    using TKey = std::pair<TString, ui16>;

    enum EResolveState {
        RS_NOT_RESOLVED,
        RS_IN_PROGRESS,
        RS_RESOLVED,
        RS_FAILED,
        RS_EXPIRED
    };

    struct TEntry: public TSimpleRefCount<TEntry> {
        TAddrs Addrs_;
        TInstant ResolveTimestamp_;
        EResolveState State_;
        TContSimpleEvent ResolvedEvent_;
        TString LastResolveError_;

        TEntry(TContExecutor* e)
            : State_(RS_NOT_RESOLVED)
            , ResolvedEvent_(e)
        {
        }
    };

    using TEntryRef = TIntrusivePtr<TEntry>;

public:
    inline TImpl(TContExecutor* e, const TCacheOptions& opts)
        : Executor_(e)
        , Opts_(opts)
        , Cache_(opts.MaxSize)
    {
    }

    inline ~TImpl() {
    }

    void LookupOrResolve(TContResolver& resolver, const TString& addr, ui16 port, TAddrs& result) {
        TKey cacheKey(addr, port);

        TEntryRef e;
        auto iter = Cache_.Find(cacheKey);
        if (iter == Cache_.End()) {
            e = MakeIntrusive<TEntry>(Executor_);
            Cache_.Insert(cacheKey, e);
        } else {
            e = iter.Value();
        }

        while (true) {
            switch (e->State_) {
                case RS_NOT_RESOLVED:
                case RS_EXPIRED:
                    e->State_ = RS_IN_PROGRESS;
                    try {
                        ResolveAddr(resolver, addr, port, result);
                    } catch (TDnsError& err) {
                        e->ResolveTimestamp_ = TInstant::Now();
                        e->LastResolveError_ = err.AsStrBuf();
                        e->State_ = RS_FAILED;
                        e->ResolvedEvent_.BroadCast();
                        throw;
                    } catch (...) {
                        // errors not related to DNS
                        e->State_ = RS_NOT_RESOLVED;
                        e->ResolvedEvent_.BroadCast();
                        throw;
                    }
                    e->ResolveTimestamp_ = TInstant::Now();
                    e->Addrs_ = result;
                    e->State_ = RS_RESOLVED;
                    e->ResolvedEvent_.BroadCast();
                    return;
                case RS_IN_PROGRESS:
                    e->ResolvedEvent_.WaitI();
                    continue;
                case RS_RESOLVED:
                    if (e->ResolveTimestamp_ + Opts_.EntryLifetime < TInstant::Now()) {
                        e->State_ = RS_EXPIRED;
                        continue; // try and resolve again
                    }
                    result = e->Addrs_;
                    return;
                case RS_FAILED:
                    if (e->ResolveTimestamp_ + Opts_.NotFoundLifetime < TInstant::Now()) {
                        e->State_ = RS_EXPIRED;
                        continue; // try and resolve again
                    }
                    ythrow TDnsError() << e->LastResolveError_;
                default:
                    Y_FAIL("Bad state, shoult not get here");
            };
        }
    }

private:
    TContExecutor* Executor_;
    const TCacheOptions Opts_;
    TLRUCache<TKey, TEntryRef> Cache_;
};

TContDnsCache::TContDnsCache(TContExecutor* e, const NAsyncDns::TCacheOptions& opts)
    : I_(MakeHolder<TImpl>(e, opts))
{
}

TContDnsCache::~TContDnsCache() {
}

void TContDnsCache::LookupOrResolve(TContResolver& resolver, const TString& addr, ui16 port, TVector<NAddr::IRemoteAddrRef>& result) {
    return I_->LookupOrResolve(resolver, addr, port, result);
}

void TContDnsCache::LookupOrResolve(TContResolver& resolver, const TString& addr, TVector<NAddr::IRemoteAddrRef>& result) {
    LookupOrResolve(resolver, addr, 80, result);
}
