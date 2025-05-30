#pragma once

#include "defs.h"

#include <ydb/core/tx/scheme_cache/scheme_cache.h>

namespace NKikimr {

IActor* CreateSchemeBoardSchemeCache(NSchemeCache::TSchemeCacheConfig* config);

namespace NSchemeCache {
    struct TSchemeCacheNavigateContext : TAtomicRefCount<TSchemeCacheNavigateContext>, TNonCopyable {
        TActorId Sender;
        ui64 Cookie;
        ui64 WaitCounter;
        TAutoPtr<TSchemeCacheNavigate> Request;
        const TInstant CreatedAt;
        TIntrusivePtr<TDomainInfo> ResolvedDomainInfo; // resolved from DatabaseName

        struct TEntryFallbackInfo {
            bool IsImplicit = false;
            TMaybe<size_t> FallbackEntryIndex;
        };

        // Vector implements fallback entry logic:
        // - Each element corresponds to an entry from ResultSet
        // - FallbackEntryIndex refers to the fallback entry
        // - IsImplicit == true when the entry is a fallback entry
        TVector<TEntryFallbackInfo> EntriesFallbackInfo;
        bool HasSysViewEntries = false;

        TSchemeCacheNavigateContext(const TActorId& sender, ui64 cookie, TAutoPtr<TSchemeCacheNavigate> request, const TInstant& now = TInstant::Now())
            : Sender(sender)
            , Cookie(cookie)
            , WaitCounter(0)
            , Request(request)
            , CreatedAt(now)
            , EntriesFallbackInfo(Request->ResultSet.size())
        {}
    };

    struct TSchemeCacheRequestContext : TAtomicRefCount<TSchemeCacheRequestContext>, TNonCopyable {
        TActorId Sender;
        ui64 Cookie;
        ui64 WaitCounter;
        TAutoPtr<TSchemeCacheRequest> Request;
        const TInstant CreatedAt;
        TIntrusivePtr<TDomainInfo> ResolvedDomainInfo; // resolved from DatabaseName

        TSchemeCacheRequestContext(const TActorId& sender, ui64 cookie, TAutoPtr<TSchemeCacheRequest> request, const TInstant& now = TInstant::Now())
            : Sender(sender)
            , Cookie(cookie)
            , WaitCounter(0)
            , Request(request)
            , CreatedAt(now)
        {}
    }; // NSchemeCache
}

} // NKikimr
