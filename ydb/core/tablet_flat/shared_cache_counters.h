#pragma once

#include <ydb/core/protos/shared_cache.pb.h>
#include <ydb/core/util/cache_cache.h>

namespace NKikimr::NSharedCache {

struct TSharedPageCacheCounters final : public TAtomicRefCount<TSharedPageCacheCounters> {
    using TCounterPtr = ::NMonitoring::TDynamicCounters::TCounterPtr;
    using TReplacementPolicy = NKikimrSharedCache::TReplacementPolicy;

    const TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;

    // lru cache counters:
    const TCounterPtr FreshBytes;
    const TCounterPtr StagingBytes;
    const TCounterPtr WarmBytes;

    // page counters:
    const TCounterPtr MemLimitBytes;
    const TCounterPtr ConfigLimitBytes;
    const TCounterPtr ActivePages;
    const TCounterPtr ActiveBytes;
    const TCounterPtr ActiveLimitBytes;
    const TCounterPtr PassivePages;
    const TCounterPtr PassiveBytes;
    const TCounterPtr RequestedPages;
    const TCounterPtr RequestedBytes;
    const TCounterPtr CacheHitPages;
    const TCounterPtr CacheHitBytes;
    const TCounterPtr CacheMissPages;
    const TCounterPtr CacheMissBytes;
    const TCounterPtr LoadInFlyPages;
    const TCounterPtr LoadInFlyBytes;
    const TCounterPtr TryKeepInMemoryBytes;

    // page collection counters:
    const TCounterPtr PageCollections;
    const TCounterPtr Owners;
    const TCounterPtr PageCollectionOwners;

    // request counters:
    const TCounterPtr PendingRequests;
    const TCounterPtr SucceedRequests;
    const TCounterPtr FailedRequests;

    explicit TSharedPageCacheCounters(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters);

    TCounterPtr ReplacementPolicySize(TReplacementPolicy policy);
};

} // namespace NKikimr::NSharedCache
