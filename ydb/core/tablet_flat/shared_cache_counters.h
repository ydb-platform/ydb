#pragma once

#include "defs.h"
#include <ydb/core/protos/shared_cache.pb.h>

namespace NKikimr::NSharedCache {

struct TSharedPageCacheCounters final : public TAtomicRefCount<TSharedPageCacheCounters> {
    using TCounterPtr = ::NMonitoring::TDynamicCounters::TCounterPtr;
    using TReplacementPolicy = NKikimrSharedCache::TReplacementPolicy;

    const TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;

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
    const TCounterPtr CacheMissInMemoryPages;
    const TCounterPtr CacheMissInMemoryBytes;
    const TCounterPtr LoadInFlyPages;
    const TCounterPtr LoadInFlyBytes;
    const TCounterPtr TargetInMemoryBytes;
    const TCounterPtr ActiveInMemoryBytes;
    const TCounterPtr EvictedPages;
    const TCounterPtr EvictedBytes;
    const TCounterPtr S3FIFOEvictOps;

    // page collection counters:
    const TCounterPtr PageCollections;
    const TCounterPtr Owners;
    const TCounterPtr PageCollectionOwners;
    // Metadata charge included in the cache's total memory consumption
    const TCounterPtr MetadataBytes;
    // Current collection-entry reference footprints, excluding request/event-held descriptors
    // Shared Raw allocations, executor attribution and page bodies can overlap these bytes
    const TCounterPtr CollectionMetadataBytes;
    const TCounterPtr DetachedCollectionMetadataBytes;
    const TCounterPtr DetachedPageCollections;

    // request counters:
    const TCounterPtr PendingRequests;
    const TCounterPtr SucceedRequests;
    const TCounterPtr FailedRequests;

    explicit TSharedPageCacheCounters(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters);
};

} // namespace NKikimr::NSharedCache
