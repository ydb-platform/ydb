#include "shared_cache_counters.h"

namespace NKikimr::NSharedCache {

TSharedPageCacheCounters::TSharedPageCacheCounters(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters)
    : Counters(counters)
    // lru cache counters:
    , FreshBytes(counters->GetCounter("fresh"))
    , StagingBytes(counters->GetCounter("staging"))
    , WarmBytes(counters->GetCounter("warm"))
    // page counters:
    , MemLimitBytes(counters->GetCounter("MemLimitBytes"))
    , ConfigLimitBytes(counters->GetCounter("ConfigLimitBytes"))
    , ActivePages(counters->GetCounter("ActivePages"))
    , ActiveBytes(counters->GetCounter("ActiveBytes"))
    , ActiveLimitBytes(counters->GetCounter("ActiveLimitBytes"))
    , PassivePages(counters->GetCounter("PassivePages"))
    , PassiveBytes(counters->GetCounter("PassiveBytes"))
    , RequestedPages(counters->GetCounter("RequestedPages", true))
    , RequestedBytes(counters->GetCounter("RequestedBytes", true))
    , CacheHitPages(counters->GetCounter("CacheHitPages", true))
    , CacheHitBytes(counters->GetCounter("CacheHitBytes", true))
    , CacheMissPages(counters->GetCounter("CacheMissPages", true))
    , CacheMissBytes(counters->GetCounter("CacheMissBytes", true))
    , LoadInFlyPages(counters->GetCounter("LoadInFlyPages"))
    , LoadInFlyBytes(counters->GetCounter("LoadInFlyBytes"))
    , TryKeepInMemoryBytes(counters->GetCounter("TryKeepInMemoryBytes"))
    // page collection counters:
    , PageCollections(counters->GetCounter("PageCollections"))
    , Owners(counters->GetCounter("Owners"))
    , PageCollectionOwners(counters->GetCounter("PageCollectionOwners"))
    // request counters:
    , PendingRequests(counters->GetCounter("PendingRequests"))
    , SucceedRequests(counters->GetCounter("SucceedRequests", true))
    , FailedRequests(counters->GetCounter("FailedRequests", true))
{ }

TSharedPageCacheCounters::TCounterPtr TSharedPageCacheCounters::ReplacementPolicySize(TReplacementPolicy policy) {
    return Counters->GetCounter(TStringBuilder() << "ReplacementPolicySize/" << policy);
}

} // namespace NKikimr::NSharedCache
