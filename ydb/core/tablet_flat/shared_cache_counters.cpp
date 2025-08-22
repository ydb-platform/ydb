#include "shared_cache_counters.h"

namespace NKikimr::NSharedCache {

TSharedPageCacheCounters::TSharedPageCacheCounters(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters)
    : Counters(counters)
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

} // namespace NKikimr::NSharedCache
