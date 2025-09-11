#include "kqp_vector_level_cache.h"

#include <ydb/library/actors/core/log.h>

namespace NKikimr {
namespace NKqp {

std::unique_ptr<NKikimr::NKMeans::IClusters> TCachedClusterLevel::CreateClusters(TString& error) const {
    auto clusters = NKikimr::NKMeans::CreateClusters(IndexSettings, 0, error);
    if (!clusters) {
        return nullptr;
    }
    
    if (!clusters->SetClusters(TVector<TString>(ClusterData))) {
        error = "Failed to set cached cluster data";
        return nullptr;
    }
    
    return clusters;
}

std::shared_ptr<TKqpVectorLevelCache::TCacheValue> TKqpVectorLevelCache::Get(const TCacheKey& key) {
    TGuard<TAdaptiveLock> guard(Lock);
    
    auto keyStr = key.ToString();
    auto it = Cache.find(keyStr);
    if (it == Cache.end()) {
        ++Misses;
        return nullptr;
    }
    
    // Check if expired
    if (TInstant::Now() > it->second.ExpiredAt) {
        Cache.erase(it);
        ++Misses;
        return nullptr;
    }
    
    ++Hits;
    return it->second.Value;
}

void TKqpVectorLevelCache::Put(const TCacheKey& key, std::shared_ptr<TCacheValue> value) {
    if (!value) {
        return;
    }
    
    TGuard<TAdaptiveLock> guard(Lock);
    
    // Cleanup expired entries if cache is getting full
    if (Cache.size() >= MaxSize) {
        CleanupExpired();
        
        // If still full after cleanup, remove oldest entries
        if (Cache.size() >= MaxSize) {
            // Simple eviction: remove first entry found
            // In a production implementation, we'd want proper LRU eviction
            if (!Cache.empty()) {
                Cache.erase(Cache.begin());
            }
        }
    }
    
    auto keyStr = key.ToString();
    auto expiredAt = TInstant::Now() + Ttl;
    
    Cache.emplace(keyStr, TCacheEntry(std::move(value), expiredAt));
}

void TKqpVectorLevelCache::InvalidateTable(const TString& tablePath) {
    TGuard<TAdaptiveLock> guard(Lock);
    
    auto it = Cache.begin();
    while (it != Cache.end()) {
        if (it->first.StartsWith(tablePath + ":")) {
            it = Cache.erase(it);
        } else {
            ++it;
        }
    }
}

void TKqpVectorLevelCache::Clear() {
    TGuard<TAdaptiveLock> guard(Lock);
    Cache.clear();
    Hits = 0;
    Misses = 0;
}

TKqpVectorLevelCache::TStats TKqpVectorLevelCache::GetStats() const {
    TGuard<TAdaptiveLock> guard(Lock);
    return TStats{Cache.size(), Hits, Misses};
}

void TKqpVectorLevelCache::CleanupExpired() {
    auto now = TInstant::Now();
    auto it = Cache.begin();
    while (it != Cache.end()) {
        if (now > it->second.ExpiredAt) {
            it = Cache.erase(it);
        } else {
            ++it;
        }
    }
}

} // namespace NKqp
} // namespace NKikimr