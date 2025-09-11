#include "kqp_vector_level_cache_manager.h"

namespace NKikimr {
namespace NKqp {

TKqpVectorLevelCacheManager& TKqpVectorLevelCacheManager::Instance() {
    static TKqpVectorLevelCacheManager instance;
    return instance;
}

TKqpVectorLevelCache& TKqpVectorLevelCacheManager::GetCache() {
    TGuard<TAdaptiveLock> guard(Lock);
    
    if (!Initialized) {
        Initialize();
    }
    
    return *Cache;
}

void TKqpVectorLevelCacheManager::Initialize(size_t maxSize, TDuration ttl) {
    TGuard<TAdaptiveLock> guard(Lock);
    
    if (!Initialized) {
        Cache = std::make_unique<TKqpVectorLevelCache>(maxSize, ttl);
        Initialized = true;
    }
}

} // namespace NKqp
} // namespace NKikimr