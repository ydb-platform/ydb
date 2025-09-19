#pragma once

#include "kqp_vector_level_cache.h"

#include <ydb/library/actors/core/actor.h>
#include <util/system/spinlock.h>

namespace NKikimr {
namespace NKqp {

/**
 * Global singleton cache manager for vector level table data.
 * Provides thread-safe access to the cache across all vector resolve actors.
 */
class TKqpVectorLevelCacheManager {
public:
    static TKqpVectorLevelCacheManager& Instance();

    /**
     * Get the global cache instance.
     */
    TKqpVectorLevelCache& GetCache();

    /**
     * Initialize cache with custom settings.
     */
    void Initialize(size_t maxSize = 1000, TDuration ttl = TDuration::Hours(1));

private:
    TKqpVectorLevelCacheManager() = default;
    
    mutable TAdaptiveLock Lock;
    std::unique_ptr<TKqpVectorLevelCache> Cache;
    bool Initialized = false;
};

} // namespace NKqp
} // namespace NKikimr