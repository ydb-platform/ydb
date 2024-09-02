#pragma once
#include "defs.h"

namespace NKikimr::NCache {

template <typename TItem>
struct ICacheCache {
    virtual TItem* EvictNext() Y_WARN_UNUSED_RESULT = 0;

    // returns evicted elements as list
    virtual TIntrusiveList<TItem> Touch(TItem *item) Y_WARN_UNUSED_RESULT = 0;

    virtual void Erase(TItem *item) = 0;

    // WARN: do not evict items
    virtual void UpdateCacheSize(ui64 cacheSize) = 0;

    virtual ~ICacheCache() = default;
};

}
