#pragma once
#include "defs.h"

namespace NKikimr::NCache {

template <typename TItem>
struct ICacheCache {
    // returns evicted elements as list
    // in most common scenarios it has only one item 
    virtual TIntrusiveList<TItem> EvictNext() Y_WARN_UNUSED_RESULT = 0;

    // returns evicted elements as list
    virtual TIntrusiveList<TItem> Touch(TItem *item) Y_WARN_UNUSED_RESULT = 0;

    virtual void Erase(TItem *item) = 0;

    // WARN: do not evict items
    virtual void UpdateLimit(ui64 limit) = 0;

    virtual ui64 GetSize() const = 0;

    virtual TString Dump() const {
        return {};
    }

    virtual ~ICacheCache() = default;
};

}
