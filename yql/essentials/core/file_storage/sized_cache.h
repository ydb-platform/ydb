#pragma once

#include <library/cpp/cache/cache.h>

#include <util/system/spinlock.h>
#include <util/generic/string.h>
#include <util/generic/ylimits.h>
#include <util/generic/ptr.h>
#include <util/generic/maybe.h>

namespace NYql {

/*
 Cache with limits by count and by occupied size.
 All added objects must have unmodifiable size.
 The same object can be added multiple times. If it is added with lock then
 it should be Release()-ed the same number of times.
 Locked objects are not removed even if total count/size exceeds the limit.
*/
class TSizedCache {
public:
    struct ICacheObj: public TThrRefBase {
        // Unique object identifier
        virtual TString GetName() = 0;
        // Object size
        virtual ui64 GetSize() = 0;
        // Called when object is removed from the cache. Should cleanup occupied resources
        virtual void Dismiss() = 0;
    };

public:
    // Constructs the cache with the specified limits.
    // maxEntries and maxSize must be greater than zero
    TSizedCache(size_t maxEntries, ui64 maxSize);
    ~TSizedCache() = default;

    // Put an object to the cache. If lock = true the increments object lock count
    void Put(const TIntrusivePtr<ICacheObj>& obj, bool lock = true);
    // Decrements object lock count. If lock count becomes zero and total count/size exceeds the limit
    // then object is removed from the cache immediately
    void Release(const TString& name, bool remove = false);
    // Returns the object if it exists in the cache
    TIntrusivePtr<ICacheObj> Get(const TString& name);
    // Removes and returns the object if it exists in the cache
    TIntrusivePtr<ICacheObj> Revoke(const TString& name);
    // Returns count of object locks, or empty if object doesn't belong to the cache
    TMaybe<ui32> GetLocks(const TString& name) const;
    // Total size of all objects in the cache
    ui64 GetOccupiedSize() const {
        return CurrentSize;
    }
    // Total count of objects in the cache
    size_t GetCount() const {
        return Cache.Size();
    }
private:
    struct TEntry {
        TIntrusivePtr<ICacheObj> Obj;
        ui32 Locks;
    };
    using TCache = TLRUCache<TString, TEntry>;

    void Remove(TCache::TIterator it);

private:
    TCache Cache;
    size_t MaxEntries = 0;
    ui64 MaxSize = 0;
    ui64 CurrentSize = 0;
    TAdaptiveLock Lock;
};

} // NYql


