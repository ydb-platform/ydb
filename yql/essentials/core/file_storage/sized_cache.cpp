#include "sized_cache.h"

#include <util/system/guard.h>

namespace NYql {

TSizedCache::TSizedCache(size_t maxEntries, ui64 maxSize)
    : Cache(Max<size_t>())
    , MaxEntries(maxEntries)
    , MaxSize(maxSize)
{
    Y_ASSERT(MaxEntries > 0);
    Y_ASSERT(MaxSize > 0);
}

void TSizedCache::Put(const TIntrusivePtr<ICacheObj>& obj, bool lock) {
    const TString name = obj->GetName();
    const ui64 size = obj->GetSize();

    auto guard = Guard(Lock);
    auto it = Cache.Find(name);
    if (it == Cache.End()) {
        Cache.Insert(name, TEntry{obj, lock ? 1U : 0U});
        CurrentSize += size;
    } else if (lock) {
        ++it->Locks;
    }
    while (Cache.Size() > MaxEntries || CurrentSize > MaxSize) {
        it = Cache.FindOldest();
        if (Cache.End() == it || 0 != it->Locks) {
            break;
        }
        Remove(it);
    }
}

void TSizedCache::Release(const TString& name, bool remove) {
    auto guard = Guard(Lock);
    auto it = Cache.FindWithoutPromote(name);
    if (it == Cache.End()) {
        Y_ASSERT(false);
    } else {
        Y_ASSERT(it->Locks);
        if (it->Locks) {
            --it->Locks;
        }
        if (!it->Locks && (remove || Cache.Size() > MaxEntries || CurrentSize > MaxSize)) {
            Remove(it);
        }
    }
}

TIntrusivePtr<TSizedCache::ICacheObj> TSizedCache::Get(const TString& name) {
    auto guard = Guard(Lock);
    auto it = Cache.Find(name);
    TIntrusivePtr<TSizedCache::ICacheObj> res;
    if (it != Cache.End()) {
        res = it->Obj;
    }
    return res;
}

TIntrusivePtr<TSizedCache::ICacheObj> TSizedCache::Revoke(const TString& name) {
    auto guard = Guard(Lock);
    auto it = Cache.FindWithoutPromote(name);
    TIntrusivePtr<TSizedCache::ICacheObj> res;
    if (it != Cache.End()) {
        res = it->Obj;
        Y_ASSERT(CurrentSize >= res->GetSize());
        CurrentSize -= res->GetSize();
        Cache.Erase(it);
    }
    return res;
}

TMaybe<ui32> TSizedCache::GetLocks(const TString& name) const {
    auto guard = Guard(Lock);
    auto it = Cache.FindWithoutPromote(name);
    TMaybe<ui32> res;
    if (it != Cache.End()) {
        res = it->Locks;
    }
    return res;
}

void TSizedCache::Remove(TCache::TIterator it) {
    TIntrusivePtr<TSizedCache::ICacheObj> obj = it->Obj;
    Y_ASSERT(CurrentSize >= obj->GetSize());
    CurrentSize -= obj->GetSize();
    Cache.Erase(it);
    // Dismissing may take a long time. Unlock temporary
    auto unguard = Unguard(Lock);
    obj->Dismiss();
}

} // NYql


