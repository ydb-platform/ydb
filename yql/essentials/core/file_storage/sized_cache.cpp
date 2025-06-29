#include "sized_cache.h"

#include <util/system/guard.h>

namespace NYql {

TSizedCache::TSizedCache(size_t maxEntries, ui64 maxSize)
    : Cache_(Max<size_t>())
    , MaxEntries_(maxEntries)
    , MaxSize_(maxSize)
{
    Y_ASSERT(MaxEntries_ > 0);
    Y_ASSERT(MaxSize_ > 0);
}

void TSizedCache::Put(const TIntrusivePtr<ICacheObj>& obj, bool lock) {
    const TString name = obj->GetName();
    const ui64 size = obj->GetSize();

    auto guard = Guard(Lock_);
    auto it = Cache_.Find(name);
    if (it == Cache_.End()) {
        Cache_.Insert(name, TEntry{obj, lock ? 1U : 0U});
        CurrentSize_ += size;
    } else if (lock) {
        ++it->Locks;
    }
    while (Cache_.Size() > MaxEntries_ || CurrentSize_ > MaxSize_) {
        it = Cache_.FindOldest();
        if (Cache_.End() == it || 0 != it->Locks) {
            break;
        }
        Remove(it);
    }
}

void TSizedCache::Release(const TString& name, bool remove) {
    auto guard = Guard(Lock_);
    auto it = Cache_.FindWithoutPromote(name);
    if (it == Cache_.End()) {
        Y_ASSERT(false);
    } else {
        Y_ASSERT(it->Locks);
        if (it->Locks) {
            --it->Locks;
        }
        if (!it->Locks && (remove || Cache_.Size() > MaxEntries_ || CurrentSize_ > MaxSize_)) {
            Remove(it);
        }
    }
}

TIntrusivePtr<TSizedCache::ICacheObj> TSizedCache::Get(const TString& name) {
    auto guard = Guard(Lock_);
    auto it = Cache_.Find(name);
    TIntrusivePtr<TSizedCache::ICacheObj> res;
    if (it != Cache_.End()) {
        res = it->Obj;
    }
    return res;
}

TIntrusivePtr<TSizedCache::ICacheObj> TSizedCache::Revoke(const TString& name) {
    auto guard = Guard(Lock_);
    auto it = Cache_.FindWithoutPromote(name);
    TIntrusivePtr<TSizedCache::ICacheObj> res;
    if (it != Cache_.End()) {
        res = it->Obj;
        Y_ASSERT(CurrentSize_ >= res->GetSize());
        CurrentSize_ -= res->GetSize();
        Cache_.Erase(it);
    }
    return res;
}

TMaybe<ui32> TSizedCache::GetLocks(const TString& name) const {
    auto guard = Guard(Lock_);
    auto it = Cache_.FindWithoutPromote(name);
    TMaybe<ui32> res;
    if (it != Cache_.End()) {
        res = it->Locks;
    }
    return res;
}

void TSizedCache::Remove(TCache::TIterator it) {
    TIntrusivePtr<TSizedCache::ICacheObj> obj = it->Obj;
    Y_ASSERT(CurrentSize_ >= obj->GetSize());
    CurrentSize_ -= obj->GetSize();
    Cache_.Erase(it);
    // Dismissing may take a long time. Unlock temporary
    auto unguard = Unguard(Lock_);
    obj->Dismiss();
}

} // NYql


