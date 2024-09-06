#include "flat_range_cache.h"
#include "util_fmt_cell.h"

namespace NKikimr {
namespace NTable {

TKeyRangeCacheNeedGCList::~TKeyRangeCacheNeedGCList()
{ }

void TKeyRangeCacheNeedGCList::Add(TKeyRangeCache* cache) {
    TListItem* item = static_cast<TListItem*>(cache);
    if (item->Empty()) {
        List.PushBack(item);
    }
}

void TKeyRangeCacheNeedGCList::RunGC() {
    while (List) {
        auto* item = List.Front();
        // Note: we call CollectGarbage while item is still in the list
        // This way item is not re-added by internal invalidations
        item->CollectGarbage();
        List.Remove(item);
    }
}

TKeyRangeCache::TKeyRangeCache(const TKeyCellDefaults& keyDefaults, const TKeyRangeCacheConfig& config,
        const TIntrusivePtr<TKeyRangeCacheNeedGCList>& gcList)
    : KeyCellDefaults(keyDefaults)
    , Config(config)
    , GCList(gcList)
    , Pool(new TSpecialMemoryPool())
    , Entries(TKeyRangeEntryCompare(KeyCellDefaults.Types), TAllocator(&UsedHeapMemory))
{ }

TKeyRangeCache::~TKeyRangeCache()
{ }

TArrayRef<TCell> TKeyRangeCache::AllocateArrayCopy(TSpecialMemoryPool* pool, TArrayRef<const TCell> key) {
    if (!key) {
        return { };
    }

    TCell* rawPtr = static_cast<TCell*>(pool->Allocate(sizeof(TCell) * key.size()));
    TCell* nextCell = rawPtr;
    for (const TCell& cell : key) {
        new(nextCell++) TCell(cell);
    }

    return TArrayRef<TCell>(rawPtr, key.size());
}

TCell TKeyRangeCache::AllocateCellCopy(TSpecialMemoryPool* pool, const TCell& cell) {
    if (TCell::CanInline(cell.Size())) {
        TCell copy(cell.Data(), cell.Size());
        Y_DEBUG_ABORT_UNLESS(copy.IsInline());
        return copy;
    }

    void* rawData = pool->Allocate(cell.Size());
    if (cell.Size()) {
        ::memcpy(rawData, cell.Data(), cell.Size());
    }
    TCell copy((const char*)rawData, cell.Size());
    Y_DEBUG_ABORT_UNLESS(!copy.IsInline());
    return copy;
}

TArrayRef<TCell> TKeyRangeCache::AllocateKey(TArrayRef<const TCell> key) {
    if (!key) {
        return { };
    }

    ++Stats_.Allocations;
    if (GCList) {
        GCList->Add(this);
    }

    auto copy = AllocateArrayCopy(Pool.Get(), key);
    for (TCell& cell : copy) {
        if (!cell.IsInline()) {
            cell = AllocateCellCopy(Pool.Get(), cell);
        }
    }
    return copy;
}

void TKeyRangeCache::DeallocateKey(TArrayRef<TCell> key) {
    if (!key) {
        return;
    }

    ++Stats_.Deallocations;
    if (GCList) {
        GCList->Add(this);
    }

    size_t index = key.size() - 1;
    do {
        TCell cell = key[index];
        if (!cell.IsInline()) {
            Pool->Deallocate((void*)cell.Data(), cell.Size());
        }
    } while (index--);

    Pool->Deallocate((void*)key.data(), sizeof(TCell) * key.size());
}

void TKeyRangeCache::ExtendLeft(const_iterator it, TArrayRef<TCell> newLeft, bool leftInclusive, TRowVersion version) {
    Y_DEBUG_ABORT_UNLESS(it != end());
    TKeyRangeEntryLRU& entry = const_cast<TKeyRangeEntryLRU&>(*it);
    DeallocateKey(entry.FromKey);
    entry.FromKey = newLeft;
    entry.FromInclusive = leftInclusive;
    entry.MaxVersion = ::Max(entry.MaxVersion, version);
}

void TKeyRangeCache::ExtendRight(const_iterator it, TArrayRef<TCell> newRight, bool rightInclusive, TRowVersion version) {
    Y_DEBUG_ABORT_UNLESS(it != end());
    TKeyRangeEntryLRU& entry = const_cast<TKeyRangeEntryLRU&>(*it);
    DeallocateKey(entry.ToKey);
    entry.ToKey = newRight;
    entry.ToInclusive = rightInclusive;
    entry.MaxVersion = ::Max(entry.MaxVersion, version);
}

TKeyRangeCache::const_iterator TKeyRangeCache::Merge(const_iterator left, const_iterator right, TRowVersion version) {
    Y_DEBUG_ABORT_UNLESS(left != end());
    Y_DEBUG_ABORT_UNLESS(right != end());
    Y_DEBUG_ABORT_UNLESS(left != right);
    TKeyRangeEntry rightCopy = *right;
    Entries.erase(right);
    DeallocateKey(rightCopy.FromKey);
    ExtendRight(left, rightCopy.ToKey, rightCopy.ToInclusive, ::Max(rightCopy.MaxVersion, version));
    if (GCList) {
        GCList->Add(this);
    }
    return left;
}

TKeyRangeCache::const_iterator TKeyRangeCache::Add(TKeyRangeEntry entry) {
    auto res = Entries.emplace(entry.FromKey, entry.ToKey, entry.FromInclusive, entry.ToInclusive, entry.MaxVersion);
    Y_DEBUG_ABORT_UNLESS(res.second);
    TKeyRangeEntryLRU& newEntry = const_cast<TKeyRangeEntryLRU&>(*res.first);
    Fresh.PushBack(&newEntry);
    if (GCList) {
        GCList->Add(this);
    }
    return res.first;
}

void TKeyRangeCache::Invalidate(const_iterator it) {
    Y_DEBUG_ABORT_UNLESS(it != end());
    TKeyRangeEntry entryCopy = *it;
    Entries.erase(it);
    DeallocateKey(entryCopy.FromKey);
    DeallocateKey(entryCopy.ToKey);
    if (GCList) {
        GCList->Add(this);
    }
}

void TKeyRangeCache::InvalidateKey(const_iterator it, TArrayRef<const TCell> key) {
    Y_DEBUG_ABORT_UNLESS(it != end());
    TKeyRangeEntryLRU& entry = const_cast<TKeyRangeEntryLRU&>(*it);
    int cmp = Entries.key_comp().CompareKeys(entry.FromKey, key);
    Y_DEBUG_ABORT_UNLESS(cmp <= 0);
    if (cmp == 0) {
        Y_DEBUG_ABORT_UNLESS(entry.FromInclusive);
        Invalidate(it);
        return;
    }
    DeallocateKey(entry.ToKey);
    entry.ToKey = AllocateKey(key);
    entry.ToInclusive = false;
}

void TKeyRangeCache::Touch(const_iterator it) {
    Y_DEBUG_ABORT_UNLESS(it != end());
    TKeyRangeEntryLRU& entry = const_cast<TKeyRangeEntryLRU&>(*it);
    Fresh.PushBack(&entry);
}

void TKeyRangeCache::EvictOld() {
    while (OverMemoryLimit() && LRU) {
        auto* entry = LRU.PopFront();
        auto it = Entries.find(*entry);
        Y_DEBUG_ABORT_UNLESS(it != end());
        Invalidate(it);
        ++Stats_.Evictions;
    }
}

void TKeyRangeCache::CollectGarbage() {
    while (Fresh) {
        // The first entry in Fresh will become the last entry in LRU
        LRU.PushBack(Fresh.PopBack());
    }
    EvictOld();

    if (GetTotalAllocated() <= Config.MaxBytes / 2) {
        // Don't bother with garbage collection yet
        return;
    }

    if (Pool->TotalUsed() < Pool->TotalGarbage() / 2) {
        THolder<TSpecialMemoryPool> newPool = MakeHolder<TSpecialMemoryPool>();
        newPool->Reserve(Pool->TotalUsed());
        for (auto& constEntry : Entries) {
            auto& entry = const_cast<TKeyRangeEntryLRU&>(constEntry);
            if (entry.FromKey) {
                entry.FromKey = AllocateArrayCopy(newPool.Get(), entry.FromKey);
                for (auto& cell : entry.FromKey) {
                    if (!cell.IsInline()) {
                        cell = AllocateCellCopy(newPool.Get(), cell);
                    }
                }
            }
            if (entry.ToKey) {
                entry.ToKey = AllocateArrayCopy(newPool.Get(), entry.ToKey);
                for (auto& cell : entry.ToKey) {
                    if (!cell.IsInline()) {
                        cell = AllocateCellCopy(newPool.Get(), cell);
                    }
                }
            }
        }
        Pool = std::move(newPool);
        ++Stats_.GarbageCollections;
    }
}

void TKeyRangeCache::TDumpRanges::DumpTo(IOutputStream& out) const {
    out << "TKeyRangeCache{";
    bool first = true;
    for (const auto& entry : Self->Entries) {
        out << (first ? " " : ", ");
        out << (entry.FromInclusive ? "[" : "(");
        out << NFmt::TPrintableTypedCells(entry.FromKey, Self->KeyCellDefaults.BasicTypes());
        out << ", ";
        out << NFmt::TPrintableTypedCells(entry.ToKey, Self->KeyCellDefaults.BasicTypes());
        out << (entry.ToInclusive ? "]" : ")");
        first = false;
    }
    out << " }";
}

}
}

template<>
void Out<NKikimr::NTable::TKeyRangeCache::TDumpRanges>(
        IOutputStream& out,
        const NKikimr::NTable::TKeyRangeCache::TDumpRanges& value)
{
    value.DumpTo(out);
}
