#pragma once

#include "defs.h"
#include "flat_row_nulls.h"

#include <ydb/core/base/row_version.h>
#include <ydb/core/scheme/scheme_tablecell.h>

#include <ydb/library/actors/util/memory_track.h>

#include <util/generic/deque.h>
#include <util/generic/intrlist.h>

#include <set>

namespace NKikimr {
namespace NTable {

static constexpr char MemoryLabelKeyRangeCache[] = "Tablet/TKeyRangeCache";

struct TKeyRangeEntry {
    TArrayRef<TCell> FromKey;
    TArrayRef<TCell> ToKey;
    bool FromInclusive;
    bool ToInclusive;
    TRowVersion MaxVersion;

    TKeyRangeEntry() = default;

    TKeyRangeEntry(TArrayRef<TCell> fromKey, TArrayRef<TCell> toKey, bool fromInclusive, bool toInclusive, TRowVersion maxVersion)
        : FromKey(fromKey)
        , ToKey(toKey)
        , FromInclusive(fromInclusive)
        , ToInclusive(toInclusive)
        , MaxVersion(maxVersion)
    { }
};

class TKeyRangeEntryCompare {
public:
    typedef void is_transparent;

    explicit TKeyRangeEntryCompare(TArrayRef<const NScheme::TTypeInfoOrder> keyTypes)
        : KeyTypes(keyTypes)
    { }

    /**
     * Suitable for exact searches (compares with left/right borders)
     */
    struct TSearchPoint {
        TArrayRef<const TCell> Key;
    };

    /**
     * Suitable for lower bound (compares with the right border only)
     */
    struct TLowerBound {
        TArrayRef<const TCell> Key;
    };

    /**
     * Suitable for lower bound in reverse (compares with the left border only)
     */
    struct TLowerBoundReverse {
        TArrayRef<const TCell> Key;
    };

    int CompareKeys(TArrayRef<const TCell> a, TArrayRef<const TCell> b) const noexcept {
        Y_DEBUG_ABORT_UNLESS(a.size() == KeyTypes.size());
        Y_DEBUG_ABORT_UNLESS(b.size() == KeyTypes.size());
        return CompareTypedCellVectors(a.data(), b.data(), KeyTypes.data(), KeyTypes.size());
    }

    template<class TPoint>
    bool PointLessThanLeftBorder(const TPoint& a, const TKeyRangeEntry& b) const noexcept {
        Y_DEBUG_ABORT_UNLESS(a.Key.size() == KeyTypes.size());
        Y_DEBUG_ABORT_UNLESS(b.FromKey.size() == KeyTypes.size());
        if (int cmp = CompareTypedCellVectors(a.Key.data(), b.FromKey.data(), KeyTypes.data(), KeyTypes.size())) {
            return cmp < 0;
        }
        return !b.FromInclusive;
    }

    template<class TPoint>
    bool RightBorderLessThanPoint(const TKeyRangeEntry& a, const TPoint& b) const noexcept {
        Y_DEBUG_ABORT_UNLESS(b.Key.size() == KeyTypes.size());
        if (!a.ToKey) {
            // the left ends at +inf
            return false;
        }
        Y_DEBUG_ABORT_UNLESS(a.ToKey.size() == KeyTypes.size());
        if (int cmp = CompareTypedCellVectors(a.ToKey.data(), b.Key.data(), KeyTypes.data(), KeyTypes.size())) {
            return cmp < 0;
        }
        return !a.ToInclusive;
    }

    bool operator()(const TSearchPoint& a, const TKeyRangeEntry& b) const noexcept {
        return PointLessThanLeftBorder(a, b);
    }

    bool operator()(const TKeyRangeEntry& a, const TSearchPoint& b) const noexcept {
        return RightBorderLessThanPoint(a, b);
    }

    bool operator()(const TKeyRangeEntry& a, const TLowerBound& b) const noexcept {
        return RightBorderLessThanPoint(a, b);
    }

    bool operator()(const TLowerBoundReverse& a, const TKeyRangeEntry& b) const noexcept {
        return PointLessThanLeftBorder(a, b);
    }

    bool operator()(const TKeyRangeEntry& a, const TKeyRangeEntry& b) const noexcept {
        Y_DEBUG_ABORT_UNLESS(b.FromKey.size() == KeyTypes.size());
        if (!a.ToKey) {
            // the left ends at +inf
            return false;
        }
        Y_DEBUG_ABORT_UNLESS(a.ToKey.size() == KeyTypes.size());
        if (int cmp = CompareTypedCellVectors(a.ToKey.data(), b.FromKey.data(), KeyTypes.data(), KeyTypes.size())) {
            return cmp < 0;
        }
        return !a.ToInclusive || !b.FromInclusive;
    }

private:
    const TArrayRef<const NScheme::TTypeInfoOrder> KeyTypes;
};

struct TKeyRangeEntryLRU
    : public TIntrusiveListItem<TKeyRangeEntryLRU>
    , public TKeyRangeEntry
{
    using TKeyRangeEntry::TKeyRangeEntry;
};

class TSpecialMemoryPool {
private:
    enum : size_t {
        MinChunkSize = 8192,
    };

    class TChunk : public TIntrusiveListItem<TChunk> {
    public:
        TChunk(size_t size) noexcept
            : Next_(Data())
            , Left_(size)
        {
            Y_DEBUG_ABORT_UNLESS(AlignUp(Next_) == Next_, "Chunk data is not properly aligned");
            Y_DEBUG_ABORT_UNLESS(AlignUp(Left_) == Left_, "Chunk size is not properly aligned");
        }

        void* Allocate(size_t len) noexcept {
            Y_DEBUG_ABORT_UNLESS(AlignUp(len) == len, "Chunk allocation is not aligned");

            if (len > Left_) {
                return nullptr;
            }

            void* ptr = Next_;
            Next_ += len;
            Left_ -= len;
            return ptr;
        }

        bool Deallocate(void* ptr, size_t len) noexcept {
            Y_DEBUG_ABORT_UNLESS(AlignUp(len) == len, "Chunk allocation is not aligned");

            if (Used() < len) {
                return false;
            }

            if (ptr != (void*)(Next_ - len)) {
                return false;
            }

            Next_ -= len;
            Left_ += len;
            return true;
        }

        char* Data() const noexcept {
            return (char*)(void*)(this + 1);
        }

        size_t Used() const noexcept {
            return Next_ - (char*)(this + 1);
        }

        size_t Left() const noexcept {
            return Left_;
        }

        bool Empty() const noexcept {
            return Used() == 0;
        }

        size_t AllocSize() const noexcept {
            return Next_ - (char*)this + Left_;
        }

    private:
        char* Next_;
        size_t Left_;
    };

public:
    ~TSpecialMemoryPool() noexcept {
        Clear();
    }

    void Clear() noexcept {
        Current = nullptr;
        while (!Chunks.Empty()) {
            TChunk* chunk = Chunks.PopBack();
            DeallocateChunk(chunk);
        }
        TotalUsed_ = 0;
        TotalGarbage_ = 0;
        TotalAllocated_ = 0;
    }

    void Reserve(size_t len) {
        while (Current && Current->Left() < len) {
            if (Current->Used() == 0) {
                // Looks like we want some large allocation
                // Make a new chunk before other chunks
                TChunk* newChunk = AllocateChunk(len);
                Y_DEBUG_ABORT_UNLESS(newChunk->Left() >= len);
                newChunk->LinkBefore(Current);
                Current = newChunk;
                break;
            }
            TotalGarbage_ += Current->Left();
            if (Current == Chunks.Back()) {
                Current = nullptr;
                break;
            }
            Current = Current->Next()->Node();
            Y_DEBUG_ABORT_UNLESS(Current->Used() == 0);
        }
        if (!Current) {
            Current = AllocateChunk(len);
            Chunks.PushBack(Current);
        }
    }

    void* Allocate(size_t len) {
        len = AlignUp(len);
        Reserve(len);
        void* ptr = Current->Allocate(len);
        Y_DEBUG_ABORT_UNLESS(ptr, "Unexpected allocation failure");
        TotalUsed_ += len;
        return ptr;
    }

    void Deallocate(void* ptr, size_t len) noexcept {
        len = AlignUp(len);
        Y_DEBUG_ABORT_UNLESS(Current, "Deallocate from an empty pool, possible bug");
        Y_DEBUG_ABORT_UNLESS(TotalUsed_ >= len, "Used memory underflow, possible bug");
        TotalUsed_ -= len;
        if (!Current->Deallocate(ptr, len)) {
            // This does not match the last allocation, mark as garbage
            TotalGarbage_ += len;
            return;
        }
        while (Current != Chunks.Front() && Current->Used() == 0) {
            // Rewind back to the earliest non-free chunk
            Current = Current->Prev()->Node();
            Y_DEBUG_ABORT_UNLESS(TotalGarbage_ >= Current->Left());
            TotalGarbage_ -= Current->Left();
        }
    }

    size_t TotalUsed() const {
        return TotalUsed_;
    }

    size_t TotalGarbage() const {
        return TotalGarbage_;
    }

    size_t TotalAllocated() const {
        return TotalAllocated_;
    }

private:
    TChunk* AllocateChunk(size_t len) {
        size_t allocSize = FastClp2(sizeof(TChunk) + len);
        if (allocSize < MinChunkSize) {
            allocSize = MinChunkSize;
        }
        void* ptr = ::operator new(allocSize);
        TChunk* chunk = new(ptr) TChunk(allocSize - sizeof(TChunk));
        TotalAllocated_ += allocSize;
        NActors::NMemory::TLabel<MemoryLabelKeyRangeCache>::Add(allocSize);
        return chunk;
    }

    void DeallocateChunk(TChunk* chunk) {
        NActors::NMemory::TLabel<MemoryLabelKeyRangeCache>::Sub(chunk->AllocSize());
        chunk->~TChunk();
        ::operator delete((void*)chunk);
    }

private:
    TChunk* Current = nullptr;
    TIntrusiveList<TChunk> Chunks;
    size_t TotalUsed_ = 0;
    size_t TotalGarbage_ = 0;
    size_t TotalAllocated_ = 0;
};

struct TKeyRangeCacheConfig {
    size_t MinRows = 16; // minimum 16 rows to cache by default
    size_t MaxBytes = 128 * 1024; // maximum 128KB by default
};

class TKeyRangeCache;
struct TKeyRangeCacheNeedGCTag;

class TKeyRangeCacheNeedGCList final
    : public TSimpleRefCount<TKeyRangeCacheNeedGCList>
{
public:
    ~TKeyRangeCacheNeedGCList();

    void Add(TKeyRangeCache* cache);

    void RunGC();

private:
    using TListItem = TIntrusiveListItem<TKeyRangeCache, TKeyRangeCacheNeedGCTag>;
    TIntrusiveList<TKeyRangeCache, TKeyRangeCacheNeedGCTag> List;
};

class TKeyRangeCache final
    : public TSimpleRefCount<TKeyRangeCache>
    , public TIntrusiveListItem<TKeyRangeCache, TKeyRangeCacheNeedGCTag>
{
    friend class TKeyRangeCacheNeedGCList;

private:
    template<class T>
    class TAccountingAllocator
        : private std::allocator<T>
    {
    private:
        typedef std::allocator<T> TBase;

    public:
        typedef T value_type;

        explicit TAccountingAllocator(size_t* usedMemory) noexcept
            : UsedMemory(usedMemory)
        { }

        template<class U>
        TAccountingAllocator(const TAccountingAllocator<U>& rhs) noexcept
            : UsedMemory(rhs.GetUsedMemoryPointer())
        { }

        T* allocate(size_t n) {
            auto size = sizeof(T) * n;
            *UsedMemory += size;
            NActors::NMemory::TLabel<MemoryLabelKeyRangeCache>::Add(size);
            return TBase::allocate(n);
        }

        void deallocate(T* ptr, size_t n) noexcept {
            auto size = sizeof(T) * n;
            Y_DEBUG_ABORT_UNLESS(*UsedMemory >= size);
            *UsedMemory -= size;
            NActors::NMemory::TLabel<MemoryLabelKeyRangeCache>::Sub(size);
            return TBase::deallocate(ptr, n);
        }

        bool operator==(const TAccountingAllocator<T>& rhs) const noexcept {
            return UsedMemory == rhs.UsedMemory;
        }

        bool operator!=(const TAccountingAllocator<T>& rhs) const noexcept {
            return UsedMemory != rhs.UsedMemory;
        }

        size_t* GetUsedMemoryPointer() const noexcept {
            return UsedMemory;
        }

    private:
        size_t* const UsedMemory;
    };

    using TAllocator = TAccountingAllocator<TKeyRangeEntryLRU>;

    using TContainer = std::set<
        TKeyRangeEntryLRU,
        TKeyRangeEntryCompare,
        TAllocator>;

    using TSearchPoint = TKeyRangeEntryCompare::TSearchPoint;
    using TLowerBound = TKeyRangeEntryCompare::TLowerBound;
    using TLowerBoundReverse = TKeyRangeEntryCompare::TLowerBoundReverse;

public:
    typedef TContainer::const_iterator const_iterator;

    struct TStats {
        size_t Allocations = 0;
        size_t Deallocations = 0;
        size_t Evictions = 0;
        size_t GarbageCollections = 0;
    };

public:
    TKeyRangeCache(const TKeyCellDefaults& keyDefaults, const TKeyRangeCacheConfig& config,
        const TIntrusivePtr<TKeyRangeCacheNeedGCList>& gcList = nullptr);

    ~TKeyRangeCache();

    const TKeyRangeCacheConfig& GetConfig() const {
        return Config;
    }

    const TStats& Stats() const {
        return Stats_;
    }

    TStats& Stats() {
        return Stats_;
    }

    size_t GetTotalUsed() const {
        return UsedHeapMemory + Pool->TotalUsed();
    }

    size_t GetTotalAllocated() const {
        return UsedHeapMemory + Pool->TotalAllocated();
    }

    /**
     * Returns true if too much memory is used already
     */
    bool OverMemoryLimit() const {
        return GetTotalUsed() > Config.MaxBytes;
    }

    /**
     * Returns true if there is something to evict or not yet over memory limit
     */
    bool CachingAllowed() const {
        return LRU || !OverMemoryLimit();
    }

    /**
     * Allocates a copy of the specified key
     */
    TArrayRef<TCell> AllocateKey(TArrayRef<const TCell> key);

    /**
     * Deallocates a previously allocated key
     */
    void DeallocateKey(TArrayRef<TCell> key);

    const_iterator begin() const {
        return Entries.begin();
    }

    const_iterator end() const {
        return Entries.end();
    }

    /**
     * Finds the first entry containing key and the found flag
     *
     * If there is no entry matching key, the first entry after the key
     * will be returned (or end) and found flag will be false.
     */
    std::pair<const_iterator, bool> FindKey(TArrayRef<const TCell> key) const {
        auto it = Entries.lower_bound(TLowerBound{ key });
        bool found = it != end() && InsideLeft(it, key);
        return { it, found };
    }

    /**
     * Returns true if key is inside the left boundary of the specified entry
     */
    bool InsideLeft(const_iterator it, TArrayRef<const TCell> key) const {
        Y_DEBUG_ABORT_UNLESS(it != end());
        return !Entries.key_comp()(TSearchPoint{ key }, *it);
    }

    /**
     * Finds the first entry containing key and the found flag
     *
     * If there is no entry matching key, the first entry before the key
     * will be returned (or end) and found flag will be false.
     */
    std::pair<const_iterator, bool> FindKeyReverse(TArrayRef<const TCell> key) const {
        auto it = Entries.upper_bound(TLowerBoundReverse{ key });
        if (it == Entries.begin()) {
            it = Entries.end();
        } else {
            --it;
        }

        bool found = it != end() && InsideRight(it, key);
        return { it, found };
    }

    /**
     * Returns true if key is inside the right boundary of the specified entry
     */
    bool InsideRight(const_iterator it, TArrayRef<const TCell> key) const {
        Y_DEBUG_ABORT_UNLESS(it != end());
        return !Entries.key_comp()(*it, TSearchPoint{ key });
    }

    /**
     * Extends left side of the entry up to the specified key
     */
    void ExtendLeft(const_iterator it, TArrayRef<TCell> newLeft, bool leftInclusive, TRowVersion version);

    /**
     * Extends right side of the entry up to the specified key
     */
    void ExtendRight(const_iterator it, TArrayRef<TCell> newRight, bool rightInclusive, TRowVersion version);

    /**
     * Merges adjacent entries left and right
     */
    const_iterator Merge(const_iterator left, const_iterator right, TRowVersion version);

    /**
     * Adds a new entry with specified boundaries
     */
    const_iterator Add(TKeyRangeEntry entry);

    /**
     * Invalidates the specified entry
     */
    void Invalidate(const_iterator it);

    /**
     * Invalidates the specified key from the entry.
     * Entry may or may not become invalidated after this call.
     * The key doesn't need to be previously allocated with AllocateKey.
     */
    void InvalidateKey(const_iterator it, TArrayRef<const TCell> key);

    /**
     * Marks the specified entry as most recently used
     */
    void Touch(const_iterator it);

    /**
     * Evicts old entries to get under memory limit
     */
    void EvictOld();

    /**
     * Collects garbage produced during a transaction
     */
    void CollectGarbage();

public:
    class TDumpRanges {
    public:
        TDumpRanges(const TKeyRangeCache* self)
            : Self(self)
        { }

        void DumpTo(IOutputStream& out) const;

    private:
        const TKeyRangeCache* const Self;
    };

    TDumpRanges DumpRanges() const {
        return TDumpRanges(this);
    }

private:
    static TArrayRef<TCell> AllocateArrayCopy(TSpecialMemoryPool* pool, TArrayRef<const TCell> key);

    static TCell AllocateCellCopy(TSpecialMemoryPool* pool, const TCell& cell);

private:
    const TKeyCellDefaults& KeyCellDefaults;
    const TKeyRangeCacheConfig Config;
    const TIntrusivePtr<TKeyRangeCacheNeedGCList> GCList;
    THolder<TSpecialMemoryPool> Pool;
    size_t UsedHeapMemory = 0;
    TContainer Entries;
    TIntrusiveList<TKeyRangeEntryLRU> Fresh;
    TIntrusiveList<TKeyRangeEntryLRU> LRU;
    TStats Stats_;
};

}
}
