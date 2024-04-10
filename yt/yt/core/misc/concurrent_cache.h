#pragma once

#include "public.h"
#include "atomic_ptr.h"
#include "lock_free_hash_table.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TConcurrentCache
{
private:
    using THashTable = TLockFreeHashTable<T>;

    struct TLookupTable;

    TIntrusivePtr<TLookupTable> RenewTable(const TIntrusivePtr<TLookupTable>& head, size_t capacity);

public:
    using TValuePtr = TIntrusivePtr<T>;

    explicit TConcurrentCache(size_t maxElementCount);

    ~TConcurrentCache();

    struct TCachedItemRef
        : public THashTable::TItemRef
    {
        TCachedItemRef() = default;

        TCachedItemRef(typename THashTable::TItemRef ref, TLookupTable* origin);

        TLookupTable* const Origin = nullptr;
    };

    class TLookuper
    {
    public:
        TLookuper() = default;

        TLookuper(TLookuper&& other) = default;

        TLookuper& operator= (TLookuper&& other);

        TLookuper(
            TConcurrentCache* parent,
            TIntrusivePtr<TLookupTable> primary,
            TIntrusivePtr<TLookupTable> secondary);

        template <class TKey>
        TCachedItemRef operator() (const TKey& key);

        explicit operator bool ();

        TLookupTable* GetPrimary();
        TLookupTable* GetSecondary();

    private:
        TConcurrentCache* Parent_ = nullptr;
        TIntrusivePtr<TLookupTable> Primary_;
        TIntrusivePtr<TLookupTable> Secondary_;
    };

    TLookuper GetLookuper();

    TLookuper GetSecondaryLookuper();

    class TInserter
    {
    public:
        TInserter() = default;

        TInserter(TInserter&& other) = default;

        TInserter& operator= (TInserter&& other);

        TInserter(
            TConcurrentCache* parent,
            TIntrusivePtr<TLookupTable> primary);

        TLookupTable* GetTable();

    private:
        TConcurrentCache* Parent_ = nullptr;
        TIntrusivePtr<TLookupTable> Primary_;
    };

    TInserter GetInserter();

    void SetCapacity(size_t capacity);

    bool IsHead(const TIntrusivePtr<TLookupTable>& head) const;

private:
    std::atomic<size_t> Capacity_;
    TAtomicPtr<TLookupTable> Head_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define CONCURRENT_CACHE_INL_H_
#include "concurrent_cache-inl.h"
#undef CONCURRENT_CACHE_INL_H_
