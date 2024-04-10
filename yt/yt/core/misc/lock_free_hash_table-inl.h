#ifndef LOCK_FREE_HASH_TABLE_INL_H_
#error "Direct inclusion of this file is not allowed, include lock_free_hash_table.h"
// For the sake of sane code completion.
#include "lock_free_hash_table.h"
#endif
#undef LOCK_FREE_HASH_TABLE_INL_H_

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
TLockFreeHashTable<T>::TLockFreeHashTable(size_t maxElementCount)
    : Size_(maxElementCount * HashTableExpansionFactor)
    , HashTable_(new std::atomic<TEntry>[Size_]())
{ }

template <class T>
TLockFreeHashTable<T>::~TLockFreeHashTable()
{
    for (size_t index = 0; index < Size_; ++index) {
        auto tableEntry = HashTable_[index].load(std::memory_order::relaxed);
        auto stamp = StampFromEntry(tableEntry);
        if (stamp != 0) {
            RetireHazardPointer(ValueFromEntry(tableEntry), [] (auto* ptr) {
                Unref(ptr);
            });
        }
    }
}

template <class T>
template <class TCallback>
void TLockFreeHashTable<T>::ForEach(TCallback callback)
{
    for (size_t index = 0; index < Size_; ++index) {
        auto tableEntry = HashTable_[index].load(std::memory_order::relaxed);
        auto stamp = StampFromEntry(tableEntry);
        if (stamp != 0) {
            callback(TItemRef(&HashTable_[index]));
        }
    }
}

template <class T>
size_t TLockFreeHashTable<T>::GetByteSize() const
{
    return sizeof(std::atomic<TEntry>) * Size_;
}

template <class T>
typename TLockFreeHashTable<T>::TItemRef TLockFreeHashTable<T>::Insert(TFingerprint fingerprint, TValuePtr value)
{
    using TItemRef = typename TLockFreeHashTable<T>::TItemRef;

    auto index = IndexFromFingerprint(fingerprint) % Size_;
    auto stamp = StampFromFingerprint(fingerprint);

    auto entry = MakeEntry(stamp, value.Get());

    for (size_t probeCount = Size_; probeCount != 0;) {
        auto tableEntry = HashTable_[index].load(std::memory_order::acquire);
        auto tableStamp = StampFromEntry(tableEntry);

        if (tableStamp == 0) {
            auto success = HashTable_[index].compare_exchange_strong(
                tableEntry,
                entry,
                std::memory_order::release,
                std::memory_order::acquire);
            if (success) {
                value.Release();
                return TItemRef(&HashTable_[index]);
            }
        }

        // This hazard ptr protects from Unref. We do not want to change ref count so frequently.
        auto item = THazardPtr<T>::Acquire([&] {
            return ValueFromEntry(HashTable_[index].load(std::memory_order::acquire));
        }, ValueFromEntry(tableEntry));

        if (TEqualTo<T>()(item.Get(), value.Get())) {
            return TItemRef(nullptr);
        }

        ++index;
        if (index == Size_) {
            index = 0;
        }
        --probeCount;
    }

    return TItemRef(nullptr);
}

template <class T>
template <class TKey>
TIntrusivePtr<T> TLockFreeHashTable<T>::Find(TFingerprint fingerprint, const TKey& key)
{
    auto index = IndexFromFingerprint(fingerprint) % Size_;
    auto stamp = StampFromFingerprint(fingerprint);

    for (size_t probeCount = Size_; probeCount != 0;) {
        auto tableEntry = HashTable_[index].load(std::memory_order::acquire);
        auto tableStamp = StampFromEntry(tableEntry);

        if (tableStamp == 0) {
            break;
        }

        if (tableStamp == stamp) {
            // This hazard ptr protects from Unref. We do not want to change ref count so frequently.
            // TIntrusivePtr::AcquireUnchecked could be used outside this function.

            auto item = THazardPtr<T>::Acquire([&] {
                return ValueFromEntry(HashTable_[index].load(std::memory_order::acquire));
            }, ValueFromEntry(tableEntry));

            if (TEqualTo<T>()(item.Get(), key)) {
                return TValuePtr(item.Get());
            }
        }

        ++index;
        if (index == Size_) {
            index = 0;
        }
        --probeCount;
    }

    return nullptr;
}

template <class T>
template <class TKey>
typename TLockFreeHashTable<T>::TItemRef TLockFreeHashTable<T>::FindRef(TFingerprint fingerprint, const TKey& key)
{
    using TItemRef = typename TLockFreeHashTable<T>::TItemRef;

    auto index = IndexFromFingerprint(fingerprint) % Size_;
    auto stamp = StampFromFingerprint(fingerprint);

    for (size_t probeCount = Size_; probeCount != 0;) {
        auto tableEntry = HashTable_[index].load(std::memory_order::relaxed);
        // TODO(lukyan): Rename to entryStamp.
        auto tableStamp = StampFromEntry(tableEntry);

        if (tableStamp == 0) {
            break;
        }

        if (tableStamp == stamp) {
            // This hazard ptr protects from Unref. We do not want to change ref count so frequently.
            // TIntrusivePtr::AcquireUnchecked could be used outside this function.

            auto item = THazardPtr<T>::Acquire([&] {
                return ValueFromEntry(HashTable_[index].load(std::memory_order::relaxed));
            }, ValueFromEntry(tableEntry));

            if (TEqualTo<T>()(item.Get(), key)) {
                return TItemRef(&HashTable_[index]);
            }
        }

        ++index;
        if (index == Size_) {
            index = 0;
        }
        --probeCount;
    }

    return TItemRef();
}

template <class T>
typename TLockFreeHashTable<T>::TStamp
    TLockFreeHashTable<T>::StampFromEntry(TEntry entry)
{
    return entry >> ValueLog;
}

template <class T>
T* TLockFreeHashTable<T>::ValueFromEntry(TEntry entry)
{
    constexpr auto Mask = (1ULL << ValueLog) - 1;
    return reinterpret_cast<T*>(entry & (Mask ^ 1ULL));
}

template <class T>
typename TLockFreeHashTable<T>::TEntry
    TLockFreeHashTable<T>::MakeEntry(TStamp stamp, T* value, bool sealed)
{
    YT_ASSERT(stamp != 0);
    YT_ASSERT((reinterpret_cast<TEntry>(value) & 1ULL) == 0);
    YT_ASSERT(StampFromEntry(reinterpret_cast<TEntry>(value)) == 0);
    return (static_cast<TEntry>(stamp) << ValueLog) | reinterpret_cast<TEntry>(value) | (sealed ? 1ULL : 0ULL);
}

template <class T>
size_t TLockFreeHashTable<T>::IndexFromFingerprint(TFingerprint fingerprint)
{
    // TODO(lukyan): Use higher bits of fingerprint. Lower are used by stamp.
    return fingerprint;
}

template <class T>
typename TLockFreeHashTable<T>::TStamp
    TLockFreeHashTable<T>::StampFromFingerprint(TFingerprint fingerprint)
{
    return (fingerprint << 1) | 1ULL;
}

/////////////////////////////////////////////////////////////////////////////

} // namespace NYT
