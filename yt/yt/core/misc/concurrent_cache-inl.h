#ifndef CONCURRENT_CACHE_INL_H_
#error "Direct inclusion of this file is not allowed, include concurrent_cache.h"
// For the sake of sane code completion.
#include "concurrent_cache.h"
#endif
#undef CONCURRENT_CACHE_INL_H_

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
struct TConcurrentCache<T>::TLookupTable final
    : public THashTable
    , public TRefTracked<TLookupTable>
{
    static constexpr bool EnableHazard = true;

    const size_t Capacity;
    std::atomic<size_t> Size = 0;
    TAtomicPtr<TLookupTable> Next;

    explicit TLookupTable(size_t capacity)
        : THashTable(capacity)
        , Capacity(capacity)
    { }

    typename THashTable::TItemRef Insert(TValuePtr item)
    {
        auto fingerprint = THash<T>()(item.Get());
        auto result = THashTable::Insert(fingerprint, std::move(item));

        if (result) {
            ++Size;
        }
        return result;
    }
};

template <class T>
TIntrusivePtr<typename TConcurrentCache<T>::TLookupTable>
TConcurrentCache<T>::RenewTable(const TIntrusivePtr<TLookupTable>& head, size_t capacity)
{
    if (head.Get() != Head_) {
        return Head_.Acquire();
    }

    // Rotate lookup table.
    auto newHead = New<TLookupTable>(capacity);
    newHead->Next = head;

    if (Head_.SwapIfCompare(head, newHead)) {
        constexpr auto& Logger = LockFreeLogger;
        YT_LOG_DEBUG("Concurrent cache lookup table rotated (LoadFactor: %v)",
            head->Size.load());

        // Head_ swapped, remove third lookup table.
        head->Next.Reset();
        return newHead;
    } else {
        return Head_.Acquire();
    }
}

template <class T>
TConcurrentCache<T>::TConcurrentCache(size_t capacity)
    : Capacity_(capacity)
    , Head_(New<TLookupTable>(capacity))
{
    YT_VERIFY(capacity > 0);
}

template <class T>
TConcurrentCache<T>::~TConcurrentCache()
{
    auto head = Head_.Acquire();

    constexpr auto& Logger = LockFreeLogger;
    YT_LOG_DEBUG("Concurrent cache head statistics (ElementCount: %v)",
        head->Size.load());
}

template <class T>
TConcurrentCache<T>::TCachedItemRef::TCachedItemRef(typename THashTable::TItemRef ref, TLookupTable* origin)
    : TConcurrentCache<T>::THashTable::TItemRef(ref)
    , Origin(origin)
{ }

template <class T>
typename TConcurrentCache<T>::TLookuper& TConcurrentCache<T>::TLookuper::operator= (TLookuper&& other)
{
    Parent_ = std::move(other.Parent_);
    Primary_ = std::move(other.Primary_);
    Secondary_ = std::move(other.Secondary_);

    return *this;
}

template <class T>
TConcurrentCache<T>::TLookuper::TLookuper(
    TConcurrentCache* parent,
    TIntrusivePtr<TLookupTable> primary,
    TIntrusivePtr<TLookupTable> secondary)
    : Parent_(parent)
    , Primary_(std::move(primary))
    , Secondary_(std::move(secondary))
{ }

template <class T>
template <class TKey>
typename TConcurrentCache<T>::TCachedItemRef TConcurrentCache<T>::TLookuper::operator() (const TKey& key)
{
    auto fingerprint = THash<T>()(key);

    // Use fixed lookup tables. No need to read head.

    if (auto item = Primary_->FindRef(fingerprint, key)) {
        return TCachedItemRef(item, Primary_.Get());
    }

    if (!Secondary_) {
        return TCachedItemRef();
    }

    return TCachedItemRef(Secondary_->FindRef(fingerprint, key), Secondary_.Get());
}

template <class T>
TConcurrentCache<T>::TLookuper::operator bool ()
{
    return Parent_;
}

template <class T>
typename TConcurrentCache<T>::TLookupTable* TConcurrentCache<T>::TLookuper::GetPrimary()
{
    return Primary_.Get();
}

template <class T>
typename TConcurrentCache<T>::TLookupTable* TConcurrentCache<T>::TLookuper::GetSecondary()
{
    return Secondary_.Get();
}

template <class T>
typename TConcurrentCache<T>::TLookuper TConcurrentCache<T>::GetLookuper()
{
    auto primary = Head_.Acquire();
    auto secondary = primary ? primary->Next.Acquire() : nullptr;

    return TLookuper(this, std::move(primary), std::move(secondary));
}

template <class T>
typename TConcurrentCache<T>::TLookuper TConcurrentCache<T>::GetSecondaryLookuper()
{
    auto primary = Head_.Acquire();
    auto secondary = primary ? primary->Next.Acquire() : nullptr;

    return TLookuper(this, secondary, nullptr);
}

template <class T>
typename TConcurrentCache<T>::TInserter& TConcurrentCache<T>::TInserter::operator= (TInserter&& other)
{
    Parent_ = std::move(other.Parent_);
    Primary_ = std::move(other.Primary_);

    return *this;
}

template <class T>
TConcurrentCache<T>::TInserter::TInserter(
    TConcurrentCache* parent,
    TIntrusivePtr<TLookupTable> primary)
    : Parent_(parent)
    , Primary_(std::move(primary))
{ }

template <class T>
typename TConcurrentCache<T>::TLookupTable* TConcurrentCache<T>::TInserter::GetTable()
{
    auto targetCapacity = Parent_->Capacity_.load(std::memory_order::acquire);
    if (Primary_->Size >= std::min(targetCapacity, Primary_->Capacity)) {
        Primary_ = Parent_->RenewTable(Primary_, targetCapacity);
    }

    return Primary_.Get();
}

template <class T>
typename TConcurrentCache<T>::TInserter TConcurrentCache<T>::GetInserter()
{
    auto primary = Head_.Acquire();
    return TInserter(this, std::move(primary));
}

template <class T>
void TConcurrentCache<T>::SetCapacity(size_t capacity)
{
    YT_VERIFY(capacity > 0);
    Capacity_.store(capacity, std::memory_order::release);

    auto primary = Head_.Acquire();
    if (primary->Size >= std::min(capacity, primary->Capacity)) {
        RenewTable(primary, capacity);
    }
}

template <class T>
bool TConcurrentCache<T>::IsHead(const TIntrusivePtr<TLookupTable>& head) const
{
    return Head_ == head.Get();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
