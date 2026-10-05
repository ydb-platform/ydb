#ifndef COMPACT_MAP_INL_H_
#error "Direct inclusion of this file is not allowed, include compact_map.h"
// For the sake of sane code completion.
#include "compact_map.h"
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, size_t N, class TCompare>
template <bool IsConst>
class TCompactMap<TKey, TValue, N, TCompare>::TIteratorBase
{
private:
    friend class TCompactMap<TKey, TValue, N, TCompare>;
    friend class TIteratorBase<!IsConst>;
    friend class iterator;
    friend class const_iterator;

    using TVecIter = std::conditional_t<IsConst, TArrayConstIterator, TArrayIterator>;
    using TMapIter = std::conditional_t<IsConst, TMapConstIterator, TMapIterator>;
    using TValueType = typename TCompactMap::value_type;
    using TReference = std::conditional_t<IsConst, const TValueType&, TValueType&>;
    using TPointer = std::conditional_t<IsConst, const TValueType*, TValueType*>;

    union
    {
        TVecIter VIter;
        TMapIter MIter;
    };
    bool Small_;

    TIteratorBase()
        : VIter()
        , Small_(true)
    { }

    explicit TIteratorBase(TVecIter it)
        : VIter(it)
        , Small_(true)
    { }

    explicit TIteratorBase(TMapIter it)
        : MIter(it)
        , Small_(false)
    { }

public:
    using difference_type = std::ptrdiff_t;
    using value_type = TValueType;
    using reference = TReference;
    using pointer = TPointer;
    using iterator_category = std::bidirectional_iterator_tag;

    reference operator*() const
    {
        if (Small_) {
            return *VIter;
        }
        return *MIter;
    }

    pointer operator->() const
    {
        if (Small_) {
            return &*VIter;
        }
        return &*MIter;
    }

    TIteratorBase& operator++()
    {
        if (Small_) {
            ++VIter;
        } else {
            ++MIter;
        }
        return *this;
    }

    TIteratorBase operator++(int)
    {
        TIteratorBase tmp = *this;
        ++*this;
        return tmp;
    }

    TIteratorBase& operator--()
    {
        if (Small_) {
            --VIter;
        } else {
            --MIter;
        }
        return *this;
    }

    TIteratorBase operator--(int)
    {
        TIteratorBase tmp = *this;
        --*this;
        return tmp;
    }

    template <bool OtherIsConst>
    bool operator==(const TIteratorBase<OtherIsConst>& other) const
    {
        if (Small_ != other.Small_) {
            return false;
        }
        return Small_ ? VIter == other.VIter : MIter == other.MIter;
    }
};

template <class TKey, class TValue, size_t N, class TCompare>
class TCompactMap<TKey, TValue, N, TCompare>::iterator
    : public TCompactMap<TKey, TValue, N, TCompare>::template TIteratorBase<false>
{
public:
    iterator() = default;

    iterator& operator++()
    {
        TBase::operator++();
        return *this;
    }

    iterator operator++(int)
    {
        auto result = *this;
        ++*this;
        return result;
    }

    iterator& operator--()
    {
        TBase::operator--();
        return *this;
    }

    iterator operator--(int)
    {
        auto result = *this;
        --*this;
        return result;
    }

private:
    friend class TCompactMap<TKey, TValue, N, TCompare>;
    using TBase = typename TCompactMap<TKey, TValue, N, TCompare>::template TIteratorBase<false>;

    explicit iterator(TArrayIterator it)
        : TBase(it)
    { }

    explicit iterator(TMapIterator it)
        : TBase(it)
    { }
};

template <class TKey, class TValue, size_t N, class TCompare>
class TCompactMap<TKey, TValue, N, TCompare>::const_iterator
    : public TCompactMap<TKey, TValue, N, TCompare>::template TIteratorBase<true>
{
public:
    const_iterator() = default;

    const_iterator& operator++()
    {
        TBase::operator++();
        return *this;
    }

    const_iterator operator++(int)
    {
        auto result = *this;
        ++*this;
        return result;
    }

    const_iterator& operator--()
    {
        TBase::operator--();
        return *this;
    }

    const_iterator operator--(int)
    {
        auto result = *this;
        --*this;
        return result;
    }

    // Allow implicit conversion from iterator to const_iterator.
    const_iterator(const iterator& other)
        : TBase(other.Small_ ? TBase(other.VIter) : TBase(other.MIter))
    { }

private:
    friend class TCompactMap<TKey, TValue, N, TCompare>;
    using TBase = typename TCompactMap<TKey, TValue, N, TCompare>::template TIteratorBase<true>;

    explicit const_iterator(TArrayConstIterator it)
        : TBase(it)
    { }

    explicit const_iterator(TMapConstIterator it)
        : TBase(it)
    { }
};

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, size_t N, class TCompare>
auto TCompactMap<TKey, TValue, N, TCompare>::ArrayData() -> TArrayIterator
{
    static_assert(sizeof(TStorage) == sizeof(TArrayValue), "Inline storage size mismatch");
    static_assert(alignof(TStorage) == alignof(TArrayValue), "Inline storage alignment mismatch");
    return reinterpret_cast<TArrayIterator>(ArrayStorage_.data());
}

template <class TKey, class TValue, size_t N, class TCompare>
auto TCompactMap<TKey, TValue, N, TCompare>::ArrayData() const -> TArrayConstIterator
{
    return reinterpret_cast<TArrayConstIterator>(ArrayStorage_.data());
}

template <class TKey, class TValue, size_t N, class TCompare>
auto TCompactMap<TKey, TValue, N, TCompare>::ArrayBegin() -> TArrayIterator
{
    return ArrayData();
}

template <class TKey, class TValue, size_t N, class TCompare>
auto TCompactMap<TKey, TValue, N, TCompare>::ArrayBegin() const -> TArrayConstIterator
{
    return ArrayData();
}

template <class TKey, class TValue, size_t N, class TCompare>
auto TCompactMap<TKey, TValue, N, TCompare>::ArrayEnd() -> TArrayIterator
{
    return ArrayData() + ArraySize_;
}

template <class TKey, class TValue, size_t N, class TCompare>
auto TCompactMap<TKey, TValue, N, TCompare>::ArrayEnd() const -> TArrayConstIterator
{
    return ArrayData() + ArraySize_;
}

template <class TKey, class TValue, size_t N, class TCompare>
void TCompactMap<TKey, TValue, N, TCompare>::ClearArray()
{
    auto* data = ArrayData();
    for (size_type index = 0; index < ArraySize_; ++index) {
        std::destroy_at(data + index);
    }
    ArraySize_ = 0;
}

template <class TKey, class TValue, size_t N, class TCompare>
void TCompactMap<TKey, TValue, N, TCompare>::CopyArrayFrom(const TCompactMap& other)
{
    auto* data = ArrayData();
    auto* otherData = other.ArrayData();

    try {
        for (size_type index = 0; index < other.ArraySize_; ++index) {
            std::construct_at(data + index, otherData[index]);
            ++ArraySize_;
        }
    } catch (...) {
        ClearArray();
        throw;
    }
}

template <class TKey, class TValue, size_t N, class TCompare>
void TCompactMap<TKey, TValue, N, TCompare>::MoveArrayFrom(TCompactMap&& other)
{
    auto* data = ArrayData();
    auto* otherData = other.ArrayData();

    try {
        for (size_type index = 0; index < other.ArraySize_; ++index) {
            std::construct_at(data + index, std::move(otherData[index]));
            ++ArraySize_;
        }
    } catch (...) {
        ClearArray();
        throw;
    }

    other.ClearArray();
}

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, size_t N, class TCompare>
TCompactMap<TKey, TValue, N, TCompare>::TCompactMap(const TCompactMap& other)
    : Compare_(other.Compare_)
{
    if (other.IsSmall()) {
        CopyArrayFrom(other);
    } else {
        Map_ = other.Map_;
        MapMode_ = true;
    }
}

template <class TKey, class TValue, size_t N, class TCompare>
TCompactMap<TKey, TValue, N, TCompare>::TCompactMap(TCompactMap&& other) noexcept(
    std::is_nothrow_move_constructible_v<value_type> &&
    std::is_nothrow_move_constructible_v<key_compare>)
    : Map_(std::move(other.Map_))
    , Compare_(std::move(other.Compare_))
{
    if (other.IsSmall()) {
        MoveArrayFrom(std::move(other));
    } else {
        MapMode_ = true;

        other.Map_.clear();
        other.MapMode_ = false;
    }
}

template <class TKey, class TValue, size_t N, class TCompare>
TCompactMap<TKey, TValue, N, TCompare>&
TCompactMap<TKey, TValue, N, TCompare>::operator=(const TCompactMap& other)
{
    if (this == &other) {
        return *this;
    }

    ClearArray();
    Map_.clear();
    MapMode_ = false;

    Compare_ = other.Compare_;

    if (other.IsSmall()) {
        CopyArrayFrom(other);
    } else {
        Map_ = other.Map_;
        MapMode_ = true;
    }

    return *this;
}

template <class TKey, class TValue, size_t N, class TCompare>
TCompactMap<TKey, TValue, N, TCompare>&
TCompactMap<TKey, TValue, N, TCompare>::operator=(TCompactMap&& other) noexcept(
    std::is_nothrow_move_constructible_v<value_type> &&
    std::is_nothrow_move_assignable_v<key_compare>)
{
    if (this == &other) {
        return *this;
    }

    ClearArray();
    Map_.clear();
    MapMode_ = false;

    Compare_ = std::move(other.Compare_);

    if (other.IsSmall()) {
        MoveArrayFrom(std::move(other));
    } else {
        Map_ = std::move(other.Map_);
        MapMode_ = true;

        other.Map_.clear();
        other.MapMode_ = false;
    }

    return *this;
}

template <class TKey, class TValue, size_t N, class TCompare>
TCompactMap<TKey, TValue, N, TCompare>::~TCompactMap()
{
    ClearArray();
}

template <class TKey, class TValue, size_t N, class TCompare>
template <class TIt>
TCompactMap<TKey, TValue, N, TCompare>::TCompactMap(TIt first, TIt last)
{
    insert(first, last);
}

template <class TKey, class TValue, size_t N, class TCompare>
TCompactMap<TKey, TValue, N, TCompare>::TCompactMap(std::initializer_list<value_type> init)
{
    insert(init.begin(), init.end());
}

template <class TKey, class TValue, size_t N, class TCompare>
bool TCompactMap<TKey, TValue, N, TCompare>::empty() const
{
    return IsSmall() ? ArraySize_ == 0 : Map_.empty();
}

template <class TKey, class TValue, size_t N, class TCompare>
typename TCompactMap<TKey, TValue, N, TCompare>::size_type
TCompactMap<TKey, TValue, N, TCompare>::size() const
{
    return IsSmall() ? ArraySize_ : Map_.size();
}

template <class TKey, class TValue, size_t N, class TCompare>
void TCompactMap<TKey, TValue, N, TCompare>::clear()
{
    ClearArray();
    Map_.clear();
    MapMode_ = false;
}

template <class TKey, class TValue, size_t N, class TCompare>
auto TCompactMap<TKey, TValue, N, TCompare>::begin() -> iterator
{
    return BeginImpl(this);
}

template <class TKey, class TValue, size_t N, class TCompare>
auto TCompactMap<TKey, TValue, N, TCompare>::begin() const -> const_iterator
{
    return BeginImpl(this);
}

template <class TKey, class TValue, size_t N, class TCompare>
auto TCompactMap<TKey, TValue, N, TCompare>::cbegin() const -> const_iterator
{
    return begin();
}

template <class TKey, class TValue, size_t N, class TCompare>
auto TCompactMap<TKey, TValue, N, TCompare>::end() -> iterator
{
    return EndImpl(this);
}

template <class TKey, class TValue, size_t N, class TCompare>
auto TCompactMap<TKey, TValue, N, TCompare>::end() const -> const_iterator
{
    return EndImpl(this);
}

template <class TKey, class TValue, size_t N, class TCompare>
auto TCompactMap<TKey, TValue, N, TCompare>::cend() const -> const_iterator
{
    return end();
}

template <class TKey, class TValue, size_t N, class TCompare>
bool TCompactMap<TKey, TValue, N, TCompare>::IsSmall() const
{
    return !MapMode_;
}

template <class TKey, class TValue, size_t N, class TCompare>
void TCompactMap<TKey, TValue, N, TCompare>::UpgradeToMap()
{
    if (!IsSmall()) {
        return;
    }

    TMap newMap(Compare_);
    auto* data = ArrayData();
    for (size_type index = 0; index < ArraySize_; ++index) {
        newMap.emplace_hint(newMap.end(), data[index].first, std::move(data[index].second));
    }

    ClearArray();
    Map_ = std::move(newMap);
    MapMode_ = true;
}

template <class TKey, class TValue, size_t N, class TCompare>
template <class TOtherKey>
std::pair<typename TCompactMap<TKey, TValue, N, TCompare>::TArrayConstIterator, bool>
TCompactMap<TKey, TValue, N, TCompare>::ArrayLowerBound(const TOtherKey& key) const
{
    return ArrayLowerBoundImpl(ArrayBegin(), ArrayEnd(), key, Compare_);
}

template <class TKey, class TValue, size_t N, class TCompare>
template <class TOtherKey>
std::pair<typename TCompactMap<TKey, TValue, N, TCompare>::TArrayIterator, bool>
TCompactMap<TKey, TValue, N, TCompare>::ArrayLowerBound(const TOtherKey& key)
{
    return ArrayLowerBoundImpl(ArrayBegin(), ArrayEnd(), key, Compare_);
}

template <class TKey, class TValue, size_t N, class TCompare>
template <class... TArgs>
auto TCompactMap<TKey, TValue, N, TCompare>::EmplaceSmall(TArrayConstIterator pos, TArgs&&... args) -> TArrayIterator
{
    auto index = static_cast<size_type>(pos - ArrayBegin());
    auto* data = ArrayData();

    // Simple path: emplace at the end.
    if (index == ArraySize_) {
        std::construct_at(data + ArraySize_, std::forward<TArgs>(args)...);
        ++ArraySize_;
        return data + index;
    }

    // Construct the new value before modifying the array.
    TArrayValue value(std::forward<TArgs>(args)...);

    // Shift elements to make space at index. If relocation throws, destroy the
    // shifted suffix and keep the remaining live prefix valid.
    std::construct_at(data + ArraySize_, std::move(data[ArraySize_ - 1]));
    for (size_type i = ArraySize_ - 1; i > index; --i) {
        std::destroy_at(data + i);
        try {
            std::construct_at(data + i, std::move(data[i - 1]));
        } catch (...) {
            for (size_type j = i + 1; j <= ArraySize_; ++j) {
                std::destroy_at(data + j);
            }
            ArraySize_ = i;
            throw;
        }
    }

    std::destroy_at(data + index);
    try {
        std::construct_at(data + index, std::move(value));
    } catch (...) {
        for (size_type i = index + 1; i <= ArraySize_; ++i) {
            std::destroy_at(data + i);
        }
        ArraySize_ = index;
        throw;
    }

    ++ArraySize_;
    return data + index;
}

template <class TKey, class TValue, size_t N, class TCompare>
auto TCompactMap<TKey, TValue, N, TCompare>::EraseSmall(TArrayConstIterator pos) -> TArrayIterator
{
    return EraseSmall(pos, pos + 1);
}

template <class TKey, class TValue, size_t N, class TCompare>
auto TCompactMap<TKey, TValue, N, TCompare>::EraseSmall(TArrayConstIterator first, TArrayConstIterator last) -> TArrayIterator
{
    auto start = static_cast<size_type>(first - ArrayBegin());
    auto finish = static_cast<size_type>(last - ArrayBegin());
    auto* data = ArrayData();

    if (start == finish) {
        return data + start;
    }

    // Destroy elements in [start, finish).
    for (size_type index = start; index < finish; ++index) {
        std::destroy_at(data + index);
    }

    // Shift elements from [finish, ArraySize_) to [start, ...).
    size_type index = finish;
    try {
        for (; index < ArraySize_; ++index) {
            std::construct_at(data + start + (index - finish), std::move(data[index]));
            std::destroy_at(data + index);
        }
    } catch (...) {
        // Relocation failed; drop the tail that has not been moved yet.
        for (size_type tailIndex = index; tailIndex < ArraySize_; ++tailIndex) {
            std::destroy_at(data + tailIndex);
        }
        ArraySize_ = start + (index - finish);
        throw;
    }

    ArraySize_ -= finish - start;
    return data + start;
}

template <class TKey, class TValue, size_t N, class TCompare>
template <class TArrayIteratorType, class TOtherKey>
std::pair<TArrayIteratorType, bool>
TCompactMap<TKey, TValue, N, TCompare>::ArrayLowerBoundImpl(
    TArrayIteratorType begin,
    TArrayIteratorType end,
    const TOtherKey& key,
    const key_compare& compare)
{
    auto comp = [&compare] (const auto& pair, const auto& k) {
        return compare(pair.first, k);
    };

    auto it = std::lower_bound(begin, end, key, comp);
    bool found = it != end && !compare(key, it->first);
    return {it, found};
}

template <class TKey, class TValue, size_t N, class TCompare>
template <class TKeyParam>
typename TCompactMap<TKey, TValue, N, TCompare>::mapped_type&
TCompactMap<TKey, TValue, N, TCompare>::Subscript(TKeyParam&& key)
{
    return TryEmplaceImpl(std::forward<TKeyParam>(key)).first->second;
}

template <class TKey, class TValue, size_t N, class TCompare>
template <class TSelf>
auto TCompactMap<TKey, TValue, N, TCompare>::BeginImpl(TSelf* self)
    -> std::conditional_t<std::is_const_v<TSelf>, const_iterator, iterator>
{
    using TResult = std::conditional_t<std::is_const_v<TSelf>, const_iterator, iterator>;
    if (self->IsSmall()) {
        return TResult(self->ArrayBegin());
    }
    return TResult(self->Map_.begin());
}

template <class TKey, class TValue, size_t N, class TCompare>
template <class TSelf>
auto TCompactMap<TKey, TValue, N, TCompare>::EndImpl(TSelf* self)
    -> std::conditional_t<std::is_const_v<TSelf>, const_iterator, iterator>
{
    using TResult = std::conditional_t<std::is_const_v<TSelf>, const_iterator, iterator>;
    if (self->IsSmall()) {
        return TResult(self->ArrayEnd());
    }
    return TResult(self->Map_.end());
}

template <class TKey, class TValue, size_t N, class TCompare>
template <class TSelf, class TOtherKey>
auto TCompactMap<TKey, TValue, N, TCompare>::FindImpl(TSelf* self, const TOtherKey& key)
    -> std::conditional_t<std::is_const_v<TSelf>, const_iterator, iterator>
{
    using TResult = std::conditional_t<std::is_const_v<TSelf>, const_iterator, iterator>;
    if (self->IsSmall()) {
        auto [it, found] = self->ArrayLowerBound(key);
        return found ? TResult(it) : TResult(self->ArrayEnd());
    }
    return TResult(self->Map_.find(key));
}

template <class TKey, class TValue, size_t N, class TCompare>
template <class TSelf>
auto TCompactMap<TKey, TValue, N, TCompare>::AtImpl(TSelf* self, const key_type& key)
    -> std::conditional_t<std::is_const_v<TSelf>, const mapped_type&, mapped_type&>
{
    auto it = FindImpl(self, key);
    if (it == EndImpl(self)) {
        throw std::out_of_range("TCompactMap::at");
    }
    return it->second;
}

template <class TKey, class TValue, size_t N, class TCompare>
template <NDetail::CComparisonAllowed<TKey, TCompare> TOtherKey>
typename TCompactMap<TKey, TValue, N, TCompare>::size_type
TCompactMap<TKey, TValue, N, TCompare>::count(const TOtherKey& key) const
{
    return FindImpl(this, key) == EndImpl(this) ? 0 : 1;
}

template <class TKey, class TValue, size_t N, class TCompare>
template <NDetail::CComparisonAllowed<TKey, TCompare> TOtherKey>
bool TCompactMap<TKey, TValue, N, TCompare>::contains(const TOtherKey& key) const
{
    return FindImpl(this, key) != EndImpl(this);
}

template <class TKey, class TValue, size_t N, class TCompare>
template <NDetail::CComparisonAllowed<TKey, TCompare> TOtherKey>
auto TCompactMap<TKey, TValue, N, TCompare>::find(const TOtherKey& key) -> iterator
{
    return FindImpl(this, key);
}

template <class TKey, class TValue, size_t N, class TCompare>
template <NDetail::CComparisonAllowed<TKey, TCompare> TOtherKey>
auto TCompactMap<TKey, TValue, N, TCompare>::find(const TOtherKey& key) const -> const_iterator
{
    return FindImpl(this, key);
}

template <class TKey, class TValue, size_t N, class TCompare>
std::pair<typename TCompactMap<TKey, TValue, N, TCompare>::iterator, bool>
TCompactMap<TKey, TValue, N, TCompare>::insert(const value_type& value)
{
    return DoInsert(value);
}

template <class TKey, class TValue, size_t N, class TCompare>
std::pair<typename TCompactMap<TKey, TValue, N, TCompare>::iterator, bool>
TCompactMap<TKey, TValue, N, TCompare>::insert(value_type&& value)
{
    return DoInsert(std::move(value));
}

template <class TKey, class TValue, size_t N, class TCompare>
template <class TValueParam>
std::pair<typename TCompactMap<TKey, TValue, N, TCompare>::iterator, bool>
TCompactMap<TKey, TValue, N, TCompare>::DoInsert(TValueParam&& value)
{
    if (IsSmall()) {
        auto [arrayIt, found] = ArrayLowerBound(value.first);

        if (found) {
            return {iterator(arrayIt), false};
        }

        if (ArraySize_ < N) {
            auto inserted = EmplaceSmall(arrayIt, std::forward<TValueParam>(value));
            return {iterator(inserted), true};
        }

        UpgradeToMap();
    }

    auto [mapIt, inserted] = Map_.insert(std::forward<TValueParam>(value));
    return {iterator(mapIt), inserted};
}

template <class TKey, class TValue, size_t N, class TCompare>
template <class... TArgs>
std::pair<typename TCompactMap<TKey, TValue, N, TCompare>::iterator, bool>
TCompactMap<TKey, TValue, N, TCompare>::emplace(TArgs&&... args)
{
    if (IsSmall()) {
        TArrayValue value(std::forward<TArgs>(args)...);
        auto [arrayIt, found] = ArrayLowerBound(value.first);

        if (found) {
            return {iterator(arrayIt), false};
        }

        if (ArraySize_ < N) {
            auto inserted = EmplaceSmall(arrayIt, std::move(value));
            return {iterator(inserted), true};
        }

        UpgradeToMap();

        // We already constructed the value above, so we cannot forward args again below.
        auto [mapIt, inserted] = Map_.emplace(std::move(value));
        return {iterator(mapIt), inserted};
    }

    auto [mapIt, inserted] = Map_.emplace(std::forward<TArgs>(args)...);
    return {iterator(mapIt), inserted};
}

template <class TKey, class TValue, size_t N, class TCompare>
template <class... TArgs>
std::pair<typename TCompactMap<TKey, TValue, N, TCompare>::iterator, bool>
TCompactMap<TKey, TValue, N, TCompare>::try_emplace(const key_type& key, TArgs&&... args)
{
    return TryEmplaceImpl(key, std::forward<TArgs>(args)...);
}

template <class TKey, class TValue, size_t N, class TCompare>
template <class... TArgs>
std::pair<typename TCompactMap<TKey, TValue, N, TCompare>::iterator, bool>
TCompactMap<TKey, TValue, N, TCompare>::try_emplace(key_type&& key, TArgs&&... args)
{
    return TryEmplaceImpl(std::move(key), std::forward<TArgs>(args)...);
}

template <class TKey, class TValue, size_t N, class TCompare>
template <class TIt>
void TCompactMap<TKey, TValue, N, TCompare>::insert(TIt first, TIt last)
{
    for (; first != last; ++first) {
        insert(*first);
    }
}

template <class TKey, class TValue, size_t N, class TCompare>
template <class TMapped>
std::pair<typename TCompactMap<TKey, TValue, N, TCompare>::iterator, bool>
TCompactMap<TKey, TValue, N, TCompare>::insert_or_assign(const key_type& key, TMapped&& obj)
{
    if (IsSmall()) {
        auto [arrayIt, found] = ArrayLowerBound(key);

        if (found) {
            arrayIt->second = std::forward<TMapped>(obj);
            return {iterator(arrayIt), false};
        }

        if (ArraySize_ < N) {
            auto inserted = EmplaceSmall(arrayIt, key, std::forward<TMapped>(obj));
            return {iterator(inserted), true};
        }

        UpgradeToMap();
    }

    auto [mapIt, inserted] = Map_.insert_or_assign(key, std::forward<TMapped>(obj));
    return {iterator(mapIt), inserted};
}

template <class TKey, class TValue, size_t N, class TCompare>
template <class TKeyParam, class... TArgs>
std::pair<typename TCompactMap<TKey, TValue, N, TCompare>::iterator, bool>
TCompactMap<TKey, TValue, N, TCompare>::TryEmplaceImpl(TKeyParam&& key, TArgs&&... args)
{
    if (IsSmall()) {
        auto [arrayIt, found] = ArrayLowerBound(key);

        if (found) {
            return {iterator(arrayIt), false};
        }

        if (ArraySize_ < N) {
            auto inserted = EmplaceSmall(
                arrayIt,
                std::piecewise_construct,
                std::forward_as_tuple(std::forward<TKeyParam>(key)),
                std::forward_as_tuple(std::forward<TArgs>(args)...));
            return {iterator(inserted), true};
        }

        UpgradeToMap();
    }

    auto [mapIt, inserted] = Map_.try_emplace(std::forward<TKeyParam>(key), std::forward<TArgs>(args)...);
    return {iterator(mapIt), inserted};
}

template <class TKey, class TValue, size_t N, class TCompare>
auto TCompactMap<TKey, TValue, N, TCompare>::erase(const key_type& key) -> size_type
{
    if (IsSmall()) {
        auto [arrayIt, found] = ArrayLowerBound(key);

        if (!found) {
            return 0;
        }

        EraseSmall(arrayIt);
        return 1;
    }

    return Map_.erase(key);
}

template <class TKey, class TValue, size_t N, class TCompare>
auto TCompactMap<TKey, TValue, N, TCompare>::erase(iterator pos) -> iterator
{
    return erase(const_iterator(pos));
}

template <class TKey, class TValue, size_t N, class TCompare>
auto TCompactMap<TKey, TValue, N, TCompare>::erase(const_iterator pos) -> iterator
{
    if (IsSmall()) {
        return iterator(EraseSmall(pos.VIter));
    }

    return iterator(Map_.erase(pos.MIter));
}

template <class TKey, class TValue, size_t N, class TCompare>
auto TCompactMap<TKey, TValue, N, TCompare>::erase(const_iterator first, const_iterator last) -> iterator
{
    if (IsSmall()) {
        return iterator(EraseSmall(first.VIter, last.VIter));
    }

    return iterator(Map_.erase(first.MIter, last.MIter));
}

template <class TKey, class TValue, size_t N, class TCompare>
typename TCompactMap<TKey, TValue, N, TCompare>::mapped_type&
TCompactMap<TKey, TValue, N, TCompare>::operator[](const key_type& key)
{
    return Subscript(key);
}

template <class TKey, class TValue, size_t N, class TCompare>
typename TCompactMap<TKey, TValue, N, TCompare>::mapped_type&
TCompactMap<TKey, TValue, N, TCompare>::operator[](key_type&& key)
{
    return Subscript(std::move(key));
}

template <class TKey, class TValue, size_t N, class TCompare>
typename TCompactMap<TKey, TValue, N, TCompare>::mapped_type&
TCompactMap<TKey, TValue, N, TCompare>::at(const key_type& key)
{
    return AtImpl(this, key);
}

template <class TKey, class TValue, size_t N, class TCompare>
const typename TCompactMap<TKey, TValue, N, TCompare>::mapped_type&
TCompactMap<TKey, TValue, N, TCompare>::at(const key_type& key) const
{
    return AtImpl(this, key);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
