#pragma once

#include "comparison.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <functional>
#include <initializer_list>
#include <iterator>
#include <map>
#include <memory>
#include <stdexcept>
#include <tuple>
#include <type_traits>
#include <utility>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! A flat map that keeps up to N elements inline in a sorted buffer,
//! similar to TCompactFlatMap, but, unlike TCompactFlatMap, transparently
//! falls back to std::map storage when the size exceeds N.
//!
//! The fallback is one-way: once the size exceeds N the container keeps using
//! std::map even after it shrinks back; only clear() returns it to inline storage.
//!
//! If switching to std::map storage throws, the container remains valid and
//! retains its keys, but its mapped values may be left in a moved-from state.
template <class TKey, class TValue, size_t N, class TCompare = std::ranges::less>
class TCompactMap
{
public:
    using key_type = TKey;
    using mapped_type = TValue;
    using value_type = std::pair<const TKey, TValue>;
    using key_compare = TCompare;
    using size_type = std::size_t;

    class iterator;
    class const_iterator;

    TCompactMap() = default;
    TCompactMap(const TCompactMap& other);
    TCompactMap(TCompactMap&& other) noexcept(
        std::is_nothrow_move_constructible_v<value_type> &&
        std::is_nothrow_move_constructible_v<key_compare>);
    TCompactMap& operator=(const TCompactMap& other);
    TCompactMap& operator=(TCompactMap&& other) noexcept(
        std::is_nothrow_move_constructible_v<value_type> &&
        std::is_nothrow_move_assignable_v<key_compare>);
    ~TCompactMap();

    template <class TIt>
    TCompactMap(TIt first, TIt last);

    TCompactMap(std::initializer_list<value_type> init);

    [[nodiscard]] bool empty() const;
    size_type size() const;

    void clear();

    iterator begin();
    const_iterator begin() const;
    const_iterator cbegin() const;

    iterator end();
    const_iterator end() const;
    const_iterator cend() const;

    template <NDetail::CComparisonAllowed<TKey, TCompare> TOtherKey>
    size_type count(const TOtherKey& key) const;

    template <NDetail::CComparisonAllowed<TKey, TCompare> TOtherKey>
    iterator find(const TOtherKey& key);
    template <NDetail::CComparisonAllowed<TKey, TCompare> TOtherKey>
    const_iterator find(const TOtherKey& key) const;

    template <NDetail::CComparisonAllowed<TKey, TCompare> TOtherKey>
    bool contains(const TOtherKey& key) const;

    std::pair<iterator, bool> insert(const value_type& value);
    std::pair<iterator, bool> insert(value_type&& value);

    template <class... TArgs>
    std::pair<iterator, bool> emplace(TArgs&&... args);

    template <class... TArgs>
    std::pair<iterator, bool> try_emplace(const key_type& key, TArgs&&... args);

    template <class... TArgs>
    std::pair<iterator, bool> try_emplace(key_type&& key, TArgs&&... args);

    template <class TIt>
    void insert(TIt first, TIt last);

    template <class TMapped>
    std::pair<iterator, bool> insert_or_assign(const key_type& key, TMapped&& obj);

    size_type erase(const key_type& key);
    iterator erase(iterator pos);
    iterator erase(const_iterator pos);
    iterator erase(const_iterator first, const_iterator last);

    mapped_type& operator[](const key_type& key);
    mapped_type& operator[](key_type&& key);

    mapped_type& at(const key_type& key);
    const mapped_type& at(const key_type& key) const;

private:
    template <bool IsConst>
    class TIteratorBase;

    using TArrayValue = value_type;
    struct alignas(TArrayValue) TStorage
    {
        std::byte Bytes[sizeof(TArrayValue)];
    };
    using TArrayIterator = TArrayValue*;
    using TArrayConstIterator = const TArrayValue*;
    using TInlineStorage = std::array<TStorage, N>;
    using TMap = std::map<key_type, mapped_type, key_compare>;
    using TMapIterator = typename TMap::iterator;
    using TMapConstIterator = typename TMap::const_iterator;

    TInlineStorage ArrayStorage_;
    size_type ArraySize_ = 0;
    TMap Map_;
    key_compare Compare_{};
    bool MapMode_ = false;

    TArrayIterator ArrayData();
    TArrayConstIterator ArrayData() const;
    TArrayIterator ArrayBegin();
    TArrayConstIterator ArrayBegin() const;
    TArrayIterator ArrayEnd();
    TArrayConstIterator ArrayEnd() const;
    void ClearArray();
    void CopyArrayFrom(const TCompactMap& other);
    void MoveArrayFrom(TCompactMap&& other);

    // Returns true if we are using the inline array.
    bool IsSmall() const;
    // Moves data from inline array to map.
    void UpgradeToMap();

    // Performs binary search in inline array. Returns iterator and found flag.
    template <class TOtherKey>
    std::pair<TArrayConstIterator, bool> ArrayLowerBound(const TOtherKey& key) const;
    template <class TOtherKey>
    std::pair<TArrayIterator, bool> ArrayLowerBound(const TOtherKey& key);

    template <class... TArgs>
    TArrayIterator EmplaceSmall(TArrayConstIterator pos, TArgs&&... args);
    TArrayIterator EraseSmall(TArrayConstIterator pos);
    TArrayIterator EraseSmall(TArrayConstIterator first, TArrayConstIterator last);

    template <class TArrayIteratorType, class TOtherKey>
    static std::pair<TArrayIteratorType, bool> ArrayLowerBoundImpl(
        TArrayIteratorType begin,
        TArrayIteratorType end,
        const TOtherKey& key,
        const key_compare& compare);

    template <class TKeyParam>
    mapped_type& Subscript(TKeyParam&& key);

    template <class TSelf>
    static auto BeginImpl(TSelf* self)
        -> std::conditional_t<std::is_const_v<TSelf>, const_iterator, iterator>;

    template <class TSelf>
    static auto EndImpl(TSelf* self)
        -> std::conditional_t<std::is_const_v<TSelf>, const_iterator, iterator>;

    template <class TSelf, class TOtherKey>
    static auto FindImpl(TSelf* self, const TOtherKey& key)
        -> std::conditional_t<std::is_const_v<TSelf>, const_iterator, iterator>;

    template <class TSelf>
    static auto AtImpl(TSelf* self, const key_type& key)
        -> std::conditional_t<std::is_const_v<TSelf>, const mapped_type&, mapped_type&>;

    template <class TKeyParam, class... TArgs>
    std::pair<iterator, bool> TryEmplaceImpl(TKeyParam&& key, TArgs&&... args);

    template <class TValueParam>
    std::pair<iterator, bool> DoInsert(TValueParam&& value);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define COMPACT_MAP_INL_H_
#include "compact_map-inl.h"
#undef COMPACT_MAP_INL_H_
