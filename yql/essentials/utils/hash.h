#pragma once
#include <unordered_set>
#include <unordered_map>
#include <util/generic/hash.h>
#include <util/generic/hash_multi_map.h>
#include <util/generic/hash_set.h>

namespace NYql {

#ifndef NDEBUG
size_t VaryingHash(size_t src);
#else
inline size_t VaryingHash(size_t src) {
    return src;
}
#endif

template <typename T, typename THasher = ::THash<T>>
struct TVaryingHash {
    THasher Underlying;

    TVaryingHash() = default;
    TVaryingHash(const TVaryingHash&) = default;
    TVaryingHash(const THasher& underlying)
        : Underlying(underlying)
    {}

    TVaryingHash& operator=(const TVaryingHash& other) = default;

    size_t operator()(const T& elem) const {
        return VaryingHash(Underlying(elem));
    }
};

template <class TKey,
    class TValue,
    class THasher = std::hash<TKey>,
    class TEqual = std::equal_to<TKey>,
    class TAlloc = std::allocator<std::pair<const TKey, TValue>>>
using TVaryingUnorderedMap = std::unordered_map<TKey, TValue, TVaryingHash<TKey, THasher>, TEqual, TAlloc>;

template <class TKey,
    class TValue,
    class THasher = std::hash<TKey>,
    class TEqual = std::equal_to<TKey>,
    class TAlloc = std::allocator<std::pair<const TKey, TValue>>>
using TVaryingUnorderedMultiMap = std::unordered_multimap<TKey, TValue, TVaryingHash<TKey, THasher>, TEqual, TAlloc>;

template <class TKey,
    class THasher = std::hash<TKey>,
    class TEqual = std::equal_to<TKey>,
    class TAlloc = std::allocator<TKey>>
using TVaryingUnorderedSet = std::unordered_set<TKey, TVaryingHash<TKey, THasher>, TEqual, TAlloc>;

template <class TKey,
    class THasher = std::hash<TKey>,
    class TEqual = std::equal_to<TKey>,
    class TAlloc = std::allocator<TKey>>
using TVaryingUnorderedMultiSet = std::unordered_multiset<TKey, TVaryingHash<TKey, THasher>, TEqual, TAlloc>;

template <class TKey,
    class TValue,
    class THasher = THash<TKey>,
    class TEqual = TEqualTo<TKey>,
    class TAlloc = std::allocator<std::pair<const TKey, TValue>>>
using TVaryingHashMap = THashMap<TKey, TValue, TVaryingHash<TKey, THasher>, TEqual, TAlloc>;

template <class TKey,
    class TValue,
    class THasher = THash<TKey>,
    class TEqual = TEqualTo<TKey>,
    class TAlloc = std::allocator<std::pair<const TKey, TValue>>>
using TVaryingHashMultiMap = THashMultiMap<TKey, TValue, TVaryingHash<TKey, THasher>, TEqual, TAlloc>;

template <class TKey,
    class THasher = THash<TKey>,
    class TEqual = TEqualTo<TKey>,
    class TAlloc = std::allocator<TKey>>
using TVaryingHashSet = THashSet<TKey, TVaryingHash<TKey, THasher>, TEqual, TAlloc>;

template <class TKey,
    class THasher = THash<TKey>,
    class TEqual = TEqualTo<TKey>,
    class TAlloc = std::allocator<TKey>>
using TVaryingHashMultiSet = THashMultiSet<TKey, TVaryingHash<TKey, THasher>, TEqual, TAlloc>;

} // namespace NYql
