#pragma once

#include "common.h"
#include "default_map.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
std::vector<typename T::const_iterator> GetSortedIterators(const T& collection);

template <class T, class C>
std::vector<typename T::const_iterator> GetSortedIterators(const T& collection, C comp);

template <class T>
std::vector<typename T::key_type> GetKeys(
    const T& collection,
    size_t sizeLimit = std::numeric_limits<size_t>::max());

template <class T>
std::vector<typename T::mapped_type> GetValues(
    const T& collection,
    size_t sizeLimit = std::numeric_limits<size_t>::max());

template <class T>
std::vector<typename T::value_type> GetItems(
    const T& collection,
    size_t sizeLimit = std::numeric_limits<size_t>::max());

template <size_t N, class T>
std::vector<typename std::tuple_element<N, typename T::value_type>::type> GetIths(
    const T& collection,
    size_t sizeLimit = std::numeric_limits<size_t>::max());

template <class T>
bool ShrinkHashTable(T&& collection);

template <class TSource, class TTarget>
void MergeFrom(TTarget* target, const TSource& source);

template <class TMap, class TKeySet>
TKeySet DropMissingKeys(TMap&& map, const TKeySet& set);

/*!
 * This function is supposed to replace a frequent pattern
 *    auto it = map.find(key);
 *    YT_VERIFY(it != map.end());
 *    use it;
 * with
 *    use GetIteratorOrCrash(map, key);
 */
template <class TMap, class TKey>
auto GetIteratorOrCrash(TMap&& map, const TKey& key);

/*!
 * This function is supposed to replace a frequent pattern
 *    auto it = map.find(key);
 *    YT_VERIFY(it != map.end());
 *    use it->second;
 * with
 *    use GetOrCrash(map, key);
 */
template <class TMap, class TKey>
const auto& GetOrCrash(const TMap& map, const TKey& key);

template <class TMap, class TKey>
auto& GetOrCrash(TMap&& map, const TKey& key);

/*!
 * This function is supposed to replace a frequent pattern
 *    YT_VERIFY(map.erase(key) > 0);
 * with
 *    EraseOrCrash(map, key);
 */
template <class TMap, class TKey>
void EraseOrCrash(TMap&& map, const TKey& key);

/*!
 * This function is supposed to replace a frequent pattern
 *    YT_VERIFY(map.insert(pair).second);
 * with
 *    InsertOrCrash(map, pair);
 */
template <class TContainer, class TArg>
auto InsertOrCrash(TContainer&& container, TArg&& arg);

/*!
 * This function is supposed to replace a frequent pattern
 *    YT_VERIFY(map.emplace(key, value).second);
 * with
 *    EmplaceOrCrash(map, key, value);
 */
template <class TContainer, class... TArgs>
auto EmplaceOrCrash(TContainer&& container, TArgs&&... args);

/*!
 * This function is supposed to replace std::get<T>(variant)
 * for those cases when exception should not be thrown.
 */
template <class T, class... TVariantArgs>
T& GetOrCrash(std::variant<TVariantArgs...>& variant);

template <class T, class... TVariantArgs>
const T& GetOrCrash(const std::variant<TVariantArgs...>& variant);

/*!
 * Returns the copy of the value in #map if #key is present
 * of the copy of #defaultValue otherwise.
 */
template <class TMap, class TKey>
typename TMap::mapped_type GetOrDefault(
    const TMap& map,
    const TKey& key,
    const typename TMap::mapped_type& defaultValue = {})
    requires (!TIsDefaultMap<TMap>::Value);

template <class TMap, class TKey, class TCtor>
auto& GetOrInsert(TMap&& map, const TKey& key, TCtor&& ctor);

template <class TVector, class... TArgs>
TVector ConcatVectors(TVector first, TArgs&&... rest);

template <class T>
void SortByFirst(T begin, T end);

template <class T>
void SortByFirst(T&& collection);

template <class T>
std::vector<std::pair<typename T::key_type, typename T::mapped_type>> SortHashMapByKeys(const T& hashMap);

// Below follow helpers for representing a map (small unsigned integer) -> T over std::vector<T> using keys as indices.

//! If vector size is less than provided size, resize vector up to provided size.
template <class T>
void EnsureVectorSize(std::vector<T>& vector, ssize_t size, const T& defaultValue = T());

//! If vector size is not enough for vector[index] to exist, resize vector up to index + 1.
template <class T>
void EnsureVectorIndex(std::vector<T>& vector, ssize_t index, const T& defaultValue = T());

//! If vector size is not enough for vector[size] to exist, resize vector to size + 1.
//! After that perform assignment vector[size] = value. Const reference version.
template <class T>
void AssignVectorAt(std::vector<T>& vector, ssize_t index, const T& value, const T& defaultValue = T());

//! If vector size is not enough for vector[size] to exist, resize vector to size + 1.
//! After that perform assignment vector[size] = std::move(value). Rvalue reference version.
template <class T>
void AssignVectorAt(std::vector<T>& vector, ssize_t index, T&& value, const T& defaultValue = T());

//! If vector size is not enough for vector[size] to exist, return defaultValue, otherwise return vector[size].
template <class T>
const T& VectorAtOr(const std::vector<T>& vector, ssize_t index, const T& defaultValue = T());

template <class T>
i64 GetVectorMemoryUsage(const std::vector<T>& vector);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define COLLECTION_HELPERS_INL_H_
#include "collection_helpers-inl.h"
#undef COLLECTION_HELPERS_INL_H_
