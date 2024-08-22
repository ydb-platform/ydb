#ifndef COLLECTION_HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include collection_helpers.h"
// For the sake of sane code completion.
#include "collection_helpers.h"
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace {

template <bool IsSet>
struct TKeyLess;

template <>
struct TKeyLess<true>
{
    template<typename T>
    bool operator()(const T& lhs, const T& rhs) const
    {
        return lhs < rhs;
    }
};

template <>
struct TKeyLess<false>
{
    template<typename T>
    bool operator()(const T& lhs, const T& rhs) const
    {
        return lhs.first < rhs.first;
    }
};

template <class TItem, class T, class TGetter>
std::vector<TItem> GetIthsImpl(const T& collection, size_t sizeLimit, const TGetter& getter)
{
    std::vector<TItem> result;
    result.reserve(std::min(collection.size(), sizeLimit));
    for (const auto& item : collection) {
        if (result.size() >= sizeLimit)
            break;
        result.push_back(getter(item));
    }
    return result;
}

} // namespace

template <class T, class C>
std::vector<typename T::const_iterator> GetSortedIterators(const T& collection, C comp)
{
    using TIterator = typename T::const_iterator;
    std::vector<TIterator> iterators;
    iterators.reserve(collection.size());
    for (auto it = collection.cbegin(); it != collection.cend(); ++it) {
        iterators.emplace_back(it);
    }

    std::sort(
        iterators.begin(),
        iterators.end(),
        [&] (auto lhsIt, auto rhtIt) {
            return comp(*lhsIt, *rhtIt);
        });

    return iterators;
}

template <class T>
std::vector<typename T::const_iterator> GetSortedIterators(const T& collection)
{
    using TIsSet = std::is_same<typename T::key_type, typename T::value_type>;
    return GetSortedIterators(collection, TKeyLess<TIsSet::value>());
}

template <class T>
std::vector<typename T::key_type> GetKeys(const T& collection, size_t sizeLimit)
{
    return GetIthsImpl<typename T::key_type>(
        collection,
        sizeLimit,
        [] (const auto& item) {
            return std::get<0u>(item);
        });
}

template <class T>
std::vector<typename T::mapped_type> GetValues(const T& collection, size_t sizeLimit)
{
    return GetIthsImpl<typename T::mapped_type>(
        collection,
        sizeLimit,
        [] (const auto& item) {
            return std::get<1u>(item);
        });
}

template <class T>
std::vector<typename T::value_type> GetItems(const T& collection, size_t sizeLimit)
{
    return GetIthsImpl<typename T::value_type>(
        collection,
        sizeLimit,
        [] (const auto& item) {
            return item;
        });
}

template <size_t I, class T>
std::vector<typename std::tuple_element<I, typename T::value_type>::type> GetIths(const T& collection, size_t sizeLimit)
{
    return GetIthsImpl<typename std::tuple_element<I, typename T::value_type>::type>(
        collection,
        sizeLimit,
        [] (const auto& item) {
            return std::get<I>(item);
        });
}

template <class T>
bool ShrinkHashTable(T&& collection)
{
    if (collection.bucket_count() <= 4 * collection.size() || collection.bucket_count() <= 16) {
        return false;
    }

    typename std::decay_t<decltype(collection)> collectionCopy(collection.begin(), collection.end());
    collectionCopy.swap(collection);
    return true;
}

template <class TSource, class TTarget>
void MergeFrom(TTarget* target, const TSource& source)
{
    for (const auto& item : source) {
        target->insert(item);
    }
}

template <class TMap, class TKeySet>
TKeySet DropMissingKeys(TMap&& map, const TKeySet& set)
{
    TKeySet dropped;
    for (auto it = map.begin(); it != map.end(); ) {
        if (!set.contains(it->first)) {
            dropped.insert(it->first);
            map.erase(it++);
        } else {
            ++it;
        }
    }
    return dropped;
}

template <class TMap, class TKey>
auto GetIteratorOrCrash(TMap&& map, const TKey& key)
{
    auto it = map.find(key);
    YT_VERIFY(it != map.end());
    return it;
}

template <class TMap, class TKey>
const auto& GetOrCrash(const TMap& map, const TKey& key)
{
    return GetIteratorOrCrash(map, key)->second;
}

template <class TMap, class TKey>
auto& GetOrCrash(TMap&& map, const TKey& key)
{
    return GetIteratorOrCrash(map, key)->second;
}

template <class TMap, class TKey>
void EraseOrCrash(TMap&& map, const TKey& key)
{
    YT_VERIFY(map.erase(key) > 0);
}

template <class TContainer, class TArg>
auto InsertOrCrash(TContainer&& container, TArg&& arg)
{
    auto [it, inserted] = container.insert(std::forward<TArg>(arg));
    YT_VERIFY(inserted);
    return it;
}

template <class TContainer, class... TArgs>
auto EmplaceOrCrash(TContainer&& container, TArgs&&... args)
{
    auto [it, emplaced] = container.emplace(std::forward<TArgs>(args)...);
    YT_VERIFY(emplaced);
    return it;
}

template <class T, class... TVariantArgs>
T& GetOrCrash(std::variant<TVariantArgs...>& variant)
{
    auto* item = get_if<T>(&variant);
    YT_VERIFY(item);
    return *item;
}

template <class T, class... TVariantArgs>
const T& GetOrCrash(const std::variant<TVariantArgs...>& variant)
{
    const auto* item = get_if<T>(&variant);
    YT_VERIFY(item);
    return *item;
}

template <class TMap, class TKey>
typename TMap::mapped_type GetOrDefault(
    const TMap& map,
    const TKey& key,
    const typename TMap::mapped_type& defaultValue)
    requires (!TIsDefaultMap<TMap>::Value)
{
    auto it = map.find(key);
    return it == map.end() ? defaultValue : it->second;
}

template <class TMap, class TKey, class TCtor>
auto& GetOrInsert(TMap&& map, const TKey& key, TCtor&& ctor)
{
    if constexpr (requires {typename TMap::insert_ctx;}) {
        typename TMap::insert_ctx context;
        auto it = map.find(key, context);
        if (it == map.end()) {
            it = map.emplace_direct(context, key, ctor()).first;
        }
        return it->second;
    } else {
        auto it = map.find(key);
        if (it == map.end()) {
            it = map.emplace(key, ctor()).first;
        }
        return it->second;
    }
}

////////////////////////////////////////////////////////////////////////////////

// See https://stackoverflow.com/questions/23439221/variadic-template-function-to-concatenate-stdvector-containers.
namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

// Nice syntax to allow in-order expansion of parameter packs.
struct TDoInOrder
{
    template <class T>
    TDoInOrder(std::initializer_list<T>&&) { }
};

// const& version.
template <class TVector>
void AppendVector(TVector& destination, const TVector& source)
{
    destination.insert(destination.end(), source.begin(), source.end());
}

// && version.
template <class TVector>
void AppendVector(TVector& destination, TVector&& source)
{
    destination.insert(
        destination.end(),
        std::make_move_iterator(source.begin()),
        std::make_move_iterator(source.end()));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

template <class TVector, class... TArgs>
TVector ConcatVectors(TVector first, TArgs&&... rest)
{
    // We need to put results somewhere; that's why we accept first parameter by value and use it as a resulting vector.
    // First, calculate total size of the result.
    std::size_t totalSize = first.size();
    NDetail::TDoInOrder { totalSize += rest.size() ... };
    first.reserve(totalSize);
    // Then, append all remaining arguments to first. Note that depending on rvalue-ness of the argument,
    // suitable overload of AppendVector will be used.
    NDetail::TDoInOrder { (NDetail::AppendVector(first, std::forward<TArgs>(rest)), 0)... };
    // Not quite sure why, but in the original article result is explicitly moved.
    return std::move(first);
}

template <class T>
void SortByFirst(T begin, T end)
{
    std::sort(begin, end, [] (const auto& lhs, const auto& rhs) { return lhs.first < rhs.first; });
}

template <class T>
void SortByFirst(T&& collection)
{
    SortByFirst(collection.begin(), collection.end());
}

template <class T>
std::vector<std::pair<typename T::key_type, typename T::mapped_type>> SortHashMapByKeys(const T& hashMap)
{
    std::vector<std::pair<typename T::key_type, typename T::mapped_type>> vector;
    vector.reserve(hashMap.size());
    for (const auto& [key, value] : hashMap) {
        vector.emplace_back(key, value);
    }
    SortByFirst(vector);
    return vector;
}

template <class T>
void EnsureVectorSize(std::vector<T>& vector, ssize_t size, const T& defaultValue)
{
    if (std::ssize(vector) < size) {
        vector.resize(size, defaultValue);
    }
}

template <class T>
void EnsureVectorIndex(std::vector<T>& vector, ssize_t index, const T& defaultValue)
{
    EnsureVectorSize(vector, index + 1, defaultValue);
}

template <class T>
void AssignVectorAt(std::vector<T>& vector, ssize_t index, const T& value, const T& defaultValue)
{
    EnsureVectorIndex(vector, index, defaultValue);
    vector[index] = value;
}

template <class T>
void AssignVectorAt(std::vector<T>& vector, ssize_t index, T&& value, const T& defaultValue)
{
    EnsureVectorIndex(vector, index, defaultValue);
    vector[index] = std::move(value);
}

template <class T>
const T& VectorAtOr(const std::vector<T>& vector, ssize_t index, const T& defaultValue)
{
    return index < std::ssize(vector) ? vector[index] : defaultValue;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
