#ifndef COLLECTION_HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include collection_helpers.h"
// For the sake of sane code completion.
#include "collection_helpers.h"
#endif

#include <library/cpp/yt/string/format.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template <bool IsSet>
struct TKeyLess;

template <>
struct TKeyLess<true>
{
    template <typename T>
    bool operator()(const T& lhs, const T& rhs) const
    {
        return lhs < rhs;
    }
};

template <>
struct TKeyLess<false>
{
    template <typename T>
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
        if (result.size() >= sizeLimit) {
            break;
        }
        result.push_back(getter(item));
    }
    return result;
}

} // namespace NDetail

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
    return GetSortedIterators(collection, NDetail::TKeyLess<TIsSet::value>());
}

template <class T>
std::vector<typename T::key_type> GetKeys(const T& collection, size_t sizeLimit)
{
    return NDetail::GetIthsImpl<typename T::key_type>(
        collection,
        sizeLimit,
        [] (const auto& item) {
            return std::get<0u>(item);
        });
}

template <class T>
THashSet<typename T::key_type> GetKeySet(const T& collection, size_t sizeLimit)
{
    auto vec = GetKeys(collection, sizeLimit);
    return THashSet<typename T::key_type>(vec.begin(), vec.end());
}

template <class T>
std::vector<typename T::mapped_type> GetValues(const T& collection, size_t sizeLimit)
{
    return NDetail::GetIthsImpl<typename T::mapped_type>(
        collection,
        sizeLimit,
        [] (const auto& item) {
            return std::get<1u>(item);
        });
}

template <class T>
std::vector<typename T::value_type> GetItems(const T& collection, size_t sizeLimit)
{
    return NDetail::GetIthsImpl<typename T::value_type>(
        collection,
        sizeLimit,
        /*getter*/ std::identity{});
}

template <size_t I, class T>
std::vector<std::tuple_element_t<I, typename T::value_type>> GetIths(const T& collection, size_t sizeLimit)
{
    return NDetail::GetIthsImpl<std::tuple_element_t<I, typename T::value_type>>(
        collection,
        sizeLimit,
        [] (const auto& item) {
            return std::get<I>(item);
        });
}

template <class T>
bool ShrinkHashTable(T& collection)
{
    if (collection.bucket_count() <= 4 * collection.size() || collection.bucket_count() <= 16) {
        return false;
    }

    collection = T(collection.begin(), collection.end());
    return true;
}

template <class TSource, class TTarget>
void MergeFrom(TTarget* target, const TSource& source)
{
    for (const auto& item : source) {
        target->insert(item);
    }
}

// NB: this and following functions take a forwarding reference instead of an lvalue reference
// so that they work with rvalue mutable views.
template <class TMap, class TKeySet>
TKeySet DropAndReturnMissingKeys(TMap&& map, const TKeySet& set)
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

template <class TMap, class TKeySet>
void DropMissingKeys(TMap&& map, const TKeySet& set)
{
    EraseNodesIf(map, [&] (const auto& keyValue) { return !set.contains(keyValue.first); });
}

template <class TMap, class TKey>
auto GetIteratorOrCrash(TMap&& map, const TKey& key)
{
    auto it = map.find(key);
    YT_VERIFY(it != map.end(), Format("GetIteratorOrCrash failed, key is not found in map (MapType: %v, KeyType: %v)",
        TypeName<TMap>(), TypeName<TKey>()));
    return it;
}

template <class TMap, class TKey>
auto& GetOrCrash(TMap&& map, const TKey& key)
{
    return GetIteratorOrCrash(std::forward<TMap>(map), key)->second;
}

template <class TMap, class TKey>
void EraseOrCrash(TMap&& map, const TKey& key)
{
    YT_VERIFY(std::forward<TMap>(map).erase(key) > 0, Format("EraseOrCrash failed, key is not found in map (MapType: %v, KeyType: %v)",
        TypeName<TMap>(), TypeName<TKey>()));
}

template <class TContainer, class TArg>
auto InsertOrCrash(TContainer&& container, TArg&& arg)
{
    auto [it, inserted] = std::forward<TContainer>(container).insert(std::forward<TArg>(arg));
    YT_VERIFY(inserted, Format("InsertOrCrash failed, item is already in container (ContainerType: %v, ItemArgType: %v)",
        TypeName<TContainer>(), TypeName<TArg>()));
    return it;
}

template <class TContainer, class... TArgs>
auto EmplaceOrCrash(TContainer&& container, TArgs&&... args)
{
    auto [it, emplaced] = std::forward<TContainer>(container).emplace(std::forward<TArgs>(args)...);
    YT_VERIFY(emplaced, Format("EmplaceOrCrash failed, item is already in container (ContainerType: %v, ItemArgsTupleType: %v)",
        TypeName<TContainer>(), TypeName<std::tuple<TArgs...>>()));
    return it;
}

template <class TMap, class TKey>
auto EmplaceDefault(TMap&& map, TKey&& key)
{
    return std::forward<TMap>(map).emplace(
        std::piecewise_construct,
        std::forward_as_tuple(std::forward<TKey>(key)),
        std::tuple{});
}

template <class T, class... TVariantArgs>
T& GetOrCrash(std::variant<TVariantArgs...>& variant)
{
    auto* item = get_if<T>(&variant);
    YT_VERIFY(item, Format("GetOrCrash failed, bad variant alternative (VariantType: %v, AlternativeType: %v)",
        TypeName<std::variant<TVariantArgs...>>(), TypeName<T>()));
    return *item;
}

template <class T, class... TVariantArgs>
const T& GetOrCrash(const std::variant<TVariantArgs...>& variant)
{
    const auto* item = get_if<T>(&variant);
    YT_VERIFY(item, Format("GetOrCrash failed, bad variant alternative (VariantType: %v, AlternativeType: %v)",
        TypeName<std::variant<TVariantArgs...>>(), TypeName<T>()));
    return *item;
}

template<typename T>
T& GetOrCrash(std::optional<T>& opt)
{
    YT_VERIFY(opt.has_value(), "GetOrCrash failed, value is nullopt");
    return *opt;
}

template<typename T>
const T& GetOrCrash(const std::optional<T>& opt)
{
    YT_VERIFY(opt.has_value(), "GetOrCrash failed, value is nullopt");
    return *opt;
}

template<typename T>
T&& GetOrCrash(std::optional<T>&& opt)
{
    YT_VERIFY(opt.has_value(), "GetOrCrash failed, value is nullopt");
    return *std::move(opt);
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

template <class TMap, class TKey>
const typename TMap::mapped_type& GetOrDefaultReference(
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
            it = map.emplace_direct(context, key, std::forward<TCtor>(ctor)());
        }
        return it->second;
    } else {
        auto it = map.find(key);
        if (it == map.end()) {
            it = map.emplace(key, std::forward<TCtor>(ctor)()).first;
        }
        return it->second;
    }
}

////////////////////////////////////////////////////////////////////////////////

// See https://stackoverflow.com/questions/23439221/variadic-template-function-to-concatenate-stdvector-containers.
namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

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
    first.reserve((first.size() + ... + rest.size()));
    // Then, append all remaining arguments to first. Note that depending on rvalue-ness of the argument,
    // suitable overload of AppendVector will be used.
    (NDetail::AppendVector(first, std::forward<TArgs>(rest)), ...);
    // Copy elison doesn't apply to function parameters, thus move.
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

template <class T, class TValue>
void AssignVectorAt(std::vector<T>& vector, ssize_t index, TValue&& value, const T& defaultValue)
{
    EnsureVectorIndex(vector, index, defaultValue);
    vector[index] = std::forward<TValue>(value);
}

template <class T>
const T& VectorAtOr(const std::vector<T>& vector, ssize_t index, const T& defaultValue)
{
    return index < std::ssize(vector) ? vector[index] : defaultValue;
}

template <class T>
i64 GetVectorMemoryUsage(const std::vector<T>& vector)
{
    return vector.capacity() * sizeof(T);
}

template <class TRange, class T>
bool Contains(TRange&& range, const T& value)
{
    return std::ranges::find(range, value) != std::ranges::end(range);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
