// Hash map implementation wrappers

#pragma once

#include <yql/essentials/minikql/comp_nodes/mkql_rh_hash.h>
#include <yql/essentials/public/udf/udf_value.h>

#include <library/cpp/containers/absl_flat_hash/flat_hash_map.h>

#include <unordered_map>

namespace NKikimr {
namespace NMiniKQL {

template<typename K, typename V>
struct TUnorderedMapImpl
{
    using TValueType = V;
    using TMapType = std::unordered_map<K, V>;
    constexpr static bool CustomOps = false;
};

template<typename K, typename V>
struct TAbslMapImpl
{
    using TValueType = V;
    using TMapType = absl::flat_hash_map<K, V>;
    constexpr static bool CustomOps = false;
};

template<typename K, typename V, typename TEqualTo = std::equal_to<K>, typename THash = std::hash<K>>
struct TRobinHoodMapImplBase
{
    // Warning: this implementation leaks long strings because it can't call destructors.
    // Also it moves keys and values by simply copying bytes so take care.
    using TValueType = V;
    using TMapType = TRobinHoodHashFixedMap<K, V, TEqualTo, THash>;
    constexpr static bool CustomOps = true;

    static void AggregateByKey(TMapType& map, const K& key, const V& delta)
    {
        bool isNew = false;
        auto ptr = map.Insert(key, isNew);
        if (isNew) {
            *(V*)map.GetMutablePayload(ptr) = delta;
            map.CheckGrow();
        } else {
            *(V*)map.GetMutablePayload(ptr) += delta;
        }
    }

    template<typename Callback>
    static void IteratePairs(const TMapType& map, Callback&& callback)
    {
        // TODO: GetPayload and IsValid should be const
        for (const char* iter = map.Begin(); iter != map.End(); map.Advance(iter)) {
            if (!const_cast<TMapType&>(map).IsValid(iter)) {
                continue;
            }
            const auto& key = map.GetKey(iter);
            const auto& value = *(V*)(const_cast<TMapType&>(map)).GetPayload(iter);
            callback(key, value);
        }
    }

    static size_t Size(const TMapType& map)
    {
        return map.GetSize();
    }
};

template<typename K, typename V>
struct TRobinHoodMapImpl: public TRobinHoodMapImplBase<K, V>
{
};

template<typename V>
struct TRobinHoodMapImpl<std::string, V>
{
    using TValueType = V;

    struct TEqualTo
    {
        bool operator()(const NYql::NUdf::TUnboxedValuePod& lhs, const NYql::NUdf::TUnboxedValuePod& rhs) {
            return lhs.AsStringRef() == rhs.AsStringRef();
        }
    };

    struct THash
    {
        absl::Hash<std::string_view> AbslHash;

        size_t operator()(const NYql::NUdf::TUnboxedValuePod& val) {
            auto result = AbslHash(val.AsStringRef());
            return result;
        }
    };

    using TBase = TRobinHoodMapImplBase<std::string, V>;
    using TRealBase = TRobinHoodMapImplBase<NYql::NUdf::TUnboxedValuePod, V, TEqualTo, THash>;
    using TMapType = TRealBase::TMapType;

    constexpr static bool CustomOps = true;

    static void AggregateByKey(TMapType& map, const std::string& key, const V& delta)
    {
        NYql::NUdf::TUnboxedValuePod ub = NYql::NUdf::TUnboxedValuePod::Embedded(NYql::NUdf::TStringRef(key));
        TRealBase::AggregateByKey(map, ub, delta);
    }

    template<typename Callback>
    static void IteratePairs(const TMapType& map, Callback&& callback)
    {
        TRealBase::IteratePairs(map, [callback](const NYql::NUdf::TUnboxedValuePod& k, const V& v) {
            callback(std::string(k.AsStringRef()), v);
        });
    }

    static size_t Size(const TMapType& map)
    {
        return TRealBase::Size(map);
    }
};

template<typename TMapImpl>
bool MapEmpty(const typename TMapImpl::TMapType& map)
{
    if constexpr (TMapImpl::CustomOps) {
        return TMapImpl::Size(map) == 0;
    } else {
        return map.empty();
    }
}

}
}