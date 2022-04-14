#pragma once

#include <util/generic/hash_set.h>

#include <map>
#include <memory>

namespace NTvmAuth::NRoles {
    using TEntity = std::map<TString, TString>;
    using TEntityPtr = std::shared_ptr<TEntity>;

    class TEntities;
    using TEntitiesPtr = std::shared_ptr<TEntities>;

    using TEntitiesByRoles = THashMap<TString, TEntitiesPtr>;

    class TConsumerRoles;
    using TConsumerRolesPtr = std::shared_ptr<TConsumerRoles>;

    class TRoles;
    using TRolesPtr = std::shared_ptr<TRoles>;

    using TRawPtr = std::shared_ptr<TString>;

    template <class T>
    struct TKeyValueBase {
        T Key;
        T Value;

        template <typename U>
        bool operator==(const TKeyValueBase<U>& o) const {
            return Key == o.Key && Value == o.Value;
        }
    };

    using TKeyValue = TKeyValueBase<TString>;
    using TKeyValueView = TKeyValueBase<TStringBuf>;
}

// Traits

template <>
struct THash<NTvmAuth::NRoles::TKeyValue> {
    std::size_t operator()(const NTvmAuth::NRoles::TKeyValue& e) const {
        return std::hash<std::string_view>()(e.Key) + std::hash<std::string_view>()(e.Value);
    }

    std::size_t operator()(const NTvmAuth::NRoles::TKeyValueView& e) const {
        return std::hash<std::string_view>()(e.Key) + std::hash<std::string_view>()(e.Value);
    }
};

template <>
struct TEqualTo<NTvmAuth::NRoles::TKeyValue> {
    using is_transparent = std::true_type;

    template <typename T, typename U>
    bool operator()(const NTvmAuth::NRoles::TKeyValueBase<T>& l,
                    const NTvmAuth::NRoles::TKeyValueBase<U>& r) {
        return l == r;
    }
};

inline bool operator<(const NTvmAuth::NRoles::TEntityPtr& l, const NTvmAuth::NRoles::TEntityPtr& r) {
    return *l < *r;
}

inline bool operator==(const NTvmAuth::NRoles::TEntityPtr& l, const NTvmAuth::NRoles::TEntityPtr& r) {
    return *l == *r;
}
