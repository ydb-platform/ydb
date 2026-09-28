#pragma once

#include "reflection.h"

#include <util/digest/sequence.h>
#include <util/generic/hash.h>
#include <util/generic/vector.h>

namespace NYql::NDetail {

template <typename T>
struct TYqlHash: ::THash<T> {
};

template <typename T>
struct TYqlHash<TVector<T>>: TRangeHash<TYqlHash<T>> {};

template <NYql::NReflection::CReflecting T>
struct TYqlReflectingHash {
    size_t operator()(const T& value) const {
        return Hash(value);
    }

private:
    Y_FORCE_INLINE static size_t Hash(const T& value) {
        static constexpr auto R = NReflection::TReflection<T>::SelfType();

        size_t total = 0;
        R.ForEachFieldValue(value, [&]<size_t Index, auto k>(const auto& value) {
            Y_UNUSED(k);

            using TValue = std::decay_t<decltype(value)>;
            const size_t hash = TYqlHash<TValue>{}(value);
            if constexpr (Index == 0) {
                total = hash;
            } else {
                total = CombineHashes(total, hash);
            }
        });

        return total;
    }
};

} // namespace NYql::NDetail

#define YQL_DERIVE_HASH(type)                                           \
    template <>                                                         \
    struct THash<::type>: ::NYql::NDetail::TYqlReflectingHash<::type> { \
    };

#define YQL_DEFINE_HASH(modifiers, type, value)                 \
    template <>                                                 \
    struct THash<::type> {                                      \
        modifiers size_t operator()(const ::type& value) const; \
    };                                                          \
    size_t THash<::type>::operator()(const ::type& value) const
