#pragma once

#include "from.h"
#include "to.h"

#include <yql/essentials/utils/meta/reflection.h>
#include <yql/essentials/utils/meta/small_string.h>

namespace NYql::NJson::NDetail {

template <size_t N>
consteval TSmallString<N> PascalToCamelCase(TSmallString<N> x) {
    if (x.empty()) {
        return x;
    }

    if ('A' <= x[0] && x[0] <= 'Z') {
        x[0] = x[0] + ('a' - 'A');
    }

    return x;
}

template <NReflection::CReflecting T>
TJsonValue ToJsonReflecting(T value) {
    constexpr auto r = NReflection::TReflection<T>::SelfType();

    TJsonValue json(JSON_MAP);
    r.ForEachFieldValue(std::move(value), [&]<size_t Index, auto name>(auto&& value) {
        Y_UNUSED(Index);

        constexpr auto key = PascalToCamelCase(name);
        SaveTo(json, key, std::forward<decltype(value)>(value));
    });
    return json;
}

template <NReflection::CReflecting T>
TExpected<T> FromJsonReflecting(TJsonValue json) {
    constexpr auto r = NReflection::TReflection<T>::SelfType();

    T value;
    TMaybe<TString> error;
    r.ForEachFieldValue(value, [&]<size_t Index, auto name>(auto& value) {
        Y_UNUSED(Index);

        using TValue = std::decay_t<decltype(value)>;

        if (error) {
            return;
        }

        constexpr auto key = NDetail::PascalToCamelCase(name);

        auto parsed = MoveFrom<TValue>(json, key);
        if (!parsed) {
            error = std::move(parsed.error());
            return;
        }

        value = std::move(*parsed);
    });

    if (error) {
        return Unexpected(std::move(*error));
    }

    return value;
}

} // namespace NYql::NJson::NDetail

#define YQL_DERIVE_JSON_FROM(type)                                 \
    JSON_DEFINE_FROM(type, json) {                                 \
        return NDetail::FromJsonReflecting<type>(std::move(json)); \
    }

#define YQL_DERIVE_JSON_TO(type)                                  \
    JSON_DEFINE_TO(type, value) {                                 \
        return NDetail::ToJsonReflecting<type>(std::move(value)); \
    }

#define YQL_DERIVE_JSON_BIDIRECTIONAL(type) \
    YQL_DERIVE_JSON_FROM(type)              \
    YQL_DERIVE_JSON_TO(type)
