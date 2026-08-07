#pragma once

#include "expected.h"
#include "meta.h"

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_value.h>

#include <util/generic/maybe.h>
#include <util/generic/string.h>

#include <expected>

namespace NYql::NJson {

using namespace ::NJson;

template <typename T>
struct TFromJson;

template <typename T>
concept CFromJson = requires(TJsonValue json) {
    { TFromJson<T>{}(std::move(json)) } -> std::same_as<TExpected<T>>;
};

template <CFromJson T>
TExpected<T> FromJson(TJsonValue json) {
    return TFromJson<T>{}(std::move(json));
}

template <>
struct TFromJson<TJsonValue> {
    TExpected<TJsonValue> operator()(TJsonValue json) const;
};

template <>
struct TFromJson<TString> {
    TExpected<TString> operator()(TJsonValue json) const;
};

template <>
struct TFromJson<i64> {
    TExpected<i64> operator()(TJsonValue json) const;
};

template <>
struct TFromJson<ui64> {
    TExpected<ui64> operator()(TJsonValue json) const;
};

template <>
struct TFromJson<bool> {
    TExpected<bool> operator()(TJsonValue json) const;
};

template <CFromJson T>
struct TFromJson<TVector<T>> {
    TExpected<TVector<T>> operator()(TJsonValue json) const {
        if (!json.IsArray()) {
            return Unexpected("must be an array");
        }

        TVector<T> result(Reserve(json.GetArraySafe().size()));
        for (auto& item : json.GetArraySafe()) {
            TExpected<T> parsed = FromJson<T>(std::move(item));
            if (!parsed) {
                return Unexpected(std::move(parsed.error()));
            }

            result.emplace_back(std::move(*parsed));
        }
        return result;
    }
};

template <typename T>
TExpected<T> MoveFrom(TJsonValue& json, TStringBuf key) {
    constexpr bool IsMaybe = NDetail::IsMaybeV<T>;

    using V = typename decltype([] {
        if constexpr (IsMaybe) {
            return std::type_identity<typename T::TValueType>{};
        } else {
            return std::type_identity<T>{};
        }
    }())::type;

    static_assert(!NDetail::IsMaybeV<V>, "TMaybe<TMaybe<T>> is not supported");
    static_assert(CFromJson<V>);

    NJson::TJsonValue* value = nullptr;
    const bool ok = json.GetValuePointer(key, &value);

    if constexpr (IsMaybe) {
        if (!ok) {
            return T();
        }
    } else {
        if (!ok) {
            return UnexpectedField(key, "is required");
        }
    }

    TExpected<V> expected = FromJson<V>(std::move(*value));
    if (!expected) {
        return UnexpectedField(key, expected.error());
    }

    return T(std::move(*expected));
}

template <CFromJson T>
TExpected<T> FromJsonString(TStringBuf string) try {
    NJson::TJsonValue json;
    Y_ENSURE(ReadJsonTree(string, &json, /*throwOnError=*/true));
    return NYql::NJson::FromJson<T>(std::move(json));
} catch (const TJsonException& e) {
    return Unexpected(TString::Join("bad json: ", e.what()));
}

} // namespace NYql::NJson

#define JSON_MOVE_FROM(source, key, target)                                  \
    do {                                                                     \
        using Y_CAT(T, __LINE__) = std::decay_t<decltype(target)>;           \
                                                                             \
        auto Y_CAT(x, __LINE__) = MoveFrom<Y_CAT(T, __LINE__)>(source, key); \
        if (!Y_CAT(x, __LINE__)) {                                           \
            return Unexpected(std::move(Y_CAT(x, __LINE__).error()));        \
        }                                                                    \
                                                                             \
        (target) = std::move(*Y_CAT(x, __LINE__));                           \
    } while (false)

#define JSON_DECLARE_FROM(t, v)                             \
    template <>                                             \
    struct TFromJson<t> {                                   \
        TExpected<t> operator()(NJson::TJsonValue v) const; \
    }

#define JSON_DEFINE_FROM(t, v) \
    TExpected<t> TFromJson<t>::operator()(TJsonValue v) const
