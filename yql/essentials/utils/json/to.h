#pragma once

#include <yql/essentials/utils/meta/maybe.h>

#include <library/cpp/json/json_value.h>
#include <library/cpp/json/json_writer.h>

#include <util/generic/string.h>

namespace NYql::NJson {

using namespace ::NJson;

template <typename T>
struct TToJson;

template <typename T>
concept CToJson = requires(T value) {
    { TToJson<T>{}(std::move(value)) } -> std::same_as<TJsonValue>;
};

template <CToJson T>
TJsonValue ToJson(T value) {
    return TToJson<T>{}(std::move(value));
}

template <std::convertible_to<TJsonValue> T>
struct TToJson<T> {
    TJsonValue operator()(T value) const {
        return TJsonValue(std::move(value));
    }
};

template <CToJson T>
struct TToJson<TVector<T>> {
    TJsonValue operator()(TVector<T> value) const {
        TJsonValue json(NJson::JSON_ARRAY);
        for (auto& item : value) {
            json.AppendValue(ToJson(std::move(item)));
        }
        return json;
    }
};

template <typename T>
void SaveTo(TJsonValue& json, TStringBuf key, T value) {
    if constexpr (IsMaybeV<T>) {
        if (value) {
            json.InsertValue(key, ToJson(std::move(*value)));
        }
    } else {
        json.InsertValue(key, ToJson(std::move(value)));
    }
}

template <CToJson T>
TString ToJsonString(T value) {
    auto json = ToJson(std::move(value));
    return WriteJson(json, /*formatOutput=*/false, /*sortkeys=*/true);
}

} // namespace NYql::NJson

#define JSON_DECLARE_TO(t, v)             \
    template <>                           \
    struct TToJson<t> {                   \
        TJsonValue operator()(t v) const; \
    }

#define JSON_DEFINE_TO(t, v) \
    TJsonValue TToJson<t>::operator()(t v) const
