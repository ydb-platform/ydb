#pragma once

// Internal implementation for TRowRange::Get. Not a supported extension point.

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/value/value.h>

#include <array>
#include <chrono>
#include <concepts>
#include <optional>
#include <stdexcept>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>
#include <variant>

namespace NYdb::inline Dev {

namespace NRowRangesDetail {

namespace NParserScope {

template <auto Open, auto Close>
struct TParserScope {
    explicit TParserScope(TValueParser& parser)
        : Parser_(parser)
    {
        (Parser_.*Open)();
    }

    ~TParserScope() {
        (Parser_.*Close)();
    }

    TParserScope(const TParserScope&) = delete;
    TParserScope& operator=(const TParserScope&) = delete;

    TValueParser& Parser_;
};

using TOptionalScope = TParserScope<&TValueParser::OpenOptional, &TValueParser::CloseOptional>;
using TListScope = TParserScope<&TValueParser::OpenList, &TValueParser::CloseList>;
using TDictScope = TParserScope<&TValueParser::OpenDict, &TValueParser::CloseDict>;
using TTupleScope = TParserScope<&TValueParser::OpenTuple, &TValueParser::CloseTuple>;
using TStructScope = TParserScope<&TValueParser::OpenStruct, &TValueParser::CloseStruct>;
using TVariantScope = TParserScope<&TValueParser::OpenVariant, &TValueParser::CloseVariant>;
using TTaggedScope = TParserScope<&TValueParser::OpenTagged, &TValueParser::CloseTagged>;

} // namespace NParserScope

//! Maps a C++ type to the TValueParser accessor that reads it.
//! Unspecialised T fails to compile ("incomplete type") — that is the intended
//! error for unsupported column types.
template <class T>
struct TValueParserGetter;

using TDate32 = std::chrono::sys_time<TWideDays>;
using TDatetime64 = std::chrono::sys_time<TWideSeconds>;
using TTimestamp64 = std::chrono::sys_time<TWideMicroseconds>;

//! Any default-constructible sequence container with push_back/emplace_back.
template <class T>
concept CRowSequenceContainer = requires(T container, typename T::value_type value) {
    typename T::value_type;
    { T{} } -> std::same_as<T>;
    requires requires { container.push_back(value); } ||
             requires { container.emplace_back(value); };
};

//! Any default-constructible associative container with emplace or insert.
template <class T>
concept CRowAssociativeContainer = requires(T container, typename T::key_type key, typename T::mapped_type mapped) {
    typename T::key_type;
    typename T::mapped_type;
    { T{} } -> std::same_as<T>;
    requires requires { container.emplace(key, mapped); } ||
             requires { container.insert({key, mapped}); };
};

template <class T>
struct TValueParserGetter<std::optional<T>> {
    static std::optional<T> Get(TValueParser& p) {
        NParserScope::TOptionalScope scope(p);
        if (p.IsNull()) {
            return std::nullopt;
        }
        return TValueParserGetter<T>::Get(p);
    }
};

#define Y_DEFINE_VALUE_PARSER_GETTER(Type, Suffix) \
    template <> struct TValueParserGetter<Type> { \
        static Type Get(TValueParser& p) { return p.Get##Suffix(); } \
    }

Y_DEFINE_VALUE_PARSER_GETTER(bool,        Bool);
Y_DEFINE_VALUE_PARSER_GETTER(int8_t,      Int8);
Y_DEFINE_VALUE_PARSER_GETTER(uint8_t,     Uint8);
Y_DEFINE_VALUE_PARSER_GETTER(int16_t,     Int16);
Y_DEFINE_VALUE_PARSER_GETTER(uint16_t,    Uint16);
Y_DEFINE_VALUE_PARSER_GETTER(int32_t,     Int32);
Y_DEFINE_VALUE_PARSER_GETTER(uint32_t,    Uint32);
Y_DEFINE_VALUE_PARSER_GETTER(uint64_t,    Uint64);
Y_DEFINE_VALUE_PARSER_GETTER(float,       Float);
Y_DEFINE_VALUE_PARSER_GETTER(double,      Double);
Y_DEFINE_VALUE_PARSER_GETTER(TUuidValue,  Uuid);
Y_DEFINE_VALUE_PARSER_GETTER(TDate32,     Date32);
Y_DEFINE_VALUE_PARSER_GETTER(TDatetime64, Datetime64);
Y_DEFINE_VALUE_PARSER_GETTER(TTimestamp64, Timestamp64);
Y_DEFINE_VALUE_PARSER_GETTER(TWideMicroseconds, Interval64);
Y_DEFINE_VALUE_PARSER_GETTER(TDecimalValue, Decimal);
Y_DEFINE_VALUE_PARSER_GETTER(TPgValue,    Pg);

#undef Y_DEFINE_VALUE_PARSER_GETTER

template <> struct TValueParserGetter<int64_t> {
    static int64_t Get(TValueParser& p) {
        switch (p.GetPrimitiveType()) {
            case EPrimitiveType::Int64:   return p.GetInt64();
            case EPrimitiveType::Interval: return p.GetInterval();
            default:
                throw std::runtime_error(
                    "TValueParserGetter<int64_t>: column type is not Int64/Interval");
        }
    }
};

template <> struct TValueParserGetter<std::string> {
    static std::string Get(TValueParser& p) {
        switch (p.GetPrimitiveType()) {
            case EPrimitiveType::String:      return p.GetString();
            case EPrimitiveType::Utf8:        return p.GetUtf8();
            case EPrimitiveType::Yson:        return p.GetYson();
            case EPrimitiveType::Json:        return p.GetJson();
            case EPrimitiveType::JsonDocument: return p.GetJsonDocument();
            case EPrimitiveType::DyNumber:    return p.GetDyNumber();
            case EPrimitiveType::TzDate:      return p.GetTzDate();
            case EPrimitiveType::TzDatetime:  return p.GetTzDatetime();
            case EPrimitiveType::TzTimestamp: return p.GetTzTimestamp();
            default:
                throw std::runtime_error(
                    "TValueParserGetter<std::string>: column type is not a string-like primitive");
        }
    }
};

//! TInstant maps to whichever date/time primitive the column actually holds:
//! Date, Datetime, or Timestamp.
template <> struct TValueParserGetter<TInstant> {
    static TInstant Get(TValueParser& p) {
        switch (p.GetPrimitiveType()) {
            case EPrimitiveType::Date:      return p.GetDate();
            case EPrimitiveType::Datetime:  return p.GetDatetime();
            case EPrimitiveType::Timestamp: return p.GetTimestamp();
            default:
                throw std::runtime_error(
                    "TValueParserGetter<TInstant>: column type is not Date/Datetime/Timestamp");
        }
    }
};

namespace NContainerDetail {

template <class Sequence, class Value>
void PushBack(Sequence& sequence, Value&& value) {
    if constexpr (requires {
        sequence.emplace_back(std::forward<Value>(value));
    }) {
        sequence.emplace_back(std::forward<Value>(value));
    } else {
        sequence.push_back(std::forward<Value>(value));
    }
}

template <class Map, class Key, class Value>
void EmplaceMapEntry(Map& map, Key&& key, Value&& value) {
    if constexpr (requires {
        map.emplace(std::forward<Key>(key), std::forward<Value>(value));
    }) {
        map.emplace(std::forward<Key>(key), std::forward<Value>(value));
    } else {
        map.insert({std::forward<Key>(key), std::forward<Value>(value)});
    }
}

template <CRowSequenceContainer Sequence>
Sequence GetList(TValueParser& p) {
    using Element = typename Sequence::value_type;
    Sequence result;
    NParserScope::TListScope scope(p);
    while (p.TryNextListItem()) {
        PushBack(result, TValueParserGetter<Element>::Get(p));
    }
    return result;
}

template <CRowAssociativeContainer Map>
Map GetDict(TValueParser& p) {
    using Key = typename Map::key_type;
    using Value = typename Map::mapped_type;

    Map result;
    NParserScope::TDictScope scope(p);
    while (p.TryNextDictItem()) {
        p.DictKey();
        Key key = TValueParserGetter<Key>::Get(p);
        p.DictPayload();
        Value value = TValueParserGetter<Value>::Get(p);
        EmplaceMapEntry(result, std::move(key), std::move(value));
    }
    return result;
}

} // namespace NContainerDetail

template <CRowSequenceContainer Sequence>
struct TValueParserGetter<Sequence> {
    static Sequence Get(TValueParser& p) {
        if (p.GetKind() != TTypeParser::ETypeKind::List) {
            throw std::runtime_error("TValueParserGetter<Sequence>: column type is not List");
        }
        return NContainerDetail::GetList<Sequence>(p);
    }
};

template <CRowAssociativeContainer Map>
struct TValueParserGetter<Map> {
    static Map Get(TValueParser& p) {
        if (p.GetKind() != TTypeParser::ETypeKind::Dict) {
            throw std::runtime_error("TValueParserGetter<Map>: column type is not Dict");
        }
        return NContainerDetail::GetDict<Map>(p);
    }
};

namespace NTupleDetail {

template <class Tuple, size_t I>
void GetTupleElement(TValueParser& p, Tuple& result, bool (*TryNext)(TValueParser&)) {
    if (!TryNext(p)) {
        throw std::runtime_error("TValueParserGetter<std::tuple<...>>: not enough elements");
    }
    std::get<I>(result) = TValueParserGetter<std::tuple_element_t<I, Tuple>>::Get(p);
}

template <class Tuple, size_t... Is>
Tuple GetTupleElements(TValueParser& p, std::index_sequence<Is...>, bool (*TryNext)(TValueParser&)) {
    Tuple result;
    (GetTupleElement<Tuple, Is>(p, result, TryNext), ...);
    return result;
}

inline void CheckNoExtraElements(TValueParser& p, bool (*TryNext)(TValueParser&)) {
    if (TryNext(p)) {
        throw std::runtime_error("TValueParserGetter<std::tuple<...>>: too many elements");
    }
}

inline bool TryNextTupleElement(TValueParser& p) {
    return p.TryNextElement();
}

inline bool TryNextStructMember(TValueParser& p) {
    return p.TryNextMember();
}

template <class... Args>
std::tuple<Args...> GetTupleFromContainer(
    TValueParser& p,
    TTypeParser::ETypeKind expectedKind,
    bool (*TryNext)(TValueParser&)) {
    if (p.GetKind() != expectedKind) {
        throw std::runtime_error("TValueParserGetter<std::tuple<...>>: column type mismatch");
    }
    if (expectedKind == TTypeParser::ETypeKind::Tuple) {
        NParserScope::TTupleScope scope(p);
        auto result = GetTupleElements<std::tuple<Args...>>(
            p, std::index_sequence_for<Args...>{}, TryNext);
        CheckNoExtraElements(p, TryNext);
        return result;
    }
    NParserScope::TStructScope scope(p);
    auto result = GetTupleElements<std::tuple<Args...>>(
        p, std::index_sequence_for<Args...>{}, TryNext);
    CheckNoExtraElements(p, TryNext);
    return result;
}

} // namespace NTupleDetail

template <class... Args>
struct TValueParserGetter<std::tuple<Args...>> {
    static std::tuple<Args...> Get(TValueParser& p) {
        switch (p.GetKind()) {
            case TTypeParser::ETypeKind::Tuple:
                return NTupleDetail::GetTupleFromContainer<Args...>(
                    p, TTypeParser::ETypeKind::Tuple, NTupleDetail::TryNextTupleElement);
            case TTypeParser::ETypeKind::Struct:
                return NTupleDetail::GetTupleFromContainer<Args...>(
                    p, TTypeParser::ETypeKind::Struct, NTupleDetail::TryNextStructMember);
            default:
                throw std::runtime_error(
                    "TValueParserGetter<std::tuple<...>>: column type is not Tuple/Struct");
        }
    }
};

namespace NVariantDetail {

template <class Variant, size_t I>
bool TryEmplaceVariantAlternative(Variant& result, TValueParser& p, size_t index) {
    if (index != I) {
        return false;
    }
    result.template emplace<I>(
        TValueParserGetter<std::variant_alternative_t<I, Variant>>::Get(p));
    return true;
}

template <class Variant, size_t... Is>
bool EmplaceVariantAlternative(Variant& result, TValueParser& p, size_t index, std::index_sequence<Is...>) {
    return (TryEmplaceVariantAlternative<Variant, Is>(result, p, index) || ...);
}

} // namespace NVariantDetail

template <class... Args>
struct TValueParserGetter<std::variant<Args...>> {
    static std::variant<Args...> Get(TValueParser& p) {
        if (p.GetKind() != TTypeParser::ETypeKind::Variant) {
            throw std::runtime_error("TValueParserGetter<std::variant<...>>: column type is not Variant");
        }
        const size_t index = p.GetVariantIndex();
        NParserScope::TVariantScope scope(p);
        std::variant<Args...> result;
        if (!NVariantDetail::EmplaceVariantAlternative(
                result, p, index, std::index_sequence_for<Args...>{})) {
            throw std::runtime_error("TValueParserGetter<std::variant<...>>: variant index out of range");
        }
        return result;
    }
};

template <class T>
struct TValueParserGetter<std::pair<std::string, T>> {
    static std::pair<std::string, T> Get(TValueParser& p) {
        if (p.GetKind() != TTypeParser::ETypeKind::Tagged) {
            throw std::runtime_error(
                "TValueParserGetter<std::pair<std::string, T>>: column type is not Tagged");
        }
        NParserScope::TTaggedScope scope(p);
        std::string tag = p.GetTag();
        T value = TValueParserGetter<T>::Get(p);
        return {std::move(tag), std::move(value)};
    }
};

template <size_t N, class It>
std::array<std::string, N> BuildColumnNames(It first, It last) {
    std::array<std::string, N> names;
    size_t i = 0;
    for (auto it = first; it != last; ++it) {
        if (i >= N) {
            throw std::invalid_argument(
                "TRowRange::Get: too many column names for the requested types");
        }
        names[i++] = std::string(*it);
    }
    if (i != N) {
        throw std::invalid_argument(
            "TRowRange::Get: not enough column names for the requested types");
    }
    return names;
}

} // namespace NRowRangesDetail
} // namespace NYdb::inline Dev
