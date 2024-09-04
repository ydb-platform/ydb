#ifndef CONVERT_INL_H_
#error "Direct inclusion of this file is not allowed, include convert.h"
// For the sake of sane code completion.
#include "convert.h"
#endif

#include "attribute_consumer.h"
#include "default_building_consumer.h"
#include "serialize.h"
#include "tree_builder.h"
#include "helpers.h"

#include <yt/yt/core/ypath/token.h>

#include <yt/yt/core/yson/tokenizer.h>
#include <yt/yt/core/yson/parser.h>
#include <yt/yt/core/yson/stream.h>
#include <yt/yt/core/yson/producer.h>
#include <yt/yt/core/yson/pull_parser.h>
#include <yt/yt/core/yson/pull_parser_deserialize.h>

#include <library/cpp/yt/misc/cast.h>

#include <type_traits>
#include <limits>

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

template <class T>
TYsonString ConvertToYsonString(const T& value)
{
    return ConvertToYsonString(value, EYsonFormat::Binary);
}

template <class T>
TYsonString ConvertToYsonString(const T& value, EYsonFormat format)
{
    auto type = NYTree::GetYsonType(value);
    TString result;
    TStringOutput stringOutput(result);
    NYTree::WriteYson(&stringOutput, value, type, format);
    return NYson::TYsonString(std::move(result), type);
}

template <class T>
TYsonString ConvertToYsonString(const T& value, EYsonFormat format, int indent)
{
    auto type = NYTree::GetYsonType(value);
    TString result;
    TStringOutput stringOutput(result);
    NYTree::WriteYson(&stringOutput, value, type, format, indent);
    return NYson::TYsonString(std::move(result), type);
}

template <class T>
TYsonString ConvertToYsonStringNestingLimited(const T& value, int nestingLevelLimit)
{
    using NYTree::Serialize;

    auto type = NYTree::GetYsonType(value);
    TStringStream stream;
    {
        TBufferedBinaryYsonWriter writer(
            &stream,
            EYsonType::Node,
            /*enableRaw*/ false,
            nestingLevelLimit);
        Serialize(value, &writer);
        writer.Flush();
    }
    return NYson::TYsonString(std::move(stream.Str()), type);
}

template <>
TYsonString ConvertToYsonStringNestingLimited(const TYsonString& value, int nestingLevelLimit);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

template <class T>
NYson::TYsonProducer ConvertToProducer(T&& value)
{
    auto type = GetYsonType(value);
    auto callback = BIND(
        [] (const T& value, NYson::IYsonConsumer* consumer) {
            Serialize(value, consumer);
        },
        std::forward<T>(value));
    return NYson::TYsonProducer(std::move(callback), type);
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
INodePtr ConvertToNode(
    const T& value,
    INodeFactory* factory,
    int treeSizeLimit)
{
    auto type = GetYsonType(value);

    auto builder = CreateBuilderFromFactory(factory, treeSizeLimit);
    builder->BeginTree();

    switch (type) {
        case NYson::EYsonType::ListFragment:
            builder->OnBeginList();
            break;
        case NYson::EYsonType::MapFragment:
            builder->OnBeginMap();
            break;
        default:
            break;
    }

    Serialize(value, builder.get());

    switch (type) {
        case NYson::EYsonType::ListFragment:
            builder->OnEndList();
            break;
        case NYson::EYsonType::MapFragment:
            builder->OnEndMap();
            break;
        default:
            break;
    }

    return builder->EndTree();
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
IAttributeDictionaryPtr ConvertToAttributes(const T& value)
{
    auto attributes = CreateEphemeralAttributes();
    TAttributeConsumer consumer(attributes.Get());
    Serialize(value, &consumer);
    return attributes;
}

////////////////////////////////////////////////////////////////////////////////

template <class TTo>
TTo ConvertTo(const INodePtr& node)
{
    auto result = ConstructYTreeConvertibleObject<TTo>();
    Deserialize(result, node);
    return result;
}

template <class TTo, class TFrom>
TTo ConvertTo(const TFrom& value)
{
    auto type = GetYsonType(value);
    if constexpr (
        NYson::ArePullParserDeserializable<TTo>() &&
        (std::is_same_v<TFrom, NYson::TYsonString> || std::is_same_v<TFrom, NYson::TYsonStringBuf>))
    {
        using NYson::Deserialize;
        using NYTree::Deserialize;

        TMemoryInput input(value.AsStringBuf());
        NYson::TYsonPullParser parser(&input, type);
        NYson::TYsonPullParserCursor cursor(&parser);
        TTo result = ConstructYTreeConvertibleObject<TTo>();

        Deserialize(result, &cursor);
        if (!cursor->IsEndOfStream()) {
            THROW_ERROR_EXCEPTION("Expected end of stream after parsing YSON, found %Qlv",
                cursor->GetType());
        }
        return result;
    }
    std::unique_ptr<NYson::IBuildingYsonConsumer<TTo>> buildingConsumer;
    CreateBuildingYsonConsumer(&buildingConsumer, type);
    Serialize(value, buildingConsumer.get());
    return buildingConsumer->Finish();
}

const NYson::TToken& SkipAttributes(NYson::TTokenizer* tokenizer);

#define IMPLEMENT_CHECKED_INTEGRAL_CONVERT_TO(type) \
    template <> \
    inline type ConvertTo(const NYson::TYsonString& str) \
    { \
        NYson::TTokenizer tokenizer(str.AsStringBuf()); \
        const auto& token = SkipAttributes(&tokenizer); \
        switch (token.GetType()) { \
            case NYson::ETokenType::Int64: \
                return CheckedIntegralCast<type>(token.GetInt64Value()); \
            case NYson::ETokenType::Uint64: \
                return CheckedIntegralCast<type>(token.GetUint64Value()); \
            default: \
                THROW_ERROR_EXCEPTION("Cannot parse \"" #type "\" from %Qlv", \
                    token.GetType()) \
                    << TErrorAttribute("data", str.AsStringBuf()); \
        } \
    }

IMPLEMENT_CHECKED_INTEGRAL_CONVERT_TO(i64)
IMPLEMENT_CHECKED_INTEGRAL_CONVERT_TO(i32)
IMPLEMENT_CHECKED_INTEGRAL_CONVERT_TO(i16)
IMPLEMENT_CHECKED_INTEGRAL_CONVERT_TO(i8)
IMPLEMENT_CHECKED_INTEGRAL_CONVERT_TO(ui64)
IMPLEMENT_CHECKED_INTEGRAL_CONVERT_TO(ui32)
IMPLEMENT_CHECKED_INTEGRAL_CONVERT_TO(ui16)
IMPLEMENT_CHECKED_INTEGRAL_CONVERT_TO(ui8)

#undef IMPLEMENT_CHECKED_INTEGRAL_CONVERT_TO

////////////////////////////////////////////////////////////////////////////////

template <class T>
T ConstructYTreeConvertibleObject()
{
    if constexpr (std::is_constructible_v<T>) {
        return T();
    } else {
        return T::Create();
    }
}

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

double ConvertYsonStringBaseToDouble(const NYson::TYsonStringBuf& yson)
{
    NYson::TTokenizer tokenizer(yson.AsStringBuf());
    const auto& token = SkipAttributes(&tokenizer);
    switch (token.GetType()) {
        case NYson::ETokenType::Int64:
            return token.GetInt64Value();
        case NYson::ETokenType::Double:
            return token.GetDoubleValue();
        case NYson::ETokenType::Boolean:
            return token.GetBooleanValue();
        default:
            THROW_ERROR_EXCEPTION("Cannot parse \"double\" from %Qlv",
                token.GetType())
                << TErrorAttribute("data", yson.AsStringBuf());
    }
}

TString ConvertYsonStringBaseToString(const NYson::TYsonStringBuf& yson)
{
    NYson::TTokenizer tokenizer(yson.AsStringBuf());
    const auto& token = SkipAttributes(&tokenizer);
    switch (token.GetType()) {
        case NYson::ETokenType::String:
            return TString(token.GetStringValue());
        default:
            THROW_ERROR_EXCEPTION("Cannot parse \"string\" from %Qlv",
                token.GetType())
                << TErrorAttribute("data", yson.AsStringBuf());
    }
}

////////////////////////////////////////////////////////////////////////////////

}

template <>
inline double ConvertTo(const NYson::TYsonString& str)
{
    return ConvertYsonStringBaseToDouble(str);
}

template <>
inline double ConvertTo(const NYson::TYsonStringBuf& str)
{
    return ConvertYsonStringBaseToDouble(str);
}

template <>
inline TString ConvertTo(const NYson::TYsonString& str)
{
    return ConvertYsonStringBaseToString(str);
}

template <>
inline TString ConvertTo(const NYson::TYsonStringBuf& str)
{
    return ConvertYsonStringBaseToString(str);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
