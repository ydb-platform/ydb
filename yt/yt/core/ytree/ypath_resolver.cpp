#include "ypath_resolver.h"

#include <yt/yt/core/ypath/tokenizer.h>

#include <yt/yt/core/yson/writer.h>

#include <yt/yt/core/misc/error.h>

#include <library/cpp/yt/misc/cast.h>

#include <util/stream/mem.h>

namespace NYT::NYTree {

using NYPath::ETokenType;
using NYPath::TTokenizer;

using NYson::EYsonType;
using NYson::EYsonItemType;
using NYson::TYsonPullParser;
using NYson::TYsonPullParserCursor;

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EExpectedItem,
    (BeginMapOrList)
    (BeginAttribute)
    (Value)
);

using TResult = std::variant<bool, i64, ui64, double, TString>;

std::optional<TResult> TryParseValue(TYsonPullParserCursor* cursor)
{
    switch ((*cursor)->GetType()) {
        case EYsonItemType::BooleanValue:
            return (*cursor)->UncheckedAsBoolean();
        case EYsonItemType::Int64Value:
            return (*cursor)->UncheckedAsInt64();
        case EYsonItemType::Uint64Value:
            return (*cursor)->UncheckedAsUint64();
        case EYsonItemType::DoubleValue:
            return (*cursor)->UncheckedAsDouble();
        case EYsonItemType::StringValue:
            return TString((*cursor)->UncheckedAsString());
        default:
            return std::nullopt;
    }
}

TResult ParseValue(TYsonPullParserCursor* cursor)
{
    auto result = TryParseValue(cursor);
    YT_VERIFY(result);
    return std::move(*result);
}

[[noreturn]] void ThrowUnexpectedToken(const TTokenizer& tokenizer)
{
    THROW_ERROR_EXCEPTION(
        "Unexpected YPath token %Qv while parsing %Qv",
        tokenizer.GetToken(),
        tokenizer.GetInput());
}

std::pair<EExpectedItem, std::optional<TString>> NextToken(TTokenizer* tokenizer)
{
    switch (tokenizer->Advance()) {
        case ETokenType::EndOfStream:
            return {EExpectedItem::Value, std::nullopt};

        case ETokenType::Slash: {
            auto type = tokenizer->Advance();
            auto expected = EExpectedItem::BeginMapOrList;
            if (type == ETokenType::At) {
                type = tokenizer->Advance();
                expected = EExpectedItem::BeginAttribute;
            }
            if (type != ETokenType::Literal) {
                ThrowUnexpectedToken(*tokenizer);
            }
            return {expected, tokenizer->GetLiteralValue()};
        }

        default:
            ThrowUnexpectedToken(*tokenizer);
    }
}

std::optional<TResult> TryParseImpl(TStringBuf yson, const TYPath& path, bool isAny)
{
    TTokenizer tokenizer(path);
    TMemoryInput input(yson);
    TYsonPullParser parser(&input, EYsonType::Node);
    TYsonPullParserCursor cursor(&parser);

    while (true) {
        auto [expected, literal] = NextToken(&tokenizer);
        if (expected == EExpectedItem::Value && isAny) {
            return {ParseAnyValue(&cursor)};
        }
        if (cursor->GetType() == EYsonItemType::BeginAttributes && expected != EExpectedItem::BeginAttribute) {
            cursor.SkipAttributes();
        }
        switch (cursor->GetType()) {
            case EYsonItemType::BeginAttributes: {
                if (expected != EExpectedItem::BeginAttribute) {
                    return std::nullopt;
                }
                YT_VERIFY(literal.has_value());
                if (!ParseMapOrAttributesUntilKey(&cursor, *literal)) {
                    return std::nullopt;
                }
                break;
            }
            case EYsonItemType::BeginMap: {
                if (expected != EExpectedItem::BeginMapOrList) {
                    return std::nullopt;
                }
                YT_VERIFY(literal.has_value());
                if (!ParseMapOrAttributesUntilKey(&cursor, *literal)) {
                    return std::nullopt;
                }
                break;
            }
            case EYsonItemType::BeginList: {
                if (expected != EExpectedItem::BeginMapOrList) {
                    return std::nullopt;
                }
                YT_VERIFY(literal.has_value());
                int index;
                if (!TryFromString(*literal, index)) {
                    return std::nullopt;
                }
                if (!ParseListUntilIndex(&cursor, index)) {
                    return std::nullopt;
                }
                break;
            }
            case EYsonItemType::EntityValue:
                return std::nullopt;

            case EYsonItemType::BooleanValue:
            case EYsonItemType::Int64Value:
            case EYsonItemType::Uint64Value:
            case EYsonItemType::DoubleValue:
            case EYsonItemType::StringValue:
                if (expected != EExpectedItem::Value) {
                    return std::nullopt;
                }
                return ParseValue(&cursor);
            case EYsonItemType::EndOfStream:
            case EYsonItemType::EndMap:
            case EYsonItemType::EndAttributes:
            case EYsonItemType::EndList:
                YT_ABORT();
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

template <class T>
struct TScalarTypeTraits
{ };

template <>
struct TScalarTypeTraits<TString>
{
    static std::optional<TString> TryCast(const NDetail::TResult& result)
    {
        if (const auto* value = std::get_if<TString>(&result)) {
            return *value;
        }
        return std::nullopt;
    }
};

template <>
struct TScalarTypeTraits<bool>
{
    static std::optional<bool> TryCast(const NDetail::TResult& result)
    {
        if (const auto* value = std::get_if<bool>(&result)) {
            return *value;
        }
        return std::nullopt;
    }
};

template <>
struct TScalarTypeTraits<i64>
{
    static std::optional<i64> TryCast(const NDetail::TResult& result)
    {
        if (const auto* value = std::get_if<i64>(&result)) {
            return *value;
        } else if (const auto* value = std::get_if<ui64>(&result)) {
            i64 typedResult;
            if (TryIntegralCast(*value, &typedResult)) {
                return typedResult;
            }
        }
        return std::nullopt;
    }
};

template <>
struct TScalarTypeTraits<ui64>
{
    static std::optional<ui64> TryCast(const NDetail::TResult& result)
    {
        if (const auto* value = std::get_if<ui64>(&result)) {
            return *value;
        } else if (const auto* value = std::get_if<i64>(&result)) {
            ui64 typedResult;
            if (TryIntegralCast(*value, &typedResult)) {
                return typedResult;
            }
        }
        return std::nullopt;
    }
};

template <>
struct TScalarTypeTraits<double>
{
    static std::optional<double> TryCast(const NDetail::TResult& result)
    {
        if (const auto* value = std::get_if<double>(&result)) {
            return *value;
        } else if (const auto* value = std::get_if<i64>(&result)) {
            return static_cast<double>(*value);
        } else if (const auto* value = std::get_if<ui64>(&result)) {
            return static_cast<double>(*value);
        }
        return std::nullopt;
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename T>
std::optional<T> TryGetValueImpl(TStringBuf yson, const TYPath& ypath, bool isAny = false)
{
    auto result = NDetail::TryParseImpl(yson, ypath, isAny);
    if (!result.has_value()) {
        return std::nullopt;
    }
    return TScalarTypeTraits<T>::TryCast(*result);
}

////////////////////////////////////////////////////////////////////////////////

template <typename T>
std::optional<T> TryGetValue(TStringBuf yson, const TYPath& ypath)
{
    return TryGetValueImpl<T>(yson, ypath, /*isAny*/ false);
}

template std::optional<i64> TryGetValue<i64>(TStringBuf yson, const TYPath& ypath);
template std::optional<ui64> TryGetValue<ui64>(TStringBuf yson, const TYPath& ypath);
template std::optional<bool> TryGetValue<bool>(TStringBuf yson, const TYPath& ypath);
template std::optional<double> TryGetValue<double>(TStringBuf yson, const TYPath& ypath);
template std::optional<TString> TryGetValue<TString>(TStringBuf yson, const TYPath& ypath);

////////////////////////////////////////////////////////////////////////////////

std::optional<i64> TryGetInt64(TStringBuf yson, const TYPath& ypath)
{
    return TryGetValueImpl<i64>(yson, ypath);
}

std::optional<ui64> TryGetUint64(TStringBuf yson, const TYPath& ypath)
{
    return TryGetValueImpl<ui64>(yson, ypath);
}

std::optional<bool> TryGetBoolean(TStringBuf yson, const TYPath& ypath)
{
    return TryGetValueImpl<bool>(yson, ypath);
}

std::optional<double> TryGetDouble(TStringBuf yson, const TYPath& ypath)
{
    return TryGetValueImpl<double>(yson, ypath);
}

std::optional<TString> TryGetString(TStringBuf yson, const TYPath& ypath)
{
    return TryGetValueImpl<TString>(yson, ypath);
}

std::optional<TString> TryGetAny(TStringBuf yson, const TYPath& ypath)
{
    return TryGetValueImpl<TString>(yson, ypath, /*isAny*/ true);
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
std::optional<T> TryParseValue(TYsonPullParserCursor* cursor)
{
    auto result = NDetail::TryParseValue(cursor);
    return result ? TScalarTypeTraits<T>::TryCast(std::move(*result)) : std::nullopt;
}

template std::optional<i64> TryParseValue<i64>(TYsonPullParserCursor* cursor);
template std::optional<ui64> TryParseValue<ui64>(TYsonPullParserCursor* cursor);
template std::optional<bool> TryParseValue<bool>(TYsonPullParserCursor* cursor);
template std::optional<double> TryParseValue<double>(TYsonPullParserCursor* cursor);
template std::optional<TString> TryParseValue<TString>(TYsonPullParserCursor* cursor);

////////////////////////////////////////////////////////////////////////////////

bool ParseListUntilIndex(TYsonPullParserCursor* cursor, int targetIndex)
{
    YT_VERIFY((*cursor)->GetType() == EYsonItemType::BeginList);
    cursor->Next();
    int index = 0;
    while ((*cursor)->GetType() != EYsonItemType::EndList) {
        if (index == targetIndex) {
            return true;
        }
        ++index;
        cursor->SkipComplexValue();
    }
    return false;
}

bool ParseMapOrAttributesUntilKey(TYsonPullParserCursor* cursor, TStringBuf key)
{
    auto endType = EYsonItemType::EndMap;
    if ((*cursor)->GetType() != EYsonItemType::BeginMap) {
        YT_VERIFY((*cursor)->GetType() == EYsonItemType::BeginAttributes);
        endType = EYsonItemType::EndAttributes;
    }
    cursor->Next();
    while ((*cursor)->GetType() != endType) {
        YT_VERIFY((*cursor)->GetType() == EYsonItemType::StringValue);
        if ((*cursor)->UncheckedAsString() == key) {
            cursor->Next();
            return true;
        }
        cursor->Next();
        cursor->SkipComplexValue();
    }
    return false;
}

TString ParseAnyValue(TYsonPullParserCursor* cursor)
{
    TStringStream stream;
    {
        NYson::TCheckedInDebugYsonTokenWriter writer(&stream);
        cursor->TransferComplexValue(&writer);
        writer.Flush();
    }
    return std::move(stream.Str());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
