#include "text_yson.h"

#include "error.h"

#include <library/cpp/yt/assert/assert.h>

#include <library/cpp/yt/string/format.h>

#include <library/cpp/yt/coding/varint.h>

#include <library/cpp/yt/misc/cast.h>

#include <array>

#include <util/string/escape.h>

#include <util/stream/mem.h>

namespace NYT::NDetail {

////////////////////////////////////////////////////////////////////////////////

size_t FloatToStringWithNanInf(double value, char* buf, size_t size)
{
    if (std::isfinite(value)) {
        return FloatToString(value, buf, size);
    }

    static const TStringBuf nanLiteral = "%nan";
    static const TStringBuf infLiteral = "%inf";
    static const TStringBuf negativeInfLiteral = "%-inf";

    TStringBuf str;
    if (std::isnan(value)) {
        str = nanLiteral;
    } else if (std::isinf(value) && value > 0) {
        str = infLiteral;
    } else {
        str = negativeInfLiteral;
    }
    YT_VERIFY(str.size() + 1 <= size);
    ::memcpy(buf, str.data(), str.size() + 1);
    return str.size();
}

////////////////////////////////////////////////////////////////////////////////

// NB(arkady-e1ppa): Copied from library/cpp/yt/yson_string/format.h
// to avoid direct dependency on it.

//! Indicates an entity.
constexpr char EntitySymbol = '#';
//! Marks the beginning of a binary string literal.
constexpr char StringMarker = '\x01';
//! Marks the beginning of a binary i64 literal.
constexpr char Int64Marker = '\x02';
//! Marks the beginning of a binary double literal.
constexpr char DoubleMarker = '\x03';
//! Marks |false| boolean value.
constexpr char FalseMarker = '\x04';
//! Marks |true| boolean value.
constexpr char TrueMarker = '\x05';
//! Marks the beginning of a binary ui64 literal.
constexpr char Uint64Marker = '\x06';

////////////////////////////////////////////////////////////////////////////////

bool IsBinaryYson(TStringBuf str)
{
    return
        std::ssize(str) != 0 &&
        (str.front() == EntitySymbol ||
         str.front() == StringMarker ||
         str.front() == Int64Marker ||
         str.front() == DoubleMarker ||
         str.front() == FalseMarker ||
         str.front() == TrueMarker ||
         str.front() == Uint64Marker);
}

////////////////////////////////////////////////////////////////////////////////

template <>
std::string ConvertToTextYsonString<i8>(const i8& value)
{
    return ConvertToTextYsonString(static_cast<i64>(value));
}

template <>
std::string ConvertToTextYsonString<i32>(const i32& value)
{
    return ConvertToTextYsonString(static_cast<i64>(value));
}

template <>
std::string ConvertToTextYsonString<i64>(const i64& value)
{
    return std::string{::ToString(value)};
}

template <>
std::string ConvertToTextYsonString<ui8>(const ui8& value)
{
    return ConvertToTextYsonString(static_cast<ui64>(value));
}

template <>
std::string ConvertToTextYsonString<ui32>(const ui32& value)
{
    return ConvertToTextYsonString(static_cast<ui64>(value));
}

template <>
std::string ConvertToTextYsonString<ui64>(const ui64& value)
{
    return std::string{::ToString(value) + 'u'};
}

template <>
std::string ConvertToTextYsonString<TStringBuf>(const TStringBuf& value)
{
    return std::string(NYT::Format("\"%v\"", ::EscapeC(value)));
}

template <>
std::string ConvertToTextYsonString<float>(const float& value)
{
    return ConvertToTextYsonString(static_cast<double>(value));
}

template <>
std::string ConvertToTextYsonString<double>(const double& value)
{
    char buf[256];
    auto str = TStringBuf(buf, NDetail::FloatToStringWithNanInf(value, buf, sizeof(buf)));
    auto ret = NYT::Format(
        "%v%v",
        str,
        MakeFormatterWrapper([&] (TStringBuilderBase* builder) {
            if (str.find('.') == std::string::npos && str.find('e') == std::string::npos && std::isfinite(value)) {
                builder->AppendChar('.');
            }
        }));
    return std::string(std::move(ret));
}

template <>
std::string ConvertToTextYsonString<bool>(const bool& value)
{
    return value
        ? std::string(TStringBuf("%true"))
        : std::string(TStringBuf("%false"));
}

template <>
std::string ConvertToTextYsonString<TInstant>(const TInstant& value)
{
    return ConvertToTextYsonString(TStringBuf(value.ToString()));
}

template <>
std::string ConvertToTextYsonString<TDuration>(const TDuration& value)
{
    // ConvertTo does unchecked cast to i64 :(.
    return ConvertToTextYsonString(static_cast<i64>(value.MilliSeconds()));
}

template <>
std::string ConvertToTextYsonString<TGuid>(const TGuid& value)
{
    return ConvertToTextYsonString(TStringBuf(NYT::ToString(value)));
}

////////////////////////////////////////////////////////////////////////////////

namespace {

template <class TSomeInt>
TSomeInt ReadTextUint(TStringBuf strBuf)
{
    // Drop 'u'
    return ::FromString<TSomeInt>(TStringBuf{strBuf.data(), strBuf.length() - 1});
}

template <class TSomeInt>
TSomeInt ReadTextInt(TStringBuf strBuf)
{
    return ::FromString<TSomeInt>(TStringBuf{strBuf.data(), strBuf.length()});
}

bool IsNumeric(TStringBuf strBuf)
{
    bool isNumeric = true;
    bool isNegative = false;
    for (int i = 0; i < std::ssize(strBuf); ++i) {
        char c = strBuf[i];

        if (!('0' <= c && c <= '9')) {
            if (i == 0 && c == '-') {
                isNegative = true;
                continue;
            }
            if (i == std::ssize(strBuf) - 1 && c == 'u' && !isNegative) {
                continue;
            }
            isNumeric = false;
            break;
        }
    }

    return isNumeric;
}

////////////////////////////////////////////////////////////////////////////////

template <class TSomeInt>
TSomeInt ParseSomeIntFromTextYsonString(TStringBuf strBuf)
{
    if (std::ssize(strBuf) == 0 || !IsNumeric(strBuf)) {
        THROW_ERROR_EXCEPTION(
            "Unexpected %v\n"
            "Value is not numeric",
            strBuf);
    }

    if (strBuf.back() == 'u') {
        // Drop 'u'
        return ReadTextUint<TSomeInt>(strBuf);
    } else {
        return ReadTextInt<TSomeInt>(strBuf);
    }
}

////////////////////////////////////////////////////////////////////////////////

std::string DoParseStringFromTextYson(TStringBuf strBuf)
{
    // Remove quotation marks.
    return ::UnescapeC(TStringBuf{strBuf.data() + 1, strBuf.length() - 2});
}

std::string ParseStringFromTextYsonString(TStringBuf strBuf)
{
    if (std::ssize(strBuf) < 2 || strBuf.front() != '\"' || strBuf.back() != '\"') {
        THROW_ERROR_EXCEPTION(
            "Unexpected %v\n"
            "Text yson string must begin and end with \\\"",
            strBuf);
    }
    return DoParseStringFromTextYson(strBuf);
}

////////////////////////////////////////////////////////////////////////////////

double ParseDoubleFromTextYsonString(TStringBuf strBuf)
{
    if (std::ssize(strBuf) < 2) {
        THROW_ERROR_EXCEPTION(
            "Incorrect remaining string length: expected at least 2, got %v",
            std::ssize(strBuf));
    }

    // Check special values first.
    // %nan
    // %inf, %+inf, %-inf
    if (strBuf[0] == '%') {
        switch (strBuf[1]) {
            case '+':
            case 'i':
                return std::numeric_limits<double>::infinity();

            case '-':
                return -std::numeric_limits<double>::infinity();

            case 'n':
                return std::numeric_limits<double>::quiet_NaN();

            default:
                THROW_ERROR_EXCEPTION(
                    "Incorrect %%-literal %v",
                    strBuf);
        }
    }

    return ::FromString<double>(strBuf);
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

#define PARSE_INT(type, underlyingType) \
    template <> \
    type ConvertFromTextYsonString<type>(TStringBuf str) \
    { \
        try { \
            return CheckedIntegralCast<type>(ParseSomeIntFromTextYsonString<underlyingType>(str)); \
        } catch (const std::exception& ex) { \
            THROW_ERROR_EXCEPTION("Error parsing \"" #type "\" value from YSON") << ex; \
        } \
    }

PARSE_INT(i8,    i64)
PARSE_INT(i16,   i64)
PARSE_INT(i32,   i64)
PARSE_INT(i64,   i64)
PARSE_INT(ui8,  ui64)
PARSE_INT(ui16, ui64)
PARSE_INT(ui32, ui64)
PARSE_INT(ui64, ui64)

#undef PARSE

////////////////////////////////////////////////////////////////////////////////

template <>
std::string ConvertFromTextYsonString<std::string>(TStringBuf str)
{
    try {
        return ParseStringFromTextYsonString(str);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error parsing \"string\" value from YSON") << ex;
    }
}

template <>
TString ConvertFromTextYsonString<TString>(TStringBuf str)
{
    return TString(ConvertFromTextYsonString<std::string>(str));
}

template <>
float ConvertFromTextYsonString<float>(TStringBuf str)
{
    try {
        return static_cast<float>(ParseDoubleFromTextYsonString(str));
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error parsing \"float\" value from YSON") << ex;
    }
}

template <>
double ConvertFromTextYsonString<double>(TStringBuf str)
{
    try {
        return ParseDoubleFromTextYsonString(str);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error parsing \"double\" value from YSON") << ex;
    }
}

template <>
bool ConvertFromTextYsonString<bool>(TStringBuf strBuf)
{
    try {
        if (std::ssize(strBuf) == 0) {
            THROW_ERROR_EXCEPTION("Empty string");
        }

        char ch = strBuf.front();

        if (ch == '%') {
            if (strBuf != "%true" && strBuf != "%false") {
                THROW_ERROR_EXCEPTION(
                    "Expected %%true or %%false but found %v",
                    strBuf);
            }
            return strBuf == "%true";
        }

        if (ch == '\"') {
            return ParseBool(DoParseStringFromTextYson(strBuf));
        }

        // NB(arkady-e1ppa): This check is linear in size(strBuf)
        // And thus is tried as the last resort.
        if (IsNumeric(strBuf)) {
            auto checkValue = [&] (const auto& functor) {
                auto value = functor(strBuf);
                if (value != 0 && value != 1) {
                    THROW_ERROR_EXCEPTION(
                        "Expected 0 or 1 but found %v",
                        value);
                }
                return static_cast<bool>(value);
            };

            if (strBuf.back() == 'u') {
                return checkValue(&ReadTextUint<ui64>);
            } else {
                return checkValue(&ReadTextInt<i64>);
            }
        }

        THROW_ERROR_EXCEPTION(
            "Unexpected %v\n"
            "No known conversion to \"boolean\" value",
            strBuf);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error parsing \"boolean\" value from YSON") << ex;
    }
}

template <>
TInstant ConvertFromTextYsonString<TInstant>(TStringBuf str)
{
    try {
        return TInstant::ParseIso8601(ParseStringFromTextYsonString(str));
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error parsing \"instant\" value from YSON") << ex;
    }
}

template <>
TDuration ConvertFromTextYsonString<TDuration>(TStringBuf str)
{
    try {
        return TDuration::MilliSeconds(ParseSomeIntFromTextYsonString<i64>(str));
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error parsing \"duration\" value from YSON") << ex;
    }
}

template <>
TGuid ConvertFromTextYsonString<TGuid>(TStringBuf str)
{
    try {
        return TGuid::FromString(ParseStringFromTextYsonString(str));
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error parsing \"guid\" value from YSON") << ex;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDetail
