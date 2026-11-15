#include "expression.h"
#include "document.h"

#include <contrib/libs/double-conversion/double-conversion/string-to-double.h>
#include <util/charset/utf8.h>
#include <library/cpp/string_utils/url/url.h>

#include <algorithm>
#include <bit>
#include <charconv>
#include <chrono>
#include <cmath>
#include <limits>
#include <locale>
#include <numeric>
#include <optional>
#include <sstream>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>

namespace NKikimr::NKqp::NFrontend {
namespace {

using K = ETypeKind;
using O = EOpcode;

constexpr std::string_view TypeNames[]{"Null", "Bool", "Int8", "Int16", "Int32", "Int64", "Uint8", "Uint16", "Uint32", "Uint64",
    "Float", "Double", "String", "Utf8", "Decimal", "Date", "Datetime", "Timestamp", "Interval",
    "Date32", "Datetime64", "Timestamp64", "Interval64", "Uuid", "Json", "JsonDocument", "Yson", "Calendar", "Tuple", "List", "Struct"};
static_assert(std::size(TypeNames) == static_cast<unsigned>(K::Struct) + 1);

bool EqualAsciiCase(std::string_view left, std::string_view right) {
    if (left.size() != right.size()) return false;
    for (std::size_t i = 0; i < left.size(); ++i) {
        const auto lower = [](char ch) { return ch >= 'A' && ch <= 'Z' ? char(ch + ('a' - 'A')) : ch; };
        if (lower(left[i]) != lower(right[i])) return false;
    }
    return true;
}

[[noreturn]] void Fail(TSourcePosition position, std::string message) {
    throw TFrontendError(position, std::move(message));
}

template<class TValue>
std::vector<std::size_t> StructOrder(std::span<const std::pair<std::string, TValue>> fields, TSourcePosition position) {
    std::vector<std::size_t> order(fields.size());
    std::iota(order.begin(), order.end(), std::size_t{0});
    std::sort(order.begin(), order.end(), [&](auto a, auto b) { return fields[a].first < fields[b].first; });
    for (std::size_t i = 1; i < order.size(); ++i) if (fields[order[i - 1]].first == fields[order[i]].first)
        Fail(position, "Duplicate struct field: " + fields[order[i]].first);
    return order;
}

std::string Lower(std::string_view text) {
    std::string result(text);
    for (auto& ch : result) {
        if (ch >= 'A' && ch <= 'Z') {
            ch += 'a' - 'A';
        }
    }
    return result;
}

bool IsUtf8(std::string_view text) {
    for (std::size_t i = 0; i < text.size();) {
        const auto first = static_cast<unsigned char>(text[i++]);
        if (first < 0x80) continue;
        unsigned remaining;
        std::uint32_t codepoint, minimum;
        if (first >= 0xC2 && first <= 0xDF) {
            remaining = 1; codepoint = first & 0x1F; minimum = 0x80;
        } else if (first >= 0xE0 && first <= 0xEF) {
            remaining = 2; codepoint = first & 0x0F; minimum = 0x800;
        } else if (first >= 0xF0 && first <= 0xF4) {
            remaining = 3; codepoint = first & 0x07; minimum = 0x10000;
        } else return false;
        if (remaining > text.size() - i) return false;
        while (remaining--) {
            const auto next = static_cast<unsigned char>(text[i++]);
            if ((next & 0xC0) != 0x80) return false;
            codepoint = (codepoint << 6) | (next & 0x3F);
        }
        if (codepoint < minimum || codepoint > 0x10FFFF || (codepoint >= 0xD800 && codepoint <= 0xDFFF)) return false;
    }
    return true;
}

int Width(K kind) {
    switch (kind) {
        case K::Int8: case K::Uint8: return 8;
        case K::Int16: case K::Uint16: return 16;
        case K::Int32: case K::Uint32: return 32;
        case K::Int64: case K::Uint64: return 64;
        default: return 0;
    }
}

// A complete selector path has no JSONPath type-checking phase: each step
// selects members or array elements. More general JSONPath expressions remain
// explicit Unsupported until their compile-time diagnostics are implemented.
void ValidateJsonSelector(std::string_view path, TSourcePosition position) {
    std::size_t at = 0;
    const auto whitespace = [](char c) { return c == ' ' || c == '\t' || c == '\r' || c == '\n' || c == '\f'; };
    const auto space = [&] {
        while (at < path.size() && whitespace(path[at])) ++at;
    };
    const auto invalid = [&] { Fail(position, "Invalid JSON selector path"); };
    const auto unsupported = [&] {
        throw TUnsupportedFeature(EFeature::Expression, position,
            "native JSON paths support root, members, wildcards and nonnegative Uint32 array indexes");
    };
    const auto start = [](char c) { return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_'; };
    space();
    for (const auto mode : {std::string_view{"lax"}, std::string_view{"strict"}})
        if (path.substr(at).starts_with(mode) && (at + mode.size() == path.size() || whitespace(path[at + mode.size()]))) {
            at += mode.size(); space(); break;
        }
    if (at == path.size()) invalid();
    if (path[at++] != '$') unsupported();
    for (;;) {
        space();
        if (at == path.size()) return;
        const auto op = path[at++];
        if (op == '.') {
            space();
            if (at == path.size()) invalid();
            if (path[at] == '*') { ++at; continue; }
            if (path[at] == '\'' || path[at] == '"') {
                const char quote = path[at++];
                while (at < path.size() && path[at] != quote) {
                    if (path[at] == '\\' || static_cast<unsigned char>(path[at]) < 32) unsupported();
                    ++at;
                }
                if (at == path.size()) invalid();
                ++at;
            } else {
                if (!start(path[at])) invalid();
                do { ++at; } while (at < path.size() && (start(path[at]) || (path[at] >= '0' && path[at] <= '9') || path[at] == '$'));
            }
        } else if (op == '[') {
            space();
            if (at == path.size() || path[at] == ']') invalid();
            if (path[at] == '*') ++at;
            else {
                if (path[at] < '0' || path[at] > '9') unsupported();
                std::uint32_t index;
                const auto parsed = std::from_chars(path.data() + at, path.data() + path.size(), index);
                if (parsed.ec != std::errc{}) unsupported();
                at = parsed.ptr - path.data();
            }
            space();
            if (at == path.size()) invalid();
            if (path[at++] != ']') unsupported();
        } else {
            if (op == ']' || op == ')') invalid();
            unsupported();
        }
    }
}

K IntegerKind(int width, bool isSigned) {
    if (isSigned) {
        return width <= 8 ? K::Int8 : width <= 16 ? K::Int16 : width <= 32 ? K::Int32 : K::Int64;
    }
    return width <= 8 ? K::Uint8 : width <= 16 ? K::Uint16 : width <= 32 ? K::Uint32 : K::Uint64;
}

bool IsDate(K kind) {
    return TType{kind}.IsDate();
}

constexpr std::int64_t MicrosPerDay = 86'400'000'000LL;
constexpr std::int64_t TimestampLimit = 49'673LL * MicrosPerDay;
constexpr std::int64_t MinTimestamp64 = -4'611'669'897'600'000'000LL;
constexpr std::int64_t MaxTimestamp64 = 4'611'669'811'199'999'999LL;
constexpr std::int64_t MaxInterval64 = MaxTimestamp64 - MinTimestamp64;

bool IsInterval(K kind) {
    return TType{kind}.IsInterval();
}

bool IsSignedPayload(TType type) {
    return type.IsSigned() || type.IsInterval() || type.IsExtendedTemporal();
}

int PayloadWidth(K kind) {
    if (kind == K::Date) return 16;
    if (kind == K::Datetime || kind == K::Date32) return 32;
    if (IsDate(kind) || IsInterval(kind)) return 64;
    return Width(kind);
}

std::int64_t DateUnit(K kind) {
    return kind == K::Date || kind == K::Date32 ? MicrosPerDay :
        kind == K::Datetime || kind == K::Datetime64 ? 1'000'000 : 1;
}

std::pair<std::int64_t, std::int64_t> TemporalBounds(K kind) {
    if (kind == K::Interval) return {-TimestampLimit + 1, TimestampLimit - 1};
    if (kind == K::Interval64) return {-MaxInterval64, MaxInterval64};
    if (TType{kind}.IsExtendedTemporal()) return {MinTimestamp64 / DateUnit(kind), MaxTimestamp64 / DateUnit(kind)};
    return {0, TimestampLimit / DateUnit(kind) - 1};
}

bool FitsTemporal(__int128 value, K kind) {
    const auto [minimum, maximum] = TemporalBounds(kind);
    return value >= minimum && value <= maximum;
}

std::chrono::year_month_day CalendarDate(std::uint64_t micros) {
    return std::chrono::year_month_day{std::chrono::sys_days{std::chrono::days{micros / MicrosPerDay}}};
}

std::optional<std::uint64_t> ShiftCalendar(std::uint64_t micros, std::int64_t amount, bool years) {
    using namespace std::chrono;
    const auto date = CalendarDate(micros);
    std::int64_t y = int(date.year()), m = unsigned(date.month());
    // Preserve the existing narrow calendar's 12-bit year field, including
    // wraparound for large shifts, then apply its normal date-range check.
    if (years) y = (y + amount) & 4095;
    else {
        const auto next = m + amount;
        y = (y + (next - 1) / 12) & 4095;
        m = 1 + (next - 1) % 12;
        if (m <= 0) { y = (y - 1) & 4095; m += 12; }
    }
    if (y < 1970 || y > 2105) return {};
    const auto ym = year{int(y)} / month{unsigned(m)};
    const auto d = std::min(unsigned(date.day()), unsigned(year_month_day_last{ym / last}.day()));
    const auto days = sys_days{ym / day{d}}.time_since_epoch().count();
    const auto result = days * MicrosPerDay + micros % MicrosPerDay;
    return result < TimestampLimit ? std::optional<std::uint64_t>{result} : std::nullopt;
}

std::optional<std::int64_t> ParseTemporal(std::string_view text, K kind) {
    if (IsInterval(kind)) {
        bool negative = false;
        if (text.starts_with('-')) {
            negative = true;
            text.remove_prefix(1);
        }
        if (!text.starts_with('P')) return {};
        text.remove_prefix(1);
        bool time = false, any = false;
        __int128 micros = 0;
        unsigned previous = 0;
        while (!text.empty()) {
            if (text.front() == 'T' && !time) {
                time = true;
                text.remove_prefix(1);
                if (text.empty()) return {};
                continue;
            }
            std::uint64_t whole = 0;
            const auto number = std::from_chars(text.data(), text.data() + text.size(), whole);
            if (number.ec != std::errc{} || number.ptr == text.data()) return {};
            text.remove_prefix(number.ptr - text.data());
            std::uint64_t fraction = 0;
            unsigned digits = 0;
            if (text.starts_with('.')) {
                text.remove_prefix(1);
                while (!text.empty() && text.front() >= '0' && text.front() <= '9') {
                    if (++digits > 6) return {};
                    fraction = fraction * 10 + (text.front() - '0');
                    text.remove_prefix(1);
                }
                if (!digits) return {};
                while (digits++ < 6) fraction *= 10;
            }
            if (text.empty()) return {};
            const char unit = text.front();
            text.remove_prefix(1);
            const unsigned order = !time && unit == 'D' ? 1 : time && unit == 'H' ? 2 : time && unit == 'M' ? 3 : time && unit == 'S' ? 4 : 0;
            if (!order || order <= previous || (digits && unit != 'S')) return {};
            previous = order;
            const std::int64_t scale = unit == 'D' ? MicrosPerDay : unit == 'H' ? 3'600'000'000LL : unit == 'M' ? 60'000'000LL : 1'000'000LL;
            micros += static_cast<__int128>(whole) * scale + fraction;
            if (!FitsTemporal(micros, kind)) return {};
            any = true;
        }
        if (!any) return {};
        return static_cast<std::int64_t>(negative ? -micros : micros);
    }
    // Match the scalar Date/Datetime/Timestamp literal contract: bounded
    // numeric components, a mandatory timezone for time values, and truncation
    // of timestamp fractional digits beyond microseconds.
    std::size_t offset = 0;
    auto component = [&](unsigned maxDigits) -> int {
        unsigned digits = 0;
        int value = 0;
        while (digits < maxDigits && offset < text.size() && text[offset] >= '0' && text[offset] <= '9') {
            value = value * 10 + text[offset++] - '0';
            ++digits;
        }
        return digits ? value : -1;
    };
    auto consume = [&](char ch) {
        if (offset == text.size() || text[offset] != ch) return false;
        ++offset;
        return true;
    };
    const bool extended = TType{kind}.IsExtendedTemporal();
    const bool negativeYear = extended && consume('-');
    if (extended && !negativeYear) consume('+');
    int year = component(extended ? 6 : 4);
    if (year < 0) return {};
    if (negativeYear) year = -year;
    if (extended && (year == 0 || year < -144169 || year >= 148108)) return {};
    // Civil years have no year zero; Gregorian arithmetic uses astronomical years.
    if (year < 0) ++year;
    if (!consume('-')) return {};
    const int month = component(2);
    if (!consume('-')) return {};
    const int day = component(2);
    if ((!extended && year < 1970) || month < 1 || month > 12 || day < 1) return {};
    constexpr int MonthDays[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    const bool leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    if (day > MonthDays[month - 1] + (month == 2 && leap)) return {};
    // Gregorian calendar to days since 1970-01-01, including civil years before 1 CE.
    const int y = year - (month <= 2), era = (y >= 0 ? y : y - 399) / 400, yearOfEra = y - era * 400;
    const int dayOfYear = (153 * (month + (month > 2 ? -3 : 9)) + 2) / 5 + day - 1;
    const std::int64_t days = era * 146097 + yearOfEra * 365 + yearOfEra / 4 - yearOfEra / 100 + dayOfYear - 719468;
    // The local date itself must be valid, even if applying a timezone offset
    // would otherwise move an out-of-range local date into the UTC range.
    if (!FitsTemporal(days, extended ? K::Date32 : K::Date)) return {};
    std::int64_t micros = days * MicrosPerDay;
    if (kind == K::Date || kind == K::Date32) {
        return offset == text.size() ? std::optional<std::int64_t>{days} : std::nullopt;
    }
    if (!consume('T')) return {};
    const int hour = component(2);
    if (!consume(':')) return {};
    const int minute = component(2);
    if (!consume(':')) return {};
    const int second = component(2);
    if (hour < 0 || hour > 23 || minute < 0 || minute > 59 || second < 0 || second > 59) return {};
    micros += (hour * 3600LL + minute * 60 + second) * 1'000'000;
    if (consume('.')) {
        if (kind != K::Timestamp && kind != K::Timestamp64) return {};
        unsigned digits = 0;
        std::int64_t fraction = 0;
        while (offset < text.size() && text[offset] >= '0' && text[offset] <= '9') {
            if (digits < 6) {
                fraction = fraction * 10 + text[offset] - '0';
                ++digits;
            }
            ++offset;
        }
        if (!digits) return {};
        while (digits++ < 6) fraction *= 10;
        micros += fraction;
    }
    if (!consume('Z')) {
        if (offset == text.size() || (text[offset] != '+' && text[offset] != '-')) return {};
        const bool negative = text[offset++] == '-';
        const int hours = component(2);
        if (!consume(':')) return {};
        const int minutes = component(2);
        if (hours < 0 || minutes < 0) return {};
        // The established grammar bounds each offset component to two digits;
        // it does not restrict them to a civil-time hour/minute range.
        const std::int64_t timezone = (hours * 60LL + minutes) * 60 * 1'000'000;
        micros += negative ? timezone : -timezone;
    }
    if (offset != text.size() || !FitsTemporal(micros, extended ? K::Timestamp64 : K::Timestamp)) return {};
    return micros / DateUnit(kind);
}

void ValidateScalarType(TType type, TSourcePosition position) {
    if (!type.IsCollection() && type.Shape) Fail(position, "Scalar type cannot have a tuple shape");
    if (type.Kind == K::Decimal) {
        if (!type.Precision || type.Precision > 35 || type.Scale > type.Precision) {
            Fail(position, "Decimal requires 1 <= precision <= 35 and scale <= precision");
        }
    } else if (type.Precision || type.Scale) {
        Fail(position, "Precision and scale are only valid for Decimal");
    }
}

__int128 Power10(unsigned power) {
    __int128 result = 1;
    for (unsigned i = 0; i < power; ++i) {
        result *= 10;
    }
    return result;
}

TType ComputeCommonType(TType a, TType b, TSourcePosition position) {
    if (a.Kind == K::Null) {
        return b.Optional();
    }
    if (b.Kind == K::Null) {
        return a.Optional();
    }
    const auto nullable = std::max(a.OptionalDepth, b.OptionalDepth);
    a.OptionalDepth = b.OptionalDepth = nullable;
    if (a == b) {
        return a;
    }
    if (a.IsString() && b.IsString()) {
        return {K::String, nullable};
    }
    if (a.IsDate() && b.IsDate()) {
        const auto unit = std::min(DateUnit(a.Kind), DateUnit(b.Kind));
        if (a.IsExtendedTemporal() || b.IsExtendedTemporal())
            return {unit == MicrosPerDay ? K::Date32 : unit == 1'000'000 ? K::Datetime64 : K::Timestamp64, nullable};
        return {unit == MicrosPerDay ? K::Date : unit == 1'000'000 ? K::Datetime : K::Timestamp, nullable};
    }
    if (a.IsInterval() && b.IsInterval()) return {K::Interval64, nullable};
    if (!a.IsNumeric() || !b.IsNumeric()) {
        Fail(position, "No common type for " + a.ToString() + " and " + b.ToString());
    }
    if (a.Kind == K::Decimal || b.Kind == K::Decimal) {
        if (a.Kind == K::Float || a.Kind == K::Double || b.Kind == K::Float || b.Kind == K::Double) {
            Fail(position, "Decimal and floating point require an explicit cast");
        }
        const unsigned scale = std::max(a.Scale, b.Scale);
        auto integralDigits = [](TType type) -> unsigned {
            if (type.Kind == K::Decimal) {
                return type.Precision - type.Scale;
            }
            // SQL reserves the same decimal width for signed/unsigned integers.
            return (Width(type.Kind) * 30103U) / 100000U + 1;
        };
        const unsigned precision = std::max(integralDigits(a), integralDigits(b)) + scale;
        if (precision > 35) {
            Fail(position, "Decimal common type exceeds precision 35");
        }
        return {K::Decimal, nullable, static_cast<std::uint8_t>(precision), static_cast<std::uint8_t>(scale)};
    }
    if (a.Kind == K::Double || b.Kind == K::Double || a.Kind == K::Float || b.Kind == K::Float) {
        return {a.Kind == K::Double || b.Kind == K::Double ? K::Double : K::Float, nullable};
    }
    // SQL's numeric rank prefers width, then signedness at equal width.
    // This is an implicit bit conversion, not a lossless integer supertype.
    if (Width(a.Kind) != Width(b.Kind)) return Width(a.Kind) > Width(b.Kind) ? a : b;
    return {IntegerKind(Width(a.Kind), a.IsSigned() || b.IsSigned()), nullable};
}

bool IsComparison(O op) {
    return op == O::Equal || op == O::NotEqual || op == O::Less || op == O::LessEqual || op == O::Greater || op == O::GreaterEqual;
}

TValue NullValue(TType type) {
    return {type.Optional(), std::monostate{}};
}

bool FitsSigned(__int128 value, int width) {
    return value >= -(__int128{1} << (width - 1)) && value < (__int128{1} << (width - 1));
}

bool FitsUnsigned(__int128 value, int width) {
    return value >= 0 && value < (__int128{1} << width);
}

TScalar WrapInteger(unsigned __int128 value, TType type) {
    const int width = PayloadWidth(type.Kind);
    const auto mask = (static_cast<unsigned __int128>(1) << width) - 1;
    std::uint64_t bits = static_cast<std::uint64_t>(value & mask);
    if (IsSignedPayload(type)) {
        if (width < 64 && (bits & (std::uint64_t{1} << (width - 1)))) {
            bits |= ~static_cast<std::uint64_t>(mask);
        }
        return std::bit_cast<std::int64_t>(bits);
    }
    return bits;
}

bool FitsDecimal(__int128 value, TType type) {
    const auto limit = Power10(type.Precision);
    return value > -limit && value < limit;
}

bool IsDecimalSpecial(__int128 value) {
    return value == TDecimal::Inf() || value == -TDecimal::Inf() || value == TDecimal::NaN();
}

__int128 BoundDecimal(__int128 value, TType type) {
    if (FitsDecimal(value, type)) return value;
    return value < 0 ? -TDecimal::Inf() : TDecimal::Inf();
}

bool IsCanonicalDecimalText(std::string_view text) {
    if (!text.empty() && (text.front() == '-' || text.front() == '+')) text.remove_prefix(1);
    if (const auto lower = Lower(text); lower == "inf" || lower == "nan") return true;
    bool digit = false, dot = false;
    for (const char c : text) {
        if (c == '.' && !dot) dot = true;
        else if (c >= '0' && c <= '9') digit = true;
        else return false;
    }
    return digit;
}

template <class T>
T DecimalFloat(__int128 value, unsigned scale) {
    if (value == TDecimal::NaN()) return std::numeric_limits<T>::quiet_NaN();
    if (value == TDecimal::Inf()) return std::numeric_limits<T>::infinity();
    if (value == -TDecimal::Inf()) return -std::numeric_limits<T>::infinity();
    while (scale && value % 10 == 0) { value /= 10; --scale; }
    return static_cast<T>(value) / static_cast<T>(Power10(scale));
}

std::string Int128ToString(__int128 value) {
    const bool negative = value < 0;
    // Decimal values have at most 35 digits, so negation cannot overflow.
    if (negative) {
        value = -value;
    }
    std::string text;
    do {
        text.push_back('0' + value % 10);
        value /= 10;
    } while (value);
    if (negative) {
        text.push_back('-');
    }
    std::reverse(text.begin(), text.end());
    return text;
}

// Decimal coefficients have at most 35 digits. A product can have 70, but
// long division needs only a 36-digit remainder and therefore fits __int128.
// Keeping this tiny reference executor in decimal digits avoids a bigint
// dependency and reproduces the runtime's nearest-even scale reduction.
std::optional<__int128> RoundedDecimalQuotient(std::string_view digits, __int128 divisor, bool negative) {
    __int128 quotient = 0, remainder = 0;
    for (char digit : digits) {
        remainder = remainder * 10 + digit - '0';
        const auto next = remainder / divisor;
        remainder %= divisor;
        if (__builtin_mul_overflow(quotient, __int128{10}, &quotient) ||
            __builtin_add_overflow(quotient, next, &quotient)) return {};
    }
    if (remainder * 2 > divisor || (remainder * 2 == divisor && quotient % 2)) {
        if (__builtin_add_overflow(quotient, __int128{1}, &quotient)) return {};
    }
    return negative ? -quotient : quotient;
}

std::optional<__int128> DecimalProduct(__int128 left, __int128 right, unsigned scale) {
    const auto a = Int128ToString(left < 0 ? -left : left);
    const auto b = Int128ToString(right < 0 ? -right : right);
    std::vector<unsigned> digits(a.size() + b.size());
    for (std::size_t i = 0; i < a.size(); ++i) {
        for (std::size_t j = 0; j < b.size(); ++j) {
            digits[i + j] += (a[a.size() - i - 1] - '0') * (b[b.size() - j - 1] - '0');
        }
    }
    for (std::size_t i = 0; i + 1 < digits.size(); ++i) {
        digits[i + 1] += digits[i] / 10;
        digits[i] %= 10;
    }
    std::string product;
    product.reserve(digits.size());
    for (auto it = digits.rbegin(); it != digits.rend(); ++it) product.push_back('0' + *it);
    return RoundedDecimalQuotient(product, Power10(scale), (left < 0) != (right < 0));
}

// Match the execution runtime's decimal lexical conversion, including exponent
// scaling, ties-to-even rounding, and its early overflow result. This code uses
// native values only; scalar folding must not invoke a YQL expression evaluator.
std::optional<__int128> ParseDecimalPlain(std::string_view text, unsigned precision, unsigned scale) {
    if (text.empty()) return {};
    bool negative = text.front() == '-';
    if (negative || text.front() == '+') text.remove_prefix(1);
    const auto lower = Lower(text);
    if (lower == "nan") return TDecimal::NaN();
    if (lower == "inf") return negative ? -TDecimal::Inf() : TDecimal::Inf();
    __int128 value = 0;
    unsigned integral = precision - scale;
    bool dot = false;
    std::size_t at = 0;
    for (; at < text.size(); ++at) {
        const char ch = text[at];
        if (ch == '.') {
            if (dot) return {};
            dot = true;
            continue;
        }
        if (dot) {
            if (scale) --scale;
            else break;
        }
        if (ch < '0' || ch > '9') return {};
        value = value * 10 + ch - '0';
        if (!dot && value && !integral--) return negative ? -TDecimal::Inf() : TDecimal::Inf();
    }
    if (at < text.size()) {
        const char first = text[at++];
        if (first < '0' || first > '9') return {};
        bool round = first > '5' || (first == '5' && value % 2);
        bool tail = false;
        for (; at < text.size(); ++at) {
            if (text[at] < '0' || text[at] > '9') return {};
            tail |= text[at] != '0';
        }
        round |= first == '5' && tail;
        if (round && ++value >= Power10(precision)) value = TDecimal::Inf();
    }
    while (scale--) value *= 10;
    return negative ? -value : value;
}

std::optional<__int128> ParseDecimal(std::string_view text, TType type) {
    const auto exponentAt = text.find_last_of("eE");
    if (exponentAt == std::string_view::npos || !exponentAt)
        return ParseDecimalPlain(text, type.Precision, type.Scale);
    auto exponentText = text.substr(exponentAt + 1);
    if (!exponentText.empty() && exponentText.front() == '+') {
        exponentText.remove_prefix(1);
        if (!exponentText.empty() && exponentText.front() == '-') return {};
    }
    int exponent;
    const auto parsed = std::from_chars(exponentText.data(), exponentText.data() + exponentText.size(), exponent);
    if (parsed.ec != std::errc{} || parsed.ptr != exponentText.data() + exponentText.size()) return {};
    const std::int64_t scale = std::int64_t{type.Scale} + exponent;
    auto value = exponent > 0 ? ParseDecimalPlain(text.substr(0, exponentAt), type.Precision, std::min<std::int64_t>(scale, type.Precision)) :
        ParseDecimalPlain(text.substr(0, exponentAt), std::min<std::int64_t>(std::int64_t{type.Precision} - exponent, 35), std::max<std::int64_t>(scale, 0));
    if (!value || *value == TDecimal::NaN()) return {};
    if (IsDecimalSpecial(*value)) {
        auto mantissa = text.substr(0, exponentAt);
        if (mantissa.front() == '+' || mantissa.front() == '-') mantissa.remove_prefix(1);
        return !mantissa.empty() && mantissa.front() >= '0' && mantissa.front() <= '9' ? value : std::nullopt;
    }
    const auto shift = exponent > 0 ? std::max<std::int64_t>(0, scale - type.Precision) : std::min<std::int64_t>(0, scale);
    if (!*value || !shift) return value;
    if (shift > 0) {
        const bool negative = *value < 0;
        if (shift > 35 || __builtin_mul_overflow(*value, Power10(shift), &*value))
            return negative ? -TDecimal::Inf() : TDecimal::Inf();
        return BoundDecimal(*value, {K::Decimal, false, 35, 0});
    }
    if (shift < -35) return 0;
    return RoundedDecimalQuotient(Int128ToString(*value < 0 ? -*value : *value), Power10(-shift), *value < 0);
}

std::optional<__int128> IntegralValue(const TValue& value) {
    if (auto p = std::get_if<std::int64_t>(&value.Data)) {
        return *p;
    }
    if (auto p = std::get_if<std::uint64_t>(&value.Data)) {
        return *p;
    }
    if (auto p = std::get_if<bool>(&value.Data)) {
        return *p ? 1 : 0;
    }
    if (auto p = std::get_if<TDecimal>(&value.Data)) {
        if (IsDecimalSpecial(p->Coefficient)) return {};
        const auto rounded = RoundedDecimalQuotient(Int128ToString(p->Coefficient < 0 ? -p->Coefficient : p->Coefficient),
            Power10(value.Type.Scale), p->Coefficient < 0);
        // The runtime first rounds to Decimal(p-s,0), then converts to integer.
        const TType integral{K::Decimal, false, static_cast<std::uint8_t>(value.Type.Precision - value.Type.Scale), 0};
        return rounded && FitsDecimal(*rounded, integral) ? rounded : std::nullopt;
    }
    if (auto p = std::get_if<double>(&value.Data)) {
        if (std::isfinite(*p) && *p >= -std::ldexp(1.0, 63) && *p < std::ldexp(1.0, 64)) {
            return static_cast<__int128>(*p);
        }
        return {};
    }
    if (auto p = std::get_if<std::string>(&value.Data)) {
        auto text = std::string_view(*p);
        if (!text.empty() && (text.front() == '+' || text.front() == '-')) text.remove_prefix(1);
        if (text.empty() || !std::all_of(text.begin(), text.end(), [](char ch) { return ch >= '0' && ch <= '9'; })) return {};
        return ParseDecimal(*p, {K::Decimal, false, 35, 0});
    }
    return {};
}

std::optional<double> FloatingValue(const TValue& value) {
    if (auto p = std::get_if<double>(&value.Data)) {
        return *p;
    }
    if (auto p = std::get_if<TDecimal>(&value.Data)) {
        return DecimalFloat<double>(p->Coefficient, value.Type.Scale);
    }
    if (auto p = std::get_if<std::string>(&value.Data)) {
        auto special = Lower(*p);
        const bool negative = special.starts_with('-');
        if (special.starts_with('-') || special.starts_with('+')) special.erase(0, 1);
        if (special == "inf" || special == "nan") {
            const double result = special == "inf" ? std::numeric_limits<double>::infinity() : std::numeric_limits<double>::quiet_NaN();
            return negative ? -result : result;
        }
        // Match the runtime's numeric text contract with the generic numeric
        // parser: leading spaces and hexadecimal integers are accepted, as are
        // overflow/underflow to infinity/zero. No locale or YQL parser is used.
        using TParser = double_conversion::StringToDoubleConverter;
        static const TParser parser(TParser::ALLOW_TRAILING_JUNK | TParser::ALLOW_HEX | TParser::ALLOW_LEADING_SPACES,
            0.0, std::numeric_limits<double>::quiet_NaN(), nullptr, nullptr);
        if (p->empty() || p->size() > std::numeric_limits<int>::max()) return {};
        int consumed = 0;
        const auto result = parser.StringToDouble(p->data(), static_cast<int>(p->size()), &consumed);
        return consumed == static_cast<int>(p->size()) ? std::optional<double>{result} : std::nullopt;
    }
    const auto integral = IntegralValue(value);
    return integral ? std::optional<double>{static_cast<double>(*integral)} : std::nullopt;
}

// UUID values carry 16 bytes, independent of their canonical text spelling.
constexpr unsigned UuidByteOrder[]{3, 2, 1, 0, 5, 4, 7, 6, 8, 9, 10, 11, 12, 13, 14, 15};
std::optional<std::string> ParseUuid(std::string_view text) {
    if (text.size() != 36) return {};
    std::string bytes(16, '\0');
    std::size_t offset = 0;
    const auto digit = [](char ch) -> int {
        if (ch >= '0' && ch <= '9') return ch - '0';
        if (ch >= 'a' && ch <= 'f') return ch - 'a' + 10;
        if (ch >= 'A' && ch <= 'F') return ch - 'A' + 10;
        return -1;
    };
    for (unsigned i = 0; i < 16; ++i) {
        if (i == 4 || i == 6 || i == 8 || i == 10) if (text[offset++] != '-') return {};
        const int high = digit(text[offset++]), low = digit(text[offset++]);
        if (high < 0 || low < 0) return {};
        bytes[UuidByteOrder[i]] = static_cast<char>((high << 4) | low);
    }
    return bytes;
}
std::string FormatUuid(std::string_view bytes) {
    constexpr char digits[] = "0123456789abcdef";
    std::string result;
    result.reserve(36);
    for (unsigned i = 0; i < 16; ++i) {
        if (i == 4 || i == 6 || i == 8 || i == 10) result += '-';
        const auto byte = static_cast<unsigned char>(bytes[UuidByteOrder[i]]);
        result += digits[byte >> 4]; result += digits[byte & 15];
    }
    return result;
}

TValue ConvertValue(const TValue& value, TType target) {
    if (value.IsNull()) {
        return NullValue(target);
    }
    if (value.Type.Required() == target.Required()) {
        return {target, value.Data};
    }
    if (target.IsString()) {
        auto text = FormatValue(value);
        if (value.Type.Kind == K::Decimal && value.Type.Scale) {
            while (text.back() == '0') text.pop_back();
            if (text.back() == '.') text.pop_back();
        }
        if (target.Kind == K::Utf8 && !IsUtf8(text)) return NullValue(target);
        return {target, std::move(text)};
    }
    if (target.Kind == K::Uuid) {
        const auto* text = std::get_if<std::string>(&value.Data);
        const auto bytes = text ? ParseUuid(*text) : std::nullopt;
        return bytes ? TValue{target, *bytes} : NullValue(target);
    }
    if (target.Kind == K::Bool) {
        if (const auto* string = std::get_if<std::string>(&value.Data)) {
            const auto lowered = Lower(*string);
            if (lowered == "true" || lowered == "false") {
                return {target, lowered == "true"};
            }
        } else if (const auto* number = std::get_if<double>(&value.Data)) {
            const int category = value.Type.Kind == K::Float ? std::fpclassify(static_cast<float>(*number)) : std::fpclassify(*number);
            if (category == FP_NAN) return NullValue(target);
            return {target, category != FP_ZERO && category != FP_SUBNORMAL};
        } else if (auto number = FloatingValue(value)) {
            return {target, *number != 0};
        }
        return NullValue(target);
    }
    if (target.Kind == K::Decimal) {
        if (auto decimal = std::get_if<TDecimal>(&value.Data)) {
            auto coefficient = decimal->Coefficient;
            if (IsDecimalSpecial(coefficient)) return {target, *decimal};
            if (target.Scale < value.Type.Scale) {
                coefficient = *RoundedDecimalQuotient(Int128ToString(coefficient < 0 ? -coefficient : coefficient),
                    Power10(value.Type.Scale - target.Scale), coefficient < 0);
            } else if (__builtin_mul_overflow(coefficient, Power10(target.Scale - value.Type.Scale), &coefficient)) {
                coefficient = decimal->Coefficient < 0 ? -TDecimal::Inf() : TDecimal::Inf();
            }
            return {target, TDecimal{BoundDecimal(coefficient, target)}};
        }
        if (auto decimal = ParseDecimal(FormatValue(value), target)) {
            return {target, TDecimal{*decimal}};
        }
        return NullValue(target);
    }
    if (target.Kind == K::Double || target.Kind == K::Float) {
        if (target.Kind == K::Float) {
            if (const auto* decimal = std::get_if<TDecimal>(&value.Data))
                return {target, static_cast<double>(DecimalFloat<float>(decimal->Coefficient, value.Type.Scale))};
            // Convert integers directly: an intermediate Double can round a
            // large integer onto a Float halfway point and change the result.
            if (const auto* integer = std::get_if<std::int64_t>(&value.Data))
                return {target, static_cast<double>(static_cast<float>(*integer))};
            if (const auto* integer = std::get_if<std::uint64_t>(&value.Data))
                return {target, static_cast<double>(static_cast<float>(*integer))};
        }
        if (auto number = FloatingValue(value)) {
            const double result = target.Kind == K::Float ? static_cast<float>(*number) : *number;
            return {target, result};
        }
        return NullValue(target);
    }
    if (target.IsIntegral() || target.IsInterval() || target.IsDate()) {
        if (IsDate(value.Type.Kind) && IsDate(target.Kind)) {
            const auto micros = *IntegralValue(value) * DateUnit(value.Type.Kind);
            const auto result = micros / DateUnit(target.Kind);
            if ((!target.IsExtendedTemporal() && micros < 0) || !FitsTemporal(result, target.Kind)) return NullValue(target);
            return IsSignedPayload(target) ? TValue{target, static_cast<std::int64_t>(result)} :
                TValue{target, static_cast<std::uint64_t>(result)};
        }
        if ((target.IsInterval() || target.IsDate()) && std::holds_alternative<std::string>(value.Data)) {
            if (const auto parsed = ParseTemporal(std::get<std::string>(value.Data), target.Kind)) {
                return IsSignedPayload(target) ? TValue{target, *parsed} : TValue{target, static_cast<std::uint64_t>(*parsed)};
            }
            return NullValue(target);
        }
        if (target.IsIntegral()) {
            if (!target.IsSigned()) {
                if (const auto* text = std::get_if<std::string>(&value.Data); text && text->starts_with('-')) return NullValue(target);
            }
            if (const auto* number = std::get_if<double>(&value.Data)) {
                const int category = value.Type.Kind == K::Float ? std::fpclassify(static_cast<float>(*number)) : std::fpclassify(*number);
                if (category == FP_ZERO || category == FP_SUBNORMAL) {
                    return target.IsSigned() ? TValue{target, std::int64_t{0}} : TValue{target, std::uint64_t{0}};
                }
                const int width = Width(target.Kind);
                const __int128 minimum = target.IsSigned() ? -(__int128{1} << (width - 1)) : 0;
                const __int128 maximum = (__int128{1} << (width - target.IsSigned())) - 1;
                if (!std::isfinite(*number) || static_cast<long double>(*number) < static_cast<long double>(minimum) ||
                    static_cast<long double>(*number) > static_cast<long double>(maximum)) return NullValue(target);
            }
        }
        if (auto number = IntegralValue(value)) {
            if ((target.IsInterval() || target.IsDate()) && !FitsTemporal(*number, target.Kind)) {
                return NullValue(target);
            }
            const bool isSigned = IsSignedPayload(target);
            const int width = PayloadWidth(target.Kind);
            if (isSigned ? FitsSigned(*number, width) : FitsUnsigned(*number, width)) {
                return isSigned ? TValue{target, static_cast<std::int64_t>(*number)} : TValue{target, static_cast<std::uint64_t>(*number)};
            }
        }
    }
    return NullValue(target);
}

void ValidateValue(const TExpressionArena& arena, const TValue& value, TSourcePosition position) {
    arena.ValidateType(value.Type, position);
    if (value.IsNull()) {
        if (!value.Type.OptionalDepth && value.Type.Kind != K::Null) {
            Fail(position, "NULL requires an optional type");
        }
        return;
    }
    if (value.Type.OptionalDepth > 1) {
        const auto* optional = std::get_if<TOptionalValue>(&value.Data);
        if (!optional || !*optional || (*optional)->Type != value.Type.Unwrapped())
            Fail(position, "Nested optional value type mismatch");
        ValidateValue(arena, **optional, position);
        return;
    }
    if (value.Type.IsCollection()) {
        const auto* tuple = std::get_if<TTuple>(&value.Data);
        const bool list = value.Type.Kind == K::List;
        const auto fields = list ? std::span<const TType>{} : value.Type.Kind == K::Struct ?
            arena.StructItems(value.Type) : arena.TupleItems(value.Type);
        if (!tuple || !*tuple || (!list && (*tuple)->Items.size() != fields.size())) Fail(position, "Collection value shape mismatch");
        for (std::size_t i = 0; i < (*tuple)->Items.size(); ++i) {
            if ((*tuple)->Items[i].Type != (list ? arena.ListItem(value.Type) : fields[i])) Fail(position, "Collection element type mismatch");
            ValidateValue(arena, (*tuple)->Items[i], position);
        }
        return;
    }
    bool valid = false;
    if (IsSignedPayload(value.Type)) {
        if (const auto* number = std::get_if<std::int64_t>(&value.Data)) {
            valid = value.Type.IsInterval() || value.Type.IsDate() ? FitsTemporal(*number, value.Type.Kind) : FitsSigned(*number, Width(value.Type.Kind));
        }
    } else if (value.Type.Kind == K::Calendar) {
        const auto* micros = std::get_if<std::uint64_t>(&value.Data);
        valid = micros && *micros < TimestampLimit;
    } else if (value.Type.IsIntegral() || IsDate(value.Type.Kind)) {
        if (const auto* number = std::get_if<std::uint64_t>(&value.Data)) {
            const int width = PayloadWidth(value.Type.Kind);
            valid = IsDate(value.Type.Kind) ? FitsTemporal(*number, value.Type.Kind) : FitsUnsigned(*number, width);
        }
    } else if (value.Type.IsString() || value.Type.IsDocument() || value.Type.Kind == K::Uuid) {
        valid = std::holds_alternative<std::string>(value.Data);
        if (valid && value.Type.Kind == K::Uuid) valid = std::get<std::string>(value.Data).size() == 16;
        if (valid && value.Type.Kind == K::Utf8) valid = IsUtf8(std::get<std::string>(value.Data));
    } else if (value.Type.Kind == K::Bool) {
        valid = std::holds_alternative<bool>(value.Data);
    } else if (value.Type.Kind == K::Double || value.Type.Kind == K::Float) {
        valid = std::holds_alternative<double>(value.Data);
    } else if (value.Type.Kind == K::Decimal) {
        // Runtime exponent conversion can produce a finite coefficient wider
        // than the declared precision. Preserve that existing behavior instead
        // of rejecting or saturating it during native evaluation/folding.
        valid = std::holds_alternative<TDecimal>(value.Data) &&
            (FitsDecimal(std::get<TDecimal>(value.Data).Coefficient, {K::Decimal, false, 35, 0}) ||
             IsDecimalSpecial(std::get<TDecimal>(value.Data).Coefficient));
    }
    if (!valid) {
        Fail(position, "Value does not match " + value.Type.ToString());
    }
}

bool WideningIntegerCast(TType source, TType target) {
    return source.IsIntegral() && target.IsIntegral() &&
        ((source.IsSigned() == target.IsSigned() && Width(source.Kind) <= Width(target.Kind)) ||
         (!source.IsSigned() && target.IsSigned() && Width(source.Kind) < Width(target.Kind)));
}

// One conversion contract for scalar operands and elements of list casts.
// A nullable result beyond the requested type denotes a potentially failing cast.
TType ScalarCastType(TType source, TType target, bool literal, TSourcePosition position) {
    if ((source.IsDocument() || target.IsDocument()) && source.Kind != K::Null && source.Required() != target.Required())
        throw TUnsupportedFeature(EFeature::Expression, position, "JSON/YSON conversion is outside the native scalar profile");
    if (source.Kind == K::Calendar || target.Kind == K::Calendar)
        throw TUnsupportedFeature(EFeature::Type, position, "calendar values require DateTime accessors or constructors");
    if (target.Kind == K::Null) {
        Fail(position, "Cannot cast to Null");
    }
    if (source.Kind == K::Float && target.IsIntegral() && Width(target.Kind) >= 32) {
        throw TUnsupportedFeature(EFeature::Expression, position,
            "Float to 32/64-bit integers is outside the native profile pending runtime boundary semantics");
    }
    if (target.IsString() && (source.Kind == K::Float || source.Kind == K::Double)) {
        throw TUnsupportedFeature(EFeature::Expression, position, "Floating point to string formatting is not supported");
    }
    if (source.Kind == K::Decimal && target.IsIntegral() && source.Precision == source.Scale)
        Fail(position, "Decimal to integer cast requires integral digits");
    if (target.Kind == K::Decimal && ((source.IsIntegral() && target.Precision == target.Scale) ||
        (source.Kind == K::Decimal && !std::min(source.Precision - source.Scale, target.Precision - target.Scale) &&
            !std::min(source.Scale, target.Scale)))) {
        Fail(position, "Decimal cast requires overlapping integral or fractional digits");
    }
    if (source.Kind != K::Null && source.Required() != target.Required() && !source.IsString() && !target.IsString()) {
        const bool sourceDate = IsDate(source.Kind), targetDate = IsDate(target.Kind);
        const bool sourceFloat = source.Kind == K::Float || source.Kind == K::Double;
        const bool targetFloat = target.Kind == K::Float || target.Kind == K::Double;
        const bool allowed = (source.IsIntegral() && (target.IsNumeric() || target.Kind == K::Bool || targetDate || target.IsInterval())) ||
            (sourceFloat && (target.IsIntegral() || targetFloat || target.Kind == K::Bool)) ||
            (source.Kind == K::Bool && (target.IsIntegral() || targetFloat)) ||
            (source.Kind == K::Decimal && (target.IsNumeric())) ||
            (sourceDate && (targetDate || target.IsIntegral() || targetFloat)) ||
            (source.IsInterval() && (target.IsIntegral() || targetFloat || target.IsInterval()));
        if (!allowed) Fail(position, "Unsupported cast from " + source.ToString() + " to " + target.ToString());
    }
    if (source.Required() == target.Required() || WideningIntegerCast(source, target)) {
        target.OptionalDepth = std::max(target.OptionalDepth, source.OptionalDepth);
        return target;
    }
    if (source.IsString() && !target.IsString()) {
        if (!literal && !target.IsIntegral() && target.Kind != K::Bool && target.Kind != K::Float &&
            target.Kind != K::Double && target.Kind != K::Date && target.Kind != K::Decimal && target.Kind != K::Uuid)
            throw TUnsupportedFeature(EFeature::Expression, position, "Text conversion to " + target.ToString() + " requires a literal; dynamic text parsing is not supported");
        return target.Optional();
    }
    // A potentially lossy conversion is not necessarily nullable: integer to
    // float, float narrowing and integral truth conversion are total in YQL.
    const bool sourceFloat = source.Kind == K::Float || source.Kind == K::Double;
    const bool targetFloat = target.Kind == K::Float || target.Kind == K::Double;
    const bool temporalInteger = (source.IsDate() || source.IsInterval()) && target.IsIntegral() && [&] {
        const auto [minimum, maximum] = TemporalBounds(source.Kind);
        return target.IsSigned() ? FitsSigned(minimum, Width(target.Kind)) && FitsSigned(maximum, Width(target.Kind)) :
            FitsUnsigned(minimum, Width(target.Kind)) && FitsUnsigned(maximum, Width(target.Kind));
    }();
    const bool total = temporalInteger ||
        (source.IsDate() && target.IsDate() && (!source.IsExtendedTemporal() || target.IsExtendedTemporal())) ||
        (source.Kind == K::Interval && target.Kind == K::Interval64) ||
        (source.IsIntegral() && (target.IsDate() || target.IsInterval()) &&
            FitsTemporal(source.IsSigned() ? -(__int128{1} << (Width(source.Kind) - 1)) : 0, target.Kind) &&
            FitsTemporal((__int128{1} << (Width(source.Kind) - source.IsSigned())) - 1, target.Kind)) ||
        (targetFloat && (source.IsIntegral() || sourceFloat || source.IsDate() || source.IsInterval() || source.Kind == K::Bool || source.Kind == K::Decimal)) ||
        (target.Kind == K::Decimal && (source.IsIntegral() || source.Kind == K::Decimal)) ||
        (source.Kind == K::Bool && target.IsIntegral()) ||
        (target.Kind == K::Bool && source.IsIntegral()) ||
        (target.IsString() && (source.IsIntegral() || source.Kind == K::Bool || source.Kind == K::Utf8 || source.Kind == K::Decimal || source.Kind == K::Uuid));
    target.OptionalDepth = std::max<std::uint8_t>(target.OptionalDepth, source.OptionalDepth || !total);
    return target;
}

bool ChargeMaterializedBytes(const TScalar& value, TType type, std::size_t& remaining) {
    if (const auto* text = std::get_if<std::string>(&value)) {
        if (text->size() > remaining) return false;
        remaining -= text->size();
    } else if (const auto* optional = std::get_if<TOptionalValue>(&value)) {
        return ChargeMaterializedBytes((*optional)->Data, (*optional)->Type, remaining);
    } else if (const auto* tuple = std::get_if<TTuple>(&value)) {
        if (type.Kind == K::List) {
            if ((*tuple)->Items.size() > remaining / sizeof(TValue)) return false;
            remaining -= (*tuple)->Items.size() * sizeof(TValue);
        }
        for (const auto& item : (*tuple)->Items) if (!ChargeMaterializedBytes(item.Data, item.Type, remaining)) return false;
    }
    return true;
}

TScalar NormalizeValue(TType type, TScalar value) {
    if (type.Kind == K::Float && std::holds_alternative<double>(value))
        return static_cast<double>(static_cast<float>(std::get<double>(value)));
    if (const auto* optional = std::get_if<TOptionalValue>(&value)) {
        return std::make_shared<const TValue>(TValue{(*optional)->Type, NormalizeValue((*optional)->Type, (*optional)->Data)});
    }
    if (const auto* tuple = std::get_if<TTuple>(&value)) {
        auto items = (*tuple)->Items;
        for (auto& item : items) item.Data = NormalizeValue(item.Type, std::move(item.Data));
        return std::make_shared<const TTupleValue>(TTupleValue{std::move(items)});
    }
    return value;
}

// Scalar identity ignores source positions but preserves type, exact literal
// bits, bindings and operand order. Callers decide which operations may share
// identity; in particular runtime assertions and volatile calls need care.
template<class TChildClass>
std::size_t ExpressionHash(const TExprNode& node, const TChildClass& childClass) {
    std::size_t hash = static_cast<std::size_t>(node.Op);
    const auto mix = [&](std::size_t value) { hash ^= value + 0x9e3779b9u + (hash << 6) + (hash >> 2); };
    mix(static_cast<std::size_t>(node.Type.Kind)); mix(node.Type.OptionalDepth);
    mix(node.Type.Precision); mix(node.Type.Scale); mix(node.Type.Shape);
    mix(node.ColumnId); mix(std::hash<std::string>{}(node.Name)); mix(node.Value.index());
    std::visit([&](const auto& value) {
        using T = std::decay_t<decltype(value)>;
        if constexpr (std::is_same_v<T, TDecimal>) {
            mix(static_cast<std::uint64_t>(value.Coefficient));
            mix(static_cast<std::uint64_t>(static_cast<unsigned __int128>(value.Coefficient) >> 64));
        } else if constexpr (std::is_same_v<T, double>) {
            mix(std::bit_cast<std::uint64_t>(value));
        } else {
            mix(std::hash<T>{}(value));
        }
    }, node.Value);
    for (auto arg : node.Arguments) mix(childClass(arg));
    return hash;
}

template<class TChildClass>
bool SameExpression(const TExprNode& left, const TExprNode& right, const TChildClass& childClass) {
    if (left.Op != right.Op || left.Type != right.Type || left.ColumnId != right.ColumnId ||
        left.Name != right.Name || left.Arguments.size() != right.Arguments.size()) return false;
    const auto* number = std::get_if<double>(&left.Value);
    const auto* otherNumber = std::get_if<double>(&right.Value);
    const bool sameValue = number && otherNumber ?
        std::bit_cast<std::uint64_t>(*number) == std::bit_cast<std::uint64_t>(*otherNumber) : left.Value == right.Value;
    return sameValue && std::equal(left.Arguments.begin(), left.Arguments.end(), right.Arguments.begin(),
        [&](TExprId a, TExprId b) { return childClass(a) == childClass(b); });
}

} // namespace

TFrontendError::TFrontendError(TSourcePosition position, std::string message)
    : std::runtime_error(std::move(message)), Position(position)
{}

TType CommonType(TType left, TType right, TSourcePosition position) {
    return ComputeCommonType(left, right, position);
}

TType TType::Optional() const {
    // This operation ensures nullability; Null already has no present value.
    // An explicit optional wrapper is constructed separately by OptionalWrap.
    if (Kind == K::Null) return Required();
    auto result = *this;
    result.OptionalDepth = std::max<std::uint8_t>(result.OptionalDepth, 1);
    return result;
}

TType TType::Required() const {
    auto result = *this;
    result.OptionalDepth = false;
    return result;
}

TType TType::Unwrapped() const {
    auto result = *this;
    if (result.OptionalDepth) --result.OptionalDepth;
    return result;
}

bool TType::IsIntegral() const { return Width(Kind) != 0; }
bool TType::IsNumeric() const { return IsIntegral() || Kind == K::Float || Kind == K::Double || Kind == K::Decimal; }
bool TType::IsString() const { return Kind == K::String || Kind == K::Utf8; }
bool TType::IsDate() const { return (Kind >= K::Date && Kind <= K::Timestamp) || (Kind >= K::Date32 && Kind <= K::Timestamp64); }
bool TType::IsInterval() const { return Kind == K::Interval || Kind == K::Interval64; }
bool TType::IsExtendedTemporal() const { return Kind >= K::Date32 && Kind <= K::Interval64; }
bool TType::IsSigned() const { return Kind >= K::Int8 && Kind <= K::Int64; }

std::string TType::ToString() const {
    std::string result(TypeNames[static_cast<unsigned>(Kind)]);
    if (Kind == K::Decimal) {
        result += "(" + std::to_string(Precision) + "," + std::to_string(Scale) + ")";
    }
    if (IsCollection()) result += "#" + std::to_string(Shape);
    return result + std::string(OptionalDepth, '?');
}

TType ParseType(std::string_view name, TSourcePosition position) {
    std::uint8_t nullable = 0;
    while (!name.empty() && name.back() == '?') {
        if (nullable == 64) throw TResourceLimitError(position, "Optional type nesting exceeds 64");
        ++nullable;
        name.remove_suffix(1);
    }
    for (unsigned i = 0; i <= static_cast<unsigned>(K::Yson); ++i) {
        if (i != static_cast<unsigned>(K::Decimal) && EqualAsciiCase(name, TypeNames[i])) {
            return {static_cast<K>(i), nullable};
        }
    }
    const auto normalized = Lower(name);
    for (const auto& [alias, kind] : {std::pair{"tinyint", K::Int8}, {"smallint", K::Int16},
            {"int", K::Int32}, {"integer", K::Int32}, {"bigint", K::Int64}, {"varchar", K::String}}) {
        if (normalized == alias) return {kind, nullable};
    }
    if (normalized.starts_with("decimal(") && normalized.back() == ')') {
        const auto comma = normalized.find(',');
        if (comma != std::string::npos) {
            unsigned precision = 0, scale = 0;
            const auto a = std::from_chars(normalized.data() + 8, normalized.data() + comma, precision);
            const auto b = std::from_chars(normalized.data() + comma + 1, normalized.data() + normalized.size() - 1, scale);
            if (a.ec == std::errc{} && b.ec == std::errc{} && a.ptr == normalized.data() + comma &&
                b.ptr == normalized.data() + normalized.size() - 1 && precision <= 35 && scale <= precision) {
                TType type{K::Decimal, nullable, static_cast<std::uint8_t>(precision), static_cast<std::uint8_t>(scale)};
                ValidateScalarType(type, position);
                return type;
            }
        }
    }
    throw TUnsupportedFeature(EFeature::Type, position, "Unsupported scalar type: " + std::string(name));
}

void TExpressionArena::ValidateType(TType type, TSourcePosition position) const {
    ValidateScalarType(type, position);
    if (type.OptionalDepth > 64) throw TResourceLimitError(position, "Optional type nesting exceeds 64");
    if (type.IsCollection() && (!type.Shape || type.Shape > Shapes_.size()))
        Fail(position, "Invalid collection shape in expression arena");
    if (type.IsCollection() && Shapes_[type.Shape - 1].Named != (type.Kind == K::Struct))
        Fail(position, "Collection shape has incompatible field names");
    if (type.Kind == K::List && Shapes_[type.Shape - 1].Items.size() != 1)
        Fail(position, "List types require exactly one item type");
}

TType TExpressionArena::TupleType(std::span<const TType> items, TSourcePosition position) {
    return InternShape(items, {}, false, position);
}

TType TExpressionArena::InternShape(std::span<const TType> items, std::span<const std::string> names,
    bool named, TSourcePosition position) {
    std::size_t hash = items.size();
    unsigned depth = 1;
    bool nullable = false;
    bool tupleOrderable = true;
    for (const auto item : items) {
        ValidateType(item, position);
        if (item.IsDocument()) throw TUnsupportedFeature(EFeature::Type, position, "JSON/YSON tuple states are outside the native scalar profile");
        if (item.Kind == K::Calendar) Fail(position, "Calendar values cannot enter tuple states");
        if (item.IsCollection()) depth = std::max(depth, 1u + Shapes_[item.Shape - 1].Depth);
        nullable |= CanContainNull(item);
        tupleOrderable &= Ordering(item) != EOrdering::Unsupported;
        const std::uint64_t bits = static_cast<unsigned>(item.Kind) | (std::uint64_t{item.OptionalDepth} << 8) |
            (std::uint64_t{item.Precision} << 16) | (std::uint64_t{item.Scale} << 24) | (std::uint64_t{item.Shape} << 32);
        hash ^= std::hash<std::uint64_t>{}(bits) + 0x9e3779b9 + (hash << 6) + (hash >> 2);
    }
    if (named) {
        hash ^= 0x85ebca6b;
        for (const auto& name : names)
            hash ^= std::hash<std::string_view>{}(name) + 0x9e3779b9 + (hash << 6) + (hash >> 2);
    }
    if (depth > 64) throw TResourceLimitError(position, "Collection type nesting exceeds 64");
    const auto kind = named ? K::Struct : K::Tuple;
    const auto [begin, end] = ShapeHashes_.equal_range(hash);
    for (auto it = begin; it != end; ++it) {
        const auto& existing = Shapes_[it->second - 1];
        if (existing.Named == named && std::equal(items.begin(), items.end(), existing.Items.begin(), existing.Items.end()) &&
            std::equal(names.begin(), names.end(), existing.Names.begin(), existing.Names.end()))
            return {kind, false, 0, 0, it->second};
    }
    if (Shapes_.size() == std::numeric_limits<std::uint32_t>::max())
        throw TResourceLimitError(position, "Too many native collection shapes");
    const auto id = static_cast<std::uint32_t>(Shapes_.size() + 1);
    const auto listOrdering = items.size() == 1 ? Ordering(items.front()) : EOrdering::Unsupported;
    Shapes_.push_back({{items.begin(), items.end()}, {names.begin(), names.end()}, static_cast<std::uint8_t>(depth),
        nullable, named, tupleOrderable, listOrdering});
    ShapeHashes_.emplace(hash, id);
    return {kind, false, 0, 0, id};
}

std::span<const TType> TExpressionArena::TupleItems(TType type) const {
    ValidateType(type);
    if (type.Kind != K::Tuple) Fail({}, "Expected a tuple type");
    return Shapes_[type.Shape - 1].Items;
}

TType TExpressionArena::StructType(std::span<const std::pair<std::string, TType>> fields, TSourcePosition position) {
    const auto order = StructOrder(fields, position);
    std::vector<std::string> names;
    std::vector<TType> types;
    names.reserve(fields.size()); types.reserve(fields.size());
    for (const auto i : order) {
        names.push_back(fields[i].first);
        types.push_back(fields[i].second);
    }
    return InternShape(types, names, true, position);
}

std::span<const std::string> TExpressionArena::StructNames(TType type) const {
    ValidateType(type);
    if (type.Kind != K::Struct) Fail({}, "Expected a struct type");
    return Shapes_[type.Shape - 1].Names;
}

std::span<const TType> TExpressionArena::StructItems(TType type) const {
    ValidateType(type);
    if (type.Kind != K::Struct) Fail({}, "Expected a struct type");
    return Shapes_[type.Shape - 1].Items;
}

bool TExpressionArena::CanContainNull(TType type) const {
    ValidateType(type);
    return type.OptionalDepth || type.Kind == K::Null ||
        (type.IsCollection() && Shapes_[type.Shape - 1].NullableItems);
}

TExpressionArena::EOrdering TExpressionArena::Ordering(TType type) const {
    if (type.IsDocument() || type.Kind == K::Struct || type.Kind == K::Calendar) return EOrdering::Unsupported;
    if (type.Kind == K::Null) return EOrdering::Null;
    if (type.Kind == K::List) return Shapes_[type.Shape - 1].ListOrdering;
    if (type.Kind == K::Tuple && !Shapes_[type.Shape - 1].TupleOrderable) return EOrdering::Unsupported;
    return EOrdering::Supported;
}

bool TExpressionArena::SupportsOrdering(TType type) const {
    ValidateType(type);
    return Ordering(type) == EOrdering::Supported;
}

TType TExpressionArena::ListType(TType item, TSourcePosition position) {
    auto type = TupleType(std::span(&item, 1), position);
    type.Kind = K::List;
    return type;
}

TType TExpressionArena::ListItem(TType type) const {
    ValidateType(type);
    if (type.Kind != K::List) Fail({}, "Expected a list type");
    return Shapes_[type.Shape - 1].Items.at(0);
}

TType TExpressionArena::ParseTypeName(std::string_view name, TSourcePosition position) {
    return ParseTypeName(name, position, 0);
}

TType TExpressionArena::ParseTypeName(std::string_view name, TSourcePosition position, unsigned depth) {
    if (depth > 64) throw TResourceLimitError(position, "Collection type nesting exceeds 64");
    const auto original = name;
    std::uint8_t optional = 0;
    while (!name.empty() && name.back() == '?') {
        if (optional == 64) throw TResourceLimitError(position, "Optional type nesting exceeds 64");
        ++optional;
        name.remove_suffix(1);
    }
    if (name.size() > 6 && EqualAsciiCase(name.substr(0, 5), "List<") && name.back() == '>') {
        auto type = ListType(ParseTypeName(name.substr(5, name.size() - 6), position, depth + 1), position);
        type.OptionalDepth = optional;
        return type;
    }
    if (name.size() >= 7 && EqualAsciiCase(name.substr(0, 6), "Tuple<") && name.back() == '>') {
        const auto fields = name.substr(6, name.size() - 7);
        std::vector<TType> items;
        std::size_t begin = 0;
        unsigned parameters = 0;
        for (std::size_t i = 0; i < fields.size(); ++i) {
            if (fields[i] == '(' || fields[i] == '<') ++parameters;
            if (fields[i] == ')' || fields[i] == '>') {
                if (!parameters) Fail(position, "Unmatched tuple field type parentheses");
                --parameters;
            }
            if (fields[i] == ',' && !parameters) {
                items.push_back(ParseTypeName(fields.substr(begin, i - begin), position, depth + 1));
                begin = i + 1;
            }
        }
        if (parameters) Fail(position, "Unclosed tuple field type parameters");
        if (!fields.empty()) items.push_back(ParseTypeName(fields.substr(begin), position, depth + 1));
        auto type = TupleType(items, position);
        type.OptionalDepth = optional;
        return type;
    }
    return ParseType(original, position);
}

TExprId TExpressionArena::Collection(std::span<const TExprId> items, bool list, TSourcePosition position) {
    std::vector<TType> types;
    std::vector<TExprId> arguments(items.begin(), items.end());
    for (auto id : items) types.push_back((*this)[id].Type);
    TType type;
    if (list) {
        auto common = types.empty() ? TType{K::Null} : types.front();
        for (auto item : types) common = CommonType(common, item, position);
        type = ListType(common, position);
        for (auto& id : arguments) id = Convert(id, common, position);
    } else type = TupleType(types, position);
    return Append({O::Collection, type, position, std::move(arguments), {}, {}, 0});
}

TExprId TExpressionArena::TupleElement(TExprId tuple, std::uint32_t index, TSourcePosition position) {
    const auto source = (*this)[tuple].Type;
    if (source.Kind != K::Tuple || source.OptionalDepth > 1) Fail(position, "Tuple element requires a flat tuple");
    const auto items = TupleItems(source);
    if (index >= items.size()) Fail(position, "Tuple element index is out of range");
    const auto type = source.OptionalDepth && items[index].Kind != K::Null ? items[index].Optional() : items[index];
    return Append({O::TupleElement, type, position, {tuple}, std::uint64_t{index}, {}, 0});
}

TExprId TExpressionArena::Struct(std::span<const std::pair<std::string, TExprId>> fields, TSourcePosition position) {
    const auto order = StructOrder(fields, position);
    std::vector<std::string> names;
    std::vector<TType> types;
    std::vector<TExprId> arguments;
    names.reserve(fields.size()); types.reserve(fields.size()); arguments.reserve(fields.size());
    for (const auto i : order) {
        names.push_back(fields[i].first);
        types.push_back((*this)[fields[i].second].Type);
        arguments.push_back(fields[i].second);
    }
    const auto type = InternShape(types, names, true, position);
    return Append({O::Collection, type, position, std::move(arguments), {}, {}, 0});
}

TExprId TExpressionArena::Member(TExprId record, std::string_view name, TSourcePosition position) {
    const auto source = (*this)[record].Type;
    if (source.Kind != K::Struct || source.OptionalDepth > 1) Fail(position, "Member access requires a flat struct");
    const auto names = StructNames(source);
    const auto found = std::lower_bound(names.begin(), names.end(), name);
    if (found == names.end() || *found != name) Fail(position, "Unknown struct field: " + std::string(name));
    const auto index = static_cast<std::uint64_t>(found - names.begin());
    const auto item = StructItems(source)[index];
    const auto type = source.OptionalDepth ? item.Optional() : item;
    // A constructed record is a set of independent fields. Selecting one
    // must not evaluate discarded fields (including their checked errors).
    if ((*this)[record].Op == O::Collection) return (*this)[record].Arguments[index];
    return Append({O::Member, type, position, {record}, index, std::string(name), 0});
}

TExprId TExpressionArena::ListElement(TExprId list, TExprId index, TSourcePosition position) {
    const auto source = (*this)[list].Type, key = (*this)[index].Type;
    if (source.Kind == K::Null) return list;
    if (source.Kind != K::List || source.OptionalDepth > 1)
        throw TUnsupportedFeature(EFeature::Type, position, "subscript requires a flat typed list");
    if (key.OptionalDepth > 1) Fail(position, "list index requires at most one optional layer");
    auto result = ListItem(source);
    if (result.Kind == K::Null)
        throw TUnsupportedFeature(EFeature::Type, position, "indexing an all-NULL list requires an explicit element type");
    if (result.OptionalDepth == 64) throw TResourceLimitError(position, "Optional type nesting exceeds 64");
    ++result.OptionalDepth;
    // Missing index and an existing NULL element are distinct: list access
    // adds one optional layer, including for an already optional element.
    // A subscript is an exact dictionary key. Explicit numeric casts may
    // truncate fractions, so accept their result only if it still equals the
    // original key. Express the check with ordinary native scalar operations.
    const auto converted = Cast(index, {K::Uint64}, position);
    if (key.Kind == K::Float || key.Kind == K::Double || (key.Kind == K::Decimal && key.Scale))
        index = If(Binary(O::Equal, index, Cast(converted, key.Required(), position), position), converted,
            Literal((*this)[converted].Type.Optional(), {}, position), position);
    else index = converted;
    return Append({O::ListElement, result, position, {list, index}, {}, {}, 0});
}

TExprId TExpressionArena::InCollection(TExprId lookup, TExprId collection, bool ansi, TSourcePosition position) {
    const auto value = (*this)[lookup].Type, values = (*this)[collection].Type;
    if ((values.Kind != K::Tuple && values.Kind != K::List) || values.OptionalDepth || value.IsCollection() || value.OptionalDepth > 1 || value.IsDocument())
        throw TUnsupportedFeature(EFeature::Type, position, "IN requires a flat collection and a scalar lookup");
    const TType item = values.Kind == K::List ? ListItem(values) : TType{};
    const bool empty = (*this)[collection].Op == O::Collection && (*this)[collection].Arguments.empty();
    const auto fields = empty ? std::span<const TType>{} : values.Kind == K::List ? std::span(&item, 1) : TupleItems(values);
    bool nullable = value.OptionalDepth || value.Kind == K::Null;
    for (auto field : fields) {
        if (field.OptionalDepth > 1) throw TUnsupportedFeature(EFeature::Type, position, "IN collection items require at most one optional layer");
        (void)CommonType(value, field, position);
        nullable |= ansi && (field.OptionalDepth || field.Kind == K::Null);
    }
    const TType resultType{K::Bool, nullable};
    // Keep fixed collections as native comparisons, matching the existing
    // SQL lowering policy. Runtime lists retain dictionary lookup semantics,
    // including checked conversion of the lookup to the list's key type.
    if ((*this)[collection].Op == O::Collection) {
        const auto items = (*this)[collection].Arguments;
        auto result = Literal({K::Bool}, false, position);
        for (auto id : items) {
            const auto equal = Binary(O::Equal, lookup, id, position);
            result = Binary(O::Or, result, equal, position);
        }
        return Convert(InResult(lookup, result, ansi, position), resultType, position);
    }
    const bool exactKey = value.Required() == item.Required() &&
        (item.IsDate() || item.IsInterval() || item.Kind == K::Uuid || item.Kind == K::Decimal);
    if (values.Kind != K::List || (!exactKey &&
        (!(item.IsIntegral() || item.IsString() || item.Kind == K::Bool || item.Kind == K::Null) ||
         !(value.IsIntegral() || value.IsString() || value.Kind == K::Double || value.Kind == K::Bool || value.Kind == K::Null))))
        throw TUnsupportedFeature(EFeature::Type, position, "unsupported scalar types for runtime IN dictionary lookup");
    return Append({O::InCollection, resultType, position, {lookup, collection}, {}, ansi ? "ansi" : "legacy", 0});
}

TExprId TExpressionArena::InResult(TExprId lookup, TExprId match, bool ansi, TSourcePosition position) {
    const auto matchType = (*this)[match].Type;
    if (matchType.Kind != K::Bool || matchType.OptionalDepth > 1)
        Fail(position, "IN match requires Bool or Bool?");
    (void)(*this)[lookup];
    if (ansi) return match;
    // Preserve SQL's type-level Null propagation, including discarded checks
    // inside a statically Null operand. A typed nullable lookup is evaluated.
    if ((*this)[lookup].Type.Kind == K::Null) return Literal({K::Bool, true}, {}, position);
    if (matchType.OptionalDepth) {
        const TExprId alternatives[]{match, Literal({K::Bool}, false, position)};
        match = Coalesce(alternatives, position);
    }
    if (!CanContainNull((*this)[lookup].Type)) return match;
    return Append({O::InResult, {K::Bool, true}, position, {lookup, match}, {}, {}, 0});
}

TType TExpressionArena::ImportType(const TExpressionArena& source, TType type) {
    source.ValidateType(type);
    if (&source == this || !type.IsCollection()) return type;
    if (source.Shapes_[type.Shape - 1].Depth == 1) {
        const auto& shape = source.Shapes_[type.Shape - 1];
        type.Shape = InternShape(shape.Items, shape.Names, shape.Named, {}).Shape;
        return type;
    }
    // Shapes are arena-local DAG nodes. Memoize shared shapes so importing a
    // nested type takes time proportional to its distinct shapes, not its tree.
    std::unordered_map<std::uint32_t, std::uint32_t> shapes;
    const auto copy = [&](auto&& self, TType item) -> TType {
        if (!item.IsCollection()) return item;
        if (const auto it = shapes.find(item.Shape); it != shapes.end()) {
            item.Shape = it->second;
            return item;
        }
        std::vector<TType> fields;
        const auto& sourceShape = source.Shapes_[item.Shape - 1];
        for (const auto field : sourceShape.Items) fields.push_back(self(self, field));
        const auto shape = InternShape(fields, sourceShape.Names, sourceShape.Named, {}).Shape;
        shapes.emplace(item.Shape, shape);
        item.Shape = shape;
        return item;
    };
    return copy(copy, type);
}

const TExprNode& TExpressionArena::operator[](TExprId id) const {
    if (id >= Nodes_.size()) {
        throw std::out_of_range("Invalid native expression ID");
    }
    return Nodes_[id];
}

TExprId TExpressionArena::Append(TExprNode node) {
    if (Nodes_.size() == std::numeric_limits<TExprId>::max()) {
        Fail(node.Position, "Too many expressions");
    }
    node.Repeatable = node.Op != O::RandomUuid && node.Op != O::RandomNumber;
    for (auto arg : node.Arguments) {
        if (arg >= Nodes_.size()) {
            Fail(node.Position, "Expression arguments must precede their consumer");
        }
        node.Repeatable &= Nodes_[arg].Repeatable;
    }
    const auto id = static_cast<TExprId>(Nodes_.size());
    Nodes_.push_back(std::move(node));
    return id;
}

TExprId TExpressionArena::Literal(TType type, TScalar value, TSourcePosition position) {
    ValidateValue(*this, {type, value}, position);
    if (type.IsDocument()) {
        const auto* payload = &value;
        while (const auto* optional = std::get_if<TOptionalValue>(payload)) payload = &(*optional)->Data;
        if (const auto* bytes = std::get_if<std::string>(payload)) ValidateDocumentLiteral(type.Kind, *bytes, position);
    }
    if (type.Kind == K::Float || type.IsCollection()) value = NormalizeValue(type, std::move(value));
    return Append({O::Literal, type, position, {}, std::move(value), {}, 0});
}

TExprId TExpressionArena::Null(TSourcePosition position) {
    return Literal({K::Null}, std::monostate{}, position);
}

TExprId TExpressionArena::Column(TColumnId column, TType type, TSourcePosition position) {
    ValidateType(type, position);
    return Append({O::Column, type, position, {}, {}, {}, column});
}

TExprId TExpressionArena::Parameter(std::string name, TType type, TSourcePosition position) {
    ValidateType(type, position);
    return Append({O::Parameter, type, position, {}, {}, std::move(name), 0});
}

TExprId TExpressionArena::BitCast(TExprId input, TType target, TSourcePosition position) {
    ValidateType(target, position);
    const auto source = (*this)[input].Type;
    if (source.OptionalDepth > 1 || target.OptionalDepth || !target.IsIntegral() ||
        !(source.IsIntegral() || source.IsDate() || source.IsInterval() || source.Kind == K::Bool))
        Fail(position, "BITCAST requires Bool, integer or temporal input and a required integer target");
    target.OptionalDepth = source.OptionalDepth;
    if (source == target) return input;
    return Append({O::BitCast, target, position, {input}, {}, {}, 0});
}

TExprId TExpressionArena::CheckedNegate(TExprId input, TSourcePosition position) {
    const auto type = (*this)[input].Type;
    if (type.Kind == K::Null) return input;
    if (!type.IsIntegral()) return Unary(O::Negate, input, position);
    return Cast(Unary(O::Negate, Cast(input, {K::Decimal, false, 20, 0}, position), position), type.Optional(), position);
}

TExprId TExpressionArena::CheckedBinary(O op, TExprId left, TExprId right, TSourcePosition position) {
    if (op != O::Add && op != O::Subtract && op != O::Multiply && op != O::Divide && op != O::Modulo)
        Fail(position, "Checked arithmetic requires an arithmetic operation");
    const auto a = (*this)[left].Type, b = (*this)[right].Type;
    if (a.Kind == K::Null || b.Kind == K::Null) return Null(position);
    if (!a.IsIntegral() || !b.IsIntegral()) return Binary(op, left, right, position);
    const auto result = CommonType(a, b, position).Optional();
    // Validate each operand before arithmetic: mixed signed/unsigned values
    // must not wrap on their way into the common result type.
    left = Cast(left, result, position);
    right = Cast(right, result, position);
    if (op == O::Divide || op == O::Modulo) return Binary(op, left, right, position);
    const TType wide{K::Decimal, false, 20, 0};
    left = Cast(left, wide, position);
    right = Cast(right, wide, position);
    return Cast(Binary(op, left, right, position), result, position);
}

TExprId TExpressionArena::Convert(TExprId input, TType target, TSourcePosition position) {
    ValidateType(target, position);
    if ((*this)[input].Type == target) {
        return input;
    }
    if ((*this)[input].Type.Required() == target.Required() && target.OptionalDepth > 1 &&
        (*this)[input].Type.OptionalDepth < target.OptionalDepth) {
        if (!(*this)[input].Type.OptionalDepth) input = Convert(input, target.Required().Optional(), position);
        while ((*this)[input].Type.OptionalDepth < target.OptionalDepth) input = Unary(O::OptionalWrap, input, position);
        return input;
    }
    if ((*this)[input].Type.Kind == K::Null) return Literal(target, {}, position);
    if ((*this)[input].Type.OptionalDepth > 1 || target.OptionalDepth > 1)
        throw TUnsupportedFeature(EFeature::Type, position, "implicit conversion between nested optional scalar types is outside the native profile");
    // SQL permits fitting integer literals and valid UTF-8 string literals in
    // an otherwise narrower target. Dynamic values still obey implicit ranks.
    const auto& source = (*this)[input];
    if (source.Op == O::Literal && source.Type.OptionalDepth <= target.OptionalDepth &&
        !std::holds_alternative<std::monostate>(source.Value) &&
        ((source.Type.IsIntegral() && target.IsIntegral()) ||
         (source.Type.Kind == K::String && target.Kind == K::Utf8))) {
        auto value = ConvertValue({source.Type, source.Value}, target.Optional());
        if (!value.IsNull()) return Literal(target, std::move(value.Data), position);
    }
    if (CommonType((*this)[input].Type, target, position) != target) {
        Fail(position, "Implicit conversion cannot narrow " + (*this)[input].Type.ToString() + " to " + target.ToString());
    }
    return Append({O::Convert, target, position, {input}, {}, {}, 0});
}

TExprId TExpressionArena::Cast(TExprId input, TType target, TSourcePosition position) {
    ValidateType(target, position);
    const auto source = (*this)[input].Type;
    if (source == target) return input;
    if (target.OptionalDepth > 1 && source.Kind == K::Null) return Literal(target, {}, position);
    if (target.OptionalDepth > 1 && source.OptionalDepth <= 1) {
        auto scalarTarget = target.Required().Optional();
        auto result = Cast(input, scalarTarget, position);
        while ((*this)[result].Type.OptionalDepth < target.OptionalDepth) result = Unary(O::OptionalWrap, result, position);
        return result;
    }
    if (source.OptionalDepth > 1) {
        if (source.Required() == target.Required() && source.OptionalDepth <= target.OptionalDepth)
            return Convert(input, target, position);
        throw TUnsupportedFeature(EFeature::Type, position, "Nested optional casts require matching scalar types and cannot remove optional layers");
    }
    if (source.Kind == K::List && target.Kind == K::List) {
        const auto from = ListItem(source), to = ListItem(target);
        if (from.Kind == K::Null && (*this)[input].Op == O::Collection && (*this)[input].Arguments.empty())
            return Literal(target, TTuple(std::make_shared<TTupleValue>()), position);
        if (from.OptionalDepth > 1 || to.OptionalDepth > 1)
            throw TUnsupportedFeature(EFeature::Type, position, "list casts require scalar elements with at most one optional layer");
        ScalarCastType(from, to, false, position);
        // SQL list casts discard failed required elements. Container
        // optionality preserves an absent input, not element-level failure.
        target.OptionalDepth = std::max(target.OptionalDepth, source.OptionalDepth);
        return Append({O::Cast, target, position, {input}, {}, {}, 0});
    }
    if (source.Kind == K::Tuple && target.Kind == K::Tuple) {
        if (source.OptionalDepth) throw TUnsupportedFeature(EFeature::Type, position, "optional tuple casts require explicit Unwrap");
        const auto from = TupleItems(source), to = TupleItems(target);
        if (from.size() != to.size()) Fail(position, "Tuple casts require matching field counts");
        const std::vector<TType> fields(to.begin(), to.end());
        std::vector<TExprId> items;
        items.reserve(fields.size());
        for (std::size_t i = 0; i < fields.size(); ++i) {
            const auto item = Cast(TupleElement(input, i, position), fields[i], position);
            if ((*this)[item].Type != fields[i])
                throw TUnsupportedFeature(EFeature::Type, position, "tuple casts require total field conversions");
            items.push_back(item);
        }
        auto result = Collection(items, false, position);
        return target.OptionalDepth ? Convert(result, (*this)[result].Type.Optional(), position) : result;
    }
    if (source.IsCollection() || target.IsCollection()) return Convert(input, target, position);
    target = ScalarCastType(source, target, (*this)[input].Op == O::Literal, position);
    if (source.Required() == target.Required() || WideningIntegerCast(source, target))
        return Convert(input, target, position);
    if (source.IsString() && !target.IsString() && (*this)[input].Op == O::Literal) {
        auto converted = ConvertValue({source, (*this)[input].Value}, target);
        return Literal(converted.Type, std::move(converted.Data), position);
    }
    return Append({O::Cast, target, position, {input}, {}, {}, 0});
}

TExprId TExpressionArena::Unary(O op, TExprId input, TSourcePosition position) {
    auto type = (*this)[input].Type;
    if (op == O::OptionalWrap) {
        if (type.Kind == K::Null) throw TUnsupportedFeature(EFeature::Type, position, "Just of bare NULL is outside the native scalar profile");
        if (type.OptionalDepth == 64) throw TResourceLimitError(position, "Optional type nesting exceeds 64");
        ++type.OptionalDepth;
        return Append({op, type, position, {input}, {}, {}, 0});
    }
    if (type.OptionalDepth > 1 && op != O::Unwrap && op != O::IsNull && op != O::IsNotNull && op != O::HasNull)
        throw TUnsupportedFeature(EFeature::Type, position, "Nested optional scalar operation requires explicit Unwrap");
    if (op == O::Unwrap) {
        if (type.Kind == K::Null) Fail(position, "Unwrap requires a typed value");
        if (!type.OptionalDepth) return input;
        type = type.Unwrapped();
    } else if (op == O::CalendarSplit) {
        if (!IsDate(type.Kind) || type.IsExtendedTemporal())
            throw TUnsupportedFeature(EFeature::Type, position, "DateTime::Split requires Date, Datetime or Timestamp");
        type = {K::Calendar, type.OptionalDepth};
    } else if (op == O::TemporalSeconds) {
        if (!type.IsDate()) Fail(position, "DateTime::ToSeconds requires a date or timestamp");
        type = {type.IsExtendedTemporal() ? K::Int64 : K::Uint32, type.OptionalDepth};
    } else if (op == O::UrlHost || op == O::UrlCutWWW) {
        if (type.Kind == K::Null) input = Convert(input, {K::String, true}, position);
        else if (type.IsString()) input = Convert(input, {K::String, true}, position);
        else Fail(position, "URL function requires String or Utf8");
        type = {K::String, true};
    } else if (op == O::CalendarDate || op == O::CalendarDatetime || op == O::CalendarTimestamp || op == O::CalendarYear || op == O::CalendarMinute) {
        if (type.Kind != K::Calendar) Fail(position, "calendar operation requires a calendar value");
        type = {op == O::CalendarDate ? K::Date : op == O::CalendarDatetime ? K::Datetime : op == O::CalendarTimestamp ? K::Timestamp : op == O::CalendarMinute ? K::Uint8 : K::Uint16, type.OptionalDepth};
    } else if (op == O::StablePickle) {
        if (type.IsDocument()) throw TUnsupportedFeature(EFeature::Type, position, "JSON/YSON key serialization is outside the native profile");
        if (type.IsCollection()) Fail(position, "Collection serialization is not supported");
        if (type.Kind == K::Calendar) throw TUnsupportedFeature(EFeature::Type, position, "calendar values cannot be persisted");
        type = {K::String};
    } else if (op == O::IsNull || op == O::IsNotNull || op == O::HasNull) {
        type = {K::Bool};
    } else if (op == O::Not) {
        const auto& value = (*this)[input];
        if (value.Op == O::IsNull || value.Op == O::IsNotNull)
            return Unary(value.Op == O::IsNull ? O::IsNotNull : O::IsNull, value.Arguments[0], position);
        if (type.Kind != K::Bool && type.Kind != K::Null) {
            Fail(position, "NOT expects Bool");
        }
        type = {K::Bool, type.OptionalDepth || type.Kind == K::Null};
        input = Convert(input, type, position);
    } else if (op == O::BitNot) {
        if (type.Kind == K::Null) return input;
        if (!type.IsIntegral() || type.IsSigned()) Fail(position, "Bitwise operations require unsigned integers");
    } else if (op == O::Negate || op == O::Abs) {
        if (!type.IsNumeric() && !type.IsInterval()) {
            Fail(position, "Unary arithmetic expects a number");
        }
    } else if (op == O::UnicodeLower || op == O::UnicodeUpper) {
        if (type.Kind != K::Utf8) Fail(position, "Unicode case conversion expects Utf8");
    } else if (op == O::ListLength || op == O::ListHasItems) {
        if (type.Kind == K::Null && op == O::ListLength) return Literal({K::Uint64, true}, {}, position);
        if (type.Kind != K::List) Fail(position, "List metadata function expects a list");
        type = {op == O::ListLength ? K::Uint64 : K::Bool, type.OptionalDepth};
    } else if (op == O::Length || op == O::Lower || op == O::Upper) {
        if (!type.IsString()) {
            Fail(position, "String function expects String or Utf8");
        }
        if (op == O::Length) {
            type = {K::Uint32, type.OptionalDepth};
        }
    } else {
        Fail(position, "Unsupported unary operation");
    }
    return Append({op, type, position, {input}, {}, {}, 0});
}

TExprId TExpressionArena::Binary(O op, TExprId left, TExprId right, TSourcePosition position) {
    const auto leftType = (*this)[left].Type;
    const auto rightType = (*this)[right].Type;
    if (leftType.OptionalDepth > 1 || rightType.OptionalDepth > 1)
        throw TUnsupportedFeature(EFeature::Type, position, "Nested optional binary operations require explicit Unwrap");
    if (leftType.IsDocument() || rightType.IsDocument())
        throw TUnsupportedFeature(EFeature::Expression, position, "JSON/YSON binary operations are outside the native scalar profile");
    if (op == O::CalendarShiftMonths || op == O::CalendarShiftYears) {
        if (leftType.Kind != K::Calendar || rightType != TType{K::Int32})
            Fail(position, "calendar shift requires a calendar value and non-null Int32 amount");
        return Append({op, {K::Calendar, true}, position, {left, right}, {}, {}, 0});
    }
    if (IsComparison(op) && leftType.Kind == K::Tuple && rightType.Kind == K::Tuple &&
        (*this)[left].Op == O::Collection && (*this)[right].Op == O::Collection) {
        // SQL row comparisons retain scalar three-valued logic. Expand the
        // fixed tuple here; no tuple runtime value crosses the scalar boundary.
        const auto a = (*this)[left].Arguments, b = (*this)[right].Arguments;
        if (a.size() != b.size())
            throw TUnsupportedFeature(EFeature::Expression, position, "tuple comparison requires equal arity");
        const bool equality = op == O::Equal || op == O::NotEqual;
        auto result = Literal({K::Bool}, equality || op == O::LessEqual || op == O::GreaterEqual, position);
        for (std::size_t i = a.size(); i-- > 0;) {
            const auto equal = Binary(O::Equal, a[i], b[i], position);
            if (equality) result = Binary(O::And, equal, result, position);
            else {
                const auto strict = op == O::Less || op == O::LessEqual ? O::Less : O::Greater;
                result = Binary(O::Or, Binary(strict, a[i], b[i], position), Binary(O::And, equal, result, position), position);
            }
        }
        return op == O::NotEqual ? Unary(O::Not, result, position) : result;
    }
    if (leftType.IsCollection() || rightType.IsCollection())
        throw TUnsupportedFeature(EFeature::Expression, position, "collection binary operation is outside the native scalar profile");
    if (leftType.Kind == K::Calendar || rightType.Kind == K::Calendar)
        throw TUnsupportedFeature(EFeature::Type, position, "calendar values require DateTime operations");
    if (op == O::IsNotDistinct) {
        if (leftType.Kind == K::Null) return Unary(O::IsNull, right, position);
        if (rightType.Kind == K::Null) return Unary(O::IsNull, left, position);
        if (leftType.Required() == rightType.Required()) {
            // Same-type SQL null-safe equality also equates NaNs. Keep this
            // distinct from ordinary equality, including after a DAG rewrite.
            const auto common = CommonType(leftType, rightType, position);
            left = Convert(left, common, position);
            right = Convert(right, common, position);
            return Append({op, {K::Bool}, position, {left, right}, {}, {}, 0});
        }
        // Mixed scalar types retain ordinary comparison semantics (including
        // unordered numeric values), with a required result for absent inputs.
        const auto equal = Binary(O::Equal, left, right, position);
        const auto absent = Binary(O::And, Unary(O::IsNull, left, position), Unary(O::IsNull, right, position), position);
        const TExprId alternatives[]{equal, absent};
        return Coalesce(alternatives, position);
    }
    if (op == O::ShiftLeft || op == O::ShiftRight) {
        if (leftType.Kind == K::Null) return left;
        if (!leftType.IsIntegral()) Fail(position, "Shifts require an integer input");
        right = Convert(right, {K::Uint8}, position);
        auto result = leftType;
        switch (result.Kind) {
            case K::Int8: result.Kind = K::Uint8; break;
            case K::Int16: result.Kind = K::Uint16; break;
            case K::Int32: result.Kind = K::Uint32; break;
            case K::Int64: result.Kind = K::Uint64; break;
            default: break;
        }
        return Append({op, result, position, {left, right}, {}, {}, 0});
    }
    if (op == O::BitAnd || op == O::BitOr || op == O::BitXor) {
        if (leftType.Kind == K::Null || rightType.Kind == K::Null) return Null(position);
        if (!leftType.IsIntegral() || leftType.IsSigned() || !rightType.IsIntegral() || rightType.IsSigned())
            Fail(position, "Bitwise operations require unsigned integers");
        const auto common = CommonType(leftType, rightType, position);
        auto leftTarget = common, rightTarget = common;
        leftTarget.OptionalDepth = leftType.OptionalDepth;
        rightTarget.OptionalDepth = rightType.OptionalDepth;
        left = Convert(left, leftTarget, position);
        right = Convert(right, rightTarget, position);
        return Append({op, common, position, {left, right}, {}, {}, 0});
    }
    if ((op == O::Add || op == O::Subtract) && IsDate(leftType.Kind) && rightType.IsInterval()) {
        return Append({op, leftType.Optional(), position, {left, right}, {}, {}, 0});
    }
    if (op == O::Add && leftType.IsInterval() && IsDate(rightType.Kind)) {
        return Append({op, rightType.Optional(), position, {right, left}, {}, {}, 0});
    }
    if (op == O::Subtract && IsDate(leftType.Kind) && IsDate(rightType.Kind)) {
        return Append({op, {leftType.IsExtendedTemporal() || rightType.IsExtendedTemporal() ? K::Interval64 : K::Interval, true}, position, {left, right}, {}, {}, 0});
    }
    if (IsComparison(op) && leftType.IsIntegral() && rightType.IsIntegral()) {
        // Compare mathematical values without narrowing or changing signedness.
        // In particular, Int64 and Uint64 have no lossless common integer type.
        return Append({op, {K::Bool, leftType.OptionalDepth || rightType.OptionalDepth}, position, {left, right}, {}, {}, 0});
    }
    if (IsComparison(op) && ((leftType.IsDate() && rightType.IsDate()) ||
        (leftType.IsInterval() && rightType.IsInterval()))) {
        // Temporal comparison consumes each operand in its original units.
        // Neither the scalar runtime nor the storage kernel needs a cast to
        // a shared date type; retaining the operands also preserves ranges.
        return Append({op, {K::Bool, leftType.OptionalDepth || rightType.OptionalDepth}, position, {left, right}, {}, {}, 0});
    }
    if (((op == O::Multiply || op == O::Divide) && leftType.Kind == K::Decimal && rightType.IsIntegral()) ||
        (op == O::Multiply && rightType.Kind == K::Decimal && leftType.IsIntegral())) {
        // An integer factor or divisor does not change Decimal precision or
        // scale. Converting it first can overflow and change the result.
        auto result = leftType.Kind == K::Decimal ? leftType : rightType;
        result.OptionalDepth = leftType.OptionalDepth || rightType.OptionalDepth;
        if (leftType.IsIntegral()) std::swap(left, right);
        return Append({op, result, position, {left, right}, {}, {}, 0});
    }
    if (IsComparison(op) && leftType.Kind == K::Decimal && rightType.Kind == K::Decimal) {
        // Comparison does not require constructing a common Decimal value.
        // Preserve scales for the runtime's scale-aware comparison primitive.
        return Append({op, {K::Bool, std::max(leftType.OptionalDepth, rightType.OptionalDepth)}, position, {left, right}, {}, {}, 0});
    }
    auto common = CommonType(leftType, rightType, position);
    auto result = common;
    if (op == O::And || op == O::Or) {
        if (common.Kind != K::Bool && common.Kind != K::Null) {
            Fail(position, "Boolean operation expects Bool operands");
        }
        common = result = {K::Bool, common.OptionalDepth || common.Kind == K::Null};
        // Neutral Boolean operands need neither execution nor an arena node.
        // Keep the common nullability even when only one operand remains.
        const auto neutral = [&](TExprId id) {
            const auto& node = (*this)[id];
            return node.Op == O::Literal && std::holds_alternative<bool>(node.Value) &&
                std::get<bool>(node.Value) == (op == O::And);
        };
        if (neutral(left)) return Convert(right, common, position);
        if (neutral(right)) return Convert(left, common, position);
    } else if (op == O::Minimum || op == O::Maximum) {
        if (common.Kind == K::Null) return Literal(common, {}, position);
        if (common.IsExtendedTemporal())
            throw TUnsupportedFeature(EFeature::Type, position, "scalar min/max of extended temporal values is outside the native execution profile");
    } else if (IsComparison(op)) {
        result = {K::Bool, common.OptionalDepth || common.Kind == K::Null};
    } else if (op == O::Concat || op == O::Contains || op == O::StartsWith || op == O::EndsWith) {
        if (!common.IsString()) {
            Fail(position, "String operation expects string operands");
        }
        result = op == O::Concat ? common : TType{K::Bool, common.OptionalDepth};
    } else if (op == O::Add || op == O::Subtract || op == O::Multiply || op == O::Divide || op == O::Modulo) {
        if (!common.IsNumeric() && !(common.IsInterval() && (op == O::Add || op == O::Subtract))) {
            Fail(position, "Arithmetic expects numeric operands");
        }
        if ((op == O::Modulo || op == O::Divide) && common.IsIntegral()) {
            result.OptionalDepth = true;
        }
        if (common.IsInterval()) result.OptionalDepth = true;
    } else {
        Fail(position, "Unsupported binary operation");
    }
    // Lift the result when an operand is nullable; retain each operand's own
    // optionality. An explicit Just(literal) remains distinguishable from the
    // required literal used by the existing optimizer's predicate rules.
    auto leftTarget = common, rightTarget = common;
    leftTarget.OptionalDepth = leftType.OptionalDepth || leftType.Kind == K::Null;
    rightTarget.OptionalDepth = rightType.OptionalDepth || rightType.Kind == K::Null;
    left = Convert(left, leftTarget, position);
    right = Convert(right, rightTarget, position);
    return Append({op, result, position, {left, right}, {}, {}, 0});
}

TExprId TExpressionArena::If(TExprId condition, TExprId thenExpr, TExprId elseExpr, TSourcePosition position) {
    const auto conditionType = (*this)[condition].Type;
    if (conditionType.OptionalDepth > 1)
        throw TUnsupportedFeature(EFeature::Type, position, "Nested optional conditions require explicit Unwrap");
    if (conditionType.Kind != K::Bool && conditionType.Kind != K::Null) {
        Fail(position, "IF condition expects Bool");
    }
    condition = Convert(condition, {K::Bool, conditionType.OptionalDepth || conditionType.Kind == K::Null}, position);
    const auto type = CommonType((*this)[thenExpr].Type, (*this)[elseExpr].Type, position);
    thenExpr = Convert(thenExpr, type, position);
    elseExpr = Convert(elseExpr, type, position);
    return Append({O::If, type, position, {condition, thenExpr, elseExpr}, {}, {}, 0});
}

TExprId TExpressionArena::Coalesce(std::span<const TExprId> arguments, TSourcePosition position) {
    if (arguments.empty()) {
        Fail(position, "COALESCE requires at least one argument");
    }
    auto type = (*this)[arguments.front()].Type;
    bool nullable = true;
    for (auto id : arguments) {
        const auto current = (*this)[id].Type;
        if (current.OptionalDepth > 1)
            throw TUnsupportedFeature(EFeature::Type, position, "Nested optional COALESCE requires explicit Unwrap");
        type = CommonType(type, current, position);
        nullable &= current.OptionalDepth || current.Kind == K::Null;
    }
    std::vector<TExprId> converted;
    converted.reserve(arguments.size());
    for (auto id : arguments) {
        auto target = type;
        const auto input = (*this)[id].Type;
        // Keep each alternative's nullability: lifting a required fallback
        // here loses that guarantee when a rewrite reconstructs Coalesce.
        target.OptionalDepth = input.OptionalDepth || input.Kind == K::Null;
        converted.push_back(Convert(id, target, position));
    }
    type.OptionalDepth = nullable;
    // Type-check and coerce every alternative first. A required first value
    // already determines the result; no runtime coalesce is needed or valid.
    if (!(*this)[converted.front()].Type.OptionalDepth && type.Kind != K::Null) return converted.front();
    return Append({O::Coalesce, type, position, std::move(converted), {}, {}, 0});
}

TExprId TExpressionArena::Ensure(TExprId value, TExprId predicate, std::string message, TSourcePosition position) {
    if ((*this)[predicate].Type != TType{K::Bool}) Fail(position, "Ensure requires a non-null Boolean predicate");
    return Append({O::Ensure, (*this)[value].Type, position, {value, predicate}, {}, std::move(message), 0});
}

TExprId TExpressionArena::Find(TExprId input, TExprId needle, TExprId offset, TSourcePosition position) {
    const auto type = (*this)[input].Type;
    if (!type.IsString() || (*this)[needle].Type != type.Required() || (*this)[offset].Type != TType{K::Uint32})
        Fail(position, "Find requires matching string types, a non-null needle and a non-null Uint32 offset");
    return Append({O::Find, {K::Uint32, true}, position, {input, needle, offset}, {}, {}, 0});
}

TExprId TExpressionArena::Like(TExprId input, TExprId pattern, std::string escape, TSourcePosition position, bool caseSensitive) {
    const auto inputType = (*this)[input].Type, patternType = (*this)[pattern].Type;
    if ((!inputType.IsString() && inputType.Kind != K::Null) || inputType.OptionalDepth > 1)
        Fail(position, "LIKE requires a string input");
    if (!patternType.IsString() || patternType.OptionalDepth)
        Fail(position, "LIKE requires a non-null string pattern");
    if (!escape.empty() && (escape.size() != 1 || static_cast<unsigned char>(escape[0]) >= 128 ||
        escape[0] == '%' || escape[0] == '_' || escape[0] == '\\'))
        Fail(position, "LIKE escape must be one ASCII character other than %, _ or backslash");
    input = Convert(input, {K::String, true}, position);
    pattern = Convert(pattern, {K::String}, position);
    return Append({caseSensitive ? O::Like : O::ILike, {K::Bool}, position, {input, pattern}, {}, std::move(escape), 0});
}

TExprId TExpressionArena::Regex(TExprId input, TExprId pattern, bool fullMatch, TSourcePosition position) {
    const auto inputType = (*this)[input].Type, patternType = (*this)[pattern].Type;
    if ((!inputType.IsString() && inputType.Kind != K::Null) || inputType.OptionalDepth > 1)
        Fail(position, "regular expression matching requires a string input");
    if (!patternType.IsString() || patternType.OptionalDepth)
        Fail(position, "regular expression matching requires a non-null string pattern");
    input = Convert(input, {K::String, true}, position);
    pattern = Convert(pattern, {K::String}, position);
    return Append({fullMatch ? O::RegexMatch : O::RegexGrep, {K::Bool}, position, {input, pattern}, {}, {}, 0});
}

TExprId TExpressionArena::JsonValue(TExprId input, std::string path, TSourcePosition position) {
    if (!(*this)[input].Type.IsJson()) Fail(position, "JSON_VALUE expects Json or JsonDocument");
    if (!IsUtf8(path)) Fail(position, "JSON path must be valid UTF-8");
    ValidateJsonSelector(path, position);
    return Append({O::JsonValue, {K::Utf8, true}, position, {input}, {}, std::move(path), 0});
}

TExprId TExpressionArena::Call(std::string_view name, std::span<const TExprId> arguments, TSourcePosition position) {
    const auto lowered = Lower(name);
    if (lowered == "listfromrange") {
        if (arguments.size() < 2 || arguments.size() > 3) Fail(position, "ListFromRange expects two or three arguments");
        auto bounds = CommonType((*this)[arguments[0]].Type, (*this)[arguments[1]].Type, position);
        if (bounds.Kind == K::Null && arguments.size() == 3)
            bounds = CommonType(bounds, (*this)[arguments[2]].Type, position);
        if (bounds.Kind == K::Null) return Null(position);
        if (!bounds.IsIntegral() || bounds.OptionalDepth > 1)
            throw TUnsupportedFeature(EFeature::Type, position, "native ListFromRange requires integral bounds");
        auto stepType = bounds.Required();
        if (!stepType.IsSigned()) stepType.Kind = static_cast<K>(static_cast<unsigned>(stepType.Kind) -
            (static_cast<unsigned>(K::Uint8) - static_cast<unsigned>(K::Int8)));
        const auto step = arguments.size() == 3 ? arguments[2] : Literal(stepType, std::int64_t{1}, position);
        const auto supplied = (*this)[step].Type;
        if (supplied.OptionalDepth > 1 || (!supplied.IsIntegral() && supplied.Kind != K::Null))
            Fail(position, "ListFromRange step requires an integer");
        stepType.OptionalDepth = supplied.OptionalDepth || supplied.Kind == K::Null;
        auto type = ListType(bounds.Required(), position);
        type.OptionalDepth = bounds.OptionalDepth || stepType.OptionalDepth;
        std::vector<TExprId> args{Convert(arguments[0], bounds, position), Convert(arguments[1], bounds, position),
            Convert(step, stepType, position)};
        return Append({O::ListFromRange, type, position, std::move(args), {}, {}, 0});
    }
    if (lowered == "listconcat") {
        if (arguments.empty() || arguments.size() > 2) Fail(position, "ListConcat expects one or two arguments");
        const auto list = arguments[0];
        const auto input = (*this)[list].Type;
        if (input.Kind == K::Null) return Null(position);
        if (input.Kind != K::List || input.OptionalDepth > 1)
            Fail(position, "ListConcat requires a flat list");
        const auto item = ListItem(input);
        if (item.Kind == K::Null) return Null(position);
        if (!item.IsString() || item.OptionalDepth > 1) Fail(position, "ListConcat requires String or Utf8 elements");
        auto separator = arguments.size() == 2 ? arguments[1] : Literal(item.Required(), std::string{}, position);
        auto separatorType = (*this)[separator].Type;
        if (separatorType.Kind == K::Null) {
            separator = Literal(item.Required(), std::string{}, position);
            separatorType = item.Required();
        }
        if (!separatorType.IsString() || separatorType.OptionalDepth > 1)
            Fail(position, "ListConcat separator requires String or Utf8");
        const auto result = CommonType(item, separatorType, position).Required();
        auto convertedItem = result; convertedItem.OptionalDepth = item.OptionalDepth;
        auto convertedList = ListType(convertedItem, position); convertedList.OptionalDepth = input.OptionalDepth;
        auto convertedSeparator = result; convertedSeparator.OptionalDepth = separatorType.OptionalDepth;
        std::vector<TExprId> args{Cast(list, convertedList, position), Convert(separator, convertedSeparator, position)};
        return Append({O::ListConcat, result.Optional(), position, std::move(args), {}, {}, 0});
    }
    if (lowered == "ensure") {
        if (arguments.size() < 2 || arguments.size() > 3) Fail(position, "Ensure expects two or three arguments");
        const auto predicate = (*this)[arguments[1]].Type;
        if (predicate.Kind != K::Bool || predicate.OptionalDepth > 1) Fail(position, "Ensure requires a Boolean predicate");
        std::string message;
        if (arguments.size() == 3) {
            const auto& text = (*this)[arguments[2]];
            if (text.Op != O::Literal || !text.Type.IsString() || text.Type.OptionalDepth)
                throw TUnsupportedFeature(EFeature::Expression, position, "Ensure message requires a string literal");
            message = std::get<std::string>(text.Value);
        }
        auto condition = arguments[1];
        if (predicate.OptionalDepth) {
            const TExprId alternatives[]{condition, Literal({K::Bool}, false, position)};
            condition = Coalesce(alternatives, position);
        }
        return Ensure(arguments[0], condition, std::move(message), position);
    }
    if (lowered == "randomuuid" || lowered == "randomnumber") {
        for (const auto argument : arguments) (void)(*this)[argument];
        const bool uuid = lowered == "randomuuid";
        return Append({uuid ? O::RandomUuid : O::RandomNumber, {uuid ? K::Uuid : K::Uint64},
            position, {arguments.begin(), arguments.end()}, {}, {}, 0});
    }
    if (lowered == "just") {
        if (arguments.size() != 1) Fail(position, "Just expects one argument");
        return Unary(O::OptionalWrap, arguments[0], position);
    }
    auto arity = [&](std::size_t expected) {
        if (arguments.size() != expected) {
            Fail(position, std::string(name) + " expects " + std::to_string(expected) + " arguments");
        }
    };
    if (lowered == "unwrap") {
        if (arguments.size() != 1 && arguments.size() != 2) Fail(position, "Unwrap expects one or two arguments");
        const auto type = (*this)[arguments[0]].Type;
        if (type.Kind == K::Null)
            throw TUnsupportedFeature(EFeature::Type, position, "Unwrap of bare NULL produces Void, outside the native scalar profile");
        // YQL ignores the message (including its type) for a required value.
        if (!type.OptionalDepth) return arguments[0];
        if (arguments.size() == 2) {
            const auto message = (*this)[arguments[1]].Type;
            if (!message.IsString() || message.OptionalDepth) Fail(position, "Unwrap message requires non-null String or Utf8");
        }
        return Append({O::Unwrap, type.Unwrapped(), position, {arguments.begin(), arguments.end()}, {}, {}, 0});
    }
    if (lowered == "math::abs") {
        arity(1);
        const auto type = (*this)[arguments[0]].Type;
        // Math::Abs has the runtime signature Double -> Double with AutoMap.
        return Unary(O::Abs, Convert(arguments[0], {K::Double, type.OptionalDepth || type.Kind == K::Null}, position), position);
    }
    if (lowered == "url::gethost" || lowered == "url::cutwww") {
        arity(1);
        return Unary(lowered == "url::gethost" ? O::UrlHost : O::UrlCutWWW, arguments[0], position);
    }
    if (lowered.starts_with("datetime::") || lowered.starts_with("datetime2::")) {
        const auto function = lowered.substr(lowered.find("::") + 2);
        if (function == "toseconds") { arity(1); return Unary(O::TemporalSeconds, arguments[0], position); }
        if (function == "intervalfromdays" || function == "intervalfromhours" || function == "intervalfromminutes") {
            arity(1);
            auto amount = arguments[0];
            const auto value = (*this)[amount];
            if (value.Type.Required() != TType{K::Int32}) {
                if (value.Op != O::Literal || !value.Type.IsIntegral() || value.Type.OptionalDepth)
                    throw TUnsupportedFeature(EFeature::Type, position, "native interval constructors require Int32 amounts or fitting integer literals");
                const auto number = IntegralValue({value.Type, value.Value});
                if (!number || !FitsSigned(*number, 32)) Fail(position, "interval amount must fit Int32");
                amount = Literal({K::Int32}, static_cast<std::int64_t>(*number), position);
            }
            const auto scale = function == "intervalfromdays" ? MicrosPerDay :
                function == "intervalfromhours" ? 3'600'000'000LL : 60'000'000LL;
            const auto wide = Convert(amount, {K::Int64, (*this)[amount].Type.OptionalDepth}, position);
            const auto factor = Literal({K::Int64}, static_cast<std::int64_t>(scale), position);
            // Runtime constructors multiply in their signed 64-bit layout,
            // then check the narrow interval range. Ordinary native operations
            // preserve that wrapping and NULL behavior without a resource/UDF.
            return Cast(Binary(O::Multiply, wide, factor, position), {K::Interval}, position);
        }
        auto calendar = [&](TExprId value) {
            if ((*this)[value].Type.Kind == K::Calendar) return value;
            return Unary(O::CalendarSplit, value, position);
        };
        if (function == "split") { arity(1); return Unary(O::CalendarSplit, arguments[0], position); }
        if (function == "shiftmonths" || function == "shiftyears") {
            arity(2);
            auto amount = arguments[1];
            const auto value = (*this)[amount];
            if (value.Type != TType{K::Int32}) {
                if (value.Op != O::Literal || !value.Type.IsIntegral() || value.Type.OptionalDepth)
                    throw TUnsupportedFeature(EFeature::Type, position, "native calendar shifts require Int32 amounts or fitting integer literals");
                const auto number = IntegralValue({value.Type, value.Value});
                if (!number || !FitsSigned(*number, 32)) Fail(position, "calendar shift amount must fit Int32");
                amount = Literal({K::Int32}, static_cast<std::int64_t>(*number), position);
            }
            return Binary(function == "shiftmonths" ? O::CalendarShiftMonths : O::CalendarShiftYears, calendar(arguments[0]), amount, position);
        }
        for (const auto& [functionName, op] : {std::pair{"makedate", O::CalendarDate}, {"makedatetime", O::CalendarDatetime},
            {"maketimestamp", O::CalendarTimestamp}, {"getyear", O::CalendarYear}, {"getminute", O::CalendarMinute}}) {
            if (function == functionName) { arity(1); return Unary(op, calendar(arguments[0]), position); }
        }
    }
    if (lowered == "decimal") {
        arity(3);
        const auto text = (*this)[arguments[0]];
        const auto precision = (*this)[arguments[1]], scale = (*this)[arguments[2]];
        if (text.Op != O::Literal || !text.Type.IsString() || text.Type.OptionalDepth ||
            precision.Op != O::Literal || !precision.Type.IsIntegral() || precision.Type.OptionalDepth ||
            scale.Op != O::Literal || !scale.Type.IsIntegral() || scale.Type.OptionalDepth)
            throw TUnsupportedFeature(EFeature::Expression, position, "Decimal constructor requires literal text, precision and scale");
        const auto p = *IntegralValue({precision.Type, precision.Value}), s = *IntegralValue({scale.Type, scale.Value});
        if (p < 1 || p > 35 || s < 0 || s > p) Fail(position, "Decimal requires 1 <= precision <= 35 and scale <= precision");
        const auto& value = std::get<std::string>(text.Value);
        if (!IsCanonicalDecimalText(value))
            throw TUnsupportedFeature(EFeature::Expression, position, "native Decimal text requires canonical digits or inf/nan");
        const TType type{K::Decimal, false, static_cast<std::uint8_t>(p), static_cast<std::uint8_t>(s)};
        return Literal(type, TDecimal{*ParseDecimal(value, type)}, position);
    }
    if (lowered == "coalesce") {
        return Coalesce(arguments, position);
    }
    if (lowered == "if") {
        if (arguments.size() == 2) {
            const auto condition = arguments[0], thenExpr = arguments[1];
            return If(condition, thenExpr, Null(position), position);
        }
        arity(3);
        return If(arguments[0], arguments[1], arguments[2], position);
    }
    if (lowered == "unicode::tolower" || lowered == "unicode::toupper") {
        arity(1);
        auto input = arguments[0];
        const auto& node = (*this)[input];
        if (node.Type.Kind == K::Null) input = Convert(input, {K::Utf8, true}, position);
        else if (node.Op == O::Literal && node.Type == TType{K::String} && IsUtf8(std::get<std::string>(node.Value)))
            input = Literal({K::Utf8}, node.Value, position);
        return Unary(lowered == "unicode::toupper" ? O::UnicodeUpper : O::UnicodeLower, input, position);
    }
    if (lowered == "math::isnan") {
        arity(1);
        const auto type = (*this)[arguments[0]].Type;
        if (type.Kind == K::Decimal || type.Kind == K::Null)
            throw TUnsupportedFeature(EFeature::Type, position, "Math::IsNaN requires a typed integer, Float or Double in the native profile");
        if (!type.IsNumeric()) Fail(position, "Math::IsNaN expects a numeric argument");
        const auto input = Convert(arguments[0], {K::Double, type.OptionalDepth}, position);
        // Scalar IEEE inequality is true only for NaN when both operands
        // are the same value. Reuse the node and preserve optional lifting.
        return Binary(O::NotEqual, input, input, position);
    }
    if (lowered == "minof" || lowered == "maxof" || lowered == "least" || lowered == "greatest") {
        if (arguments.empty()) Fail(position, "Scalar min/max requires at least one argument");
        auto type = (*this)[arguments[0]].Type;
        for (const auto id : arguments) type = CommonType(type, (*this)[id].Type, position);
        if (type.IsDocument()) Fail(position, "Scalar min/max requires comparable values");
        if (type.IsCollection() || type.Kind == K::Calendar)
            throw TUnsupportedFeature(EFeature::Type, position, "scalar min/max requires supported scalar values");
        auto result = Convert(arguments[0], type, position);
        const auto op = lowered == "minof" || lowered == "least" ? O::Minimum : O::Maximum;
        for (std::size_t i = 1; i < arguments.size(); ++i)
            result = Binary(op, result, Convert(arguments[i], type, position), position);
        return result;
    }
    for (const auto& [function, op] : {std::pair{"abs", O::Abs}, {"length", O::Length}, {"lower", O::Lower}, {"upper", O::Upper},
        {"listlength", O::ListLength}, {"listhasitems", O::ListHasItems},
        {"string::tolower", O::Lower}, {"string::toupper", O::Upper},
        {"string::asciitolower", O::Lower}, {"string::asciitoupper", O::Upper}}) {
        if (lowered == function) {
            arity(1);
            return Unary(op, arguments[0], position);
        }
    }
    struct TStringPredicate { std::string_view Name; O Op; bool IgnoreCase; };
    static constexpr TStringPredicate stringPredicates[]{
        {"string::contains", O::Contains, false}, {"string::startswith", O::StartsWith, false},
        {"string::endswith", O::EndsWith, false},
        {"string::_yql_asciiequalsignorecase", O::Equal, true},
        {"string::_yql_asciicontainsignorecase", O::Contains, true},
        {"string::_yql_asciistartswithignorecase", O::StartsWith, true},
        {"string::_yql_asciiendswithignorecase", O::EndsWith, true},
        {"string::asciiequalsignorecase", O::Equal, true},
        {"string::asciicontainsignorecase", O::Contains, true},
        {"string::asciistartswithignorecase", O::StartsWith, true},
        {"string::asciiendswithignorecase", O::EndsWith, true},
    };
    for (const auto& function : stringPredicates) if (lowered == function.Name) {
        arity(2);
        const auto leftType = (*this)[arguments[0]].Type, rightType = (*this)[arguments[1]].Type;
        if ((!leftType.IsString() && leftType.Kind != K::Null) || leftType.OptionalDepth > 1 ||
            !rightType.IsString() || rightType.OptionalDepth)
            Fail(position, "String predicate requires an optional string input and a required string pattern");
        auto left = Convert(arguments[0], {K::String, leftType.Kind == K::Null || bool(leftType.OptionalDepth)}, position);
        auto right = Convert(arguments[1], {K::String}, position);
        if (function.IgnoreCase) {
            left = Unary(O::Lower, left, position);
            right = Unary(O::Lower, right, position);
        }
        const TExprId values[]{Binary(function.Op, left, right, position), Literal({K::Bool}, false, position)};
        return Coalesce(values, position);
    }
    for (const auto& [function, op] : {std::pair{"contains", O::Contains}, {"startswith", O::StartsWith}, {"starts_with", O::StartsWith},
        {"endswith", O::EndsWith}, {"ends_with", O::EndsWith}, {"concat", O::Concat}}) {
        if (lowered == function) {
            arity(2);
            return Binary(op, arguments[0], arguments[1], position);
        }
    }
    if (lowered == "substring" || lowered == "substr") {
        if (arguments.size() != 2 && arguments.size() != 3) {
            Fail(position, "Substring expects two or three arguments");
        }
        const auto type = (*this)[arguments[0]].Type;
        if (type.Kind == K::Null) return arguments[0];
        if (type.Kind != K::String) Fail(position, "Substring expects String");
        std::vector<TExprId> args{arguments[0]};
        for (std::size_t i = 1; i != 3; ++i) {
            const std::uint64_t defaultValue = i == 1 ? 0 : std::numeric_limits<std::uint32_t>::max();
            if (i == arguments.size()) {
                args.push_back(Literal({K::Uint32}, defaultValue, position));
                continue;
            }
            const auto input = arguments[i];
            const auto node = (*this)[input];
            if (node.Type.Kind == K::Null || (node.Op == O::Literal && std::holds_alternative<std::monostate>(node.Value))) {
                args.push_back(Literal({K::Uint32}, defaultValue, position));
            } else if (node.Op == O::Literal && node.Type.IsIntegral()) {
                const auto number = IntegralValue({node.Type, node.Value});
                if (!number || !FitsUnsigned(*number, 32)) Fail(position, "Substring offset/length must fit Uint32");
                args.push_back(Literal({K::Uint32}, static_cast<std::uint64_t>(*number), position));
            } else if (node.Type.Kind == K::Uint8 || node.Type.Kind == K::Uint16 || node.Type.Kind == K::Uint32) {
                auto value = Convert(input, {K::Uint32, node.Type.OptionalDepth}, position);
                if (node.Type.OptionalDepth) {
                    const TExprId alternatives[]{value, Literal({K::Uint32}, defaultValue, position)};
                    value = Coalesce(alternatives, position);
                }
                args.push_back(value);
            } else if (node.Type.IsIntegral()) {
                throw TUnsupportedFeature(EFeature::Expression, position, "dynamic Substring offsets require Uint8/16/32");
            } else Fail(position, "Substring offsets must be integral");
        }
        return Append({O::Substring, type, position, std::move(args), {}, {}, 0});
    }
    std::optional<TType> castType;
    try {
        castType = ParseType(name, position);
    } catch (const TFrontendError&) {
    }
    if (castType && castType->Kind != K::Null) {
        arity(1);
        const auto source = (*this)[arguments[0]];
        if (source.Op != O::Literal || !source.Type.IsString())
            throw TUnsupportedFeature(EFeature::Expression, position, "native data constructors require a string literal");
        if (castType->IsDocument()) {
            if (!std::holds_alternative<std::string>(source.Value)) Fail(position, "Invalid " + castType->ToString() + " literal");
            return Literal(*castType, source.Value, position);
        }
        auto value = ConvertValue({source.Type, source.Value}, castType->Optional());
        if (value.IsNull()) Fail(position, "Invalid " + castType->ToString() + " literal");
        return Literal(castType->Required(), std::move(value.Data), position);
    }
    throw TUnsupportedFeature(EFeature::Function, position, "Unsupported scalar function: " + std::string(name));
}

TExprId TExpressionArena::Rebuild(TExprNode node) {
    switch (node.Op) {
        case O::ListFromRange: return Call("ListFromRange", node.Arguments, node.Position);
        case O::ListConcat: return Call("ListConcat", node.Arguments, node.Position);
        case O::RandomUuid: return Call("RandomUuid", node.Arguments, node.Position);
        case O::RandomNumber: return Call("RandomNumber", node.Arguments, node.Position);
        case O::Collection: {
            if (node.Type.Kind != K::Struct) return Collection(node.Arguments, node.Type.Kind == K::List, node.Position);
            std::vector<std::pair<std::string, TExprId>> fields;
            const auto names = StructNames(node.Type);
            for (std::size_t i = 0; i < names.size(); ++i) fields.emplace_back(names[i], node.Arguments[i]);
            return Struct(fields, node.Position);
        }
        case O::Member: return Member(node.Arguments[0], node.Name, node.Position);
        case O::TupleElement: return TupleElement(node.Arguments[0], std::get<std::uint64_t>(node.Value), node.Position);
        case O::ListElement: return ListElement(node.Arguments[0], node.Arguments[1], node.Position);
        case O::InCollection: return InCollection(node.Arguments[0], node.Arguments[1], node.Name == "ansi", node.Position);
        case O::InResult: return InResult(node.Arguments[0], node.Arguments[1], false, node.Position);
        case O::Literal: return Literal(node.Type, std::move(node.Value), node.Position);
        case O::Column: return Column(node.ColumnId, node.Type, node.Position);
        case O::Parameter: return Parameter(std::move(node.Name), node.Type, node.Position);
        case O::Convert:
            return Convert(node.Arguments[0], CommonType(node.Type, (*this)[node.Arguments[0]].Type, node.Position), node.Position);
        case O::Cast: return Cast(node.Arguments[0], node.Type, node.Position);
        case O::BitCast: return BitCast(node.Arguments[0], node.Type.Required(), node.Position);
        case O::If: return If(node.Arguments[0], node.Arguments[1], node.Arguments[2], node.Position);
        case O::Coalesce: return Coalesce(node.Arguments, node.Position);
        case O::Substring: return Call("Substring", node.Arguments, node.Position);
        case O::Ensure: return Ensure(node.Arguments[0], node.Arguments[1], std::move(node.Name), node.Position);
        case O::Unwrap: return Call("Unwrap", node.Arguments, node.Position);
        case O::Find: return Find(node.Arguments[0], node.Arguments[1], node.Arguments[2], node.Position);
        case O::JsonValue: return JsonValue(node.Arguments[0], std::move(node.Name), node.Position);
        case O::Like: case O::ILike:
            return Like(node.Arguments[0], node.Arguments[1], std::move(node.Name), node.Position, node.Op == O::Like);
        case O::RegexMatch: case O::RegexGrep:
            return Regex(node.Arguments[0], node.Arguments[1], node.Op == O::RegexMatch, node.Position);
        default:
            return node.Arguments.size() == 1 ? Unary(node.Op, node.Arguments[0], node.Position) :
                Binary(node.Op, node.Arguments[0], node.Arguments[1], node.Position);
    }
}

TExprId TExpressionArena::Import(const TExpressionArena& source, TExprId root,
    const std::function<TExprId(TColumnId, TType, TSourcePosition)>& bindColumn) {
    std::unordered_map<TExprId, TExprId> copied;
    std::vector<std::pair<TExprId, bool>> stack{{root, false}};
    while (!stack.empty()) {
        const auto [id, ready] = stack.back();
        stack.pop_back();
        if (copied.contains(id)) {
            continue;
        }
        // Copy because importing into this same arena may reallocate its storage.
        auto node = source[id];
        if (!ready) {
            stack.emplace_back(id, true);
            for (auto it = node.Arguments.rbegin(); it != node.Arguments.rend(); ++it) {
                if (!copied.contains(*it)) {
                    stack.emplace_back(*it, false);
                }
            }
            continue;
        }
        for (auto& arg : node.Arguments) {
            arg = copied.at(arg);
        }
        node.Type = ImportType(source, node.Type);
        if (&source != this && node.Op == O::Literal && node.Type.IsCollection()) {
            const auto copyValue = [&](auto&& self, TScalar data, TType type) -> TScalar {
                if (const auto* optional = std::get_if<TOptionalValue>(&data); optional && *optional)
                    return std::make_shared<const TValue>(TValue{type.Unwrapped(), self(self, (*optional)->Data, type.Unwrapped())});
                if (const auto* collection = std::get_if<TTuple>(&data); collection && *collection) {
                    auto items = (*collection)->Items;
                    for (std::size_t i = 0; i < items.size(); ++i) {
                        items[i].Type = type.Kind == K::List ? ListItem(type) : type.Kind == K::Struct ?
                            StructItems(type)[i] : TupleItems(type)[i];
                        items[i].Data = self(self, std::move(items[i].Data), items[i].Type);
                    }
                    return std::make_shared<const TTupleValue>(TTupleValue{std::move(items)});
                }
                return data;
            };
            node.Value = copyValue(copyValue, std::move(node.Value), node.Type);
        }
        const auto result = node.Op == O::Column ? bindColumn(node.ColumnId, node.Type, node.Position) : Rebuild(std::move(node));
        (void)(*this)[result];
        copied.emplace(id, result);
    }
    return copied.at(root);
}

TExprId TExpressionArena::RewriteColumns(TExprId root,
    const std::function<TExprId(TExprId, TColumnId, TType, TSourcePosition)>& bindColumn) {
    const auto& rootNode = (*this)[root];
    if (rootNode.Op == O::Column) {
        const auto replacement = bindColumn(root, rootNode.ColumnId, rootNode.Type, rootNode.Position);
        (void)(*this)[replacement];
        return replacement;
    }
    if (rootNode.Arguments.empty()) return root;
    return Rewrite(root, [&](TExprId id) {
        const auto& node = (*this)[id];
        return node.Op == O::Column ? bindColumn(id, node.ColumnId, node.Type, node.Position) : id;
    });
}

TExprId TExpressionArena::Rewrite(TExprId root, const std::function<TExprId(TExprId)>& replace) {
    std::unordered_map<TExprId, TExprId> rewritten;
    std::vector<std::pair<TExprId, bool>> stack{{root, false}};
    while (!stack.empty()) {
        const auto [id, ready] = stack.back();
        stack.pop_back();
        if (rewritten.contains(id)) continue;
        // The callback and rebuilding may append and reallocate this arena.
        auto node = (*this)[id];
        if (!ready) {
            const auto replacement = replace(id);
            (void)(*this)[replacement];
            if (replacement != id) {
                rewritten.emplace(id, replacement);
                continue;
            }
        }
        if (!ready && !node.Arguments.empty()) {
            stack.emplace_back(id, true);
            for (auto it = node.Arguments.rbegin(); it != node.Arguments.rend(); ++it) {
                if (!rewritten.contains(*it)) stack.emplace_back(*it, false);
            }
        } else {
            bool changed = false;
            for (auto& argument : node.Arguments) {
                const auto replacement = rewritten.at(argument);
                changed |= replacement != argument;
                argument = replacement;
            }
            rewritten.emplace(id, changed ? Rebuild(std::move(node)) : id);
        }
    }
    return rewritten.at(root);
}

TExprId TExpressionArena::FactorCommonConjuncts(TExprId root) {
    if (!(*this)[root].Repeatable) return root;
    const auto type = (*this)[root].Type;
    const auto pos = (*this)[root].Position;
    const auto unlift = [&](TExprId id) {
        while ((*this)[id].Op == O::Convert &&
            (*this)[id].Type.Required() == (*this)[(*this)[id].Arguments[0]].Type.Required())
            id = (*this)[id].Arguments[0];
        return id;
    };
    if ((*this)[unlift(root)].Op != O::Or) return root;

    // Intern structural identities locally, without changing the arena or
    // comparing whole subtrees repeatedly. Positions do not affect identity.
    std::unordered_map<TExprId, TExprId> classes;
    std::unordered_multimap<std::size_t, TExprId> representatives;
    std::vector<std::pair<TExprId, bool>> pending{{root, false}};
    while (!pending.empty()) {
        const auto [id, ready] = pending.back(); pending.pop_back();
        if (classes.contains(id)) continue;
        const auto& node = (*this)[id];
        // Factoring can move a check ahead of an earlier short-circuit guard.
        if (node.Op == O::Ensure || node.Op == O::Unwrap) return root;
        if (!ready) {
            pending.emplace_back(id, true);
            for (auto arg : node.Arguments) pending.emplace_back(arg, false);
            continue;
        }
        const auto childClass = [&](TExprId id) { return classes.at(id); };
        const auto hash = ExpressionHash(node, childClass);
        auto representative = id;
        const auto [begin, end] = representatives.equal_range(hash);
        for (auto it = begin; it != end; ++it) {
            if (SameExpression(node, (*this)[it->second], childClass)) {
                representative = it->second;
                break;
            }
        }
        classes.emplace(id, representative);
        if (representative == id) representatives.emplace(hash, id);
    }
    const auto split = [&](TExprId input, O op) {
        std::vector<TExprId> terms, stack{input};
        std::unordered_set<TExprId> visited;
        while (!stack.empty()) {
            const auto id = unlift(stack.back()); stack.pop_back();
            if (!visited.insert(id).second) continue;
            const auto& node = (*this)[id];
            if (node.Op == op) stack.insert(stack.end(), node.Arguments.rbegin(), node.Arguments.rend());
            else terms.push_back(id);
        }
        return terms;
    };
    const auto disjuncts = split(root, O::Or);
    if (disjuncts.size() == 1) return Convert(disjuncts.front(), type, pos);
    std::vector<std::vector<TExprId>> branches;
    std::unordered_map<TExprId, std::size_t> occurrences;
    for (auto branch : disjuncts) {
        auto& terms = branches.emplace_back();
        std::unordered_set<TExprId> seen;
        for (auto term : split(branch, O::And)) if (seen.insert(classes.at(term)).second) {
            terms.push_back(term);
            ++occurrences[classes.at(term)];
        }
    }
    std::vector<TExprId> common;
    std::unordered_set<TExprId> extracted;
    for (auto term : branches.front()) if (occurrences.at(classes.at(term)) == branches.size()) {
        common.push_back(term);
        extracted.insert(classes.at(term));
    }
    if (common.empty()) return root;
    const auto join = [&](O op, const std::vector<TExprId>& terms) {
        auto result = terms.front();
        for (std::size_t i = 1; i < terms.size(); ++i) result = Binary(op, result, terms[i], pos);
        return result;
    };
    std::vector<TExprId> residuals;
    for (const auto& branch : branches) {
        std::vector<TExprId> terms;
        for (auto term : branch) if (!extracted.contains(classes.at(term))) terms.push_back(term);
        if (terms.empty()) return Convert(join(O::And, common), type, pos);
        residuals.push_back(join(O::And, terms));
    }
    common.push_back(join(O::Or, residuals));
    return Convert(join(O::And, common), type, pos);
}

std::string FormatValue(const TValue& value) {
    if (const auto* optional = std::get_if<TOptionalValue>(&value.Data)) return "Just(" + FormatValue(**optional) + ")";
    if (value.IsNull()) {
        return "NULL";
    }
    if (auto p = std::get_if<std::string>(&value.Data)) {
        if (value.Type.Kind == K::Uuid) return FormatUuid(*p);
        return *p;
    }
    if (auto p = std::get_if<bool>(&value.Data)) {
        return *p ? "true" : "false";
    }
    if (auto p = std::get_if<std::int64_t>(&value.Data)) {
        return std::to_string(*p);
    }
    if (auto p = std::get_if<std::uint64_t>(&value.Data)) {
        return std::to_string(*p);
    }
    if (auto p = std::get_if<double>(&value.Data)) {
        std::ostringstream out;
        out.imbue(std::locale::classic());
        out.precision(std::numeric_limits<double>::max_digits10);
        out << *p;
        return out.str();
    }
    if (const auto* tuple = std::get_if<TTuple>(&value.Data)) {
        std::string result = "(";
        for (const auto& item : (*tuple)->Items) {
            if (result.size() != 1) result += ", ";
            result += FormatValue(item);
        }
        return result + ")";
    }
    const auto coefficient = std::get<TDecimal>(value.Data).Coefficient;
    if (coefficient == TDecimal::NaN()) return "nan";
    if (coefficient == TDecimal::Inf()) return "inf";
    if (coefficient == -TDecimal::Inf()) return "-inf";
    auto text = Int128ToString(coefficient < 0 ? -coefficient : coefficient);
    if (value.Type.Scale) {
        if (text.size() <= value.Type.Scale) {
            text.insert(std::size_t{0}, value.Type.Scale + 1 - text.size(), '0');
        }
        text.insert(text.size() - value.Type.Scale, 1, '.');
    }
    return (coefficient < 0 ? "-" : "") + text;
}

namespace {

// The scalar subset of the backend's stable encoding. Correlation domains use
// these bytes as null-safe keys. Keep this executable without a YQL dependency;
// backend lowering calls StablePickle directly, and parity tests check the wire
// format for every native type, including NaNs, signed zero and NULL.
std::string StablePickle(const TValue& value) {
    std::string payload;
    auto littleEndian = [&](std::uint64_t bits, unsigned bytes) {
        for (unsigned i = 0; i < bytes; ++i, bits >>= 8) payload.push_back(static_cast<char>(bits));
    };
    auto packedInteger = [&](std::uint64_t bits, unsigned width) {
        const unsigned maxBytes = width <= 32 ? 3 : 7;
        for (unsigned bytes = 1; bytes <= maxBytes; ++bytes) {
            if (bits < (std::uint64_t{1} << (7 * bytes))) {
                littleEndian((bits << bytes) | ((std::uint64_t{1} << (bytes - 1)) - 1), bytes);
                return;
            }
        }
        payload.push_back(static_cast<char>((1u << maxBytes) - 1));
        littleEndian(bits, width <= 32 ? 4 : 8);
    };
    if (!value.IsNull()) {
        const auto kind = value.Type.Kind;
        if (kind == K::Bool) {
            payload.push_back(std::get<bool>(value.Data));
        } else if (value.Type.IsIntegral() || IsDate(kind) || IsInterval(kind)) {
            const unsigned width = PayloadWidth(kind);
            const bool isSigned = IsSignedPayload(value.Type);
            auto bits = isSigned ? static_cast<std::uint64_t>(std::get<std::int64_t>(value.Data)) :
                std::get<std::uint64_t>(value.Data);
            if (width == 8) {
                littleEndian(bits, 1);
            } else {
                if (isSigned) bits = (bits << 1) ^ (std::uint64_t{0} - (bits >> 63));
                packedInteger(bits, width);
            }
        } else if (kind == K::Float || kind == K::Double) {
            const auto number = std::get<double>(value.Data);
            if (kind == K::Float) {
                littleEndian(std::isnan(number) ? 0x7fc00000u : number == 0 ? 0 :
                    std::bit_cast<std::uint32_t>(static_cast<float>(number)), 4);
            } else {
                littleEndian(std::isnan(number) ? 0x7ff8000000000000ULL : number == 0 ? 0 :
                    std::bit_cast<std::uint64_t>(number), 8);
            }
        } else if (kind == K::Decimal) {
            const auto coefficient = std::get<TDecimal>(value.Data).Coefficient;
            if (coefficient == TDecimal::NaN()) payload.push_back('\xff');
            else if (coefficient == TDecimal::Inf()) payload.push_back('\xfe');
            else if (coefficient == -TDecimal::Inf()) payload.push_back('\x01');
            else if (coefficient == -TDecimal::NaN()) payload.push_back('\x00');
            else {
                const auto bits = static_cast<unsigned __int128>(coefficient);
                const unsigned signByte = coefficient < 0 ? 0xff : 0;
                unsigned bytes = 15;
                while (bytes && ((bits >> (8 * (bytes - 1))) & 0xff) == signByte) --bytes;
                payload.push_back(static_cast<char>(coefficient < 0 ? 0x7f - bytes : 0x80 + bytes));
                while (bytes) payload.push_back(static_cast<char>(bits >> (8 * --bytes)));
            }
        } else if (value.Type.IsString() || kind == K::Uuid) {
            payload = std::get<std::string>(value.Data);
        } else {
            throw std::logic_error("invalid native stable key type");
        }
    }
    const auto size = payload.size();
    if (size > std::numeric_limits<std::uint32_t>::max())
        throw TResourceLimitError({}, "stable scalar encoding exceeds 4 GiB");
    if (size <= 7) {
        const auto empty = value.IsNull() && value.Type.Kind != K::Null;
        payload.insert(payload.begin(), static_cast<char>(1 | (size << 1) | (empty ? 0x10 : 0)));
    } else {
        payload.insert(payload.begin(), 4, '\0');
        for (unsigned i = 0; i < 4; ++i) payload[i] = static_cast<char>(size >> (8 * i));
    }
    return payload;
}

std::uint64_t IntegralRangeSize(const TValue& begin, const TValue& end, const TValue& increment) {
    if (begin.IsNull() || end.IsNull() || increment.IsNull()) return 0;
    const auto step = *IntegralValue(increment);
    const auto distance = step < 0 ? *IntegralValue(begin) - *IntegralValue(end) : *IntegralValue(end) - *IntegralValue(begin);
    if (!step || distance <= 0) return 0;
    const auto magnitude = step < 0 ? -step : step;
    return static_cast<std::uint64_t>(distance / magnitude + (distance % magnitude != 0));
}

bool HasNullValue(const TValue& value) {
    if (value.IsNull()) return true;
    if (const auto* optional = std::get_if<TOptionalValue>(&value.Data)) return HasNullValue(**optional);
    if (const auto* collection = std::get_if<TTuple>(&value.Data)) {
        for (const auto& item : (*collection)->Items) if (HasNullValue(item)) return true;
    }
    return false;
}

TValue Execute(const TExpressionArena& arena, const TExprNode& node, const std::vector<std::optional<TValue>>& values) {
    auto argument = [&](std::size_t index) -> const TValue& { return *values[node.Arguments[index]]; };
    if (node.Op == O::ListFromRange) {
        for (auto id : node.Arguments) if (values[id]->IsNull()) return NullValue(node.Type);
        const auto count = IntegralRangeSize(argument(0), argument(1), argument(2));
        // The reference executor materializes lists; physical execution keeps
        // ranges lazy. Constant folding checks its own smaller budget first.
        if (count > 1024 * 1024) throw TResourceLimitError(node.Position, "reference list evaluation exceeds one million elements");
        auto result = std::make_shared<TTupleValue>();
        result->Items.reserve(count);
        const auto item = arena.ListItem(node.Type);
        auto value = *IntegralValue(argument(0));
        const auto step = *IntegralValue(argument(2));
        for (std::uint64_t i = 0; i < count; ++i, value += step)
            result->Items.push_back({item, item.IsSigned() ? TScalar{static_cast<std::int64_t>(value)} : TScalar{static_cast<std::uint64_t>(value)}});
        return {node.Type, TTuple(std::move(result))};
    }
    if (node.Op == O::ListConcat) {
        if (argument(0).IsNull()) return NullValue(node.Type);
        std::optional<std::string> result;
        const auto separator = argument(1).IsNull() ? std::string_view{} : std::string_view(std::get<std::string>(argument(1).Data));
        for (const auto& item : std::get<TTuple>(argument(0).Data)->Items) {
            // Fold1 applies Concat(state, separator) before AggrConcat(item).
            // A NULL item can therefore leave a separator in a present state.
            if (result) *result += separator;
            if (!item.IsNull()) {
                if (!result) result.emplace();
                *result += std::get<std::string>(item.Data);
            }
        }
        return {node.Type, result ? TScalar{std::move(*result)} : TScalar{}};
    }
    if ((node.Op == O::Cast || node.Op == O::Convert) && node.Type.Kind == K::List) {
        if (argument(0).IsNull()) return NullValue(node.Type);
        const auto item = arena.ListItem(node.Type);
        auto result = std::make_shared<TTupleValue>();
        const auto& input = std::get<TTuple>(argument(0).Data)->Items;
        result->Items.reserve(input.size());
        for (const auto& value : input) {
            auto converted = ConvertValue(value, item);
            if (converted.IsNull() && !item.OptionalDepth) {
                continue;
            }
            result->Items.push_back(std::move(converted));
        }
        return {node.Type, TTuple(std::move(result))};
    }
    if (node.Op == O::Collection) {
        auto result = std::make_shared<TTupleValue>();
        for (auto id : node.Arguments) result->Items.push_back(*values[id]);
        return {node.Type, TTuple(std::move(result))};
    }
    if (node.Op == O::ListElement) {
        if (argument(0).IsNull() || argument(1).IsNull()) return NullValue(node.Type);
        const auto& items = std::get<TTuple>(argument(0).Data)->Items;
        const auto index = std::get<std::uint64_t>(argument(1).Data);
        if (index >= items.size()) return NullValue(node.Type);
        const auto& value = items[index];
        return {node.Type, node.Type.OptionalDepth > 1 ? TScalar{std::make_shared<const TValue>(value)} : value.Data};
    }
    if (node.Op == O::TupleElement || node.Op == O::Member) {
        const auto& tuple = argument(0);
        if (tuple.IsNull()) return {node.Type, {}};
        const auto& item = std::get<TTuple>(tuple.Data)->Items.at(std::get<std::uint64_t>(node.Value));
        return ConvertValue(item, node.Type);
    }
    if (node.Op == O::InCollection) {
        const auto& lookup = argument(0);
        const auto& items = std::get<TTuple>(argument(1).Data)->Items;
        const bool ansi = node.Name == "ansi";
        if (lookup.IsNull()) return ansi && items.empty() ? TValue{node.Type, false} : NullValue(node.Type);
        bool hasNull = false;
        for (const auto& item : items) {
            if (item.IsNull()) { hasNull = true; continue; }
            const auto key = ConvertValue(lookup, item.Type.Required());
            if (key.IsNull()) continue;
            // A runtime dictionary checks casts before lookup: fractions and
            // out-of-range integers cannot become matching truncated keys.
            if (lookup.Type.Kind == K::Double && item.Type.IsIntegral() &&
                *FloatingValue(key) != std::get<double>(lookup.Data)) continue;
            if (key.Data == item.Data) return {node.Type, true};
        }
        return ansi && hasNull ? NullValue(node.Type) : TValue{node.Type, false};
    }
    if (node.Op == O::OptionalWrap) {
        return {node.Type, node.Type.OptionalDepth > 1 ? TScalar{std::make_shared<const TValue>(argument(0))} : argument(0).Data};
    }
    if (node.Op == O::Unwrap) {
        if (argument(0).IsNull()) {
            std::string message = "Failed to unwrap empty optional";
            if (node.Arguments.size() == 2) {
                const auto& detail = std::get<std::string>(argument(1).Data);
                if (!detail.empty()) message += ":\n\n" + detail + "\n\n";
            }
            Fail(node.Position, std::move(message));
        }
        return argument(0).Type.OptionalDepth > 1 ? *std::get<TOptionalValue>(argument(0).Data) : TValue{node.Type, argument(0).Data};
    }
    if (node.Op == O::BitCast || (node.Op == O::Convert && argument(0).Type.IsIntegral() && node.Type.IsIntegral())) {
        const auto& value = argument(0);
        return value.IsNull() ? NullValue(node.Type) :
            TValue{node.Type, WrapInteger(static_cast<unsigned __int128>(*IntegralValue(value)), node.Type)};
    }
    if (node.Op == O::Cast || node.Op == O::Convert) {
        return ConvertValue(argument(0), node.Type);
    }
    if (node.Op == O::IsNull || node.Op == O::IsNotNull) {
        return {node.Type, argument(0).IsNull() == (node.Op == O::IsNull)};
    }
    if (node.Op == O::HasNull) return {node.Type, HasNullValue(argument(0))};
    if (node.Op == O::InResult)
        return HasNullValue(argument(0)) ? NullValue(node.Type) : TValue{node.Type, argument(1).Data};
    if (node.Op == O::StablePickle) {
        return {node.Type, StablePickle(argument(0))};
    }
    if (node.Op == O::IsNotDistinct) {
        const auto& left = argument(0);
        const auto& right = argument(1);
        if (left.IsNull() || right.IsNull()) return {node.Type, left.IsNull() && right.IsNull()};
        if (const auto* a = std::get_if<double>(&left.Data)) {
            const auto b = std::get<double>(right.Data);
            return {node.Type, *a == b || (std::isnan(*a) && std::isnan(b))};
        }
        return {node.Type, left.Data == right.Data};
    }
    for (auto id : node.Arguments) {
        if (values[id]->IsNull()) {
            return NullValue(node.Type);
        }
    }
    const auto& left = argument(0);
    if (node.Op == O::CalendarSplit)
        return {node.Type, std::get<std::uint64_t>(left.Data) * DateUnit(left.Type.Kind)};
    if (node.Op == O::CalendarDate || node.Op == O::CalendarDatetime || node.Op == O::CalendarTimestamp)
        return {node.Type, std::get<std::uint64_t>(left.Data) / DateUnit(node.Type.Kind)};
    if (node.Op == O::CalendarYear)
        return {node.Type, static_cast<std::uint64_t>(int(CalendarDate(std::get<std::uint64_t>(left.Data)).year()))};
    if (node.Op == O::CalendarMinute)
        return {node.Type, std::get<std::uint64_t>(left.Data) / 60'000'000 % 60};
    if (node.Op == O::TemporalSeconds) {
        if (left.Type.IsExtendedTemporal()) {
            const auto value = std::get<std::int64_t>(left.Data);
            return {node.Type, left.Type.Kind == K::Date32 ? value * 86400 :
                left.Type.Kind == K::Timestamp64 ? value / 1'000'000 : value};
        }
        const auto value = std::get<std::uint64_t>(left.Data);
        return {node.Type, std::uint64_t(std::uint32_t(left.Type.Kind == K::Date ? value * 86400 :
            left.Type.Kind == K::Timestamp ? value / 1'000'000 : value))};
    }
    if (node.Op == O::UrlHost || node.Op == O::UrlCutWWW) {
        const auto& text = std::get<std::string>(left.Data);
        const TStringBuf input(text.data(), text.size());
        const auto result = node.Op == O::UrlHost ? GetOnlyHost(input) : CutWWWPrefix(input);
        return result.empty() ? NullValue(node.Type) : TValue{node.Type, std::string(result.data(), result.size())};
    }
    if (node.Op == O::CalendarShiftMonths || node.Op == O::CalendarShiftYears) {
        const auto shifted = ShiftCalendar(std::get<std::uint64_t>(left.Data), std::get<std::int64_t>(argument(1).Data), node.Op == O::CalendarShiftYears);
        return shifted ? TValue{node.Type, *shifted} : NullValue(node.Type);
    }
    if (node.Op == O::Not) {
        return {node.Type, !std::get<bool>(left.Data)};
    }
    if (node.Op == O::BitNot) return {node.Type, WrapInteger(~std::get<std::uint64_t>(left.Data), node.Type)};
    if (node.Op == O::ShiftLeft || node.Op == O::ShiftRight) {
        const auto bits = std::get<std::uint64_t>(argument(1).Data);
        const auto input = std::get<std::uint64_t>(WrapInteger(static_cast<unsigned __int128>(*IntegralValue(left)), node.Type));
        return {node.Type, WrapInteger(bits >= static_cast<unsigned>(Width(node.Type.Kind)) ? 0 :
            node.Op == O::ShiftLeft ? input << bits : input >> bits, node.Type)};
    }
    if (node.Op == O::BitAnd || node.Op == O::BitOr || node.Op == O::BitXor) {
        const auto a = std::get<std::uint64_t>(left.Data), b = std::get<std::uint64_t>(argument(1).Data);
        return {node.Type, node.Op == O::BitAnd ? a & b : node.Op == O::BitOr ? a | b : a ^ b};
    }
    if (node.Op == O::Length) {
        return {node.Type, static_cast<std::uint64_t>(std::get<std::string>(left.Data).size())};
    }
    if (node.Op == O::ListLength || node.Op == O::ListHasItems) {
        const auto count = std::get<TTuple>(left.Data)->Items.size();
        return {node.Type, node.Op == O::ListLength ? TScalar{static_cast<std::uint64_t>(count)} : TScalar{count != 0}};
    }
    if (node.Op == O::UnicodeLower || node.Op == O::UnicodeUpper) {
        const auto& text = std::get<std::string>(left.Data);
        const TStringBuf input(text.data(), text.size());
        const auto result = node.Op == O::UnicodeUpper ? ToUpperUTF8(input) : ToLowerUTF8(input);
        return {node.Type, std::string(result.data(), result.size())};
    }
    if (node.Op == O::Lower || node.Op == O::Upper) {
        auto result = std::get<std::string>(left.Data);
        for (auto& ch : result) {
            if (node.Op == O::Lower && ch >= 'A' && ch <= 'Z') {
                ch += 'a' - 'A';
            } else if (node.Op == O::Upper && ch >= 'a' && ch <= 'z') {
                ch -= 'a' - 'A';
            }
        }
        return {node.Type, std::move(result)};
    }
    if (node.Op == O::Substring) {
        const auto& text = std::get<std::string>(left.Data);
        const auto offset = std::get<std::uint64_t>(argument(1).Data);
        const auto count = node.Arguments.size() == 3 ? std::get<std::uint64_t>(argument(2).Data) : text.size();
        return {node.Type, offset >= text.size() ? std::string{} : text.substr(offset, std::min<std::uint64_t>(count, text.size()))};
    }
    if (node.Op == O::Find) {
        const auto found = std::get<std::string>(left.Data).find(std::get<std::string>(argument(1).Data), std::get<std::uint64_t>(argument(2).Data));
        return found == std::string::npos ? NullValue(node.Type) : TValue{node.Type, static_cast<std::uint64_t>(found)};
    }
    if (node.Op == O::Negate || node.Op == O::Abs) {
        if (auto number = std::get_if<double>(&left.Data)) {
            return {node.Type, node.Op == O::Negate ? -*number : std::abs(*number)};
        }
        __int128 number = left.Type.Kind == K::Decimal ? std::get<TDecimal>(left.Data).Coefficient : *IntegralValue(left);
        if (left.Type.Kind == K::Decimal && number == TDecimal::NaN()) return {node.Type, TDecimal{number}};
        if (node.Op == O::Negate || number < 0) number = -number;
        if (node.Type.Kind == K::Decimal) {
            return {node.Type, TDecimal{number}};
        }
        return {node.Type, WrapInteger(static_cast<unsigned __int128>(number), node.Type)};
    }
    const auto& right = argument(1);
    if (node.Op == O::Minimum || node.Op == O::Maximum) {
        const bool minimum = node.Op == O::Minimum;
        if (const auto* a = std::get_if<double>(&left.Data)) {
            const auto b = std::get<double>(right.Data);
            return {node.Type, minimum ? std::fmin(*a, b) : std::fmax(*a, b)};
        }
        bool less;
        if (const auto* a = std::get_if<std::string>(&left.Data)) less = *a < std::get<std::string>(right.Data);
        else if (left.Type.Kind == K::Decimal) {
            const auto a = std::get<TDecimal>(left.Data).Coefficient, b = std::get<TDecimal>(right.Data).Coefficient;
            if (b == TDecimal::NaN()) return {node.Type, left.Data};
            if (a == TDecimal::NaN()) return {node.Type, right.Data};
            less = a < b;
        } else less = *IntegralValue(left) < *IntegralValue(right);
        return {node.Type, less == minimum ? left.Data : right.Data};
    }
    if ((node.Op == O::Add || node.Op == O::Subtract) && IsDate(left.Type.Kind)) {
        const __int128 a = *IntegralValue(left) * DateUnit(left.Type.Kind);
        const __int128 b = IsDate(right.Type.Kind) ? *IntegralValue(right) * DateUnit(right.Type.Kind) :
            std::get<std::int64_t>(right.Data);
        const __int128 result = node.Op == O::Add ? a + b : a - b;
        if (node.Type.IsInterval()) {
            return FitsTemporal(result, node.Type.Kind) ? TValue{node.Type, static_cast<std::int64_t>(result)} : NullValue(node.Type);
        }
        if (!FitsTemporal(result, node.Type.IsExtendedTemporal() ? K::Timestamp64 : K::Timestamp)) return NullValue(node.Type);
        return IsSignedPayload(node.Type) ? TValue{node.Type, static_cast<std::int64_t>(result / DateUnit(node.Type.Kind))} :
            TValue{node.Type, static_cast<std::uint64_t>(result / DateUnit(node.Type.Kind))};
    }
    if (node.Op == O::Concat || node.Op == O::Contains || node.Op == O::StartsWith || node.Op == O::EndsWith) {
        const auto& a = std::get<std::string>(left.Data);
        const auto& b = std::get<std::string>(right.Data);
        if (node.Op == O::Concat) {
            return {node.Type, a + b};
        }
        return {node.Type, node.Op == O::Contains ? a.find(b) != std::string::npos : node.Op == O::StartsWith ? a.starts_with(b) : a.ends_with(b)};
    }
    if (IsComparison(node.Op)) {
        if (left.Type.Kind == K::Decimal && (std::get<TDecimal>(left.Data).Coefficient == TDecimal::NaN() ||
            std::get<TDecimal>(right.Data).Coefficient == TDecimal::NaN())) return {node.Type, node.Op == O::NotEqual};
        int order = 0;
        if (left.Type.IsDate() && right.Type.IsDate()) {
            const __int128 a = *IntegralValue(left) * DateUnit(left.Type.Kind);
            const __int128 b = *IntegralValue(right) * DateUnit(right.Type.Kind);
            order = (a > b) - (a < b);
        } else if (auto a = std::get_if<std::string>(&left.Data)) {
            order = a->compare(std::get<std::string>(right.Data));
        } else if (auto a = std::get_if<double>(&left.Data)) {
            const auto b = std::get<double>(right.Data);
            const bool result = node.Op == O::Equal ? *a == b : node.Op == O::NotEqual ? *a != b :
                node.Op == O::Less ? *a < b : node.Op == O::LessEqual ? *a <= b : node.Op == O::Greater ? *a > b : *a >= b;
            return {node.Type, result};
        } else {
            const auto leftNumber = left.Type.Kind == K::Decimal ? std::get<TDecimal>(left.Data).Coefficient : *IntegralValue(left);
            const auto rightNumber = right.Type.Kind == K::Decimal ? std::get<TDecimal>(right.Data).Coefficient : *IntegralValue(right);
            if (left.Type.Kind == K::Decimal && right.Type.Kind == K::Decimal &&
                !IsDecimalSpecial(leftNumber) && !IsDecimalSpecial(rightNumber) && left.Type.Scale != right.Type.Scale) {
                // Divide the finer coefficient rather than multiplying the
                // coarser one: both precision-35 operands stay within Int128.
                const bool leftFine = left.Type.Scale > right.Type.Scale;
                const auto fine = leftFine ? leftNumber : rightNumber;
                const auto coarse = leftFine ? rightNumber : leftNumber;
                const auto divisor = Power10(leftFine ? left.Type.Scale - right.Type.Scale : right.Type.Scale - left.Type.Scale);
                const auto quotient = fine / divisor, remainder = fine % divisor;
                order = quotient == coarse ? (remainder > 0) - (remainder < 0) : (quotient > coarse) - (quotient < coarse);
                if (!leftFine) order = -order;
            } else order = (leftNumber > rightNumber) - (leftNumber < rightNumber);
        }
        const bool result = node.Op == O::Equal ? order == 0 : node.Op == O::NotEqual ? order != 0 :
            node.Op == O::Less ? order < 0 : node.Op == O::LessEqual ? order <= 0 : node.Op == O::Greater ? order > 0 : order >= 0;
        return {node.Type, result};
    }
    if (auto a = std::get_if<double>(&left.Data)) {
        const auto b = std::get<double>(right.Data);
        const auto result = node.Op == O::Add ? *a + b : node.Op == O::Subtract ? *a - b : node.Op == O::Multiply ? *a * b :
            node.Op == O::Modulo ? (node.Type.Kind == K::Float ? std::fmod(static_cast<float>(*a), static_cast<float>(b)) : std::fmod(*a, b)) : *a / b;
        return {node.Type, node.Type.Kind == K::Float ? static_cast<double>(static_cast<float>(result)) : result};
    }
    const bool decimal = left.Type.Kind == K::Decimal;
    const __int128 a = decimal ? std::get<TDecimal>(left.Data).Coefficient : *IntegralValue(left);
    const __int128 b = right.Type.Kind == K::Decimal ? std::get<TDecimal>(right.Data).Coefficient : *IntegralValue(right);
    if (!decimal && (node.Op == O::Divide || node.Op == O::Modulo) && b == 0) {
        return NullValue(node.Type);
    }
    if (!decimal) {
        if (node.Type.IsInterval()) {
            const __int128 result = node.Op == O::Add ? a + b : a - b;
            return FitsTemporal(result, node.Type.Kind) ? TValue{node.Type, static_cast<std::int64_t>(result)} : NullValue(node.Type);
        }
        const auto unsignedA = static_cast<unsigned __int128>(a);
        const auto unsignedB = static_cast<unsigned __int128>(b);
        if (node.Op == O::Add || node.Op == O::Subtract || node.Op == O::Multiply) {
            return {node.Type, WrapInteger(node.Op == O::Add ? unsignedA + unsignedB :
                node.Op == O::Subtract ? unsignedA - unsignedB : unsignedA * unsignedB, node.Type)};
        }
        const auto result = node.Op == O::Divide ? a / b : a % b;
        if (IsSignedPayload(node.Type) &&
            !FitsSigned(a / b, PayloadWidth(node.Type.Kind))) {
            return NullValue(node.Type);
        }
        return {node.Type, WrapInteger(static_cast<unsigned __int128>(result), node.Type)};
    }
    const auto nan = TDecimal::NaN(), inf = TDecimal::Inf();
    auto decimalResult = [&](const __int128 result) -> TValue { return {node.Type, TDecimal{result}}; };
    if (a == nan || b == nan) return decimalResult(nan);
    const bool specialA = IsDecimalSpecial(a), specialB = IsDecimalSpecial(b);
    const auto signedInf = (a < 0) != (b < 0) ? -inf : inf;
    if (node.Op == O::Add || node.Op == O::Subtract) {
        const auto result = node.Op == O::Add ? a + b : a - b;
        if (specialA || specialB) return decimalResult(!result ? nan : result < 0 ? -inf : inf);
        return decimalResult(BoundDecimal(result, node.Type));
    }
    if (node.Op == O::Multiply) {
        if (specialA || specialB) return decimalResult(!a || !b ? nan : signedInf);
        const auto result = DecimalProduct(a, b, right.Type.IsIntegral() ? 0 : node.Type.Scale);
        return decimalResult(result ? BoundDecimal(*result, node.Type) : signedInf);
    }
    if (node.Op == O::Divide) {
        if (!b) return decimalResult(!a ? nan : a < 0 ? -inf : inf);
        if (specialB) return decimalResult(specialA ? nan : 0);
        if (specialA) return decimalResult(signedInf);
        if (right.Type.IsIntegral()) {
            auto quotient = a / b;
            const auto remainder = a % b;
            const auto twiceRemainder = (remainder < 0 ? -remainder : remainder) * 2;
            const auto divisor = b < 0 ? -b : b;
            // Preserve the existing runtime's negative-integral-divisor quirk:
            // non-ties truncate, while exact ties still round to even.
            if ((b > 0 && twiceRemainder > divisor) || (twiceRemainder == divisor && quotient % 2))
                quotient += (a < 0) != (b < 0) ? -1 : 1;
            return decimalResult(quotient);
        }
        const auto result = RoundedDecimalQuotient(Int128ToString(a < 0 ? -a : a) + std::string(node.Type.Scale, '0'),
            b < 0 ? -b : b, (a < 0) != (b < 0));
        return decimalResult(result ? BoundDecimal(*result, node.Type) : signedInf);
    }
    if (node.Op == O::Modulo) return decimalResult(!b || specialA || specialB ? nan : a % b);
    Fail(node.Position, "Invalid arithmetic type");
}

} // namespace

TValue Evaluate(const TExpressionArena& arena, TExprId root, const TEvaluationContext& context) {
    // Validate the root before allocating the cache.
    (void)arena[root];
    std::vector<std::optional<TValue>> values(static_cast<std::size_t>(root) + 1);
    struct TFrame {
        TExprId Id;
        std::size_t Next = 0;
    };
    std::vector<TFrame> stack{{root}};
    while (!stack.empty()) {
        const auto id = stack.back().Id;
        const auto& node = arena[id];
        if (values[id]) {
            stack.pop_back();
            continue;
        }
        auto finish = [&](TValue value) {
            value.Type = node.Type;
            ValidateValue(arena, value, node.Position);
            if (node.Type.Kind == K::Float || node.Type.IsCollection())
                value.Data = NormalizeValue(node.Type, std::move(value.Data));
            values[id] = std::move(value);
            stack.pop_back();
        };
        if (node.Op == O::Literal) {
            finish({node.Type, node.Value});
            continue;
        }
        if (node.Op == O::Column || node.Op == O::Parameter) {
            if (node.Op == O::Column ? !context.Column : !context.Parameter) {
                Fail(node.Position, node.Op == O::Column ? "Missing column binding" : "Missing parameter binding: " + node.Name);
            }
            auto value = node.Op == O::Column ? context.Column(node.ColumnId) : context.Parameter(node.Name);
            ValidateValue(arena, value, node.Position);
            if (value.Type.Required() != node.Type.Required() || (value.IsNull() && !node.Type.OptionalDepth)) {
                Fail(node.Position, "Binding type differs from " + node.Type.ToString());
            }
            finish(std::move(value));
            continue;
        }
        if (node.Op == O::Unwrap) {
            const auto value = node.Arguments[0];
            if (!values[value]) stack.push_back({value});
            else if (!values[value]->IsNull()) finish(values[value]->Type.OptionalDepth > 1 ?
                *std::get<TOptionalValue>(values[value]->Data) : *values[value]);
            else if (node.Arguments.size() == 2 && !values[node.Arguments[1]]) stack.push_back({node.Arguments[1]});
            else finish(Execute(arena, node, values));
            continue;
        }
        if (node.Op == O::Ensure) {
            const auto predicate = node.Arguments[1], value = node.Arguments[0];
            if (!values[predicate]) { stack.push_back({predicate}); continue; }
            if (!std::get<bool>(values[predicate]->Data)) Fail(node.Position, node.Name);
            if (!values[value]) { stack.push_back({value}); continue; }
            finish(*values[value]);
            continue;
        }
        if (node.Op == O::InResult) {
            const auto lookup = node.Arguments[0], match = node.Arguments[1];
            if (!values[lookup]) { stack.push_back({lookup}); continue; }
            if (!stack.back().Next) {
                if (HasNullValue(*values[lookup])) { finish(NullValue(node.Type)); continue; }
                stack.back().Next = 1;
            }
            if (!values[match]) { stack.push_back({match}); continue; }
            finish(*values[match]);
            continue;
        }
        if (node.Op == O::If) {
            const auto condition = node.Arguments[0];
            if (!values[condition]) {
                stack.push_back({condition});
                continue;
            }
            const auto selected = node.Arguments[!values[condition]->IsNull() && std::get<bool>(values[condition]->Data) ? 1 : 2];
            if (!values[selected]) {
                stack.push_back({selected});
                continue;
            }
            finish(*values[selected]);
            continue;
        }
        if (node.Op == O::Coalesce) {
            const auto next = stack.back().Next;
            if (next == node.Arguments.size()) {
                finish(NullValue(node.Type));
                continue;
            }
            const auto arg = node.Arguments[next];
            if (!values[arg]) {
                stack.push_back({arg});
            } else if (!values[arg]->IsNull()) {
                finish(*values[arg]);
            } else {
                ++stack.back().Next;
            }
            continue;
        }
        if (node.Op == O::And || node.Op == O::Or) {
            const auto left = node.Arguments[0], right = node.Arguments[1];
            if (!values[left]) {
                stack.push_back({left});
                continue;
            }
            const bool decisive = node.Op == O::Or;
            if (!values[left]->IsNull() && std::get<bool>(values[left]->Data) == decisive) {
                finish({node.Type, decisive});
                continue;
            }
            if (!values[right]) {
                stack.push_back({right});
                continue;
            }
            if (!values[right]->IsNull() && std::get<bool>(values[right]->Data) == decisive) {
                finish({node.Type, decisive});
            } else if (values[left]->IsNull() || values[right]->IsNull()) {
                finish(NullValue(node.Type));
            } else {
                finish({node.Type, !decisive});
            }
            continue;
        }
        auto& frame = stack.back();
        while (frame.Next < node.Arguments.size() && values[node.Arguments[frame.Next]]) {
            ++frame.Next;
        }
        if (frame.Next < node.Arguments.size()) {
            stack.push_back({node.Arguments[frame.Next]});
            continue;
        }
        if (node.Op == O::JsonValue) {
            if (!context.JsonValue) Fail(node.Position, "JSON_VALUE reference execution requires a JSON runtime service");
            auto value = context.JsonValue(*values[node.Arguments[0]], node.Name);
            if (value.Type != node.Type) Fail(node.Position, "JSON_VALUE runtime returned an unexpected type");
            finish(std::move(value));
        } else if (node.Op == O::Like || node.Op == O::ILike) {
            if (!context.Like) Fail(node.Position, "LIKE reference execution requires a pattern matching service");
            finish({node.Type, context.Like(*values[node.Arguments[0]],
                std::get<std::string>(values[node.Arguments[1]]->Data), node.Name, node.Op == O::Like)});
        } else if (node.Op == O::RegexMatch || node.Op == O::RegexGrep) {
            if (!context.Regex) Fail(node.Position, "regular expression reference execution requires a matching service");
            finish({node.Type, context.Regex(*values[node.Arguments[0]],
                std::get<std::string>(values[node.Arguments[1]]->Data), node.Op == O::RegexMatch)});
        } else if (node.Op == O::RandomUuid) {
            if (!context.RandomUuid) Fail(node.Position, "Missing UUID generation service");
            finish({node.Type, context.RandomUuid()});
        } else if (node.Op == O::RandomNumber) {
            if (!context.RandomNumber) Fail(node.Position, "Missing random number generation service");
            finish({node.Type, context.RandomNumber()});
        } else finish(Execute(arena, node, values));
    }
    return *values[root];
}

std::vector<TExprId> TExpressionArena::FoldConstants(std::span<const TExprId> roots, std::size_t maxStringBytes) {
    struct TRewritten {
        TExprId Id;
        bool Total;
    };
    std::unordered_map<TExprId, TRewritten> rewritten;
    std::unordered_multimap<std::size_t, TExprId> representatives;
    std::vector<std::pair<TExprId, bool>> pending;
    // Restore shared scalar identity at the same boundary as folding, before
    // relational rules inspect expression DAGs. This does not evaluate runtime
    // operations or move them across guards; original roots remain immutable.
    const auto intern = [&](TExprId id) {
        const auto& node = (*this)[id];
        if (!node.Repeatable || node.Op == O::Ensure || node.Op == O::Unwrap) return id;
        const auto identity = [](TExprId child) { return child; };
        const auto hash = ExpressionHash(node, identity);
        const auto [begin, end] = representatives.equal_range(hash);
        for (auto it = begin; it != end; ++it)
            if (SameExpression(node, (*this)[it->second], identity)) return it->second;
        representatives.emplace(hash, id);
        return id;
    };
    // Conservative totality for this traversal only. Unknown/runtime operations
    // remain observable; errors and nondeterministic calls cannot be discarded
    // merely because another Boolean operand determines the result.
    const auto totalOperation = [](O op) {
        switch (op) {
            case O::Literal: case O::Column: case O::Parameter:
            case O::Negate: case O::Not: case O::Add: case O::Subtract: case O::Multiply: case O::Divide: case O::Modulo:
            case O::Equal: case O::NotEqual: case O::Less: case O::LessEqual: case O::Greater: case O::GreaterEqual:
            case O::And: case O::Or: case O::If: case O::Coalesce: case O::OptionalWrap: case O::InResult:
            case O::IsNull: case O::IsNotNull: case O::IsNotDistinct: case O::HasNull:
                return true;
            default: return false;
        }
    };
    auto fold = [&](TExprNode node, TExprId original, bool changed, bool firstTotal, bool total) {
        auto retain = [&] { return changed ? Rebuild(node) : original; };
        auto literal = [&](TScalar value) {
            auto remaining = maxStringBytes;
            if (!ChargeMaterializedBytes(value, node.Type, remaining)) return retain();
            maxStringBytes = remaining;
            return Literal(node.Type, std::move(value), node.Position);
        };
        if (node.Op == O::Literal || node.Op == O::Column || node.Op == O::Parameter) return original;
        // Path compilation and JSON evaluation belong to the execution runtime,
        // including invalid-path errors and NULL inputs. Never invoke it here.
        if (node.Op == O::JsonValue || node.Op == O::RandomUuid || node.Op == O::RandomNumber ||
            node.Op == O::Like || node.Op == O::ILike || node.Op == O::RegexMatch || node.Op == O::RegexGrep) return retain();
        if (node.Op == O::InResult && (*this)[node.Arguments[0]].Op == O::Literal) {
            const auto& lookup = (*this)[node.Arguments[0]];
            return HasNullValue({lookup.Type, lookup.Value}) ? literal(std::monostate{})
                : Convert(node.Arguments[1], node.Type, node.Position);
        }
        if (node.Op == O::If && (*this)[node.Arguments[0]].Op == O::Literal) {
            const auto& condition = (*this)[node.Arguments[0]].Value;
            return node.Arguments[std::holds_alternative<bool>(condition) && std::get<bool>(condition) ? 1 : 2];
        }
        // Ordinary comparisons are strict in NULL. Discard their other
        // operands only when this traversal has proved evaluation total;
        // checked and volatile operands must remain observable.
        if (total && IsComparison(node.Op)) {
            for (const auto arg : node.Arguments) {
                const auto& value = (*this)[arg];
                if (value.Op == O::Literal && std::holds_alternative<std::monostate>(value.Value))
                    return literal(std::monostate{});
            }
        }
        if (node.Op == O::Ensure) {
            const auto& condition = (*this)[node.Arguments[1]];
            // Failed assertions remain runtime errors, even inside dead branches.
            return condition.Op == O::Literal && std::get<bool>(condition.Value) ? node.Arguments[0] : retain();
        }
        if (node.Op == O::Unwrap) {
            const auto& value = (*this)[node.Arguments[0]];
            return value.Op == O::Literal && !std::holds_alternative<std::monostate>(value.Value) ?
                literal(value.Type.OptionalDepth > 1 ? std::get<TOptionalValue>(value.Value)->Data : value.Value) : retain();
        }
        if (node.Op == O::And || node.Op == O::Or) {
            const auto& left = (*this)[node.Arguments[0]];
            const auto& right = (*this)[node.Arguments[1]];
            const bool decisive = node.Op == O::Or;
            // Neutral literals do not affect NULLs, evaluation order or errors
            // in the other operand. A decisive right literal can discard its
            // left operand only when that evaluation is known to be total.
            if (right.Op == O::Literal && std::holds_alternative<bool>(right.Value) && std::get<bool>(right.Value) != decisive)
                return Convert(node.Arguments[0], node.Type, node.Position);
            if (left.Op == O::Literal && std::holds_alternative<bool>(left.Value) && std::get<bool>(left.Value) != decisive)
                return Convert(node.Arguments[1], node.Type, node.Position);
            if (firstTotal && right.Op == O::Literal && std::holds_alternative<bool>(right.Value) && std::get<bool>(right.Value) == decisive)
                return literal(decisive);
            if (left.Op != O::Literal) return retain();
            if (std::holds_alternative<bool>(left.Value) && std::get<bool>(left.Value) == decisive) return literal(decisive);
            if (right.Op != O::Literal) return retain();
            if (std::holds_alternative<bool>(right.Value) && std::get<bool>(right.Value) == decisive) return literal(decisive);
            return literal(std::holds_alternative<std::monostate>(left.Value) || std::holds_alternative<std::monostate>(right.Value) ?
                TScalar{std::monostate{}} : TScalar{!decisive});
        }
        if ((node.Op == O::IsNull || node.Op == O::IsNotNull) &&
            firstTotal && (*this)[node.Arguments[0]].Type.Kind != K::Null &&
            !(*this)[node.Arguments[0]].Type.OptionalDepth)
            return literal(node.Op == O::IsNotNull);
        if (node.Op == O::Coalesce) {
            for (const auto arg : node.Arguments) {
                const auto& value = (*this)[arg];
                if (value.Op != O::Literal) return retain();
                if (!std::holds_alternative<std::monostate>(value.Value))
                    return value.Type == node.Type ? arg : literal(value.Value);
            }
            return literal(std::monostate{});
        }
        auto inputBudget = maxStringBytes;
        for (const auto arg : node.Arguments) {
            const auto& value = (*this)[arg];
            if (value.Op != O::Literal) return retain();
            if (!ChargeMaterializedBytes(value.Value, value.Type, inputBudget)) return retain();
        }
        // Compact operands avoid allocating a root-sized evaluator cache for
        // every constant operation in a large/shared arena.
        std::vector<std::optional<TValue>> values;
        values.reserve(node.Arguments.size());
        auto executable = node;
        for (auto& arg : executable.Arguments) {
            const auto& value = (*this)[arg];
            values.push_back(TValue{value.Type, value.Value});
            arg = values.size() - 1;
        }
        if (node.Op == O::ListFromRange &&
            IntegralRangeSize(*values[0], *values[1], *values[2]) > maxStringBytes / sizeof(TValue)) return retain();
        if (node.Op == O::ListConcat && !values[0]->IsNull()) {
            const auto& items = std::get<TTuple>(values[0]->Data)->Items;
            const auto separator = values[1]->IsNull() ? 0 : std::get<std::string>(values[1]->Data).size();
            unsigned __int128 size = items.empty() ? 0 : static_cast<unsigned __int128>(items.size() - 1) * separator;
            for (const auto& item : items) if (!item.IsNull()) size += std::get<std::string>(item.Data).size();
            if (size > maxStringBytes) return retain();
        }
        auto value = Execute(*this, executable, values);
        return literal(std::move(value.Data));
    };
    for (auto root : roots) {
        pending.emplace_back(root, false);
        while (!pending.empty()) {
            const auto [id, ready] = pending.back();
            pending.pop_back();
            if (rewritten.contains(id)) continue;
            if (!ready) {
                const auto& node = (*this)[id];
                if (node.Op == O::Literal || node.Op == O::Column || node.Op == O::Parameter) {
                    rewritten.emplace(id, TRewritten{intern(id), true});
                    continue;
                }
                if (!node.Arguments.empty()) {
                    pending.emplace_back(id, true);
                    for (auto it = node.Arguments.rbegin(); it != node.Arguments.rend(); ++it)
                        if (!rewritten.contains(*it)) pending.emplace_back(*it, false);
                    continue;
                }
            }
            auto node = (*this)[id]; // folding may append and reallocate the arena
            bool changed = false;
            bool total = totalOperation(node.Op);
            const bool firstTotal = !node.Arguments.empty() && rewritten.at(node.Arguments.front()).Total;
            for (auto& arg : node.Arguments) {
                const auto replacement = rewritten.at(arg);
                total &= replacement.Total;
                changed |= replacement.Id != arg;
                arg = replacement.Id;
            }
            const auto result = intern(fold(std::move(node), id, changed, firstTotal, total));
            rewritten.emplace(id, TRewritten{result, total || (*this)[result].Op == O::Literal});
        }
    }
    std::vector<TExprId> result;
    result.reserve(roots.size());
    for (auto root : roots) result.push_back(rewritten.at(root).Id);
    return result;
}

} // namespace NKikimr::NKqp::NFrontend
