#include "codec.h"

#include <util/string/printf.h>

#include <cstring>
#include <limits>

namespace NKikimr::NSql {

namespace {

constexpr i32 kPgEpochDays = 10957;
constexpr i32 kMaxDateDays = 49672;
constexpr i64 kMaxDatetimeSeconds = (static_cast<i64>(kMaxDateDays) + 1) * 86400 - 1;
constexpr i64 kMaxTimestampMicros = (static_cast<i64>(kMaxDateDays) + 1) * 86400 * 1000000 - 1;

TCodecValue Fail(const char* sqlState, TString message) {
    TCodecValue value;
    value.SqlState = sqlState;
    value.Message = std::move(message);
    return value;
}

TCodecValue Unsupported() {
    return Fail("0A000", "not supported");
}

TCodecValue OkBytes(TString bytes) {
    TCodecValue value;
    value.Ok = true;
    value.Bytes = std::move(bytes);
    return value;
}

TCodecValue OkNull() {
    TCodecValue value;
    value.Ok = true;
    value.Null = true;
    return value;
}

template <class T>
T ReadNative(TStringBuf cell) {
    T value{};
    memcpy(&value, cell.data(), sizeof(T));
    return value;
}

template <class T>
TString WriteNative(T value) {
    TString bytes(sizeof(T), '\0');
    memcpy(bytes.begin(), &value, sizeof(T));
    return bytes;
}

i32 ReadBeI32(const char* data) {
    ui32 value = 0;
    for (int i = 0; i < 4; ++i) {
        value = (value << 8) | static_cast<ui8>(data[i]);
    }
    return static_cast<i32>(value);
}

i64 ReadBeI64(const char* data) {
    ui64 value = 0;
    for (int i = 0; i < 8; ++i) {
        value = (value << 8) | static_cast<ui8>(data[i]);
    }
    return static_cast<i64>(value);
}

void AppendBe32(TString& out, i32 value) {
    const ui32 bits = static_cast<ui32>(value);
    out.push_back(static_cast<char>(bits >> 24));
    out.push_back(static_cast<char>(bits >> 16));
    out.push_back(static_cast<char>(bits >> 8));
    out.push_back(static_cast<char>(bits));
}

void AppendBe64(TString& out, i64 value) {
    const ui64 bits = static_cast<ui64>(value);
    for (int i = 0; i < 8; ++i) {
        out.push_back(static_cast<char>(bits >> (8 * (7 - i))));
    }
}

enum class EKind {
    None,
    Date,
    Date32,
    Datetime,
    Datetime64,
    Timestamp,
    Timestamp64,
    Interval,
    Interval64,
};

EKind KindOf(NScheme::TTypeId type) {
    switch (type) {
        case NScheme::NTypeIds::Date:
            return EKind::Date;
        case NScheme::NTypeIds::Date32:
            return EKind::Date32;
        case NScheme::NTypeIds::Datetime:
            return EKind::Datetime;
        case NScheme::NTypeIds::Datetime64:
            return EKind::Datetime64;
        case NScheme::NTypeIds::Timestamp:
            return EKind::Timestamp;
        case NScheme::NTypeIds::Timestamp64:
            return EKind::Timestamp64;
        case NScheme::NTypeIds::Interval:
            return EKind::Interval;
        case NScheme::NTypeIds::Interval64:
            return EKind::Interval64;
        default:
            return EKind::None;
    }
}

size_t CellSize(EKind kind) {
    switch (kind) {
        case EKind::Date:
            return 2;
        case EKind::Date32:
        case EKind::Datetime:
            return 4;
        default:
            return 8;
    }
}

bool IsInterval(EKind kind) {
    return kind == EKind::Interval || kind == EKind::Interval64;
}

void CivilFromDays(i64 days, int& year, unsigned& month, unsigned& day) {
    days += 719468;
    const i64 era = (days >= 0 ? days : days - 146096) / 146097;
    const unsigned doe = static_cast<unsigned>(days - era * 146097);
    const unsigned yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    year = static_cast<int>(yoe) + static_cast<int>(era) * 400;
    const unsigned doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    const unsigned mp = (5 * doy + 2) / 153;
    day = doy - (153 * mp + 2) / 5 + 1;
    month = mp < 10 ? mp + 3 : mp - 9;
    year += month <= 2;
}

i64 DaysFromCivil(int year, unsigned month, unsigned day) {
    year -= month <= 2;
    const int era = (year >= 0 ? year : year - 399) / 400;
    const unsigned yoe = static_cast<unsigned>(year - era * 400);
    const unsigned doy = (153 * (month > 2 ? month - 3 : month + 9) + 2) / 5 + day - 1;
    const unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return static_cast<i64>(era) * 146097 + static_cast<i64>(doe) - 719468;
}

TCodecValue RangeError() {
    return Fail("22003", "time is out of range");
}

TCodecValue StoreDays(EKind kind, i64 days) {
    if (kind == EKind::Date) {
        if (days < 0 || days > kMaxDateDays) {
            return RangeError();
        }
        return OkBytes(WriteNative(static_cast<ui16>(days)));
    }
    if (days < std::numeric_limits<i32>::min() || days > std::numeric_limits<i32>::max()) {
        return RangeError();
    }
    return OkBytes(WriteNative(static_cast<i32>(days)));
}

TCodecValue StoreMicros(EKind kind, i64 micros) {
    if (kind == EKind::Timestamp && (micros < 0 || micros > kMaxTimestampMicros)) {
        return RangeError();
    }
    if (kind == EKind::Interval && (micros > kMaxTimestampMicros || micros < -kMaxTimestampMicros)) {
        return RangeError();
    }
    if (kind == EKind::Timestamp) {
        return OkBytes(WriteNative(static_cast<ui64>(micros)));
    }
    return OkBytes(WriteNative(micros));
}

TCodecValue StoreSeconds(EKind kind, i64 seconds) {
    if (kind == EKind::Datetime) {
        if (seconds < 0 || seconds > kMaxDatetimeSeconds) {
            return RangeError();
        }
        return OkBytes(WriteNative(static_cast<ui32>(seconds)));
    }
    return OkBytes(WriteNative(seconds));
}

i64 CellDays(EKind kind, TStringBuf cell) {
    if (kind == EKind::Date) {
        return ReadNative<ui16>(cell);
    }
    return ReadNative<i32>(cell);
}

i64 CellSeconds(EKind kind, TStringBuf cell) {
    if (kind == EKind::Datetime) {
        return ReadNative<ui32>(cell);
    }
    return ReadNative<i64>(cell);
}

i64 CellMicros(EKind kind, TStringBuf cell) {
    if (kind == EKind::Timestamp) {
        const ui64 value = ReadNative<ui64>(cell);
        return value > static_cast<ui64>(std::numeric_limits<i64>::max()) ? -1 : static_cast<i64>(value);
    }
    return ReadNative<i64>(cell);
}

TString FormatDate(i64 days) {
    int year = 0;
    unsigned month = 0;
    unsigned day = 0;
    CivilFromDays(days, year, month, day);
    return Sprintf("%04d-%02u-%02u", year, month, day);
}

TString FormatTimestamp(i64 micros) {
    const i64 seconds = micros / 1000000;
    i64 fraction = micros % 1000000;
    i64 whole = seconds;
    if (fraction < 0) {
        fraction += 1000000;
        --whole;
    }
    i64 dayIndex = 0;
    i64 secondOfDay = 0;
    if (whole >= 0) {
        dayIndex = whole / 86400;
        secondOfDay = whole % 86400;
    } else {
        dayIndex = (whole - 86399) / 86400;
        secondOfDay = whole - dayIndex * 86400;
    }
    const int hour = static_cast<int>(secondOfDay / 3600);
    const int minute = static_cast<int>((secondOfDay % 3600) / 60);
    const int second = static_cast<int>(secondOfDay % 60);
    return Sprintf("%s %02d:%02d:%02d.%06d", FormatDate(dayIndex).c_str(), hour, minute, second, static_cast<int>(fraction));
}

TString FormatInterval(i64 micros) {
    const bool negative = micros < 0;
    ui64 magnitude = negative ? static_cast<ui64>(-(micros + 1)) + 1 : static_cast<ui64>(micros);
    const ui64 hours = magnitude / 3600000000ull;
    magnitude %= 3600000000ull;
    const ui64 minutes = magnitude / 60000000ull;
    magnitude %= 60000000ull;
    const ui64 seconds = magnitude / 1000000ull;
    const ui64 fraction = magnitude % 1000000ull;
    return Sprintf("%s%llu:%02llu:%02llu.%06llu", negative ? "-" : "", hours, minutes, seconds, fraction);
}

bool ParseUnsigned(TStringBuf& text, ui64& value) {
    if (text.empty() || text[0] < '0' || text[0] > '9') {
        return false;
    }
    value = 0;
    while (!text.empty() && text[0] >= '0' && text[0] <= '9') {
        value = value * 10 + static_cast<ui64>(text[0] - '0');
        text.Skip(1);
    }
    return true;
}

TCodecValue ParseDate(TStringBuf text, EKind kind) {
    ui64 year = 0;
    ui64 month = 0;
    ui64 day = 0;
    if (!ParseUnsigned(text, year) || !text.SkipPrefix("-") || !ParseUnsigned(text, month) || !text.SkipPrefix("-")
        || !ParseUnsigned(text, day) || !text.empty() || month < 1 || month > 12 || day < 1 || day > 31)
    {
        return Fail("22007", "invalid date");
    }
    const i64 check = DaysFromCivil(static_cast<int>(year), static_cast<unsigned>(month), static_cast<unsigned>(day));
    int backYear = 0;
    unsigned backMonth = 0;
    unsigned backDay = 0;
    CivilFromDays(check, backYear, backMonth, backDay);
    if (backYear != static_cast<int>(year) || backMonth != month || backDay != day) {
        return Fail("22007", "invalid date");
    }
    return StoreDays(kind, check);
}

TCodecValue ParseTimestamp(TStringBuf text, EKind kind) {
    if (text.Contains('Z') || text.Contains('+')) {
        return Unsupported();
    }
    const size_t space = text.find(' ');
    if (space == TStringBuf::npos) {
        return Fail("22007", "invalid timestamp");
    }
    const TCodecValue date = ParseDate(text.substr(0, space), EKind::Date32);
    if (!date.Ok) {
        return date;
    }
    TStringBuf clock = text.substr(space + 1);
    ui64 hour = 0;
    ui64 minute = 0;
    ui64 second = 0;
    ui64 fraction = 0;
    if (!ParseUnsigned(clock, hour) || !clock.SkipPrefix(":") || !ParseUnsigned(clock, minute) || !clock.SkipPrefix(":")
        || !ParseUnsigned(clock, second) || hour > 23 || minute > 59 || second > 60)
    {
        return Fail("22007", "invalid timestamp");
    }
    if (clock.SkipPrefix(".")) {
        size_t digits = 0;
        while (!clock.empty() && clock[0] >= '0' && clock[0] <= '9' && digits < 6) {
            fraction = fraction * 10 + static_cast<ui64>(clock[0] - '0');
            clock.Skip(1);
            ++digits;
        }
        while (digits < 6) {
            fraction *= 10;
            ++digits;
        }
    }
    if (!clock.empty()) {
        return Fail("22007", "invalid timestamp");
    }
    const i32 days = ReadNative<i32>(date.Bytes);
    const i64 seconds = days * 86400 + static_cast<i64>(hour) * 3600 + static_cast<i64>(minute) * 60 + static_cast<i64>(second);
    if (kind == EKind::Datetime || kind == EKind::Datetime64) {
        return StoreSeconds(kind, seconds);
    }
    return StoreMicros(kind, seconds * 1000000 + static_cast<i64>(fraction));
}

TCodecValue ParseInterval(TStringBuf text, EKind kind) {
    if (text.Contains("day") || text.Contains("mon") || text.Contains("year")) {
        return Unsupported();
    }
    const bool negative = text.SkipPrefix("-");
    ui64 hours = 0;
    ui64 minutes = 0;
    ui64 seconds = 0;
    ui64 fraction = 0;
    if (!ParseUnsigned(text, hours) || !text.SkipPrefix(":") || !ParseUnsigned(text, minutes) || !text.SkipPrefix(":")
        || !ParseUnsigned(text, seconds) || minutes > 59 || seconds > 59)
    {
        return Fail("22007", "invalid interval");
    }
    if (text.SkipPrefix(".")) {
        size_t digits = 0;
        while (!text.empty() && text[0] >= '0' && text[0] <= '9' && digits < 6) {
            fraction = fraction * 10 + static_cast<ui64>(text[0] - '0');
            text.Skip(1);
            ++digits;
        }
        while (digits < 6) {
            fraction *= 10;
            ++digits;
        }
    }
    if (!text.empty()) {
        return Fail("22007", "invalid interval");
    }
    const ui64 magnitude = hours * 3600000000ull + minutes * 60000000ull + seconds * 1000000ull + fraction;
    if (magnitude > static_cast<ui64>(std::numeric_limits<i64>::max())) {
        return RangeError();
    }
    const i64 micros = negative ? -static_cast<i64>(magnitude) : static_cast<i64>(magnitude);
    return StoreMicros(kind, micros);
}

TCodecValue EncodeBinaryDate(i64 days) {
    TString out;
    AppendBe32(out, static_cast<i32>(days - kPgEpochDays));
    return OkBytes(std::move(out));
}

TCodecValue EncodeBinaryTimestamp(i64 micros) {
    TString out;
    AppendBe64(out, micros - static_cast<i64>(kPgEpochDays) * 86400 * 1000000);
    return OkBytes(std::move(out));
}

TCodecValue EncodeBinaryInterval(i64 micros) {
    TString out;
    AppendBe64(out, micros);
    AppendBe32(out, 0);
    AppendBe32(out, 0);
    return OkBytes(std::move(out));
}

} // namespace

TCodecValue EncodeTimeCell(NScheme::TTypeId type, bool null, TStringBuf cell, EWireFormat format) {
    const EKind kind = KindOf(type);
    if (kind == EKind::None) {
        return Unsupported();
    }
    if (null || cell.empty()) {
        return OkNull();
    }
    if (cell.size() != CellSize(kind)) {
        return Fail("22P03", "invalid cell");
    }
    if (IsInterval(kind)) {
        const i64 micros = ReadNative<i64>(cell);
        if (kind == EKind::Interval && (micros > kMaxTimestampMicros || micros < -kMaxTimestampMicros)) {
            return RangeError();
        }
        if (format == EWireFormat::Text) {
            return OkBytes(FormatInterval(micros));
        }
        return EncodeBinaryInterval(micros);
    }
    if (kind == EKind::Date || kind == EKind::Date32) {
        const i64 days = CellDays(kind, cell);
        if (kind == EKind::Date && days > kMaxDateDays) {
            return RangeError();
        }
        if (format == EWireFormat::Text) {
            return OkBytes(FormatDate(days));
        }
        return EncodeBinaryDate(days);
    }
    if (kind == EKind::Datetime || kind == EKind::Datetime64) {
        const i64 seconds = CellSeconds(kind, cell);
        if (kind == EKind::Datetime && static_cast<ui64>(seconds) > static_cast<ui64>(kMaxDatetimeSeconds) && seconds > kMaxDatetimeSeconds) {
            return RangeError();
        }
        if (format == EWireFormat::Text) {
            return OkBytes(FormatTimestamp(seconds * 1000000));
        }
        return EncodeBinaryTimestamp(seconds * 1000000);
    }
    const i64 micros = CellMicros(kind, cell);
    if (kind == EKind::Timestamp) {
        const ui64 stored = ReadNative<ui64>(cell);
        if (stored > static_cast<ui64>(kMaxTimestampMicros)) {
            return RangeError();
        }
    }
    if (format == EWireFormat::Text) {
        return OkBytes(FormatTimestamp(micros));
    }
    return EncodeBinaryTimestamp(micros);
}

TCodecValue DecodeTimeCell(NScheme::TTypeId type, bool null, TStringBuf wire, EWireFormat format) {
    const EKind kind = KindOf(type);
    if (kind == EKind::None) {
        return Unsupported();
    }
    if (null) {
        return OkNull();
    }
    if (format == EWireFormat::Text) {
        if (IsInterval(kind)) {
            return ParseInterval(wire, kind);
        }
        if (kind == EKind::Date || kind == EKind::Date32) {
            return ParseDate(wire, kind);
        }
        return ParseTimestamp(wire, kind);
    }
    if (IsInterval(kind)) {
        if (wire.size() != 16) {
            return Fail("22P03", "invalid interval");
        }
        const i64 micros = ReadBeI64(wire.data());
        const i32 days = ReadBeI32(wire.data() + 8);
        const i32 months = ReadBeI32(wire.data() + 12);
        if (days != 0 || months != 0) {
            return Unsupported();
        }
        return StoreMicros(kind, micros);
    }
    if (kind == EKind::Date || kind == EKind::Date32) {
        if (wire.size() != 4) {
            return Fail("22P03", "invalid date");
        }
        return StoreDays(kind, static_cast<i64>(ReadBeI32(wire.data())) + kPgEpochDays);
    }
    if (wire.size() != 8) {
        return Fail("22P03", "invalid timestamp");
    }
    const i64 unixMicros = ReadBeI64(wire.data()) + static_cast<i64>(kPgEpochDays) * 86400 * 1000000;
    if (kind == EKind::Datetime || kind == EKind::Datetime64) {
        if (unixMicros % 1000000 != 0) {
            return RangeError();
        }
        return StoreSeconds(kind, unixMicros / 1000000);
    }
    return StoreMicros(kind, unixMicros);
}

} // namespace NKikimr::NSql
