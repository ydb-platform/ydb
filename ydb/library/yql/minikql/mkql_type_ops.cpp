#include "mkql_type_ops.h"
#include "mkql_string_util.h"
#include "mkql_unboxed_value_stream.h"

#include <ydb/library/yql/minikql/dom/json.h>
#include <ydb/library/yql/minikql/dom/yson.h>
#include <ydb/library/yql/public/udf/tz/udf_tz.h>
#include <ydb/library/yql/utils/parse_double.h>
#include <ydb/library/yql/utils/swap_bytes.h>
#include <ydb/library/yql/utils/utf8.h>

#include <ydb/library/binary_json/write.h>
#include <ydb/library/binary_json/read.h>
#include <ydb/library/uuid/uuid.h>
#include <ydb/library/dynumber/dynumber.h>

#include <library/cpp/containers/stack_vector/stack_vec.h>

#include <library/cpp/yson/parser.h>
#include <library/cpp/yson/consumer.h>
#include <library/cpp/yson/varint.h>
#include <library/cpp/yson/detail.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/resource/resource.h>

#include <contrib/libs/cctz/include/cctz/civil_time.h>
#include <contrib/libs/cctz/include/cctz/time_zone.h>

#include <util/string/cast.h>
#include <util/string/ascii.h>
#include <util/string/split.h>
#include <util/stream/format.h>
#include <util/generic/singleton.h>
#include <util/system/unaligned_mem.h>
#include <array>
#include <functional>

namespace NKikimr {
namespace NMiniKQL {

using namespace NYql;

struct TTimezones {
    std::unordered_map<std::string_view, ui16> Name2Id;
    std::vector<std::optional<cctz::time_zone>> Zones;

    TTimezones() {
        NResource::TResources resList;
        const TStringBuf prefix = "/cctz/tzdata/";
        NResource::FindMatch(prefix, &resList);
        const auto allTimezones = NUdf::GetTimezones();
        for (ui16 id = 0; id < allTimezones.size(); ++id) {
            const auto& t = allTimezones[id];
            if (!t.empty()) {
                MKQL_ENSURE(Name2Id.emplace(t, id).second, "Duplicated zone: " << t);
            }
        }

        for (const auto& res : resList) {
            TStringBuf zoneBuf;
            MKQL_ENSURE(res.Key.AfterPrefix(prefix, zoneBuf), "Bad resource key: " << res.Key);
        }

        Zones.resize(allTimezones.size());
        for (const auto& zone : Name2Id) {
            auto& tz = Zones[zone.second];
            tz.emplace();
            MKQL_ENSURE(cctz::load_time_zone(std::string(zone.first), &*tz), "Failed to load zone: " << zone.first);
        }
    }

    const cctz::time_zone& GetZone(ui16 tzId) const {
        MKQL_ENSURE(tzId < Zones.size(), "Bad timezone id:" << tzId);
        return *Zones[tzId];
    }
};

bool IsValidDecimal(NUdf::TStringRef buf) {
    return NDecimal::IsValid(buf);
}

TStringBuf AdaptLegacyYqlType(const TStringBuf& type) {
    if (type == "ByteString") {
        return "String";
    }

    if (type == "Utf8String") {
        return "Utf8";
    }

    return type;
}

bool IsValidValue(NUdf::EDataSlot type, const NUdf::TUnboxedValuePod& value) {
    switch (type) {
    case NUdf::EDataSlot::Bool:
    case NUdf::EDataSlot::Int8:
    case NUdf::EDataSlot::Uint8:
    case NUdf::EDataSlot::Int16:
    case NUdf::EDataSlot::Uint16:
    case NUdf::EDataSlot::Int32:
    case NUdf::EDataSlot::Uint32:
    case NUdf::EDataSlot::Int64:
    case NUdf::EDataSlot::Uint64:
    case NUdf::EDataSlot::Float:
    case NUdf::EDataSlot::Double:
    case NUdf::EDataSlot::String:
        return bool(value);

    case NUdf::EDataSlot::Decimal:
        return bool(value) && !NYql::NDecimal::IsError(value.GetInt128());

    case NUdf::EDataSlot::Date:
        return bool(value) && value.Get<ui16>() < NUdf::MAX_DATE;

    case NUdf::EDataSlot::Datetime:
        return bool(value) && value.Get<ui32>() < NUdf::MAX_DATETIME;

    case NUdf::EDataSlot::Timestamp:
        return bool(value) && value.Get<ui64>() < NUdf::MAX_TIMESTAMP;

    case NUdf::EDataSlot::Interval:
        return bool(value) && (ui64)std::abs(value.Get<i64>()) < NUdf::MAX_TIMESTAMP;

    case NUdf::EDataSlot::Date32:
        return bool(value) && value.Get<i32>() >= NUdf::MIN_DATE32 && value.Get<i32>() <= NUdf::MAX_DATE32;

    case NUdf::EDataSlot::Datetime64:
        return bool(value) && value.Get<i64>() >= NUdf::MIN_DATETIME64 && value.Get<i64>() <= NUdf::MAX_DATETIME64;

    case NUdf::EDataSlot::Timestamp64:
        return bool(value) && value.Get<i64>() >= NUdf::MIN_TIMESTAMP64 && value.Get<i64>() <= NUdf::MAX_TIMESTAMP64;

    case NUdf::EDataSlot::Interval64:
        return bool(value) && (ui64)std::abs(value.Get<i64>()) <= NUdf::MAX_INTERVAL64;

    case NUdf::EDataSlot::TzDate:
        return bool(value) && value.Get<ui16>() < NUdf::MAX_DATE && value.GetTimezoneId() < NUdf::GetTimezones().size();

    case NUdf::EDataSlot::TzDatetime:
        return bool(value) && value.Get<ui32>() < NUdf::MAX_DATETIME && value.GetTimezoneId() < NUdf::GetTimezones().size();

    case NUdf::EDataSlot::TzTimestamp:
        return bool(value) && value.Get<ui64>() < NUdf::MAX_TIMESTAMP && value.GetTimezoneId() < NUdf::GetTimezones().size();

    case NUdf::EDataSlot::TzDate32:
        return bool(value) && value.Get<i32>() >= NUdf::MIN_DATE32 && value.Get<i32>() <= NUdf::MAX_DATE32 && value.GetTimezoneId() < NUdf::GetTimezones().size();

    case NUdf::EDataSlot::TzDatetime64:
        return bool(value) && value.Get<i64>() >= NUdf::MIN_DATETIME64 && value.Get<i64>() <= NUdf::MAX_DATETIME64 && value.GetTimezoneId() < NUdf::GetTimezones().size();

    case NUdf::EDataSlot::TzTimestamp64:
        return bool(value) && value.Get<i64>() >= NUdf::MIN_TIMESTAMP64 && value.Get<i64>() <= NUdf::MAX_TIMESTAMP64 && value.GetTimezoneId() < NUdf::GetTimezones().size();

    case NUdf::EDataSlot::Utf8:
        return bool(value) && IsUtf8(value.AsStringRef());
    case NUdf::EDataSlot::Yson:
         return bool(value) && NDom::IsValidYson(value.AsStringRef());
    case NUdf::EDataSlot::Json:
        return bool(value) && NDom::IsValidJson(value.AsStringRef());
    case NUdf::EDataSlot::Uuid:
        return bool(value) && value.AsStringRef().Size() == 16;
    case NUdf::EDataSlot::DyNumber:
        return NDyNumber::IsValidDyNumber(value.AsStringRef());
    case NUdf::EDataSlot::JsonDocument:
        return bool(value) && NKikimr::NBinaryJson::IsValidBinaryJson(value.AsStringRef());
    }
    MKQL_ENSURE(false, "Incorrect data slot: " << (ui32)type);
}

bool IsLeapYear(i32 year) {
    Y_ASSERT(year != 0);
    if (Y_UNLIKELY(year < 0)) {
        ++year;
    }
    bool isLeap = (year % 4 == 0);
    if (year % 100 == 0) {
        isLeap = year % 400 == 0;
    }
    return isLeap;
}

ui32 GetMonthLength(ui32 month, bool isLeap) {
    switch (month) {
    case 1: return 31;
    case 2: return isLeap ? 29 : 28;
    case 3: return 31;
    case 4: return 30;
    case 5: return 31;
    case 6: return 30;
    case 7: return 31;
    case 8: return 31;
    case 9: return 30;
    case 10: return 31;
    case 11: return 30;
    case 12: return 31;
    default:
        ythrow yexception() << "Unknown month: " << month;
    }
}

namespace {

ui32 LeapDaysSinceEpoch(ui32 yearsSinceEpoch) {
    ui32 leapDaysCount = (yearsSinceEpoch + 1) / 4;
    if (yearsSinceEpoch >= 131) {
        ui32 roundUpCentury = yearsSinceEpoch - 31;
        leapDaysCount -= roundUpCentury / 100;
        leapDaysCount += roundUpCentury / 400;
    }

    return leapDaysCount;
}

void WriteDate(IOutputStream& out, i32 year, ui32 month, ui32 day) {
    out << year << '-' << LeftPad(month, 2, '0') << '-' << LeftPad(day, 2, '0');
}

bool WriteDate(IOutputStream& out, ui16 value) {
    ui32 year, month, day;
    if (!SplitDate(value, year, month, day)) {
        return false;
    }

    WriteDate(out, year, month, day);
    return true;
}

bool WriteDate32(IOutputStream& out, i32 value) {
    i32 year;
    ui32 month, day;
    if (!SplitDate32(value, year, month, day)) {
        return false;
    }

    WriteDate(out, year, month, day);
    return true;
}

void SplitTime(ui32 value, ui32& hour, ui32& min, ui32& sec) {
    Y_ASSERT(value < 86400);
    hour = value / 3600;
    value -= hour * 3600;
    min = value / 60;
    sec = value - min * 60;
}

void WriteTime(IOutputStream& out, ui32 hour, ui32 min, ui32 sec) {
    out << LeftPad(hour, 2, '0') << ':' << LeftPad(min, 2, '0') << ':' << LeftPad(sec, 2, '0');
}

void WriteTime(IOutputStream& out, ui32 time) {
    ui32 hour, min, sec;
    SplitTime(time, hour, min, sec);
    WriteTime(out, hour, min, sec);
}

bool WriteDatetime(IOutputStream& out, ui32 value) {
    if (value >= NUdf::MAX_DATETIME) {
        return false;
    }

    const auto date = value / 86400u;
    value -= date * 86400u;
    if (!WriteDate(out, date)) {
        return false;
    }
    out << 'T';
    WriteTime(out, value);
    return true;
}

bool WriteDatetime64(IOutputStream& out, i64 value) {
    if (Y_UNLIKELY(NUdf::MIN_DATETIME64 > value || value > NUdf::MAX_DATETIME64)) {
        return false;
    }

    auto date = value / 86400;
    value -= date * 86400;
    if (value < 0) {
        date -= 1;
        value += 86400;
    }
    if (!WriteDate32(out, date)) {
        return false;
    }

    out << 'T';
    WriteTime(out, value);
    return true;
}

void WriteUs(IOutputStream& out, ui32 value) {
    if (value) {
        out << '.';
        out << LeftPad(value, 6, '0');
    }
}

bool WriteTimestamp(IOutputStream& out, ui64 value) {
    if (value >= NUdf::MAX_TIMESTAMP) {
        return false;
    }

    const auto date = value / 86400000000ull;
    value -= date * 86400000000ull;
    if (!WriteDate(out, date)) {
        return false;
    }
    out << 'T';
    const auto time = value / 1000000ull;
    value -= time * 1000000ull;
    WriteTime(out, time);
    WriteUs(out, value);
    return true;
}

bool WriteTimestamp64(IOutputStream& out, i64 value) {
    if (Y_UNLIKELY(NUdf::MIN_TIMESTAMP64 > value || value > NUdf::MAX_TIMESTAMP64)) {
        return false;
    }

    auto date = value / 86400000000ll;
    value -= date * 86400000000ll;
    if (value < 0) {
        date -= 1;
        value += 86400000000ll;
    }
    if (!WriteDate32(out, date)) {
        return false;
    }

    out << 'T';
    const auto time = value / 1000000ull;
    value -= time * 1000000ull;
    WriteTime(out, time);
    WriteUs(out, value);
    return true;
}

template <i64 UpperBound>
bool WriteInterval(IOutputStream& out, i64 signedValue) {
    ui64 value = signedValue < 0 ? -signedValue : signedValue;

    if (value > UpperBound) {
        return false;
    }

    if (signedValue < 0) {
        out << '-';
    }

    const auto days = value / 86400000000ull;
    value -= 86400000000ull * days;
    const auto hours = value / 3600000000ull;
    value -= 3600000000ull * hours;
    const auto minutes = value / 60000000ull;
    value -= 60000000ull * minutes;
    const auto seconds = value / 1000000ull;
    value -= 1000000ull * seconds;

    out << 'P';
    if (days) {
        out << days << 'D';
    }

    if (!days || hours || minutes || seconds || value) {
        out << 'T';
        if (hours) {
            out << hours << 'H';
        }

        if (minutes) {
            out << minutes << 'M';
        }

        if (seconds || value || !(days || hours || minutes)) {
            out << seconds;
            if (value) {
                out << '.';
                auto d = 6U;
                while (!(value % 10ull)) {
                    value /= 10ull;
                    --d;
                }
                out << LeftPad(value, d, '0');
            }
            out << 'S';
        }
    }
    return true;
}

i64 FromCctzYear(i64 value) {
    if (value <= 0) {
        return value - 1;
    }

    return value;
}

void ToLocalTime(ui32 utcSeconds, ui16 tzId, ui32& year, ui32& month, ui32& day, ui32& hour, ui32& min, ui32& sec) {
    const auto& tz = Singleton<TTimezones>()->GetZone(tzId);
    auto converted = cctz::convert(std::chrono::system_clock::from_time_t(utcSeconds), tz);
    year = converted.year();
    month = converted.month();
    day = converted.day();
    hour = converted.hour();
    min = converted.minute();
    sec = converted.second();
}

/*
ui32 FromLocalTime(ui16 tzId, ui32 year, ui32 month, ui32 day, ui32 hour, ui32 min, ui32 sec) {
    const auto& tz = Singleton<TTimezones>()->GetZone(tzId);
    cctz::civil_second cs(year, month, day, hour, min, sec);
    auto absoluteSeconds = std::chrono::system_clock::to_time_t(tz.lookup(cs).pre);
    if (absoluteSeconds < 0) {
        return 0;
    }

    if ((ui32)absoluteSeconds >= NUdf::MAX_DATETIME) {
        return NUdf::MAX_DATETIME - 1;
    }

    return absoluteSeconds;
}
*/

/*
void ToLocalTime64(i64 utcSeconds, ui16 tzId, ui32& year, ui32& month, ui32& day, ui32& hour, ui32& min, ui32& sec) {
    const auto& tz = Singleton<TTimezones>()->GetZone(tzId);
    auto converted = cctz::convert(std::chrono::system_clock::from_time_t(utcSeconds), tz);
    year = converted.year();
    month = converted.month();
    day = converted.day();
    hour = converted.hour();
    min = converted.minute();
    sec = converted.second();
}

i64 FromLocalTime64(ui16 tzId, i32 year, ui32 month, ui32 day, ui32 hour, ui32 min, ui32 sec) {
    const auto& tz = Singleton<TTimezones>()->GetZone(tzId);
    cctz::civil_second cs(year, month, day, hour, min, sec);
    return std::chrono::system_clock::to_time_t(tz.lookup(cs).pre);
}
*/
}

NUdf::TUnboxedValuePod ValueToString(NUdf::EDataSlot type, NUdf::TUnboxedValuePod value) {
    TUnboxedValueStream out;
    switch (type) {
    case NUdf::EDataSlot::Bool:
        out << (value.Get<bool>() ? "true" : "false");
        break;

    case NUdf::EDataSlot::Int8:
        out << i16(value.Get<i8>());
        break;

    case NUdf::EDataSlot::Uint8:
        out << ui16(value.Get<ui8>());
        break;

    case NUdf::EDataSlot::Int16:
        out << value.Get<i16>();
        break;

    case NUdf::EDataSlot::Uint16:
        out << value.Get<ui16>();
        break;

    case NUdf::EDataSlot::Int32:
        out << value.Get<i32>();
        break;

    case NUdf::EDataSlot::Uint32:
        out << value.Get<ui32>();
        break;

    case NUdf::EDataSlot::Int64:
        out << value.Get<i64>();
        break;

    case NUdf::EDataSlot::Uint64:
        out << value.Get<ui64>();
        break;

    case NUdf::EDataSlot::Float:
        out << ::FloatToString(value.Get<float>());
        break;

    case NUdf::EDataSlot::Double:
        out << ::FloatToString(value.Get<double>());
        break;

    case NUdf::EDataSlot::String:
    case NUdf::EDataSlot::Utf8:
    case NUdf::EDataSlot::Yson:
    case NUdf::EDataSlot::Json:
        return value;

    case NUdf::EDataSlot::Uuid: {
        ui16 dw[8];
        std::memcpy(dw, value.AsStringRef().Data(), sizeof(dw));
        NUuid::UuidToString(dw, out);
        break;
    }

    case NUdf::EDataSlot::Date:
        if (!WriteDate(out, value.Get<ui16>())) {
            return NUdf::TUnboxedValuePod();
        }
        break;

    case NUdf::EDataSlot::Datetime:
        if (!WriteDatetime(out, value.Get<ui32>())) {
            return NUdf::TUnboxedValuePod();
        }
        out << 'Z';
        break;

    case NUdf::EDataSlot::Timestamp:
        if (!WriteTimestamp(out, value.Get<ui64>())) {
            return NUdf::TUnboxedValuePod();
        }
        out << 'Z';
        break;

    case NUdf::EDataSlot::Interval:
        if (!WriteInterval<NUdf::MAX_TIMESTAMP - 1>(out, value.Get<i64>())) {
            return NUdf::TUnboxedValuePod();
        }
        break;

    case NUdf::EDataSlot::TzDate: {
        const auto& tz = Singleton<TTimezones>()->GetZone(value.GetTimezoneId());
        const ui32 seconds = 86400u * value.Get<ui16>() + (86400u - 1u);
        const auto converted = cctz::convert(std::chrono::system_clock::from_time_t(seconds), tz);
        WriteDate(out, converted.year(), converted.month(), converted.day());
        out << ',' << tz.name();
        break;
    }

    case NUdf::EDataSlot::TzDatetime: {
        const auto& tz = Singleton<TTimezones>()->GetZone(value.GetTimezoneId());
        const ui32 seconds = value.Get<ui32>();
        const auto converted = cctz::convert(std::chrono::system_clock::from_time_t(seconds), tz);
        WriteDate(out, converted.year(), converted.month(), converted.day());
        out << 'T';
        WriteTime(out, converted.hour(), converted.minute(), converted.second());
        out << ',' << tz.name();
        break;
    }

    case NUdf::EDataSlot::TzTimestamp: {
        const auto& tz = Singleton<TTimezones>()->GetZone(value.GetTimezoneId());
        const ui32 seconds = ui32(value.Get<ui64>() / 1000000u);
        const ui32 frac = ui32(value.Get<ui64>() % 1000000u);
        const auto converted = cctz::convert(std::chrono::system_clock::from_time_t(seconds), tz);
        WriteDate(out, converted.year(), converted.month(), converted.day());
        out << 'T';
        WriteTime(out, converted.hour(), converted.minute(), converted.second());
        WriteUs(out, frac);
        out << ',' << tz.name();
        break;
    }

    case NUdf::EDataSlot::Date32:
        if (!WriteDate32(out, value.Get<i32>())) {
            return NUdf::TUnboxedValuePod();
        }
        break;

    case NUdf::EDataSlot::Datetime64:
        if (!WriteDatetime64(out, value.Get<i64>())) {
            return NUdf::TUnboxedValuePod();
        }
        out << 'Z';
        break;

    case NUdf::EDataSlot::Timestamp64:
        if (!WriteTimestamp64(out, value.Get<i64>())) {
            return NUdf::TUnboxedValuePod();
        }
        out << 'Z';
        break;

    case NUdf::EDataSlot::Interval64:
        if (!WriteInterval<NUdf::MAX_INTERVAL64>(out, value.Get<i64>())) {
            return NUdf::TUnboxedValuePod();
        }
        break;

    case NUdf::EDataSlot::TzDate32: {
        const auto& tz = Singleton<TTimezones>()->GetZone(value.GetTimezoneId());
        const i64 seconds = 86400ull * value.Get<i32>() + (86400u - 1u);
        const auto converted = cctz::convert(std::chrono::system_clock::from_time_t(seconds), tz);
        WriteDate(out, FromCctzYear(converted.year()), converted.month(), converted.day());
        out << ',' << tz.name();
        break;
    }

    case NUdf::EDataSlot::TzDatetime64: {
        const auto& tz = Singleton<TTimezones>()->GetZone(value.GetTimezoneId());
        const i64 seconds = value.Get<i64>();
        const auto converted = cctz::convert(std::chrono::system_clock::from_time_t(seconds), tz);
        WriteDate(out, FromCctzYear(converted.year()), converted.month(), converted.day());
        out << 'T';
        WriteTime(out, converted.hour(), converted.minute(), converted.second());
        out << ',' << tz.name();
        break;
    }

    case NUdf::EDataSlot::TzTimestamp64: {
        const auto& tz = Singleton<TTimezones>()->GetZone(value.GetTimezoneId());
        i64 seconds = value.Get<i64>() / 1000000u;
        i32 frac = value.Get<i64>() % 1000000u;
        if (frac < 0) {
            frac += 1000000u;
            seconds -= 1;
        }

        const auto converted = cctz::convert(std::chrono::system_clock::from_time_t(seconds), tz);
        WriteDate(out, FromCctzYear(converted.year()), converted.month(), converted.day());
        out << 'T';
        WriteTime(out, converted.hour(), converted.minute(), converted.second());
        WriteUs(out, frac);
        out << ',' << tz.name();
        break;
    }

    case NUdf::EDataSlot::DyNumber: {
        out << NDyNumber::DyNumberToString(value.AsStringRef());
        break;
    }

    case NUdf::EDataSlot::JsonDocument: {
        out << NKikimr::NBinaryJson::SerializeToJson(value.AsStringRef());
        break;
    }

    case NUdf::EDataSlot::Decimal:
        THROW yexception() << "Decimal is unexpected";
    }

    return out.Value();
}

template <typename T>
bool IsValidNumberString(NUdf::TStringRef buf);

template <>
bool IsValidNumberString<float>(NUdf::TStringRef buf) {
    float value;
    return NYql::TryFloatFromString(buf, value);
}

template <>
bool IsValidNumberString<double>(NUdf::TStringRef buf) {
    double value;
    return NYql::TryDoubleFromString(buf, value);
}

template <>
bool IsValidNumberString<i8>(NUdf::TStringRef buf) {
    i16 value;
    return TryFromString(buf.Data(), buf.Size(), value) && value < 128 && value > -129;
}

template <>
bool IsValidNumberString<ui8>(NUdf::TStringRef buf) {
    ui16 value;
    return TryFromString(buf, value) && value < 256;
}

template <typename T>
bool IsValidNumberString(NUdf::TStringRef buf) {
    T value;
    return TryFromString(buf.Data(), buf.Size(), value);
}

template <typename T>
NUdf::TUnboxedValuePod NumberFromString(NUdf::TStringRef buf);

template <>
NUdf::TUnboxedValuePod NumberFromString<i8>(NUdf::TStringRef buf) {
    i16 value;
    if (!TryFromString(buf.Data(), buf.Size(), value) || value > 127 || value < -128) {
        return NUdf::TUnboxedValuePod();
    }
    return NUdf::TUnboxedValuePod(i8(value));
}

template <>
NUdf::TUnboxedValuePod NumberFromString<ui8>(NUdf::TStringRef buf) {
    ui16 value;
    if (!TryFromString(buf.Data(), buf.Size(), value) || value > 255) {
        return NUdf::TUnboxedValuePod();
    }
    return NUdf::TUnboxedValuePod(ui8(value));
}

template <>
NUdf::TUnboxedValuePod NumberFromString<float>(NUdf::TStringRef buf) {
    float value;
    if (!NYql::TryFloatFromString((TStringBuf)buf, value)) {
        return NUdf::TUnboxedValuePod();
    }

    return NUdf::TUnboxedValuePod(value);
}

template <>
NUdf::TUnboxedValuePod NumberFromString<double>(NUdf::TStringRef buf) {
    double value;
    if (!NYql::TryDoubleFromString((TStringBuf)buf, value)) {
        return NUdf::TUnboxedValuePod();
    }

    return NUdf::TUnboxedValuePod(value);
}

template <typename T>
NUdf::TUnboxedValuePod NumberFromString(NUdf::TStringRef buf) {
    T value;
    if (!TryFromString(buf.Data(), buf.Size(), value)) {
        return NUdf::TUnboxedValuePod();
    }

    return NUdf::TUnboxedValuePod(value);
}

bool MakeTime(ui32 hour, ui32 minute, ui32 second, ui32& value) {
    if (hour >= 24 || minute >= 60 || second >= 60) {
        return false;
    }

    value = hour * 3600 + minute * 60 + second;
    return true;
}

bool SplitDateUncached(ui16 value, ui32& year, ui32& month, ui32& day) {
    if (!value) {
        year = NUdf::MIN_YEAR - 1;
        month = 12;
        day = 31;
        return true;
    } else if (--value > NUdf::MAX_DATE) {
        return false;
    }

    ui32 approxYears = value / 365;
    ui32 remainDays = value - 365 * approxYears;
    ui32 leapDaysCount = LeapDaysSinceEpoch(approxYears);

    year = NUdf::MIN_YEAR + approxYears;
    if (remainDays < leapDaysCount) {
        --year;
    }

    const bool isLeap = IsLeapYear(year);
    if (remainDays < leapDaysCount) {
        remainDays += isLeap ? 366 : 365;
    }

    remainDays -= leapDaysCount;

    month = 1;
    day = 1;
    while (remainDays > 0) {
        ui32 monthLength = GetMonthLength(month, isLeap);
        if (remainDays < monthLength) {
            day = 1 + remainDays;
            break;
        }

        ++month;
        remainDays -= monthLength;
    }

    return true;
}

namespace {

constexpr i32 SOLAR_CYCLE_DAYS = 146097;
constexpr i32 SOLAR_CYCLE_YEARS = 400;

i32 UpdateBySolarCycleModulo2(i32 date, i32& solarCycles) {
    solarCycles = date / SOLAR_CYCLE_DAYS;
    date = date % SOLAR_CYCLE_DAYS;
    if (Y_UNLIKELY(date < 0)) {
        solarCycles -= 1;
        date += SOLAR_CYCLE_DAYS;
    }
    Y_ASSERT(0 <= date && date < SOLAR_CYCLE_DAYS);
    return date;
}

i32 UpdateBySolarCycleModulo(i32& date) {
    i32 solarCycles = date / SOLAR_CYCLE_DAYS;
    date = date % SOLAR_CYCLE_DAYS;
    if (Y_UNLIKELY(date < 0)) {
        solarCycles -= 1;
        date += SOLAR_CYCLE_DAYS;
    }
    Y_ASSERT(0 <= date && date < SOLAR_CYCLE_DAYS);
    return solarCycles;
}

class TDateTable {
public:
    TDateTable() {
        ui32 prevYear = NUdf::MIN_YEAR - 1;
        YearsOffsets_[0] = 0;

        ui32 dayOfYear = 365;
        ui32 dayOfWeek = 2;
        ui32 weekOfYear = 52;
        ui32 weekOfYearIso8601 = 1;

        Months_[0] = 0;
        LeapMonths_[0] = 0;
        ui16 monthDays = 0;
        ui16 leapMonthDays = 0;
        for (auto month = 1u; month < Months_.size(); ++month) {
            Months_[month] = monthDays;
            LeapMonths_[month] = leapMonthDays;
            monthDays += GetMonthLength(month, false);
            leapMonthDays += GetMonthLength(month, true);
        }

        for (ui16 date = 0; date < Days_.size(); ++date) {
            ui32 year, month, day;
            Y_ABORT_UNLESS(SplitDateUncached(date, year, month, day));

            ++dayOfYear;
            if (++dayOfWeek > 7) {
                dayOfWeek = 1;
                ++weekOfYear;
                if ((month == 12 && day >= 29) || (month == 1 && day <= 4)) {
                    weekOfYearIso8601 = 1;
                } else {
                    ++weekOfYearIso8601;
                }
            }

            if (year > prevYear) {
                YearsOffsets_[year - NUdf::MIN_YEAR] = date;
                prevYear = year;
                dayOfYear = 1;
                weekOfYear = 1;
            }

            Days_[date] = TDayInfo{month, day, dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek};
        }

        InitializeSolarCycle();
    }

    bool SplitDate(ui16 value, ui32& year, ui32& month, ui32& day) const {
        if (Y_UNLIKELY(++value >= Days_.size())) {
            return false;
        }

        year = NUdf::MIN_YEAR + std::distance(YearsOffsets_.cbegin(), std::upper_bound(YearsOffsets_.cbegin(), YearsOffsets_.cend(), value)) - 1;
        auto& info = Days_[value];
        month = info.Month;
        day = info.Day;
        return true;
    }

    void SplitDate32(i32 value, i32& year, ui32& month, ui32& day) const {
        i32 solarCycles;
        value = UpdateBySolarCycleModulo2(value, solarCycles);
        value = EnrichYear2(value, solarCycles, year);
        EnrichMonthDay(year, value, month, day);
    }

    void FullSplitDate32(i32 date, i32& year, ui32& month, ui32& day,
            ui32& dayOfYear, ui32& weekOfYear, ui32& weekOfYearIso8601, ui32& dayOfWeek) const
    {
        i32 solarCycles;
        auto solarDate = UpdateBySolarCycleModulo2(date, solarCycles);
        date = EnrichYear2(solarDate, solarCycles, year);
        EnrichMonthDay(year, date, month, day);

        auto cache = -1 + std::upper_bound(YearsCache_.cbegin(), YearsCache_.cend(), solarDate,
                [](ui32 value, const TYearCache& entry) {
                    return value < entry.CumulatveDays;
                });
        dayOfYear = 1 + date;
        dayOfWeek = 1 + (3 + solarDate) % 7;
        weekOfYear = (date + cache->WeekOffset) / 7;
        weekOfYearIso8601 = (date + cache->Iso8601WeekOffset) / 7;
        if (weekOfYearIso8601 == 0) {
            weekOfYearIso8601 = cache->FirstIsoWeek53 ? 53 : 52;
        } else if (weekOfYearIso8601 == 53 && cache->LastDayOfWeek < 3) {
                weekOfYearIso8601 = 1;
        }
    }

    void FullSplitTzDate32(i32 date, i32& year, ui32& month, ui32& day,
            ui32& dayOfYear, ui32& weekOfYear, ui32& weekOfYearIso8601, ui32& dayOfWeek, ui16 tzId) const
    {
        if (tzId == 0) {
            FullSplitDate32(date, year, month, day, dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek);
            return;
        }
    }

    bool GetDateOffset(ui32 year, ui32 month, ui32 day, ui16& value) const {
        if (Y_UNLIKELY(year < NUdf::MIN_YEAR - 1U || year > NUdf::MAX_YEAR
            || (year == NUdf::MAX_YEAR && (day > 1U || month > 1U))
            || (year == NUdf::MIN_YEAR - 1U && (day < 31U || month < 12U)))) {
            return false;
        }

        if (Y_UNLIKELY(year == NUdf::MIN_YEAR - 1U && day == 31U && month == 12U)) {
            value = 0;
            return true;
        }

        const auto ptr = YearsOffsets_.data() + year - NUdf::MIN_YEAR;
        const auto begin = Days_.cbegin() + ptr[0];
        const auto end = year == NUdf::MAX_YEAR ? Days_.cend() : Days_.cbegin() + ptr[1];

        // search valid month/day in this year
        const auto target = PackMonthDay(month, day);
        const auto it = std::lower_bound(begin, end, target, [](const TDayInfo& info, ui16 y) {
            return PackMonthDay(info.Month, info.Day) < y;
        });

        if (Y_UNLIKELY(it == end || PackMonthDay(it->Month, it->Day) != target)) {
            return false;
        }

        value = std::distance(Days_.cbegin(), it);
        return true;
    }

    bool MakeDate(ui32 year, ui32 month, ui32 day, ui16& value) const {
        if (Y_UNLIKELY(year < NUdf::MIN_YEAR || year >= NUdf::MAX_YEAR)) {
            return false;
        }

        if (Y_LIKELY(GetDateOffset(year, month, day, value) && value))
            --value;
        else
            return false;

        return true;
    }

    bool MakeDate32(i32 year, ui32 month, ui32 day, i32& value) const {
        if (Y_UNLIKELY(year == 0 || year < NUdf::MIN_YEAR32 || year >= NUdf::MAX_YEAR32)) {
            return false;
        }
        auto isLeap = IsLeapYear(year);
        auto monthLength = GetMonthLength(month, isLeap);

        if (Y_UNLIKELY(day < 1 || day > monthLength)) {
            return false;
        }

        if (Y_UNLIKELY(year < 0)) {
            year += 1;
        }
        year -= NUdf::MIN_YEAR;
        i32 val;
        if (Y_LIKELY(year%SOLAR_CYCLE_YEARS >= 0)) {
            val = (year / SOLAR_CYCLE_YEARS) * SOLAR_CYCLE_DAYS + Years_[year % SOLAR_CYCLE_YEARS];
        } else {
            val = (year / SOLAR_CYCLE_YEARS - 1) * SOLAR_CYCLE_DAYS + Years_[SOLAR_CYCLE_YEARS + year % SOLAR_CYCLE_YEARS];
        }
        val += isLeap ? LeapMonths_[month] : Months_[month];
        val += day - 1;
        value = val;
        return true;
    }

    bool EnrichByOffset(ui16 value, ui32& dayOfYear, ui32& weekOfYear, ui32& weekOfYearIso8601, ui32& dayOfWeek) const {
        if (Y_UNLIKELY(value >= Days_.size())) {
            return false;
        }

        auto& info = Days_[value];
        dayOfYear = info.DayOfYear;
        weekOfYear = info.WeekOfYear;
        weekOfYearIso8601 = info.WeekOfYearIso8601;
        dayOfWeek = info.DayOfWeek;
        return true;
    }

    // Seems it is not used
    // TODO remove from DateBuilder API in distinct PR
    bool EnrichDate(ui16 value, ui32& dayOfYear, ui32& weekOfYear, ui32& weekOfYearIso8601, ui32& dayOfWeek) const {
        return EnrichByOffset(++value, dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek);
    }

    void EnrichDate32(i32 date, ui32& dayOfYear, ui32& weekOfYear, ui32& weekOfYearIso8601, ui32& dayOfWeek) const {
        UpdateBySolarCycleModulo(date);
        auto& info = DaysCache_[date];
        dayOfYear = 1 + info.DayOfYear;
        weekOfYear = info.WeekOfYear;
        weekOfYearIso8601 = info.WeekOfYearIso8601;
        dayOfWeek = 1 + (3 + date) % 7;
    }

    static const TDateTable& Instance() {
        return *HugeSingleton<TDateTable>();
    }

    static ui16 PackMonthDay(ui32 month, ui32 day) {
        return (month << 8) | day;
    }

private:
    struct TDayInfo {
        ui32 Month : 4;
        ui32 Day : 5;
        ui32 DayOfYear : 9;
        ui32 WeekOfYear : 6;
        ui32 WeekOfYearIso8601: 6;
        ui32 DayOfWeek : 3;
    };

    struct TDayCache {
        ui32 Month : 4;
        ui32 Day : 5;
        ui32 DayOfYear : 9;
        ui32 WeekOfYear : 6;
        ui32 WeekOfYearIso8601: 6;
    };

    struct TYearCache {
        // TODO change to i32?
        ui32 CumulatveDays : 18; // max SOLAR_CYCLE_DAYS
        ui32 WeekOffset: 4;
        ui32 Iso8601WeekOffset: 4;
        ui32 LastDayOfWeek: 3;
        ui32 FirstIsoWeek53: 1;
    };

    std::array<ui16, NUdf::MAX_YEAR - NUdf::MIN_YEAR + 1> YearsOffsets_; // start of linear date for each year
    std::array<TDayInfo, NUdf::MAX_DATE + 2> Days_; // packed info for each date
    std::array<TDayCache, SOLAR_CYCLE_DAYS> DaysCache_; // packed info for each date in solar cycle
    std::array<ui32, SOLAR_CYCLE_YEARS> Years_; // start of linear date for each year in [1970, 2370] - solar cycle period
    std::array<TYearCache, SOLAR_CYCLE_YEARS> YearsCache_; // years cache for solar cycle period
    std::array<ui16, 13> Months_; // cumulative days count for months
    std::array<ui16, 13> LeapMonths_; // cumulative days count for months in a leap year

private:
    void EnrichMonthDay(i32 year, ui32 dayOfYear, ui32& month, ui32& day) const {
        auto& months = IsLeapYear(year) ? LeapMonths_ : Months_;
        auto m = std::upper_bound(months.cbegin() + 1, months.cend(), dayOfYear) - 1;
        Y_ASSERT(m >= months.cbegin());
        month = std::distance(months.cbegin(), m);
        day = 1 + dayOfYear - *m;
    }

    i32 EnrichYear2(i32 value, i32 solarCycles, i32& year) const {
        // TODO use YearsCache_ and return/update TYearCache
        auto y = std::upper_bound(Years_.cbegin(), Years_.cend(), value) - 1;
        value -= *y;
        year = NUdf::MIN_YEAR + SOLAR_CYCLE_YEARS * solarCycles + std::distance(Years_.cbegin(), y);
        if (Y_UNLIKELY(year <= 0)) {
            --year;
        }
        return value;
    }

    void EnrichYear(i32& value, i32 solarCycles, i32& year) const {
        auto y = std::upper_bound(Years_.cbegin(), Years_.cend(), value) - 1;
        value -= *y;
        year = NUdf::MIN_YEAR + SOLAR_CYCLE_YEARS * solarCycles + std::distance(Years_.cbegin(), y);
        if (Y_UNLIKELY(year <= 0)) {
            --year;
        }
    }

    void InitializeSolarCycle() {
        // starting from 1970-01-01
        ui32 date = 0u;
        ui32 dayOfWeek = 3;
        ui32 weekOfYearIso8601 = 1;
        for (auto yearIdx = 0u; yearIdx < Years_.size(); ++yearIdx) {
            Years_[yearIdx] = date;
            i32 year = yearIdx + NUdf::MIN_YEAR;
            auto daysInYear = IsLeapYear(year) ? 366u : 365u;
            auto lastDayOfWeek = (dayOfWeek + daysInYear - 1) % 7;
            YearsCache_[yearIdx] = TYearCache(date, 7 + dayOfWeek, (dayOfWeek >= 4) ? dayOfWeek : dayOfWeek + 7, lastDayOfWeek, weekOfYearIso8601 == 53);
// if (yearIdx <= 15) {
//     Cerr
//         << " year " << year
//         << " days " << YearsCache_[yearIdx].CumulatveDays
//         << " weekOffset " << YearsCache_[yearIdx].WeekOffset
//         << " isoWeekOffset " << YearsCache_[yearIdx].Iso8601WeekOffset
//         << " lastDayOfWeek " << YearsCache_[yearIdx].LastDayOfWeek
//         << " firstIsoWeek " << YearsCache_[yearIdx].FirstIsoWeek53
//         << Endl;
// }
            ui32 weekOfYear = 1;
            for (ui32 dayOfYear = 0; dayOfYear < daysInYear; ++dayOfYear) {
                ui32 month, day;
                EnrichMonthDay(year, dayOfYear, month, day);
                DaysCache_[date] = TDayCache(month, day, dayOfYear, weekOfYear, weekOfYearIso8601);

//if (date == 0) {
//    Cerr
//        << "date " << date
//        << " year " << year
//        << " Month " << DaysCache_[date].Month
//        << " Day " << DaysCache_[date].Day
//        << " DayOfYear " << DaysCache_[date].DayOfYear
//        << " WeekOfYear " << DaysCache_[date].WeekOfYear
//        << " WeekOfYearIso8601 " << DaysCache_[date].WeekOfYearIso8601
//        << " DayOfWeek " << DaysCache_[date].DayOfWeek
//        << Endl;
//}
                date++;
                if (dayOfWeek < 6) {
                    dayOfWeek++;
                } else {
                    dayOfWeek = 0;
                    weekOfYear++;
                    if ((month == 12 && day >= 28) || (month == 1 && day < 4)) {
                        weekOfYearIso8601 = 1;
                    } else {
                        ++weekOfYearIso8601;
                    }
                }
            }
        }
        Y_ASSERT(date == SOLAR_CYCLE_DAYS);
        Y_ASSERT(dayOfWeek == 3);
        Y_ASSERT(weekOfYearIso8601 == 1);
    }

};

}

bool SplitDate(ui16 value, ui32& year, ui32& month, ui32& day) {
    return TDateTable::Instance().SplitDate(value, year, month, day);
}

bool SplitDate32(i32 value, i32& year, ui32& month, ui32& day) {
    TDateTable::Instance().SplitDate32(value, year, month, day);
    return true;
}

bool MakeDate(ui32 year, ui32 month, ui32 day, ui16& value) {
    return TDateTable::Instance().MakeDate(year, month, day, value);
}

bool MakeDate32(i32 year, ui32 month, ui32 day, i32& value) {
    return TDateTable::Instance().MakeDate32(year, month, day, value);
}

bool SplitDatetime(ui32 value, ui32& year, ui32& month, ui32& day, ui32& hour, ui32& min, ui32& sec) {
    if (value >= NUdf::MAX_DATETIME) {
        return false;
    }

    auto date = value / 86400u;
    value -= date * 86400u;
    SplitDate(date, year, month, day);
    SplitTime(value, hour, min, sec);
    return true;
}

bool SplitTimestamp(ui64 value, ui32& year, ui32& month, ui32& day, ui32& hour, ui32& min, ui32& sec, ui32& usec) {
    if (value >= NUdf::MAX_TIMESTAMP) {
        return false;
    }

    auto date = value / 86400000000ull;
    value -= date * 86400000000ull;
    SplitDate(date, year, month, day);
    auto time = value / 1000000ull;
    value -= time * 1000000ull;
    SplitTime(time, hour, min, sec);
    usec = value;
    return true;
}

bool SplitInterval(i64 value, bool& sign, ui32& day, ui32& hour, ui32& min, ui32& sec, ui32& usec) {
    if (value >= (i64)NUdf::MAX_TIMESTAMP || value <= -(i64)NUdf::MAX_TIMESTAMP) {
        return false;
    }

    if (value < 0) {
        sign = true;
        value = -value;
    } else {
        sign = false;
    }

    ui64 posValue = value;
    day = posValue / 86400000000ull;
    posValue -= day * 86400000000ull;
    auto time = posValue / 1000000ull;
    posValue -= time * 1000000ull;
    SplitTime(time, hour, min, sec);
    usec = posValue;
    return true;
}

bool MakeTzDatetime(ui32 year, ui32 month, ui32 day, ui32 hour, ui32 min, ui32 sec, ui32& value, ui16 tzId) {
    if (tzId) {
        const auto& tz = Singleton<TTimezones>()->GetZone(tzId);
        cctz::civil_second cs(year, month, day, hour, min, sec);
        auto utcSeconds = cctz::TimePointToUnixSeconds(tz.lookup(cs).pre);
        if (utcSeconds < 0 || utcSeconds >= (std::int_fast64_t) NUdf::MAX_DATETIME) {
            return false;
        }

        value = (ui32)utcSeconds;
        return true;

    } else {
        ui16 date;
        ui32 time;
        if (!MakeDate(year, month, day, date)) {
            return false;
        }
        if (!MakeTime(hour, min, sec, time)) {
            return false;
        }
        value = date * 86400u + time;
        return true;
    }
}

bool SplitTzDatetime(ui32 value, ui32& year, ui32& month, ui32& day, ui32& hour, ui32& min, ui32& sec, ui16 tzId) {
    if (tzId) {
        if (value >= NUdf::MAX_DATETIME) {
            return false;
        }
        ToLocalTime(value, tzId, year, month, day, hour, min, sec);
        return true;
    } else {
        return SplitDatetime(value, year, month, day, hour, min, sec);
    }
}

bool SplitTzDate(ui16 value, ui32& year, ui32& month, ui32& day, ui32& dayOfYear, ui32& weekOfYear, ui32& weekOfYearIso8601, ui32& dayOfWeek, ui16 tzId) {
    if (tzId) {
        if (value >= NUdf::MAX_DATE) {
            return false;
        }

        ui32 hour, min, sec;
        ToLocalTime(86400u * ++value - 1u, tzId, year, month, day, hour, min, sec);
        if (!TDateTable::Instance().GetDateOffset(year, month, day, value)) {
            return false;
        }
    } else if (SplitDate(value, year, month, day)) {
        ++value;
    } else {
        return false;
    }
    return TDateTable::Instance().EnrichByOffset(value, dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek);
}

void FullSplitDate32(i32 value, i32& year, ui32& month, ui32& day, ui32& dayOfYear, ui32& weekOfYear, ui32& weekOfYearIso8601, ui32& dayOfWeek) {
    TDateTable::Instance().FullSplitDate32(value, year, month, day, dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek);
}

void FullSplitTzDate32(i32 value, i32& year, ui32& month, ui32& day, ui32& dayOfYear, ui32& weekOfYear, ui32& weekOfYearIso8601, ui32& dayOfWeek, ui16 tzId) {
    TDateTable::Instance().FullSplitTzDate32(value, year, month, day, dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek, tzId);
}

bool SplitTzDatetime(ui32 value, ui32& year, ui32& month, ui32& day, ui32& hour, ui32& min, ui32& sec, ui32& dayOfYear, ui32& weekOfYear, ui32& weekOfYearIso8601, ui32& dayOfWeek, ui16 tzId) {
    ui16 offset;
    if (tzId) {
        if (value >= NUdf::MAX_DATETIME) {
            return false;
        }
        ToLocalTime(value, tzId, year, month, day, hour, min, sec);
        if (!TDateTable::Instance().GetDateOffset(year, month, day, offset)) {
            return false;
        }
    } else if (SplitDatetime(value, year, month, day, hour, min, sec)) {
        offset = value / 86400u + 1u;
    } else {
        return false;
    }

    return TDateTable::Instance().EnrichByOffset(offset, dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek);
}

bool EnrichDate(ui16 date, ui32& dayOfYear, ui32& weekOfYear, ui32& weekOfYearIso8601, ui32& dayOfWeek) {
    return TDateTable::Instance().EnrichDate(date, dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek);
}

bool GetTimezoneShift(ui32 year, ui32 month, ui32 day, ui32 hour, ui32 min, ui32 sec, ui16 tzId, i32& value) {
    if (!tzId) {
        value = 0;
        return true;
    }

    const auto& tz = Singleton<TTimezones>()->GetZone(tzId);
    cctz::civil_second cs(year, month, day, hour, min, sec);
    value = tz.lookup(tz.lookup(cs).pre).offset / 60;
    return true;
}

namespace {

template <bool Big>
bool FromLocalTimeValidated(ui16 tzId, bool beforeChrist, ui32 year, ui32 month, ui32 day, ui32 hour, ui32 minute, ui32 second, i64& value) {
    if constexpr (Big) {
        if (!year) {
            return false;
        }

        if (beforeChrist) {
            if (year > ui32(-NUdf::MIN_YEAR32) + 1) {
                return false;
            }

            if (year == ui32(-NUdf::MIN_YEAR32) + 1 && (month != 12 || day != 31)) {
                return false;
            }
        } else {
            if (year > ui32(NUdf::MAX_YEAR32) + 1) {
                return false;
            }

            if (year == ui32(NUdf::MAX_YEAR32) + 1 && (month != 1 || day != 1)) {
                return false;
            }
        }
    } else {
        Y_ENSURE(!beforeChrist);
        if (year < NUdf::MIN_YEAR - 1 || year > NUdf::MAX_YEAR) {
            return false;
        } else if (year == NUdf::MIN_YEAR - 1) {
            if (month != 12 || day != 31) {
                return false;
            }
        } else if (year == NUdf::MAX_YEAR) {
            if (month != 1 || day != 1) {
                return false;
            }
        }
    }

    i64 cctzYear = year;
    if (beforeChrist) {
        cctzYear = -cctzYear + 1;
    }

    const auto& tz = Singleton<TTimezones>()->GetZone(tzId);

    cctz::civil_second sec(cctzYear, month, day, hour, minute, second);
    if (sec.year() != cctzYear || (ui32)sec.month() != month || (ui32)sec.day() != day ||
        (ui32)sec.hour() != hour || (ui32)sec.minute() != minute || (ui32)sec.second() != second) {
        // normalized
        return false;
    }

    const auto absoluteSeconds = std::chrono::system_clock::to_time_t(tz.lookup(sec).pre);
    if constexpr (Big) {
        if (absoluteSeconds < NUdf::MIN_DATETIME64 || absoluteSeconds > NUdf::MAX_DATETIME64) {
            return false;
        }
    } else {
        if (absoluteSeconds < 0 || (ui32)absoluteSeconds >= NUdf::MAX_DATETIME) {
            return false;
        }
    }

    value = absoluteSeconds;
    return true;
}

} // namespace

ui32 ParseNumber(ui32& pos, NUdf::TStringRef buf, ui32& value, i8 dig_cnt) {
    value = 0;
    ui32 count = 0;
    for (; dig_cnt && pos < buf.Size(); --dig_cnt, ++pos) {
        char c = buf.Data()[pos];
        if (c >= '0' && c <= '9') {
            value = value * 10 + (c - '0');
            ++count;
            continue;
        }

        break;
    }

    return count;
}

NUdf::TUnboxedValuePod ParseUuid(NUdf::TStringRef buf, bool shortForm) {
    ui16 dw[8];

    if (!NUuid::ParseUuidToArray(buf, dw, shortForm)) {
        return NUdf::TUnboxedValuePod();
    }

    return MakeString(NUdf::TStringRef(reinterpret_cast<char*>(dw), sizeof(dw)));
}

bool ParseUuid(NUdf::TStringRef buf, void* out, bool shortForm) {
    ui16 dw[8];

    if (!NUuid::ParseUuidToArray(buf, dw, shortForm)) {
        return false;
    }

    if (out) {
        std::memcpy(out, dw, sizeof(dw));
    }
    return true;
}

NUdf::TUnboxedValuePod ParseDate(NUdf::TStringRef buf) {
    ui32 year, month, day;
    ui32 pos = 0;
    if (!ParseNumber(pos, buf, year, 4) || pos == buf.Size() || buf.Data()[pos] != '-') {
        return NUdf::TUnboxedValuePod();
    }

    // skip '-'
    ++pos;
    if (!ParseNumber(pos, buf, month, 2) || pos == buf.Size() || buf.Data()[pos] != '-') {
        return NUdf::TUnboxedValuePod();
    }

    // skip '-'
    ++pos;
    if (!ParseNumber(pos, buf, day, 2) || pos != buf.Size()) {
        return NUdf::TUnboxedValuePod();
    }

    ui16 value;
    if (!MakeDate(year, month, day, value)) {
        return NUdf::TUnboxedValuePod();
    }

    if (value >= NUdf::MAX_DATE) {
        return NUdf::TUnboxedValuePod();
    }

    return NUdf::TUnboxedValuePod(value);
}

bool ParseDate32(ui32& pos, NUdf::TStringRef buf, i32& value) {
    ui32 year, month, day;
    bool beforeChrist = false;
    if (pos < buf.Size()) {
        char c = buf.Data()[pos];
        if (c == '-') {
            beforeChrist = true;
            ++pos;
        } else if (c == '+') {
            ++pos;
        }
    }

    if (!ParseNumber(pos, buf, year, 6) || pos == buf.Size() || buf.Data()[pos] != '-') {
        return false;
    }
    i32 iyear = beforeChrist ? -year : year;

    // skip '-'
    ++pos;
    if (!ParseNumber(pos, buf, month, 2) || pos == buf.Size() || buf.Data()[pos] != '-') {
        return false;
    }

    // skip '-'
    ++pos;
    if (!ParseNumber(pos, buf, day, 2)) {
        return false;
    }

    if (Y_LIKELY(MakeDate32(iyear, month, day, value))) {
        return true;
    }

    return false;
}

NUdf::TUnboxedValuePod ParseDate32(NUdf::TStringRef buf) {
    i32 value;
    ui32 pos = 0;
    if (Y_LIKELY(ParseDate32(pos, buf, value) && pos == buf.Size())) {
        return NUdf::TUnboxedValuePod(value);
    }
    return NUdf::TUnboxedValuePod();
}

template <bool Big>
NUdf::TUnboxedValuePod ParseTzDate(NUdf::TStringRef str) {
    TStringBuf full = str;
    TStringBuf buf;
    GetNext(full, ',', buf);
    const auto tzId = FindTimezoneId(full);
    if (!tzId) {
        return NUdf::TUnboxedValuePod();
    }

    ui32 year, month, day;
    ui32 pos = 0;
    bool beforeChrist = false;
    if (pos < buf.Size()) {
        char c = buf.Data()[pos];
        if (c == '-') {
            beforeChrist = true;
            ++pos;
        } else if (c == '+') {
            ++pos;
        }
    }

    if (!ParseNumber(pos, buf, year, Big ? 6 : 4) || pos == buf.size() || buf.data()[pos] != '-') {
        return NUdf::TUnboxedValuePod();
    }

    // skip '-'
    ++pos;
    if (!ParseNumber(pos, buf, month, 2) || pos == buf.size() || buf.data()[pos] != '-') {
        return NUdf::TUnboxedValuePod();
    }

    // skip '-'
    ++pos;
    if (!ParseNumber(pos, buf, day, 2) || pos != buf.size()) {
        return NUdf::TUnboxedValuePod();
    }

    i64 absoluteSeconds;
    if (!FromLocalTimeValidated<Big>(*tzId, beforeChrist, year, month, day, 0, 0, 0, absoluteSeconds)) {
        return NUdf::TUnboxedValuePod();
    }

    if constexpr (Big) {
        i32 value = (absoluteSeconds - ((absoluteSeconds < 0) ? (86400u-1) : 0)) / 86400u;
        NUdf::TUnboxedValuePod out(value);
        out.SetTimezoneId(*tzId);
        return out;
    } else {
        ui16 value = absoluteSeconds / 86400u;
        NUdf::TUnboxedValuePod out(value);
        out.SetTimezoneId(*tzId);
        return out;
    }
}

bool ParseTime(ui32& pos, NUdf::TStringRef buf, ui32& timeValue) {
    if (pos == buf.Size() || buf.Data()[pos] != 'T') {
        return false;
    }
    ui32 hour, minute, second;
    // skip 'T'
    ++pos;
    if (!ParseNumber(pos, buf, hour, 2) || pos == buf.Size() || buf.Data()[pos] != ':') {
        return false;
    }

    // skip ':'
    ++pos;
    if (!ParseNumber(pos, buf, minute, 2) || pos == buf.Size() || buf.Data()[pos] != ':') {
        return false;
    }

    // skip ':'
    ++pos;
    if (!ParseNumber(pos, buf, second, 2) || pos == buf.Size()) {
        return false;
    }

    if (!MakeTime(hour, minute, second, timeValue)) {
        return false;
    }
    return true;
}

bool ParseTimezoneOffset(ui32& pos, NUdf::TStringRef buf, i32& offset) {
    bool waiting_for_z = true;
    ui32 offset_hours = 0;
    ui32 offset_minutes = 0;
    bool is_offset_negative = false;

    if (buf.Data()[pos] == '+' || buf.Data()[pos] == '-') {
        is_offset_negative = buf.Data()[pos] == '-';

        // Skip sign
        ++pos;

        if (!ParseNumber(pos, buf, offset_hours, 2) ||
            pos == buf.Size() || buf.Data()[pos] != ':')
        {
            return false;
        }

        // Skip ':'
        ++pos;

        if (!ParseNumber(pos, buf, offset_minutes, 2) || pos != buf.Size()) {
            return false;
        }

        waiting_for_z = false;
    }

    if (waiting_for_z) {
        if (pos == buf.Size() || buf.Data()[pos] != 'Z') {
            return false;
        }

        // skip 'Z'
        ++pos;
    }

    ui32 offset_value = ((offset_hours) * 60 + offset_minutes) * 60;
    offset = is_offset_negative ? offset_value : -offset_value;
    return true;
}

NUdf::TUnboxedValuePod ParseDatetime64(NUdf::TStringRef buf) {
    i32 date;
    ui32 pos = 0;
    if (Y_UNLIKELY(!ParseDate32(pos, buf, date))) {
        return NUdf::TUnboxedValuePod();
    }

    ui32 time;
    if (Y_UNLIKELY(!ParseTime(pos, buf, time))) {
        return NUdf::TUnboxedValuePod();
    }

    i32 zoneOffset = 0;
    if (Y_UNLIKELY(!ParseTimezoneOffset(pos, buf, zoneOffset))) {
        return NUdf::TUnboxedValuePod();
    }
    if (Y_UNLIKELY(pos != buf.Size())) {
        return NUdf::TUnboxedValuePod();
    }
    i64 value = 86400;
    value *= date;
    value += time;
    value += zoneOffset;
    if (Y_UNLIKELY(NUdf::MIN_DATETIME64 > value || value > NUdf::MAX_DATETIME64)) {
        return NUdf::TUnboxedValuePod();
    }
    return NUdf::TUnboxedValuePod(value);
}

NUdf::TUnboxedValuePod ParseDatetime(NUdf::TStringRef buf) {
    ui32 year, month, day;
    ui32 pos = 0;
    if (!ParseNumber(pos, buf, year, 4) || pos == buf.Size() || buf.Data()[pos] != '-') {
        return NUdf::TUnboxedValuePod();
    }

    // skip '-'
    ++pos;
    if (!ParseNumber(pos, buf, month, 2) || pos == buf.Size() || buf.Data()[pos] != '-') {
        return NUdf::TUnboxedValuePod();
    }

    // skip '-'
    ++pos;
    if (!ParseNumber(pos, buf, day, 2) || pos == buf.Size() || buf.Data()[pos] != 'T') {
        return NUdf::TUnboxedValuePod();
    }

    ui16 dateValue;
    if (!MakeDate(year, month, day, dateValue)) {
        return NUdf::TUnboxedValuePod();
    }

    ui32 hour, minute, second;
    // skip 'T'
    ++pos;
    if (!ParseNumber(pos, buf, hour, 2) || pos == buf.Size() || buf.Data()[pos] != ':') {
        return NUdf::TUnboxedValuePod();
    }

    // skip ':'
    ++pos;
    if (!ParseNumber(pos, buf, minute, 2) || pos == buf.Size() || buf.Data()[pos] != ':') {
        return NUdf::TUnboxedValuePod();
    }

    // skip ':'
    ++pos;
    if (!ParseNumber(pos, buf, second, 2) || pos == buf.Size()) {
        return NUdf::TUnboxedValuePod();
    }

    bool waiting_for_z = true;
    
    ui32 offset_hours = 0;
    ui32 offset_minutes = 0;
    bool is_offset_negative = false;
    if (buf.Data()[pos] == '+' || buf.Data()[pos] == '-') {
        is_offset_negative = buf.Data()[pos] == '-';

        // Skip sign
        ++pos;

        if (!ParseNumber(pos, buf, offset_hours, 2) || 
            pos == buf.Size() || buf.Data()[pos] != ':')
        {
            return NUdf::TUnboxedValuePod();
        }
 
        // Skip ':'
        ++pos;

        if (!ParseNumber(pos, buf, offset_minutes, 2) || pos != buf.Size()) {
            return NUdf::TUnboxedValuePod();
        }

        waiting_for_z = false;
    }

    ui32 offset_value = ((offset_hours) * 60 + offset_minutes) * 60;

    if (waiting_for_z) {
        if (pos == buf.Size() || buf.Data()[pos] != 'Z') {
            return NUdf::TUnboxedValuePod();
        }

        // skip 'Z'
        ++pos;
        if (pos != buf.Size()) {
            return NUdf::TUnboxedValuePod();
        }
    }

    ui32 timeValue;
    if (!MakeTime(hour, minute, second, timeValue)) {
        return NUdf::TUnboxedValuePod();
    }

    ui32 value = dateValue * 86400u + timeValue;

    if (is_offset_negative) {
        if (UINT32_MAX - value < offset_value) {
            return NUdf::TUnboxedValuePod();
        }
        value += offset_value;
    } else {
        if (value < offset_value) {
            return NUdf::TUnboxedValuePod();
        }
        value -= offset_value;
    }

    if (value >= NUdf::MAX_DATETIME) {
        return NUdf::TUnboxedValuePod();
    }

    return NUdf::TUnboxedValuePod(value);
}

template <bool Big>
NUdf::TUnboxedValuePod ParseTzDatetime(NUdf::TStringRef str) {
    TStringBuf full = str;
    TStringBuf buf;
    GetNext(full, ',', buf);
    const auto tzId = FindTimezoneId(full);
    if (!tzId) {
        return NUdf::TUnboxedValuePod();
    }

    ui32 year, month, day;
    ui32 pos = 0;
    bool beforeChrist = false;
    if (pos < buf.Size()) {
        char c = buf.Data()[pos];
        if (c == '-') {
            beforeChrist = true;
            ++pos;
        } else if (c == '+') {
            ++pos;
        }
    }

    if (!ParseNumber(pos, buf, year, Big ? 6 : 4) || pos == buf.size() || buf.data()[pos] != '-') {
        return NUdf::TUnboxedValuePod();
    }

    // skip '-'
    ++pos;
    if (!ParseNumber(pos, buf, month, 2) || pos == buf.size() || buf.data()[pos] != '-') {
        return NUdf::TUnboxedValuePod();
    }

    // skip '-'
    ++pos;
    if (!ParseNumber(pos, buf, day, 2) || pos == buf.size() || buf.data()[pos] != 'T') {
        return NUdf::TUnboxedValuePod();
    }

    ui32 hour, minute, second;
    // skip 'T'
    ++pos;
    if (!ParseNumber(pos, buf, hour, 2) || pos == buf.size() || buf.data()[pos] != ':') {
        return NUdf::TUnboxedValuePod();
    }

    // skip ':'
    ++pos;
    if (!ParseNumber(pos, buf, minute, 2) || pos == buf.size() || buf.data()[pos] != ':') {
        return NUdf::TUnboxedValuePod();
    }

    // skip ':'
    ++pos;
    if (!ParseNumber(pos, buf, second, 2) || pos != buf.size()) {
        return NUdf::TUnboxedValuePod();
    }

    i64 absoluteSeconds;
    if (!FromLocalTimeValidated<Big>(*tzId, beforeChrist, year, month, day, hour, minute, second, absoluteSeconds)) {
        return NUdf::TUnboxedValuePod();
    }

    if constexpr (Big) {
        i64 value = absoluteSeconds;
        NUdf::TUnboxedValuePod out(value);
        out.SetTimezoneId(*tzId);
        return out;
    } else {
        ui32 value = absoluteSeconds;
        NUdf::TUnboxedValuePod out(value);
        out.SetTimezoneId(*tzId);
        return out;
    }
}

bool ParseMicroseconds(ui32& pos, NUdf::TStringRef buf, ui32& microseconds) {
    if (buf.Data()[pos] == '.') {
        ui32 ms = 0;
        // Skip dot
        ++pos;
        ui32 prevPos = pos;
        if (!ParseNumber(pos, buf, ms, 6)) {
            return false;
        }

        prevPos = pos - prevPos;

        while (prevPos < 6) {
            ms *= 10;
            ++prevPos;
        }
        microseconds = ms;

        // Skip unused digits
        while (pos < buf.Size() && '0' <= buf.Data()[pos] && buf.Data()[pos] <= '9') {
            ++pos;
        }
    }
    return true;
}

NUdf::TUnboxedValuePod ParseTimestamp64(NUdf::TStringRef buf) {
    i32 date;
    ui32 pos = 0;
    if (Y_UNLIKELY(!ParseDate32(pos, buf, date))) {
        return NUdf::TUnboxedValuePod();
    }

    ui32 time;
    if (Y_UNLIKELY(!ParseTime(pos, buf, time))) {
        return NUdf::TUnboxedValuePod();
    }

    ui32 microseconds = 0;
    if (Y_UNLIKELY(!ParseMicroseconds(pos, buf, microseconds))) {
        return NUdf::TUnboxedValuePod();
    }

    i32 zoneOffset = 0;
    if (Y_UNLIKELY(!ParseTimezoneOffset(pos, buf, zoneOffset))) {
        return NUdf::TUnboxedValuePod();
    }
    if (Y_UNLIKELY(pos != buf.Size())) {
        return NUdf::TUnboxedValuePod();
    }
    i64 value = 86400000000ull;
    value *= date;
    value += (i32(time) + zoneOffset)*1000000ull;
    value += microseconds;
    if (Y_UNLIKELY(NUdf::MIN_TIMESTAMP64 > value || value > NUdf::MAX_TIMESTAMP64)) {
        return NUdf::TUnboxedValuePod();
    }
    return NUdf::TUnboxedValuePod(value);
}

NUdf::TUnboxedValuePod ParseTimestamp(NUdf::TStringRef buf) {
    ui32 year, month, day;
    ui32 pos = 0;
    if (!ParseNumber(pos, buf, year, 4) || pos == buf.Size() || buf.Data()[pos] != '-') {
        return NUdf::TUnboxedValuePod();
    }

    // skip '-'
    ++pos;
    if (!ParseNumber(pos, buf, month, 2) || pos == buf.Size() || buf.Data()[pos] != '-') {
        return NUdf::TUnboxedValuePod();
    }

    // skip '-'
    ++pos;
    if (!ParseNumber(pos, buf, day, 2) || pos == buf.Size() || buf.Data()[pos] != 'T') {
        return NUdf::TUnboxedValuePod();
    }

    ui16 dateValue;
    if (!MakeDate(year, month, day, dateValue)) {
        return NUdf::TUnboxedValuePod();
    }

    ui32 hour, minute, second;
    // skip 'T'
    ++pos;
    if (!ParseNumber(pos, buf, hour, 2) || pos == buf.Size() || buf.Data()[pos] != ':') {
        return NUdf::TUnboxedValuePod();
    }

    // skip ':'
    ++pos;
    if (!ParseNumber(pos, buf, minute, 2) || pos == buf.Size() || buf.Data()[pos] != ':') {
        return NUdf::TUnboxedValuePod();
    }

    // skip ':'
    ++pos;
    if (!ParseNumber(pos, buf, second, 2) || pos == buf.Size()) {
        return NUdf::TUnboxedValuePod();
    }

    bool waiting_for_z = true;

    ui32 microseconds = 0;
    if (buf.Data()[pos] == '.') {
        // Skip dot
        ++pos;
        ui32 prevPos = pos;
        if (!ParseNumber(pos, buf, microseconds, 6)) {
            return NUdf::TUnboxedValuePod();
        }

        prevPos = pos - prevPos;

        while (prevPos < 6) {
            microseconds *= 10;
            ++prevPos;
        }

        // Skip unused digits
        while (pos < buf.Size() && '0' <= buf.Data()[pos] && buf.Data()[pos] <= '9') {
            ++pos;
        }

        if (pos == buf.Size()) {
            return NUdf::TUnboxedValuePod();
        }

    }

    ui32 offset_hours = 0;
    ui32 offset_minutes = 0;
    bool is_offset_negative = false;
    if (buf.Data()[pos] == '+' || buf.Data()[pos] == '-') {
        is_offset_negative = buf.Data()[pos] == '-';

        // Skip sign
        ++pos;

        if (!ParseNumber(pos, buf, offset_hours, 2) || 
            pos == buf.Size() || buf.Data()[pos] != ':')
        {
            return NUdf::TUnboxedValuePod();
        }
 
        // Skip ':'
        ++pos;

        if (!ParseNumber(pos, buf, offset_minutes, 2) || pos != buf.Size()) {
            return NUdf::TUnboxedValuePod();
        }

        waiting_for_z = false;
    }

    ui64 offset_value = ((offset_hours) * 60 + offset_minutes) * 60 * 1000000ull;

    if (waiting_for_z) {
        if (pos == buf.Size() || buf.Data()[pos] != 'Z') {
            return NUdf::TUnboxedValuePod();
        }

        // skip 'Z'
        ++pos;
        if (pos != buf.Size()) {
            return NUdf::TUnboxedValuePod();
        }
    }

    ui32 timeValue;
    if (!MakeTime(hour, minute, second, timeValue)) {
        return NUdf::TUnboxedValuePod();
    }

    ui64 value = dateValue * 86400000000ull + timeValue * 1000000ull + microseconds;
    
    if (is_offset_negative) {
        if (UINT64_MAX - value < offset_value) {
            return NUdf::TUnboxedValuePod();
        }
        value += offset_value;
    } else {
        if (value < offset_value) {
            return NUdf::TUnboxedValuePod();
        }
        value -= offset_value;
    }

    if (value >= NUdf::MAX_TIMESTAMP) {
        return NUdf::TUnboxedValuePod();
    }

    return NUdf::TUnboxedValuePod(value);
}

template <bool Big>
NUdf::TUnboxedValuePod ParseTzTimestamp(NUdf::TStringRef str) {
    TStringBuf full = str;
    TStringBuf buf;
    GetNext(full, ',', buf);
    const auto tzId = FindTimezoneId(full);
    if (!tzId) {
        return NUdf::TUnboxedValuePod();
    }

    ui32 year, month, day;
    ui32 pos = 0;
    bool beforeChrist = false;
    if (pos < buf.Size()) {
        char c = buf.Data()[pos];
        if (c == '-') {
            beforeChrist = true;
            ++pos;
        } else if (c == '+') {
            ++pos;
        }
    }

    if (!ParseNumber(pos, buf, year, Big ? 6 : 4) || pos == buf.size() || buf.data()[pos] != '-') {
        return NUdf::TUnboxedValuePod();
    }

    // skip '-'
    ++pos;
    if (!ParseNumber(pos, buf, month, 2) || pos == buf.size() || buf.data()[pos] != '-') {
        return NUdf::TUnboxedValuePod();
    }

    // skip '-'
    ++pos;
    if (!ParseNumber(pos, buf, day, 2) || pos == buf.size() || buf.data()[pos] != 'T') {
        return NUdf::TUnboxedValuePod();
    }

    ui32 hour, minute, second;
    // skip 'T'
    ++pos;
    if (!ParseNumber(pos, buf, hour, 2) || pos == buf.size() || buf.data()[pos] != ':') {
        return NUdf::TUnboxedValuePod();
    }

    // skip ':'
    ++pos;
    if (!ParseNumber(pos, buf, minute, 2) || pos == buf.size() || buf.data()[pos] != ':') {
        return NUdf::TUnboxedValuePod();
    }

    // skip ':'
    ++pos;
    if (!ParseNumber(pos, buf, second, 2)) {
        return NUdf::TUnboxedValuePod();
    }

    ui32 microseconds = 0;
    if (pos != buf.size()) {
        if (buf.data()[pos] != '.') {
            return NUdf::TUnboxedValuePod();
        }

        ++pos;
        ui32 prevPos = pos;
        if (!ParseNumber(pos, buf, microseconds, 6)) {
            return NUdf::TUnboxedValuePod();
        }

        prevPos = pos - prevPos;

        while (prevPos < 6) {
            microseconds *= 10;
            ++prevPos;
        }

        // Skip unused digits
        while (pos < buf.size() && '0' <= buf.data()[pos] && buf.data()[pos] <= '9') {
            ++pos;
        }

        if (pos != buf.size()) {
            return NUdf::TUnboxedValuePod();
        }
    }

    i64 absoluteSeconds;
    if (!FromLocalTimeValidated<Big>(*tzId, beforeChrist, year, month, day, hour, minute, second, absoluteSeconds)) {
        return NUdf::TUnboxedValuePod();
    }

    if constexpr (Big) {
        const i64 value = absoluteSeconds * 1000000ull + microseconds;
        NUdf::TUnboxedValuePod out(value);
        out.SetTimezoneId(*tzId);
        return out;
    } else {
        const ui64 value = absoluteSeconds * 1000000ull + microseconds;
        NUdf::TUnboxedValuePod out(value);
        out.SetTimezoneId(*tzId);
        return out;
    }
}

template <bool DecimalPart = false, i8 MaxDigits = 6>
bool ParseNumber(std::string_view::const_iterator& pos, const std::string_view& buf, ui32& value) {
    value = 0U;

    if (buf.cend() == pos || !std::isdigit(*pos)) {
        return false;
    }

    auto digits = MaxDigits;
    do {
        value *= 10U;
        value += *pos - '0';
        if (buf.cend() == ++pos || !digits--) {
            return false;
        }
    } while (std::isdigit(*pos));
    if (DecimalPart) {
        while (digits--) {
            value *= 10U;
        }
    }
    return true;
}

template <i64 UpperBound, ui32 MaxDays>
NUdf::TUnboxedValuePod ParseInterval(const std::string_view& buf) {
    if (buf.empty()) {
        return NUdf::TUnboxedValuePod();
    }

    auto pos = buf.cbegin();
    bool isSigned = *pos == '-';
    if (isSigned || *pos == '+') {
        ++pos;
    }

    if (buf.cend() == pos || *pos++ != 'P' || buf.cend() == pos) {
        return NUdf::TUnboxedValuePod();
    }

    std::optional<ui32> days, hours, minutes, seconds, microseconds;
    ui32 num;

    if (*pos != 'T') {
        if (!ParseNumber<false, 9>(pos, buf, num)) {
            return NUdf::TUnboxedValuePod();
        }

        switch (*pos++) {
            case 'D': days = num; break;
            case 'W': days = 7U * num; break;
            default: return NUdf::TUnboxedValuePod();
        }
    }

    if (days > MaxDays) {
        return NUdf::TUnboxedValuePod();
    }

    if (buf.cend() != pos) {
        if (*pos++ != 'T') {
            return NUdf::TUnboxedValuePod();
        }

        if (buf.cend() != pos) // TODO: Remove this line later.
        do {
            if (!ParseNumber(pos, buf, num)) {
                return NUdf::TUnboxedValuePod();
            }

            switch (*pos++) {
                case 'H':
                    if (hours || minutes || seconds) {
                        return NUdf::TUnboxedValuePod();
                    }
                    hours = num;
                    break;
                case 'M':
                    if (minutes || seconds) {
                        return NUdf::TUnboxedValuePod();
                    }
                    minutes = num;
                    break;
                case 'S':
                    if (seconds) {
                        return NUdf::TUnboxedValuePod();
                    }
                    seconds = num;
                    break;
                case '.':
                    if (seconds) {
                        return NUdf::TUnboxedValuePod();
                    }
                    seconds = num;
                    if (!ParseNumber<true>(pos, buf, num) || *pos++ != 'S') {
                        return NUdf::TUnboxedValuePod();
                    }
                    microseconds = num;
                    break;
                default: return NUdf::TUnboxedValuePod();
            }
        } while (buf.cend() != pos);
    }

    const ui64 value
        = days.value_or(0U) * 86400000000ull
        + hours.value_or(0U) * 3600000000ull
        + minutes.value_or(0U) * 60000000ull
        + seconds.value_or(0U) * 1000000ull
        + microseconds.value_or(0U);

    if (value > UpperBound) {
        return NUdf::TUnboxedValuePod();
    }

    i64 signedValue = value;
    if (isSigned) {
        signedValue =-signedValue;
    }

    return NUdf::TUnboxedValuePod(signedValue);
}

bool IsValidStringValue(NUdf::EDataSlot type, NUdf::TStringRef buf) {
    switch (type) {
    case NUdf::EDataSlot::Bool:
        return AsciiEqualsIgnoreCase(buf, TStringBuf("true")) || AsciiEqualsIgnoreCase(buf, TStringBuf("false"));

    case NUdf::EDataSlot::Int8:
        return IsValidNumberString<i8>(buf);
    case NUdf::EDataSlot::Uint8:
        return IsValidNumberString<ui8>(buf);
    case NUdf::EDataSlot::Int16:
        return IsValidNumberString<i16>(buf);
    case NUdf::EDataSlot::Uint16:
        return IsValidNumberString<ui16>(buf);
    case NUdf::EDataSlot::Int32:
        return IsValidNumberString<i32>(buf);
    case NUdf::EDataSlot::Uint32:
        return IsValidNumberString<ui32>(buf);
    case NUdf::EDataSlot::Int64:
        return IsValidNumberString<i64>(buf);
    case NUdf::EDataSlot::Uint64:
        return IsValidNumberString<ui64>(buf);
    case NUdf::EDataSlot::Float:
        return IsValidNumberString<float>(buf);
    case NUdf::EDataSlot::Double:
        return IsValidNumberString<double>(buf);
    case NUdf::EDataSlot::Decimal:
        return IsValidDecimal(buf);
    case NUdf::EDataSlot::String:
        return true;
    case NUdf::EDataSlot::Utf8:
        return IsUtf8(buf);
    case NUdf::EDataSlot::Yson:
        return NDom::IsValidYson(buf);
    case NUdf::EDataSlot::Json:
    case NUdf::EDataSlot::JsonDocument:
        return NDom::IsValidJson(buf);
    case NUdf::EDataSlot::Uuid:
        return NUuid::IsValidUuid(buf);

    case NUdf::EDataSlot::DyNumber:
        return NDyNumber::IsValidDyNumberString(buf);

    case NUdf::EDataSlot::Date:
    case NUdf::EDataSlot::Datetime:
    case NUdf::EDataSlot::Timestamp:
    case NUdf::EDataSlot::Interval:
    case NUdf::EDataSlot::TzDate:
    case NUdf::EDataSlot::TzDatetime:
    case NUdf::EDataSlot::TzTimestamp:
    case NUdf::EDataSlot::Date32:
    case NUdf::EDataSlot::Datetime64:
    case NUdf::EDataSlot::Timestamp64:
    case NUdf::EDataSlot::Interval64:
    case NUdf::EDataSlot::TzDate32:
    case NUdf::EDataSlot::TzDatetime64:
    case NUdf::EDataSlot::TzTimestamp64:
        return bool(ValueFromString(type, buf));

    default:
        break;
    }

    MKQL_ENSURE(false, "Incorrect data slot: " << (ui32)type);
}

NUdf::TUnboxedValuePod ValueFromString(NUdf::EDataSlot type, NUdf::TStringRef buf) {
    switch (type) {
    case NUdf::EDataSlot::Bool: {
        if (AsciiEqualsIgnoreCase(buf, TStringBuf("true"))) {
            return NUdf::TUnboxedValuePod(true);
        }
        if (AsciiEqualsIgnoreCase(buf, TStringBuf("false"))) {
            return NUdf::TUnboxedValuePod(false);
        }
        return NUdf::TUnboxedValuePod();
    }

    case NUdf::EDataSlot::Int8:
        return NumberFromString<i8>(buf);

    case NUdf::EDataSlot::Uint8:
        return NumberFromString<ui8>(buf);

    case NUdf::EDataSlot::Int16:
        return NumberFromString<i16>(buf);

    case NUdf::EDataSlot::Uint16:
        return NumberFromString<ui16>(buf);

    case NUdf::EDataSlot::Int32:
        return NumberFromString<i32>(buf);

    case NUdf::EDataSlot::Uint32:
        return NumberFromString<ui32>(buf);

    case NUdf::EDataSlot::Int64:
        return NumberFromString<i64>(buf);

    case NUdf::EDataSlot::Uint64:
        return NumberFromString<ui64>(buf);

    case NUdf::EDataSlot::Float:
        return NumberFromString<float>(buf);

    case NUdf::EDataSlot::Double:
        return NumberFromString<double>(buf);

    case NUdf::EDataSlot::String:
        return MakeString(buf);

    case NUdf::EDataSlot::Utf8:
        if (!IsUtf8(buf)) {
            return NUdf::TUnboxedValuePod();
        }

        return MakeString(buf);

    case NUdf::EDataSlot::Yson:
        if (!NDom::IsValidYson(buf)) {
            return NUdf::TUnboxedValuePod();
        }

        return MakeString(buf);

    case NUdf::EDataSlot::Json:
        if (!NDom::IsValidJson(buf)) {
            return NUdf::TUnboxedValuePod();
        }

        return MakeString(buf);

    case NUdf::EDataSlot::Uuid:
        return ParseUuid(buf);

    case NUdf::EDataSlot::Date:
        return ParseDate(buf);

    case NUdf::EDataSlot::Datetime:
        return ParseDatetime(buf);

    case NUdf::EDataSlot::Timestamp:
        return ParseTimestamp(buf);

    case NUdf::EDataSlot::Interval:
        return ParseInterval<NUdf::MAX_TIMESTAMP - 1, 2*NUdf::MAX_DATE>(buf);

    case NUdf::EDataSlot::TzDate:
        return ParseTzDate<false>(buf);

    case NUdf::EDataSlot::TzDatetime:
        return ParseTzDatetime<false>(buf);

    case NUdf::EDataSlot::TzTimestamp:
        return ParseTzTimestamp<false>(buf);

    case NUdf::EDataSlot::Date32:
        return ParseDate32(buf);

    case NUdf::EDataSlot::Datetime64:
        return ParseDatetime64(buf);

    case NUdf::EDataSlot::Timestamp64:
        return ParseTimestamp64(buf);

    case NUdf::EDataSlot::Interval64:
        return ParseInterval<NUdf::MAX_INTERVAL64, NUdf::MAX_DATE32 - NUdf::MIN_DATE32>(buf);

    case NUdf::EDataSlot::TzDate32:
        return ParseTzDate<true>(buf);

    case NUdf::EDataSlot::TzDatetime64:
        return ParseTzDatetime<true>(buf);

    case NUdf::EDataSlot::TzTimestamp64:
        return ParseTzTimestamp<true>(buf);

    case NUdf::EDataSlot::DyNumber: {
        auto dyNumber = NDyNumber::ParseDyNumberString(buf);
        if (!dyNumber.Defined()) {
            // DyNumber parse error happened, return NULL
            return NUdf::TUnboxedValuePod();
        }
        return MakeString(*dyNumber);
    }

    case NUdf::EDataSlot::JsonDocument: {
        auto binaryJson = NKikimr::NBinaryJson::SerializeToBinaryJson(buf);
        if (!binaryJson.Defined()) {
            // JSON parse error happened, return NULL
            return NUdf::TUnboxedValuePod();
        }
        return MakeString(TStringBuf(binaryJson->Data(), binaryJson->Size()));
    }

    case NUdf::EDataSlot::Decimal:
        THROW yexception() << "Decimal is unexpected";
    }
}

NUdf::TUnboxedValuePod SimpleValueFromYson(NUdf::EDataSlot type, NUdf::TStringRef buf) {
    const bool isBinYson = !buf.Empty() && *buf.Data() <= NYson::NDetail::Uint64Marker;
    if (!isBinYson) {
        auto textBuf = buf;
        switch (type) {
        case NUdf::EDataSlot::Bool:
            if (buf.Empty()) {
                return NUdf::TUnboxedValuePod();
            }
            textBuf = buf.Substring(1, buf.Size() - 1);
            break;
        case NUdf::EDataSlot::Float:
        case NUdf::EDataSlot::Double:
            if (buf.Empty()) {
                return NUdf::TUnboxedValuePod();
            }

            if (buf.Data()[0] == '%') {
                textBuf = buf.Substring(1, buf.Size() - 1);
            }

            break;
        case NUdf::EDataSlot::Uint8:
        case NUdf::EDataSlot::Uint16:
        case NUdf::EDataSlot::Uint32:
        case NUdf::EDataSlot::Uint64:
        case NUdf::EDataSlot::Date:
        case NUdf::EDataSlot::Datetime:
        case NUdf::EDataSlot::Timestamp:
            if (buf.Empty()) {
                return NUdf::TUnboxedValuePod();
            }
            /// YSON for unsigned may be with or without suffix 'u'
            if (buf.Data()[buf.Size() - 1] == 'u') {
                textBuf = buf.Substring(0, buf.Size() - 1);
            }
            break;
        case NUdf::EDataSlot::String: {
            if (buf.Empty()) {
                return NUdf::TUnboxedValuePod::Zero();
            }

            const char ysonQuote = '"';
            if (*buf.Data() == NYson::NDetail::EntitySymbol) {
                return NUdf::TUnboxedValuePod();
            } else if (*buf.Data() != ysonQuote) {
                return MakeString(buf);
            }

            if (const auto count = std::count(buf.Data(), buf.Data() + buf.Size(), '\\')) {
                if (const auto size = buf.Size() - count) {
                    auto out = MakeStringNotFilled(size);
                    std::copy_if(buf.Data(), buf.Data() + buf.Size(), out.AsStringRef().Data(), [](char c){ return c != '\\'; });
                    return out;
                } else {
                    return NUdf::TUnboxedValuePod::Zero();
                }
            } else {
                return MakeString(buf);
            }
        }
        case NUdf::EDataSlot::TzDate:
        case NUdf::EDataSlot::TzDatetime:
        case NUdf::EDataSlot::TzTimestamp:
        case NUdf::EDataSlot::Decimal:
        case NUdf::EDataSlot::Uuid:
            Y_ABORT("TODO");

        default:
            ;
        }

        return ValueFromString(type, textBuf);
    }

    const ui8 ytBinType = *buf.Data();
    auto binPayload = buf.Substring(1, buf.Size() - 1);
    switch (type) {
    case NUdf::EDataSlot::Bool: {
        if (ytBinType == NYson::NDetail::FalseMarker) {
            return NUdf::TUnboxedValuePod(false);
        }

        if (ytBinType == NYson::NDetail::TrueMarker) {
            return NUdf::TUnboxedValuePod(true);
        }

        return NUdf::TUnboxedValuePod();
    }

    case NUdf::EDataSlot::Uint8:
    case NUdf::EDataSlot::Uint16:
    case NUdf::EDataSlot::Uint32:
    case NUdf::EDataSlot::Uint64:
    case NUdf::EDataSlot::Date:
    case NUdf::EDataSlot::Datetime:
    case NUdf::EDataSlot::Timestamp: {
        if (ytBinType != NYson::NDetail::Uint64Marker) {
            return NUdf::TUnboxedValuePod();
        }

        TMemoryInput stringRefStream(binPayload.Data(), binPayload.Size());
        ui64 value;
        const size_t read = NYson::ReadVarUInt64(&stringRefStream, &value);
        if (read != binPayload.Size()) {
            return NUdf::TUnboxedValuePod();
        }

        return NUdf::TUnboxedValuePod(value);
    }

    case NUdf::EDataSlot::Int8:
    case NUdf::EDataSlot::Int16:
    case NUdf::EDataSlot::Int32:
    case NUdf::EDataSlot::Int64:
    case NUdf::EDataSlot::Date32:
    case NUdf::EDataSlot::Datetime64:
    case NUdf::EDataSlot::Timestamp64:
    case NUdf::EDataSlot::Interval64:
    case NUdf::EDataSlot::Interval: {
        if (ytBinType != NYson::NDetail::Int64Marker) {
            return NUdf::TUnboxedValuePod();
        }

        TMemoryInput stringRefStream(binPayload.Data(), binPayload.Size());
        i64 value;
        const size_t read = NYson::ReadVarInt64(&stringRefStream, &value);
        if (read != binPayload.Size()) {
            return NUdf::TUnboxedValuePod();
        }

        return NUdf::TUnboxedValuePod(value);
    }

    case NUdf::EDataSlot::Float: {
        if (ytBinType != NYson::NDetail::DoubleMarker || binPayload.Size() != 8) {
            return NUdf::TUnboxedValuePod();
        }

        const float x = *reinterpret_cast<const double*>(binPayload.Data());
        return NUdf::TUnboxedValuePod(x);
    }

    case NUdf::EDataSlot::Double: {
        if (ytBinType != NYson::NDetail::DoubleMarker || binPayload.Size() != 8) {
            return NUdf::TUnboxedValuePod();
        }

        const double x = *reinterpret_cast<const double*>(binPayload.Data());
        return NUdf::TUnboxedValuePod(x);
    }

    case NUdf::EDataSlot::String:
    case NUdf::EDataSlot::Utf8:
    case NUdf::EDataSlot::Json: {
        if (ytBinType != NYson::NDetail::StringMarker) {
            return NUdf::TUnboxedValuePod();
        }

        TMemoryInput stringRefStream(binPayload.Data(), binPayload.Size());
        i32 value;
        const size_t read = NYson::ReadVarInt32(&stringRefStream, &value);
        binPayload = binPayload.Substring(read, binPayload.Size() - read);
        const size_t strLen = value;
        if (strLen != binPayload.Size()) {
            return NUdf::TUnboxedValuePod();
        }

        return MakeString(NUdf::TStringRef(binPayload.Data(), strLen));
    }

    case NUdf::EDataSlot::Yson:
        return MakeString(buf);

    case NUdf::EDataSlot::TzDate:
    case NUdf::EDataSlot::TzDatetime:
    case NUdf::EDataSlot::TzTimestamp:
    case NUdf::EDataSlot::TzDate32:
    case NUdf::EDataSlot::TzDatetime64:
    case NUdf::EDataSlot::TzTimestamp64:
    case NUdf::EDataSlot::Decimal:
    case NUdf::EDataSlot::Uuid:
    case NUdf::EDataSlot::DyNumber:
    case NUdf::EDataSlot::JsonDocument:
        Y_ABORT("TODO");
    }

    MKQL_ENSURE(false, "SimpleValueFromYson: Incorrect typeid: " << type);
}

ui16 InitTimezones() {
    return ui16(Singleton<TTimezones>()->Zones.size());
}

TMaybe<ui16> FindTimezoneId(TStringBuf ianaName) {
    const auto& zones = *Singleton<TTimezones>();
    auto it = zones.Name2Id.find(ianaName);
    if (it == zones.Name2Id.end()) {
        return Nothing();
    }

    return it->second;
}

ui16 GetTimezoneId(TStringBuf ianaName) {
    const auto& zones = *Singleton<TTimezones>();
    const auto it = zones.Name2Id.find(ianaName);
    MKQL_ENSURE(it != zones.Name2Id.cend(), "Unknown time zone name: " << ianaName);
    return it->second;
}

bool IsValidTimezoneId(ui16 id) {
    const auto zones = NUdf::GetTimezones();
    return id < zones.size() && !zones[id].empty();
}

TMaybe<TStringBuf> FindTimezoneIANAName(ui16 id) {
    const auto zones = NUdf::GetTimezones();
    if (id >= zones.size() || zones[id].empty()) {
        return Nothing();
    }

    return TStringBuf(zones[id]);
}

TStringBuf GetTimezoneIANAName(ui16 id) {
    const auto zones = NUdf::GetTimezones();
    MKQL_ENSURE(id < zones.size() && !zones[id].empty(), "Invalid time zone id: " << id);
    return TStringBuf(zones[id]);
}

std::vector<ui16> GetTzBlackList() {
    std::vector<ui16> result;
    const auto& zones = NUdf::GetTimezones();
    for (ui16 id = 0; id < zones.size(); ++id) {
        if (zones[id].empty()) {
            result.emplace_back(id);
        }
    }
    return result;
}

void SerializeTzDate(ui16 date, ui16 tzId, IOutputStream& out) {
    date = SwapBytes(date);
    tzId = SwapBytes(tzId);
    out.Write(&date, sizeof(date));
    out.Write(&tzId, sizeof(tzId));
}

void SerializeTzDatetime(ui32 datetime, ui16 tzId, IOutputStream& out) {
    datetime = SwapBytes(datetime);
    tzId = SwapBytes(tzId);
    out.Write(&datetime, sizeof(datetime));
    out.Write(&tzId, sizeof(tzId));
}

void SerializeTzTimestamp(ui64 timestamp, ui16 tzId, IOutputStream& out) {
    timestamp = SwapBytes(timestamp);
    tzId = SwapBytes(tzId);
    out.Write(&timestamp, sizeof(timestamp));
    out.Write(&tzId, sizeof(tzId));
}

bool DeserializeTzDate(TStringBuf buf, ui16& date, ui16& tzId) {
    if (buf.size() != sizeof(ui16) + sizeof(ui16)) {
        return false;
    }

    date = ReadUnaligned<ui16>(buf.data());
    date = SwapBytes(date);
    if (date >= NUdf::MAX_DATE) {
        return false;
    }

    tzId = ReadUnaligned<ui16>(buf.data() + sizeof(date));
    tzId = SwapBytes(tzId);
    if (!IsValidTimezoneId(tzId)) {
        return false;
    }

    return true;
}

bool DeserializeTzDatetime(TStringBuf buf, ui32& datetime, ui16& tzId) {
    if (buf.size() != sizeof(ui32) + sizeof(ui16)) {
        return false;
    }

    datetime = ReadUnaligned<ui32>(buf.data());
    datetime = SwapBytes(datetime);
    if (datetime >= NUdf::MAX_DATETIME) {
        return false;
    }

    tzId = ReadUnaligned<ui16>(buf.data() + sizeof(datetime));
    tzId = SwapBytes(tzId);
    if (!IsValidTimezoneId(tzId)) {
        return false;
    }

    return true;
}

bool DeserializeTzTimestamp(TStringBuf buf, ui64& timestamp, ui16& tzId) {
    if (buf.size() != sizeof(ui64) + sizeof(ui16)) {
        return false;
    }

    timestamp = ReadUnaligned<ui64>(buf.data());
    timestamp = SwapBytes(timestamp);
    if (timestamp >= NUdf::MAX_TIMESTAMP) {
        return false;
    }

    tzId = ReadUnaligned<ui16>(buf.data() + sizeof(timestamp));
    tzId = SwapBytes(tzId);
    if (!IsValidTimezoneId(tzId)) {
        return false;
    }

    return true;
}

void SerializeTzDate32(i32 date, ui16 tzId, IOutputStream& out) {
    auto value = 0x80 ^ SwapBytes((ui32)date);
    tzId = SwapBytes(tzId);
    out.Write(&value, sizeof(value));
    out.Write(&tzId, sizeof(tzId));
}

void SerializeTzDatetime64(i64 datetime, ui16 tzId, IOutputStream& out) {
    auto value = 0x80 ^ SwapBytes((ui64)datetime);
    tzId = SwapBytes(tzId);
    out.Write(&value, sizeof(value));
    out.Write(&tzId, sizeof(tzId));
}

void SerializeTzTimestamp64(i64 timestamp, ui16 tzId, IOutputStream& out) {
    auto value = 0x80 ^ SwapBytes((ui64)timestamp);
    tzId = SwapBytes(tzId);
    out.Write(&value, sizeof(value));
    out.Write(&tzId, sizeof(tzId));
}

bool DeserializeTzDate32(TStringBuf buf, i32& date, ui16& tzId) {
    if (buf.size() != sizeof(i32) + sizeof(ui16)) {
        return false;
    }

    auto value = ReadUnaligned<ui32>(buf.data());
    date = (i32)(SwapBytes(value ^ 0x80));
    if (date < NUdf::MIN_DATE32 || date > NUdf::MAX_DATE32) {
        return false;
    }

    tzId = ReadUnaligned<ui16>(buf.data() + sizeof(date));
    tzId = SwapBytes(tzId);
    if (!IsValidTimezoneId(tzId)) {
        return false;
    }

    return true;
}

bool DeserializeTzDatetime64(TStringBuf buf, i64& datetime, ui16& tzId) {
    if (buf.size() != sizeof(i64) + sizeof(ui16)) {
        return false;
    }

    auto value = ReadUnaligned<ui64>(buf.data());
    datetime = (i64)(SwapBytes(0x80 ^ value));
    if (datetime < NUdf::MIN_DATETIME64 || datetime > NUdf::MAX_DATETIME64) {
        return false;
    }

    tzId = ReadUnaligned<ui16>(buf.data() + sizeof(datetime));
    tzId = SwapBytes(tzId);
    if (!IsValidTimezoneId(tzId)) {
        return false;
    }

    return true;
}

bool DeserializeTzTimestamp64(TStringBuf buf, i64& timestamp, ui16& tzId) {
    if (buf.size() != sizeof(i64) + sizeof(ui16)) {
        return false;
    }

    auto value = ReadUnaligned<ui64>(buf.data());
    timestamp = (i64)(SwapBytes(0x80 ^ value));
    if (timestamp < NUdf::MIN_TIMESTAMP64 || timestamp > NUdf::MAX_TIMESTAMP64) {
        return false;
    }

    tzId = ReadUnaligned<ui16>(buf.data() + sizeof(timestamp));
    tzId = SwapBytes(tzId);
    if (!IsValidTimezoneId(tzId)) {
        return false;
    }

    return true;
}

} // namespace NMiniKQL
} // namespace NKikimr
