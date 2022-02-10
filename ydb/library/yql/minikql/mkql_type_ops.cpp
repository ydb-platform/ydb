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

    case NUdf::EDataSlot::TzDate:
        return bool(value) && value.Get<ui16>() < NUdf::MAX_DATE && value.GetTimezoneId() < NUdf::GetTimezones().size();

    case NUdf::EDataSlot::TzDatetime:
        return bool(value) && value.Get<ui32>() < NUdf::MAX_DATETIME && value.GetTimezoneId() < NUdf::GetTimezones().size();

    case NUdf::EDataSlot::TzTimestamp:
        return bool(value) && value.Get<ui64>() < NUdf::MAX_TIMESTAMP && value.GetTimezoneId() < NUdf::GetTimezones().size();

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

bool IsLeapYear(ui32 year) {
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

void WriteDate(IOutputStream& out, ui32 year, ui32 month, ui32 day) {
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

void SplitTime(ui32 value, ui32& hour, ui32& min, ui32& sec) {
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

bool WriteInterval(IOutputStream& out, i64 signedValue) {
    ui64 value = signedValue < 0 ? -signedValue : signedValue;

    if (value >= NUdf::MAX_TIMESTAMP) {
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

static void WriteHexDigit(ui8 digit, IOutputStream& out) {
    if (digit <= 9) {
        out << char('0' + digit);
    }
    else {
        out << char('a' + digit - 10);
    }
}

static void WriteHex(ui16 bytes, IOutputStream& out, bool reverseBytes = false) {
    if (reverseBytes) {
        WriteHexDigit((bytes >> 4) & 0x0f, out);
        WriteHexDigit(bytes & 0x0f, out);
        WriteHexDigit((bytes >> 12) & 0x0f, out);
        WriteHexDigit((bytes >> 8) & 0x0f, out);
    } else {
        WriteHexDigit((bytes >> 12) & 0x0f, out);
        WriteHexDigit((bytes >> 8) & 0x0f, out);
        WriteHexDigit((bytes >> 4) & 0x0f, out);
        WriteHexDigit(bytes & 0x0f, out);
    }
}

static void UuidToString(ui16 dw[8], IOutputStream& out) {
    WriteHex(dw[1], out);
    WriteHex(dw[0], out);
    out << '-';
    WriteHex(dw[2], out);
    out << '-';
    WriteHex(dw[3], out);
    out << '-';
    WriteHex(dw[4], out, true);
    out << '-';
    WriteHex(dw[5], out, true);
    WriteHex(dw[6], out, true);
    WriteHex(dw[7], out, true);
}

}

void UuidHalfsToByteString(ui64 low, ui64 hi, IOutputStream& out) {
    union {
        char bytes[16];
        ui64 half[2];
    } buf;
    buf.half[0] = low;
    buf.half[1] = hi;
    out.Write(buf.bytes, 16);
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
        UuidToString(dw, out);
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
        if (!WriteInterval(out, value.Get<i64>())) {
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

    case NUdf::EDataSlot::DyNumber: {
        out << NDyNumber::DyNumberToString(value.AsStringRef());
        break;
    }

    case NUdf::EDataSlot::JsonDocument: {
        out << NKikimr::NBinaryJson::SerializeToJson(value.AsStringRef());
        break;
    }

    case NUdf::EDataSlot::Decimal:
    default:
        THROW yexception() << "Incorrect data slot: " << (ui32)type;
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

bool MakeDateUncached(ui32 year, ui32 month, ui32 day, ui16& value) {
    if (year < NUdf::MIN_YEAR || year >= NUdf::MAX_YEAR) {
        return false;
    }

    if (month < 1 || month > 12) {
        return false;
    }

    const bool isLeap = IsLeapYear(year);
    auto monthLength = GetMonthLength(month, isLeap);
    if (day < 1 || day > monthLength) {
        return false;
    }

    year -= NUdf::MIN_YEAR;
    ui32 leapDaysCount = LeapDaysSinceEpoch(year);
    value = year * 365 + leapDaysCount;
    while (month > 1) {
        --month;
        value += GetMonthLength(month, isLeap);
    }

    value += day - 1;
    return true;
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

class TDateTable {
public:
    TDateTable() {
        ui32 prevYear = NUdf::MIN_YEAR - 1;
        YearsOffsets_[0] = 0;

        ui32 dayOfYear = 365;
        ui32 dayOfWeek = 2;
        ui32 weekOfYear = 52;
        ui32 weekOfYearIso8601 = 1;

        for (ui16 date = 0; date < Days_.size(); ++date) {
            ui32 year, month, day;
            Y_VERIFY(SplitDateUncached(date, year, month, day));

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
        if (Y_UNLIKELY(year < NUdf::MIN_YEAR || year > NUdf::MAX_YEAR
            || (year == NUdf::MAX_YEAR && (day > 1U || month > 1U)))) {
            return false;
        }

        if (Y_LIKELY(GetDateOffset(year, month, day, value) && value))
            --value;
        else
            return false;

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

    bool EnrichDate(ui16 value, ui32& dayOfYear, ui32& weekOfYear, ui32& weekOfYearIso8601, ui32& dayOfWeek) const {
        return EnrichByOffset(++value, dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek);
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

    std::array<ui16, NUdf::MAX_YEAR - NUdf::MIN_YEAR + 1> YearsOffsets_; // start of linear date for each year
    std::array<TDayInfo, NUdf::MAX_DATE + 2> Days_; // packed info for each date
};

}

bool SplitDate(ui16 value, ui32& year, ui32& month, ui32& day) {
    return TDateTable::Instance().SplitDate(value, year, month, day);
}

bool MakeDate(ui32 year, ui32 month, ui32 day, ui16& value) {
    return TDateTable::Instance().MakeDate(year, month, day, value);
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

bool FromLocalTimeValidated(ui16 tzId, ui32 year, ui32 month, ui32 day, ui32 hour, ui32 minute, ui32 second, ui32& value) {
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

    const auto& tz = Singleton<TTimezones>()->GetZone(tzId);

    cctz::civil_second sec(year, month, day, hour, minute, second);
    if ((ui32)sec.year() != year || (ui32)sec.month() != month || (ui32)sec.day() != day ||
        (ui32)sec.hour() != hour || (ui32)sec.minute() != minute || (ui32)sec.second() != second) {
        // normalized
        return false;
    }

    const auto absoluteSeconds = std::chrono::system_clock::to_time_t(tz.lookup(sec).pre);
    if (absoluteSeconds < 0 || (ui32)absoluteSeconds >= NUdf::MAX_DATETIME) {
        return false;
    }

    value = (ui32)absoluteSeconds;
    return true;
}

} // namespace

ui32 ParseNumber(ui32& pos, NUdf::TStringRef buf, ui32& value) {
    value = 0;
    ui32 count = 0;
    for (; pos < buf.Size(); ++pos) {
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

static bool GetDigit(char c, ui32& digit) {
    digit = 0;
    if ('0' <= c && c <= '9') {
        digit = c - '0';
    }
    else if ('a' <= c && c <= 'f') {
        digit = c - 'a' + 10;
    }
    else if ('A' <= c && c <= 'F') {
        digit = c - 'A' + 10;
    }
    else {
        return false; // non-hex character
    }
    return true;
}

bool IsValidUuid(NUdf::TStringRef buf) {
    if (buf.Size() != 36) {
        return false;
    }

    for (size_t i = 0; i < buf.Size(); ++i) {
        const char c = buf.Data()[i];

        if (c == '-') {
            if (i != 8 && i != 13 && i != 18 && i != 23) {
                return false;
            }
        } else if (!std::isxdigit(c)) {
            return false;
        }
    }

    return true;
}


static bool ParseUuidToArray(NUdf::TStringRef buf, ui16* dw, bool shortForm) {
    if (buf.Size() != (shortForm ? 32 : 36)) {
        return false;
    }

    size_t partId = 0;
    ui64 partValue = 0;
    size_t digitCount = 0;

    for (size_t i = 0; i < buf.Size(); ++i) {
        const char c = buf.Data()[i];

        if (!shortForm && (i == 8 || i == 13 || i == 18 || i == 23)) {
            if (c == '-') {
                continue;
            } else {
                return false;
            }
        }

        ui32 digit = 0;
        if (!GetDigit(c, digit)) {
            return false;
        }

        partValue = partValue * 16 + digit;

        if (++digitCount == 4) {
            dw[partId++] = partValue;
            digitCount = 0;
        }
    }

    std::swap(dw[0], dw[1]);
    for (ui32 i = 4; i < 8; ++i) {
        dw[i] = ((dw[i] >> 8) & 0xff) | ((dw[i] & 0xff) << 8);
    }

    return true;
}

NUdf::TUnboxedValuePod ParseUuid(NUdf::TStringRef buf, bool shortForm) {
    ui16 dw[8];

    if (!ParseUuidToArray(buf, dw, shortForm)) {
        return NUdf::TUnboxedValuePod();
    }

    return MakeString(NUdf::TStringRef(reinterpret_cast<char*>(dw), sizeof(dw)));
}

bool ParseUuid(NUdf::TStringRef buf, void* out, bool shortForm) {
    ui16 dw[8];

    if (!ParseUuidToArray(buf, dw, shortForm)) {
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
    if (!ParseNumber(pos, buf, year) || pos == buf.Size() || buf.Data()[pos] != '-') {
        return NUdf::TUnboxedValuePod();
    }

    // skip '-'
    ++pos;
    if (!ParseNumber(pos, buf, month) || pos == buf.Size() || buf.Data()[pos] != '-') {
        return NUdf::TUnboxedValuePod();
    }

    // skip '-'
    ++pos;
    if (!ParseNumber(pos, buf, day) || pos != buf.Size()) {
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
    if (!ParseNumber(pos, buf, year) || pos == buf.size() || buf.data()[pos] != '-') {
        return NUdf::TUnboxedValuePod();
    }

    // skip '-'
    ++pos;
    if (!ParseNumber(pos, buf, month) || pos == buf.size() || buf.data()[pos] != '-') {
        return NUdf::TUnboxedValuePod();
    }

    // skip '-'
    ++pos;
    if (!ParseNumber(pos, buf, day) || pos != buf.size()) {
        return NUdf::TUnboxedValuePod();
    }

    ui32 absoluteSeconds;
    if (!FromLocalTimeValidated(*tzId, year, month, day, 0, 0, 0, absoluteSeconds)) {
        return NUdf::TUnboxedValuePod();
    }

    ui16 value = absoluteSeconds / 86400u;
    NUdf::TUnboxedValuePod out(value);
    out.SetTimezoneId(*tzId);
    return out;
}

NUdf::TUnboxedValuePod ParseDatetime(NUdf::TStringRef buf) {
    ui32 year, month, day;
    ui32 pos = 0;
    if (!ParseNumber(pos, buf, year) || pos == buf.Size() || buf.Data()[pos] != '-') {
        return NUdf::TUnboxedValuePod();
    }

    // skip '-'
    ++pos;
    if (!ParseNumber(pos, buf, month) || pos == buf.Size() || buf.Data()[pos] != '-') {
        return NUdf::TUnboxedValuePod();
    }

    // skip '-'
    ++pos;
    if (!ParseNumber(pos, buf, day) || pos == buf.Size() || buf.Data()[pos] != 'T') {
        return NUdf::TUnboxedValuePod();
    }

    ui16 dateValue;
    if (!MakeDate(year, month, day, dateValue)) {
        return NUdf::TUnboxedValuePod();
    }

    ui32 hour, minute, second;
    // skip 'T'
    ++pos;
    if (!ParseNumber(pos, buf, hour) || pos == buf.Size() || buf.Data()[pos] != ':') {
        return NUdf::TUnboxedValuePod();
    }

    // skip ':'
    ++pos;
    if (!ParseNumber(pos, buf, minute) || pos == buf.Size() || buf.Data()[pos] != ':') {
        return NUdf::TUnboxedValuePod();
    }

    // skip ':'
    ++pos;
    if (!ParseNumber(pos, buf, second) || pos == buf.Size() || buf.Data()[pos] != 'Z') {
        return NUdf::TUnboxedValuePod();
    }

    // skip 'Z'
    ++pos;
    if (pos != buf.Size()) {
        return NUdf::TUnboxedValuePod();
    }

    ui32 timeValue;
    if (!MakeTime(hour, minute, second, timeValue)) {
        return NUdf::TUnboxedValuePod();
    }

    ui32 value = dateValue * 86400u + timeValue;
    if (value >= NUdf::MAX_DATETIME) {
        return NUdf::TUnboxedValuePod();
    }

    return NUdf::TUnboxedValuePod(value);
}

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
    if (!ParseNumber(pos, buf, year) || pos == buf.size() || buf.data()[pos] != '-') {
        return NUdf::TUnboxedValuePod();
    }

    // skip '-'
    ++pos;
    if (!ParseNumber(pos, buf, month) || pos == buf.size() || buf.data()[pos] != '-') {
        return NUdf::TUnboxedValuePod();
    }

    // skip '-'
    ++pos;
    if (!ParseNumber(pos, buf, day) || pos == buf.size() || buf.data()[pos] != 'T') {
        return NUdf::TUnboxedValuePod();
    }

    ui32 hour, minute, second;
    // skip 'T'
    ++pos;
    if (!ParseNumber(pos, buf, hour) || pos == buf.size() || buf.data()[pos] != ':') {
        return NUdf::TUnboxedValuePod();
    }

    // skip ':'
    ++pos;
    if (!ParseNumber(pos, buf, minute) || pos == buf.size() || buf.data()[pos] != ':') {
        return NUdf::TUnboxedValuePod();
    }

    // skip ':'
    ++pos;
    if (!ParseNumber(pos, buf, second) || pos != buf.size()) {
        return NUdf::TUnboxedValuePod();
    }

    ui32 absoluteSeconds;
    if (!FromLocalTimeValidated(*tzId, year, month, day, hour, minute, second, absoluteSeconds)) {
        return NUdf::TUnboxedValuePod();
    }

    ui32 value = absoluteSeconds;
    NUdf::TUnboxedValuePod out(value);
    out.SetTimezoneId(*tzId);
    return out;
}

NUdf::TUnboxedValuePod ParseTimestamp(NUdf::TStringRef buf) {
    ui32 year, month, day;
    ui32 pos = 0;
    if (!ParseNumber(pos, buf, year) || pos == buf.Size() || buf.Data()[pos] != '-') {
        return NUdf::TUnboxedValuePod();
    }

    // skip '-'
    ++pos;
    if (!ParseNumber(pos, buf, month) || pos == buf.Size() || buf.Data()[pos] != '-') {
        return NUdf::TUnboxedValuePod();
    }

    // skip '-'
    ++pos;
    if (!ParseNumber(pos, buf, day) || pos == buf.Size() || buf.Data()[pos] != 'T') {
        return NUdf::TUnboxedValuePod();
    }

    ui16 dateValue;
    if (!MakeDate(year, month, day, dateValue)) {
        return NUdf::TUnboxedValuePod();
    }

    ui32 hour, minute, second;
    // skip 'T'
    ++pos;
    if (!ParseNumber(pos, buf, hour) || pos == buf.Size() || buf.Data()[pos] != ':') {
        return NUdf::TUnboxedValuePod();
    }

    // skip ':'
    ++pos;
    if (!ParseNumber(pos, buf, minute) || pos == buf.Size() || buf.Data()[pos] != ':') {
        return NUdf::TUnboxedValuePod();
    }

    // skip ':'
    ++pos;
    if (!ParseNumber(pos, buf, second) || pos == buf.Size()) {
        return NUdf::TUnboxedValuePod();
    }

    ui32 microseconds = 0;
    if (buf.Data()[pos] != 'Z') {
        if (buf.Data()[pos] != '.') {
            return NUdf::TUnboxedValuePod();
        }

        ++pos;
        ui32 prevPos = pos;
        if (!ParseNumber(pos, buf, microseconds)) {
            return NUdf::TUnboxedValuePod();
        }

        prevPos = pos - prevPos;
        if (prevPos > 6) {
            return NUdf::TUnboxedValuePod();
        }

        while (prevPos < 6) {
            microseconds *= 10;
            ++prevPos;
        }

        if (pos == buf.Size() || buf.Data()[pos] != 'Z') {
            return NUdf::TUnboxedValuePod();
        }
    }

    // skip 'Z'
    ++pos;
    if (pos != buf.Size()) {
        return NUdf::TUnboxedValuePod();
    }

    ui32 timeValue;
    if (!MakeTime(hour, minute, second, timeValue)) {
        return NUdf::TUnboxedValuePod();
    }

    ui64 value = dateValue * 86400000000ull + timeValue * 1000000ull + microseconds;
    if (value >= NUdf::MAX_TIMESTAMP) {
        return NUdf::TUnboxedValuePod();
    }

    return NUdf::TUnboxedValuePod(value);
}

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
    if (!ParseNumber(pos, buf, year) || pos == buf.size() || buf.data()[pos] != '-') {
        return NUdf::TUnboxedValuePod();
    }

    // skip '-'
    ++pos;
    if (!ParseNumber(pos, buf, month) || pos == buf.size() || buf.data()[pos] != '-') {
        return NUdf::TUnboxedValuePod();
    }

    // skip '-'
    ++pos;
    if (!ParseNumber(pos, buf, day) || pos == buf.size() || buf.data()[pos] != 'T') {
        return NUdf::TUnboxedValuePod();
    }

    ui32 hour, minute, second;
    // skip 'T'
    ++pos;
    if (!ParseNumber(pos, buf, hour) || pos == buf.size() || buf.data()[pos] != ':') {
        return NUdf::TUnboxedValuePod();
    }

    // skip ':'
    ++pos;
    if (!ParseNumber(pos, buf, minute) || pos == buf.size() || buf.data()[pos] != ':') {
        return NUdf::TUnboxedValuePod();
    }

    // skip ':'
    ++pos;
    if (!ParseNumber(pos, buf, second)) {
        return NUdf::TUnboxedValuePod();
    }

    ui32 microseconds = 0;
    if (pos != buf.size()) {
        if (buf.data()[pos] != '.') {
            return NUdf::TUnboxedValuePod();
        }

        ++pos;
        ui32 prevPos = pos;
        if (!ParseNumber(pos, buf, microseconds)) {
            return NUdf::TUnboxedValuePod();
        }

        prevPos = pos - prevPos;
        if (prevPos > 6) {
            return NUdf::TUnboxedValuePod();
        }

        while (prevPos < 6) {
            microseconds *= 10;
            ++prevPos;
        }

        if (pos != buf.size()) {
            return NUdf::TUnboxedValuePod();
        }
    }

    ui32 absoluteSeconds;
    if (!FromLocalTimeValidated(*tzId, year, month, day, hour, minute, second, absoluteSeconds)) {
        return NUdf::TUnboxedValuePod();
    }

    const ui64 value = absoluteSeconds * 1000000ull + microseconds;
    NUdf::TUnboxedValuePod out(value);
    out.SetTimezoneId(*tzId);
    return out;
}

template <bool DecimalPart = false>
bool ParseNumber(std::string_view::const_iterator& pos, const std::string_view& buf, ui32& value) {
    value = 0U;

    if (buf.cend() == pos || !std::isdigit(*pos)) {
        return false;
    }

    auto digits = 6U;
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
        if (!ParseNumber(pos, buf, num)) {
            return NUdf::TUnboxedValuePod();
        }

        switch (*pos++) {
            case 'D': days = num; break;
            case 'W': days = 7U * num; break;
            default: return NUdf::TUnboxedValuePod();
        }
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

    if (value >= NUdf::MAX_TIMESTAMP) {
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
        return IsValidUuid(buf);

    case NUdf::EDataSlot::DyNumber:
        return NDyNumber::IsValidDyNumberString(buf);

    case NUdf::EDataSlot::Date:
    case NUdf::EDataSlot::Datetime:
    case NUdf::EDataSlot::Timestamp:
    case NUdf::EDataSlot::Interval:
    case NUdf::EDataSlot::TzDate:
    case NUdf::EDataSlot::TzDatetime:
    case NUdf::EDataSlot::TzTimestamp:
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
        return ParseInterval(buf);

    case NUdf::EDataSlot::TzDate:
        return ParseTzDate(buf);

    case NUdf::EDataSlot::TzDatetime:
        return ParseTzDatetime(buf);

    case NUdf::EDataSlot::TzTimestamp:
        return ParseTzTimestamp(buf);

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
    default:
        break;
    }

    MKQL_ENSURE(false, "Incorrect data slot: " << (ui32)type);
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
            Y_FAIL("TODO");

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
    case NUdf::EDataSlot::Decimal:
    case NUdf::EDataSlot::Uuid:
    case NUdf::EDataSlot::DyNumber:
    case NUdf::EDataSlot::JsonDocument:
        Y_FAIL("TODO");
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

} // namespace NMiniKQL
} // namespace NKikimr
