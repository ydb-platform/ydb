#include <ydb/library/yql/public/udf/udf_helpers.h>

#include <util/datetime/base.h>
#include <util/draft/datetime.h>
#include <util/string/cast.h>

#include <library/cpp/timezone_conversion/convert.h>

#include <unordered_map>

using namespace NKikimr;
using namespace NUdf;
using namespace NDatetime;

namespace {
    SIMPLE_STRICT_UDF(TToString, char*(TAutoMap<TTimestamp>)) {
        const auto input = args[0].Get<ui64>();
        TInstant instant = TInstant::MicroSeconds(input);
        return valueBuilder->NewString(instant.ToString());
    }

    SIMPLE_STRICT_UDF(TToStringUpToSeconds, char*(TAutoMap<TTimestamp>)) {
        const auto input = args[0].Get<ui64>();
        TInstant instant = TInstant::MicroSeconds(input);
        return valueBuilder->NewString(instant.ToStringUpToSeconds());
    }

    SIMPLE_STRICT_UDF(TToStringFormat, char*(TAutoMap<TTimestamp>, char*)) {
        const auto input = args[0].Get<ui64>();
        const TString format(args[1].AsStringRef());
        TInstant instant = TInstant::MicroSeconds(input);
        TSimpleTM tm = TSimpleTM::New(static_cast<time_t>(instant.Seconds()));
        return valueBuilder->NewString(tm.ToString(format.c_str()));
    }

    SIMPLE_STRICT_UDF(TToDate, char*(TAutoMap<TTimestamp>)) {
        const auto input = args[0].Get<ui64>();
        TInstant instant = TInstant::MicroSeconds(input);
        TSimpleTM tm = TSimpleTM::New(static_cast<time_t>(instant.Seconds()));
        return valueBuilder->NewString(tm.ToString("%Y-%m-%d"));
    }

    SIMPLE_UDF(TToTimeZone, ui64(TAutoMap<TTimestamp>, char*)) {
        Y_UNUSED(valueBuilder);
        const auto input = args[0].Get<ui64>();
        TInstant instant = TInstant::MicroSeconds(input);
        const TString tz_name(args[1].AsStringRef());
        TTimeZone tz = GetTimeZone(tz_name);
        TTimeZone gmt_tz = GetTimeZone("GMT+0");
        TSimpleTM ct = ToCivilTime(instant, tz);
        ui64 result = ToAbsoluteTime(ct, gmt_tz).MicroSeconds() + instant.MicroSecondsOfSecond();
        return TUnboxedValuePod(result);
    }

    SIMPLE_UDF(TFromTimeZone, ui64(TAutoMap<ui64>, char*)) {
        Y_UNUSED(valueBuilder);
        const auto input = args[0].Get<ui64>();
        TInstant instant = TInstant::MicroSeconds(input);
        const TString tz_name(args[1].AsStringRef());
        TTimeZone tz = GetTimeZone(tz_name);
        TSimpleTM tm = TSimpleTM::New(static_cast<time_t>(instant.Seconds()));
        TInstant at = ToAbsoluteTime(tm, tz);
        ui64 result = at.MicroSeconds() + instant.MicroSecondsOfSecond();
        return TUnboxedValuePod(result);
    }

    SIMPLE_UDF(TTimestampFromTimeZone, TTimestamp(TAutoMap<ui64>, char*)) {
        Y_UNUSED(valueBuilder);
        const auto input = args[0].Get<ui64>();
        TInstant instant = TInstant::MicroSeconds(input);
        const TString tz_name(args[1].AsStringRef());
        TTimeZone tz = GetTimeZone(tz_name);
        TSimpleTM tm = TSimpleTM::New(static_cast<time_t>(instant.Seconds()));
        TInstant at = ToAbsoluteTime(tm, tz);
        ui64 result = at.MicroSeconds() + instant.MicroSecondsOfSecond();
        return TUnboxedValuePod(result);
    }

    SIMPLE_STRICT_UDF(TIsWeekend, bool(TAutoMap<TTimestamp>)) {
        Y_UNUSED(valueBuilder);
        const auto input = args[0].Get<ui64>();
        TInstant instant = TInstant::MicroSeconds(input);
        TSimpleTM tm = TSimpleTM::New(static_cast<time_t>(instant.Seconds()));
        return TUnboxedValuePod(tm.WDay == 0 || tm.WDay == 6);
    }

    bool TryStrptime(const TString& input, const TString& format, TInstant& result) {
        struct tm inputTm;
        memset(&inputTm, 0, sizeof(tm));
        inputTm.tm_mday = 1;
        if (strptime(input.data(), format.data(), &inputTm) != nullptr) {
            const time_t seconds = TimeGM(&inputTm);
            if (seconds != static_cast<time_t>(-1)) {
                result = TInstant::Seconds(seconds);
                return true;
            }
        }
        return false;
    }

#define TRY_PARSE(mode)                                    \
    if (!success) {                                        \
        success = TInstant::TryParse##mode(input, result); \
    }

#define TRY_STRPTIME(format)                          \
    if (!success) {                                   \
        success = TryStrptime(input, format, result); \
    }

    TUnboxedValue FromStringImpl(const TUnboxedValuePod* args) {
        EMPTY_RESULT_ON_EMPTY_ARG(0);
        TString input(args[0].AsStringRef());
        bool success = false;
        TInstant result;
        ui64 bonus = 0;

        const static std::unordered_map<size_t, TString> iso8601withoutTzMap = {
            {7, "%Y-%m"},
            {10, "%Y-%m-%d"},
            {16, "%Y-%m-%d %H:%M"},
            {19, "%Y-%m-%d %H:%M:%S"},
            {6, "%Y%m"},
            {8, "%Y%m%d"},
            {12, "%Y%m%d%H%M"},
            {14, "%Y%m%d%H%M%S"}};

        if (input.length() == 26) {
            if (!TryFromString<ui64>(input.substr(20), bonus)) {
                return TUnboxedValuePod();
            }

            input = input.substr(0, 19);
        }

        const auto& iso8601withoutTzPattern = iso8601withoutTzMap.find(input.length());
        if (iso8601withoutTzPattern != iso8601withoutTzMap.end()) {
            TRY_STRPTIME((*iso8601withoutTzPattern).second);
        }

        TRY_PARSE(HttpDeprecated)
        TRY_PARSE(Iso8601Deprecated)
        TRY_PARSE(Rfc822Deprecated)
        TRY_PARSE(X509Deprecated)

        TRY_STRPTIME("%d/%h/%Y:%H:%M:%S")
        TRY_STRPTIME("%a %h %d %H:%M:%S %Y")

        return success ? TUnboxedValuePod(result.MicroSeconds() + bonus) : TUnboxedValuePod();
    }

    SIMPLE_STRICT_UDF(TFromString, TOptional<ui64>(TOptional<char*>)) {
        Y_UNUSED(valueBuilder);
        return FromStringImpl(args);
    }

    SIMPLE_STRICT_UDF(TTimestampFromString, TOptional<TTimestamp>(TOptional<char*>)) {
        Y_UNUSED(valueBuilder);
        return FromStringImpl(args);
    }

    TUnboxedValue FromStringFormatImpl(const TUnboxedValuePod* args) {
        EMPTY_RESULT_ON_EMPTY_ARG(0);
        const TString input(args[0].AsStringRef());
        const TString format(args[1].AsStringRef());
        TInstant result;
        bool success = TryStrptime(input, format, result);

        return success ? TUnboxedValuePod(result.MicroSeconds()) : TUnboxedValuePod();
    }

    SIMPLE_STRICT_UDF(TFromStringFormat, TOptional<ui64>(TOptional<char*>, char*)) {
        Y_UNUSED(valueBuilder);
        return FromStringFormatImpl(args);
    }

    SIMPLE_STRICT_UDF(TTimestampFromStringFormat, TOptional<TTimestamp>(TOptional<char*>, char*)) {
        Y_UNUSED(valueBuilder);
        return FromStringFormatImpl(args);
    }

    SIMPLE_STRICT_UDF(TDateStartOfDay, TDate(TAutoMap<TDate>)) {
        Y_UNUSED(valueBuilder);
        const auto input = args[0].Get<ui16>();
        return TUnboxedValuePod(ui16(input));
    }

    SIMPLE_STRICT_UDF(TDatetimeStartOfDay, TDatetime(TAutoMap<TDatetime>)) {
        Y_UNUSED(valueBuilder);
        const auto input = args[0].Get<ui32>();
        return TUnboxedValuePod(ui32(input - input % 86400));
    }

    SIMPLE_STRICT_UDF(TTimestampStartOfDay, TTimestamp(TAutoMap<TTimestamp>)) {
        Y_UNUSED(valueBuilder);
        const auto input = args[0].Get<ui64>();
        return TUnboxedValuePod(ui64(input - input % 86400000000ull));
    }

    SIMPLE_STRICT_UDF(TGetTimeOfDay, TInterval(TAutoMap<TTimestamp>)) {
        Y_UNUSED(valueBuilder);
        const auto input = args[0].Get<ui64>();
        return TUnboxedValuePod(ui64(input % 86400000000ull));
    }

#define DATETIME_TO_UDF(unit, type)                                 \
    SIMPLE_STRICT_UDF(TTo##unit, type(TAutoMap<TTimestamp>)) {      \
        Y_UNUSED(valueBuilder);                                     \
        const ui64 input = args[0].Get<ui64>();                     \
        TInstant instant = TInstant::MicroSeconds(input);           \
        return TUnboxedValuePod(static_cast<type>(instant.unit())); \
    }

#define DATETIME_INTERVAL_TO_UDF(unit, type)                                                         \
    SIMPLE_STRICT_UDF(TIntervalTo##unit, type(TAutoMap<TInterval>)) {                                \
        Y_UNUSED(valueBuilder);                                                                      \
        const i64 input = args[0].Get<i64>();                                                        \
        TDuration duration = TDuration::MicroSeconds(std::abs(input));                               \
        return TUnboxedValuePod(static_cast<type>(input >= 0 ? duration.unit() : -duration.unit())); \
    }

#define DATETIME_FROM_UDF(unit)                                        \
    SIMPLE_STRICT_UDF(TFrom##unit, ui64(TAutoMap<ui64>)) {             \
        Y_UNUSED(valueBuilder);                                        \
        EMPTY_RESULT_ON_EMPTY_ARG(0);                                  \
        const auto input = args[0].Get<ui64>();                        \
        return TUnboxedValuePod(TInstant::unit(input).MicroSeconds()); \
    }

#define DATETIME_TIMESTAMP_FROM_UDF(unit)                                             \
    SIMPLE_STRICT_UDF(TTimestampFrom##unit, TOptional<TTimestamp>(TOptional<ui64>)) { \
        Y_UNUSED(valueBuilder);                                                       \
        EMPTY_RESULT_ON_EMPTY_ARG(0);                                                 \
        const auto input = args[0].Get<ui64>();                                       \
        ui64 result = TInstant::unit(input).MicroSeconds();                           \
        if (result < MAX_TIMESTAMP) {                                                 \
            return TUnboxedValuePod(result);                                          \
        } else {                                                                      \
            return TUnboxedValuePod();                                                \
        }                                                                             \
    }

#define DATETIME_INTERVAL_FROM_UDF(unit)                                           \
    SIMPLE_STRICT_UDF(TIntervalFrom##unit, TOptional<TInterval>(TOptional<i64>)) { \
        Y_UNUSED(valueBuilder);                                                    \
        EMPTY_RESULT_ON_EMPTY_ARG(0);                                              \
        const auto input = args[0].Get<i64>();                                     \
        i64 result = TInstant::unit(std::abs(input)).MicroSeconds();               \
        if (static_cast<ui64>(result) < MAX_TIMESTAMP) {                           \
            return TUnboxedValuePod(input >= 0 ? result : -result);                \
        } else {                                                                   \
            return TUnboxedValuePod();                                             \
        }                                                                          \
    }

#define DATETIME_GET_UDF(udfName, resultType, result)                   \
    SIMPLE_STRICT_UDF(udfName, resultType(TAutoMap<TTimestamp>)) {      \
        Y_UNUSED(valueBuilder);                                         \
        const auto input = args[0].Get<ui64>();                         \
        const TInstant& instant = TInstant::MicroSeconds(input);        \
        TSimpleTM tm = TSimpleTM::New(                                  \
            static_cast<time_t>(instant.Seconds()));                    \
        Y_UNUSED(tm);                                                   \
        return TUnboxedValuePod(result);                                \
    }

#define DATETIME_GET_STRING_UDF(udfName, result)                        \
    SIMPLE_STRICT_UDF(udfName, char*(TAutoMap<TTimestamp>)) {           \
        const auto input = args[0].Get<ui64>();                         \
        const TInstant& instant = TInstant::MicroSeconds(input);        \
        TSimpleTM tm = TSimpleTM::New(                                  \
            static_cast<time_t>(instant.Seconds()));                    \
        Y_UNUSED(tm);                                                   \
        return valueBuilder->NewString(result);                         \
    }

    TInstant InstantFromMicroseconds(ui64 value) {
        return TInstant::MicroSeconds(value);
    }

    ui64 InstantToMicroseconds(TInstant value) {
        return value.MicroSeconds();
    }

    TInstant InstantFromSeconds(ui32 value) {
        return TInstant::Seconds(value);
    }

    ui32 InstantToSeconds(TInstant value) {
        return value.Seconds();
    }

    TInstant InstantFromDays(ui16 value) {
        return TInstant::Days(value);
    }

    ui16 InstantToDays(TInstant value) {
        return value.Days();
    }

#define DATETIME_START_UDF(udfName, type, logic, inputConv, outputConv)            \
    SIMPLE_STRICT_UDF(udfName, type(TAutoMap<type>)) {                             \
        Y_UNUSED(valueBuilder);                                                    \
        const auto input = args[0].Get<typename NUdf::TDataType<type>::TLayout>(); \
        TInstant instant = inputConv(input);                                       \
        TSimpleTM tm = TSimpleTM::New(                                             \
            static_cast<time_t>(instant.Seconds()));                               \
        logic;                                                                     \
        return TUnboxedValuePod(outputConv(instant));                              \
    }

#define DATETIME_TO_REGISTER_UDF(unit, ...) TTo##unit,
#define DATETIME_TO_INTERVAL_REGISTER_UDF(unit, ...) TIntervalTo##unit,
#define DATETIME_FROM_REGISTER_UDF(unit) TFrom##unit,
#define DATETIME_TIMESTAMP_FROM_REGISTER_UDF(unit) TTimestampFrom##unit,
#define DATETIME_INTERVAL_FROM_REGISTER_UDF(unit) TIntervalFrom##unit,
#define DATETIME_GET_REGISTER_UDF(udfName, ...) udfName,
#define DATETIME_GET_REGISTER_STRING_UDF(udfName, ...) udfName,
#define DATETIME_START_REGISTER_UDF(udfName, ...) udfName,

#define DATETIME_FROM_UDF_MAP(XX) \
    XX(MicroSeconds)              \
    XX(MilliSeconds)              \
    XX(Seconds)                   \
    XX(Minutes)                   \
    XX(Hours)                     \
    XX(Days)

#define DATETIME_TO_UDF_MAP(XX) \
    XX(NanoSeconds, ui64)       \
    XX(MicroSeconds, ui64)      \
    XX(MilliSeconds, ui64)      \
    XX(Seconds, ui64)           \
    XX(SecondsFloat, double)    \
    XX(Minutes, ui64)           \
    XX(Hours, ui64)             \
    XX(Days, ui64)

#define DATETIME_INTERVAL_TO_UDF_MAP(XX) \
    XX(NanoSeconds, i64)                 \
    XX(MicroSeconds, i64)                \
    XX(MilliSeconds, i64)                \
    XX(Seconds, i64)                     \
    XX(SecondsFloat, double)             \
    XX(Minutes, i64)                     \
    XX(Hours, i64)                       \
    XX(Days, i64)

#define DATETIME_GET_UDF_MAP(XX)                                \
    XX(TGetYear, ui32, tm.RealYear())                           \
    XX(TGetWeekOfYear, ui8, FromString<ui8>(tm.ToString("%V"))) \
    XX(TGetDayOfYear, ui32, (ui32)tm.YDay)                      \
    XX(TGetMonth, ui8, static_cast<ui8>(tm.Mon + 1))            \
    XX(TGetDayOfMonth, ui8, tm.MDay)                            \
    XX(TGetDayOfWeek, ui8, tm.WDay)                             \
    XX(TGetHour, ui8, tm.Hour)                                  \
    XX(TGetMinute, ui8, tm.Min)                                 \
    XX(TGetSecond, ui8, tm.Sec)                                 \
    XX(TGetMillisecondOfSecond, ui32,                           \
       instant.MilliSecondsOfSecond())                          \
    XX(TGetMicrosecondOfSecond, ui32,                           \
       instant.MicroSecondsOfSecond())

#define DATETIME_GET_UDF_STRING_MAP(XX)  \
    XX(TGetMonthName, tm.ToString("%B")) \
    XX(TGetDayOfWeekName, tm.ToString("%A"))

#define START_OF_WEEK_LOGIC instant -= TDuration::Days(tm.WDay ? tm.WDay - 1 : 6)
#define START_OF_MONTH_LOGIC instant -= TDuration::Days(tm.MDay - 1)
#define START_OF_QUARTER_LOGIC      \
    tm.Mon = tm.Mon - (tm.Mon % 3); \
    tm.MDay = 1;                    \
    tm = tm.RegenerateFields();     \
    instant = TInstant::Seconds(tm.AsTimeT())

#define START_OF_YEAR_LOGIC instant -= TDuration::Days(tm.YDay)

#define DATETIME_START_UDF_MAP(XX)                                                                                   \
    XX(TStartOfWeek, ui64, START_OF_WEEK_LOGIC, InstantFromMicroseconds, InstantToMicroseconds)                      \
    XX(TStartOfMonth, ui64, START_OF_MONTH_LOGIC, InstantFromMicroseconds, InstantToMicroseconds)                    \
    XX(TStartOfQuarter, ui64, START_OF_QUARTER_LOGIC, InstantFromMicroseconds, InstantToMicroseconds)                \
    XX(TStartOfYear, ui64, START_OF_YEAR_LOGIC, InstantFromMicroseconds, InstantToMicroseconds)                      \
    XX(TDateStartOfWeek, TDate, START_OF_WEEK_LOGIC, InstantFromDays, InstantToDays)                                 \
    XX(TDateStartOfMonth, TDate, START_OF_MONTH_LOGIC, InstantFromDays, InstantToDays)                               \
    XX(TDateStartOfQuarter, TDate, START_OF_QUARTER_LOGIC, InstantFromDays, InstantToDays)                           \
    XX(TDateStartOfYear, TDate, START_OF_YEAR_LOGIC, InstantFromDays, InstantToDays)                                 \
    XX(TDatetimeStartOfWeek, TDatetime, START_OF_WEEK_LOGIC, InstantFromSeconds, InstantToSeconds)                   \
    XX(TDatetimeStartOfMonth, TDatetime, START_OF_MONTH_LOGIC, InstantFromSeconds, InstantToSeconds)                 \
    XX(TDatetimeStartOfQuarter, TDatetime, START_OF_QUARTER_LOGIC, InstantFromSeconds, InstantToSeconds)             \
    XX(TDatetimeStartOfYear, TDatetime, START_OF_YEAR_LOGIC, InstantFromSeconds, InstantToSeconds)                   \
    XX(TTimestampStartOfWeek, TTimestamp, START_OF_WEEK_LOGIC, InstantFromMicroseconds, InstantToMicroseconds)       \
    XX(TTimestampStartOfMonth, TTimestamp, START_OF_MONTH_LOGIC, InstantFromMicroseconds, InstantToMicroseconds)     \
    XX(TTimestampStartOfQuarter, TTimestamp, START_OF_QUARTER_LOGIC, InstantFromMicroseconds, InstantToMicroseconds) \
    XX(TTimestampStartOfYear, TTimestamp, START_OF_YEAR_LOGIC, InstantFromMicroseconds, InstantToMicroseconds)

    DATETIME_TO_UDF_MAP(DATETIME_TO_UDF)
    DATETIME_INTERVAL_TO_UDF_MAP(DATETIME_INTERVAL_TO_UDF)
    DATETIME_FROM_UDF_MAP(DATETIME_FROM_UDF)
    DATETIME_FROM_UDF_MAP(DATETIME_TIMESTAMP_FROM_UDF)
    DATETIME_FROM_UDF_MAP(DATETIME_INTERVAL_FROM_UDF)
    DATETIME_GET_UDF_MAP(DATETIME_GET_UDF)
    DATETIME_GET_UDF_STRING_MAP(DATETIME_GET_STRING_UDF)
    DATETIME_START_UDF_MAP(DATETIME_START_UDF)

    SIMPLE_MODULE(TDateTimeModule,
                  DATETIME_TO_UDF_MAP(DATETIME_TO_REGISTER_UDF)
                      DATETIME_INTERVAL_TO_UDF_MAP(DATETIME_TO_INTERVAL_REGISTER_UDF)
                          DATETIME_FROM_UDF_MAP(DATETIME_FROM_REGISTER_UDF)
                              DATETIME_FROM_UDF_MAP(DATETIME_TIMESTAMP_FROM_REGISTER_UDF)
                                  DATETIME_FROM_UDF_MAP(DATETIME_INTERVAL_FROM_REGISTER_UDF)
                                      DATETIME_GET_UDF_MAP(DATETIME_GET_REGISTER_UDF)
                                          DATETIME_GET_UDF_STRING_MAP(DATETIME_GET_REGISTER_STRING_UDF)
                                              DATETIME_START_UDF_MAP(DATETIME_START_REGISTER_UDF)
                                                  TFromString,
                  TTimestampFromString,
                  TFromStringFormat, TTimestampFromStringFormat,
                  TToString, TToStringUpToSeconds,
                  TToStringFormat, TToDate,
                  TToTimeZone, TFromTimeZone, TTimestampFromTimeZone,
                  TIsWeekend, TDateStartOfDay, TDatetimeStartOfDay, TTimestampStartOfDay,
                  TGetTimeOfDay)
}

REGISTER_MODULES(TDateTimeModule)
