#include "datetime64.h"

#include <ydb/library/yql/minikql/mkql_type_ops.h>

namespace NYql::DateTime {

void TTM64Storage::FromDate32(i32 value) {
    i32 year;
    ui32 month, day, dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek;

    NKikimr::NMiniKQL::SplitDate32(
            value, year, month, day, dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek);

    TimezoneId = 0;
    Year = year;
    Month = month;
    Day = day;
    DayOfYear = dayOfYear;
    WeekOfYear = weekOfYear;
    WeekOfYearIso8601 = weekOfYearIso8601;
    DayOfWeek = dayOfWeek;
}

void TTM64Storage::FromTzDate32(i32 value, ui16 tzId) {
    i32 year;
    ui32 month, day, dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek;

    NKikimr::NMiniKQL::SplitTzDate32(
            value, year, month, day, dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek, tzId);

    TimezoneId = tzId;
    Year = year;
    Month = month;
    Day = day;
    DayOfYear = dayOfYear;
    WeekOfYear = weekOfYear;
    WeekOfYearIso8601 = weekOfYearIso8601;
    DayOfWeek = dayOfWeek;
}

void TTM64Storage::FromDatetime64(i64 value) {
    i32 year;
    ui32 month, day, hour, minute, second, dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek;

    NKikimr::NMiniKQL::SplitDatetime64(
            value, year, month, day, hour, minute, second,
            dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek);

    TimezoneId = 0;
    Year = year;
    Month = month;
    Day = day;
    Hour = hour;
    Minute = minute;
    Second = second;
    DayOfYear = dayOfYear;
    WeekOfYear = weekOfYear;
    WeekOfYearIso8601 = weekOfYearIso8601;
    DayOfWeek = dayOfWeek;
}

void TTM64Storage::FromTzDatetime64(i64 value, ui16 tzId) {
    i32 year;
    ui32 month, day, dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek;
    ui32 hour, minute, second;

    NKikimr::NMiniKQL::SplitTzDatetime64(
            value, year, month, day, hour, minute, second,
            dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek, tzId);

    TimezoneId = tzId;
    Year = year;
    Month = month;
    Day = day;
    Hour = hour;
    Minute = minute;
    Second = second;
    DayOfYear = dayOfYear;
    WeekOfYear = weekOfYear;
    WeekOfYearIso8601 = weekOfYearIso8601;
    DayOfWeek = dayOfWeek;
}

void TTM64Storage::FromTimestamp64(i64 value) {
    i32 year;
    ui32 month, day, dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek;
    ui32 hour, minute, second, usec;

    NKikimr::NMiniKQL::SplitTimestamp64(
            value, year, month, day, hour, minute, second, usec, dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek);

    TimezoneId = 0;
    Year = year;
    Month = month;
    Day = day;
    Hour = hour;
    Minute = minute;
    Second = second;
    Microsecond = usec;
    DayOfYear = dayOfYear;
    WeekOfYear = weekOfYear;
    WeekOfYearIso8601 = weekOfYearIso8601;
    DayOfWeek = dayOfWeek;
}

void TTM64Storage::FromTzTimestamp64(i64 value, ui16 tzId) {
    i32 year;
    ui32 month, day, dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek;
    ui32 hour, minute, second, usec;

    NKikimr::NMiniKQL::SplitTzTimestamp64(
            value, year, month, day, hour, minute, second, usec, dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek, tzId);

    TimezoneId = tzId;
    Year = year;
    Month = month;
    Day = day;
    Hour = hour;
    Minute = minute;
    Second = second;
    Microsecond = usec;
    DayOfYear = dayOfYear;
    WeekOfYear = weekOfYear;
    WeekOfYearIso8601 = weekOfYearIso8601;
    DayOfWeek = dayOfWeek;
}

i32 TTM64Storage::ToDate32() const {
    Y_ASSERT(IsUniversal(TimezoneId));
    i32 date;
    if (!NKikimr::NMiniKQL::MakeDate32(Year, Month, Day, date)) {
        ythrow yexception() << "Error in MakeDate32";
    }
    return date;
}

i32 TTM64Storage::ToTzDate32() const {
    i32 value;
    if (!NKikimr::NMiniKQL::MakeTzDate32(Year, Month, Day, value, TimezoneId)) {
        ythrow yexception() << "Error in MakeTzDate32";
    }
    return value;
}

i64 TTM64Storage::ToDatetime64() const {
    Y_ASSERT(IsUniversal(TimezoneId));
    i64 value;
    if (!NKikimr::NMiniKQL::MakeDatetime64(Year, Month, Day, Hour, Minute, Second, value)) {
        ythrow yexception() << "Error in MakeDatetime64";
    }
    return value;
}

i64 TTM64Storage::ToTzDatetime64() const {
    i64 value;
    if (!NKikimr::NMiniKQL::MakeTzDatetime64(Year, Month, Day, Hour, Minute, Second, value, TimezoneId)) {
        ythrow yexception() << "Error in MakeTzDatetime64";
    }
    return value;
}

}
