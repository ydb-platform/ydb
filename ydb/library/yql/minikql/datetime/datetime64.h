#pragma once

#include <util/datetime/base.h>

#include "datetime.h"

namespace NYql::DateTime {

struct TTM64Storage {
    i32 Year : 19;
    ui32 DayOfYear : 9;
    ui32 WeekOfYear : 6;
    ui32 WeekOfYearIso8601 : 6;
    ui32 DayOfWeek : 3;
    ui32 Month : 4;
    ui32 Day : 5;
    ui32 Hour : 5;
    ui32 Minute : 6;
    ui32 Second : 6;
    ui32 Microsecond : 20;
    ui32 TimezoneId : 16;

    TTM64Storage() {
        Zero(*this);
    }

    static bool IsUniversal(ui16 tzId) {
        return tzId == 0;
    }

    void MakeDefault() {
        Year = 1970;
        Month = 1;
        Day = 1;
        Hour = 0;
        Minute = 0;
        Second = 0;
        Microsecond = 0;
        TimezoneId = 0;
    }

    void From(const TTMStorage& narrow) {
        Year = narrow.Year;
        DayOfYear = narrow.DayOfYear;
        WeekOfYear = narrow.WeekOfYear;
        WeekOfYearIso8601 = narrow.WeekOfYearIso8601;
        DayOfWeek = narrow.DayOfWeek;
        Month = narrow.Month;
        Day = narrow.Day;
        Hour = narrow.Hour;
        Minute = narrow.Minute;
        Second = narrow.Second;
        Microsecond = narrow.Microsecond;
        TimezoneId = narrow.TimezoneId;
    }

    void FromDate32(const NUdf::IDateBuilder& builder, i32 value, ui16 tzId = 0) {
        i32 year;
        ui32 month, day, dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek;

        if (!builder.SplitTzDate32(value, year, month, day, dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek, tzId)) {
            ythrow yexception() << "Error in SplitTzDate32";
        }

        TimezoneId = tzId;
        Year = year;
        Month = month;
        Day = day;
        DayOfYear = dayOfYear;
        WeekOfYear = weekOfYear;
        WeekOfYearIso8601 = weekOfYearIso8601;
        DayOfWeek = dayOfWeek;
    }

    void FromDatetime64(const NUdf::IDateBuilder& builder, i64 value, ui16 tzId = 0) {
        i32 year;
        ui32 month, day, dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek;
        ui32 hour, minute, second;

        if (!builder.SplitTzDatetime64(
                value, year, month, day, hour, minute, second,
                dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek, tzId))
        {
            ythrow yexception() << "Error in SplitTzDatetime64";
        }

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

    void FromTimestamp64(const NUdf::IDateBuilder& builder, i64 value, ui16 tzId = 0) {
        i64 datetime = value / 1000000ll;
        if (value % 1000000ll < 0) {
            datetime -= 1;
        }
        FromDatetime64(builder, datetime, tzId);
        Microsecond = value - datetime * 1000000ll;
    }

    i32 ToDate32(const NUdf::IDateBuilder& builder) const {
        i32 date;
        if (!builder.MakeTzDate32(Year, Month, Day, date, TimezoneId)) {
            ythrow yexception() << "Error in MakeTzDate32";
        }
        return date;
    }

    i64 ToDatetime64(const NUdf::IDateBuilder& builder) const {
        i64 datetime;
        if (!builder.MakeTzDatetime64(Year, Month, Day, Hour, Minute, Second, datetime, TimezoneId)) {
            ythrow yexception() << "Error in MakeTzDatetime64";
        }
        return datetime;
    }

    i64 ToTimestamp64(const NUdf::IDateBuilder& builder) const {
        return ToDatetime64(builder) * 1000000ll + Microsecond;
    }

};

}
