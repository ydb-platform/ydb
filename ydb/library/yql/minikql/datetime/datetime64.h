#pragma once

#include <util/datetime/base.h>

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

    inline static bool IsUniversal(ui16 tzId) {
        return tzId == 0;
    }

    inline void MakeDefault() {
        Year = 1970;
        Month = 1;
        Day = 1;
        Hour = 0;
        Minute = 0;
        Second = 0;
        Microsecond = 0;
        TimezoneId = 0;
    }

    // TODO add to UDF ABI interface
    void FromDate32(i32 value);
    void FromTzDate32(i32 value, ui16 tzId);

    void FromDatetime64(i64 value);
    void FromTzDatetime64(i64 value, ui16 tzId);

    void FromTimestamp64(i64 value);
    void FromTzTimestamp64(i64 value, ui16 tzId);

    i32 ToDate32() const;
    i32 ToTzDate32() const;

    i64 ToDatetime64() const;
    i64 ToTzDatetime64() const;

    inline i64 ToTimestamp64() const {
        return ToDatetime64() * 1000000ll + Microsecond;
    }

    inline i64 ToTzTimestamp64() const {
        return ToTzDatetime64() * 1000000ll + Microsecond;
    }
};

}
