#pragma once

// #include <ydb/library/yql/public/udf/tz/udf_tz.h>

#include <util/datetime/base.h>
// #include <util/string/printf.h>

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
    void FromDate32(i32 value, ui16 tzId = 0);
    i32 ToDate32() const; // TODO add arg "local"?
};

}
