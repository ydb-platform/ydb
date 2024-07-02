#pragma once

// #include <ydb/library/yql/public/udf/tz/udf_tz.h>

#include <util/datetime/base.h>
// #include <util/string/printf.h>

namespace NYql::DateTime {

struct TTM64Storage {
    int Year : 19;
    unsigned int DayOfYear : 9;
    unsigned int WeekOfYear : 6;
    unsigned int WeekOfYearIso8601 : 6;
    unsigned int DayOfWeek : 3;
    unsigned int Month : 4;
    unsigned int Day : 5;
    unsigned int Hour : 5;
    unsigned int Minute : 6;
    unsigned int Second : 6;
    unsigned int Microsecond : 20;
    unsigned int TimezoneId : 16;

    TTM64Storage() {
        Zero(*this);
    }

    inline static bool IsUniversal(ui16 timezoneId) {
        return timezoneId == 0;
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

    void FromDate32(i32 value, ui16 timezoneId = 0);

};

}
