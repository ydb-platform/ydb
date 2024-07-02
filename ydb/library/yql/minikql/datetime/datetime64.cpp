#include "datetime64.h"

#include <ydb/library/yql/minikql/mkql_type_ops.h>

namespace NYql::DateTime {

void TTM64Storage::FromDate32(i32 value, ui16 timezoneId) {
    i32 year;
    ui32 month, day; // , dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek;

    if (!NKikimr::NMiniKQL::SplitDate32(value, year, month, day)) {
        ythrow yexception() << "Error in SplitDate32";
    }

    TimezoneId = timezoneId;

    Year = year;
    Month = month;
    Day = day;

    /* TODO impl enrich
    DayOfYear = dayOfYear;
    WeekOfYear = weekOfYear;
    WeekOfYearIso8601 = weekOfYearIso8601;
    DayOfWeek = dayOfWeek;
    */
}

}
