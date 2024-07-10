#include "datetime64.h"

#include <ydb/library/yql/minikql/mkql_type_ops.h>

namespace NYql::DateTime {

void TTM64Storage::FromDate32(i32 value, ui16 tzId) {
    i32 year;
    ui32 month, day, dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek;

    NKikimr::NMiniKQL::FullSplitDate32(
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

i32 TTM64Storage::ToDate32() const {
    Y_ASSERT(IsUniversal(TimezoneId));
    i32 date;
    if (!NKikimr::NMiniKQL::MakeDate32(Year, Month, Day, date)) {
        ythrow yexception() << "Error in MakeDate32";
    }
    return date;
}



}
