#include "datetime.h"

namespace NYql::NDateTime {

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
        case 1:
            return 31;
        case 2:
            return isLeap ? 29 : 28;
        case 3:
            return 31;
        case 4:
            return 30;
        case 5:
            return 31;
        case 6:
            return 30;
        case 7:
            return 31;
        case 8:
            return 31;
        case 9:
            return 30;
        case 10:
            return 31;
        case 11:
            return 30;
        case 12:
            return 31;
        default:
            ythrow yexception() << "Unknown month: " << month;
    }
}

TInstant DoAddMonths(TInstant current, i64 months, const NUdf::IDateBuilder& builder) {
    TTMStorage storage;
    storage.FromTimestamp(builder, current.GetValue());
    if (!DoAddMonths(storage, months, builder)) {
        ythrow yexception() << "Shift error " << current.ToIsoStringLocal() << " by " << months << " months";
    }
    return TInstant::FromValue(storage.ToTimestamp(builder));
}

TInstant DoAddYears(TInstant current, i64 years, const NUdf::IDateBuilder& builder) {
    TTMStorage storage;
    storage.FromTimestamp(builder, current.GetValue());
    if (!DoAddYears(storage, years, builder)) {
        ythrow yexception() << "Shift error " << current.ToIsoStringLocal() << " by " << years << " years";
    }
    return TInstant::FromValue(storage.ToTimestamp(builder));
}

} // namespace NYql::NDateTime

// TODO(YQL-20086): Migrate YDB to NYql::NDateTime
namespace NYql::DateTime { // NOLINT(readability-identifier-naming)
using namespace NYql::NDateTime;
} // namespace NYql::DateTime
