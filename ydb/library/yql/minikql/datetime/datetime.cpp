#include "datetime.h"

#include <ydb/library/yql/minikql/mkql_type_ops.h>

namespace NYql::DateTime {

bool DoAddMonths(TTMStorage& storage, i64 months, const NUdf::IDateBuilder& builder) {
    i64 newMonth = months + storage.Month;
    storage.Year += (newMonth - 1) / 12;
    newMonth = 1 + (newMonth - 1) % 12;
    if (newMonth <= 0) {
        storage.Year--;
        newMonth += 12;
    }
    storage.Month = newMonth;
    bool isLeap = NKikimr::NMiniKQL::IsLeapYear(storage.Year);
    ui32 monthLength = NKikimr::NMiniKQL::GetMonthLength(storage.Month, isLeap);
    storage.Day = std::min(monthLength, storage.Day);
    return storage.Validate(builder);
}

bool DoAddYears(TTMStorage& storage, i64 years, const NUdf::IDateBuilder& builder) {
    storage.Year += years;
    if (storage.Month == 2 && storage.Day == 29) {
        bool isLeap = NKikimr::NMiniKQL::IsLeapYear(storage.Year);
        if (!isLeap) {
            storage.Day--;
        }
    }
    return storage.Validate(builder);
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

}
