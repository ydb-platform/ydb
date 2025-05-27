#include "datetime.h"

namespace NYql::DateTime {

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
