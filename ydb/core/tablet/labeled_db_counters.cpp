#include "labeled_db_counters.h"
#include "private/labeled_db_counters.h"


namespace NKikimr {

TIntrusivePtr<NSysView::IDbCounters> CreateLabeledDbCounters(
    ::NMonitoring::TDynamicCounterPtr externalGroup) {
    return MakeIntrusive<NPrivate::TDbLabeledCounters>(externalGroup);
}

} // namespace NKikimr
