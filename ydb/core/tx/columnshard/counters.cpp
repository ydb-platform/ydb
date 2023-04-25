#include "counters.h"
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>

namespace NKikimr::NColumnShard {

TScanCounters::TScanCounters(const TString& module) {
    ::NMonitoring::TDynamicCounterPtr subGroup = GetServiceCounters(AppData()->Counters, "tablets")->GetSubgroup("subsystem", "columnshard");
    PortionBytes = subGroup->GetCounter(module + "/PortionBytes", true);
    FilterBytes = subGroup->GetCounter(module + "/FilterBytes", true);
    PostFilterBytes = subGroup->GetCounter(module + "/PostFilterBytes", true);
    PostFilterPortionsCount = subGroup->GetCounter(module + "/PostFilterPortionsCount", true);
    FilterOnlyPortionsCount = subGroup->GetCounter(module + "/FilterOnlyPortionsCount", true);
    FilterOnlyPortionsBytes = subGroup->GetCounter(module + "/FilterOnlyPortionsBytes", true);
    EmptyFilterPortionsCount = subGroup->GetCounter(module + "/EmptyFilterPortionsCount", true);
    EmptyFilterPortionsBytes = subGroup->GetCounter(module + "/EmptyFilterPortionsBytes", true);
    FilteredRowsCount = subGroup->GetCounter(module + "/FilteredRowsCount", true);
    UsefulFilterBytes = subGroup->GetCounter(module + "/UsefulFilterBytes", true);
    UsefulPostFilterBytes = subGroup->GetCounter(module + "/UsefulPostFilterBytes", true);
    OriginalRowsCount = subGroup->GetCounter(module + "/OriginalRowsCount", true);
}

}
