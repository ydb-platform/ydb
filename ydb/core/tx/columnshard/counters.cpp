#include "counters.h"
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>

namespace NKikimr::NColumnShard {

TScanCounters::TScanCounters(const TString& module) {
    ::NMonitoring::TDynamicCounterPtr subGroup = GetServiceCounters(AppData()->Counters, "tablets")->GetSubgroup("subsystem", "columnshard");
    PortionBytes = subGroup->GetCounter(module + "/PortionBytes", true);
    FilterBytes = subGroup->GetCounter(module + "/FilterBytes", true);
    PostFilterBytes = subGroup->GetCounter(module + "/PostFilterBytes", true);
}

}
