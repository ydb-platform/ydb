#include "engine_logs.h"
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>

namespace NKikimr::NColumnShard {

TEngineLogsCounters::TEngineLogsCounters() {
    const TString module = "EngineLogs";
    if (NActors::TlsActivationContext) {
        SubGroup = GetServiceCounters(AppData()->Counters, "tablets")->GetSubgroup("subsystem", "columnshard");
    } else {
        SubGroup = new NMonitoring::TDynamicCounters();
    }
    OverloadGranules = SubGroup->GetCounter(module + "/Granules/Overload", false);
    CompactOverloadGranulesSelection = SubGroup->GetCounter(module + "/Granules/Selection/Overload/Count", true);
    NoCompactGranulesSelection = SubGroup->GetCounter(module + "/Granules/Selection/No/Count", true);
    SplitCompactGranulesSelection = SubGroup->GetCounter(module + "/Granules/Selection/Split/Count", true);
    InternalCompactGranulesSelection = SubGroup->GetCounter(module + "/Granules/Selection/Internal/Count", true);
}

}
