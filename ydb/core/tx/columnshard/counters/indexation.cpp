#include "indexation.h"
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>

namespace NKikimr::NColumnShard {

TIndexationCounters::TIndexationCounters(const TString& module) {
    if (NActors::TlsActivationContext) {
        SubGroup = GetServiceCounters(AppData()->Counters, "tablets")->GetSubgroup("subsystem", "columnshard");
    } else {
        SubGroup = new NMonitoring::TDynamicCounters();
    }
    ReadBytes = SubGroup->GetCounter(module + "/ReadBytes", true);
    AnalizeCompactedPortions = SubGroup->GetCounter(module + "/AnalizeCompactedPortions", true);
    AnalizeInsertedPortions = SubGroup->GetCounter(module + "/AnalizeInsertedPortions", true);
    RepackedInsertedPortions = SubGroup->GetCounter(module + "/RepackedInsertedPortions", true);
    RepackedInsertedPortionBytes = SubGroup->GetCounter(module + "/RepackedInsertedPortionBytes", true);
    SkipPortionsMoveThroughIntersection = SubGroup->GetCounter(module + "/SkipPortionsMoveThroughIntersection", true);
    SkipPortionBytesMoveThroughIntersection = SubGroup->GetCounter(module + "/SkipPortionBytesMoveThroughIntersection", true);
    RepackedCompactedPortions = SubGroup->GetCounter(module + "/RepackedCompactedPortions", true);
    MovedPortions = SubGroup->GetCounter(module + "/MovedPortions", true);
    MovedPortionBytes = SubGroup->GetCounter(module + "/MovedPortionBytes", true);

    TrashDataSerializationBytes = SubGroup->GetCounter(module + "/TrashDataSerializationBytes", true);
    TrashDataSerialization = SubGroup->GetCounter(module + "/TrashDataSerialization", true);
    CorrectDataSerializationBytes = SubGroup->GetCounter(module + "/CorrectDataSerializationBytes", true);
    CorrectDataSerialization = SubGroup->GetCounter(module + "/CorrectDataSerialization", true);
}

}
