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
    ReadBytes = SubGroup->GetCounter(module + "/Read/Bytes", true);
    AnalizeInsertedPortions = SubGroup->GetCounter(module + "/AnalizeInsertion/Portions", true);
    AnalizeInsertedBytes = SubGroup->GetCounter(module + "/AnalizeInsertion/Bytes", true);
    RepackedInsertedPortions = SubGroup->GetCounter(module + "/RepackedInsertion/Portions", true);
    RepackedInsertedPortionBytes = SubGroup->GetCounter(module + "/RepackedInsertion/Bytes", true);

    AnalizeCompactedPortions = SubGroup->GetCounter(module + "/AnalizeCompaction/Portions", true);
    AnalizeCompactedBytes = SubGroup->GetCounter(module + "/AnalizeCompaction/Bytes", true);
    SkipPortionsMoveThroughIntersection = SubGroup->GetCounter(module + "/SkipMoveThroughIntersection/Portions", true);
    SkipPortionBytesMoveThroughIntersection = SubGroup->GetCounter(module + "/SkipMoveThroughIntersection/Bytes", true);
    RepackedCompactedPortions = SubGroup->GetCounter(module + "/RepackedCompaction/Portions", true);
    MovedPortions = SubGroup->GetCounter(module + "/Moved/Portions", true);
    MovedPortionBytes = SubGroup->GetCounter(module + "/Moved/Bytes", true);

    TrashDataSerializationBytes = SubGroup->GetCounter(module + "/TrashDataSerialization/Bytes", true);
    TrashDataSerialization = SubGroup->GetCounter(module + "/TrashDataSerialization/Count", true);
    CorrectDataSerializationBytes = SubGroup->GetCounter(module + "/CorrectDataSerialization/Bytes", true);
    CorrectDataSerialization = SubGroup->GetCounter(module + "/CorrectDataSerialization/Count", true);

    SplittedPortionsSize = SubGroup->GetHistogram(module + "/Histogram/SplittedPortionsSize", NMonitoring::ExponentialHistogram(15, 2, 1024));
}

}
