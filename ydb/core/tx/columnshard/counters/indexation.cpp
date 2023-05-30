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
    ReadBytes = SubGroup->GetCounter(module + "/Counters/Read/Bytes", true);
    AnalizeInsertedPortions = SubGroup->GetCounter(module + "/Counters/AnalizeInsertion/Portions", true);
    AnalizeInsertedBytes = SubGroup->GetCounter(module + "/Counters/AnalizeInsertion/Bytes", true);
    RepackedInsertedPortions = SubGroup->GetCounter(module + "/Counters/RepackedInsertion/Portions", true);
    RepackedInsertedPortionBytes = SubGroup->GetCounter(module + "/Counters/RepackedInsertion/Bytes", true);

    AnalizeCompactedPortions = SubGroup->GetCounter(module + "/Counters/AnalizeCompaction/Portions", true);
    AnalizeCompactedBytes = SubGroup->GetCounter(module + "/Counters/AnalizeCompaction/Bytes", true);
    SkipPortionsMoveThroughIntersection = SubGroup->GetCounter(module + "/Counters/SkipMoveThroughIntersection/Portions", true);
    SkipPortionBytesMoveThroughIntersection = SubGroup->GetCounter(module + "/Counters/SkipMoveThroughIntersection/Bytes", true);
    RepackedCompactedPortions = SubGroup->GetCounter(module + "/Counters/RepackedCompaction/Portions", true);
    MovedPortions = SubGroup->GetCounter(module + "/Counters/Moved/Portions", true);
    MovedPortionBytes = SubGroup->GetCounter(module + "/Counters/Moved/Bytes", true);

    TrashDataSerializationBytes = SubGroup->GetCounter(module + "/Counters/TrashDataSerialization/Bytes", true);
    TrashDataSerialization = SubGroup->GetCounter(module + "/Counters/TrashDataSerialization/Count", true);
    TrashDataSerializationHistogramBytes = SubGroup->GetHistogram(module + "/Histogram/TrashDataSerialization/Bytes", NMonitoring::ExponentialHistogram(15, 2, 1024));
    CorrectDataSerializationBytes = SubGroup->GetCounter(module + "/Counters/CorrectDataSerialization/Bytes", true);
    CorrectDataSerialization = SubGroup->GetCounter(module + "/Counters/CorrectDataSerialization/Count", true);

    SplittedPortionLargestColumnSize = SubGroup->GetHistogram(module + "/Histogram/SplittedPortionLargestColumnSize", NMonitoring::ExponentialHistogram(15, 2, 1024));
    SplittedPortionColumnSize = SubGroup->GetHistogram(module + "/Histogram/SplittedPortionColumnSize", NMonitoring::ExponentialHistogram(15, 2, 1024));
    TooSmallBlob = SubGroup->GetCounter(module + "/Counters/TooSmallBlob/Count", true);
    TooSmallBlobFinish = SubGroup->GetCounter(module + "/Counters/TooSmallBlobFinish/Count", true);
    TooSmallBlobStart = SubGroup->GetCounter(module + "/Counters/TooSmallBlobStart/Count", true);
}

}
