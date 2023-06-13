#include "engine_logs.h"
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>

namespace NKikimr::NColumnShard {

TEngineLogsCounters::TEngineLogsCounters()
    : TBase("EngineLogs")
    , GranuleDataAgent("EngineLogs")
{
    OverloadGranules = TBase::GetValue("Granules/Overload");
    CompactOverloadGranulesSelection = TBase::GetDeriviative("Granules/Selection/Overload/Count");
    NoCompactGranulesSelection = TBase::GetDeriviative("Granules/Selection/No/Count");
    SplitCompactGranulesSelection = TBase::GetDeriviative("Granules/Selection/Split/Count");
    InternalCompactGranulesSelection = TBase::GetDeriviative("Granules/Selection/Internal/Count");

    PortionToDropCount = TBase::GetDeriviative("Ttl/PortionToDrop/Count");
    PortionToDropBytes = TBase::GetDeriviative("Ttl/PortionToDrop/Bytes");

    PortionToEvictCount = TBase::GetDeriviative("Ttl/PortionToEvict/Count");
    PortionToEvictBytes = TBase::GetDeriviative("Ttl/PortionToEvict/Bytes");

    PortionNoTtlColumnCount = TBase::GetDeriviative("Ttl/PortionNoTtlColumn/Count");
    PortionNoTtlColumnBytes = TBase::GetDeriviative("Ttl/PortionNoTtlColumn/Bytes");

    PortionNoBorderCount = TBase::GetDeriviative("Ttl/PortionNoBorder/Count");
    PortionNoBorderBytes = TBase::GetDeriviative("Ttl/PortionNoBorder/Bytes");
}

}
