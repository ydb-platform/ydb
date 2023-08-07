#include "engine_logs.h"
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <util/generic/serialized_enum.h>

namespace NKikimr::NColumnShard {

TEngineLogsCounters::TEngineLogsCounters()
    : TBase("EngineLogs")
    , GranuleDataAgent("EngineLogs")
{
    const std::map<i64, TString> borders = {{0, "0"}, {512 * 1024, "512kb"}, {1024 * 1024, "1Mb"},
        {2 * 1024 * 1024, "2Mb"}, {4 * 1024 * 1024, "4Mb"},
        {5 * 1024 * 1024, "5Mb"}, {6 * 1024 * 1024, "6Mb"},
        {7 * 1024 * 1024, "7Mb"}, {8 * 1024 * 1024, "8Mb"}};
    for (auto&& i : GetEnumNames<NOlap::NPortion::EProduced>()) {
        if (BlobSizeDistribution.size() <= (ui32)i.first) {
            BlobSizeDistribution.resize((ui32)i.first + 1);
        }
        BlobSizeDistribution[(ui32)i.first] = std::make_shared<TIncrementalHistogram>("EngineLogs", "BlobSizeDistribution", i.second, borders);
    }
    for (auto&& i : BlobSizeDistribution) {
        Y_VERIFY(i);
    }
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
