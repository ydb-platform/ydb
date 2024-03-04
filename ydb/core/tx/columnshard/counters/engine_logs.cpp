#include "engine_logs.h"
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
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
    const std::map<i64, TString> portionSizeBorders = {{0, "0"}, {512 * 1024, "512kb"}, {1024 * 1024, "1Mb"},
        {2 * 1024 * 1024, "2Mb"}, {4 * 1024 * 1024, "4Mb"},
        {8 * 1024 * 1024, "8Mb"}, {16 * 1024 * 1024, "16Mb"},
        {32 * 1024 * 1024, "32Mb"}, {64 * 1024 * 1024, "64Mb"}};
    const std::set<i64> portionRecordBorders = {0, 2500, 5000, 7500, 9000, 10000, 20000, 40000, 80000, 160000};
    for (auto&& i : GetEnumNames<NOlap::NPortion::EProduced>()) {
        if (BlobSizeDistribution.size() <= (ui32)i.first) {
            BlobSizeDistribution.resize((ui32)i.first + 1);
            PortionSizeDistribution.resize((ui32)i.first + 1);
            PortionRecordsDistribution.resize((ui32)i.first + 1);
        }
        if (PortionSizeDistribution.size() <= (ui32)i.first) {
            PortionSizeDistribution.resize((ui32)i.first + 1);
        }
        BlobSizeDistribution[(ui32)i.first] = std::make_shared<TIncrementalHistogram>("EngineLogs", "BlobSizeDistribution", i.second, borders);
        PortionSizeDistribution[(ui32)i.first] = std::make_shared<TIncrementalHistogram>("EngineLogs", "PortionSizeDistribution", i.second, portionSizeBorders);
        PortionRecordsDistribution[(ui32)i.first] = std::make_shared<TIncrementalHistogram>("EngineLogs", "PortionRecordsDistribution", i.second, portionRecordBorders);
    }
    for (auto&& i : BlobSizeDistribution) {
        Y_ABORT_UNLESS(i);
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

void TEngineLogsCounters::TPortionsInfoGuard::OnNewPortion(const std::shared_ptr<NOlap::TPortionInfo>& portion) const {
    const ui32 producedId = (ui32)(portion->HasRemoveSnapshot() ? NOlap::NPortion::EProduced::INACTIVE : portion->GetMeta().Produced);
    Y_ABORT_UNLESS(producedId < BlobGuards.size());
    for (auto&& i : portion->GetBlobIds()) {
        BlobGuards[producedId]->Add(i.BlobSize(), i.BlobSize());
    }
    PortionRecordCountGuards[producedId]->Add(portion->GetRecordsCount(), 1);
    PortionSizeGuards[producedId]->Add(portion->GetBlobBytes(), 1);
}

void TEngineLogsCounters::TPortionsInfoGuard::OnDropPortion(const std::shared_ptr<NOlap::TPortionInfo>& portion) const {
    const ui32 producedId = (ui32)(portion->HasRemoveSnapshot() ? NOlap::NPortion::EProduced::INACTIVE : portion->GetMeta().Produced);
    Y_ABORT_UNLESS(producedId < BlobGuards.size());
    for (auto&& i : portion->GetBlobIds()) {
        BlobGuards[producedId]->Sub(i.BlobSize(), i.BlobSize());
    }
    PortionRecordCountGuards[producedId]->Sub(portion->GetRecordsCount(), 1);
    PortionSizeGuards[producedId]->Sub(portion->GetBlobBytes(), 1);
}

}
