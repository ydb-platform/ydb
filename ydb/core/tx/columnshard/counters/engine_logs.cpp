#include "engine_logs.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/tx/columnshard/counters/histogram_borders.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>

#include <ydb/library/actors/core/log.h>

#include <util/generic/serialized_enum.h>

namespace NKikimr::NColumnShard {

namespace {

ui64 GetBadPortionSizeLimit() {
    return HasAppData() ? AppData()->ColumnShardConfig.GetBadPortionSizeLimit() : 512_KB;
}

}   // namespace

TEngineLogsCounters::TEngineLogsCounters()
    : TBase("EngineLogs")
    , GranuleDataAgent("EngineLogs")
{
    for (auto&& i : GetEnumNames<NOlap::NPortion::EProduced>()) {
        if (PortionSizeDistribution.size() <= (ui32)i.first) {
            PortionSizeDistribution.resize((ui32)i.first + 1);
            PortionRecordsDistribution.resize((ui32)i.first + 1);
        }
        PortionSizeDistribution[(ui32)i.first] = std::make_shared<TIncrementalHistogram>(
            "EngineLogs", "PortionSizeDistribution", i.second, NColumnShard::THistorgamBorders::BytesBorders);
        PortionRecordsDistribution[(ui32)i.first] = std::make_shared<TIncrementalHistogram>(
            "EngineLogs", "PortionRecordsDistribution", i.second, NColumnShard::THistorgamBorders::PortionRecordBorders);
    }

    PortionToDropCount = TBase::GetDeriviative("Ttl/PortionToDrop/Count");
    PortionToDropBytes = TBase::GetDeriviative("Ttl/PortionToDrop/Bytes");
    PortionToDropLag = TBase::GetHistogram("Ttl/PortionToDrop/Lag/Duration", NMonitoring::ExponentialHistogram(18, 2, 5));
    SkipDeleteWithProcessMemory =
        TBase::GetHistogram("Ttl/PortionToDrop/Skip/ProcessMemory/Lag/Duration", NMonitoring::ExponentialHistogram(18, 2, 5));
    SkipDeleteWithTxLimit = TBase::GetHistogram("Ttl/PortionToDrop/Skip/TxLimit/Lag/Duration", NMonitoring::ExponentialHistogram(18, 2, 5));

    PortionToEvictCount = TBase::GetDeriviative("Ttl/PortionToEvict/Count");
    PortionToEvictBytes = TBase::GetDeriviative("Ttl/PortionToEvict/Bytes");
    PortionToEvictLag = TBase::GetHistogram("Ttl/PortionToEvict/Lag/Duration", NMonitoring::ExponentialHistogram(18, 2, 5));
    SkipEvictionWithProcessMemory =
        TBase::GetHistogram("Ttl/PortionToEvict/Skip/ProcessMemory/Lag/Duration", NMonitoring::ExponentialHistogram(18, 2, 5));
    SkipEvictionWithTxLimit = TBase::GetHistogram("Ttl/PortionToEvict/Skip/TxLimit/Lag/Duration", NMonitoring::ExponentialHistogram(18, 2, 5));

    ActualizationTaskSizeRemove = TBase::GetHistogram("Actualization/RemoveTasks/Size", NMonitoring::ExponentialHistogram(18, 2));
    ActualizationTaskSizeEvict = TBase::GetHistogram("Actualization/EvictTasks/Size", NMonitoring::ExponentialHistogram(18, 2));

    ActualizationSkipRWProgressCount = TBase::GetDeriviative("Actualization/Skip/RWProgress/Count");
    ActualizationSkipTooFreshPortion = TBase::GetHistogram("Actualization//Skip/TooFresh/Duration", NMonitoring::LinearHistogram(12, 0, 360));

    PortionNoTtlColumnCount = TBase::GetDeriviative("Ttl/PortionNoTtlColumn/Count");
    PortionNoTtlColumnBytes = TBase::GetDeriviative("Ttl/PortionNoTtlColumn/Bytes");

    PortionNoBorderCount = TBase::GetDeriviative("Ttl/PortionNoBorder/Count");
    PortionNoBorderBytes = TBase::GetDeriviative("Ttl/PortionNoBorder/Bytes");

    GranuleOptimizerLocked = TBase::GetDeriviative("Optimizer/Granules/Locked");

    IndexMetadataUsageBytes = TBase::GetValue("IndexMetadata/Usage/Bytes");

    StatUsageForTTLCount = TBase::GetDeriviative("Ttl/StatUsageForTTLCount/Count");
    ChunkUsageForTTLCount = TBase::GetDeriviative("Ttl/ChunkUsageForTTLCount/Count");

    BadPortionsCount = TBase::GetValue("BadPortions/Count");

    CleanupPortionsSkippedByLock = TBase::GetDeriviative("CleanupPortions/SkippedByLock");
    CleanupPortionsLimitExceeded = TBase::GetDeriviative("CleanupPortions/LimitExceeded");

    HistogramAddPortionDurationMs = TBase::GetHistogram("Portions/AddDurationMs", NMonitoring::ExponentialHistogram(15, 2, 2));
}

void TEngineLogsCounters::OnActualizationTask(const ui32 evictCount, const ui32 removeCount) const {
    YDB_LOG_VERIFY(evictCount * removeCount == 0,
        {"evict", evictCount},
        {"remove", removeCount});
    YDB_LOG_VERIFY(evictCount + removeCount);
    if (evictCount) {
        ActualizationTaskSizeEvict->Collect(evictCount);
    } else {
        ActualizationTaskSizeRemove->Collect(removeCount);
    }
}

void TEngineLogsCounters::TPortionSizeHistograms::OnNewPortion(const std::shared_ptr<NOlap::TPortionInfo>& portion) {
    const ui32 producedId = (ui32)portion->GetProduced();
    PortionRecordCountGuards[producedId]->Add(portion->GetRecordsCount(), 1);
    PortionSizeGuards[producedId]->Add(portion->GetTotalBlobBytes(), 1);

    if (portion->GetTotalBlobBytes() <= GetBadPortionSizeLimit()) {
        BadPortionsCount.Add(1);
    }
}

void TEngineLogsCounters::TPortionSizeHistograms::OnDropPortion(const std::shared_ptr<NOlap::TPortionInfo>& portion) {
    const ui32 producedId = (ui32)portion->GetProduced();
    PortionRecordCountGuards[producedId]->Sub(portion->GetRecordsCount(), 1);
    PortionSizeGuards[producedId]->Sub(portion->GetTotalBlobBytes(), 1);

    if (portion->GetTotalBlobBytes() <= GetBadPortionSizeLimit()) {
        BadPortionsCount.Sub(1);
    }
}

}   // namespace NKikimr::NColumnShard
