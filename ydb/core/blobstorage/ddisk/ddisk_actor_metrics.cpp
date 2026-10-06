#include "ddisk_actor.h"

namespace NKikimr::NDDisk {

void TDDiskActor::InitMemoryMetrics() {
    if (auto* registry = GetMetricSystem()) {
        const std::array<TLabel, 3> labels = {{
            {.Name = "pdisk", .Value = ToString(BaseInfo.PDiskId)},
            {.Name = "slot", .Value = ToString(BaseInfo.VDiskSlotId)},
            {.Name = "incarnation", .Value = SelfId().ToString()},
        }};
        MemoryMetric = registry->CreateLine<TMemoryMetricsFrontend>(IsPersistentBufferActor
            ? "ddisk.memory.pb_cache_bytes" : "ddisk.memory.checksum_cache_estimated_bytes", labels);
        if (!IsPersistentBufferActor) {
            OperationMetric = registry->CreateLine<TOperationMetricsFrontend>("ddisk.operations.counters", labels);
            SpaceMetric = registry->CreateLine<TSpaceMetricsFrontend>("ddisk.space.allocated_bytes", labels);
        }
        CollectMemoryMetrics();
    }
}

void TDDiskActor::CollectMemoryMetrics() {
    if (Stopping || (!MemoryMetric && !SpaceMetric && !OperationMetric)) {
        return;
    }
    if (IsPersistentBufferActor) {
        MemoryMetric.Append(PersistentBufferInMemoryCacheSize);
    } else if (IntegrityManager || !Config.EnableChecksums) {
        MemoryMetric.Append(IntegrityManager
            ? IntegrityManager->CachedBlockStates() * TIntegrityManager::BlockStateApproxBytes : 0);
    }
    if (!IsPersistentBufferActor && HandlingQueries && !IsBroken() && DiskFormat) {
        const ui64 chunkSize = DiskFormat->ChunkSize;
        const ui64 checksumChunks = IntegrityManager
            ? IntegrityManager->GetIntegrityChunkCount() : CommittedIntegrityChunks.size();
        SpaceMetric.Append({
            TSpaceMetricsFrontend::Value<TSpaceMetrics::TData>(MonMappedDataChunks * chunkSize),
            TSpaceMetricsFrontend::Value<TSpaceMetrics::TChecksums>(checksumChunks * chunkSize),
            TSpaceMetricsFrontend::Value<TSpaceMetrics::TPersistentBuffer>(PersistentBufferChunks.size() * chunkSize),
            TSpaceMetricsFrontend::Value<TSpaceMetrics::TReserve>(ChunkReserve.size() * chunkSize),
        });
    }
    if (!IsPersistentBufferActor) {
        RecordOperationMetrics(TActivationContext::Monotonic());
    }
    Schedule(TDuration::Seconds(1), new TEvents::TEvWakeup(EWakeupTag::WakeupCollectMemoryMetrics));
}

void TDDiskActor::RecordOperationMetrics(TMonotonic sampledAt) {
    if (OperationMetric) {
        const std::array<const TOpCountersBase*, 5> counters = {
            &Counters.Interface.Read, &Counters.Interface.Write, &Counters.Interface.Sync,
            &Counters.DirectIO.Read, &Counters.DirectIO.Write};
        std::array<ui64, 11> values;
        for (size_t i = 0; i < counters.size(); ++i) {
            values[2 * i] = counters[i]->Requests ? counters[i]->Requests->Val() : 0;
            values[2 * i + 1] = counters[i]->Bytes ? counters[i]->Bytes->Val() : 0;
        }
        values[10] = sampledAt.MicroSeconds();
        OperationMetric.Append(OperationMetricValues(values, std::make_index_sequence<11>{}));
    }
}

} // namespace NKikimr::NDDisk
