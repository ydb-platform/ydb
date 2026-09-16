#pragma once

#include <ydb/core/memory_controller/memory_controller.h>
#include <ydb/core/protos/config.pb.h>

#include <ydb/library/actors/core/config.h>
#include <ydb/library/actors/util/cpu_topology.h>

#include <util/generic/vector.h>

namespace NKikimr {

namespace NActorSystemConfigHelpers {

void AddExecutorPools(NActors::TCpuManagerConfig& cpuManager, const NKikimrConfig::TActorSystemConfig& systemConfig, NMonitoring::TDynamicCounterPtr counters);
void AddExecutorPools(NActors::TCpuManagerConfig& cpuManager, const NKikimrConfig::TActorSystemConfig& systemConfig,
    NMonitoring::TDynamicCounterPtr counters, const TCpuTopology& cpuTopology);

// Returns the pool ids referenced by BlobStorageExecutor, validated against the executor
// list. Aborts on an out-of-range or duplicate id, or when the (non-empty) list is
// combined with UseSharedThreads.
TVector<ui32> GetBlobStorageExecutorPoolIds(const NKikimrConfig::TActorSystemConfig& systemConfig);

// Returns the pool ids referenced by InterconnectSessionExecutor.
TVector<ui32> GetInterconnectSessionExecutorPoolIds(const NKikimrConfig::TActorSystemConfig& systemConfig);

NActors::TSchedulerConfig CreateSchedulerConfig(const NKikimrConfig::TActorSystemConfig::TScheduler& config);

}  // namespace NActorSystemConfigHelpers

namespace NKikimrConfigHelpers {

NMemory::TResourceBrokerConfig CreateMemoryControllerResourceBrokerConfig(const NKikimrConfig::TAppConfig& config);

}  // namespace NKikimrConfigHelpers

}  // namespace NKikimr
