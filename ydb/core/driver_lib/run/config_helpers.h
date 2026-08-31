#pragma once

#include <ydb/core/memory_controller/memory_controller.h>
#include <ydb/core/protos/config.pb.h>

#include <ydb/library/actors/core/config.h>
#include <ydb/library/actors/util/cpu_topology.h>

namespace NKikimr {

namespace NActorSystemConfigHelpers {

void AddExecutorPools(NActors::TCpuManagerConfig& cpuManager, const NKikimrConfig::TActorSystemConfig& systemConfig, NMonitoring::TDynamicCounterPtr counters);
void AddExecutorPools(NActors::TCpuManagerConfig& cpuManager, const NKikimrConfig::TActorSystemConfig& systemConfig,
    NMonitoring::TDynamicCounterPtr counters, const TCpuTopology& cpuTopology);

NActors::TSchedulerConfig CreateSchedulerConfig(const NKikimrConfig::TActorSystemConfig::TScheduler& config);

}  // namespace NActorSystemConfigHelpers

namespace NKikimrConfigHelpers {

NMemory::TResourceBrokerConfig CreateMemoryControllerResourceBrokerConfig(const NKikimrConfig::TAppConfig& config);

}  // namespace NKikimrConfigHelpers

}  // namespace NKikimr
