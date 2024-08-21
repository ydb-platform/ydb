#pragma once

#include <ydb/core/protos/config.pb.h>

#include <ydb/library/actors/core/config.h>


namespace NKikimr {

namespace NActorSystemConfigHelpers {

void AddExecutorPool(NActors::TCpuManagerConfig& cpuManager, const NKikimrConfig::TActorSystemConfig::TExecutor& poolConfig, const NKikimrConfig::TActorSystemConfig& systemConfig, ui32 poolId, NMonitoring::TDynamicCounterPtr counters);

NActors::TSchedulerConfig CreateSchedulerConfig(const NKikimrConfig::TActorSystemConfig::TScheduler& config);

}  // namespace NActorSystemConfigHelpers

}  // namespace NKikimr
