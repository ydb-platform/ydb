#pragma once

#include <ydb/library/actors/core/actor.h>

namespace NKikimrConfig {
class TSharedReadingConfig_TCompileServiceConfig;
} // namespace NKikimrConfig

namespace NFq::NRowDispatcher {

NActors::IActor* CreatePurecalcCompileService(const NKikimrConfig::TSharedReadingConfig_TCompileServiceConfig& config, NMonitoring::TDynamicCounterPtr counters);

}  // namespace NFq::NRowDispatcher
