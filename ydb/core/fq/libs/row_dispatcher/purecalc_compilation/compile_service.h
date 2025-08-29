#pragma once

#include <ydb/library/actors/core/actor.h>

#include <ydb/core/protos/config.pb.h>

namespace NFq::NRowDispatcher {

NActors::IActor* CreatePurecalcCompileService(const NKikimrConfig::TSharedReadingConfig::TCompileServiceConfig& config, NMonitoring::TDynamicCounterPtr counters);

}  // namespace NFq::NRowDispatcher
