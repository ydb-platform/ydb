#pragma once

#include <ydb/core/fq/libs/row_dispatcher/common/row_dispatcher_settings.h>
#include <ydb/library/actors/core/actor.h>

namespace NFq::NRowDispatcher {

NActors::IActor* CreatePurecalcCompileService(const TRowDispatcherSettings::TCompileServiceSettings& config, NMonitoring::TDynamicCounterPtr counters);

}  // namespace NFq::NRowDispatcher
