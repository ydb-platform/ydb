#pragma once

#include <ydb/library/actors/core/actor.h>

namespace NFq::NRowDispatcher {

NActors::IActor* CreatePurecalcCompileService();

}  // namespace NFq::NRowDispatcher
