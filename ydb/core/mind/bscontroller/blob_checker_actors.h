#pragma once

#include "defs.h"

namespace NKikimr {
namespace NBsController {

NActors::IActor* CreateBlobCheckerWorkerActor(TGroupId groupId, TActorId orchestratorId);

NActors::IActor* CreateBlobCheckerOrchestratorActor(TActorId bscActorId,
        std::unordered_map<TGroupId, TString> serializedGroups,
        TDuration periodicity, ::NMonitoring::TDynamicCounterPtr counters);

} // namespace NBsController
} // namespace NKikimr
