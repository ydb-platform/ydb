#pragma once

#include "defs.h"

#include <ydb/core/base/blobstorage_common.h>
#include <ydb/core/base/logoblob.h>

#include <unordered_map>

namespace NKikimr {
namespace NBsController {

NActors::IActor* CreateBlobCheckerWorkerActor(TGroupId groupId, TActorId orchestratorId,
        TLogoBlobID maxCheckedBlob);

NActors::IActor* CreateBlobCheckerOrchestratorActor(TActorId bscActorId,
        std::unordered_map<TGroupId, TString> serializedGroups,
        TDuration periodicity, ::NMonitoring::TDynamicCounterPtr counters);

} // namespace NBsController
} // namespace NKikimr
