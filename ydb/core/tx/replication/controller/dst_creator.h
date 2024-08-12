#pragma once

#include "replication.h"

namespace NKikimr::NReplication::NController {

IActor* CreateDstCreator(TReplication* replication, ui64 targetId, const TActorContext& ctx);
IActor* CreateDstCreator(const TActorId& parent, ui64 schemeShardId, const TActorId& proxy, const TPathId& pathId,
    ui64 rid, ui64 tid, TReplication::ETargetKind kind, const TString& srcPath, const TString& dstPath);

}
