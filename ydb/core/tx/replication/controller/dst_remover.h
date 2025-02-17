#pragma once

#include "replication.h"

namespace NKikimr::NReplication::NController {

IActor* CreateDstRemover(TReplication* replication, ui64 targetId, const TActorContext& ctx);
IActor* CreateDstRemover(const TActorId& parent, ui64 schemeShardId, const TActorId& proxy,
    ui64 rid, ui64 tid, TReplication::ETargetKind kind, const TPathId& dstPathId);

}
