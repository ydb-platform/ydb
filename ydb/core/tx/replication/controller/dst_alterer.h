#pragma once

#include "replication.h"

namespace NKikimr::NReplication::NController {

IActor* CreateDstAlterer(TReplication::TPtr replication, ui64 targetId, const TActorContext& ctx);
IActor* CreateDstAlterer(const TActorId& parent, ui64 schemeShardId,
    ui64 rid, ui64 tid, TReplication::ETargetKind kind, const TPathId& dstPathId);

}
