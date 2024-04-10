#pragma once

#include "replication.h"

namespace NKikimr::NReplication::NController {

IActor* CreateDstCreator(TReplication::TPtr replication, ui64 targetId, const TActorContext& ctx);
IActor* CreateDstCreator(const TActorId& parent, ui64 schemeShardId, const TActorId& proxy,
    ui64 rid, ui64 tid, TReplication::ETargetKind kind, const TString& srcPath, const TString& dstPath);

}
