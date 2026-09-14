#pragma once

#include "replication.h"

namespace NKikimr::NReplication::NController {

IActor* CreateStreamConsumerRemover(TReplication* replication, ui64 targetId, const TActorContext& ctx);
IActor* CreateStreamConsumerRemover(const TActorId& parent, const TActorId& proxy, ui64 rid, ui64 tid,
    TReplication::ETargetKind kind, const TString& srcPath, const TString& consumerName, bool needDrop);

}
