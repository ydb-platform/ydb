#pragma once

#include "replication.h"
#include "topic_mirror.h"

namespace NKikimr::NReplication::NController {

IActor* CreateDstAlterer(TReplication* replication, ui64 targetId, const TActorContext& ctx);
IActor* CreateDstAlterer(const TActorId& parent, ui64 schemeShardId,
    ui64 rid, ui64 tid, TReplication::ETargetKind kind, const TPathId& dstPathId, TReplication::EState desiredState,
    const TTopicMirrorSettings& topicMirror = {});

}
