#pragma once

#include "replication.h"

namespace NKikimr::NReplication::NController {

struct TTopicMirrorSettings;

IActor* CreateDstAlterer(TReplication* replication, ui64 targetId, const TActorContext& ctx);
IActor* CreateDstAlterer(const TActorId& parent, ui64 schemeShardId,
    ui64 rid, ui64 tid, TReplication::ETargetKind kind, const TPathId& dstPathId, TReplication::EState desiredState,
    const TPathId& replicationPathId = {});
IActor* CreateDstTopicAlterer(const TActorId& parent, ui64 schemeShardId,
    ui64 rid, ui64 tid, const TPathId& dstPathId, TReplication::EState desiredState,
    TTopicMirrorSettings&& topicMirror);

}
