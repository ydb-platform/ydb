#pragma once

#include "replication.h"

namespace NKikimr::NReplication::NController {

// Removes only the replication consumer from a retained user CDC stream.
IActor* CreateCdcStreamConsumerRemover(const TActorId& parent, const TActorId& proxy, ui64 rid, ui64 tid,
    const TString& sourceTablePath, const TString& streamName, const TString& consumerName);

IActor* CreateStreamRemover(TReplication* replication, ui64 targetId, const TActorContext& ctx);
IActor* CreateStreamRemover(const TActorId& parent, const TActorId& proxy, ui64 rid, ui64 tid,
    TReplication::ETargetKind kind, const TString& srcPath, const TString& streamName);

}
