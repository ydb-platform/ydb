#pragma once

#include <ydb/core/persqueue/public/key.h>
#include <ydb/library/actors/core/actorsystem_fwd.h>
#include <util/generic/fwd.h>

namespace NKikimrClient {
class TPersQueueRequest;
}

namespace NKikimr::NPQ {

NActors::IActor* CreateReadProxy(
    const NActors::TActorId& sender,
    const ui64 tabletId,
    const NActors::TActorId& tablet,
    const ui32 tabletGeneration,
    const TDirectReadKey& directReadKey,
    const NKikimrClient::TPersQueueRequest& request);

}
