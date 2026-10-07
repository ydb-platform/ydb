#pragma once

#include <ydb/library/actors/interconnect/rdma/rdma.h>
#include "interconnect_common.h"
#include "interconnect_stream.h"

namespace NActors {
class IActor;
}

namespace NInterconnect::NRdma {

NActors::IActor* CreateRdmaOutgoingSyncActor(
    NActors::TInterconnectProxyCommon::TPtr common,
    const NActors::TActorId& selfVirtualId,
    const NActors::TActorId& peerVirtualId,
    ui32 peerNodeId,
    TIntrusivePtr<NInterconnect::TStreamSocket> socket,
    TQueuePair::TPtr qp,
    ICq::TPtr cq);

NActors::IActor* CreateRdmaIncommingSyncActor(
    NActors::TInterconnectProxyCommon::TPtr common,
    const NActors::TActorId& selfVirtualId,
    const NActors::TActorId& peerVirtualId,
    ui32 peerNodeId,
    TIntrusivePtr<NInterconnect::TStreamSocket> socket,
    TQueuePair::TPtr qp,
    ICq::TPtr cq);

}
