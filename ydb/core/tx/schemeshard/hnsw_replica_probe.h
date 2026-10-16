#pragma once

#include "schemeshard_types.h"
#include <ydb/core/protos/hnsw.pb.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/library/actors/core/actor.h>

namespace NKikimr::NSchemeShard {

NActors::IActor* CreateHnswReplicaProbe(NActors::TActorId replyTo, TOperationId operationId,
    ui64 round, TPathId pathId, ui64 schemaVersion,
    NKikimrSchemeOp::TPartitionConfig config,
    TVector<std::pair<ui64, ui64>> tabletsAndHives,
    TVector<NKikimrHnsw::TReplicaProbeResult::TCheckpoint> checkpoints);

} // namespace NKikimr::NSchemeShard
