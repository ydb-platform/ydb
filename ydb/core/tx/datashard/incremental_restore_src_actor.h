#pragma once

#include <ydb/core/protos/tx_datashard.pb.h>
#include <ydb/library/actors/core/actor.h>

namespace NKikimr::NDataShard {

class TDataShard;

// Spawns the RPC-driven incremental-restore scan actor. The caller must run
// on the DataShard's mailbox; the actor registers with the same mailbox so it
// can safely invoke `TDataShard::QueueScan` and other DS-private API.
//
// On TEvIncrementalRestoreScan::TEvFinished the actor sends a
// TEvIncrementalRestoreShardProgress to the requesting SchemeShard and exits.
NActors::TActorId CreateIncrementalRestoreSrcActor(
        TDataShard* self,
        const NKikimrTxDataShard::TEvIncrementalRestoreSrcCreateRequest& request);

} // namespace NKikimr::NDataShard
