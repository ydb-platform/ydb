#pragma once

#include <ydb/core/protos/tx_datashard.pb.h>
#include <ydb/library/actors/core/actor.h>

namespace NKikimr::NDataShard {

class TDataShard;

NActors::TActorId CreateIncrementalRestoreSrcActor(
        TDataShard* self,
        const NKikimrTxDataShard::TEvIncrementalRestoreSrcCreateRequest& request);

} // namespace NKikimr::NDataShard
