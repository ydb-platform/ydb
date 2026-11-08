#pragma once

#include "schemeshard_identificators.h"

#include <ydb/library/actors/core/actor.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>

namespace NKikimr::NSchemeShard {

NActors::IActor* CreateKesusRestorer(
    NActors::TActorId schemeShard,
    ui64 importId,
    ui32 itemIdx,
    TPathId dstPathId,
    TTxId createTxId,
    NKikimrSchemeOp::TImportKesus contents,
    ui32 retries);

} // namespace NKikimr::NSchemeShard
