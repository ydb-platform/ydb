#pragma once

#include "replication.h"

#include <ydb/core/base/defs.h>

namespace NKikimr::NReplication::NController {

IActor* CreateDstRemover(const TActorId& parent, ui64 schemeShardId, const TActorId& proxy,
    ui64 rid, ui64 tid, TReplication::ETargetKind kind, const TPathId& dstPathId);

}
