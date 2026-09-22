#pragma once

#include "replication.h"

namespace NKikimrReplication {
    class TSchemaChange;
}

namespace NKikimr::NReplication::NController {

// Reconciles an OLTP replica table with a complete source schema snapshot.
IActor* CreateSchemaChangeDstAlterer(const TActorId& parent, ui64 schemeShardId,
    ui64 rid, ui64 tid, TReplication::ETargetKind kind, const TPathId& dstPathId,
    const NKikimrReplication::TSchemaChange& desiredSchema, ui64 txId = 0);

}
