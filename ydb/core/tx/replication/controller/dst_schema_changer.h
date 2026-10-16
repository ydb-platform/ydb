#pragma once

#include "replication.h"

namespace NKikimrReplication {
    class TSchemaChange;
}

namespace NKikimr::NReplication::NController {

struct TSchemaChangeDstAlterSettings {
    ui64 TxId = 0;
    bool RequireTargetFlush = false;
    bool GlobalConsistency = false;
    TString IndexName;
    ui64 SnapshotTxId = 0;
    bool CancelIndex = false;
};

// Reconciles an OLTP replica table with a complete source schema snapshot.
IActor* CreateSchemaChangeDstAlterer(
    const TActorId& parent,
    ui64 schemeShardId,
    ui64 rid,
    ui64 tid,
    TReplication::ETargetKind kind,
    const TPathId& dstPathId,
    const NKikimrReplication::TSchemaChange& desiredSchema,
    const TSchemaChangeDstAlterSettings& settings = {});

}
