#pragma once

#include <library/cpp/actors/core/actor.h>
#include <ydb/core/base/pathid.h>
#include <ydb/core/tablet_flat/flat_cxx_database.h>

namespace NKikimr::NDataShard {

class TCdcStreamScanManager {
public:
    TCdcStreamScanManager();

    void Enqueue(ui64 txId, ui64 scanId, const TPathId& streamPathId);
    void Register(const NActors::TActorId& actorId);
    void Clear();
    void Forget(NTable::TDatabase& db, const TPathId& tablePathId, const TPathId& streamPathId);

    ui64 GetTxId() const { return TxId; }
    ui64 GetScanId() const { return ScanId; }
    const NActors::TActorId& GetActorId() const { return ActorId; }
    const TPathId& GetStreamPathId() const { return StreamPathId; }

    void PersistLastKey(NIceDb::TNiceDb& db, const TSerializedCellVec& value,
        const TPathId& tablePathId, const TPathId& streamPathId);
    bool LoadLastKey(NIceDb::TNiceDb& db, TMaybe<TSerializedCellVec>& result,
        const TPathId& tablePathId, const TPathId& streamPathId);
    void RemoveLastKey(NIceDb::TNiceDb& db,
        const TPathId& tablePathId, const TPathId& streamPathId);

private:
    ui64 TxId;
    ui64 ScanId;
    NActors::TActorId ActorId;
    TPathId StreamPathId;
};

}
