#pragma once

#include <ydb/core/engine/minikql/change_collector_iface.h>
#include <ydb/core/tablet_flat/tablet_flat_executor.h>

namespace NKikimr {
namespace NDataShard {

class TDataShard;
struct TUserTable;

class IDataShardUserDb;

class IDataShardChangeCollector : public NMiniKQL::IChangeCollector {
public:
    // basic change record's info
    struct TChange {
        ui64 Order;
        ui64 Group;
        ui64 Step;
        ui64 TxId;
        TPathId PathId;
        ui64 BodySize;
        TPathId TableId;
        ui64 SchemaVersion;
        ui64 LockId = 0;
        ui64 LockOffset = 0;
    };

public:
    virtual void OnRestart() = 0;
    virtual bool NeedToReadKeys() const = 0;

    virtual void CommitLockChanges(ui64 lockId, const TRowVersion& writeVersion) = 0;

    virtual const TVector<TChange>& GetCollected() const = 0;
    virtual TVector<TChange>&& GetCollected() = 0;
};

IDataShardChangeCollector* CreateChangeCollector(TDataShard& dataShard, IDataShardUserDb& userDb, NTable::TDatabase& db, const TUserTable& table, bool isImmediateTx);
IDataShardChangeCollector* CreateChangeCollector(TDataShard& dataShard, IDataShardUserDb& userDb, NTable::TDatabase& db, ui64 tableId, bool isImmediateTx);

} // NDataShard
} // NKikimr
