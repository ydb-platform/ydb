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
    virtual void CommitLockChanges(ui64 lockId, const TVector<TChange>& changes, NTabletFlatExecutor::TTransactionContext& txc) = 0;
};

IDataShardChangeCollector* CreateChangeCollector(TDataShard& dataShard, IDataShardUserDb& userDb, NTable::TDatabase& db, const TUserTable& table, bool isImmediateTx);
IDataShardChangeCollector* CreateChangeCollector(TDataShard& dataShard, IDataShardUserDb& userDb, NTable::TDatabase& db, ui64 tableId, bool isImmediateTx);

} // NDataShard
} // NKikimr
