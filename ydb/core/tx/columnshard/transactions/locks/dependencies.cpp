#include "dependencies.h"
#include <ydb/core/tx/columnshard/columnshard_schema.h>

namespace NKikimr::NOlap::NTxInteractions {

bool TTxConflicts::LoadFromDatabase(NIceDb::TNiceDb& db) {
    using namespace NColumnShard;
    auto rowset = db.Table<Schema::TxDependencies>().Select();
    if (!rowset.IsReady()) {
        return false;
    }

    THashMap<ui64, THashSet<ui64>> local;
    while (!rowset.EndOfSet()) {
        const ui64 txId = rowset.GetValue<Schema::TxDependencies::CommitTxId>();
        local[txId].emplace(rowset.GetValue<Schema::TxDependencies::BrokenTxId>());
        if (!rowset.Next()) {
            return false;
        }
    }
    std::swap(local, TxIdsFromCommitToBroken);
    return true;
}

void TTxConflicts::AddOnExecute(NTabletFlatExecutor::TTransactionContext& txc) const {
    using namespace NColumnShard;
    NIceDb::TNiceDb db(txc.DB);
    for (auto&& [commitTxId, brokeTxIds] : TxIdsFromCommitToBroken) {
        for (auto&& brokeTxId : brokeTxIds) {
            db.Table<Schema::TxDependencies>().Key(commitTxId, brokeTxId).Update();
        }
    }
}

bool TTxConflicts::RemoveOnExecute(NTabletFlatExecutor::TTransactionContext& txc, const ui64 txId) const {
    using namespace NColumnShard;
    NIceDb::TNiceDb db(txc.DB);
    auto it = TxIdsFromCommitToBroken.find(txId);
    if (it == TxIdsFromCommitToBroken.end()) {
        return false;
    }
    for (auto&& brokeTxId : it->second) {
        db.Table<Schema::TxDependencies>().Key(txId, brokeTxId).Delete();
    }
    return true;
}

}
