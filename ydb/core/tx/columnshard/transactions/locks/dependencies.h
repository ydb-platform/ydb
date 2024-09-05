#pragma once
#include <util/generic/hash_set.h>
#include <ydb/core/tablet_flat/flat_cxx_database.h>
#include <ydb/core/tablet_flat/tablet_flat_executor.h>

namespace NKikimr::NOlap::NTxInteractions {

class TTxConflicts {
private:
    THashMap<ui64, THashSet<ui64>> TxIdsFromCommitToBroken;

public:
    THashMap<ui64, THashSet<ui64>>::const_iterator begin() const {
        return TxIdsFromCommitToBroken.begin();
    }

    THashMap<ui64, THashSet<ui64>>::const_iterator end() const {
        return TxIdsFromCommitToBroken.end();
    }

    bool Add(const ui64 commitTxId, const ui64 brokenTxId) {
        return TxIdsFromCommitToBroken[commitTxId].emplace(brokenTxId).second;
    }

    THashSet<ui64> GetBrokenTxIds(const ui64 txId) const {
        auto it = TxIdsFromCommitToBroken.find(txId);
        if (it == TxIdsFromCommitToBroken.end()) {
            return Default<THashSet<ui64>>();
        }
        return it->second;
    }

    bool LoadFromDatabase(NIceDb::TNiceDb& db);

    bool RemoveOnExecute(NTabletFlatExecutor::TTransactionContext& txc, const ui64 txId) const;

    [[nodiscard]] bool RemoveOnComplete(const ui64 txId) {
        return TxIdsFromCommitToBroken.erase(txId);
    }

    void AddOnExecute(NTabletFlatExecutor::TTransactionContext& txc) const;

    void MergeTo(TTxConflicts& dest) const {
        for (auto&& i : TxIdsFromCommitToBroken) {
            auto it = dest.TxIdsFromCommitToBroken.find(i.first);
            if (it == dest.TxIdsFromCommitToBroken.end()) {
                dest.TxIdsFromCommitToBroken.emplace(i.first, i.second);
            } else {
                it->second.insert(i.second.begin(), i.second.end());
            }
        }
    }
};

}   // namespace NKikimr::NOlap::NTxInteractions
