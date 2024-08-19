#pragma once
#include "abstract.h"

#include <ydb/core/tablet_flat/tablet_flat_executor.h>
#include <ydb/core/tx/columnshard/data_sharing/common/transactions/tx_extension.h>

#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NOlap::NTxInteractions {

class TTxState {
private:
    YDB_READONLY(ui64, TxId, 0);
    YDB_READONLY_DEF(std::vector<TTxEventContainer>, Events);
    YDB_READONLY(bool, Broken, false);

public:
    TTxState(const ui64 txId, const bool broken)
        : TxId(txId)
        , Broken(broken)

    {
    }

    void BrokenOnExecute(NIceDb::TNiceDb& db);
    void RemoveOnExecute(NIceDb::TNiceDb& db);

    void AddEvent(TTxEventContainer&& container) {
        if (Events.size()) {
            AFL_VERIFY(Events.back() < container);
        }
        Events.emplace_back(std::move(container));
    }

    void AddToInteraction(TInteractionsContext& context) const {
        for (auto&& i : Events) {
            i.AddToInteraction(context);
        }
    }

    void RemoveFromInteraction(TInteractionsContext& context) const {
        for (auto&& i : Events) {
            i.RemoveFromInteraction(context);
        }
    }
};

class TManager {
private:
    const ui64 Generation;
    TInteractionsContext InteractionsContext;
    THashMap<ui64, TTxState> Transactions;
    TTxConflicts TxConflicts;

public:
    TManager(const ui64 generation)
        : Generation(generation) {
    }

    const TInteractionsContext& GetInteractionContext() const {
        return InteractionsContext;
    }

    TInteractionsContext& MutableInteractionContext() {
        return InteractionsContext;
    }

    [[nodiscard]] std::unique_ptr<NTabletFlatExecutor::ITransaction> TxAddEvent(
        NColumnShard::TColumnShard* shard, const ui64 txId, const std::shared_ptr<ITxEventWriter>& writer);

    void AddEventOnExecute(NTabletFlatExecutor::TTransactionContext& txc, const TTxEventContainer& txEventContainer) const;

    void AddEventOnComplete(TTxEventContainer&& txEventContainer) {
        auto it = Transactions.find(txEventContainer.GetTxId());
        AFL_VERIFY(it != Transactions.end());
        it->second.AddEvent(std::move(txEventContainer));
    }

    void AddConflictsOnExecute(NTabletFlatExecutor::TTransactionContext& txc, const TTxConflicts& txConflicts) const {
        txConflicts.AddOnExecute(txc);
    }

    void AddConflictsOnComplete(const TTxConflicts& txConflicts) {
        txConflicts.MergeTo(TxConflicts);
    }

    [[nodiscard]] std::unique_ptr<NTabletFlatExecutor::ITransaction> TxCommitTx(NColumnShard::TColumnShard* shard, const ui64 txId);

    void CommitTxOnExecute(NTabletFlatExecutor::TTransactionContext& txc, const ui64 txId) {
        auto it = Transactions.find(txId);
        AFL_VERIFY(it != Transactions.end())("tx_id", txId);
        AFL_VERIFY(!it->second.GetBroken());
        NIceDb::TNiceDb db(txc.DB);
        const THashSet<ui64> txBroken = TxConflicts.GetBrokenTxIds(txId);
        for (auto&& i : txBroken) {
            auto it = Transactions.find(i);
            AFL_VERIFY(it != Transactions.end())("tx_id", i);
            it->second.BrokenOnExecute(db);
        }
        TxConflicts.RemoveOnExecute(txc, txId);
        it->second.RemoveOnExecute(db);
    }

    void CommitTxOnComplete(const ui64 txId) {
        Y_UNUSED(TxConflicts.RemoveOnComplete(txId));
        auto it = Transactions.find(txId);
        AFL_VERIFY(it != Transactions.end())("tx_id", txId);
        Transactions.erase(it);
    }

    [[nodiscard]] std::unique_ptr<NTabletFlatExecutor::ITransaction> TxRollbackTx(NColumnShard::TColumnShard* shard, const ui64 txId);

    void RollbackTxOnExecute(NTabletFlatExecutor::TTransactionContext& txc, const ui64 txId) {
        auto it = Transactions.find(txId);
        if (it == Transactions.end()) {
            return;
        }
        NIceDb::TNiceDb db(txc.DB);
        TxConflicts.RemoveOnExecute(txc, txId);
        it->second.RemoveOnExecute(db);
    }

    void RollbackTxOnComplete(const ui64 txId) {
        Y_UNUSED(TxConflicts.RemoveOnComplete(txId));
        auto it = Transactions.find(txId);
        AFL_VERIFY(it != Transactions.end())("tx_id", txId);
        Transactions.erase(it);
    }

    bool LoadFromDatabase(NTabletFlatExecutor::TTransactionContext& txc);

    bool CheckToCommit(const ui64 txId) const {
        auto it = Transactions.find(txId);
        if (it == Transactions.end()) {
            return true;
        }
        return !it->second.GetBroken();
    }
};

}   // namespace NKikimr::NOlap::NTxInteractions
