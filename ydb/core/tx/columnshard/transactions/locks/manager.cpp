#include "manager.h"
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>

namespace NKikimr::NOlap::NTxInteractions {

namespace {
class TAddEventTransaction: public NDataSharing::TExtendedTransactionBase<NColumnShard::TColumnShard> {
private:
    using TBase = NDataSharing::TExtendedTransactionBase<NColumnShard::TColumnShard>;
    const ui64 TxId;
    const std::shared_ptr<ITxEventWriter> Writer;
    TTxConflicts ConflictedTxIds;
    std::optional<TTxEventContainer> TxEventContainer;

protected:
    virtual bool DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const NActors::TActorContext& /*ctx*/) override {
        ConflictedTxIds = Writer->CheckInteraction(TxId, Self->GetProgressTxController().MutableInteractionsManager().MutableInteractionContext());
        TxEventContainer = TTxEventContainer(TxId, Self->Generation(), Writer->BuildEvent());
        Self->GetProgressTxController().MutableInteractionsManager().AddConflictsOnExecute(txc, ConflictedTxIds);
        Self->GetProgressTxController().MutableInteractionsManager().AddEventOnExecute(txc, *TxEventContainer);
        return true;
    }
    virtual void DoComplete(const NActors::TActorContext& /*ctx*/) override {
        Self->GetProgressTxController().MutableInteractionsManager().AddConflictsOnComplete(ConflictedTxIds);
        Self->GetProgressTxController().MutableInteractionsManager().AddEventOnComplete(std::move(*TxEventContainer));
    }

public:
    TAddEventTransaction(NColumnShard::TColumnShard* shard, const ui64 txId, const std::shared_ptr<ITxEventWriter> writer)
        : TBase(shard)
        , TxId(txId)
        , Writer(writer) {
        AFL_VERIFY(Writer);
    }
};

}   // namespace

void TTxState::BrokenOnExecute(NIceDb::TNiceDb& db) {
    if (Broken) {
        return;
    }
    Broken = true;
    using namespace NColumnShard;
    db.Table<Schema::TxStates>().Key(TxId).Update(NIceDb::TUpdate<Schema::TxStates::Broken>(true));
}

void TTxState::RemoveOnExecute(NIceDb::TNiceDb& db) {
    using namespace NColumnShard;
    db.Table<Schema::TxStates>().Key(TxId).Delete();
}

std::unique_ptr<NKikimr::NTabletFlatExecutor::ITransaction> TManager::TxAddEvent(
    NColumnShard::TColumnShard* shard, const ui64 txId, const std::shared_ptr<ITxEventWriter>& writer) {
    AFL_VERIFY(!!writer);
    return std::make_unique<TAddEventTransaction>(shard, txId, writer);
}

namespace {
class TCommitTransaction: public NDataSharing::TExtendedTransactionBase<NColumnShard::TColumnShard> {
private:
    using TBase = NDataSharing::TExtendedTransactionBase<NColumnShard::TColumnShard>;
    const ui64 TxId;

protected:
    virtual bool DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const NActors::TActorContext& /*ctx*/) override {
        Self->GetProgressTxController().MutableInteractionsManager().CommitTxOnExecute(txc, TxId);
        return true;
    }
    virtual void DoComplete(const NActors::TActorContext& /*ctx*/) override {
        Self->GetProgressTxController().MutableInteractionsManager().CommitTxOnComplete(TxId);
    }

public:
    TCommitTransaction(NColumnShard::TColumnShard* shard, const ui64 txId)
        : TBase(shard)
        , TxId(txId) {
    }
};

}   // namespace

std::unique_ptr<NKikimr::NTabletFlatExecutor::ITransaction> TManager::TxCommitTx(NColumnShard::TColumnShard* shard, const ui64 txId) {
    return std::make_unique<TCommitTransaction>(shard, txId);
}

namespace {
class TRollbackTransaction: public NDataSharing::TExtendedTransactionBase<NColumnShard::TColumnShard> {
private:
    using TBase = NDataSharing::TExtendedTransactionBase<NColumnShard::TColumnShard>;
    const ui64 TxId;

protected:
    virtual bool DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const NActors::TActorContext& /*ctx*/) override {
        Self->GetProgressTxController().MutableInteractionsManager().RollbackTxOnExecute(txc, TxId);
        return true;
    }
    virtual void DoComplete(const NActors::TActorContext& /*ctx*/) override {
        Self->GetProgressTxController().MutableInteractionsManager().RollbackTxOnComplete(TxId);
    }

public:
    TRollbackTransaction(NColumnShard::TColumnShard* shard, const ui64 txId)
        : TBase(shard)
        , TxId(txId) {
    }
};

}   // namespace

std::unique_ptr<NKikimr::NTabletFlatExecutor::ITransaction> TManager::TxRollbackTx(NColumnShard::TColumnShard* shard, const ui64 txId) {
    return std::make_unique<TRollbackTransaction>(shard, txId);
}

void TManager::AddEventOnExecute(NTabletFlatExecutor::TTransactionContext& txc, const TTxEventContainer& txEventContainer) const {
    using namespace NColumnShard;
    NIceDb::TNiceDb db(txc.DB);
    db.Table<Schema::TxEvents>()
        .Key(txEventContainer.GetTxId(), txEventContainer.GetGeneration(), txEventContainer.GetGenerationInternalId())
        .Update(NIceDb::TUpdate<Schema::TxEvents::Data>(txEventContainer.SerializeToString()));
}

bool TManager::LoadFromDatabase(NTabletFlatExecutor::TTransactionContext& txc) {
    using namespace NColumnShard;
    NIceDb::TNiceDb db(txc.DB);

    THashMap<ui64, TTxState> transactions;
    {
        auto rowset = db.Table<Schema::TxStates>().Select();
        if (!rowset.IsReady()) {
            return false;
        }

        while (!rowset.EndOfSet()) {
            const ui64 txId = rowset.GetValue<Schema::TxStates::TxId>();
            transactions.emplace(txId, TTxState(txId, rowset.GetValue<Schema::TxStates::Broken>()));
            if (!rowset.Next()) {
                return false;
            }
        }
    }

    {
        auto rowset = db.Table<Schema::TxEvents>().Select();
        if (!rowset.IsReady()) {
            return false;
        }

        while (!rowset.EndOfSet()) {
            const ui64 txId = rowset.GetValue<Schema::TxEvents::TxId>();
            auto it = transactions.emplace(txId, TTxState(txId, false)).first;
            TTxEventContainer container(
                txId, rowset.GetValue<Schema::TxEvents::GenerationId>(), rowset.GetValue<Schema::TxEvents::GenerationInternalId>());
            container.DeserializeFromString(rowset.GetValue<Schema::TxEvents::Data>()).Validate();
            it->second.AddEvent(std::move(container));

            if (!rowset.Next()) {
                return false;
            }
        }
    }

    if (!TxConflicts.LoadFromDatabase(db)) {
        return false;
    }

    std::swap(transactions, Transactions);

    for (auto&& i : Transactions) {
        i.second.AddToInteraction(InteractionsContext);
    }

    return true;
}

}
