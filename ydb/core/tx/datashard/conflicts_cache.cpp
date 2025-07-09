#include "conflicts_cache.h"
#include "datashard_impl.h"

namespace NKikimr::NDataShard {

TTableConflictsCache::TTableConflictsCache(ui64 localTid)
    : LocalTid(localTid)
{}

void TTableConflictsCache::AddUncommittedWrite(TConstArrayRef<TCell> key, ui64 txId, NTable::TDatabase& db) {
    if (WriteKeys.empty()) {
        // Avoid expensive key lookup when we know cache is empty
        return;
    }

    auto itWriteKey = WriteKeys.find(key);
    if (itWriteKey == WriteKeys.end()) {
        return; // we don't have matching distributed writes
    }

    auto* k = itWriteKey->second.get();
    auto res = k->UncommittedWrites.insert(txId);
    if (res.second) {
        // New write key for this uncommitted tx
        auto& p = UncommittedWrites[txId];
        if (!p) {
            p = std::make_unique<TUncommittedWrite>();
        }
        auto r = p->WriteKeys.insert(k);
        Y_ENSURE(r.second);
        AddRollbackOp(TRollbackOpRemoveUncommittedWrite{ txId, k }, db);
    }
}

void TTableConflictsCache::RemoveUncommittedWrites(TConstArrayRef<TCell> key, NTable::TDatabase& db) {
    if (UncommittedWrites.empty()) {
        // Avoid expensive kep lookup when we know there are no writes
        return;
    }

    auto itWriteKey = WriteKeys.find(key);
    if (itWriteKey == WriteKeys.end()) {
        return; // we don't have matching distributed writes
    }

    auto* k = itWriteKey->second.get();
    for (ui64 txId : k->UncommittedWrites) {
        auto it = UncommittedWrites.find(txId);
        Y_ENSURE(it != UncommittedWrites.end());
        auto r = it->second->WriteKeys.erase(k);
        Y_ENSURE(r);
        AddRollbackOp(TRollbackOpAddUncommittedWrite{ txId, k }, db);
        if (it->second->WriteKeys.empty()) {
            AddRollbackOp(TRollbackOpRestoreUncommittedWrite{ txId, std::move(it->second) }, db);
            UncommittedWrites.erase(it);
        }
    }

    k->UncommittedWrites.clear();
}

void TTableConflictsCache::RemoveUncommittedWrites(ui64 txId, NTable::TDatabase& db) {
    auto it = UncommittedWrites.find(txId);
    if (it == UncommittedWrites.end()) {
        return;
    }

    auto* p = it->second.get();

    // Note: UncommittedWrite will be kept alive by rollback buffer
    AddRollbackOp(TRollbackOpRestoreUncommittedWrite{ txId, std::move(it->second) }, db);
    UncommittedWrites.erase(it);

    // Note: on rollback these links will be restored
    for (auto* k : p->WriteKeys) {
        k->UncommittedWrites.erase(txId);
    }
}

class TTableConflictsCache::TTxObserver : public NTable::ITransactionObserver {
public:
    TTxObserver() {}

    void OnSkipUncommitted(ui64 txId) override {
        Y_ENSURE(Target);
        Target->insert(txId);
    }

    void OnSkipCommitted(const TRowVersion&) override {}
    void OnSkipCommitted(const TRowVersion&, ui64) override {}
    void OnApplyCommitted(const TRowVersion&) override {}
    void OnApplyCommitted(const TRowVersion&, ui64) override {}

    void SetTarget(absl::flat_hash_set<ui64>* target) {
        Target = target;
    }

private:
    absl::flat_hash_set<ui64>* Target = nullptr;
};

bool TTableConflictsCache::RegisterDistributedWrite(ui64 txId, const TOwnedCellVec& key, NTable::TDatabase& db) {
    if (auto it = WriteKeys.find(key); it != WriteKeys.end()) {
        // We already have this write key cached
        auto* k = it->second.get();
        auto& p = DistributedWrites[txId];
        if (!p) {
            p = std::make_unique<TDistributedWrite>();
        }
        auto r = p->WriteKeys.insert(k);
        if (r.second) {
            k->DistributedWrites++;
        }
        return true;
    }

    absl::flat_hash_set<ui64> txIds;

    // Search for conflicts only when we know there are uncommitted changes right now
    if (db.GetOpenTxCount(LocalTid)) {
        if (!TxObserver) {
            TxObserver = new TTxObserver();
        }

        static_cast<TTxObserver*>(TxObserver.Get())->SetTarget(&txIds);
        auto res = db.SelectRowVersion(LocalTid, key, /* readFlags */ 0, nullptr, TxObserver);
        static_cast<TTxObserver*>(TxObserver.Get())->SetTarget(nullptr);

        if (res.Ready == NTable::EReady::Page) {
            return false;
        }

        auto itTxIds = txIds.begin();
        while (itTxIds != txIds.end()) {
            // Filter removed transactions
            if (db.HasRemovedTx(LocalTid, *itTxIds)) {
                txIds.erase(itTxIds++);
            } else {
                ++itTxIds;
            }
        }
    }

    auto& k = WriteKeys[key];
    Y_ENSURE(!k);
    k = std::make_unique<TWriteKey>();
    k->Key = key;

    if (!txIds.empty()) {
        k->UncommittedWrites = std::move(txIds);
        for (ui64 txId : k->UncommittedWrites) {
            auto& p = UncommittedWrites[txId];
            if (!p) {
                p = std::make_unique<TUncommittedWrite>();
            }
            auto r = p->WriteKeys.insert(k.get());
            Y_ENSURE(r.second);
        }
    }

    auto& p = DistributedWrites[txId];
    if (!p) {
        p = std::make_unique<TDistributedWrite>();
    }
    auto r = p->WriteKeys.insert(k.get());
    Y_ENSURE(r.second);
    k->DistributedWrites++;

    return true;
}

void TTableConflictsCache::UnregisterDistributedWrites(ui64 txId) {
    if (!RollbackOps.empty()) {
        RollbackAllowed = false;
    }

    auto it = DistributedWrites.find(txId);
    if (it == DistributedWrites.end()) {
        return;
    }

    std::unique_ptr<TDistributedWrite> p = std::move(it->second);
    DistributedWrites.erase(it);

    for (auto* k : p->WriteKeys) {
        Y_ENSURE(k->DistributedWrites > 0);
        if (0 == --k->DistributedWrites) {
            DropWriteKey(k);
        }
    }
}

void TTableConflictsCache::DropWriteKey(TWriteKey* k) {
    Y_ENSURE(k->DistributedWrites == 0);

    auto itWriteKey = WriteKeys.find(k->Key);
    Y_ENSURE(itWriteKey != WriteKeys.end());
    Y_ENSURE(itWriteKey->second.get() == k);

    std::unique_ptr<TWriteKey> saved = std::move(itWriteKey->second);
    WriteKeys.erase(itWriteKey);

    for (ui64 txId : k->UncommittedWrites) {
        auto it = UncommittedWrites.find(txId);
        Y_ENSURE(it != UncommittedWrites.end());
        it->second->WriteKeys.erase(k);
        if (it->second->WriteKeys.empty()) {
            UncommittedWrites.erase(it);
        }
    }
}

const absl::flat_hash_set<ui64>* TTableConflictsCache::FindUncommittedWrites(TConstArrayRef<TCell> key) {
    if (WriteKeys.empty()) {
        // Avoid expensive key lookup when we know cache is empty
        return nullptr;
    }

    auto it = WriteKeys.find(key);
    if (it == WriteKeys.end()) {
        return nullptr;
    }

    return &it->second->UncommittedWrites;
}

void TTableConflictsCache::AddRollbackOp(TRollbackOp&& op, NTable::TDatabase& db) {
    if (RollbackOps.empty()) {
        db.OnCommit([this]() {
            OnCommitChanges();
        });
        db.OnRollback([this]() {
            OnRollbackChanges();
        });
    }
    RollbackOps.push_back(std::move(op));
}

void TTableConflictsCache::OnCommitChanges() {
    RollbackOps.clear();
    RollbackAllowed = true;
}

void TTableConflictsCache::OnRollbackChanges() {
    Y_ENSURE(RollbackAllowed, "Unexpected transaction rollback");

    struct TPerformRollback {
        TTableConflictsCache* Self;

        void operator()(TRollbackOpAddUncommittedWrite& op) const {
            auto it = Self->UncommittedWrites.find(op.TxId);
            Y_ENSURE(it != Self->UncommittedWrites.end());
            auto* p = it->second.get();
            auto* k = op.WriteKey;
            auto r1 = p->WriteKeys.insert(k);
            Y_ENSURE(r1.second);
            auto r2 = k->UncommittedWrites.insert(op.TxId);
            Y_ENSURE(r2.second);
        }

        void operator()(TRollbackOpRemoveUncommittedWrite& op) const {
            auto it = Self->UncommittedWrites.find(op.TxId);
            Y_ENSURE(it != Self->UncommittedWrites.end());
            auto* p = it->second.get();
            auto* k = op.WriteKey;
            auto r1 = p->WriteKeys.erase(k);
            Y_ENSURE(r1);
            auto r2 = k->UncommittedWrites.erase(op.TxId);
            Y_ENSURE(r2);
            if (p->WriteKeys.empty()) {
                Self->UncommittedWrites.erase(it);
            }
        }

        void operator()(TRollbackOpRestoreUncommittedWrite& op) const {
            auto r1 = Self->UncommittedWrites.emplace(op.TxId, std::move(op.Data));
            Y_ENSURE(r1.second);
            auto& p = r1.first->second;
            for (auto* k : p->WriteKeys) {
                auto r2 = k->UncommittedWrites.insert(op.TxId);
                Y_ENSURE(r2.second);
            }
        }
    };

    while (!RollbackOps.empty()) {
        std::visit(TPerformRollback{ this }, RollbackOps.back());
        RollbackOps.pop_back();
    }
}

class TConflictsCache::TTxFindWriteConflicts
    : public NTabletFlatExecutor::TTransactionBase<TDataShard>
{
public:
    TTxFindWriteConflicts(TDataShard* self, ui64 txId)
        : TBase(self)
        , TxId(txId)
    { }

    TTxType GetTxType() const override { return TXTYPE_FIND_WRITE_CONFLICTS; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        auto& cache = Self->GetConflictsCache();
        auto itWrites = cache.PendingWrites.find(TxId);
        if (itWrites == cache.PendingWrites.end()) {
            // Already cancelled
            return true;
        }

        auto& writes = itWrites->second;
        Y_ENSURE(!writes.empty());

        auto dst = writes.begin();
        for (auto it = writes.begin(); it != writes.end(); ++it) {
            if (!cache.GetTableCache(it->LocalTid).RegisterDistributedWrite(TxId, it->WriteKey, txc.DB)) {
                if (dst != it) {
                    *dst = std::move(*it);
                }
                ++dst;
            }
        }
        writes.erase(dst, writes.end());

        if (!writes.empty()) {
            return false;
        }

        cache.PendingWrites.erase(itWrites);
        return true;
    }

    void Complete(const TActorContext&) override {
        // nothing
    }

private:
    const ui64 TxId;
};

void TConflictsCache::RegisterDistributedWrites(ui64 txId, TPendingWrites&& writes, NTable::TDatabase& db) {
    auto dst = writes.begin();
    for (auto it = writes.begin(); it != writes.end(); ++it) {
        if (!GetTableCache(it->LocalTid).RegisterDistributedWrite(txId, it->WriteKey, db)) {
            if (dst != it) {
                *dst = std::move(*it);
            }
            ++dst;
        }
    }
    writes.erase(dst, writes.end());

    if (!writes.empty()) {
        PendingWrites[txId] = std::move(writes);
        Self->Enqueue(new TTxFindWriteConflicts(Self, txId));
    }
}

void TConflictsCache::UnregisterDistributedWrites(ui64 txId) {
    for (auto& pr : Tables) {
        pr.second.UnregisterDistributedWrites(txId);
    }
    PendingWrites.erase(txId);
}

} // namespace NKikimr::NDataShard
