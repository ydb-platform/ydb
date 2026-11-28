#include "datashard_locks_db.h"

#include <library/cpp/containers/stack_vector/stack_vec.h>

namespace NKikimr::NDataShard {

void TDataShardLocksDb::PersistLockCounter(ui64 lockId, ui64 counter) {
    TBase::PersistLockCounter(lockId, counter);
    if (counter == Max<ui64>()) {
        NIceDb::TNiceDb db(DB);
        Self.GetMultiTxIdManager().OnLockBroken(db, lockId);
    }
}

void TDataShardLocksDb::PersistRemoveLock(ui64 lockId) {
    // We remove lock changes unless it's managed by volatile tx manager
    bool isVolatile = Self.GetVolatileTxManager().FindByCommitTxId(lockId);
    if (!isVolatile) {
        for (auto& pr : Self.GetUserTables()) {
            auto tid = pr.second->LocalTid;
            // Removing the lock also removes any uncommitted data
            if (DB.HasOpenTx(tid, lockId)) {
                DB.RemoveTx(tid, lockId);
                Self.AbortHnswIndexChanges(tid, lockId, DB);
                Self.GetConflictsCache().GetTableCache(tid).RemoveUncommittedWrites(lockId, DB);
            }
        }
    }

    using Schema = TDataShard::Schema;
    NIceDb::TNiceDb db(DB);
    db.Table<typename Schema::Locks>().Key(lockId).Delete();
    HasChanges_ = true;

    Self.GetMultiTxIdManager().OnLockRemoved(db, lockId);

    if (!isVolatile) {
        Self.ScheduleRemoveLockChanges(lockId);
    }
}

namespace {

constexpr size_t MaxAncestorGcRedoBytes = 256 * 1024;

TStackVec<TRawTypeValue, 5> AncestorGcRawKey(TConstArrayRef<TCell> cells) {
    TStackVec<TRawTypeValue, 5> key;
    for (size_t i = 0; i < cells.size(); ++i) {
        const auto type = i < 4 ? NScheme::NTypeIds::Uint64 : NScheme::NTypeIds::String;
        key.emplace_back(cells[i].Data(), cells[i].Size(), type);
    }
    return key;
}

using EAncestorGcResult = TDataShardLocksDb::EAncestorGcResult;

EAncestorGcResult RemoveAncestorGcRange(NTable::TDatabase& database, ui32 tableId, size_t keyColumns,
        TConstArrayRef<TCell> prefix, TOwnedCellVec& lastKey, size_t& remaining) {
    const auto upper = AncestorGcRawKey(prefix);
    const bool fromStart = lastKey.empty();
    auto lower = fromStart ? upper : AncestorGcRawKey(lastKey);
    if (fromStart) {
        // A short inclusive upper bound covers the whole prefix. The inclusive
        // lower bound must have every key column, with NULL for missing suffixes.
        lower.resize(keyColumns);
    }

    TVector<TOwnedCellVec> deletes;
    TOwnedCellVec last;
    bool finished = false;
    {
        auto it = database.IterateRange(tableId, {lower, upper, fromStart, true}, {});
        size_t bytes = database.GetCommitRedoBytes();
        while (remaining) {
            const auto ready = it->Next(NTable::ENext::All);
            if (ready == NTable::EReady::Gone) {
                // Empty ranges still cost a seek and consume part of the batch.
                --remaining;
                finished = true;
                break;
            }
            if (ready == NTable::EReady::Page) {
                break;
            }

            const auto key = it->GetKey().Cells();
            if (it->GetKeyState() != NTable::ERowOp::Erase) {
                // Leave room for the redo framing. A single large key gets
                // a transaction of its own.
                size_t cost = 128;
                for (const auto& cell : key) {
                    cost += cell.Size();
                }
                if (bytes && bytes + cost > MaxAncestorGcRedoBytes) {
                    break;
                }
                bytes += cost;
                deletes.emplace_back(key);
                last = deletes.back();
            } else {
                last = TOwnedCellVec(key);
            }
            --remaining;
        }
    }
    for (const auto& key : deletes) {
        database.Update(tableId, NTable::ERowOp::Erase, AncestorGcRawKey(key), {});
    }
    if (finished) {
        lastKey = {};
        return EAncestorGcResult::Done;
    }
    if (last.empty()) {
        return EAncestorGcResult::NotReady;
    }
    lastKey = std::move(last);
    return EAncestorGcResult::More;
}

} // namespace

TDataShardLocksDb::EAncestorGcResult TDataShardLocksDb::RemoveAncestorGcBatch(NTable::TDatabase& database,
        const TAncestorGcTask& task, TAncestorGcCursor& cursor, size_t& remaining) {
    using EStage = EAncestorGcStage;
    const auto initialStage = cursor.Stage;
    TStackVec<TCell, 4> prefix{TCell::Make(task.LockId)};
    if (task.Writer) {
        const auto& [owner, createdOn, writer] = *task.Writer;
        prefix.push_back(TCell::Make(owner));
        prefix.push_back(TCell::Make(createdOn));
        prefix.push_back(TCell::Make(writer));
    }

    // A full task keeps its ancestor metadata on disk until every branch
    // is gone, so Load can recover the task after any committed batch.
    while (cursor.Stage != EStage::Done && remaining && database.GetCommitRedoBytes() < MaxAncestorGcRedoBytes) {
        EAncestorGcResult result;
        EStage next;
        switch (cursor.Stage) {
            case EStage::Branches:
                result = RemoveAncestorGcRange(database, TSchema::AncestorLockBranches::TableId, 5,
                    prefix, cursor.LastKey, remaining);
                next = task.Writer ? EStage::Done : EStage::WriterStates;
                break;
            case EStage::WriterStates:
                result = RemoveAncestorGcRange(database, TSchema::AncestorLockWriteSeqNums::TableId, 4,
                    prefix, cursor.LastKey, remaining);
                next = EStage::Ancestors;
                break;
            case EStage::Ancestors:
                result = RemoveAncestorGcRange(database, TSchema::AncestorShardsLocks::TableId, 3,
                    prefix, cursor.LastKey, remaining);
                next = EStage::Done;
                break;
            case EStage::Done:
                Y_ABORT("Ancestor GC task is already complete");
        }
        if (result == EAncestorGcResult::NotReady && cursor.Stage != initialStage) {
            return EAncestorGcResult::More;
        }
        if (result != EAncestorGcResult::Done) {
            return result;
        }
        cursor.Stage = next;
    }
    return cursor.Stage == EStage::Done ? EAncestorGcResult::Done : EAncestorGcResult::More;
}

class TDataShard::TTxAncestorGc : public NTabletFlatExecutor::TTransactionBase<TDataShard> {
    using TTask = ILocksDb::TAncestorGcTask;
    using TCursor = TDataShardLocksDb::TAncestorGcCursor;
    using TContinuation = std::pair<TTask, TCursor>;

public:
    explicit TTxAncestorGc(TDataShard* self, std::optional<TContinuation> resume = {})
        : TBase(self)
        , Resume(std::move(resume))
    {}

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        const auto& pending = Self->PendingAncestorGc;
        auto it = Resume ? pending.find(Resume->first) : pending.begin();
        AFL_ENSURE(it != pending.end());
        TCursor cursor = Resume ? Resume->second : TCursor{};
        size_t remaining = TDataShardLocksDb::AncestorGcScanBudget;
        for (; it != pending.end() && remaining && txc.DB.GetCommitRedoBytes() < MaxAncestorGcRedoBytes; ++it) {
            switch (TDataShardLocksDb::RemoveAncestorGcBatch(txc.DB, *it, cursor, remaining)) {
                case EAncestorGcResult::NotReady:
                    return !Finished.empty();
                case EAncestorGcResult::More:
                    Next.emplace(*it, std::move(cursor));
                    return true;
                case EAncestorGcResult::Done:
                    Finished.push_back(*it);
                    cursor = {};
                    break;
            }
        }
        return true;
    }

    void Complete(const TActorContext&) override {
        for (const auto& task : Finished) {
            AFL_ENSURE(Self->PendingAncestorGc.erase(task));
        }
        if (!Self->PendingAncestorGc.empty()) {
            Self->EnqueueLowPriority(new TTxAncestorGc(Self, std::move(Next)));
        }
    }

private:
    const std::optional<TContinuation> Resume;
    std::optional<TContinuation> Next;
    TVector<TTask> Finished;
};

void TDataShard::ScheduleAncestorGc(ILocksDb::TAncestorGcTask task) {
    if (PendingAncestorGc.insert(std::move(task)).second && PendingAncestorGc.size() == 1) {
        EnqueueLowPriority(new TTxAncestorGc(this));
    }
}

bool TDataShardLocksDb::HasPendingAncestorGc(ui64 lockId) const {
    const auto it = Self.PendingAncestorGc.lower_bound({lockId, std::nullopt});
    return it != Self.PendingAncestorGc.end() && it->LockId == lockId;
}

bool TDataShardLocksDb::MayAddLock(ui64 lockId) {
    // TODO: If clients reuse LockTxId across transactions, also reserve it
    // until old uncommitted change records are gone. KQP assigns a new id.
    if (HasPendingAncestorGc(lockId)) {
        return false;
    }
    for (auto& pr : Self.GetUserTables()) {
        auto tid = pr.second->LocalTid;
        // We cannot start a new lockId if it has any uncompacted data
        if (DB.HasTxData(tid, lockId)) {
            return false;
        }
    }
    return true;
}

bool TDataShardLocksDb::MayActivateLock(ui64 lockId) {
    if (HasPendingAncestorGc(lockId)) {
        return false;
    }
    // Unlike MayAddLock, activation has a known tracker proving there are no
    // local deltas. HasTxData would also see harmless references elsewhere in
    // borrowed parts and incorrectly forbid the first local write.
    for (const auto& [_, table] : Self.GetUserTables()) {
        // Commit/abort decisions still apply to future writes with this id.
        if (DB.HasCommittedTx(table->LocalTid, lockId) || DB.HasRemovedTx(table->LocalTid, lockId)) {
            return false;
        }
    }
    return true;
}

} // namespace NKikimr::NDataShard
