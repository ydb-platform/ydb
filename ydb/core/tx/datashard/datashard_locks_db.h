#pragma once
#include <ydb/core/tx/locks/locks_db.h>

#include "datashard_impl.h"

namespace NKikimr::NDataShard {

class TDataShardLocksDb : public NLocks::TShardLocksDb<TDataShard, TDataShard::Schema> {
private:
    using TBase = NLocks::TShardLocksDb<TDataShard, TDataShard::Schema>;

public:
    using TSchema = TDataShard::Schema;
    using TBase::TBase;

    // Block reuse of this LockId until the last batch of ancestor deletes commits.
    bool HasPendingAncestorGc(ui64 lockId) const override;
    void PersistLockCounter(ui64 lockId, ui64 counter) override;
    void PersistRemoveLock(ui64 lockId) override;

    enum class EAncestorGcStage {
        Branches,     // Delete branch rows first. A writer-only task ends here.
        WriterStates, // Delete the writer aggregates after their branches are gone.
        Ancestors,    // Delete ancestor rows last. Startup uses them to recover the task.
        Done,         // All ranges covered by this task have been scanned.
    };

    enum class EAncestorGcResult {
        NotReady, // No progress on this task: a page is missing or the next delete exceeds the redo budget.
        More,     // Continue the unfinished task from the updated cursor in the next batch.
        Done,     // Drop the task from the queue once this transaction commits.
    };

    struct TAncestorGcCursor {
        EAncestorGcStage Stage = EAncestorGcStage::Branches;
        // An empty key starts the range. A full key resumes strictly after it,
        // including when the last processed row was a tombstone.
        TOwnedCellVec LastKey;
    };

    // Share the scan budget across all tasks in one transaction. Tombstones and
    // end-of-range seeks also consume it, even when they produce no deletes.
    static constexpr size_t AncestorGcScanBudget = 32;
    static EAncestorGcResult RemoveAncestorGcBatch(NTable::TDatabase& database, const TAncestorGcTask& task,
        TAncestorGcCursor& cursor, size_t& remaining);

    bool MayAddLock(ui64 lockId) override;
    // Activation adds logical state to an existing TrackingOnly object. Unlike
    // a new id, it may already have harmless references in borrowed parts.
    bool MayActivateLock(ui64 lockId) override;

};

} // namespace NKikimr::NDataShard
